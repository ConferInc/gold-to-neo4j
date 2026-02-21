"""Schema drift detection, heuristic resolution, and alias normalization."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from shared.agent_client import resolve_schema_drift
from shared.logging import get_logger

LOG = get_logger(__name__)


class SchemaDriftError(RuntimeError):
    """Raised when required columns are missing and cannot be resolved."""


@dataclass
class ColumnMeta:
    name: str
    data_type: Optional[str] = None
    udt_name: Optional[str] = None
    is_nullable: Optional[str] = None
    is_primary_key: bool = False
    is_unique: bool = False
    foreign_key_target: Optional[Tuple[str, str]] = None


@dataclass
class TablePlan:
    select_columns: Optional[List[str]]
    alias_map: Dict[str, str]
    updated_at_column: Optional[str]
    filters: Dict[str, Any]


UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$")
ROOT = Path(__file__).resolve().parents[1]


def _tokenize(value: str) -> List[str]:
    return [token for token in value.lower().split("_") if token]


def _name_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def _token_overlap(a: str, b: str) -> float:
    a_tokens = set(_tokenize(a))
    b_tokens = set(_tokenize(b))
    if not a_tokens or not b_tokens:
        return 0.0
    return len(a_tokens & b_tokens) / max(len(a_tokens), len(b_tokens))


def _suffix_bonus(expected: str, candidate: str) -> float:
    if expected.endswith("_id"):
        if candidate.endswith("_id"):
            return 0.15
        if candidate.endswith("_uuid") or candidate.endswith("_number"):
            return 0.1
    return 0.0


def _default_name_score(expected: str, candidate: str) -> float:
    sim = _name_similarity(expected, candidate)
    overlap = _token_overlap(expected, candidate)
    bonus = _suffix_bonus(expected, candidate)
    return (0.7 * sim) + (0.2 * overlap) + bonus


def _score_name(expected: str, candidates: List[str]) -> Dict[str, float]:
    return {candidate: _default_name_score(expected, candidate) for candidate in candidates}


def _normalize_type(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value.lower()


def _type_match(expected: Optional[str], actual: Optional[str]) -> float:
    expected_norm = _normalize_type(expected)
    actual_norm = _normalize_type(actual)
    if not expected_norm or not actual_norm:
        return 0.5
    if expected_norm == actual_norm:
        return 1.0
    compatible = {
        ("int4", "int8"),
        ("int8", "int4"),
        ("integer", "bigint"),
        ("bigint", "integer"),
    }
    if (expected_norm, actual_norm) in compatible:
        return 0.7
    return 0.0


def _value_shape_match(expected_type: Optional[str], sample: Iterable[Any]) -> float:
    expected_norm = _normalize_type(expected_type)
    if not expected_norm:
        return 0.5
    sample_vals = [val for val in sample if val is not None]
    if not sample_vals:
        return 0.5
    if expected_norm in {"uuid"}:
        matches = sum(1 for val in sample_vals if isinstance(val, str) and UUID_RE.match(val))
        return matches / len(sample_vals)
    if expected_norm in {"int4", "int8", "integer", "bigint"}:
        matches = sum(1 for val in sample_vals if isinstance(val, int))
        return matches / len(sample_vals)
    if expected_norm in {"text", "varchar", "character varying"}:
        matches = sum(1 for val in sample_vals if isinstance(val, str))
        return matches / len(sample_vals)
    return 0.5


def _collect_required_columns(table_cfg: Dict[str, Any]) -> List[str]:
    contract = table_cfg.get("schema_contract", {})
    required = contract.get("required_columns")
    if required:
        return list(required)

    columns = table_cfg.get("columns", [])
    primary_key = table_cfg.get("primary_key")
    updated_at = table_cfg.get("updated_at")
    filters = table_cfg.get("filters", {})

    required_cols: List[str] = []
    required_cols.extend(columns)
    if primary_key:
        required_cols.append(primary_key)
    if updated_at:
        required_cols.append(updated_at)
    required_cols.extend(filters.keys())

    seen = set()
    deduped: List[str] = []
    for col in required_cols:
        if col not in seen:
            deduped.append(col)
            seen.add(col)
    return deduped


def _collect_expected_types(table_cfg: Dict[str, Any]) -> Dict[str, str]:
    contract = table_cfg.get("schema_contract", {})
    types = contract.get("types", {})
    if isinstance(types, dict):
        return {str(k): str(v) for k, v in types.items()}
    return {}


def _expected_is_pk(table_cfg: Dict[str, Any], column: str) -> bool:
    return column == table_cfg.get("primary_key")


def _load_table_schema(supabase, schema: str, table: str) -> Dict[str, ColumnMeta]:
    columns = supabase.fetch_table_columns(schema, table)
    constraints = supabase.fetch_table_constraints(schema, table)
    key_usage = supabase.fetch_key_column_usage(schema, table)
    constraint_usage = supabase.fetch_constraint_column_usage(schema, table)

    constraint_types: Dict[str, str] = {}
    for row in constraints:
        name = row.get("constraint_name")
        ctype = row.get("constraint_type")
        if name and ctype:
            constraint_types[name] = ctype

    pk_cols: set[str] = set()
    unique_cols: set[str] = set()
    fk_cols: Dict[str, Tuple[str, str]] = {}

    for row in key_usage:
        constraint_name = row.get("constraint_name")
        column_name = row.get("column_name")
        if not constraint_name or not column_name:
            continue
        ctype = constraint_types.get(constraint_name)
        if ctype == "PRIMARY KEY":
            pk_cols.add(column_name)
        elif ctype == "UNIQUE":
            unique_cols.add(column_name)
        elif ctype == "FOREIGN KEY":
            fk_cols[column_name] = ("", "")

    ref_lookup: Dict[str, Tuple[str, str]] = {}
    for row in constraint_usage:
        cname = row.get("constraint_name")
        if not cname:
            continue
        ref_table = row.get("table_name") or ""
        ref_col = row.get("column_name") or ""
        ref_lookup[cname] = (ref_table, ref_col)

    for row in key_usage:
        constraint_name = row.get("constraint_name")
        column_name = row.get("column_name")
        if not constraint_name or not column_name:
            continue
        ctype = constraint_types.get(constraint_name)
        if ctype != "FOREIGN KEY":
            continue
        ref = ref_lookup.get(constraint_name)
        if ref:
            fk_cols[column_name] = ref

    info: Dict[str, ColumnMeta] = {}
    for col in columns:
        name = col.get("column_name")
        if not name:
            continue
        info[name] = ColumnMeta(
            name=name,
            data_type=col.get("data_type"),
            udt_name=col.get("udt_name"),
            is_nullable=col.get("is_nullable"),
            is_primary_key=name in pk_cols,
            is_unique=name in unique_cols or name in pk_cols,
            foreign_key_target=fk_cols.get(name),
        )
    return info


def _append_review_record(table_name: str, missing: List[str], aliases: Dict[str, str]) -> None:
    out_dir = ROOT / "state"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "agent_reviews.jsonl"
    record = {
        "table": table_name,
        "missing": missing,
        "aliases": aliases,
    }
    try:
        with out_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record))
            f.write("\n")
    except Exception:
        LOG.exception("failed to write agent review record", extra={"path": str(out_path)})


def _score_candidate(
    expected: str,
    expected_type: Optional[str],
    expected_pk: bool,
    candidate: ColumnMeta,
    name_score: float,
    sample_values: Optional[List[Any]] = None,
) -> float:
    type_score = _type_match(expected_type, candidate.udt_name or candidate.data_type)
    pk_score = 1.0 if (not expected_pk or candidate.is_unique) else 0.0
    expected_fk = expected.endswith("_id") and not expected_pk
    if expected_fk:
        fk_score = 1.0 if candidate.foreign_key_target else 0.0
    else:
        fk_score = 0.5
    shape_score = 0.5
    if sample_values is not None:
        shape_score = _value_shape_match(expected_type, sample_values)

    # Weights: name similarity is dominant but not absolute.
    return (
        0.45 * name_score
        + 0.25 * type_score
        + 0.15 * pk_score
        + 0.10 * fk_score
        + 0.05 * shape_score
    )


def _is_safety_critical(table_cfg: Dict[str, Any]) -> bool:
    return bool(table_cfg.get("safety_critical", False))


def _validate_agent_aliases(
    aliases: Dict[str, str],
    schema_info: Dict[str, ColumnMeta],
    expected_types: Dict[str, str],
    existing_aliases: Dict[str, str],
) -> Dict[str, str]:
    threshold = float(os.getenv("DRIFT_AGENT_NAME_THRESHOLD", "0.7"))
    validated: Dict[str, str] = {}
    used_actuals = set(existing_aliases.values())
    for expected, actual in aliases.items():
        if actual in used_actuals:
            continue
        if actual not in schema_info:
            continue
        candidate = schema_info[actual]
        actual_type = candidate.udt_name or candidate.data_type
        name_score = _default_name_score(expected, actual)
        expected_type = expected_types.get(expected)
        type_score = _type_match(expected_type, actual_type)
        if name_score < threshold:
            continue
        if expected_type and type_score < 1.0:
            continue
        validated[expected] = actual
        used_actuals.add(actual)
    return validated


def build_table_plan(
    supabase,
    schema: str,
    table_name: str,
    table_cfg: Dict[str, Any],
    *,
    confidence_threshold: Optional[float] = None,
) -> TablePlan:
    if confidence_threshold is None:
        confidence_threshold = float(os.getenv("DRIFT_CONFIDENCE_THRESHOLD", "0.85"))
    schema_info = _load_table_schema(supabase, schema, table_name)
    available = sorted(schema_info.keys())
    required = _collect_required_columns(table_cfg)
    expected_types = _collect_expected_types(table_cfg)
    alias_map: Dict[str, str] = dict(table_cfg.get("alias_map", {}))
    missing: List[str] = []
    decisions: Dict[str, Dict[str, Any]] = {}

    # If alias_map already provides a mapping, validate it.
    for expected, actual in list(alias_map.items()):
        if actual not in schema_info:
            LOG.warning(
                "alias_map target missing in schema",
                extra={"table": table_name, "expected": expected, "actual": actual},
            )
            alias_map.pop(expected, None)

    for col in required:
        if col in schema_info:
            expected_type = expected_types.get(col)
            if expected_type:
                actual_type = schema_info[col].udt_name or schema_info[col].data_type
                if _type_match(expected_type, actual_type) < 1.0:
                    LOG.warning(
                        "type mismatch detected",
                        extra={
                            "table": table_name,
                            "column": col,
                            "expected": expected_type,
                            "actual": actual_type,
                        },
                    )
            continue
        if col in alias_map and alias_map[col] in schema_info:
            continue

        name_scores = _score_name(col, available)
        expected_type = expected_types.get(col)
        expected_pk = _expected_is_pk(table_cfg, col)

        best_candidate = None
        best_score = 0.0
        for candidate_name in available:
            candidate = schema_info[candidate_name]
            name_score = name_scores.get(candidate_name, 0.0)
            score = _score_candidate(col, expected_type, expected_pk, candidate, name_score)
            if score > best_score:
                best_score = score
                best_candidate = candidate_name

        if best_candidate and best_score >= confidence_threshold:
            alias_map[col] = best_candidate
            decisions[col] = {
                "candidate": best_candidate,
                "score": round(best_score, 4),
            }
        else:
            missing.append(col)

    if missing:
        payload = {
            "table": table_name,
            "missing": missing,
            "available": available,
            "schema_contract": table_cfg.get("schema_contract", {}),
            "alias_map": alias_map,
        }
        agent_response = resolve_schema_drift(payload)
        agent_aliases = None
        if agent_response and isinstance(agent_response.get("aliases"), dict):
            agent_aliases = {str(k): str(v) for k, v in agent_response["aliases"].items()}
        if agent_aliases:
            validated_aliases = _validate_agent_aliases(
                agent_aliases,
                schema_info,
                expected_types,
                alias_map,
            )
            if validated_aliases:
                if _is_safety_critical(table_cfg):
                    _append_review_record(table_name, missing, validated_aliases)
                    LOG.warning(
                        "agent suggestion requires review",
                        extra={"table": table_name, "aliases": validated_aliases},
                    )
                else:
                    for expected, actual in validated_aliases.items():
                        if expected in missing:
                            alias_map[expected] = actual
                            decisions[expected] = {"candidate": actual, "score": "agent"}
            else:
                LOG.warning(
                    "agent suggestions failed validation",
                    extra={"table": table_name, "aliases": agent_aliases},
                )
            missing = [col for col in missing if col not in alias_map]

    if missing:
        LOG.error(
            "schema drift unresolved",
            extra={"table": table_name, "missing_columns": missing, "decisions": decisions},
        )
        raise SchemaDriftError(
            f"missing required columns for {table_name}: {', '.join(missing)}"
        )

    if decisions:
        LOG.warning("schema drift resolved", extra={"table": table_name, "decisions": decisions})

    columns_cfg = table_cfg.get("columns", [])
    select_columns: Optional[List[str]] = None
    if columns_cfg:
        select_set = list(columns_cfg)
        primary_key = table_cfg.get("primary_key")
        updated_at = table_cfg.get("updated_at")
        if primary_key and primary_key not in select_set:
            select_set.append(primary_key)
        if updated_at and updated_at not in select_set:
            select_set.append(updated_at)

        seen = set()
        mapped: List[str] = []
        for col in select_set:
            actual = alias_map.get(col, col)
            if actual not in seen:
                mapped.append(actual)
                seen.add(actual)
        select_columns = mapped

    updated_at = table_cfg.get("updated_at")
    updated_at_column = alias_map.get(updated_at, updated_at) if updated_at else None
    filters = table_cfg.get("filters", {})
    mapped_filters = {alias_map.get(k, k): v for k, v in filters.items()}

    return TablePlan(
        select_columns=select_columns,
        alias_map=alias_map,
        updated_at_column=updated_at_column,
        filters=mapped_filters,
    )


def normalize_rows(rows: List[Dict[str, Any]], alias_map: Dict[str, str]) -> None:
    if not alias_map:
        return
    for row in rows:
        for expected, actual in alias_map.items():
            if expected not in row and actual in row:
                row[expected] = row.get(actual)
