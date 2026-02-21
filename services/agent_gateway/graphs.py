"""LangGraph workflows for the agent gateway."""

from __future__ import annotations

import json
from typing import Any, Dict, List, TypedDict

import litellm
from langgraph.graph import END, StateGraph

from services.agent_gateway.tools import (
    classify_error_basic,
    compute_drift_ratio,
    top_name_candidates,
    type_compatible,
)


class SchemaDriftState(TypedDict):
    payload: Dict[str, Any]
    candidate_hints: Dict[str, List[Dict[str, Any]]]
    llm_output: Dict[str, Any]
    result: Dict[str, Any]


class FailureTriageState(TypedDict):
    payload: Dict[str, Any]
    llm_output: Dict[str, Any]
    result: Dict[str, Any]


class ReconciliationState(TypedDict):
    payload: Dict[str, Any]
    needs_action: bool
    llm_output: Dict[str, Any]
    result: Dict[str, Any]


class EmbeddingConfigState(TypedDict):
    payload: Dict[str, Any]
    label_hints: Dict[str, List[Dict[str, Any]]]
    relationship_hints: Dict[str, List[Dict[str, Any]]]
    llm_output: Dict[str, Any]
    result: Dict[str, Any]


def _model_name() -> str:
    import os

    return os.getenv("AGENT_LLM_MODEL", "gpt-4.1-mini")


def _call_llm_json(system_prompt: str, user_prompt: str, *, model: str) -> Dict[str, Any]:
    try:
        response = litellm.completion(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
        )
    except Exception:
        return {}

    content = response["choices"][0]["message"]["content"]
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return {}


# Schema drift workflow

def _schema_candidates(state: SchemaDriftState) -> SchemaDriftState:
    payload = state["payload"]
    missing = payload.get("missing", [])
    available = payload.get("available", [])
    schema_contract = payload.get("schema_contract", {})
    type_map = schema_contract.get("types", schema_contract) if isinstance(schema_contract, dict) else {}

    hints: Dict[str, List[Dict[str, Any]]] = {}
    for col in missing:
        candidates = top_name_candidates(col, available, k=5)
        hints[col] = []
        for name, score in candidates:
            hints[col].append(
                {
                    "name": name,
                    "score": round(score, 4),
                    "type_match": type_compatible(type_map.get(col), None),
                }
            )

    return {"candidate_hints": hints}


def _schema_llm(state: SchemaDriftState) -> SchemaDriftState:
    payload = state["payload"]
    hints = state.get("candidate_hints", {})
    model = payload.get("model") or _model_name()

    system_prompt = (
        "You are a schema drift resolver. "
        "Return JSON only with an 'aliases' object mapping expected columns to actual columns. "
        "Use only columns from the 'available' list. "
        "If no safe mapping exists, return {\"aliases\": {}}."
    )

    user_prompt = json.dumps(
        {
            "table": payload.get("table"),
            "missing": payload.get("missing"),
            "available": payload.get("available"),
            "schema_contract": payload.get("schema_contract"),
            "alias_map": payload.get("alias_map"),
            "candidate_hints": hints,
        },
        indent=2,
    )

    output = _call_llm_json(system_prompt, user_prompt, model=model)
    return {"llm_output": output}


def _schema_validate(state: SchemaDriftState) -> SchemaDriftState:
    output = state.get("llm_output", {})
    aliases = output.get("aliases")
    if not isinstance(aliases, dict):
        aliases = {}
    return {
        "result": {
            "aliases": {str(k): str(v) for k, v in aliases.items()},
            "confidence": float(output.get("confidence", 0.0) or 0.0),
            "reason": str(output.get("reason", "")),
        }
    }


def build_schema_drift_graph():
    graph = StateGraph(SchemaDriftState)
    graph.add_node("candidates", _schema_candidates)
    graph.add_node("llm", _schema_llm)
    graph.add_node("validate", _schema_validate)
    graph.set_entry_point("candidates")
    graph.add_edge("candidates", "llm")
    graph.add_edge("llm", "validate")
    graph.add_edge("validate", END)
    return graph.compile()


# Failure triage workflow

def _triage_llm(state: FailureTriageState) -> FailureTriageState:
    payload = state["payload"]
    model = payload.get("model") or _model_name()

    system_prompt = (
        "You are a failure triage agent. "
        "Classify errors into retryable, poison, or needs_review. "
        "Return JSON only with classification, error_code, retry_in_seconds."
    )
    user_prompt = json.dumps(payload, indent=2)

    output = _call_llm_json(system_prompt, user_prompt, model=model)
    return {"llm_output": output}


def _triage_validate(state: FailureTriageState) -> FailureTriageState:
    output = state.get("llm_output", {})
    classification = output.get("classification")
    if classification not in {"retryable", "poison", "needs_review"}:
        classification = classify_error_basic(str(state.get("payload", {}).get("error", "")))
    retry_in = output.get("retry_in_seconds")
    try:
        retry_in_seconds = int(retry_in)
    except (TypeError, ValueError):
        retry_in_seconds = 0
    return {
        "result": {
            "classification": classification,
            "error_code": str(output.get("error_code", "")),
            "retry_in_seconds": max(retry_in_seconds, 0),
        }
    }


def build_failure_triage_graph():
    graph = StateGraph(FailureTriageState)
    graph.add_node("llm", _triage_llm)
    graph.add_node("validate", _triage_validate)
    graph.set_entry_point("llm")
    graph.add_edge("llm", "validate")
    graph.add_edge("validate", END)
    return graph.compile()


# Reconciliation workflow

def _reconcile_check(state: ReconciliationState) -> ReconciliationState:
    payload = state["payload"]
    ratio = compute_drift_ratio(int(payload.get("source_count", 0)), int(payload.get("target_count", 0)))
    threshold = float(payload.get("drift_threshold", 0.005))
    needs_action = ratio >= threshold
    return {"needs_action": needs_action}


def _reconcile_llm(state: ReconciliationState) -> ReconciliationState:
    if not state.get("needs_action"):
        return {"llm_output": {"action": "observe"}}

    payload = state["payload"]
    model = payload.get("model") or _model_name()

    system_prompt = (
        "You are a reconciliation agent. "
        "Propose a safe backfill window when drift is detected. "
        "Return JSON only with action, from, to, reason."
    )
    user_prompt = json.dumps(payload, indent=2)

    output = _call_llm_json(system_prompt, user_prompt, model=model)
    return {"llm_output": output}


def _reconcile_validate(state: ReconciliationState) -> ReconciliationState:
    output = state.get("llm_output", {})
    action = output.get("action") or ("observe" if not state.get("needs_action") else "backfill")
    return {
        "result": {
            "action": action,
            "from": output.get("from"),
            "to": output.get("to"),
            "reason": str(output.get("reason", "")),
        }
    }


def build_reconciliation_graph():
    graph = StateGraph(ReconciliationState)
    graph.add_node("check", _reconcile_check)
    graph.add_node("llm", _reconcile_llm)
    graph.add_node("validate", _reconcile_validate)
    graph.set_entry_point("check")
    graph.add_edge("check", "llm")
    graph.add_edge("llm", "validate")
    graph.add_edge("validate", END)
    return graph.compile()


# Embedding config resolver workflow

def _embed_config_candidates(state: EmbeddingConfigState) -> EmbeddingConfigState:
    payload = state["payload"]
    missing_labels = payload.get("missing_labels", [])
    available_labels = payload.get("available_labels", [])
    missing_relationships = payload.get("missing_relationship_types", [])
    available_relationships = payload.get("available_relationship_types", [])

    label_hints: Dict[str, List[Dict[str, Any]]] = {}
    for item in missing_labels:
        candidates = top_name_candidates(item, available_labels, k=5)
        label_hints[item] = [{"name": name, "score": round(score, 4)} for name, score in candidates]

    rel_hints: Dict[str, List[Dict[str, Any]]] = {}
    for item in missing_relationships:
        candidates = top_name_candidates(item, available_relationships, k=5)
        rel_hints[item] = [{"name": name, "score": round(score, 4)} for name, score in candidates]

    return {"label_hints": label_hints, "relationship_hints": rel_hints}


def _embed_config_llm(state: EmbeddingConfigState) -> EmbeddingConfigState:
    payload = state["payload"]
    model = payload.get("model") or _model_name()

    system_prompt = (
        "You are an embedding config resolver. "
        "Return JSON only with 'label_aliases' and 'relationship_aliases' objects. "
        "Use only values from the available lists. "
        "If no safe mapping exists, return empty objects."
    )
    user_prompt = json.dumps(
        {
            "expected_labels": payload.get("expected_labels"),
            "missing_labels": payload.get("missing_labels"),
            "available_labels": payload.get("available_labels"),
            "label_hints": state.get("label_hints", {}),
            "expected_relationship_types": payload.get("expected_relationship_types"),
            "missing_relationship_types": payload.get("missing_relationship_types"),
            "available_relationship_types": payload.get("available_relationship_types"),
            "relationship_hints": state.get("relationship_hints", {}),
        },
        indent=2,
    )

    output = _call_llm_json(system_prompt, user_prompt, model=model)
    return {"llm_output": output}


def _embed_config_validate(state: EmbeddingConfigState) -> EmbeddingConfigState:
    output = state.get("llm_output", {})
    label_aliases = output.get("label_aliases")
    rel_aliases = output.get("relationship_aliases")
    if not isinstance(label_aliases, dict):
        label_aliases = {}
    if not isinstance(rel_aliases, dict):
        rel_aliases = {}
    return {
        "result": {
            "label_aliases": {str(k): str(v) for k, v in label_aliases.items()},
            "relationship_aliases": {str(k): str(v) for k, v in rel_aliases.items()},
            "reason": str(output.get("reason", "")),
        }
    }


def build_embedding_config_graph():
    graph = StateGraph(EmbeddingConfigState)
    graph.add_node("candidates", _embed_config_candidates)
    graph.add_node("llm", _embed_config_llm)
    graph.add_node("validate", _embed_config_validate)
    graph.set_entry_point("candidates")
    graph.add_edge("candidates", "llm")
    graph.add_edge("llm", "validate")
    graph.add_edge("validate", END)
    return graph.compile()
