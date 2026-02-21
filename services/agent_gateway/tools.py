"""Internal helper tools for the agent gateway."""

from __future__ import annotations

import difflib
from typing import Iterable, List, Tuple


def score_similarity(expected: str, candidate: str) -> float:
    """Return a normalized similarity score between two strings."""
    return difflib.SequenceMatcher(None, expected.lower(), candidate.lower()).ratio()


def top_name_candidates(expected: str, available: Iterable[str], k: int = 5) -> List[Tuple[str, float]]:
    """Return top-K candidate column names with similarity scores."""
    scored = [(name, score_similarity(expected, name)) for name in available]
    scored.sort(key=lambda item: item[1], reverse=True)
    return scored[:k]


def normalize_type(type_name: str | None) -> str:
    if not type_name:
        return ""
    return type_name.lower().strip()


def type_compatible(expected_type: str | None, candidate_type: str | None) -> bool:
    """Loose type compatibility check used for hints in prompts."""
    exp = normalize_type(expected_type)
    cand = normalize_type(candidate_type)
    if not exp or not cand:
        return True
    if exp == cand:
        return True
    synonyms = {
        "uuid": {"uuid", "uniqueidentifier"},
        "text": {"text", "varchar", "character varying", "string"},
        "int": {"int", "integer", "int4", "bigint", "int8"},
        "float": {"float", "float8", "double", "numeric", "decimal"},
        "bool": {"bool", "boolean"},
        "timestamp": {"timestamptz", "timestamp", "datetime"},
    }
    for key, values in synonyms.items():
        if exp in values and cand in values:
            return True
    return False


def classify_error_basic(error: str) -> str:
    """Basic error classification fallback."""
    message = error.lower()
    if "validation" in message or "payload" in message or "missing" in message or "null" in message:
        return "poison"
    if "timeout" in message or "connection" in message or "network" in message:
        return "retryable"
    if "unauthorized" in message or "auth" in message:
        return "needs_review"
    return "retryable"


def compute_drift_ratio(source_count: int, target_count: int) -> float:
    if source_count <= 0:
        return 0.0
    return abs(source_count - target_count) / float(source_count)
