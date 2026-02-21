"""Agent gateway client helpers."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import httpx

from shared.logging import get_logger

LOG = get_logger(__name__)


def _base_url() -> str:
    return os.getenv("AGENT_BASE_URL", "").rstrip("/")


def _timeout() -> float:
    return float(os.getenv("AGENT_TIMEOUT_SECONDS", "5"))


def call_agent(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    base_url = _base_url()
    if not base_url:
        return None
    try:
        resp = httpx.post(
            f"{base_url}/agent",
            json=payload,
            timeout=_timeout(),
        )
    except Exception:
        LOG.exception("agent gateway request failed")
        return None

    if resp.status_code != 200:
        LOG.error("agent gateway error", extra={"status_code": resp.status_code, "body": resp.text})
        return None

    try:
        return resp.json()
    except ValueError:
        LOG.error("agent gateway returned non-json response")
        return None


def resolve_schema_drift(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    payload = dict(payload)
    payload["task"] = "schema_drift_resolver"
    return call_agent(payload)


def triage_failure(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    payload = dict(payload)
    payload["task"] = "failure_triage"
    return call_agent(payload)


def propose_reconciliation(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    payload = dict(payload)
    payload["task"] = "reconciliation_backfill"
    return call_agent(payload)


def resolve_embedding_config(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    payload = dict(payload)
    payload["task"] = "embedding_config_resolver"
    return call_agent(payload)
