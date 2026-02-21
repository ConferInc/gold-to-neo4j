"""Agent gateway service with LangGraph workflows."""

from __future__ import annotations

import os
from typing import Any, Dict, List, Literal, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from services.agent_gateway.graphs import (
    build_failure_triage_graph,
    build_embedding_config_graph,
    build_reconciliation_graph,
    build_schema_drift_graph,
)

app = FastAPI(title="Agent Gateway", version="0.1.0")


class SchemaDriftRequest(BaseModel):
    task: Literal["schema_drift_resolver"]
    table: str
    missing: List[str]
    available: List[str]
    schema_contract: Dict[str, Any] = Field(default_factory=dict)
    alias_map: Dict[str, str] = Field(default_factory=dict)
    model: Optional[str] = None


class SchemaDriftResponse(BaseModel):
    aliases: Dict[str, str]
    confidence: float = 0.0
    reason: str = ""


class FailureTriageRequest(BaseModel):
    task: Literal["failure_triage"]
    event_id: str
    event_type: str
    error: str
    retry_count: int = 0
    payload: Dict[str, Any] = Field(default_factory=dict)
    model: Optional[str] = None


class FailureTriageResponse(BaseModel):
    classification: Literal["retryable", "poison", "needs_review"]
    error_code: str = ""
    retry_in_seconds: int = 0


class ReconciliationRequest(BaseModel):
    task: Literal["reconciliation_backfill"]
    entity: str
    source_count: int
    target_count: int
    last_checkpoint: Optional[str] = None
    sampling_window: Optional[str] = None
    checksums: Dict[str, Any] = Field(default_factory=dict)
    drift_threshold: float = 0.005
    model: Optional[str] = None


class ReconciliationResponse(BaseModel):
    action: Literal["backfill", "observe"]
    from_: Optional[str] = Field(default=None, alias="from")
    to: Optional[str] = None
    reason: str = ""


class EmbeddingConfigRequest(BaseModel):
    task: Literal["embedding_config_resolver"]
    expected_labels: List[str]
    available_labels: List[str]
    missing_labels: List[str] = Field(default_factory=list)
    expected_relationship_types: List[str]
    available_relationship_types: List[str]
    missing_relationship_types: List[str] = Field(default_factory=list)
    model: Optional[str] = None


class EmbeddingConfigResponse(BaseModel):
    label_aliases: Dict[str, str] = Field(default_factory=dict)
    relationship_aliases: Dict[str, str] = Field(default_factory=dict)
    reason: str = ""


schema_drift_graph = build_schema_drift_graph()
failure_triage_graph = build_failure_triage_graph()
reconciliation_graph = build_reconciliation_graph()
embedding_config_graph = build_embedding_config_graph()


@app.post("/agent")
async def run_agent(payload: Dict[str, Any]) -> Dict[str, Any]:
    task = payload.get("task")
    if task == "schema_drift_resolver":
        request = SchemaDriftRequest.model_validate(payload)
        result = schema_drift_graph.invoke({"payload": request.model_dump()})
        response = SchemaDriftResponse.model_validate(result.get("result", {}))
        return response.model_dump()

    if task == "failure_triage":
        request = FailureTriageRequest.model_validate(payload)
        result = failure_triage_graph.invoke({"payload": request.model_dump()})
        response = FailureTriageResponse.model_validate(result.get("result", {}))
        return response.model_dump()

    if task == "reconciliation_backfill":
        request = ReconciliationRequest.model_validate(payload)
        result = reconciliation_graph.invoke({"payload": request.model_dump()})
        response = ReconciliationResponse.model_validate(result.get("result", {}))
        return response.model_dump(by_alias=True)

    if task == "embedding_config_resolver":
        request = EmbeddingConfigRequest.model_validate(payload)
        result = embedding_config_graph.invoke({"payload": request.model_dump()})
        response = EmbeddingConfigResponse.model_validate(result.get("result", {}))
        return response.model_dump()

    raise HTTPException(status_code=400, detail="unknown task")


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("AGENT_PORT", "8000"))
    uvicorn.run("services.agent_gateway.service:app", host="0.0.0.0", port=port)
