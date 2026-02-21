"""Shared models and type hints."""

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class OutboxEvent:
    id: str
    event_type: str
    table_name: str
    row_id: str
    payload: Dict[str, Any]
    status: str
    created_at: str
