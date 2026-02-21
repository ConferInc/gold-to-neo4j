"""Neo4j client wrapper."""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, Optional

from neo4j import GraphDatabase
from neo4j.exceptions import AuthError

from shared.logging import get_logger

LOG = get_logger(__name__)


def is_auth_error(exc: Exception) -> bool:
    """Return True when Neo4j failure is authentication/authorization related."""
    if isinstance(exc, AuthError):
        return True

    error_code = str(getattr(exc, "code", ""))
    if "Security.Unauthorized" in error_code:
        return True

    message = str(exc).lower()
    return "unauthorized" in message or "authentication" in message


def is_non_retryable_write_error(exc: Exception) -> bool:
    """Return True when Neo4j write failure is deterministic and data-related."""
    error_code = str(getattr(exc, "code", ""))
    if error_code.startswith("Neo.ClientError.Schema."):
        return True
    if error_code in {
        "Neo.ClientError.Statement.ConstraintVerificationFailed",
        "Neo.ClientError.Statement.PropertyNotFound",
        "Neo.ClientError.Statement.TypeError",
        "Neo.ClientError.Statement.SemanticError",
    }:
        return True

    message = str(exc).lower()
    if "constraint" in message:
        return True
    if "null" in message and ("merge" in message or "id" in message):
        return True
    return False


class Neo4jClient:
    def __init__(self, uri: str, user: str, password: str, database: str) -> None:
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    @classmethod
    def from_env(cls) -> "Neo4jClient":
        uri = os.getenv("NEO4J_URI", "")
        user = os.getenv("NEO4J_USER", "")
        password = os.getenv("NEO4J_PASSWORD", "")
        database = os.getenv("NEO4J_DATABASE", "neo4j")
        if not uri or not user or not password:
            LOG.warning("missing NEO4J_URI or NEO4J_USER or NEO4J_PASSWORD")
        return cls(uri, user, password, database)

    def close(self) -> None:
        self._driver.close()

    def verify_auth(self) -> None:
        """Verify Neo4j connectivity and authentication before ingestion starts."""
        self._driver.verify_connectivity()
        with self._driver.session(database=self.database) as session:
            session.run("RETURN 1").consume()

    def execute(self, cypher: str, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Execute a cypher statement."""
        with self._driver.session(database=self.database) as session:
            session.run(cypher, parameters or {})

    def query(self, cypher: str, parameters: Optional[Dict[str, Any]] = None) -> list[Dict[str, Any]]:
        """Execute a cypher statement and return rows as dicts."""
        with self._driver.session(database=self.database) as session:
            result = session.run(cypher, parameters or {})
            return [record.data() for record in result]

    def execute_many(self, cypher: str, rows: Iterable[Dict[str, Any]]) -> None:
        """Execute a cypher statement using an UNWIND batch."""
        with self._driver.session(database=self.database) as session:
            session.run(cypher, {"rows": list(rows)})

    def count_nodes(self, label: str) -> int:
        """Count nodes for a given label."""
        cypher = f"MATCH (n:{label}) RETURN count(n) AS count"
        with self._driver.session(database=self.database) as session:
            result = session.run(cypher).single()
            if result and "count" in result:
                return int(result["count"])
        return 0

    def count_relationships(self, rel_type: str) -> int:
        """Count relationships for a given type."""
        cypher = f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS count"
        with self._driver.session(database=self.database) as session:
            result = session.run(cypher).single()
            if result and "count" in result:
                return int(result["count"])
        return 0

    def fetch_sample_ids(self, label: str, key: str, limit: int = 200) -> list[str]:
        """Fetch a sample of node ids for checksum comparisons."""
        cypher = f"""
        MATCH (n:{label})
        WHERE n.{key} IS NOT NULL
        RETURN n.{key} AS value
        ORDER BY value
        LIMIT $limit
        """
        with self._driver.session(database=self.database) as session:
            result = session.run(cypher, {"limit": limit})
            return [str(record["value"]) for record in result if record.get("value") is not None]
