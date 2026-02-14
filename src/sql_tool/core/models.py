"""Query result models for SQL Tool.

Pydantic models for representing query results and column metadata
returned by PgClient.execute_query().
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class ColumnMeta(BaseModel):
    """Metadata for a single result column."""

    name: str
    type_oid: int
    type_name: str


class QueryResult(BaseModel):
    """Result of a SQL query execution."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    columns: list[ColumnMeta]
    rows: list[tuple[Any, ...]]
    row_count: int
    status_message: str
