"""PostgreSQL client for SQL Tool.

Wraps psycopg v3 synchronous connections with query execution,
statement timeout, and exception mapping to SqlToolError hierarchy.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import psycopg
import psycopg.errors
import sentry_sdk
import structlog

from tsdb_tool.core.exceptions import NetworkError, SqlToolError, TimeoutError
from tsdb_tool.core.models import ColumnMeta, QueryResult

if TYPE_CHECKING:
    from tsdb_tool.core.config import ResolvedConfig

# Mapping from psycopg type OIDs to human-readable names.
# Covers the most common PostgreSQL types; unknown OIDs fall back to "unknown".
_TYPE_NAMES: dict[int, str] = {
    16: "bool",
    20: "int8",
    21: "int2",
    23: "int4",
    25: "text",
    26: "oid",
    114: "json",
    142: "xml",
    700: "float4",
    701: "float8",
    790: "money",
    1042: "bpchar",
    1043: "varchar",
    1082: "date",
    1083: "time",
    1114: "timestamp",
    1184: "timestamptz",
    1186: "interval",
    1700: "numeric",
    2950: "uuid",
    3802: "jsonb",
}


class PgClient:
    """Synchronous PostgreSQL client using psycopg v3."""

    def __init__(self, config: ResolvedConfig) -> None:
        self.config = config
        self._connection: psycopg.Connection[Any] | None = None

    def __enter__(self) -> PgClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _connect(self) -> psycopg.Connection[Any]:
        if self._connection is not None and not self._connection.closed:
            return self._connection

        try:
            self._connection = psycopg.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.dbname,
                user=self.config.user,
                password=self.config.password,
                sslmode=self.config.sslmode,
                connect_timeout=self.config.connect_timeout,
                application_name=self.config.application_name,
                autocommit=True,
            )
        except psycopg.OperationalError as e:
            msg = (
                f"Connection failed to {self.config.host}:{self.config.port} "
                f"database '{self.config.dbname}': {e}"
            )
            raise NetworkError(msg) from e

        return self._connection

    def execute_query(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> QueryResult:
        """Execute SQL and return a QueryResult."""
        log = structlog.get_logger()
        conn = self._connect()
        timeout_ms = int(self.config.default_timeout * 1000)

        sql_normalized = " ".join(sql.split())
        span_description = sql_normalized[:100]
        log.debug("executing query", sql=sql_normalized)
        with sentry_sdk.start_span(op="db.query", description=span_description) as span:
            start_time = time.monotonic()
            try:
                with conn.cursor() as cur:
                    cur.execute(f"SET statement_timeout = {timeout_ms}")
                    cur.execute(sql, params)

                    columns: list[ColumnMeta] = []
                    rows: list[tuple[Any, ...]] = []

                    if cur.description:
                        for desc in cur.description:
                            columns.append(
                                ColumnMeta(
                                    name=desc.name,
                                    type_oid=desc.type_code,
                                    type_name=_TYPE_NAMES.get(
                                        desc.type_code, "unknown"
                                    ),
                                )
                            )
                        rows = cur.fetchall()

                    duration_ms = (time.monotonic() - start_time) * 1000
                    span.set_data("row_count", len(rows))
                    span.set_data("duration_ms", duration_ms)
                    log.debug(
                        "query complete",
                        duration_ms=f"{duration_ms:.1f}",
                        row_count=len(rows),
                    )

                    return QueryResult(
                        columns=columns,
                        rows=rows,
                        row_count=len(rows),
                        status_message=cur.statusmessage or "",
                    )

            except psycopg.errors.QueryCanceled as e:
                duration_ms = (time.monotonic() - start_time) * 1000
                span.set_data("duration_ms", duration_ms)
                span.set_status("deadline_exceeded")
                log.error(
                    "query timeout",
                    sql=sql_normalized,
                    duration_ms=f"{duration_ms:.1f}",
                )
                msg = f"Query timed out after {self.config.default_timeout}s: {e}"
                raise TimeoutError(msg) from e
            except psycopg.errors.SyntaxError as e:
                span.set_status("invalid_argument")
                log.error("query syntax error", sql=sql_normalized, error=str(e))
                raise SqlToolError(f"SQL error: {e}") from e
            except psycopg.OperationalError as e:
                span.set_status("unavailable")
                log.error("database error", sql=sql_normalized, error=str(e))
                raise NetworkError(f"Database error: {e}") from e

    def close(self) -> None:
        """Close the database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
