"""Live-tail business logic for monitoring real-time data ingestion."""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

import sentry_sdk
import structlog

from tsdb_tool.core.exceptions import InputError, SqlToolError

if TYPE_CHECKING:
    import typer

    from tsdb_tool.core.client import PgClient
    from tsdb_tool.core.models import QueryResult

_MAX_DISCOVERED_TABLES = 50


@dataclass
class LiveTailConfig:
    schema: str
    tables: list[str]
    interval: int = 30
    duration: int = 180
    full: bool = False
    columns: list[str] | None = None
    liveness_probe_interval: int = 20
    liveness_timeout: int = 120

    def __post_init__(self) -> None:
        if not (1 <= self.interval <= 3600):
            msg = f"interval must be between 1 and 3600, got {self.interval}"
            raise ValueError(msg)
        if self.duration < 0:
            msg = f"duration must be >= 0, got {self.duration}"
            raise ValueError(msg)


@dataclass
class TableInfo:
    schema: str
    table: str
    time_column: str

    @property
    def fqn(self) -> str:
        return f'"{self.schema}"."{self.table}"'


@dataclass
class PollResult:
    table_fqn: str
    count: int
    max_ts: str | None
    interval: int

    @property
    def rows_per_second(self) -> float:
        if self.count == 0 or self.interval == 0:
            return 0.0
        return self.count / self.interval


@dataclass
class TailSummary:
    tables_monitored: int
    total_rows: int
    elapsed_seconds: int
    per_table: dict[str, int] = field(default_factory=dict)


def discover_tables(client: PgClient, schema: str) -> list[tuple[str, str]]:
    log = structlog.get_logger()
    sql = """
    SELECT DISTINCT c.table_schema, c.table_name
    FROM information_schema.columns c
    WHERE c.table_schema = %(schema)s
      AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
      AND c.table_schema NOT LIKE '_timescaledb_%%'
      AND c.data_type IN ('timestamp without time zone', 'timestamp with time zone')
    ORDER BY c.table_schema, c.table_name
    """
    result = client.execute_query(sql, {"schema": schema})
    tables = [(str(row[0]), str(row[1])) for row in result.rows]

    if len(tables) > _MAX_DISCOVERED_TABLES:
        log.warning(
            "discovered tables exceed limit, truncating",
            found=len(tables),
            limit=_MAX_DISCOVERED_TABLES,
        )
        tables = tables[:_MAX_DISCOVERED_TABLES]

    return tables


def _get_time_column_tsdb(client: PgClient, schema: str, table: str) -> str | None:
    log = structlog.get_logger()
    sql = """
    SELECT d.column_name
    FROM timescaledb_information.dimensions d
    WHERE d.hypertable_schema = %(schema)s
      AND d.hypertable_name = %(table)s
      AND d.dimension_number = 1
    LIMIT 1
    """
    try:
        result = client.execute_query(sql, {"schema": schema, "table": table})
        if result.rows:
            return str(result.rows[0][0])
    except SqlToolError:
        log.debug("TimescaleDB dimensions not available", schema=schema, table=table)
    return None


def _get_time_column_info_schema(
    client: PgClient, schema: str, table: str
) -> str | None:
    sql = """
    SELECT c.column_name
    FROM information_schema.columns c
    WHERE c.table_schema = %(schema)s
      AND c.table_name = %(table)s
      AND c.data_type IN ('timestamp without time zone', 'timestamp with time zone')
    ORDER BY c.ordinal_position
    LIMIT 1
    """
    result = client.execute_query(sql, {"schema": schema, "table": table})
    if result.rows:
        return str(result.rows[0][0])
    return None


def _find_time_column(client: PgClient, schema: str, table: str) -> str | None:
    col = _get_time_column_tsdb(client, schema, table)
    if col is not None:
        return col
    return _get_time_column_info_schema(client, schema, table)


def _parse_table_arg(table_arg: str, default_schema: str = "public") -> tuple[str, str]:
    if "." in table_arg:
        schema, table = table_arg.split(".", 1)
        return schema, table
    return default_schema, table_arg


def _table_exists(client: PgClient, schema: str, table: str) -> bool:
    sql = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = %(schema)s
      AND table_name = %(table)s
    """
    result = client.execute_query(sql, {"schema": schema, "table": table})
    return len(result.rows) > 0


def resolve_tables(client: PgClient, config: LiveTailConfig) -> list[TableInfo]:
    log = structlog.get_logger()

    tables: list[TableInfo] = []

    if config.tables:
        for table_arg in config.tables:
            schema, table = _parse_table_arg(table_arg, default_schema=config.schema)
            if not _table_exists(client, schema, table):
                msg = f"Table '{schema}.{table}' does not exist"
                raise InputError(msg)
            time_col = _find_time_column(client, schema, table)
            if time_col is None:
                log.warning("no timestamp column found", schema=schema, table=table)
                continue
            tables.append(TableInfo(schema=schema, table=table, time_column=time_col))
    else:
        discovered = discover_tables(client, config.schema)
        for schema, table in discovered:
            time_col = _find_time_column(client, schema, table)
            if time_col is None:
                continue
            tables.append(TableInfo(schema=schema, table=table, time_column=time_col))

    return tables


def detect_liveness(
    client: PgClient,
    tables: list[TableInfo],
    probe_interval: int = 20,
    timeout: int = 120,
) -> bool:
    log = structlog.get_logger()

    def get_max_timestamps() -> dict[str, str | None]:
        result: dict[str, str | None] = {}
        for table_info in tables:
            sql = (
                f"SELECT {table_info.time_column} FROM {table_info.fqn}"
                f" ORDER BY {table_info.time_column} DESC LIMIT 5"
            )
            try:
                query_result = client.execute_query(sql)
            except SqlToolError:
                log.warning(
                    "skipping table during liveness probe",
                    table=table_info.fqn,
                )
                continue
            max_ts = query_result.rows[0][0] if query_result.rows else None
            result[table_info.fqn] = str(max_ts) if max_ts is not None else None
        return result

    baseline = get_max_timestamps()
    log.debug("liveness baseline captured", tables=len(baseline))
    start = time.monotonic()

    while True:
        elapsed_time = time.monotonic() - start
        if elapsed_time >= timeout:
            break
        remaining = int(timeout - elapsed_time)
        log.debug(
            "liveness probe sleeping",
            sleep_seconds=probe_interval,
            timeout_remaining=remaining,
        )
        with sentry_sdk.start_span(
            op="sleep", description=f"Liveness probe wait {probe_interval}s"
        ):
            time.sleep(probe_interval)
        current = get_max_timestamps()

        for fqn in baseline:
            if baseline[fqn] != current[fqn] and current[fqn] is not None:
                log.debug("liveness detected", table=fqn)
                return True

    return False


def fetch_watermarks(
    client: PgClient,
    tables: list[TableInfo],
) -> dict[str, str | None]:
    """Fetch MAX(time_column) for each table to establish baseline watermarks."""
    log = structlog.get_logger()
    watermarks: dict[str, str | None] = {}
    for table in tables:
        sql = f"SELECT MAX({table.time_column}) FROM {table.fqn}"
        try:
            result = client.execute_query(sql)
        except SqlToolError:
            log.warning("baseline query failed", table=table.fqn)
            watermarks[table.fqn] = None
            continue
        max_ts_raw = result.rows[0][0] if result.rows else None
        watermarks[table.fqn] = str(max_ts_raw) if max_ts_raw is not None else None
    return watermarks


def poll_count(
    client: PgClient,
    table: TableInfo,
    watermark: str | None,
    elapsed_seconds: int,
) -> PollResult:
    log = structlog.get_logger()
    where_clause = f"WHERE {table.time_column} > %(watermark)s" if watermark else ""
    sql = f"""
    SELECT COUNT(*), MAX({table.time_column})
    FROM {table.fqn}
    {where_clause}
    """

    try:
        result = client.execute_query(
            sql, {"watermark": watermark} if watermark else {}
        )
    except SqlToolError:
        log.warning(
            "poll query failed, skipping table",
            table=table.fqn,
        )
        return PollResult(
            table_fqn=table.fqn,
            count=0,
            max_ts=watermark,
            interval=elapsed_seconds,
        )

    count = (
        int(result.rows[0][0]) if result.rows and result.rows[0][0] is not None else 0
    )
    max_ts_raw = result.rows[0][1] if result.rows else None
    max_ts = str(max_ts_raw) if max_ts_raw is not None else None

    return PollResult(
        table_fqn=table.fqn,
        count=count,
        max_ts=max_ts,
        interval=elapsed_seconds,
    )


def validate_columns(
    client: PgClient,
    table: TableInfo,
    requested_columns: list[str],
) -> list[str]:
    """Validate requested columns against table schema.

    Args:
        client: Database client
        table: Table to validate against
        requested_columns: Columns requested by user

    Returns:
        List of valid column names (always includes timestamp column)
    """
    log = structlog.get_logger()

    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %(schema)s
      AND table_name = %(table)s
    """
    result = client.execute_query(sql, {"schema": table.schema, "table": table.table})
    actual_columns = {str(row[0]) for row in result.rows}

    valid_columns = []
    for col in requested_columns:
        if col in actual_columns:
            valid_columns.append(col)
        else:
            log.warning(
                "column not found in table, skipping",
                table=table.fqn,
                column=col,
            )

    if table.time_column not in valid_columns:
        valid_columns.insert(0, table.time_column)

    return valid_columns


def poll_rows(
    client: PgClient,
    table: TableInfo,
    watermark: str | None,
    columns: list[str] | None = None,
) -> tuple[QueryResult, str | None]:
    """Poll for new rows since watermark, return QueryResult and new max timestamp.

    Args:
        client: Database client
        table: Table to poll
        watermark: Last seen timestamp (rows > watermark are returned)
        columns: Optional list of column names to select (timestamp always included)

    Returns:
        Tuple of (QueryResult with rows, new max timestamp for watermark)
    """
    select_cols = f"{table.time_column}, " + ", ".join(columns) if columns else "*"

    where_clause = f"WHERE {table.time_column} > %(watermark)s" if watermark else ""
    sql = f"""
    SELECT {select_cols}
    FROM {table.fqn}
    {where_clause}
    ORDER BY {table.time_column} ASC
    """

    result = client.execute_query(sql, {"watermark": watermark} if watermark else {})

    if result.rows:
        ts_col_idx = next(
            (
                i
                for i, col in enumerate(result.columns)
                if col.name == table.time_column
            ),
            0,
        )
        max_ts_raw = result.rows[-1][ts_col_idx]
        new_max_ts = str(max_ts_raw) if max_ts_raw is not None else watermark
    else:
        new_max_ts = watermark

    return result, new_max_ts


def poll_tables(
    client: PgClient,
    tables: list[TableInfo],
    config: LiveTailConfig,
    initial_watermarks: dict[str, str | None],
    ctx: typer.Context | None = None,
) -> TailSummary:
    watermarks = dict(initial_watermarks)
    totals: dict[str, int] = {table.fqn: 0 for table in tables}
    start = time.monotonic()
    poll_number = 0

    validated_columns_cache: dict[str, list[str]] = {}

    try:
        while True:
            elapsed = int(time.monotonic() - start)
            if config.duration > 0 and elapsed >= config.duration:
                break

            cycle_start = time.monotonic()
            poll_number += 1

            with sentry_sdk.start_span(
                op="poll", description=f"Poll cycle {poll_number}"
            ) as span:
                for table in tables:
                    if config.full:
                        columns_to_use = config.columns
                        if config.columns and table.fqn not in validated_columns_cache:
                            validated_columns_cache[table.fqn] = validate_columns(
                                client,
                                table,
                                config.columns,
                            )
                        if config.columns:
                            columns_to_use = validated_columns_cache[table.fqn]

                        query_result, new_max_ts = poll_rows(
                            client,
                            table,
                            watermarks[table.fqn],
                            columns=columns_to_use,
                        )
                        watermarks[table.fqn] = new_max_ts or watermarks[table.fqn]
                        row_count = query_result.row_count
                        totals[table.fqn] += row_count

                        if row_count > 0:
                            sys.stderr.write(f"{table.fqn}: {row_count} rows\n")
                            sys.stderr.flush()

                            if ctx is not None:
                                from tsdb_tool.cli.commands._shared import output_result

                                output_result(ctx, query_result)
                    else:
                        result = poll_count(
                            client, table, watermarks[table.fqn], config.interval
                        )
                        watermarks[table.fqn] = result.max_ts or watermarks[table.fqn]
                        totals[table.fqn] += result.count

                        line = format_count_line(result, totals[table.fqn])
                        sys.stdout.write(line + "\n")
                        sys.stdout.flush()

                cycle_elapsed = time.monotonic() - cycle_start
                span.set_data("cycle_duration_ms", cycle_elapsed * 1000)
                span.set_data("tables_polled", len(tables))

            sleep_time = max(0, config.interval - cycle_elapsed)
            if sleep_time > 0:
                log = structlog.get_logger()
                log.debug("sleeping until next poll", sleep_seconds=f"{sleep_time:.1f}")
                with sentry_sdk.start_span(
                    op="sleep", description=f"Poll interval wait {sleep_time:.0f}s"
                ):
                    time.sleep(sleep_time)

    except KeyboardInterrupt:
        elapsed = int(time.monotonic() - start)
        return TailSummary(
            tables_monitored=len(tables),
            total_rows=sum(totals.values()),
            elapsed_seconds=elapsed,
            per_table=totals,
        )

    elapsed = int(time.monotonic() - start)
    return TailSummary(
        tables_monitored=len(tables),
        total_rows=sum(totals.values()),
        elapsed_seconds=elapsed,
        per_table=totals,
    )


def format_count_line(result: PollResult, running_total: int) -> str:
    timestamp = datetime.now().strftime("%H:%M:%S")
    return f"[{timestamp}] {result.table_fqn}: {result.rows_per_second:.1f} rows/s ({running_total} total)"
