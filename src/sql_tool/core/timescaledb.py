"""TimescaleDB administration operations.

Framework-agnostic business logic for TimescaleDB commands.
CLI layer in cli/commands/ts.py provides the typer interface.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sql_tool.core.exceptions import SqlToolError

if TYPE_CHECKING:
    from sql_tool.core.client import PgClient
    from sql_tool.core.models import QueryResult


def check_timescaledb_available(client: PgClient) -> None:
    """Raise SqlToolError if TimescaleDB extension is not installed."""
    sql = """
    SELECT EXISTS(
        SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
    ) AS timescaledb_installed
    """
    result = client.execute_query(sql)
    if not result.rows or not result.rows[0][0]:
        raise SqlToolError(
            "TimescaleDB extension is not installed in this database. "
            "Install it with: CREATE EXTENSION IF NOT EXISTS timescaledb;"
        )


# ---------------------------------------------------------------------------
# Read-only queries
# ---------------------------------------------------------------------------


def list_hypertables(
    client: PgClient,
    *,
    schema_filter: str | None = None,
) -> QueryResult:
    if schema_filter:
        where_clause = "WHERE h.hypertable_schema = %(schema)s"
        params: dict[str, Any] | None = {"schema": schema_filter}
    else:
        where_clause = ""
        params = None

    sql = f"""
    SELECT
        h.hypertable_schema,
        h.hypertable_name,
        d.column_name,
        d.time_interval::text,
        hypertable_size((quote_ident(h.hypertable_schema) || '.' || quote_ident(h.hypertable_name))::regclass) AS size_bytes,
        (SELECT COUNT(*) FILTER (WHERE NOT c.is_compressed) FROM timescaledb_information.chunks c
         WHERE c.hypertable_schema = h.hypertable_schema AND c.hypertable_name = h.hypertable_name) AS uncompr_chunks,
        (SELECT COUNT(*) FILTER (WHERE c.is_compressed) FROM timescaledb_information.chunks c
         WHERE c.hypertable_schema = h.hypertable_schema AND c.hypertable_name = h.hypertable_name) AS compr_chunks,
        cs.before_compression_total_bytes,
        cs.after_compression_total_bytes,
        h.compression_enabled
    FROM timescaledb_information.hypertables h
    LEFT JOIN timescaledb_information.dimensions d
      ON h.hypertable_schema = d.hypertable_schema
      AND h.hypertable_name = d.hypertable_name
      AND d.dimension_number = 1
    LEFT JOIN LATERAL (
        SELECT
            SUM(before_compression_total_bytes)::bigint AS before_compression_total_bytes,
            SUM(after_compression_total_bytes)::bigint AS after_compression_total_bytes
        FROM hypertable_compression_stats(
            (quote_ident(h.hypertable_schema) || '.' || quote_ident(h.hypertable_name))::regclass
        )
    ) cs ON true
    {where_clause}
    ORDER BY h.hypertable_schema, h.hypertable_name
    """
    return client.execute_query(sql, params)


def list_chunks(client: PgClient, schema: str, table: str) -> QueryResult:
    sql = """
    SELECT
        c.chunk_name,
        c.range_start::text AS range_start,
        c.range_end::text AS range_end,
        c.is_compressed,
        pg_total_relation_size(('_timescaledb_internal.' || quote_ident(c.chunk_name))::regclass) AS chunk_size_bytes
    FROM timescaledb_information.chunks c
    WHERE c.hypertable_schema = %(schema)s
      AND c.hypertable_name = %(table)s
    ORDER BY c.range_start
    """
    return client.execute_query(sql, {"schema": schema, "table": table})


def compression_stats(
    client: PgClient,
    *,
    schema: str | None = None,
    table: str | None = None,
) -> QueryResult:
    """Query before/after compression byte counts per hypertable."""
    if schema and table:
        where_clause = """
        WHERE h.hypertable_schema = %(schema)s
          AND h.hypertable_name = %(table)s
        """
        params: dict[str, Any] = {"schema": schema, "table": table}
    else:
        where_clause = ""
        params = {}

    sql = f"""
    SELECT
        h.hypertable_schema,
        h.hypertable_name,
        (SELECT COUNT(*) FROM timescaledb_information.chunks c
         WHERE c.hypertable_schema = h.hypertable_schema
           AND c.hypertable_name = h.hypertable_name) AS total_chunks,
        (SELECT COUNT(*) FROM timescaledb_information.chunks c
         WHERE c.hypertable_schema = h.hypertable_schema
           AND c.hypertable_name = h.hypertable_name
           AND c.is_compressed) AS compr_chunks,
        d.before_compression_total_bytes,
        d.after_compression_total_bytes
    FROM timescaledb_information.hypertables h
    LEFT JOIN LATERAL (
        SELECT
            SUM(before_compression_total_bytes)::bigint AS before_compression_total_bytes,
            SUM(after_compression_total_bytes)::bigint AS after_compression_total_bytes
        FROM hypertable_compression_stats(
            (quote_ident(h.hypertable_schema) || '.' || quote_ident(h.hypertable_name))::regclass
        )
    ) d ON true
    {where_clause}
    ORDER BY h.hypertable_schema, h.hypertable_name
    """
    return client.execute_query(sql, params if params else None)


def list_continuous_aggregates(client: PgClient) -> QueryResult:
    sql = """
    SELECT
        ca.view_schema,
        ca.view_name,
        format('%I.%I', ca.hypertable_schema, ca.hypertable_name) AS source_hypertable,
        ca.materialized_only,
        ca.compression_enabled,
        format('%I.%I', ca.materialization_hypertable_schema, ca.materialization_hypertable_name) AS materialization_hypertable,
        ca.finalized
    FROM timescaledb_information.continuous_aggregates ca
    ORDER BY ca.view_schema, ca.view_name
    """
    return client.execute_query(sql)


def list_retention_policies(client: PgClient) -> QueryResult:
    sql = """
    SELECT
        j.hypertable_schema,
        j.hypertable_name,
        j.config->>'drop_after' AS drop_after,
        j.schedule_interval::text AS schedule_interval,
        js.last_run_started_at::text AS last_run_started_at,
        js.next_start::text AS next_start
    FROM timescaledb_information.jobs j
    LEFT JOIN timescaledb_information.job_stats js
      ON j.job_id = js.job_id
    WHERE j.proc_name = 'policy_retention'
    ORDER BY j.hypertable_schema, j.hypertable_name
    """
    return client.execute_query(sql)


def list_refresh_status(client: PgClient) -> QueryResult:
    sql = """
    SELECT
        j.hypertable_schema,
        j.hypertable_name,
        j.schedule_interval::text AS schedule_interval,
        js.last_run_started_at::text AS last_run_started_at,
        js.next_start::text AS next_start,
        js.last_run_status,
        js.total_runs,
        js.total_successes,
        js.total_failures
    FROM timescaledb_information.jobs j
    LEFT JOIN timescaledb_information.job_stats js
      ON j.job_id = js.job_id
    WHERE j.proc_name = 'policy_refresh_continuous_aggregate'
    ORDER BY j.hypertable_schema, j.hypertable_name
    """
    return client.execute_query(sql)


def list_jobs(client: PgClient) -> QueryResult:
    sql = """
    SELECT
        j.job_id,
        j.application_name,
        CASE WHEN j.hypertable_schema IS NOT NULL
             THEN format('%I.%I', j.hypertable_schema, j.hypertable_name)
             ELSE NULL END AS hypertable,
        j.schedule_interval::text AS schedule,
        js.last_run_started_at::text AS last_run,
        CASE WHEN js.last_run_started_at IS NOT NULL
             AND js.last_run_started_at > '-infinity'::timestamptz
             AND js.last_run_started_at < 'infinity'::timestamptz
             THEN EXTRACT(EPOCH FROM (now() - js.last_run_started_at))
             ELSE NULL END AS last_run_seconds,
        js.last_run_status,
        js.next_start::text AS next_start,
        CASE WHEN js.next_start IS NOT NULL
             AND js.next_start > '-infinity'::timestamptz
             AND js.next_start < 'infinity'::timestamptz
             THEN EXTRACT(EPOCH FROM (js.next_start - now()))
             ELSE NULL END AS next_start_seconds,
        js.total_runs,
        js.total_successes,
        js.total_failures
    FROM timescaledb_information.jobs j
    LEFT JOIN timescaledb_information.job_stats js
      ON j.job_id = js.job_id
    ORDER BY j.job_id
    """
    return client.execute_query(sql)


_JOB_HISTORY_TABLES = [
    "_timescaledb_internal.bgw_job_stat_history",
    "_timescaledb_internal.bgw_job_stat",
]


def list_job_history(
    client: PgClient,
    *,
    job_id: int | None = None,
) -> QueryResult:
    """Try bgw_job_stat_history first, fall back to bgw_job_stat."""
    where_clause = "WHERE job_id = %(job_id)s" if job_id else ""
    params: dict[str, Any] = {"job_id": job_id} if job_id else {}

    for table_name in _JOB_HISTORY_TABLES:
        try:
            sql = f"""
            SELECT
                job_id,
                succeeded,
                execution_start::text,
                execution_finish::text,
                data::text AS error_data
            FROM {table_name}
            {where_clause}
            ORDER BY execution_start DESC
            LIMIT 20
            """
            return client.execute_query(sql, params if params else None)
        except Exception:  # noqa: BLE001 - table may not exist in this TS version
            continue

    raise SqlToolError("No job history table found")


def list_compression_settings(
    client: PgClient,
    *,
    schema: str | None = None,
    table: str | None = None,
    effective_schema: str | None = None,
    include_policy: bool = True,
) -> QueryResult:
    """Query segment_by/order_by settings, optionally joined with policy schedule."""
    conditions: list[str] = []
    params: dict[str, Any] = {}

    if schema and table:
        conditions.append("cs.hypertable_schema = %(schema)s")
        conditions.append("cs.hypertable_name = %(table)s")
        params = {"schema": schema, "table": table}
    elif effective_schema:
        conditions.append("cs.hypertable_schema = %(schema)s")
        params = {"schema": effective_schema}

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    policy_join = ""
    policy_select = ""
    if include_policy:
        policy_join = """
        LEFT JOIN (
            SELECT
                j.hypertable_schema AS p_schema,
                j.hypertable_name AS p_table,
                j.config->>'compress_after' AS compress_after,
                j.schedule_interval::text AS sched,
                js.last_run_started_at::text AS last_run,
                js.next_start::text AS next_start,
                js.last_run_status AS status
            FROM timescaledb_information.jobs j
            LEFT JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id
            WHERE j.proc_name = 'policy_compression'
        ) pol ON pol.p_schema = cs.hypertable_schema AND pol.p_table = cs.hypertable_name
        """
        policy_select = """,
        pol.compress_after,
        pol.sched,
        pol.last_run,
        pol.next_start,
        pol.status"""

    sql = f"""
    SELECT
        cs.hypertable_schema,
        cs.hypertable_name,
        d.column_name AS time_col,
        d.time_interval::text AS chunk_iv,
        STRING_AGG(
            CASE WHEN cs.segmentby_column_index IS NOT NULL THEN cs.attname END,
            ', ' ORDER BY cs.segmentby_column_index
        ) AS segment_by,
        STRING_AGG(
            CASE WHEN cs.orderby_column_index IS NOT NULL THEN
                cs.attname ||
                CASE WHEN cs.orderby_asc THEN '' ELSE ' DESC' END ||
                CASE WHEN cs.orderby_nullsfirst THEN ' NULLS FIRST' ELSE '' END
            END,
            ', ' ORDER BY cs.orderby_column_index
        ) AS order_by{policy_select}
    FROM timescaledb_information.compression_settings cs
    LEFT JOIN timescaledb_information.dimensions d
        ON cs.hypertable_schema = d.hypertable_schema
        AND cs.hypertable_name = d.hypertable_name
        AND d.dimension_number = 1
    {policy_join}
    {where_clause}
    GROUP BY cs.hypertable_schema, cs.hypertable_name, d.column_name, d.time_interval
        {", pol.compress_after, pol.sched, pol.last_run, pol.next_start, pol.status" if include_policy else ""}
    ORDER BY cs.hypertable_schema, cs.hypertable_name
    """
    return client.execute_query(sql, params if params else None)


# ---------------------------------------------------------------------------
# Mutation operations
# ---------------------------------------------------------------------------


def alter_compression_settings(
    client: PgClient,
    fqn: str,
    *,
    segmentby: str | None = None,
    orderby: str | None = None,
) -> None:
    parts = []
    if segmentby:
        parts.append(f"timescaledb.compress_segmentby = '{segmentby}'")
    if orderby:
        parts.append(f"timescaledb.compress_orderby = '{orderby}'")
    if parts:
        client.execute_query(f"ALTER TABLE {fqn} SET ({', '.join(parts)})")


def set_compression_enabled(client: PgClient, fqn: str, *, enabled: bool) -> None:
    if enabled:
        client.execute_query(f"ALTER TABLE {fqn} SET (timescaledb.compress)")
    else:
        client.execute_query(f"ALTER TABLE {fqn} SET (timescaledb.compress = false)")


def add_compression_policy(
    client: PgClient,
    schema: str,
    table: str,
    compress_after: str,
    *,
    schedule: str | None = None,
) -> int | None:
    """Returns job_id if created, None otherwise."""
    if schedule:
        sql = """
        SELECT add_compression_policy(
            (quote_ident(%(schema)s) || '.' || quote_ident(%(table)s))::regclass,
            compress_after => %(after)s::interval,
            schedule_interval => %(schedule)s::interval,
            if_not_exists => true
        )
        """
        params: dict[str, Any] = {
            "schema": schema,
            "table": table,
            "after": compress_after,
            "schedule": schedule,
        }
    else:
        sql = """
        SELECT add_compression_policy(
            (quote_ident(%(schema)s) || '.' || quote_ident(%(table)s))::regclass,
            compress_after => %(after)s::interval,
            if_not_exists => true
        )
        """
        params = {"schema": schema, "table": table, "after": compress_after}

    result = client.execute_query(sql, params)
    return result.rows[0][0] if result.rows else None


def remove_compression_policy(client: PgClient, schema: str, table: str) -> None:
    sql = """
    SELECT remove_compression_policy(
        (quote_ident(%(schema)s) || '.' || quote_ident(%(table)s))::regclass
    )
    """
    client.execute_query(sql, {"schema": schema, "table": table})


def set_chunk_time_interval(
    client: PgClient, schema: str, table: str, interval: str
) -> None:
    sql = """
    SELECT set_chunk_time_interval(
        (quote_ident(%(schema)s) || '.' || quote_ident(%(table)s))::regclass,
        %(interval)s::interval
    )
    """
    client.execute_query(sql, {"schema": schema, "table": table, "interval": interval})


def count_compressed_chunks(client: PgClient, schema: str, table: str) -> int:
    sql = """
    SELECT COUNT(*) FROM timescaledb_information.chunks
    WHERE hypertable_schema = %(schema)s
      AND hypertable_name = %(table)s
      AND is_compressed = true
    """
    result = client.execute_query(sql, {"schema": schema, "table": table})
    return result.rows[0][0] if result.rows else 0


def list_compressed_chunk_names(client: PgClient, schema: str, table: str) -> list[str]:
    sql = """
    SELECT chunk_name FROM timescaledb_information.chunks
    WHERE hypertable_schema = %(schema)s
      AND hypertable_name = %(table)s
      AND is_compressed = true
    ORDER BY range_start
    """
    result = client.execute_query(sql, {"schema": schema, "table": table})
    return [row[0] for row in result.rows]


def recompress_chunk(client: PgClient, chunk_name: str) -> None:
    chunk_fqn = f"_timescaledb_internal.{chunk_name}"
    client.execute_query(f"SELECT decompress_chunk('{chunk_fqn}'::regclass)")
    client.execute_query(f"SELECT compress_chunk('{chunk_fqn}'::regclass)")


def list_chunk_info(
    client: PgClient, schema: str, table: str
) -> list[tuple[str, bool]]:
    sql = """
    SELECT chunk_name, is_compressed
    FROM timescaledb_information.chunks
    WHERE hypertable_schema = %(schema)s
      AND hypertable_name = %(table)s
    ORDER BY range_start
    """
    result = client.execute_query(sql, {"schema": schema, "table": table})
    return [(row[0], row[1]) for row in result.rows]


def compress_single_chunk(client: PgClient, chunk_name: str) -> None:
    chunk_fqn = f"_timescaledb_internal.{chunk_name}"
    client.execute_query(f"SELECT compress_chunk('{chunk_fqn}'::regclass)")


def parse_chunk_id(chunk_name: str) -> int | None:
    """Extract numeric chunk ID from chunk name like '_hyper_16_11420_chunk'."""
    parts = chunk_name.split("_")
    if len(parts) >= 4:
        try:
            return int(parts[-2])
        except ValueError:
            pass
    return None
