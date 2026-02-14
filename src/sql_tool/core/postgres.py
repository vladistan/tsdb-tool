"""PostgreSQL service and introspection operations.

Framework-agnostic business logic for database commands.
CLI layers in cli/commands/service.py and cli/main.py provide the typer interface.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sql_tool.core.models import ColumnMeta, QueryResult

if TYPE_CHECKING:
    from collections.abc import Callable

    from sql_tool.core.client import PgClient

# ---------------------------------------------------------------------------
# Service operations (used by cli/commands/service.py)
# ---------------------------------------------------------------------------


def check_server(client: PgClient) -> QueryResult:
    """Check PostgreSQL server connectivity and return server info."""
    queries = [
        ("version", "SELECT version()"),
        ("database", "SELECT current_database()"),
        ("user", "SELECT current_user"),
        ("uptime", "SELECT pg_postmaster_start_time()"),
    ]

    rows: list[tuple[Any, ...]] = []
    for key, sql in queries:
        result = client.execute_query(sql)
        if result.rows:
            rows.append((key, str(result.rows[0][0])))

    return QueryResult(
        columns=[
            ColumnMeta(name="property", type_oid=25, type_name="text"),
            ColumnMeta(name="value", type_oid=25, type_name="text"),
        ],
        rows=rows,
        row_count=len(rows),
        status_message=f"SELECT {len(rows)}",
    )


def list_user_tables(client: PgClient) -> list[str]:
    """List all user tables (excluding system schemas)."""
    sql = """
    SELECT schemaname || '.' || tablename
    FROM pg_catalog.pg_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    """
    result = client.execute_query(sql)
    return [row[0] for row in result.rows]


def vacuum_tables(
    client: PgClient, table_names: list[str], *, full: bool = False
) -> int:
    """Run VACUUM ANALYZE on the given tables. Returns count of tables vacuumed."""
    vacuum_cmd = "VACUUM FULL ANALYZE" if full else "VACUUM ANALYZE"
    for tbl in table_names:
        client.execute_query(f"{vacuum_cmd} {tbl}")
    return len(table_names)


def kill_backend(client: PgClient, pid: int, *, cancel: bool = False) -> bool:
    """Terminate or cancel a PostgreSQL backend. Returns True if successful."""
    if cancel:
        sql = f"SELECT pg_cancel_backend({pid})"
    else:
        sql = f"SELECT pg_terminate_backend({pid})"

    result = client.execute_query(sql)
    return bool(result.rows and result.rows[0][0])


# ---------------------------------------------------------------------------
# Database introspection (used by cli/main.py)
# ---------------------------------------------------------------------------


def list_databases(client: PgClient) -> QueryResult:
    """Query all databases with size and owner info, sorted by size DESC."""
    sql = """
    SELECT
        d.datname AS name,
        pg_catalog.pg_get_userbyid(d.datdba) AS owner,
        pg_catalog.pg_encoding_to_char(d.encoding) AS encoding,
        pg_catalog.pg_database_size(d.datname) AS size_bytes
    FROM pg_catalog.pg_database d
    ORDER BY pg_catalog.pg_database_size(d.datname) DESC
    """
    return client.execute_query(sql)


def list_all_database_names(client: PgClient) -> list[str]:
    """List all non-template database names."""
    sql = """
    SELECT datname FROM pg_catalog.pg_database
    WHERE datistemplate = false
    ORDER BY datname
    """
    result = client.execute_query(sql)
    return [row[0] for row in result.rows]


_SCHEMA_SQL = """
SELECT
    schemaname AS schema,
    COUNT(*) AS tables,
    SUM(pg_catalog.pg_total_relation_size(
        quote_ident(schemaname)||'.'||quote_ident(tablename)
    )) AS total_bytes
FROM pg_catalog.pg_tables
WHERE schemaname NOT IN (
    'pg_catalog', 'information_schema',
    '_timescaledb_cache', '_timescaledb_catalog',
    '_timescaledb_internal', '_timescaledb_config'
)
GROUP BY schemaname
"""

_CHUNK_SQL = """
SELECT
    h.hypertable_schema,
    SUM(COALESCE(cs.before_compression_total_bytes,
        (SELECT SUM(pg_total_relation_size(
            ('_timescaledb_internal.' || quote_ident(c.chunk_name))::regclass))
         FROM timescaledb_information.chunks c
         WHERE c.hypertable_schema = h.hypertable_schema
           AND c.hypertable_name = h.hypertable_name)
    ))::bigint AS before_bytes,
    SUM(cs.after_compression_total_bytes)::bigint AS after_bytes,
    SUM(hypertable_size(
        (quote_ident(h.hypertable_schema) || '.' || quote_ident(h.hypertable_name))::regclass
    )) AS ht_total_bytes
FROM timescaledb_information.hypertables h
LEFT JOIN LATERAL (
    SELECT
        SUM(before_compression_total_bytes)::bigint AS before_compression_total_bytes,
        SUM(after_compression_total_bytes)::bigint AS after_compression_total_bytes
    FROM hypertable_compression_stats(
        (quote_ident(h.hypertable_schema) || '.' || quote_ident(h.hypertable_name))::regclass
    )
) cs ON true
GROUP BY h.hypertable_schema
"""


def _query_chunk_map(client: PgClient) -> dict[str, tuple[int, int, int]]:
    """Query TimescaleDB chunk stats per schema. Returns empty dict if unavailable."""
    chunk_map: dict[str, tuple[int, int, int]] = {}
    try:
        chunk_result = client.execute_query(_CHUNK_SQL)
        for ht_schema, before_b, after_b, ht_total in chunk_result.rows:
            chunk_map[ht_schema] = (before_b or 0, after_b or 0, ht_total or 0)
    except Exception:
        pass
    return chunk_map


def list_schemas(
    client: PgClient,
) -> tuple[QueryResult, dict[str, tuple[int, int, int]]]:
    """Query schemas with table counts/sizes and optional TimescaleDB chunk stats.

    Returns (schema_result, chunk_map) where:
    - schema_result rows: (schema, tables, total_bytes)
    - chunk_map: schema -> (before_bytes, after_bytes, ht_total_bytes)
    """
    schema_result = client.execute_query(_SCHEMA_SQL)
    chunk_map = _query_chunk_map(client)
    return schema_result, chunk_map


def list_schemas_all_databases(
    db_names: list[str],
    client_factory: Callable[[str], PgClient],
) -> tuple[list[tuple[str, str, int, int, int, int, int]], bool]:
    """Query schemas across multiple databases.

    Returns (raw_data, has_chunks) where raw_data rows are:
    (db_name, schema, tables, total_bytes, before_b, after_b, ht_total).
    """
    raw_data: list[tuple[str, str, int, int, int, int, int]] = []
    has_chunks = False

    for db_name in db_names:
        try:
            with client_factory(db_name) as client:
                schema_result, chunk_map = list_schemas(client)
                if chunk_map:
                    has_chunks = True

                for schema, tables, total_bytes in schema_result.rows:
                    before_b, after_b, ht_total = chunk_map.get(schema, (0, 0, 0))
                    raw_data.append(
                        (
                            db_name,
                            schema,
                            tables or 0,
                            total_bytes or 0,
                            before_b,
                            after_b,
                            ht_total,
                        )
                    )
        except Exception:
            raw_data.append((db_name, "(connection failed)", 0, 0, 0, 0, 0))

    return raw_data, has_chunks


def list_tables(
    client: PgClient,
    *,
    schema_filter: str | None = None,
    include_internal_tables: bool = False,
) -> tuple[QueryResult, dict[tuple[str, str], tuple[int, int, int | None, int | None]]]:
    """Query tables with size breakdown and optional hypertable stats.

    Returns (table_result, ht_map) where:
    - table_result rows: if schema_filter set: (name, table_size, index_size, total)
      otherwise: (schema, name, table_size, index_size, total)
    - ht_map: (schema, name) -> (uncompr_chunks, compr_chunks, before_bytes, after_bytes)
    """
    if schema_filter:
        where_clause = "WHERE schemaname = %(schema)s"
        params: dict[str, Any] = {"schema": schema_filter}
        schema_column = ""
    else:
        if include_internal_tables:
            where_clause = (
                "WHERE schemaname NOT IN ('pg_catalog', 'information_schema')"
            )
        else:
            where_clause = """WHERE schemaname NOT IN ('pg_catalog', 'information_schema',
                '_timescaledb_cache', '_timescaledb_catalog',
                '_timescaledb_internal', '_timescaledb_config')"""
        params = {}
        schema_column = "schemaname AS schema,"

    sql = f"""
    SELECT
        {schema_column}
        tablename AS name,
        pg_catalog.pg_table_size(quote_ident(schemaname)||'.'||quote_ident(tablename)) AS table_size_bytes,
        pg_catalog.pg_indexes_size(quote_ident(schemaname)||'.'||quote_ident(tablename)) AS index_size_bytes,
        pg_catalog.pg_total_relation_size(quote_ident(schemaname)||'.'||quote_ident(tablename)) AS total_bytes
    FROM pg_catalog.pg_tables
    {where_clause}
    ORDER BY schemaname, tablename
    """

    ht_sql = """
    SELECT
        h.hypertable_schema,
        h.hypertable_name,
        (SELECT COUNT(*) FILTER (WHERE NOT c.is_compressed) FROM timescaledb_information.chunks c
         WHERE c.hypertable_schema = h.hypertable_schema AND c.hypertable_name = h.hypertable_name) AS uncompr_chunks,
        (SELECT COUNT(*) FILTER (WHERE c.is_compressed) FROM timescaledb_information.chunks c
         WHERE c.hypertable_schema = h.hypertable_schema AND c.hypertable_name = h.hypertable_name) AS compr_chunks,
        COALESCE(d.before_compression_total_bytes,
            (SELECT SUM(pg_total_relation_size(
                ('_timescaledb_internal.' || quote_ident(c.chunk_name))::regclass))
             FROM timescaledb_information.chunks c
             WHERE c.hypertable_schema = h.hypertable_schema
               AND c.hypertable_name = h.hypertable_name)
        ) AS before_bytes,
        d.after_compression_total_bytes AS after_bytes
    FROM timescaledb_information.hypertables h
    LEFT JOIN LATERAL (
        SELECT
            SUM(before_compression_total_bytes)::bigint AS before_compression_total_bytes,
            SUM(after_compression_total_bytes)::bigint AS after_compression_total_bytes
        FROM hypertable_compression_stats(
            (quote_ident(h.hypertable_schema) || '.' || quote_ident(h.hypertable_name))::regclass
        )
    ) d ON true
    """

    result = client.execute_query(sql, params if params else None)

    ht_map: dict[tuple[str, str], tuple[int, int, int | None, int | None]] = {}
    try:
        ht_result = client.execute_query(ht_sql)
        for (
            ht_schema,
            ht_name,
            uncompr_chunks,
            compr_chunks,
            before_bytes,
            after_bytes,
        ) in ht_result.rows:
            ht_map[(ht_schema, ht_name)] = (
                uncompr_chunks or 0,
                compr_chunks or 0,
                before_bytes,
                after_bytes,
            )
    except Exception:
        pass

    return result, ht_map


def describe_table(client: PgClient, schema_name: str, table_name: str) -> QueryResult:
    """Get column definitions for a table."""
    sql = """
    SELECT
        column_name,
        data_type,
        is_nullable,
        column_default
    FROM information_schema.columns
    WHERE table_schema = %(schema)s AND table_name = %(table)s
    ORDER BY ordinal_position
    """
    return client.execute_query(sql, {"schema": schema_name, "table": table_name})


def get_time_column(client: PgClient, schema_name: str, table_name: str) -> str | None:
    """Get the primary time dimension column for a hypertable. Returns None if not a hypertable."""
    sql = """
    SELECT d.column_name
    FROM timescaledb_information.dimensions d
    WHERE d.hypertable_schema = %(schema)s
      AND d.hypertable_name = %(table)s
      AND d.dimension_number = 1
    LIMIT 1
    """
    try:
        result = client.execute_query(sql, {"schema": schema_name, "table": table_name})
        if result.rows:
            return str(result.rows[0][0])
    except Exception:
        pass
    return None


def get_timestamp_range(
    client: PgClient,
    schema_name: str,
    table_name: str,
    time_column: str,
) -> QueryResult:
    """Get min/max timestamps for a table's time column."""
    sql = f"""
    SELECT
        MIN({time_column})::text AS min_timestamp,
        MAX({time_column})::text AS max_timestamp
    FROM {schema_name}.{table_name}
    """
    return client.execute_query(sql)


def preview_table(
    client: PgClient,
    schema_name: str,
    table_name: str,
    *,
    head: int | None = None,
    tail: int | None = None,
    sample: int | None = None,
    time_column: str | None = None,
) -> QueryResult | None:
    """Preview table data with head/tail/sample modes. Returns None if no rows."""
    limit = head or tail or sample
    if not limit:
        return None

    if sample is not None:
        if time_column:
            sql = f"""
            SELECT * FROM {schema_name}.{table_name}
            WHERE {time_column} >= (
                SELECT MAX({time_column}) - interval '7 days'
                FROM {schema_name}.{table_name}
            )
            ORDER BY random()
            LIMIT %(limit)s
            """
        else:
            sql = f"""
            SELECT * FROM {schema_name}.{table_name}
            TABLESAMPLE BERNOULLI(1)
            LIMIT %(limit)s
            """
    else:
        if head is not None and time_column:
            order_clause = f"ORDER BY {time_column} ASC"
        elif tail is not None and time_column:
            order_clause = f"ORDER BY {time_column} DESC"
        else:
            order_clause = ""

        sql = f"""
        SELECT * FROM {schema_name}.{table_name}
        {order_clause}
        LIMIT %(limit)s
        """

    result = client.execute_query(sql, {"limit": limit})
    return result if result.rows else None


def list_connections(
    client: PgClient,
    *,
    include_all: bool = False,
    min_duration: float | None = None,
    filter_user: str | None = None,
    filter_db: str | None = None,
    filter_state: str | None = None,
) -> QueryResult:
    """Query pg_stat_activity with filters.

    Returns rows of (pid, user, db, app, client_addr, state, wait_event,
    connected_since, connected_seconds, query_start, query_seconds, query).
    """
    filters = ["pid != pg_backend_pid()"]
    query_params: dict[str, Any] = {}

    if not include_all:
        filters.append("state IS NOT NULL AND state != 'idle'")

    if min_duration is not None:
        filters.append(f"(now() - query_start) > interval '{min_duration} seconds'")

    if filter_user is not None:
        filters.append("usename = %(filter_user)s")
        query_params["filter_user"] = filter_user

    if filter_db is not None:
        filters.append("datname = %(filter_db)s")
        query_params["filter_db"] = filter_db

    if filter_state is not None:
        filters.append("state = %(filter_state)s")
        query_params["filter_state"] = filter_state

    where_clause = " AND ".join(f"({f})" for f in filters)

    sql = f"""
    SELECT
        pid,
        usename AS user,
        datname AS database,
        application_name,
        client_addr::text AS client_address,
        state,
        wait_event,
        backend_start::text AS connected_since,
        EXTRACT(EPOCH FROM (now() - backend_start)) AS connected_seconds,
        query_start::text AS query_start,
        EXTRACT(EPOCH FROM (now() - query_start)) AS query_seconds,
        query
    FROM pg_stat_activity
    WHERE {where_clause}
    ORDER BY query_start
    """

    return client.execute_query(sql, query_params if query_params else None)


def connections_summary(client: PgClient) -> QueryResult:
    """Connection counts grouped by state plus memory configuration settings."""
    conn_sql = """
    SELECT
        COALESCE(state, 'total') AS state,
        COUNT(*) AS count
    FROM pg_stat_activity
    WHERE pid != pg_backend_pid()
    GROUP BY ROLLUP(state)
    ORDER BY CASE WHEN state IS NULL THEN 1 ELSE 0 END, count DESC
    """

    mem_sql = """
    SELECT name AS setting, current_setting(name) AS value
    FROM pg_settings
    WHERE name IN (
        'max_connections', 'shared_buffers', 'effective_cache_size',
        'work_mem', 'maintenance_work_mem'
    )
    ORDER BY name
    """

    conn_result = client.execute_query(conn_sql)
    mem_result = client.execute_query(mem_sql)

    combined_rows: list[tuple[Any, ...]] = []
    for row in conn_result.rows:
        combined_rows.append((row[0], str(row[1])))
    combined_rows.append(("---", "---"))
    for row in mem_result.rows:
        combined_rows.append((row[0], str(row[1])))

    return QueryResult(
        columns=[
            ColumnMeta(name="property", type_oid=25, type_name="text"),
            ColumnMeta(name="value", type_oid=25, type_name="text"),
        ],
        rows=combined_rows,
        row_count=len(combined_rows),
        status_message=f"SELECT {len(combined_rows)}",
    )
