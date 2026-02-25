from __future__ import annotations

from typing import Annotated, Any

import typer

from tsdb_tool.cli.commands._shared import (
    apply_local_format_options,
    get_client,
    output_result,
    size_formatter,
)
from tsdb_tool.cli.helpers import format_duration_human, format_relative_time
from tsdb_tool.cli.output import OutputFormat  # noqa: TC001
from tsdb_tool.core.models import ColumnMeta, QueryResult
from tsdb_tool.core.postgres import connections_summary, list_connections


def connections_command(
    ctx: typer.Context,
    database: Annotated[
        str | None,
        typer.Option("--database", "-d", help="Database name"),
    ] = None,
    summary: Annotated[
        bool,
        typer.Option(
            "--summary", help="Show connection counts and memory configuration"
        ),
    ] = False,
    include_all: Annotated[
        bool,
        typer.Option("--all", help="Include idle connections"),
    ] = False,
    min_duration: Annotated[
        float | None,
        typer.Option(
            "--min-duration", help="Filter queries running longer than N seconds"
        ),
    ] = None,
    filter_user: Annotated[
        str | None,
        typer.Option("--filter-user", help="Filter by username"),
    ] = None,
    filter_db: Annotated[
        str | None,
        typer.Option("--filter-db", help="Filter by database name"),
    ] = None,
    filter_state: Annotated[
        str | None,
        typer.Option(
            "--filter-state", help="Filter by connection state (active, idle, etc.)"
        ),
    ] = None,
    format: Annotated[
        OutputFormat | None,
        typer.Option("--format", "-f", help="Output format: table|json|csv"),
    ] = None,
    table: Annotated[
        bool,
        typer.Option("--table", help="Shorthand for --format table"),
    ] = False,
    compact: Annotated[
        bool,
        typer.Option("--compact", help="Compact JSON output (no indentation)"),
    ] = False,
    width: Annotated[
        int | None,
        typer.Option("--width", help="Column width for table format"),
    ] = None,
    no_header: Annotated[
        bool,
        typer.Option("--no-header", help="Suppress header row in CSV output"),
    ] = False,
) -> None:
    """
    Show database connections with details and filtering.

    By default shows non-idle connections with full details including
    application name, client address, wait events, and query duration.
    Use --all to include idle connections.
    Use --summary for connection counts and memory configuration.
    Use --filter-user, --filter-db, --filter-state to narrow results.
    """
    if database is not None:
        ctx.ensure_object(dict)["database"] = database
    apply_local_format_options(
        ctx,
        format=format,
        table=table,
        compact=compact,
        width=width,
        no_header=no_header,
    )

    if summary:
        with get_client(ctx) as client:
            result = connections_summary(client)
        output_result(ctx, result)
        return

    with get_client(ctx) as client:
        raw_result = list_connections(
            client,
            include_all=include_all,
            min_duration=min_duration,
            filter_user=filter_user,
            filter_db=filter_db,
            filter_state=filter_state,
        )

    _, is_table = size_formatter(ctx)

    rows: list[tuple[Any, ...]]
    if is_table:
        rows = [
            (
                pid,
                user,
                db,
                app,
                addr,
                state,
                wait_event,
                format_relative_time(conn_secs),
                format_relative_time(q_secs),
                format_duration_human(q_secs),
                query,
            )
            for pid, user, db, app, addr, state, wait_event, _, conn_secs, _, q_secs, query in raw_result.rows
        ]
    else:
        rows = [
            (
                pid,
                user,
                db,
                app,
                addr,
                state,
                wait_event,
                connected_since,
                query_start,
                query,
            )
            for pid, user, db, app, addr, state, wait_event, connected_since, _, query_start, _, query in raw_result.rows
        ]

    if is_table:
        columns = [
            ColumnMeta(name="pid", type_oid=23, type_name="int4"),
            ColumnMeta(name="user", type_oid=25, type_name="text"),
            ColumnMeta(name="database", type_oid=25, type_name="text"),
            ColumnMeta(name="app", type_oid=25, type_name="text"),
            ColumnMeta(name="client", type_oid=25, type_name="text"),
            ColumnMeta(name="state", type_oid=25, type_name="text"),
            ColumnMeta(name="wait_event", type_oid=25, type_name="text"),
            ColumnMeta(name="connected", type_oid=25, type_name="text"),
            ColumnMeta(name="query_start", type_oid=25, type_name="text"),
            ColumnMeta(name="duration", type_oid=25, type_name="text"),
            ColumnMeta(name="query", type_oid=25, type_name="text"),
        ]
    else:
        columns = [
            ColumnMeta(name="pid", type_oid=23, type_name="int4"),
            ColumnMeta(name="user", type_oid=25, type_name="text"),
            ColumnMeta(name="database", type_oid=25, type_name="text"),
            ColumnMeta(name="application_name", type_oid=25, type_name="text"),
            ColumnMeta(name="client_address", type_oid=25, type_name="text"),
            ColumnMeta(name="state", type_oid=25, type_name="text"),
            ColumnMeta(name="wait_event", type_oid=25, type_name="text"),
            ColumnMeta(name="connected_since", type_oid=25, type_name="text"),
            ColumnMeta(name="query_start", type_oid=25, type_name="text"),
            ColumnMeta(name="query", type_oid=25, type_name="text"),
        ]

    result = QueryResult(
        columns=columns,
        rows=rows,
        row_count=len(rows),
        status_message=raw_result.status_message,
    )
    output_result(ctx, result)
