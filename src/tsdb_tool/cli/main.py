"""SQL Tool main entry point and command registration."""

from __future__ import annotations

import atexit
from pathlib import Path  # noqa: TC003
from typing import TYPE_CHECKING, Annotated, Any

import sentry_sdk
import typer

from tsdb_tool.__about__ import __version__
from tsdb_tool.cli.commands._shared import (
    apply_local_format_options,
    get_client,
    output_result,
    parse_table_arg,
    preprocess_optional_int_flags,
    size_formatter,
)
from tsdb_tool.cli.commands.config import config_app
from tsdb_tool.cli.commands.query import query_command
from tsdb_tool.cli.commands.service import service_app
from tsdb_tool.cli.commands.ts import ts_app
from tsdb_tool.cli.helpers import format_duration_human, format_relative_time
from tsdb_tool.cli.output import OutputFormat  # noqa: TC001
from tsdb_tool.core.exceptions import SqlToolError
from tsdb_tool.core.logging import setup_logging
from tsdb_tool.core.models import ColumnMeta, QueryResult
from tsdb_tool.core.monitoring import setup_sentry
from tsdb_tool.core.postgres import (
    connections_summary,
    describe_table,
    get_time_column,
    get_timestamp_range,
    list_all_database_names,
    list_connections,
    list_databases,
    list_schemas,
    list_schemas_all_databases,
    list_tables,
    preview_table,
)

if TYPE_CHECKING:
    from tsdb_tool.core.client import PgClient


class SentryTestError(RuntimeError):
    """Test exception for validating Sentry integration."""


app = typer.Typer(
    help="SQL Tool - PostgreSQL query and administration tool",
    no_args_is_help=True,
)

app.add_typer(config_app, name="config")
app.add_typer(service_app, name="service")
app.add_typer(ts_app, name="ts")
app.command("query")(query_command)


def version_callback(value: bool) -> None:
    if value:
        typer.echo(f"tsdb-tool {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    ctx: typer.Context,
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-V",
            help="Show version and exit",
            callback=version_callback,
            is_eager=True,
        ),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", help="Enable verbose logging"),
    ] = False,
    profile: Annotated[
        str | None,
        typer.Option("--profile", "-P", help="Named connection profile"),
    ] = None,
    host: Annotated[
        str | None,
        typer.Option("--host", "-H", help="PostgreSQL host"),
    ] = None,
    port: Annotated[
        int | None,
        typer.Option("--port", "-p", help="PostgreSQL port"),
    ] = None,
    database: Annotated[
        str | None,
        typer.Option("--database", "-d", help="Database name"),
    ] = None,
    user: Annotated[
        str | None,
        typer.Option("--user", "-U", help="User name"),
    ] = None,
    password: Annotated[
        str | None,
        typer.Option("--password", "-W", help="Password"),
    ] = None,
    dsn: Annotated[
        str | None,
        typer.Option("--dsn", help="Connection DSN"),
    ] = None,
    config_file: Annotated[
        Path | None,
        typer.Option("--config", help="Path to config file"),
    ] = None,
    schema: Annotated[
        str | None,
        typer.Option("--schema", "-s", help="Default schema for queries"),
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
        int,
        typer.Option("--width", help="Column width for table format"),
    ] = 40,
    no_header: Annotated[
        bool,
        typer.Option("--no-header", help="Suppress header row in CSV output"),
    ] = False,
) -> None:
    """SQL Tool - PostgreSQL query and administration tool."""
    setup_logging(verbose)
    setup_sentry()

    transaction = sentry_sdk.start_transaction(
        op="cli", name=ctx.invoked_subcommand or "tsdb-tool"
    )
    transaction.__enter__()

    def cleanup() -> None:
        transaction.__exit__(None, None, None)
        sentry_sdk.flush(timeout=2)

    atexit.register(cleanup)

    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["profile"] = profile
    ctx.obj["host"] = host
    ctx.obj["port"] = port
    ctx.obj["database"] = database
    ctx.obj["user"] = user
    ctx.obj["password"] = password
    ctx.obj["dsn"] = dsn
    ctx.obj["config_file"] = config_file
    ctx.obj["schema"] = schema

    # Format options (global)
    fmt = "table" if table else (format.value if format else None)
    ctx.obj["format"] = fmt
    ctx.obj["compact"] = compact
    ctx.obj["width"] = width
    ctx.obj["no_header"] = no_header


def run() -> None:
    """Entry point with global error handling."""
    preprocess_optional_int_flags()
    try:
        app()
    except SqlToolError as e:
        sentry_sdk.capture_exception(e)
        typer.echo(f"Error: {e.message}", err=True)
        raise SystemExit(e.exit_code) from None
    except SystemExit:
        raise
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception as e:
        sentry_sdk.capture_exception(e)
        typer.echo(f"Error: {e}", err=True)
        raise SystemExit(1) from None


@app.command("test-sentry")
def test_sentry() -> None:
    """Send test events to Sentry for validation."""
    typer.echo("Sending test error to Sentry...")
    try:
        raise SentryTestError("tsdb-tool Phase 1 Sentry test error")
    except Exception as e:
        sentry_sdk.capture_exception(e)

    typer.echo("Sending test performance span to Sentry...")
    with sentry_sdk.start_transaction(op="test", name="test_sentry") as txn:
        with (
            sentry_sdk.start_span(op="test.parent", description="Parent span"),
            sentry_sdk.start_span(op="test.child", description="Child span"),
        ):
            pass
        txn.set_status("ok")

    sentry_sdk.flush(timeout=5)
    typer.echo("Test events sent. Check Sentry console:")
    typer.echo("  - Issues: Verify 'tsdb-tool Phase 1 Sentry test error' appears")
    typer.echo("  - Performance: Verify 'test_sentry' transaction appears")


# ---------------------------------------------------------------------------
# Commands: databases, schema, table, connections
# ---------------------------------------------------------------------------


@app.command("databases")
def databases_command(
    ctx: typer.Context,
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
    List all databases with size and owner information.

    Queries pg_catalog.pg_database to show all databases accessible
    to the current user, sorted by size descending. Includes a TOTAL row.
    """
    apply_local_format_options(
        ctx,
        format=format,
        table=table,
        compact=compact,
        width=width,
        no_header=no_header,
    )

    with get_client(ctx) as client:
        result = list_databases(client)

    fmt, _ = size_formatter(ctx)
    total_bytes = sum(row[3] or 0 for row in result.rows)
    rows: list[tuple[Any, ...]] = [
        (name, owner, enc, fmt(size_bytes))
        for name, owner, enc, size_bytes in result.rows
    ]
    rows.append(("TOTAL", "", "", fmt(total_bytes)))

    result = QueryResult(
        columns=[
            ColumnMeta(name="name", type_oid=25, type_name="text"),
            ColumnMeta(name="owner", type_oid=25, type_name="text"),
            ColumnMeta(name="encoding", type_oid=25, type_name="text"),
            ColumnMeta(name="size", type_oid=25, type_name="text"),
        ],
        rows=rows,
        row_count=len(rows),
        status_message=result.status_message,
    )
    output_result(ctx, result)


def _schemas_all_databases(ctx: typer.Context) -> None:
    """Format and output schemas across all databases."""
    with get_client(ctx) as client:
        db_names = list_all_database_names(client)

    def make_client(db_name: str) -> PgClient:
        ctx.ensure_object(dict)["database"] = db_name
        return get_client(ctx)

    raw_data, has_chunks = list_schemas_all_databases(db_names, make_client)

    fmt, _ = size_formatter(ctx)

    grand_tables = sum(r[2] for r in raw_data)
    grand_total = sum(r[3] for r in raw_data)
    grand_before = sum(r[4] for r in raw_data)
    grand_after = sum(r[5] for r in raw_data)
    grand_ht_total = sum(r[6] for r in raw_data)

    all_rows: list[tuple[Any, ...]]
    if has_chunks:
        all_rows = [
            (
                db,
                schema,
                tables,
                fmt(total),
                fmt(before) if before else "-",
                fmt(after) if after else "-",
                fmt(ht) if ht else "-",
            )
            for db, schema, tables, total, before, after, ht in raw_data
        ]
        all_rows.append(
            (
                "TOTAL",
                "",
                grand_tables,
                fmt(grand_total),
                fmt(grand_before) if grand_before else "-",
                fmt(grand_after) if grand_after else "-",
                fmt(grand_ht_total) if grand_ht_total else "-",
            )
        )
        columns = [
            ColumnMeta(name="database", type_oid=25, type_name="text"),
            ColumnMeta(name="schema", type_oid=25, type_name="text"),
            ColumnMeta(name="tables", type_oid=20, type_name="int8"),
            ColumnMeta(name="total_size", type_oid=25, type_name="text"),
            ColumnMeta(name="before_compr", type_oid=25, type_name="text"),
            ColumnMeta(name="after_compr", type_oid=25, type_name="text"),
            ColumnMeta(name="ht_size", type_oid=25, type_name="text"),
        ]
    else:
        all_rows = [
            (db, schema, tables, fmt(total))
            for db, schema, tables, total, _, _, _ in raw_data
        ]
        all_rows.append(("TOTAL", "", grand_tables, fmt(grand_total)))
        columns = [
            ColumnMeta(name="database", type_oid=25, type_name="text"),
            ColumnMeta(name="schema", type_oid=25, type_name="text"),
            ColumnMeta(name="tables", type_oid=20, type_name="int8"),
            ColumnMeta(name="total_size", type_oid=25, type_name="text"),
        ]

    result = QueryResult(
        columns=columns,
        rows=all_rows,
        row_count=len(all_rows),
        status_message=f"SELECT {len(all_rows)}",
    )
    output_result(ctx, result)


@app.command("schema")
def schema_command(
    ctx: typer.Context,
    database: Annotated[
        str | None,
        typer.Option("--database", "-d", help="Database name"),
    ] = None,
    all_databases: Annotated[
        bool,
        typer.Option("--all-databases", help="Show schemas across all databases"),
    ] = False,
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
    List schemas with space usage including TimescaleDB chunk breakdown.

    Shows each schema with table size and chunk usage from hypertables.
    Sorted by total footprint (tables + chunks) descending. Includes TOTAL row.
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

    if all_databases:
        _schemas_all_databases(ctx)
        return

    with get_client(ctx) as client:
        schema_result, chunk_map = list_schemas(client)
        db_name = client.execute_query("SELECT current_database()").rows[0][0]

    fmt, is_table = size_formatter(ctx)

    rows: list[tuple[Any, ...]]
    if chunk_map:
        decorated = []
        grand_total = 0
        grand_tables = 0
        grand_before = 0
        grand_after = 0
        grand_ht_total = 0
        for schema, tables, total_bytes in schema_result.rows:
            before_b, after_b, ht_total = chunk_map.get(schema, (0, 0, 0))
            footprint = (total_bytes or 0) + ht_total
            grand_total += total_bytes or 0
            grand_tables += tables or 0
            grand_before += before_b
            grand_after += after_b
            grand_ht_total += ht_total
            decorated.append(
                (footprint, schema, tables, total_bytes, before_b, after_b, ht_total)
            )

        decorated.sort(key=lambda r: r[0], reverse=True)
        rows = [
            (
                schema,
                tables,
                fmt(total_bytes),
                fmt(before_b) if before_b else "-",
                fmt(after_b) if after_b else "-",
                fmt(ht_total) if ht_total else "-",
            )
            for _, schema, tables, total_bytes, before_b, after_b, ht_total in decorated
        ]
        rows.append(
            (
                "TOTAL",
                grand_tables,
                fmt(grand_total),
                fmt(grand_before) if grand_before else "-",
                fmt(grand_after) if grand_after else "-",
                fmt(grand_ht_total) if grand_ht_total else "-",
            )
        )

        result = QueryResult(
            columns=[
                ColumnMeta(name="schema", type_oid=25, type_name="text"),
                ColumnMeta(name="tables", type_oid=20, type_name="int8"),
                ColumnMeta(name="total_size", type_oid=25, type_name="text"),
                ColumnMeta(name="before_compr", type_oid=25, type_name="text"),
                ColumnMeta(name="after_compr", type_oid=25, type_name="text"),
                ColumnMeta(name="ht_size", type_oid=25, type_name="text"),
            ],
            rows=rows,
            row_count=len(rows),
            status_message=schema_result.status_message,
        )
    else:
        sorted_data = sorted(schema_result.rows, key=lambda r: r[2] or 0, reverse=True)
        grand_total = sum(r[2] or 0 for r in sorted_data)
        grand_tables = sum(r[1] or 0 for r in sorted_data)
        rows = [
            (schema, tables, fmt(total_bytes))
            for schema, tables, total_bytes in sorted_data
        ]
        rows.append(("TOTAL", grand_tables, fmt(grand_total)))
        result = QueryResult(
            columns=[
                ColumnMeta(name="schema", type_oid=25, type_name="text"),
                ColumnMeta(name="tables", type_oid=20, type_name="int8"),
                ColumnMeta(name="total_size", type_oid=25, type_name="text"),
            ],
            rows=rows,
            row_count=len(rows),
            status_message=schema_result.status_message,
        )

    if is_table:
        typer.echo(f"Schemas of: {db_name}", err=True)
    output_result(ctx, result)


def _table_list(
    ctx: typer.Context,
    *,
    schema_filter: str | None = None,
    include_internal_tables: bool = False,
) -> None:
    """Format and output table list with size breakdown."""
    with get_client(ctx) as client:
        result, ht_map = list_tables(
            client,
            schema_filter=schema_filter,
            include_internal_tables=include_internal_tables,
        )

    fmt, _ = size_formatter(ctx)
    has_schema = not schema_filter

    def _ratio(before: int | None, after: int | None) -> str:
        if not before or not after:
            return "-"
        return f"{before / after:.1f}x"

    new_rows: list[tuple[Any, ...]] = []
    totals_table = 0
    totals_index = 0
    totals_total = 0
    totals_before = 0
    totals_after = 0

    for row in result.rows:
        if has_schema:
            schema_val, name_val, table_b, index_b, total_b = row
            base: tuple[Any, ...] = (
                schema_val,
                name_val,
                fmt(table_b),
                fmt(index_b),
                fmt(total_b),
            )
        else:
            name_val, table_b, index_b, total_b = row
            schema_val = schema_filter or "public"
            base = (name_val, fmt(table_b), fmt(index_b), fmt(total_b))

        totals_table += table_b or 0
        totals_index += index_b or 0
        totals_total += total_b or 0

        if ht_map:
            ht_info = ht_map.get((schema_val, name_val))
            if ht_info:
                uncompr_c, compr_c, before_b, after_b = ht_info
                totals_before += before_b or 0
                totals_after += after_b or 0
                new_rows.append(
                    (
                        *base,
                        f"{uncompr_c}/{compr_c}",
                        fmt(before_b) if before_b else "-",
                        fmt(after_b) if after_b else "-",
                        _ratio(before_b, after_b),
                    )
                )
            else:
                new_rows.append((*base, "-", "-", "-", "-"))
        else:
            new_rows.append(base)

    if has_schema:
        columns = [
            ColumnMeta(name="schema", type_oid=25, type_name="text"),
            ColumnMeta(name="name", type_oid=25, type_name="text"),
            ColumnMeta(name="table_size", type_oid=25, type_name="text"),
            ColumnMeta(name="index_size", type_oid=25, type_name="text"),
            ColumnMeta(name="total", type_oid=25, type_name="text"),
        ]
    else:
        columns = [
            ColumnMeta(name="name", type_oid=25, type_name="text"),
            ColumnMeta(name="table_size", type_oid=25, type_name="text"),
            ColumnMeta(name="index_size", type_oid=25, type_name="text"),
            ColumnMeta(name="total", type_oid=25, type_name="text"),
        ]

    if ht_map:
        columns.extend(
            [
                ColumnMeta(name="chunks_u/c", type_oid=25, type_name="text"),
                ColumnMeta(name="before_size", type_oid=25, type_name="text"),
                ColumnMeta(name="after_size", type_oid=25, type_name="text"),
                ColumnMeta(name="ratio", type_oid=25, type_name="text"),
            ]
        )

    if has_schema:
        totals_row: tuple[Any, ...] = (
            "TOTAL",
            "",
            fmt(totals_table),
            fmt(totals_index),
            fmt(totals_total),
        )
    else:
        totals_row = ("TOTAL", fmt(totals_table), fmt(totals_index), fmt(totals_total))
    if ht_map:
        totals_row = (
            *totals_row,
            "",
            fmt(totals_before) if totals_before else "-",
            fmt(totals_after) if totals_after else "-",
            _ratio(totals_before, totals_after),
        )
    new_rows.append(totals_row)

    result = QueryResult(
        columns=columns,
        rows=new_rows,
        row_count=len(new_rows),
        status_message=result.status_message,
    )
    output_result(ctx, result)


@app.command("table")
def table_command(
    ctx: typer.Context,
    table_arg: Annotated[
        str | None,
        typer.Argument(
            help="Table name (schema.table or table). Omit to list all tables."
        ),
    ] = None,
    database: Annotated[
        str | None,
        typer.Option("--database", "-d", help="Database name"),
    ] = None,
    schema_filter: Annotated[
        str | None,
        typer.Option("--schema", "-s", help="Filter tables by schema"),
    ] = None,
    include_internal_tables: Annotated[
        bool,
        typer.Option(
            "--include-internal-tables", help="Include TimescaleDB internal tables"
        ),
    ] = False,
    show_range: Annotated[
        bool,
        typer.Option("--range", help="Show timestamp range (min/max) for hypertables"),
    ] = False,
    head: Annotated[
        int | None,
        typer.Option(
            "--head", help="Show first N rows ordered by time ASC (default: 10)"
        ),
    ] = None,
    tail: Annotated[
        int | None,
        typer.Option(
            "--tail", help="Show last N rows ordered by time DESC (default: 10)"
        ),
    ] = None,
    sample: Annotated[
        int | None,
        typer.Option("--sample", help="Show N random rows (default: 10)"),
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
    List tables or show table details.

    Without a table name, lists all tables with size breakdown.
    With a table name, shows column definitions with optional data preview.

    List mode: Use --schema to filter, --include-internal-tables for TimescaleDB internals.
    Detail mode: Use --range for timestamp range, --head/--tail/--sample for data preview.
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

    if table_arg is None:
        _table_list(
            ctx,
            schema_filter=schema_filter,
            include_internal_tables=include_internal_tables,
        )
        return

    schema_name, table_name = parse_table_arg(table_arg)

    has_preview = head is not None or tail is not None or sample is not None

    time_column = None
    needs_time_col = show_range or head is not None or tail is not None
    if needs_time_col:
        with get_client(ctx) as client:
            time_column = get_time_column(client, schema_name, table_name)

    if not has_preview:
        with get_client(ctx) as client:
            result = describe_table(client, schema_name, table_name)
        output_result(ctx, result)

        if show_range and time_column:
            with get_client(ctx) as client:
                range_result = get_timestamp_range(
                    client, schema_name, table_name, time_column
                )
            if range_result.rows:
                typer.echo("\nTimestamp Range:", err=True)
                output_result(ctx, range_result)
    else:
        with get_client(ctx) as client:
            preview_result = preview_table(
                client,
                schema_name,
                table_name,
                head=head,
                tail=tail,
                sample=sample,
                time_column=time_column,
            )
        if preview_result:
            output_result(ctx, preview_result)


@app.command("connections")
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
