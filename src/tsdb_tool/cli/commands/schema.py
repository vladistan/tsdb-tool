from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any

import typer

from tsdb_tool.cli.commands._shared import (
    apply_local_format_options,
    get_client,
    output_result,
    size_formatter,
)
from tsdb_tool.cli.output import OutputFormat  # noqa: TC001
from tsdb_tool.core.models import ColumnMeta, QueryResult
from tsdb_tool.core.postgres import (
    list_all_database_names,
    list_schemas,
    list_schemas_all_databases,
)

if TYPE_CHECKING:
    from tsdb_tool.core.client import PgClient


def _schemas_all_databases(ctx: typer.Context) -> None:
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
