from __future__ import annotations

from typing import Annotated, Any

import typer

from tsdb_tool.cli.commands._shared import (
    apply_local_format_options,
    get_client,
    output_result,
    parse_table_arg,
    size_formatter,
)
from tsdb_tool.cli.output import OutputFormat  # noqa: TC001
from tsdb_tool.core.models import ColumnMeta, QueryResult
from tsdb_tool.core.postgres import (
    describe_table,
    get_time_column,
    get_timestamp_range,
    list_tables,
    preview_table,
)


def _table_list(
    ctx: typer.Context,
    *,
    schema_filter: str | None = None,
    include_internal_tables: bool = False,
) -> None:
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
