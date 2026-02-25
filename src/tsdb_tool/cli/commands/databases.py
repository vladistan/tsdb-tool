from __future__ import annotations

from typing import Annotated, Any

import typer

from tsdb_tool.cli.commands._shared import (
    apply_local_format_options,
    get_client,
    output_result,
    size_formatter,
)
from tsdb_tool.cli.output import OutputFormat  # noqa: TC001
from tsdb_tool.core.models import ColumnMeta, QueryResult
from tsdb_tool.core.postgres import list_databases


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
