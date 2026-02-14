from __future__ import annotations

from typing import Annotated

import structlog
import typer

from sql_tool.cli.commands._shared import get_client, output_result
from sql_tool.core.query_source import resolve_query_source

log = structlog.get_logger()


def query_command(
    ctx: typer.Context,
    file: Annotated[
        str | None,
        typer.Argument(help="SQL file to execute"),
    ] = None,
    execute: Annotated[
        str | None,
        typer.Option("--execute", "-e", help="Execute inline SQL query"),
    ] = None,
    timeout: Annotated[
        float | None,
        typer.Option("--timeout", "-t", help="Query timeout in seconds"),
    ] = None,
) -> None:
    """Execute a SQL query from file, inline (-e), or stdin."""
    sql = resolve_query_source(inline=execute, file_path=file)

    with get_client(ctx, timeout=timeout) as client:
        result = client.execute_query(sql)

    output_result(ctx, result)
