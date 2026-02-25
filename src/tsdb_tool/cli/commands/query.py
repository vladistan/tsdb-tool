from __future__ import annotations

import sys
from typing import Annotated

import typer

from tsdb_tool.cli.commands._shared import get_client, output_result
from tsdb_tool.core.exceptions import InputError
from tsdb_tool.core.exit_codes import ExitCode
from tsdb_tool.core.query_source import resolve_query_source


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
    try:
        is_tty = sys.stdin.isatty()
    except (ValueError, AttributeError):
        is_tty = False
    if execute is None and file is None and is_tty:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    try:
        sql = resolve_query_source(inline=execute, file_path=file)
    except InputError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(ExitCode.INPUT_ERROR) from exc

    with get_client(ctx, timeout=timeout) as client:
        result = client.execute_query(sql)

    output_result(ctx, result)
