"""Service and maintenance CLI commands.

Thin CLI layer: typer decorators, argument parsing, output formatting.
Business logic delegated to core.postgres module.
"""

from __future__ import annotations

from typing import Annotated

import typer

from tsdb_tool.cli.commands._shared import get_client, output_result
from tsdb_tool.core.postgres import (
    check_server,
    kill_backend,
    list_user_tables,
    vacuum_tables,
)

service_app = typer.Typer(help="Service and maintenance commands")


@service_app.callback(invoke_without_command=True)
def service_callback(
    ctx: typer.Context,
) -> None:
    if not ctx.invoked_subcommand:
        typer.echo(ctx.get_help())
        raise typer.Exit()


@service_app.command("vacuum")
def vacuum_command(
    ctx: typer.Context,
    table_arg: Annotated[
        str | None,
        typer.Argument(help="Table name to vacuum"),
    ] = None,
    full: Annotated[
        bool,
        typer.Option("--full", help="Run VACUUM FULL (locks table, rewrites)"),
    ] = False,
    all_tables: Annotated[
        bool,
        typer.Option("--all", help="Vacuum all user tables"),
    ] = False,
) -> None:
    """VACUUM marks dead rows as reusable. VACUUM FULL rewrites tables but requires exclusive lock."""
    if not table_arg and not all_tables:
        typer.echo("Error: Must specify table name or --all", err=True)
        raise typer.Exit(2)

    with get_client(ctx) as client:
        table_names = list_user_tables(client) if all_tables else [table_arg]  # type: ignore[list-item]

        count = vacuum_tables(client, table_names, full=full)

    typer.echo(f"Vacuumed {count} table(s)")


@service_app.command("kill")
def kill_command(
    ctx: typer.Context,
    pid: Annotated[int, typer.Argument(help="Backend process ID to terminate")],
    cancel: Annotated[
        bool,
        typer.Option("--cancel", help="Cancel query only (don't kill connection)"),
    ] = False,
) -> None:
    """pg_terminate_backend() kills the connection. pg_cancel_backend() cancels query but keeps connection alive."""
    with get_client(ctx) as client:
        success = kill_backend(client, pid, cancel=cancel)

    action = "Cancelled" if cancel else "Terminated"
    if success:
        typer.echo(f"{action} backend {pid}")
    else:
        typer.echo(f"Error: Backend {pid} not found or already terminated", err=True)
        raise typer.Exit(1)


@service_app.command("check")
def check_command(ctx: typer.Context) -> None:
    """
    Check PostgreSQL server connectivity and version.

    Reports server version, current database, connected user, and uptime.
    Useful for verifying connection parameters and server availability.
    """
    with get_client(ctx) as client:
        result = check_server(client)

    output_result(ctx, result)
