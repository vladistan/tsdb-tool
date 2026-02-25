"""SQL Tool main entry point and command registration."""

from __future__ import annotations

import atexit
from pathlib import Path  # noqa: TC003
from typing import Annotated

import sentry_sdk
import typer

from tsdb_tool.__about__ import __version__
from tsdb_tool.cli.commands._shared import preprocess_optional_int_flags
from tsdb_tool.cli.commands.config import config_app
from tsdb_tool.cli.commands.connections import connections_command
from tsdb_tool.cli.commands.databases import databases_command
from tsdb_tool.cli.commands.live_tail import live_tail_command
from tsdb_tool.cli.commands.query import query_command
from tsdb_tool.cli.commands.schema import schema_command
from tsdb_tool.cli.commands.service import service_app
from tsdb_tool.cli.commands.table import table_command
from tsdb_tool.cli.commands.ts import ts_app
from tsdb_tool.cli.output import OutputFormat  # noqa: TC001
from tsdb_tool.core.exceptions import SqlToolError
from tsdb_tool.core.logging import setup_logging
from tsdb_tool.core.monitoring import setup_sentry


class SentryTestError(RuntimeError):
    """Test exception for validating Sentry integration."""


app = typer.Typer(
    help="SQL Tool - PostgreSQL query and administration tool",
    invoke_without_command=True,
)

app.add_typer(config_app, name="config")
app.add_typer(service_app, name="service")
app.add_typer(ts_app, name="ts")
app.command("query")(query_command)
app.command("live-tail")(live_tail_command)
app.command("databases")(databases_command)
app.command("schema")(schema_command)
app.command("table")(table_command)
app.command("connections")(connections_command)


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
        if transaction.status is None:
            transaction.set_status("ok")
        transaction.__exit__(None, None, None)
        sentry_sdk.flush(timeout=2)

    atexit.register(cleanup)

    ctx.ensure_object(dict)
    ctx.obj["_sentry_transaction"] = transaction
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

    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()


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
