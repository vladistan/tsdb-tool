from __future__ import annotations

from typing import Annotated

import sentry_sdk
import structlog
import typer

from tsdb_tool.cli.commands._shared import get_client
from tsdb_tool.core.exceptions import InputError
from tsdb_tool.core.exit_codes import ExitCode
from tsdb_tool.core.live_tail import (
    LiveTailConfig,
    TableInfo,
    TailSummary,
    detect_liveness,
    fetch_watermarks,
    poll_tables,
    resolve_tables,
)


def live_tail_command(
    ctx: typer.Context,
    tables: Annotated[
        list[str] | None,
        typer.Argument(help="Table names to monitor (schema.table or table)"),
    ] = None,
    interval: Annotated[
        int,
        typer.Option("--interval", "-i", help="Polling interval in seconds"),
    ] = 30,
    duration: Annotated[
        int,
        typer.Option(
            "--duration", "-D", help="Total duration in seconds (0 = indefinite)"
        ),
    ] = 180,
    full: Annotated[
        bool,
        typer.Option("--full", help="Full query mode: show actual row data"),
    ] = False,
    columns: Annotated[
        str | None,
        typer.Option(
            "--columns", "-c", help="Comma-separated column list for full mode"
        ),
    ] = None,
    schema: Annotated[
        str | None,
        typer.Option("--schema", "-s", help="Schema to use for auto-discovery"),
    ] = None,
) -> None:
    """Monitor real-time data ingestion by polling table row counts at regular intervals."""
    log = structlog.get_logger()
    obj = ctx.ensure_object(dict)
    schema_name = schema or obj.get("schema") or "public"

    column_list = columns.split(",") if columns else None

    config = LiveTailConfig(
        schema=schema_name,
        tables=tables or [],
        interval=interval,
        duration=duration,
        full=full,
        columns=column_list,
    )

    with get_client(ctx) as client:
        with sentry_sdk.start_span(
            op="discovery", description="Resolve tables and detect liveness"
        ):
            try:
                resolved_tables = resolve_tables(client, config)
            except InputError as exc:
                log.error("input error", error=str(exc))
                raise typer.Exit(ExitCode.INPUT_ERROR) from exc

            if not resolved_tables:
                log.error("no tables with timestamp columns found", schema=schema_name)
                raise typer.Exit(ExitCode.INPUT_ERROR)

            print_banner(resolved_tables, config)

            typer.echo("Detecting liveness...", err=True)
            is_alive = detect_liveness(
                client,
                resolved_tables,
                probe_interval=config.liveness_probe_interval,
                timeout=config.liveness_timeout,
            )

            if not is_alive:
                log.warning(
                    "schema appears inactive -- no timestamp movement detected",
                    timeout_sec=config.liveness_timeout,
                )
                raise typer.Exit(ExitCode.SUCCESS)

        typer.echo("Establishing baseline...", err=True)
        with sentry_sdk.start_span(
            op="baseline", description="Fetch initial watermarks"
        ):
            initial_watermarks = fetch_watermarks(client, resolved_tables)

        # Run polling loop
        summary = poll_tables(
            client, resolved_tables, config, initial_watermarks, ctx=ctx
        )
        print_summary(summary)
        raise typer.Exit(ExitCode.SUCCESS)


def print_banner(tables: list[TableInfo], config: LiveTailConfig) -> None:
    schema_name = config.schema
    n_tables = len(tables)

    typer.echo(f'Monitoring {n_tables} tables in schema "{schema_name}":', err=True)
    for table_info in tables:
        typer.echo(
            f"  - {table_info.fqn} (time_column: {table_info.time_column})", err=True
        )

    interval_str = f"{config.interval}s"
    duration_str = "indefinite" if config.duration == 0 else f"{config.duration}s"
    mode_str = "full" if config.full else "count-only"

    typer.echo(
        f"Interval: {interval_str} | Duration: {duration_str} | Mode: {mode_str}",
        err=True,
    )


def print_summary(summary: TailSummary) -> None:
    typer.echo("\n--- Live Tail Summary ---", err=True)
    mins, secs = divmod(summary.elapsed_seconds, 60)
    duration_str = f"{mins}m {secs}s" if mins > 0 else f"{secs}s"
    typer.echo(f"Duration: {duration_str}", err=True)
    typer.echo(f"Tables monitored: {summary.tables_monitored}", err=True)

    for fqn, count in summary.per_table.items():
        typer.echo(f"  {fqn}: {count:,} rows", err=True)

    typer.echo(f"Total: {summary.total_rows:,} rows", err=True)
