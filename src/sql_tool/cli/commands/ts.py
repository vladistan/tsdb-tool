"""TimescaleDB administration commands.

Thin CLI layer: typer decorators, argument parsing, output formatting.
Business logic delegated to core.timescaledb module.
"""

from __future__ import annotations

from typing import Annotated, Any

import typer

from sql_tool.cli.commands._shared import (
    apply_local_format_options,
    get_client,
    is_table_format,
    output_result,
    parse_table_arg,
)
from sql_tool.cli.helpers import (
    format_relative_time,
    format_size_compact,
    format_size_gb,
    format_timestamp,
    normalize_pg_interval,
)
from sql_tool.cli.output import OutputFormat
from sql_tool.core.models import ColumnMeta, QueryResult
from sql_tool.core.timescaledb import (
    add_compression_policy,
    alter_compression_settings,
    check_timescaledb_available,
    compress_single_chunk,
    compression_stats,
    count_compressed_chunks,
    list_chunk_info,
    list_chunks,
    list_compressed_chunk_names,
    list_compression_settings,
    list_continuous_aggregates,
    list_hypertables,
    list_job_history,
    list_jobs,
    list_refresh_status,
    list_retention_policies,
    parse_chunk_id,
    recompress_chunk,
    remove_compression_policy,
    set_chunk_time_interval,
    set_compression_enabled,
)

ts_app = typer.Typer(help="TimescaleDB administration commands")


@ts_app.callback(invoke_without_command=True)
def ts_callback(
    ctx: typer.Context,
    database: Annotated[
        str | None,
        typer.Option("--database", "-d", help="Database name"),
    ] = None,
) -> None:
    if database is not None:
        ctx.ensure_object(dict)["database"] = database
    if not ctx.invoked_subcommand:
        typer.echo(ctx.get_help())
        raise typer.Exit()


@ts_app.command("hypertables")
def hypertables_command(
    ctx: typer.Context,
    schema_filter: Annotated[
        str | None,
        typer.Option("--schema", "-s", help="Filter by schema"),
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
    """List hypertables with size, chunk counts (compressed/uncompressed), and compression info."""
    apply_local_format_options(
        ctx,
        format=format,
        table=table,
        compact=compact,
        width=width,
        no_header=no_header,
    )

    effective_schema = schema_filter or ctx.ensure_object(dict).get("schema")

    with get_client(ctx) as client:
        check_timescaledb_available(client)
        raw = list_hypertables(client, schema_filter=effective_schema)

    is_tbl = is_table_format(ctx)
    fmt_size = format_size_compact if is_tbl else (lambda b: str(b or 0))

    total_size = 0
    total_uncompr = 0
    total_compr = 0
    total_before = 0
    total_after = 0

    rows: list[tuple[Any, ...]] = []
    for (
        schema,
        tbl_name,
        col,
        interval,
        size,
        uncompr_c,
        compr_c,
        before_bytes,
        after_bytes,
        compr_enabled,
    ) in raw.rows:
        total_size += size or 0
        total_uncompr += uncompr_c or 0
        total_compr += compr_c or 0
        total_before += before_bytes or 0
        total_after += after_bytes or 0
        uncompr_size = (size or 0) - (after_bytes or 0) if after_bytes else (size or 0)
        rows.append(
            (
                schema,
                tbl_name,
                col,
                normalize_pg_interval(interval),
                fmt_size(size),
                uncompr_c,
                compr_c,
                fmt_size(uncompr_size) if uncompr_size else "-",
                fmt_size(before_bytes) if before_bytes else "-",
                fmt_size(after_bytes) if after_bytes else "-",
                compr_enabled,
            )
        )

    total_uncompr_size = total_size - total_after
    rows.append(
        (
            "TOTAL",
            "",
            "",
            "",
            fmt_size(total_size),
            total_uncompr,
            total_compr,
            fmt_size(total_uncompr_size) if total_uncompr_size else "-",
            fmt_size(total_before) if total_before else "-",
            fmt_size(total_after) if total_after else "-",
            "",
        )
    )

    result = QueryResult(
        columns=[
            ColumnMeta(name="schema", type_oid=25, type_name="text"),
            ColumnMeta(name="table", type_oid=25, type_name="text"),
            ColumnMeta(name="time_col", type_oid=25, type_name="text"),
            ColumnMeta(name="chunk_iv", type_oid=25, type_name="text"),
            ColumnMeta(name="size", type_oid=25, type_name="text"),
            ColumnMeta(name="uncompr", type_oid=20, type_name="int8"),
            ColumnMeta(name="compr", type_oid=20, type_name="int8"),
            ColumnMeta(name="uncompr_size", type_oid=25, type_name="text"),
            ColumnMeta(name="before_size", type_oid=25, type_name="text"),
            ColumnMeta(name="after_size", type_oid=25, type_name="text"),
            ColumnMeta(name="compr_on", type_oid=16, type_name="bool"),
        ],
        rows=rows,
        row_count=len(rows),
        status_message=raw.status_message,
    )
    output_result(ctx, result)


@ts_app.command("chunks")
def chunks_command(
    ctx: typer.Context,
    hypertable: Annotated[
        str, typer.Argument(help="Hypertable name (schema.table or table)")
    ],
    format: Annotated[
        str | None,
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
    """Show chunks for a hypertable with size and compression status.

    Supports schema-qualified names like 'public.metrics' or just 'metrics'."""
    apply_local_format_options(
        ctx,
        format=OutputFormat(format) if format else None,
        table=table,
        compact=compact,
        width=width,
        no_header=no_header,
    )

    schema_name, table_name = parse_table_arg(hypertable)

    with get_client(ctx) as client:
        check_timescaledb_available(client)
        raw = list_chunks(client, schema_name, table_name)

    is_tbl = is_table_format(ctx)
    fmt_size = format_size_compact if is_tbl else (lambda b: str(b or 0))

    total_bytes = 0
    uncompr_count = 0
    compr_count = 0
    uncompr_bytes = 0
    compr_bytes = 0

    rows: list[tuple[Any, ...]] = []
    for chunk_name, range_start, range_end, is_compressed, size_bytes in raw.rows:
        total_bytes += size_bytes or 0
        if is_compressed:
            compr_count += 1
            compr_bytes += size_bytes or 0
        else:
            uncompr_count += 1
            uncompr_bytes += size_bytes or 0
        rows.append(
            (chunk_name, range_start, range_end, is_compressed, fmt_size(size_bytes))
        )

    rows.append(("TOTAL", "", "", "", fmt_size(total_bytes)))

    result = QueryResult(
        columns=[
            ColumnMeta(name="chunk_name", type_oid=25, type_name="text"),
            ColumnMeta(name="range_start", type_oid=25, type_name="text"),
            ColumnMeta(name="range_end", type_oid=25, type_name="text"),
            ColumnMeta(name="is_compressed", type_oid=16, type_name="bool"),
            ColumnMeta(name="size", type_oid=25, type_name="text"),
        ],
        rows=rows,
        row_count=len(rows),
        status_message=raw.status_message,
    )
    output_result(ctx, result)

    if is_tbl:
        typer.echo(
            f"\nUncompressed: {uncompr_count} chunks, {format_size_compact(uncompr_bytes)}\n"
            f"Compressed: {compr_count} chunks, {format_size_compact(compr_bytes)}\n"
            f"Total: {uncompr_count + compr_count} chunks, {format_size_compact(total_bytes)}",
            err=True,
        )


@ts_app.command("compression")
def compression_command(
    ctx: typer.Context,
    hypertable: Annotated[
        str | None,
        typer.Argument(help="Hypertable name (schema.table or table). Omit for all."),
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
    """Show compression statistics per hypertable with compressed/uncompressed sizes."""
    apply_local_format_options(
        ctx,
        format=format,
        table=table,
        compact=compact,
        width=width,
        no_header=no_header,
    )

    schema_name = table_name = None
    if hypertable:
        schema_name, table_name = parse_table_arg(hypertable)

    with get_client(ctx) as client:
        check_timescaledb_available(client)
        raw = compression_stats(client, schema=schema_name, table=table_name)

    fmt_size = format_size_compact if is_table_format(ctx) else format_size_gb
    rows = [
        (schema, tbl, total, compr, fmt_size(before), fmt_size(after))
        for schema, tbl, total, compr, before, after in raw.rows
    ]

    result = QueryResult(
        columns=[
            ColumnMeta(name="schema", type_oid=25, type_name="text"),
            ColumnMeta(name="table", type_oid=25, type_name="text"),
            ColumnMeta(name="chunks", type_oid=20, type_name="int8"),
            ColumnMeta(name="compr", type_oid=20, type_name="int8"),
            ColumnMeta(name="before", type_oid=25, type_name="text"),
            ColumnMeta(name="after", type_oid=25, type_name="text"),
        ],
        rows=rows,
        row_count=len(rows),
        status_message=raw.status_message,
    )
    output_result(ctx, result)


@ts_app.command("caggs")
def caggs_command(
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
    """List continuous aggregates with source hypertable and materialization details."""
    apply_local_format_options(
        ctx,
        format=format,
        table=table,
        compact=compact,
        width=width,
        no_header=no_header,
    )

    with get_client(ctx) as client:
        check_timescaledb_available(client)
        result = list_continuous_aggregates(client)

    output_result(ctx, result)


@ts_app.command("retention")
def retention_command(
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
    """List retention policies with drop interval, schedule, and execution times."""
    apply_local_format_options(
        ctx,
        format=format,
        table=table,
        compact=compact,
        width=width,
        no_header=no_header,
    )

    with get_client(ctx) as client:
        check_timescaledb_available(client)
        result = list_retention_policies(client)

    output_result(ctx, result)


@ts_app.command("refresh-status")
def refresh_status_command(
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
    """Refresh schedule, last run status, and execution statistics for each continuous aggregate."""
    apply_local_format_options(
        ctx,
        format=format,
        table=table,
        compact=compact,
        width=width,
        no_header=no_header,
    )

    with get_client(ctx) as client:
        check_timescaledb_available(client)
        result = list_refresh_status(client)

    output_result(ctx, result)


@ts_app.command("jobs")
def jobs_command(
    ctx: typer.Context,
    history: Annotated[
        bool,
        typer.Option("--history", help="Show job execution history (last 20 runs)"),
    ] = False,
    job_id: Annotated[
        int | None,
        typer.Option("--job", help="Filter by job ID (use with --history)"),
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
    """Show all TimescaleDB background jobs with schedule and execution stats."""
    apply_local_format_options(
        ctx,
        format=format,
        table=table,
        compact=compact,
        width=width,
        no_header=no_header,
    )
    if history:
        _jobs_history_output(ctx, job_id)
        return

    with get_client(ctx) as client:
        check_timescaledb_available(client)
        raw = list_jobs(client)

    is_tbl = is_table_format(ctx)

    if is_tbl:
        rows = [
            (
                jid,
                app_name,
                hypertable_val,
                normalize_pg_interval(schedule),
                format_relative_time(last_run_secs),
                status,
                format_relative_time(-next_start_secs)
                if next_start_secs and next_start_secs < 0
                else (
                    f"in {format_relative_time(next_start_secs).replace(' ago', '')}"
                    if next_start_secs
                    else "-"
                ),
                total_runs,
                total_successes,
                total_failures,
            )
            for jid, app_name, hypertable_val, schedule, _last_run, last_run_secs, status, _next_start, next_start_secs, total_runs, total_successes, total_failures in raw.rows
        ]
    else:
        rows = [
            (
                jid,
                app_name,
                hypertable_val,
                normalize_pg_interval(schedule),
                format_timestamp(last_run),
                status,
                format_timestamp(next_start),
                total_runs,
                total_successes,
                total_failures,
            )
            for jid, app_name, hypertable_val, schedule, last_run, _last_run_secs, status, next_start, _next_start_secs, total_runs, total_successes, total_failures in raw.rows
        ]

    result = QueryResult(
        columns=[
            ColumnMeta(name="job_id", type_oid=23, type_name="int4"),
            ColumnMeta(name="application_name", type_oid=25, type_name="text"),
            ColumnMeta(name="hypertable", type_oid=25, type_name="text"),
            ColumnMeta(name="schedule", type_oid=25, type_name="text"),
            ColumnMeta(name="last_run", type_oid=25, type_name="text"),
            ColumnMeta(name="last_run_status", type_oid=25, type_name="text"),
            ColumnMeta(name="next_start", type_oid=25, type_name="text"),
            ColumnMeta(name="total_runs", type_oid=20, type_name="int8"),
            ColumnMeta(name="total_successes", type_oid=20, type_name="int8"),
            ColumnMeta(name="total_failures", type_oid=20, type_name="int8"),
        ],
        rows=rows,
        row_count=len(rows),
        status_message=raw.status_message,
    )
    output_result(ctx, result)


def _jobs_history_output(ctx: typer.Context, job_id: int | None) -> None:
    with get_client(ctx) as client:
        check_timescaledb_available(client)
        try:
            raw = list_job_history(client, job_id=job_id)
        except Exception:
            typer.echo("No job history table found.", err=True)
            raise typer.Exit(1) from None

    rows = [
        (
            jid,
            succeeded,
            format_timestamp(exec_start),
            format_timestamp(exec_finish),
            error_data[:200] if error_data else "-",
        )
        for jid, succeeded, exec_start, exec_finish, error_data in raw.rows
    ]

    result = QueryResult(
        columns=[
            ColumnMeta(name="job_id", type_oid=23, type_name="int4"),
            ColumnMeta(name="succeeded", type_oid=16, type_name="bool"),
            ColumnMeta(name="execution_start", type_oid=25, type_name="text"),
            ColumnMeta(name="execution_finish", type_oid=25, type_name="text"),
            ColumnMeta(name="error_data", type_oid=25, type_name="text"),
        ],
        rows=rows,
        row_count=len(rows),
        status_message=raw.status_message,
    )
    output_result(ctx, result)


@ts_app.command("compression-settings")
def compression_settings_command(
    ctx: typer.Context,
    hypertable: Annotated[
        str | None,
        typer.Argument(help="Hypertable name (schema.table or table). Omit for all."),
    ] = None,
    schema_filter: Annotated[
        str | None,
        typer.Option("--schema", "-s", help="Filter by schema"),
    ] = None,
    no_policy: Annotated[
        bool,
        typer.Option("--no-policy", help="Hide policy columns (show settings only)"),
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
    """Show compression settings and policy info for hypertables."""
    apply_local_format_options(
        ctx,
        format=format,
        table=table,
        compact=compact,
        width=width,
        no_header=no_header,
    )

    schema_name = table_name = None
    if hypertable:
        schema_name, table_name = parse_table_arg(hypertable)

    eff_schema = schema_filter or ctx.ensure_object(dict).get("schema")

    with get_client(ctx) as client:
        check_timescaledb_available(client)
        raw = list_compression_settings(
            client,
            schema=schema_name,
            table=table_name,
            effective_schema=eff_schema if not hypertable else None,
            include_policy=not no_policy,
        )

    rows: list[tuple[Any, ...]]
    if no_policy:
        rows = [
            (
                schema,
                tbl,
                time_col or "-",
                normalize_pg_interval(chunk_iv) if chunk_iv else "-",
                segment_by or "-",
                order_by or "-",
            )
            for schema, tbl, time_col, chunk_iv, segment_by, order_by in raw.rows
        ]
        columns = [
            ColumnMeta(name="schema", type_oid=25, type_name="text"),
            ColumnMeta(name="table", type_oid=25, type_name="text"),
            ColumnMeta(name="time_col", type_oid=25, type_name="text"),
            ColumnMeta(name="chunk_iv", type_oid=25, type_name="text"),
            ColumnMeta(name="seg_by", type_oid=25, type_name="text"),
            ColumnMeta(name="order_by", type_oid=25, type_name="text"),
        ]
    else:
        rows = [
            (
                schema,
                tbl,
                time_col or "-",
                normalize_pg_interval(chunk_iv) if chunk_iv else "-",
                segment_by or "-",
                order_by or "-",
                normalize_pg_interval(compress_after) if compress_after else "-",
                normalize_pg_interval(sched) if sched else "-",
                format_timestamp(last_run),
                format_timestamp(next_start),
                status or "-",
            )
            for schema, tbl, time_col, chunk_iv, segment_by, order_by, compress_after, sched, last_run, next_start, status in raw.rows
        ]
        columns = [
            ColumnMeta(name="schema", type_oid=25, type_name="text"),
            ColumnMeta(name="table", type_oid=25, type_name="text"),
            ColumnMeta(name="time_col", type_oid=25, type_name="text"),
            ColumnMeta(name="chunk_iv", type_oid=25, type_name="text"),
            ColumnMeta(name="seg_by", type_oid=25, type_name="text"),
            ColumnMeta(name="order_by", type_oid=25, type_name="text"),
            ColumnMeta(name="compress", type_oid=25, type_name="text"),
            ColumnMeta(name="sched", type_oid=25, type_name="text"),
            ColumnMeta(name="last_run", type_oid=25, type_name="text"),
            ColumnMeta(name="next_start", type_oid=25, type_name="text"),
            ColumnMeta(name="status", type_oid=25, type_name="text"),
        ]

    result = QueryResult(
        columns=columns,
        rows=rows,
        row_count=len(rows),
        status_message=raw.status_message,
    )
    output_result(ctx, result)


@ts_app.command("compression-set")
def compression_set_command(
    ctx: typer.Context,
    hypertable: Annotated[
        str, typer.Argument(help="Hypertable name (schema.table or table)")
    ],
    segmentby: Annotated[
        str | None,
        typer.Option("--segmentby", help="Comma-separated segment_by columns"),
    ] = None,
    orderby: Annotated[
        str | None,
        typer.Option(
            "--orderby", help="Comma-separated order_by columns (e.g. 'timestamp DESC')"
        ),
    ] = None,
    enable: Annotated[
        bool,
        typer.Option("--enable", help="Enable compression on the hypertable"),
    ] = False,
    disable: Annotated[
        bool,
        typer.Option("--disable", help="Disable compression on the hypertable"),
    ] = False,
    policy: Annotated[
        str | None,
        typer.Option(
            "--policy", help="Set compression policy interval (e.g. '1 hour', '7 days')"
        ),
    ] = None,
    schedule: Annotated[
        str | None,
        typer.Option(
            "--schedule", help="Policy schedule interval (default: TimescaleDB default)"
        ),
    ] = None,
    remove_policy_flag: Annotated[
        bool,
        typer.Option("--remove-policy", help="Remove the compression policy"),
    ] = False,
    chunk_interval: Annotated[
        str | None,
        typer.Option(
            "--chunk-interval", help="Set chunk interval (e.g. '4 hours', '1 day')"
        ),
    ] = None,
) -> None:
    """Configure compression settings, policies, and chunk interval for a hypertable."""
    if enable and disable:
        typer.echo("Error: --enable and --disable are mutually exclusive.", err=True)
        raise typer.Exit(1)

    if policy and remove_policy_flag:
        typer.echo(
            "Error: --policy and --remove-policy are mutually exclusive.", err=True
        )
        raise typer.Exit(1)

    has_action = any(
        [
            segmentby,
            orderby,
            enable,
            disable,
            policy,
            remove_policy_flag,
            chunk_interval,
        ]
    )
    if not has_action:
        typer.echo(
            "Error: Provide at least one of --segmentby, --orderby, --enable, --disable, "
            "--policy, --remove-policy, --chunk-interval.",
            err=True,
        )
        raise typer.Exit(1)

    schema_name, table_name = parse_table_arg(hypertable)
    fqn = f"{schema_name}.{table_name}"

    typer.echo(f"Applying to {fqn}:", err=True)
    if segmentby:
        typer.echo(f"  segment_by: {segmentby}", err=True)
    if orderby:
        typer.echo(f"  order_by: {orderby}", err=True)
    if enable:
        typer.echo("  compression: enable", err=True)
    if disable:
        typer.echo("  compression: disable", err=True)
    if policy:
        typer.echo(f"  policy: compress_after {policy}", err=True)
    if schedule:
        typer.echo(f"  schedule: {schedule}", err=True)
    if remove_policy_flag:
        typer.echo("  policy: remove", err=True)
    if chunk_interval:
        typer.echo(f"  chunk_interval: {chunk_interval}", err=True)
    if segmentby or orderby:
        typer.echo("Warning: Existing compressed chunks keep old settings.", err=True)
    if chunk_interval:
        typer.echo("Note: Chunk interval only affects new chunks.", err=True)

    if not typer.confirm("Proceed?"):
        raise typer.Abort()

    with get_client(ctx) as client:
        check_timescaledb_available(client)

        if segmentby or orderby:
            alter_compression_settings(
                client, fqn, segmentby=segmentby, orderby=orderby
            )

        if enable:
            set_compression_enabled(client, fqn, enabled=True)
        elif disable:
            set_compression_enabled(client, fqn, enabled=False)

        if policy:
            job_result = add_compression_policy(
                client,
                schema_name,
                table_name,
                policy,
                schedule=schedule,
            )
            if job_result:
                typer.echo(f"  Policy set (job_id: {job_result})", err=True)

        if remove_policy_flag:
            remove_compression_policy(client, schema_name, table_name)

        if chunk_interval:
            set_chunk_time_interval(client, schema_name, table_name, chunk_interval)

    typer.echo(f"Done. Settings applied to {fqn}.", err=True)


@ts_app.command("recompress")
def recompress_command(
    ctx: typer.Context,
    hypertable: Annotated[
        str, typer.Argument(help="Hypertable name (schema.table or table)")
    ],
) -> None:
    """Decompress and recompress all chunks with current settings."""
    schema_name, table_name = parse_table_arg(hypertable)
    fqn = f"{schema_name}.{table_name}"

    with get_client(ctx) as client:
        check_timescaledb_available(client)
        chunk_count = count_compressed_chunks(client, schema_name, table_name)

    if chunk_count == 0:
        typer.echo(f"No compressed chunks found for {fqn}.", err=True)
        raise typer.Exit()

    typer.echo(
        f"This will decompress and recompress {chunk_count} chunks in {fqn}.\n"
        "This is an expensive operation.",
        err=True,
    )
    if not typer.confirm("Proceed?"):
        raise typer.Abort()

    with get_client(ctx, timeout=600) as client:
        chunk_names = list_compressed_chunk_names(client, schema_name, table_name)

        for i, chunk_name in enumerate(chunk_names, 1):
            typer.echo(
                f"Recompressing chunk {i}/{chunk_count}: {chunk_name}...", err=True
            )
            recompress_chunk(client, chunk_name)

    typer.echo(f"Recompressed {chunk_count} chunks in {fqn}.", err=True)


@ts_app.command("compress")
def compress_command(
    ctx: typer.Context,
    hypertable: Annotated[
        str, typer.Argument(help="Hypertable name (schema.table or table)")
    ],
    chunk: Annotated[
        int | None,
        typer.Option(
            "--chunk",
            help="Compress specific chunk by ID (e.g. 11420 from _hyper_16_11420_chunk)",
        ),
    ] = None,
) -> None:
    """Compress chunks for a hypertable.

    Without --chunk, compresses all uncompressed chunks except the latest (active) one.
    With --chunk ID, compresses a specific chunk.
    """
    schema_name, table_name = parse_table_arg(hypertable)

    timeout = 300 if chunk is not None else 600

    with get_client(ctx, timeout=timeout) as client:
        check_timescaledb_available(client)
        chunks = list_chunk_info(client, schema_name, table_name)

        if chunk is not None:
            _compress_specific_chunk(client, chunks, chunk, schema_name, table_name)
        else:
            _compress_all_uncompressed(client, chunks)


def _compress_specific_chunk(
    client: Any,
    chunks: list[tuple[str, bool]],
    chunk_id: int,
    schema_name: str,
    table_name: str,
) -> None:
    chunk_map: dict[int, tuple[str, bool]] = {}
    for name, compressed in chunks:
        cid = parse_chunk_id(name)
        if cid is not None:
            chunk_map[cid] = (name, compressed)

    if chunk_id not in chunk_map:
        typer.echo(
            f"Error: Chunk ID {chunk_id} not found for {schema_name}.{table_name}.",
            err=True,
        )
        raise typer.Exit(1)

    chunk_name, is_compressed = chunk_map[chunk_id]
    if is_compressed:
        typer.echo(f"Chunk {chunk_name} is already compressed.", err=True)
        raise typer.Exit()

    typer.echo(f"Compressing {chunk_name}...", err=True)
    compress_single_chunk(client, chunk_name)
    typer.echo(f"Compressed {chunk_name}.", err=True)


def _compress_all_uncompressed(
    client: Any,
    chunks: list[tuple[str, bool]],
) -> None:
    if not chunks:
        typer.echo("No chunks found.", err=True)
        raise typer.Exit()

    candidates = chunks[:-1]
    to_compress = [
        (name, idx)
        for idx, (name, compressed) in enumerate(candidates)
        if not compressed
    ]

    if not to_compress:
        typer.echo(
            "No uncompressed chunks to compress (excluding active chunk).", err=True
        )
        raise typer.Exit()

    typer.echo(
        f"Compressing {len(to_compress)} uncompressed chunks "
        f"(excluding latest active chunk)...",
        err=True,
    )

    for i, (chunk_name, _) in enumerate(to_compress, 1):
        typer.echo(
            f"Compressing chunk {i}/{len(to_compress)}: {chunk_name}...", err=True
        )
        compress_single_chunk(client, chunk_name)
