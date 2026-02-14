"""Shared CLI plumbing for command modules.

Client creation, format-option handling, and output helpers.
Distinct from cli.helpers which contains pure data-formatting functions.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from tsdb_tool.cli.helpers import fmt_size
from tsdb_tool.cli.output import get_formatter, resolve_format, write_output
from tsdb_tool.core.client import PgClient
from tsdb_tool.core.config import load_config, resolve_config

if TYPE_CHECKING:
    import typer

    from tsdb_tool.cli.output import OutputFormat
    from tsdb_tool.core.models import QueryResult


def get_client(ctx: typer.Context, timeout: float | None = None) -> PgClient:
    obj = ctx.ensure_object(dict)
    config = load_config(obj.get("config_file"))

    cli_overrides: dict[str, Any] = {}
    for key in ("host", "port", "database", "user", "password", "schema"):
        val = obj.get(key)
        if val is not None:
            cli_overrides[key] = val
    if timeout is not None:
        cli_overrides["timeout"] = timeout

    resolved = resolve_config(
        config,
        profile_name=obj.get("profile"),
        dsn=obj.get("dsn"),
        **cli_overrides,
    )

    return PgClient(resolved)


def format_options(ctx: typer.Context) -> dict[str, Any]:
    obj = ctx.ensure_object(dict)
    return {
        "format_flag": obj.get("format"),
        "compact": obj.get("compact", False),
        "width": obj.get("width", 40),
        "no_header": obj.get("no_header", False),
    }


def output_result(ctx: typer.Context, result: QueryResult) -> None:
    opts = format_options(ctx)
    formatter = get_formatter(**opts)
    write_output(formatter, result)


def apply_local_format_options(
    ctx: typer.Context,
    *,
    format: OutputFormat | None = None,
    table: bool = False,
    compact: bool = False,
    width: int | None = None,
    no_header: bool = False,
) -> None:
    if format is not None or table or compact or width is not None or no_header:
        obj = ctx.ensure_object(dict)
        if format is not None:
            obj["format"] = format.value
        if table:
            obj["format"] = "table"
        if compact:
            obj["compact"] = compact
        if width is not None:
            obj["width"] = width
        if no_header:
            obj["no_header"] = no_header


def is_table_format(ctx: typer.Context) -> bool:
    obj = ctx.ensure_object(dict)
    return resolve_format(obj.get("format")) == "table"


def size_formatter(ctx: typer.Context) -> tuple[Any, bool]:
    is_tbl = is_table_format(ctx)
    fmt = fmt_size if is_tbl else (lambda b: str(b or 0))
    return fmt, is_tbl


def parse_table_arg(table_arg: str) -> tuple[str, str]:
    if "." in table_arg:
        schema, table = table_arg.split(".", 1)
        return schema, table
    return "public", table_arg


def preprocess_optional_int_flags() -> None:
    """Insert default '10' after bare --head/--tail/--sample when used without a value.

    Typer 0.23 doesn't support Click's flag_value parameter, so these options
    always require an explicit argument. This preprocessor lets users write
    ``--head`` instead of ``--head 10`` by inserting the default before Typer
    parses argv.
    """
    if "table" not in sys.argv:
        return
    flags = {"--head", "--tail", "--sample"}
    new_argv: list[str] = []
    i = 0
    while i < len(sys.argv):
        new_argv.append(sys.argv[i])
        if sys.argv[i] in flags:
            next_is_value = i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith(
                "-"
            )
            if not next_is_value:
                new_argv.append("10")
        i += 1
    sys.argv = new_argv
