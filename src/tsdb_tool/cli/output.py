"""Output format selection and TTY auto-detection."""

from __future__ import annotations

import sys
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tsdb_tool.formatters.base import Formatter


class OutputFormat(StrEnum):
    TABLE = "table"
    JSON = "json"
    CSV = "csv"


def detect_tty() -> bool:
    return sys.stdout.isatty()


def resolve_format(format_flag: str | None) -> str:
    """Determine the output format.

    Explicit --format overrides TTY detection.
    Default: table for TTY, csv for pipes.
    """
    if format_flag is not None:
        return format_flag
    return "table" if detect_tty() else "csv"


def get_formatter(
    format_flag: str | None = None,
    *,
    compact: bool = False,
    width: int = 40,
    no_header: bool = False,
) -> Formatter:
    """Build and return the appropriate formatter instance."""
    # Import here to trigger registry population from formatter modules.
    import tsdb_tool.formatters.csv  # noqa: F401
    import tsdb_tool.formatters.json  # noqa: F401
    import tsdb_tool.formatters.table  # noqa: F401
    from tsdb_tool.formatters.base import registry

    fmt_name = resolve_format(format_flag)

    kwargs: dict[str, object] = {}
    if fmt_name == "table":
        kwargs["width"] = width
    elif fmt_name == "json":
        kwargs["compact"] = compact
    elif fmt_name == "csv":
        kwargs["no_header"] = no_header

    return registry.get(fmt_name, **kwargs)


def write_output(formatter: Formatter, result: object) -> None:
    """Write formatted output to stdout."""
    for line in formatter.format(result):  # type: ignore[arg-type]
        sys.stdout.write(line + "\n")
