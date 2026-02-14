"""CSV formatter for QueryResult output (RFC 4180 compliant)."""

from __future__ import annotations

import csv
from io import StringIO
from typing import TYPE_CHECKING

from sql_tool.formatters.base import registry

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sql_tool.core.models import QueryResult


def _write_row(values: list[str]) -> str:
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(values)
    return buf.getvalue().rstrip("\r\n")


class CSVFormatter:
    def __init__(self, no_header: bool = False) -> None:
        self.no_header = no_header

    def format(self, result: QueryResult) -> Iterator[str]:
        if not self.no_header:
            yield _write_row([col.name for col in result.columns])

        for row in result.rows:
            yield _write_row([str(v) if v is not None else "" for v in row])


registry.register("csv", CSVFormatter)
