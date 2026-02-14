"""Rich table formatter for QueryResult output."""

from __future__ import annotations

import shutil
from io import StringIO
from typing import TYPE_CHECKING

from rich.console import Console
from rich.table import Table

from sql_tool.formatters.base import registry

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sql_tool.core.models import QueryResult

_NO_RESULTS = "No results"


def _truncate(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    return value[: width - 1] + "…"


class TableFormatter:
    def __init__(self, width: int = 40) -> None:
        self.width = width

    def format(self, result: QueryResult) -> Iterator[str]:
        if not result.rows:
            yield _NO_RESULTS
            return

        table = Table(show_edge=True, pad_edge=True)
        for col in result.columns:
            table.add_column(col.name, no_wrap=True)

        for row in result.rows:
            table.add_row(
                *(_truncate(str(v) if v is not None else "", self.width) for v in row)
            )

        buf = StringIO()
        term_width = shutil.get_terminal_size((120, 24)).columns
        console = Console(file=buf, force_terminal=True, width=term_width)
        console.print(table)
        yield buf.getvalue().rstrip("\n")


registry.register("table", TableFormatter)
