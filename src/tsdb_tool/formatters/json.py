"""JSON formatter for QueryResult output."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from tsdb_tool.formatters.base import registry

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tsdb_tool.core.models import QueryResult


def _serialize_value(val: Any) -> Any:
    if isinstance(val, (int, float, str, bool, type(None))):
        return val
    return str(val)


class JSONFormatter:
    def __init__(self, compact: bool = False) -> None:
        self.compact = compact

    def format(self, result: QueryResult) -> Iterator[str]:
        rows_as_dicts = [
            {
                col.name: _serialize_value(val)
                for col, val in zip(result.columns, row, strict=True)
            }
            for row in result.rows
        ]

        if self.compact:
            yield json.dumps(rows_as_dicts, default=str)
        else:
            yield json.dumps(rows_as_dicts, indent=2, default=str)


registry.register("json", JSONFormatter)
