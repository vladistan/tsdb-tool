"""Tests for TableFormatter (Phase 5, Step 5.2)."""

import pytest

from sql_tool.core.models import ColumnMeta, QueryResult
from sql_tool.formatters.base import Formatter
from sql_tool.formatters.table import TableFormatter


def _make_result(rows=None, columns=None):
    if columns is None:
        columns = [
            ColumnMeta(name="id", type_oid=23, type_name="int4"),
            ColumnMeta(name="name", type_oid=25, type_name="text"),
        ]
    if rows is None:
        rows = [(1, "alice"), (2, "bob")]
    return QueryResult(
        columns=columns,
        rows=rows,
        row_count=len(rows),
        status_message=f"SELECT {len(rows)}",
    )


@pytest.mark.unit
def test_table_formatter_implements_protocol():
    assert isinstance(TableFormatter(), Formatter)


@pytest.mark.unit
def test_table_formatter_outputs_column_headers():
    result = _make_result()
    output = "\n".join(TableFormatter().format(result))
    assert "id" in output
    assert "name" in output


@pytest.mark.unit
def test_table_formatter_outputs_row_values():
    result = _make_result()
    output = "\n".join(TableFormatter().format(result))
    assert "alice" in output
    assert "bob" in output


@pytest.mark.unit
def test_table_formatter_empty_result_shows_no_results():
    result = _make_result(rows=[])
    lines = list(TableFormatter().format(result))
    assert lines == ["No results"]


@pytest.mark.unit
def test_table_formatter_truncates_wide_values():
    long_val = "x" * 60
    result = _make_result(
        rows=[(1, long_val)],
        columns=[
            ColumnMeta(name="id", type_oid=23, type_name="int4"),
            ColumnMeta(name="val", type_oid=25, type_name="text"),
        ],
    )
    output = "\n".join(TableFormatter(width=20).format(result))
    # Should be truncated with ellipsis, not the full 60-char string
    assert long_val not in output
    assert "…" in output


@pytest.mark.unit
def test_table_formatter_respects_custom_width():
    result = _make_result(rows=[(1, "short")])
    fmt = TableFormatter(width=80)
    assert fmt.width == 80
    output = "\n".join(fmt.format(result))
    assert "short" in output


@pytest.mark.unit
def test_table_formatter_handles_none_values():
    result = _make_result(rows=[(1, None)])
    output = "\n".join(TableFormatter().format(result))
    # None should render as empty string, not "None"
    assert "None" not in output


@pytest.mark.unit
def test_table_formatter_single_column():
    result = _make_result(
        rows=[(42,)],
        columns=[ColumnMeta(name="count", type_oid=23, type_name="int4")],
    )
    output = "\n".join(TableFormatter().format(result))
    assert "count" in output
    assert "42" in output
