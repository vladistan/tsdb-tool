"""Tests for CSVFormatter (Phase 5, Step 5.4)."""

import csv
from io import StringIO

import pytest

from sql_tool.core.models import ColumnMeta, QueryResult
from sql_tool.formatters.base import Formatter
from sql_tool.formatters.csv import CSVFormatter


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
def test_csv_formatter_implements_protocol():
    assert isinstance(CSVFormatter(), Formatter)


@pytest.mark.unit
def test_csv_formatter_outputs_header_and_data():
    result = _make_result()
    lines = list(CSVFormatter().format(result))
    assert lines[0] == "id,name"
    assert lines[1] == "1,alice"
    assert lines[2] == "2,bob"


@pytest.mark.unit
def test_csv_formatter_no_header():
    result = _make_result()
    lines = list(CSVFormatter(no_header=True).format(result))
    assert len(lines) == 2
    assert lines[0] == "1,alice"


@pytest.mark.unit
def test_csv_formatter_escapes_commas():
    result = _make_result(rows=[(1, "last, first")])
    lines = list(CSVFormatter().format(result))
    # csv module should quote the field
    assert lines[1] == '1,"last, first"'


@pytest.mark.unit
def test_csv_formatter_escapes_quotes():
    result = _make_result(rows=[(1, 'he said "hi"')])
    lines = list(CSVFormatter().format(result))
    # csv module doubles quotes inside quoted fields
    assert lines[1] == '1,"he said ""hi"""'


@pytest.mark.unit
def test_csv_formatter_empty_result_header_only():
    result = _make_result(rows=[])
    lines = list(CSVFormatter().format(result))
    assert lines == ["id,name"]


@pytest.mark.unit
def test_csv_formatter_empty_result_no_header():
    result = _make_result(rows=[])
    lines = list(CSVFormatter(no_header=True).format(result))
    assert lines == []


@pytest.mark.unit
def test_csv_formatter_handles_none_values():
    result = _make_result(rows=[(1, None)])
    lines = list(CSVFormatter().format(result))
    assert lines[1] == "1,"


@pytest.mark.unit
def test_csv_formatter_rfc4180_valid():
    """Verify output is parseable as RFC 4180 CSV."""
    result = _make_result(rows=[(1, "alice"), (2, "bob, jr"), (3, 'say "hi"')])
    output = "\n".join(CSVFormatter().format(result))
    reader = csv.reader(StringIO(output))
    rows = list(reader)
    assert rows[0] == ["id", "name"]
    assert rows[1] == ["1", "alice"]
    assert rows[2] == ["2", "bob, jr"]
    assert rows[3] == ["3", 'say "hi"']
