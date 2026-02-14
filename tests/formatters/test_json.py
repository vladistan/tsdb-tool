"""Tests for JSONFormatter (Phase 5, Step 5.3)."""

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from tsdb_tool.core.models import ColumnMeta, QueryResult
from tsdb_tool.formatters.base import Formatter
from tsdb_tool.formatters.json import JSONFormatter


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
def test_json_formatter_implements_protocol():
    assert isinstance(JSONFormatter(), Formatter)


@pytest.mark.unit
def test_json_formatter_outputs_valid_json():
    result = _make_result()
    output = "\n".join(JSONFormatter().format(result))
    parsed = json.loads(output)
    assert isinstance(parsed, list)
    assert len(parsed) == 2


@pytest.mark.unit
def test_json_formatter_rows_as_dicts():
    result = _make_result()
    output = "\n".join(JSONFormatter().format(result))
    parsed = json.loads(output)
    assert parsed[0] == {"id": 1, "name": "alice"}
    assert parsed[1] == {"id": 2, "name": "bob"}


@pytest.mark.unit
def test_json_formatter_pretty_print_default():
    result = _make_result(rows=[(1, "alice")])
    output = "\n".join(JSONFormatter().format(result))
    # Pretty-print has indentation
    assert "\n" in output
    assert "  " in output


@pytest.mark.unit
def test_json_formatter_compact_mode():
    result = _make_result(rows=[(1, "alice")])
    output = "\n".join(JSONFormatter(compact=True).format(result))
    # Compact has no newlines within the JSON
    parsed = json.loads(output)
    assert parsed == [{"id": 1, "name": "alice"}]
    assert "\n" not in output


@pytest.mark.unit
def test_json_formatter_empty_result():
    result = _make_result(rows=[])
    output = "\n".join(JSONFormatter().format(result))
    parsed = json.loads(output)
    assert parsed == []


@pytest.mark.unit
def test_json_formatter_handles_none_values():
    result = _make_result(rows=[(1, None)])
    output = "\n".join(JSONFormatter().format(result))
    parsed = json.loads(output)
    assert parsed[0]["name"] is None


@pytest.mark.unit
def test_json_formatter_handles_special_types():
    dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
    result = _make_result(
        rows=[(dt, Decimal("123.45"))],
        columns=[
            ColumnMeta(name="ts", type_oid=1184, type_name="timestamptz"),
            ColumnMeta(name="amount", type_oid=1700, type_name="numeric"),
        ],
    )
    output = "\n".join(JSONFormatter().format(result))
    parsed = json.loads(output)
    # datetime and Decimal serialized as strings
    assert isinstance(parsed[0]["ts"], str)
    assert isinstance(parsed[0]["amount"], str)


@pytest.mark.unit
def test_json_formatter_handles_special_characters():
    result = _make_result(rows=[(1, 'he said "hello" & <bye>')])
    output = "\n".join(JSONFormatter().format(result))
    parsed = json.loads(output)
    assert parsed[0]["name"] == 'he said "hello" & <bye>'
