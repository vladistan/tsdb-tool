"""Tests for Formatter protocol and registry (Phase 5, Step 5.1)."""

import pytest

from sql_tool.core.models import ColumnMeta, QueryResult
from sql_tool.formatters.base import Formatter, FormatterRegistry


def _make_result(rows=None, columns=None):
    if columns is None:
        columns = [ColumnMeta(name="id", type_oid=23, type_name="int4")]
    if rows is None:
        rows = [(1,)]
    return QueryResult(
        columns=columns,
        rows=rows,
        row_count=len(rows),
        status_message=f"SELECT {len(rows)}",
    )


class _StubFormatter:
    def format(self, result):
        for row in result.rows:
            yield str(row)


class _BadFormatter:
    """Missing format method."""

    pass


@pytest.mark.unit
def test_stub_formatter_implements_protocol():
    assert isinstance(_StubFormatter(), Formatter)


@pytest.mark.unit
def test_bad_formatter_does_not_implement_protocol():
    assert not isinstance(_BadFormatter(), Formatter)


@pytest.mark.unit
def test_formatter_yields_strings():
    fmt = _StubFormatter()
    result = _make_result(rows=[(1,), (2,)])
    lines = list(fmt.format(result))
    assert lines == ["(1,)", "(2,)"]


@pytest.mark.unit
def test_registry_register_and_get():
    reg = FormatterRegistry()
    reg.register("stub", _StubFormatter)
    fmt = reg.get("stub")
    assert isinstance(fmt, _StubFormatter)


@pytest.mark.unit
def test_registry_get_unknown_raises_key_error():
    reg = FormatterRegistry()
    with pytest.raises(KeyError, match="Unknown format 'nope'"):
        reg.get("nope")


@pytest.mark.unit
def test_registry_get_unknown_lists_available():
    reg = FormatterRegistry()
    reg.register("csv", _StubFormatter)
    reg.register("json", _StubFormatter)
    with pytest.raises(KeyError, match="csv, json"):
        reg.get("nope")


@pytest.mark.unit
def test_registry_available_returns_sorted_names():
    reg = FormatterRegistry()
    reg.register("json", _StubFormatter)
    reg.register("csv", _StubFormatter)
    reg.register("table", _StubFormatter)
    assert reg.available == ["csv", "json", "table"]


@pytest.mark.unit
def test_registry_passes_kwargs_to_constructor():
    class _WidthFormatter:
        def __init__(self, width=40):
            self.width = width

        def format(self, result):
            yield f"width={self.width}"

    reg = FormatterRegistry()
    reg.register("width", _WidthFormatter)
    fmt = reg.get("width", width=80)
    assert list(fmt.format(_make_result())) == ["width=80"]
