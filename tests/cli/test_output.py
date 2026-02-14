"""Tests for output format selection and TTY detection (Phase 5, Step 5.5)."""

import pytest

from sql_tool.cli.output import OutputFormat, get_formatter, resolve_format
from sql_tool.formatters.csv import CSVFormatter
from sql_tool.formatters.json import JSONFormatter
from sql_tool.formatters.table import TableFormatter


@pytest.mark.unit
def test_output_format_enum_values():
    assert OutputFormat.TABLE.value == "table"
    assert OutputFormat.JSON.value == "json"
    assert OutputFormat.CSV.value == "csv"


@pytest.mark.unit
def test_resolve_format_explicit_json():
    assert resolve_format("json") == "json"


@pytest.mark.unit
def test_resolve_format_explicit_table():
    assert resolve_format("table") == "table"


@pytest.mark.unit
def test_resolve_format_explicit_csv():
    assert resolve_format("csv") == "csv"


@pytest.mark.unit
def test_resolve_format_tty_defaults_to_table(monkeypatch):
    monkeypatch.setattr("sql_tool.cli.output.detect_tty", lambda: True)
    assert resolve_format(None) == "table"


@pytest.mark.unit
def test_resolve_format_non_tty_defaults_to_csv(monkeypatch):
    monkeypatch.setattr("sql_tool.cli.output.detect_tty", lambda: False)
    assert resolve_format(None) == "csv"


@pytest.mark.unit
def test_resolve_format_explicit_overrides_tty(monkeypatch):
    monkeypatch.setattr("sql_tool.cli.output.detect_tty", lambda: True)
    assert resolve_format("json") == "json"


@pytest.mark.unit
def test_get_formatter_returns_table():
    fmt = get_formatter("table")
    assert isinstance(fmt, TableFormatter)


@pytest.mark.unit
def test_get_formatter_returns_json():
    fmt = get_formatter("json")
    assert isinstance(fmt, JSONFormatter)


@pytest.mark.unit
def test_get_formatter_returns_csv():
    fmt = get_formatter("csv")
    assert isinstance(fmt, CSVFormatter)


@pytest.mark.unit
def test_get_formatter_passes_width_to_table():
    fmt = get_formatter("table", width=80)
    assert isinstance(fmt, TableFormatter)
    assert fmt.width == 80


@pytest.mark.unit
def test_get_formatter_passes_compact_to_json():
    fmt = get_formatter("json", compact=True)
    assert isinstance(fmt, JSONFormatter)
    assert fmt.compact is True


@pytest.mark.unit
def test_get_formatter_passes_no_header_to_csv():
    fmt = get_formatter("csv", no_header=True)
    assert isinstance(fmt, CSVFormatter)
    assert fmt.no_header is True


@pytest.mark.unit
def test_get_formatter_unknown_format_raises():
    with pytest.raises(KeyError, match="Unknown format 'nope'"):
        get_formatter("nope")
