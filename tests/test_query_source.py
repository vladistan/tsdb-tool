"""Tests for query source resolution (Phase 3, Step 3.3)."""

import io
from pathlib import Path
from unittest.mock import patch

import pytest

from sql_tool.core.exceptions import InputError
from sql_tool.core.query_source import resolve_query_source

FIXTURE_SQL = str(Path(__file__).parent / "fixtures" / "select_42.sql")


@pytest.mark.unit
def test_inline_query():
    result = resolve_query_source(inline="SELECT 1", file_path=None)
    assert result == "SELECT 1"


@pytest.mark.unit
def test_inline_takes_precedence_over_file():
    result = resolve_query_source(inline="SELECT 1", file_path=FIXTURE_SQL)
    assert result == "SELECT 1"


@pytest.mark.unit
def test_file_query():
    result = resolve_query_source(inline=None, file_path=FIXTURE_SQL)
    assert result == "SELECT 42 AS answer\n"


@pytest.mark.unit
def test_file_not_found_raises_input_error():
    with pytest.raises(InputError, match="Query file not found"):
        resolve_query_source(inline=None, file_path="/nonexistent/file.sql")


@pytest.mark.unit
def test_stdin_query():
    with (
        patch("sys.stdin", new=io.StringIO("SELECT 99")),
        patch("sys.stdin.isatty", return_value=False),
    ):
        result = resolve_query_source(inline=None, file_path=None)
    assert result == "SELECT 99"


@pytest.mark.unit
def test_no_query_source_raises_input_error():
    with (
        patch("sys.stdin.isatty", return_value=True),
        pytest.raises(InputError, match="No query provided"),
    ):
        resolve_query_source(inline=None, file_path=None)


@pytest.mark.unit
def test_file_takes_precedence_over_stdin():
    with patch("sys.stdin", new=io.StringIO("SELECT FROM STDIN")):
        result = resolve_query_source(inline=None, file_path=FIXTURE_SQL)
    assert result == "SELECT 42 AS answer\n"


@pytest.mark.unit
def test_empty_inline_query():
    """Empty string is a valid inline query (PostgreSQL will error, not us)."""
    result = resolve_query_source(inline="", file_path=None)
    assert result == ""


@pytest.mark.unit
def test_file_with_trailing_whitespace(temp_dir):
    sql_file = temp_dir / "test.sql"
    sql_file.write_text("SELECT 1;\n\n")
    result = resolve_query_source(inline=None, file_path=str(sql_file))
    assert result == "SELECT 1;\n\n"
