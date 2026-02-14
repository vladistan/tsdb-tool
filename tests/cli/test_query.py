"""Tests for query command (Phase 3, Step 3.4).

Integration tests against real PostgreSQL using test_db profile.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.integration_config import PROFILE_ARGS, TEST_DATABASE
from tsdb_tool.cli.main import app
from tsdb_tool.core.exceptions import InputError

FIXTURE_SQL = str(Path(__file__).parent.parent / "fixtures" / "select_42.sql")


# -- Help --


@pytest.mark.unit
def test_query_help(runner):
    result = runner.invoke(app, ["query", "--help"])
    assert result.exit_code == 0
    assert "Execute a SQL query" in result.stdout
    assert "--execute" in result.stdout


# -- Inline queries --


@pytest.mark.integration
def test_query_inline(runner):
    result = runner.invoke(
        app, [*PROFILE_ARGS, "--format", "json", "query", "-e", "SELECT 1 AS num"]
    )
    assert result.exit_code == 0, f"output: {result.stdout}"
    parsed = json.loads(result.stdout)
    assert parsed == [{"num": 1}]


@pytest.mark.integration
def test_query_inline_multiple_rows(runner):
    result = runner.invoke(
        app,
        [
            *PROFILE_ARGS,
            "--format",
            "json",
            "query",
            "-e",
            "SELECT v FROM generate_series(1,3) AS v",
        ],
    )
    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert parsed == [{"v": 1}, {"v": 2}, {"v": 3}]


@pytest.mark.integration
def test_query_inline_multiple_columns(runner):
    result = runner.invoke(
        app,
        [
            *PROFILE_ARGS,
            "--format",
            "json",
            "query",
            "-e",
            "SELECT 1 AS id, 'alice' AS name",
        ],
    )
    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert parsed == [{"id": 1, "name": "alice"}]


# -- File queries --


@pytest.mark.integration
def test_query_from_file(runner):
    result = runner.invoke(
        app, [*PROFILE_ARGS, "--format", "json", "query", FIXTURE_SQL]
    )
    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert parsed == [{"answer": 42}]


@pytest.mark.unit
def test_query_file_not_found(runner):
    result = runner.invoke(app, [*PROFILE_ARGS, "query", "/nonexistent/file.sql"])
    assert result.exit_code != 0
    assert isinstance(result.exception, InputError)


# -- No source --


@pytest.mark.unit
def test_query_no_source(runner):
    with patch(
        "tsdb_tool.cli.commands.query.resolve_query_source",
        side_effect=InputError(
            "No query provided. Use -e, file path, or pipe to stdin."
        ),
    ):
        result = runner.invoke(app, [*PROFILE_ARGS, "query"])

    assert result.exit_code != 0
    assert isinstance(result.exception, InputError)


# -- Error handling --


@pytest.mark.integration
def test_query_connection_failure(runner):
    result = runner.invoke(
        app,
        [
            "--host",
            "192.0.2.1",
            "--port",
            "9999",
            "query",
            "-e",
            "SELECT 1",
            "--timeout",
            "1",
        ],
    )
    assert result.exit_code != 0


@pytest.mark.integration
def test_query_syntax_error(runner):
    result = runner.invoke(app, [*PROFILE_ARGS, "query", "-e", "SELECTT 1"])
    assert result.exit_code != 0


@pytest.mark.integration
def test_query_timeout(runner):
    result = runner.invoke(
        app,
        [*PROFILE_ARGS, "query", "-e", "SELECT pg_sleep(10)", "--timeout", "0.1"],
    )
    assert result.exit_code != 0


# -- Profile and options --


@pytest.mark.integration
def test_query_uses_profile(runner):
    result = runner.invoke(
        app,
        [
            *PROFILE_ARGS,
            "--format",
            "json",
            "query",
            "-e",
            "SELECT current_database() AS db",
        ],
    )
    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert parsed[0]["db"] == TEST_DATABASE


@pytest.mark.integration
def test_query_timeout_option_applied(runner):
    result = runner.invoke(
        app,
        [
            *PROFILE_ARGS,
            "--format",
            "json",
            "query",
            "-e",
            "SHOW statement_timeout",
            "--timeout",
            "5",
        ],
    )
    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert parsed[0]["statement_timeout"] == "5s"
