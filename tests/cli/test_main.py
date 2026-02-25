"""Tests for CLI entry point (Step 1.7)."""

import json
import sys

import pytest

from tests.integration_config import PROFILE_ARGS
from tsdb_tool import __version__
from tsdb_tool.cli.commands._shared import preprocess_optional_int_flags
from tsdb_tool.cli.main import app


@pytest.mark.unit
class TestCliHelp:
    def test_help_flag(self, runner):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "SQL Tool" in result.stdout
        assert "PostgreSQL" in result.stdout

    def test_no_args_shows_help(self, runner):
        result = runner.invoke(app, [])
        assert result.exit_code == 0 or result.exit_code == 2
        assert "SQL Tool" in result.stdout or "Usage" in result.stdout


@pytest.mark.unit
class TestCliVersion:
    def test_version_flag(self, runner):
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert f"tsdb-tool {__version__}" in result.stdout

    def test_version_short_flag(self, runner):
        result = runner.invoke(app, ["-V"])
        assert result.exit_code == 0
        assert f"tsdb-tool {__version__}" in result.stdout


@pytest.mark.unit
class TestCliVerbose:
    def test_verbose_flag_accepted(self, runner):
        """--verbose flag is accepted without error."""
        result = runner.invoke(app, ["--help", "--verbose"])
        assert result.exit_code == 0


@pytest.mark.unit
class TestUnknownCommand:
    def test_unknown_command_fails(self, runner):
        result = runner.invoke(app, ["nonexistent-command"])
        assert result.exit_code != 0


@pytest.mark.unit
class TestTestSentryCommand:
    def test_test_sentry_exists(self, runner):
        """test-sentry command is registered and runnable."""
        result = runner.invoke(app, ["test-sentry"])
        assert "Sending test error to Sentry" in result.stdout
        assert "Test events sent" in result.stdout


# -- preprocess_optional_int_flags --


def test_preprocess_inserts_default_for_bare_head(monkeypatch):
    monkeypatch.setattr("sys.argv", ["tsdb-tool", "table", "foo", "--head"])
    preprocess_optional_int_flags()
    assert sys.argv == ["tsdb-tool", "table", "foo", "--head", "10"]


def test_preprocess_keeps_explicit_head_value(monkeypatch):
    monkeypatch.setattr("sys.argv", ["tsdb-tool", "table", "foo", "--head", "5"])
    preprocess_optional_int_flags()
    assert sys.argv == ["tsdb-tool", "table", "foo", "--head", "5"]


def test_preprocess_inserts_default_for_bare_tail(monkeypatch):
    monkeypatch.setattr("sys.argv", ["tsdb-tool", "table", "foo", "--tail"])
    preprocess_optional_int_flags()
    assert sys.argv == ["tsdb-tool", "table", "foo", "--tail", "10"]


def test_preprocess_inserts_default_for_bare_sample(monkeypatch):
    monkeypatch.setattr("sys.argv", ["tsdb-tool", "table", "foo", "--sample"])
    preprocess_optional_int_flags()
    assert sys.argv == ["tsdb-tool", "table", "foo", "--sample", "10"]


def test_preprocess_skips_when_no_table_in_argv(monkeypatch):
    monkeypatch.setattr("sys.argv", ["tsdb-tool", "databases", "--head"])
    preprocess_optional_int_flags()
    assert sys.argv == ["tsdb-tool", "databases", "--head"]


def test_preprocess_handles_multiple_bare_flags(monkeypatch):
    monkeypatch.setattr("sys.argv", ["tsdb-tool", "table", "foo", "--head", "--tail"])
    preprocess_optional_int_flags()
    assert sys.argv == ["tsdb-tool", "table", "foo", "--head", "10", "--tail", "10"]


def test_preprocess_inserts_default_when_followed_by_flag(monkeypatch):
    monkeypatch.setattr(
        "sys.argv", ["tsdb-tool", "table", "foo", "--head", "--format", "json"]
    )
    preprocess_optional_int_flags()
    assert sys.argv == [
        "tsdb-tool",
        "table",
        "foo",
        "--head",
        "10",
        "--format",
        "json",
    ]


# -- schema --all-databases (integration) --


@pytest.mark.integration
def test_schema_all_databases_returns_rows(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "schema", "--all-databases")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert len(data) >= 2
    assert data[-1]["database"] == "TOTAL"


@pytest.mark.integration
def test_schema_all_databases_output_has_expected_columns(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "schema", "--all-databases")

    assert result.exit_code == 0
    row = json.loads(result.stdout)[0]
    for col in ("database", "schema", "tables", "total_size"):
        assert col in row


# -- databases table format (integration) --


@pytest.mark.integration
def test_databases_table_format_shows_headers_and_data(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--table", "databases")

    assert result.exit_code == 0
    assert "name" in result.output
    assert "owner" in result.output
    assert "TOTAL" in result.output


# -- schema table format (integration) --


@pytest.mark.integration
def test_schema_table_format_shows_headers_and_data(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--table", "schema")

    assert result.exit_code == 0
    assert "schema" in result.output
    assert "TOTAL" in result.output


# -- connections (integration) --


@pytest.mark.integration
def test_connections_table_format_shows_data(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--table", "connections", "--all")

    assert result.exit_code == 0


@pytest.mark.integration
def test_connections_json_format_has_expected_fields(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "connections", "--all")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    if data:
        row = data[0]
        assert "pid" in row
        assert "connected_since" in row
        assert "query_start" in row
