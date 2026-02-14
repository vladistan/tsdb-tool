"""E2E tests for output formatting (Phase 5, Step 5.7).

Full CLI pipeline tests: query execution → formatter → stdout.
Covers edge cases: empty results, NULLs, Unicode, large datasets,
format options, and pipeline compatibility.
"""

import csv
import json
from io import StringIO

import pytest

from tests.integration_config import PROFILE_ARGS
from tsdb_tool.cli.main import app

# -- helpers --


def _invoke(runner, *extra_args):
    return runner.invoke(app, [*PROFILE_ARGS, *extra_args])


def _query(runner, sql, fmt="json", extra=None):
    args = [*PROFILE_ARGS, "--format", fmt]
    if extra:
        args.extend(extra)
    args.extend(["query", "-e", sql])
    return runner.invoke(app, args)


# -- Full workflow: each format --


@pytest.mark.integration
def test_e2e_json_output(runner):
    result = _query(runner, "SELECT 1 AS id, 'hello' AS msg")

    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert parsed == [{"id": 1, "msg": "hello"}]


@pytest.mark.integration
def test_e2e_csv_output(runner):
    result = _query(runner, "SELECT 1 AS id, 'hello' AS msg", fmt="csv")

    assert result.exit_code == 0
    lines = result.stdout.strip().split("\n")
    assert lines[0] == "id,msg"
    assert lines[1] == "1,hello"


@pytest.mark.integration
def test_e2e_table_output(runner):
    result = _query(runner, "SELECT 1 AS id, 'hello' AS msg", fmt="table")

    assert result.exit_code == 0
    assert "id" in result.stdout
    assert "msg" in result.stdout
    assert "hello" in result.stdout


# -- Empty result set --


@pytest.mark.integration
def test_e2e_empty_result_json(runner):
    result = _query(runner, "SELECT 1 AS id WHERE false")

    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert parsed == []


@pytest.mark.integration
def test_e2e_empty_result_csv(runner):
    result = _query(runner, "SELECT 1 AS id WHERE false", fmt="csv")

    assert result.exit_code == 0
    lines = result.stdout.strip().split("\n")
    assert lines[0] == "id"
    assert len(lines) == 1


@pytest.mark.integration
def test_e2e_empty_result_table(runner):
    result = _query(runner, "SELECT 1 AS id WHERE false", fmt="table")

    assert result.exit_code == 0
    assert "No results" in result.stdout


# -- NULL values --


@pytest.mark.integration
def test_e2e_null_json(runner):
    result = _query(runner, "SELECT NULL AS val")

    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert parsed == [{"val": None}]


@pytest.mark.integration
def test_e2e_null_csv(runner):
    result = _query(runner, "SELECT 1 AS id, NULL AS val", fmt="csv")

    assert result.exit_code == 0
    reader = csv.reader(StringIO(result.stdout.strip()))
    rows = list(reader)
    assert rows[0] == ["id", "val"]
    assert rows[1] == ["1", ""]


@pytest.mark.integration
def test_e2e_null_table(runner):
    result = _query(runner, "SELECT NULL AS val", fmt="table")

    assert result.exit_code == 0
    assert "val" in result.stdout
    assert "None" not in result.stdout


# -- Unicode --


@pytest.mark.integration
def test_e2e_unicode_json(runner):
    result = _query(runner, "SELECT '日本語' AS text, 'emojis 🎉' AS fun")

    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert parsed[0]["text"] == "日本語"
    assert parsed[0]["fun"] == "emojis 🎉"


@pytest.mark.integration
def test_e2e_unicode_csv(runner):
    result = _query(runner, "SELECT '日本語' AS text", fmt="csv")

    assert result.exit_code == 0
    lines = result.stdout.strip().split("\n")
    assert lines[1] == "日本語"


@pytest.mark.integration
def test_e2e_unicode_table(runner):
    result = _query(runner, "SELECT '日本語' AS text", fmt="table")

    assert result.exit_code == 0
    assert "日本語" in result.stdout


# -- Large result set --


@pytest.mark.integration
def test_e2e_large_result_csv(runner):
    """10k rows streamed as CSV without issues."""
    result = _query(
        runner,
        "SELECT g AS n FROM generate_series(1, 10000) AS g",
        fmt="csv",
    )

    assert result.exit_code == 0
    lines = result.stdout.strip().split("\n")
    assert lines[0] == "n"
    assert len(lines) == 10001  # header + 10k rows


@pytest.mark.integration
def test_e2e_large_result_json(runner):
    """10k rows as JSON array."""
    result = _query(
        runner,
        "SELECT g AS n FROM generate_series(1, 10000) AS g",
    )

    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert len(parsed) == 10000
    assert parsed[0] == {"n": 1}
    assert parsed[-1] == {"n": 10000}


# -- Format options via CLI --


@pytest.mark.integration
def test_e2e_compact_json(runner):
    result = _query(runner, "SELECT 1 AS n", extra=["--compact"])

    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert parsed == [{"n": 1}]
    # Compact output has no indentation newlines
    assert result.stdout.strip().count("\n") == 0


@pytest.mark.integration
def test_e2e_csv_no_header(runner):
    result = _query(runner, "SELECT 1 AS n, 2 AS m", fmt="csv", extra=["--no-header"])

    assert result.exit_code == 0
    lines = result.stdout.strip().split("\n")
    assert len(lines) == 1
    assert lines[0] == "1,2"


@pytest.mark.integration
def test_e2e_table_shorthand(runner):
    """--table flag is shorthand for --format table."""
    result = runner.invoke(
        app, [*PROFILE_ARGS, "--table", "query", "-e", "SELECT 1 AS n"]
    )

    assert result.exit_code == 0
    assert "n" in result.stdout
    assert "1" in result.stdout


# -- Pipeline compatibility --


@pytest.mark.integration
def test_e2e_csv_parseable_by_csv_reader(runner):
    """CSV output is valid RFC 4180, parseable by Python csv module."""
    result = _query(
        runner,
        "SELECT 1 AS id, 'comma, here' AS val, 'quote \"this\"' AS q",
        fmt="csv",
    )

    assert result.exit_code == 0
    reader = csv.reader(StringIO(result.stdout.strip()))
    rows = list(reader)
    assert rows[0] == ["id", "val", "q"]
    assert rows[1][1] == "comma, here"
    assert rows[1][2] == 'quote "this"'


@pytest.mark.integration
def test_e2e_json_parseable_by_json_loads(runner):
    """JSON output from multi-row query is valid JSON array."""
    result = _query(
        runner,
        "SELECT v, v * 2 AS doubled FROM generate_series(1, 5) AS v",
    )

    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert len(parsed) == 5
    assert all("v" in row and "doubled" in row for row in parsed)


# -- TTY auto-detection --


@pytest.mark.integration
def test_e2e_non_tty_defaults_to_csv(runner):
    """CliRunner is non-TTY, so default format should be CSV."""
    result = runner.invoke(app, [*PROFILE_ARGS, "query", "-e", "SELECT 1 AS n"])

    assert result.exit_code == 0
    lines = result.stdout.strip().split("\n")
    assert lines[0] == "n"
    assert lines[1] == "1"


# -- Admin commands with formatting --


@pytest.mark.integration
def test_e2e_databases_csv(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "csv", "databases")

    assert result.exit_code == 0
    reader = csv.reader(StringIO(result.stdout.strip()))
    rows = list(reader)
    header = rows[0]
    assert "name" in header
    assert "owner" in header


@pytest.mark.integration
def test_e2e_databases_table(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "table", "databases")

    assert result.exit_code == 0
    assert "name" in result.stdout
    assert "postgres" in result.stdout


@pytest.mark.integration
def test_e2e_check_all_formats(cli_runner):
    """check works with all three output formats."""
    for fmt in ("json", "csv", "table"):
        result = cli_runner(*PROFILE_ARGS, "--format", fmt, "service", "check")

        assert result.exit_code == 0, f"format={fmt} failed"
        assert len(result.stdout.strip()) > 0, f"format={fmt} produced empty output"
