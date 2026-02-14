"""Tests for admin commands and service commands."""

import json

import pytest

from sql_tool.cli.helpers import fmt_size as _fmt_size
from tests.integration_config import (
    PROFILE_ARGS,
    TEST_DATABASE,
    TEST_PLAIN_TABLE_REF,
    TEST_SCHEMA,
    TEST_TABLE_REF,
)

# -- _fmt_size unit tests --


@pytest.mark.unit
def test_fmt_size_none_returns_dash():
    assert _fmt_size(None) == "-"


@pytest.mark.unit
def test_fmt_size_zero_returns_dash():
    assert _fmt_size(0) == "-"


@pytest.mark.unit
def test_fmt_size_bytes_below_kb():
    assert _fmt_size(512) == "512B"


@pytest.mark.unit
def test_fmt_size_kb_range():
    assert _fmt_size(2048) == "2.0 KB"


@pytest.mark.unit
def test_fmt_size_kb_large():
    assert _fmt_size(50 * 1024) == "50 KB"


@pytest.mark.unit
def test_fmt_size_mb_range():
    assert _fmt_size(5 * 1024 * 1024) == "5.0 MB"


@pytest.mark.unit
def test_fmt_size_gb_range():
    assert _fmt_size(15 * (1 << 30)) == "15 GB"


@pytest.mark.unit
def test_fmt_size_tb_range():
    assert _fmt_size(2 * (1 << 40)) == "2.0 TB"


# -- databases --


@pytest.mark.integration
def test_pg_databases_returns_database_list(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "databases")

    assert result.exit_code == 0
    assert "postgres" in result.stdout


@pytest.mark.integration
def test_pg_databases_output_includes_required_fields(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "databases")

    assert result.exit_code == 0
    assert "name" in result.stdout
    assert "owner" in result.stdout
    assert "encoding" in result.stdout
    assert "size" in result.stdout


@pytest.mark.integration
def test_pg_databases_includes_total_row(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "databases")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    names = [row["name"] for row in data]
    assert "TOTAL" in names


@pytest.mark.integration
def test_pg_databases_sorted_by_size_desc(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "databases")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    # Exclude TOTAL row, check sizes are descending
    sizes = [int(row["size"]) for row in data if row["name"] != "TOTAL"]
    assert sizes == sorted(sizes, reverse=True)


@pytest.mark.integration
def test_pg_databases_local_format_option(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "databases", "-f", "json")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


# -- schema --


@pytest.mark.integration
def test_pg_schema_returns_schema_list(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "schema")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_pg_schema_output_includes_required_fields(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "schema")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    if data:
        row = data[0]
        assert "schema" in row
        assert "tables" in row
        assert "total_size" in row


@pytest.mark.integration
def test_pg_schema_includes_total_row(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "schema")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    schemas = [row["schema"] for row in data]
    assert "TOTAL" in schemas


@pytest.mark.integration
def test_pg_schema_local_format_option(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "schema", "-f", "json")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


# -- table (list mode) --


@pytest.mark.integration
def test_pg_table_list_returns_table_list(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "table")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_pg_table_list_with_schema_filter(cli_runner):
    result = cli_runner(
        *PROFILE_ARGS, "--format", "json", "--schema", "public", "table"
    )

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_pg_table_list_includes_total_row(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "table")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    has_total = any(
        row.get("name") == "TOTAL" or row.get("schema") == "TOTAL" for row in data
    )
    assert has_total


@pytest.mark.integration
def test_pg_table_list_with_schema_filter_has_chunk_columns(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "table", "-s", TEST_SCHEMA)

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    if data:
        row = data[0]
        assert "before_size" in row
        assert "after_size" in row
        assert "chunks_u/c" in row
        assert "ratio" in row


@pytest.mark.integration
def test_pg_table_list_local_format_option(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "table", "-f", "json")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


# -- table (detail mode) --


@pytest.mark.integration
def test_pg_table_shows_column_definitions(cli_runner):
    result = cli_runner(
        *PROFILE_ARGS, "--format", "json", "table", "pg_catalog.pg_database"
    )

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)
    if data:
        assert "column_name" in data[0]
        assert "data_type" in data[0]


@pytest.mark.integration
def test_pg_table_head_preview(cli_runner):
    result = cli_runner(
        *PROFILE_ARGS,
        "--format",
        "json",
        "table",
        TEST_TABLE_REF,
        "--head",
        "3",
    )

    assert result.exit_code == 0


@pytest.mark.integration
def test_pg_table_tail_preview(cli_runner):
    result = cli_runner(
        *PROFILE_ARGS,
        "--format",
        "json",
        "table",
        TEST_TABLE_REF,
        "--tail",
        "3",
    )

    assert result.exit_code == 0


@pytest.mark.integration
def test_pg_table_sample_preview(cli_runner):
    result = cli_runner(
        *PROFILE_ARGS,
        "--format",
        "json",
        "table",
        TEST_PLAIN_TABLE_REF,
        "--sample",
        "3",
    )

    assert result.exit_code == 0


@pytest.mark.integration
def test_pg_table_local_format_option(cli_runner):
    result = cli_runner(
        *PROFILE_ARGS,
        "table",
        "pg_catalog.pg_database",
        "-f",
        "json",
    )

    assert result.exit_code == 0


# -- service check --


@pytest.mark.integration
def test_pg_service_check_returns_server_info(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "service", "check")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)
    props = {row["property"]: row["value"] for row in data}
    assert "version" in props
    assert "database" in props
    assert "user" in props


# -- connections --


@pytest.mark.integration
def test_pg_connections_shows_details(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "connections")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_pg_connections_summary(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "connections", "--summary")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_pg_connections_with_min_duration_filter(cli_runner):
    result = cli_runner(
        *PROFILE_ARGS,
        "--format",
        "json",
        "connections",
        "--min-duration",
        "1",
    )

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_pg_connections_with_filter_user(cli_runner):
    result = cli_runner(
        *PROFILE_ARGS,
        "--format",
        "json",
        "connections",
        "--all",
        "--filter-user",
        "tsdbadmin",
    )

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_pg_connections_with_filter_db(cli_runner):
    result = cli_runner(
        *PROFILE_ARGS,
        "--format",
        "json",
        "connections",
        "--all",
        "--filter-db",
        TEST_DATABASE,
    )

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_pg_connections_with_filter_state(cli_runner):
    result = cli_runner(
        *PROFILE_ARGS,
        "--format",
        "json",
        "connections",
        "--all",
        "--filter-state",
        "idle",
    )

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_pg_connections_local_format_option(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "connections", "--all", "-f", "json")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


# -- vacuum --


def test_pg_vacuum_requires_table_or_all(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "service", "vacuum")

    assert result.exit_code == 2


# -- kill --


def test_pg_kill_requires_pid_argument(cli_runner):
    result = cli_runner("service", "kill")

    assert result.exit_code != 0
