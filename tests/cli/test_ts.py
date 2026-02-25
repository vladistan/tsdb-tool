import json
from unittest.mock import MagicMock, patch

import pytest

from tests.integration_config import PROFILE_ARGS, TEST_TABLE, TEST_TABLE_REF
from tsdb_tool.core.models import ColumnMeta, QueryResult

_TS = "tsdb_tool.cli.commands.ts"


@pytest.fixture
def _patch_ts_client():
    """Stub get_client and check_timescaledb_available for mock-based tests."""
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    with (
        patch(f"{_TS}.get_client", return_value=mock_client),
        patch(f"{_TS}.check_timescaledb_available"),
    ):
        yield mock_client


def _make_result(rows, status="SELECT"):
    return QueryResult(
        columns=[ColumnMeta(name="col", type_oid=25, type_name="text")],
        rows=rows,
        row_count=len(rows),
        status_message=f"{status} {len(rows)}",
    )


# -- TimescaleDB detection (6.7) --


@pytest.mark.integration
def test_ts_hypertables_against_timescaledb(cli_runner):
    """ts hypertables returns results from real TimescaleDB."""
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "hypertables")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_ts_hypertables_output_includes_required_fields(cli_runner):
    """ts hypertables output includes expected column names."""
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "hypertables")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    if data:
        row = data[0]
        assert "schema" in row
        assert "table" in row
        assert "time_col" in row
        assert "chunk_iv" in row
        assert "size" in row
        assert "uncompr" in row
        assert "compr" in row
        assert "compr_on" in row


@pytest.mark.integration
def test_ts_hypertables_table_format_shows_total_row(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--table", "ts", "hypertables")

    assert result.exit_code == 0
    assert "TOTAL" in result.output


# -- chunks (6.2) --


@pytest.mark.integration
def test_ts_chunks_returns_chunk_list(cli_runner):
    """ts chunks returns chunk info for a hypertable with data."""
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "chunks", TEST_TABLE_REF)

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)
    assert len(data) > 1, "Expected chunks + TOTAL row"


@pytest.mark.integration
def test_ts_chunks_output_includes_required_fields(cli_runner):
    """ts chunks output includes expected column names."""
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "chunks", TEST_TABLE_REF)

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert len(data) >= 2, "Expected at least one chunk + TOTAL row"
    row = data[0]
    assert "chunk_name" in row
    assert "range_start" in row
    assert "range_end" in row
    assert "is_compressed" in row
    assert "size" in row


def test_ts_chunks_requires_hypertable_argument(cli_runner):
    """ts chunks requires a hypertable name argument."""
    result = cli_runner("ts", "chunks")

    assert result.exit_code != 0


@pytest.mark.integration
def test_ts_chunks_table_format_shows_summary(cli_runner):
    """ts chunks in table format shows compression summary footer."""
    result = cli_runner(*PROFILE_ARGS, "--table", "ts", "chunks", TEST_TABLE_REF)

    assert result.exit_code == 0
    assert "Uncompressed:" in result.stderr
    assert "Compressed:" in result.stderr
    assert "Total:" in result.stderr
    assert "chunks" in result.stderr


# -- compression (6.3) --


@pytest.mark.integration
def test_ts_compression_summary(cli_runner):
    """ts compression without argument shows all hypertable compression status."""
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "compression")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_ts_compression_for_specific_hypertable(cli_runner):
    """ts compression with hypertable argument filters to that table."""
    ht_result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "hypertables")
    hypertables = json.loads(ht_result.stdout)

    if not hypertables:
        pytest.skip("No hypertables available for compression test")

    ht = hypertables[0]
    table_ref = f"{ht['schema']}.{ht['table']}"

    result = cli_runner(
        *PROFILE_ARGS, "--format", "json", "ts", "compression", table_ref
    )

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_ts_compression_output_includes_required_fields(cli_runner):
    """ts compression output includes expected column names."""
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "compression")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    if data:
        row = data[0]
        assert "schema" in row
        assert "table" in row
        assert "chunks" in row
        assert "compr" in row
        assert "before" in row
        assert "after" in row


@pytest.mark.integration
def test_ts_compression_with_hypertable_arg(cli_runner):
    result = cli_runner(
        *PROFILE_ARGS, "--format", "json", "ts", "compression", TEST_TABLE_REF
    )

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert len(data) >= 1
    assert data[0]["table"] == TEST_TABLE


# -- caggs (6.4) --


@pytest.mark.integration
def test_ts_caggs_returns_continuous_aggregates(cli_runner):
    """ts caggs returns list of continuous aggregates."""
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "caggs")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_ts_caggs_output_includes_required_fields(cli_runner):
    """ts caggs output includes expected column names."""
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "caggs")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    if data:
        row = data[0]
        assert "view_schema" in row
        assert "view_name" in row
        assert "source_hypertable" in row


# -- retention (6.5) --


@pytest.mark.integration
def test_ts_retention_returns_policies(cli_runner):
    """ts retention returns retention policy info."""
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "retention")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_ts_retention_output_includes_required_fields(cli_runner):
    """ts retention output includes expected column names."""
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "retention")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    if data:
        row = data[0]
        assert "hypertable_schema" in row
        assert "hypertable_name" in row
        assert "drop_after" in row
        assert "schedule_interval" in row


# -- refresh-status (6.6) --


@pytest.mark.integration
def test_ts_refresh_status_returns_aggregate_status(cli_runner):
    """ts refresh-status returns continuous aggregate refresh status."""
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "refresh-status")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_ts_refresh_status_output_includes_required_fields(cli_runner):
    """ts refresh-status output includes expected column names."""
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "refresh-status")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    if data:
        row = data[0]
        assert "hypertable_schema" in row
        assert "hypertable_name" in row
        assert "schedule_interval" in row
        assert "last_run_status" in row


# -- TimescaleDB detection against plain PostgreSQL (6.7) --


@pytest.mark.integration
def test_ts_command_against_plain_postgres_fails_gracefully(cli_runner):
    """ts commands fail with helpful error when TimescaleDB not installed.

    Uses direct connection to plain postgres database (no TimescaleDB).
    """
    result = cli_runner(
        "--host",
        "localhost",
        "--database",
        "postgres",
        "--user",
        "postgres",
        "ts",
        "hypertables",
    )

    # Should fail because plain postgres doesn't have TimescaleDB
    assert result.exit_code != 0


# -- jobs (Issue 8) --


@pytest.mark.integration
def test_ts_jobs_returns_job_list(cli_runner):
    """ts jobs returns background job info."""
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "jobs")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_ts_jobs_output_includes_required_fields(cli_runner):
    """ts jobs output includes expected column names."""
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "jobs")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    if data:
        row = data[0]
        assert "job_id" in row
        assert "schedule" in row
        assert "last_run_status" in row


@pytest.mark.integration
def test_ts_jobs_json_formats_schedule_as_human_readable(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "jobs")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    if data:
        schedule = data[0]["schedule"]
        assert ":" not in schedule


@pytest.mark.integration
def test_ts_jobs_table_format(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--table", "ts", "jobs")

    assert result.exit_code == 0


# -- jobs --history (integration) --


@pytest.mark.integration
def test_ts_jobs_history_returns_data(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "jobs", "--history")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)


@pytest.mark.integration
def test_ts_jobs_history_output_includes_expected_fields(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "jobs", "--history")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    if data:
        row = data[0]
        for col in (
            "job_id",
            "succeeded",
            "execution_start",
            "execution_finish",
            "error_data",
        ):
            assert col in row


# -- compression-settings (integration) --


@pytest.mark.integration
def test_ts_compression_settings_returns_data(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "compression-settings")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)
    assert len(data) > 0


@pytest.mark.integration
def test_ts_compression_settings_output_includes_expected_fields(cli_runner):
    result = cli_runner(*PROFILE_ARGS, "--format", "json", "ts", "compression-settings")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    if data:
        row = data[0]
        for col in ("schema", "table", "time_col", "chunk_iv", "seg_by", "order_by"):
            assert col in row


@pytest.mark.integration
def test_ts_compression_settings_no_policy_hides_policy_columns(cli_runner):
    result = cli_runner(
        *PROFILE_ARGS,
        "--format",
        "json",
        "ts",
        "compression-settings",
        "--no-policy",
    )

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    if data:
        row = data[0]
        assert "schema" in row
        assert "compress" not in row
        assert "sched" not in row
        assert "status" not in row


@pytest.mark.integration
def test_ts_compression_settings_with_hypertable_arg(cli_runner):
    result = cli_runner(
        *PROFILE_ARGS,
        "--format",
        "json",
        "ts",
        "compression-settings",
        TEST_TABLE_REF,
    )

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert len(data) >= 1
    assert data[0]["table"] == TEST_TABLE


# -- Format options --


@pytest.mark.integration
def test_ts_hypertables_csv_format(cli_runner):
    """ts hypertables works with CSV format."""
    result = cli_runner(*PROFILE_ARGS, "--format", "csv", "ts", "hypertables")

    assert result.exit_code == 0


# ====================================================================
# Mock-based tests for specific formatting logic and edge cases
# ====================================================================


# -- jobs table format: relative time formatting --


def test_jobs_table_format_uses_relative_times(cli_runner, _patch_ts_client):
    raw_rows = [
        (
            1000,
            "Compression",
            "public.metrics",
            "01:00:00",
            "2026-02-13 10:00:00",
            3600.0,
            "Success",
            "2026-02-14 10:00:00",
            -1800.0,
            50,
            49,
            1,
        ),
    ]
    with patch(f"{_TS}.list_jobs", return_value=_make_result(raw_rows)):
        result = cli_runner("--table", "ts", "jobs")

    assert result.exit_code == 0
    assert "Compression" in result.output
    assert "1h ago" in result.output
    assert "30m ago" in result.output
    assert "Success" in result.output


def test_jobs_table_format_future_next_start(cli_runner, _patch_ts_client):
    raw_rows = [
        (
            1000,
            "Compression",
            "public.metrics",
            "01:00:00",
            "2026-02-13 10:00:00",
            3600.0,
            "Success",
            "2026-02-14 12:00:00",
            7200.0,
            50,
            49,
            1,
        ),
    ]
    with patch(f"{_TS}.list_jobs", return_value=_make_result(raw_rows)):
        result = cli_runner("--table", "ts", "jobs")

    assert result.exit_code == 0
    assert "in 2h" in result.output


def test_jobs_table_format_none_next_start(cli_runner, _patch_ts_client):
    raw_rows = [
        (
            1000,
            "Compression",
            "public.metrics",
            "01:00:00",
            "2026-02-13 10:00:00",
            3600.0,
            "Success",
            None,
            None,
            50,
            49,
            1,
        ),
    ]
    with patch(f"{_TS}.list_jobs", return_value=_make_result(raw_rows)):
        result = cli_runner("--table", "ts", "jobs")

    assert result.exit_code == 0
    assert "1h ago" in result.output


# -- compression-settings: exact formatting tests --


def test_compression_settings_formats_intervals_and_timestamps(
    cli_runner, _patch_ts_client
):
    raw_rows = [
        (
            "public",
            "metrics",
            "time",
            "01:00:00",
            "device_id",
            "time DESC",
            "7 days",
            "24:00:00",
            "2026-02-13 10:00:00.123+00",
            "2026-02-14 10:00:00.456+00",
            "Success",
        ),
    ]
    with patch(f"{_TS}.list_compression_settings", return_value=_make_result(raw_rows)):
        result = cli_runner("--format", "json", "ts", "compression-settings")

    row = json.loads(result.stdout)[0]
    assert row["chunk_iv"] == "1 hour"
    assert row["sched"] == "1 day"
    assert row["last_run"] == "2026-02-13 10:00:00"
    assert row["next_start"] == "2026-02-14 10:00:00"


def test_compression_settings_none_values_show_dash(cli_runner, _patch_ts_client):
    raw_rows = [
        (
            "public",
            "events",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
    ]
    with patch(f"{_TS}.list_compression_settings", return_value=_make_result(raw_rows)):
        result = cli_runner("--format", "json", "ts", "compression-settings")

    row = json.loads(result.stdout)[0]
    assert row["time_col"] == "-"
    assert row["chunk_iv"] == "-"
    assert row["seg_by"] == "-"
    assert row["order_by"] == "-"
    assert row["compress"] == "-"
    assert row["sched"] == "-"
    assert row["status"] == "-"


# -- compression: exact size formatting --


def test_compression_json_uses_gb_sizes(cli_runner, _patch_ts_client):
    raw_rows = [
        ("public", "metrics", 10, 7, 1073741824, 268435456),
    ]
    with patch(f"{_TS}.compression_stats", return_value=_make_result(raw_rows)):
        result = cli_runner("--format", "json", "ts", "compression")

    row = json.loads(result.stdout)[0]
    assert row["before"] == "1.00 GB"
    assert row["after"] == "256.00 MB"


def test_compression_table_uses_compact_sizes(cli_runner, _patch_ts_client):
    raw_rows = [
        ("public", "metrics", 10, 7, 1073741824, 268435456),
    ]
    with patch(f"{_TS}.compression_stats", return_value=_make_result(raw_rows)):
        result = cli_runner("--table", "ts", "compression")

    assert result.exit_code == 0
    assert "1.0GB" in result.output
    assert "256M" in result.output


# -- jobs --history: formatting edge cases --


def test_jobs_history_truncates_long_error_data(cli_runner, _patch_ts_client):
    long_error = "x" * 300
    raw_rows = [
        (1000, False, "2026-02-13 10:00:00", "2026-02-13 10:01:00", long_error),
    ]
    with patch(f"{_TS}.list_job_history", return_value=_make_result(raw_rows)):
        result = cli_runner("--format", "json", "ts", "jobs", "--history")

    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert len(data[0]["error_data"]) == 200


def test_jobs_history_replaces_none_error_with_dash(cli_runner, _patch_ts_client):
    raw_rows = [
        (1000, True, "2026-02-13 10:00:00", "2026-02-13 10:01:00", None),
    ]
    with patch(f"{_TS}.list_job_history", return_value=_make_result(raw_rows)):
        result = cli_runner("--format", "json", "ts", "jobs", "--history")

    data = json.loads(result.stdout)
    assert data[0]["error_data"] == "-"


def test_jobs_history_shows_error_when_no_table(cli_runner, _patch_ts_client):
    with patch(
        f"{_TS}.list_job_history", side_effect=Exception("relation does not exist")
    ):
        result = cli_runner("--format", "json", "ts", "jobs", "--history")

    assert result.exit_code == 1
    assert "No job history table found" in result.output


# -- compress command: edge cases --


def test_compress_already_compressed_chunk_shows_message(cli_runner, _patch_ts_client):
    chunks = [
        ("_hyper_1_1_chunk", True),
        ("_hyper_1_2_chunk", False),
    ]
    with patch(f"{_TS}.list_chunk_info", return_value=chunks):
        result = cli_runner("ts", "compress", "public.metrics", "--chunk", "1")

    assert result.exit_code == 0
    assert "already compressed" in result.output


def test_compress_chunk_not_found_shows_error(cli_runner, _patch_ts_client):
    chunks = [
        ("_hyper_1_1_chunk", False),
    ]
    with patch(f"{_TS}.list_chunk_info", return_value=chunks):
        result = cli_runner("ts", "compress", "public.metrics", "--chunk", "999")

    assert result.exit_code == 1
    assert "not found" in result.output


def test_compress_all_skips_active_chunk(cli_runner, _patch_ts_client):
    chunks = [
        ("_hyper_1_1_chunk", False),
        ("_hyper_1_2_chunk", False),
        ("_hyper_1_3_chunk", False),
    ]
    with (
        patch(f"{_TS}.list_chunk_info", return_value=chunks),
        patch(f"{_TS}.compress_single_chunk") as mock_compress,
    ):
        result = cli_runner("ts", "compress", "public.metrics")

    assert result.exit_code == 0
    assert mock_compress.call_count == 2
    compressed_names = [c[0][1] for c in mock_compress.call_args_list]
    assert "_hyper_1_1_chunk" in compressed_names
    assert "_hyper_1_2_chunk" in compressed_names
    assert "_hyper_1_3_chunk" not in compressed_names


def test_compress_all_no_uncompressed_exits(cli_runner, _patch_ts_client):
    chunks = [
        ("_hyper_1_1_chunk", True),
        ("_hyper_1_2_chunk", True),
        ("_hyper_1_3_chunk", False),
    ]
    with patch(f"{_TS}.list_chunk_info", return_value=chunks):
        result = cli_runner("ts", "compress", "public.metrics")

    assert result.exit_code == 0
    assert "No uncompressed chunks" in result.output


def test_compress_all_no_chunks_exits(cli_runner, _patch_ts_client):
    with patch(f"{_TS}.list_chunk_info", return_value=[]):
        result = cli_runner("ts", "compress", "public.metrics")

    assert result.exit_code == 0
    assert "No chunks found" in result.output
