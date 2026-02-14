"""Tests for TimescaleDB administration operations."""

import pytest

from sql_tool.core.client import PgClient
from sql_tool.core.config import load_config, resolve_config
from sql_tool.core.models import ColumnMeta, QueryResult
from sql_tool.core.timescaledb import (
    compression_stats,
    count_compressed_chunks,
    list_chunk_info,
    list_chunks,
    list_compressed_chunk_names,
    list_compression_settings,
    list_continuous_aggregates,
    list_hypertables,
    list_job_history,
    list_jobs,
    list_refresh_status,
    list_retention_policies,
    parse_chunk_id,
)
from tests.integration_config import TEST_PROFILE, TEST_SCHEMA, TEST_TABLE


def _make_result(rows, columns=None, status_message="SELECT 1"):
    if columns is None:
        columns = [ColumnMeta(name="col", type_oid=25, type_name="text")]
    return QueryResult(
        columns=columns,
        rows=rows,
        row_count=len(rows),
        status_message=status_message,
    )


@pytest.fixture
def resolved_config():
    config = load_config()
    return resolve_config(config, profile_name=TEST_PROFILE)


@pytest.fixture
def client(resolved_config):
    with PgClient(resolved_config) as c:
        yield c


# ---------------------------------------------------------------------------
# list_hypertables
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_hypertables_no_filter(client):
    result = list_hypertables(client)

    assert len(result.rows) > 0
    assert len(result.rows[0]) == 10


@pytest.mark.integration
def test_list_hypertables_with_schema_filter(client):
    result = list_hypertables(client, schema_filter=TEST_SCHEMA)

    assert len(result.rows) > 0
    for row in result.rows:
        assert row[0] == TEST_SCHEMA


# ---------------------------------------------------------------------------
# list_chunks
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_chunks_returns_result(client):
    result = list_chunks(client, TEST_SCHEMA, TEST_TABLE)

    assert len(result.rows) > 0
    assert len(result.rows[0]) == 5


# ---------------------------------------------------------------------------
# compression_stats
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_compression_stats_no_filter(client):
    result = compression_stats(client)

    assert len(result.rows) > 0
    assert len(result.rows[0]) == 6


@pytest.mark.integration
def test_compression_stats_with_table_filter(client):
    result = compression_stats(client, schema=TEST_SCHEMA, table=TEST_TABLE)

    assert len(result.rows) >= 1
    assert result.rows[0][0] == TEST_SCHEMA
    assert result.rows[0][1] == TEST_TABLE


# ---------------------------------------------------------------------------
# list_continuous_aggregates
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_continuous_aggregates_returns_result(client):
    result = list_continuous_aggregates(client)

    assert isinstance(result.rows, list)


# ---------------------------------------------------------------------------
# list_retention_policies
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_retention_policies_returns_result(client):
    result = list_retention_policies(client)

    assert isinstance(result.rows, list)


# ---------------------------------------------------------------------------
# list_refresh_status
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_refresh_status_returns_result(client):
    result = list_refresh_status(client)

    assert isinstance(result.rows, list)


# ---------------------------------------------------------------------------
# list_jobs
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_jobs_returns_result(client):
    result = list_jobs(client)

    assert len(result.rows) > 0
    assert len(result.rows[0]) == 12


# ---------------------------------------------------------------------------
# list_job_history
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_job_history_returns_result(client):
    result = list_job_history(client)

    assert isinstance(result.rows, list)
    if result.rows:
        assert len(result.rows[0]) == 5


@pytest.mark.integration
def test_list_job_history_with_job_id_filter(client):
    jobs = list_jobs(client)
    if not jobs.rows:
        pytest.skip("No jobs found")

    job_id = jobs.rows[0][0]
    result = list_job_history(client, job_id=job_id)

    assert isinstance(result.rows, list)


# ---------------------------------------------------------------------------
# list_compression_settings
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_compression_settings_no_filter(client):
    result = list_compression_settings(client)

    assert len(result.rows) > 0


@pytest.mark.integration
def test_list_compression_settings_with_table_filter(client):
    result = list_compression_settings(client, schema=TEST_SCHEMA, table=TEST_TABLE)

    assert len(result.rows) >= 1


@pytest.mark.integration
def test_list_compression_settings_with_effective_schema(client):
    result = list_compression_settings(client, effective_schema=TEST_SCHEMA)

    assert len(result.rows) >= 1


@pytest.mark.integration
def test_list_compression_settings_includes_policy_by_default(client):
    result = list_compression_settings(client)

    if result.rows:
        assert len(result.rows[0]) == 11


@pytest.mark.integration
def test_list_compression_settings_excludes_policy(client):
    result = list_compression_settings(client, include_policy=False)

    if result.rows:
        assert len(result.rows[0]) == 6


# ---------------------------------------------------------------------------
# count_compressed_chunks (read-only)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_count_compressed_chunks_returns_count(client):
    count = count_compressed_chunks(client, TEST_SCHEMA, TEST_TABLE)

    assert isinstance(count, int)
    assert count >= 0


# ---------------------------------------------------------------------------
# list_compressed_chunk_names (read-only)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_compressed_chunk_names_returns_names(client):
    names = list_compressed_chunk_names(client, TEST_SCHEMA, TEST_TABLE)

    assert isinstance(names, list)
    for name in names:
        assert isinstance(name, str)


# ---------------------------------------------------------------------------
# list_chunk_info (read-only)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_chunk_info_returns_tuples(client):
    result = list_chunk_info(client, TEST_SCHEMA, TEST_TABLE)

    assert isinstance(result, list)
    assert len(result) > 0
    for name, is_compressed in result:
        assert isinstance(name, str)
        assert isinstance(is_compressed, bool)


# ---------------------------------------------------------------------------
# parse_chunk_id (pure function, no DB needed)
# ---------------------------------------------------------------------------


def test_parse_chunk_id_standard_format():
    assert parse_chunk_id("_hyper_16_11420_chunk") == 11420


def test_parse_chunk_id_single_digit():
    assert parse_chunk_id("_hyper_1_5_chunk") == 5


def test_parse_chunk_id_returns_none_for_short_name():
    assert parse_chunk_id("short") is None


def test_parse_chunk_id_returns_none_for_non_numeric():
    assert parse_chunk_id("_hyper_1_abc_chunk") is None


def test_parse_chunk_id_empty_string():
    assert parse_chunk_id("") is None
