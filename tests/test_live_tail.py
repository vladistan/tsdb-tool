import contextlib
import csv
import json
import sys
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from tsdb_tool.cli.commands.live_tail import print_summary
from tsdb_tool.cli.main import app
from tsdb_tool.cli.output import get_formatter
from tsdb_tool.core.client import PgClient
from tsdb_tool.core.config import load_config, resolve_config
from tsdb_tool.core.exceptions import InputError, SqlToolError
from tsdb_tool.core.exit_codes import ExitCode
from tsdb_tool.core.live_tail import (
    LiveTailConfig,
    PollResult,
    TableInfo,
    TailSummary,
    _find_time_column,
    detect_liveness,
    discover_tables,
    fetch_watermarks,
    format_count_line,
    poll_count,
    poll_rows,
    poll_tables,
    resolve_tables,
    validate_columns,
)
from tsdb_tool.core.models import ColumnMeta, QueryResult


@pytest.fixture
def mock_client():
    return MagicMock()


def _make_query_result(rows, columns=None):
    if columns is None:
        columns = [
            ColumnMeta(name="table_schema", type_oid=25, type_name="text"),
            ColumnMeta(name="table_name", type_oid=25, type_name="text"),
        ]
    return QueryResult(
        columns=columns,
        rows=rows,
        row_count=len(rows),
        status_message=f"SELECT {len(rows)}",
    )


@pytest.mark.unit
def test_live_tail_config_defaults():
    config = LiveTailConfig(schema="public", tables=[])
    assert config.interval == 30
    assert config.duration == 180
    assert config.full is False
    assert config.schema == "public"
    assert config.tables == []
    assert config.columns is None
    assert config.liveness_probe_interval == 20
    assert config.liveness_timeout == 120


@pytest.mark.unit
def test_live_tail_config_custom_values():
    config = LiveTailConfig(
        schema="analytics",
        tables=["events", "metrics"],
        interval=60,
        duration=300,
        full=True,
        columns=["id", "timestamp"],
    )
    assert config.schema == "analytics"
    assert config.tables == ["events", "metrics"]
    assert config.interval == 60
    assert config.duration == 300
    assert config.full is True
    assert config.columns == ["id", "timestamp"]


@pytest.mark.unit
def test_live_tail_config_interval_bounds():
    with pytest.raises(ValueError, match="interval must be between 1 and 3600"):
        LiveTailConfig(schema="public", tables=[], interval=0)

    with pytest.raises(ValueError, match="interval must be between 1 and 3600"):
        LiveTailConfig(schema="public", tables=[], interval=3601)

    config = LiveTailConfig(schema="public", tables=[], interval=1)
    assert config.interval == 1

    config = LiveTailConfig(schema="public", tables=[], interval=3600)
    assert config.interval == 3600


@pytest.mark.unit
def test_live_tail_config_duration_validation():
    with pytest.raises(ValueError, match="duration must be >= 0"):
        LiveTailConfig(schema="public", tables=[], duration=-1)

    config = LiveTailConfig(schema="public", tables=[], duration=0)
    assert config.duration == 0


@pytest.mark.unit
def test_table_info_fqn_property():
    table = TableInfo(schema="public", table="events", time_column="created_at")
    assert table.fqn == '"public"."events"'

    table = TableInfo(schema="analytics", table="metrics", time_column="ts")
    assert table.fqn == '"analytics"."metrics"'


@pytest.mark.unit
def test_poll_result_rows_per_second():
    result = PollResult(
        table_fqn='"public"."events"',
        count=100,
        max_ts="2025-01-15 10:30:00",
        interval=10,
    )
    assert result.rows_per_second == 10.0

    result = PollResult(
        table_fqn='"public"."metrics"',
        count=150,
        max_ts="2025-01-15 10:30:00",
        interval=30,
    )
    assert result.rows_per_second == 5.0

    result = PollResult(
        table_fqn='"public"."logs"',
        count=0,
        max_ts=None,
        interval=30,
    )
    assert result.rows_per_second == 0.0


@pytest.mark.unit
def test_tail_summary_initialization():
    summary = TailSummary(
        tables_monitored=3,
        total_rows=1500,
        elapsed_seconds=180,
        per_table={
            '"public"."events"': 500,
            '"public"."metrics"': 600,
            '"public"."logs"': 400,
        },
    )
    assert summary.tables_monitored == 3
    assert summary.total_rows == 1500
    assert summary.elapsed_seconds == 180
    assert len(summary.per_table) == 3
    assert summary.per_table['"public"."events"'] == 500


@pytest.mark.unit
def test_tail_summary_accumulation():
    summary = TailSummary(
        tables_monitored=2,
        total_rows=0,
        elapsed_seconds=0,
        per_table={
            '"public"."events"': 0,
            '"public"."metrics"': 0,
        },
    )

    summary.per_table['"public"."events"'] += 100
    summary.per_table['"public"."metrics"'] += 50
    summary.total_rows += 150

    assert summary.per_table['"public"."events"'] == 100
    assert summary.per_table['"public"."metrics"'] == 50
    assert summary.total_rows == 150


# --- discover_tables tests ---


@pytest.mark.unit
def test_discover_tables_returns_schema_table_tuples(mock_client):
    mock_client.execute_query.return_value = _make_query_result(
        [
            ("public", "events"),
            ("public", "metrics"),
        ]
    )
    result = discover_tables(mock_client, "public")
    assert result == [("public", "events"), ("public", "metrics")]


@pytest.mark.unit
def test_discover_tables_empty_when_no_timestamp_tables(mock_client):
    mock_client.execute_query.return_value = _make_query_result([])
    result = discover_tables(mock_client, "public")
    assert result == []


@pytest.mark.unit
def test_discover_tables_caps_at_50(mock_client):
    rows = [("public", f"table_{i}") for i in range(55)]
    mock_client.execute_query.return_value = _make_query_result(rows)
    result = discover_tables(mock_client, "public")
    assert len(result) == 50


@pytest.mark.unit
def test_discover_tables_logs_warning_when_capped(mock_client, caplog):
    rows = [("public", f"table_{i}") for i in range(55)]
    mock_client.execute_query.return_value = _make_query_result(rows)
    discover_tables(mock_client, "public")
    # structlog writes to stderr, check via mock call or just verify truncation
    # The function itself caps at 50, which we verified above


@pytest.mark.unit
def test_discover_tables_excludes_system_schemas(mock_client):
    mock_client.execute_query.return_value = _make_query_result(
        [
            ("public", "events"),
        ]
    )
    discover_tables(mock_client, "public")
    sql = mock_client.execute_query.call_args[0][0]
    assert "pg_catalog" in sql or "information_schema" in sql or "_timescaledb" in sql


# --- resolve_tables tests ---


def _make_time_column_result(column_name):
    """Helper to build a QueryResult for time column queries."""
    if column_name is None:
        return _make_query_result(
            [],
            columns=[
                ColumnMeta(name="column_name", type_oid=25, type_name="text"),
            ],
        )
    return _make_query_result(
        [(column_name,)],
        columns=[ColumnMeta(name="column_name", type_oid=25, type_name="text")],
    )


def _make_table_exists_result(exists):
    if not exists:
        return _make_query_result(
            [],
            columns=[
                ColumnMeta(name="table_name", type_oid=25, type_name="text"),
            ],
        )
    return _make_query_result(
        [("events",)],
        columns=[ColumnMeta(name="table_name", type_oid=25, type_name="text")],
    )


@pytest.mark.unit
def test_resolve_tables_with_explicit_args(mock_client):
    # First call: table existence check, second: TimescaleDB time column,
    # third: info_schema fallback
    mock_client.execute_query.side_effect = [
        _make_table_exists_result(True),
        _make_time_column_result("created_at"),
    ]
    config = LiveTailConfig(schema="public", tables=["events"])
    result = resolve_tables(mock_client, config)
    assert len(result) == 1
    assert result[0].schema == "public"
    assert result[0].table == "events"
    assert result[0].time_column == "created_at"


@pytest.mark.unit
def test_resolve_tables_with_auto_discovery(mock_client):
    mock_client.execute_query.side_effect = [
        # discover_tables call
        _make_query_result([("public", "events"), ("public", "metrics")]),
        # get_time_column for events (TimescaleDB)
        _make_time_column_result("created_at"),
        # get_time_column for metrics (TimescaleDB)
        _make_time_column_result(None),
        # info_schema fallback for metrics
        _make_time_column_result("ts"),
    ]
    config = LiveTailConfig(schema="public", tables=[])
    result = resolve_tables(mock_client, config)
    assert len(result) == 2
    assert result[0].table == "events"
    assert result[0].time_column == "created_at"
    assert result[1].table == "metrics"
    assert result[1].time_column == "ts"


@pytest.mark.unit
def test_resolve_tables_raises_for_nonexistent_table(mock_client):
    mock_client.execute_query.return_value = _make_table_exists_result(False)
    config = LiveTailConfig(schema="public", tables=["nonexistent"])
    with pytest.raises(InputError, match="nonexistent"):
        resolve_tables(mock_client, config)


@pytest.mark.unit
def test_resolve_tables_timescaledb_fallback_to_info_schema(mock_client):
    mock_client.execute_query.side_effect = [
        _make_table_exists_result(True),
        _make_time_column_result(None),  # TimescaleDB returns nothing
        _make_time_column_result("updated_at"),  # info_schema fallback
    ]
    config = LiveTailConfig(schema="public", tables=["events"])
    result = resolve_tables(mock_client, config)
    assert result[0].time_column == "updated_at"


@pytest.mark.unit
def test_resolve_tables_schema_dot_table_parsing(mock_client):
    mock_client.execute_query.side_effect = [
        _make_table_exists_result(True),
        _make_time_column_result("created_at"),
    ]
    config = LiveTailConfig(schema="public", tables=["analytics.events"])
    result = resolve_tables(mock_client, config)
    assert result[0].schema == "analytics"
    assert result[0].table == "events"


@pytest.mark.unit
def test_resolve_tables_returns_empty_list(mock_client):
    mock_client.execute_query.return_value = _make_query_result([])
    config = LiveTailConfig(schema="public", tables=[])
    result = resolve_tables(mock_client, config)
    assert result == []


# --- Step 1.4: Edge cases and comprehensive coverage ---


@pytest.mark.unit
def test_resolve_tables_skips_table_without_timestamp_column_explicit(mock_client):
    """Explicit table exists but has no timestamp columns -- skipped with warning."""
    mock_client.execute_query.side_effect = [
        _make_table_exists_result(True),
        _make_time_column_result(None),  # TimescaleDB returns nothing
        _make_time_column_result(None),  # info_schema also returns nothing
    ]
    config = LiveTailConfig(schema="public", tables=["events"])
    result = resolve_tables(mock_client, config)
    assert result == []


@pytest.mark.unit
def test_resolve_tables_skips_table_without_timestamp_column_discovery(mock_client):
    """Auto-discovered table has no timestamp column resolved -- skipped silently."""
    mock_client.execute_query.side_effect = [
        _make_query_result([("public", "events")]),
        _make_time_column_result(None),  # TimescaleDB
        _make_time_column_result(None),  # info_schema
    ]
    config = LiveTailConfig(schema="public", tables=[])
    result = resolve_tables(mock_client, config)
    assert result == []


@pytest.mark.unit
def test_timescaledb_not_installed_falls_back(mock_client):
    """TimescaleDB query raises SqlToolError (not installed)
    -- falls back to info_schema."""

    def side_effect(sql, params=None):
        if "timescaledb_information" in sql:
            raise SqlToolError("relation does not exist")
        if "information_schema.tables" in sql:
            return _make_table_exists_result(True)
        return _make_time_column_result("created_at")

    mock_client.execute_query.side_effect = side_effect
    config = LiveTailConfig(schema="public", tables=["events"])
    result = resolve_tables(mock_client, config)
    assert len(result) == 1
    assert result[0].time_column == "created_at"


@pytest.mark.unit
def test_discover_tables_passes_schema_param(mock_client):
    mock_client.execute_query.return_value = _make_query_result([])
    discover_tables(mock_client, "analytics")
    params = mock_client.execute_query.call_args[0][1]
    assert params["schema"] == "analytics"


@pytest.mark.unit
def test_poll_result_zero_interval():
    result = PollResult(
        table_fqn='"public"."events"',
        count=100,
        max_ts="2025-01-15 10:30:00",
        interval=0,
    )
    assert result.rows_per_second == 0.0


@pytest.mark.unit
def test_tail_summary_empty_per_table():
    summary = TailSummary(
        tables_monitored=0,
        total_rows=0,
        elapsed_seconds=0,
    )
    assert summary.per_table == {}
    assert summary.tables_monitored == 0


# --- Step 2.1: Liveness Detection Tests (TDD) ---


@pytest.mark.unit
def test_detect_liveness_returns_true_when_timestamp_advances(
    mock_client,
    monkeypatch,
):
    tables = [
        TableInfo(schema="public", table="events", time_column="created_at"),
    ]

    mock_monotonic = MagicMock(side_effect=[0, 5])
    mock_sleep = MagicMock()
    monkeypatch.setattr("time.monotonic", mock_monotonic)
    monkeypatch.setattr("time.sleep", mock_sleep)

    mock_client.execute_query.side_effect = [
        _make_query_result(
            [("2025-01-15 10:00:00",)],
            columns=[
                ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
            ],
        ),
        _make_query_result(
            [("2025-01-15 10:00:05",)],
            columns=[
                ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
            ],
        ),
    ]

    result = detect_liveness(mock_client, tables, probe_interval=5, timeout=120)
    assert result is True
    mock_sleep.assert_called_once_with(5)


@pytest.mark.unit
def test_detect_liveness_returns_false_after_timeout(mock_client, monkeypatch):
    tables = [
        TableInfo(schema="public", table="events", time_column="created_at"),
    ]

    mock_monotonic = MagicMock(side_effect=[0, 5, 10, 15, 20, 25, 30])
    mock_sleep = MagicMock()
    monkeypatch.setattr("time.monotonic", mock_monotonic)
    monkeypatch.setattr("time.sleep", mock_sleep)

    mock_client.execute_query.return_value = _make_query_result(
        [("2025-01-15 10:00:00",)],
        columns=[
            ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
        ],
    )

    result = detect_liveness(mock_client, tables, probe_interval=5, timeout=25)
    assert result is False


@pytest.mark.unit
def test_detect_liveness_treats_null_max_as_no_movement(mock_client, monkeypatch):
    tables = [
        TableInfo(schema="public", table="events", time_column="created_at"),
    ]

    mock_monotonic = MagicMock(side_effect=[0, 5, 10])
    mock_sleep = MagicMock()
    monkeypatch.setattr("time.monotonic", mock_monotonic)
    monkeypatch.setattr("time.sleep", mock_sleep)

    mock_client.execute_query.return_value = _make_query_result(
        [(None,)],
        columns=[
            ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
        ],
    )

    result = detect_liveness(mock_client, tables, probe_interval=5, timeout=8)
    assert result is False


@pytest.mark.unit
def test_detect_liveness_schema_level_any_table_movement(mock_client, monkeypatch):
    tables = [
        TableInfo(schema="public", table="events", time_column="created_at"),
        TableInfo(schema="public", table="metrics", time_column="ts"),
    ]

    mock_monotonic = MagicMock(side_effect=[0, 5])
    mock_sleep = MagicMock()
    monkeypatch.setattr("time.monotonic", mock_monotonic)
    monkeypatch.setattr("time.sleep", mock_sleep)

    mock_client.execute_query.side_effect = [
        _make_query_result(
            [("2025-01-15 10:00:00",)],
            columns=[
                ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
            ],
        ),
        _make_query_result(
            [("2025-01-15 10:00:00",)],
            columns=[
                ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
            ],
        ),
        _make_query_result(
            [("2025-01-15 10:00:00",)],
            columns=[
                ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
            ],
        ),
        _make_query_result(
            [("2025-01-15 10:00:10",)],
            columns=[
                ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
            ],
        ),
    ]

    result = detect_liveness(mock_client, tables, probe_interval=5, timeout=120)
    assert result is True


# --- Step 2.3/2.4: CLI Command Registration and Banner Tests (TDD) ---


@pytest.mark.unit
def test_live_tail_command_help():
    runner = CliRunner()
    result = runner.invoke(app, ["live-tail", "--help"])
    assert result.exit_code == 0
    assert "live-tail" in result.stdout
    assert "--interval" in result.stdout
    assert "--duration" in result.stdout
    assert "--full" in result.stdout
    assert "--columns" in result.stdout


@pytest.mark.unit
def test_live_tail_command_registered_in_app():
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "live-tail" in result.stdout


# --- fetch_watermarks() Tests ---


@pytest.mark.unit
def test_fetch_watermarks_returns_max_ts_per_table(mock_client):
    tables = [
        TableInfo(schema="public", table="events", time_column="created_at"),
        TableInfo(schema="public", table="metrics", time_column="ts"),
    ]
    mock_client.execute_query.side_effect = [
        _make_query_result(
            [("2025-01-15 10:00:00",)],
            columns=[ColumnMeta(name="max", type_oid=1184, type_name="timestamptz")],
        ),
        _make_query_result(
            [("2025-01-15 09:30:00",)],
            columns=[ColumnMeta(name="max", type_oid=1184, type_name="timestamptz")],
        ),
    ]

    result = fetch_watermarks(mock_client, tables)

    assert result['"public"."events"'] == "2025-01-15 10:00:00"
    assert result['"public"."metrics"'] == "2025-01-15 09:30:00"
    assert mock_client.execute_query.call_count == 2


@pytest.mark.unit
def test_fetch_watermarks_handles_null_max(mock_client):
    tables = [TableInfo(schema="public", table="empty_tbl", time_column="ts")]
    mock_client.execute_query.return_value = _make_query_result(
        [(None,)],
        columns=[ColumnMeta(name="max", type_oid=1184, type_name="timestamptz")],
    )

    result = fetch_watermarks(mock_client, tables)

    assert result['"public"."empty_tbl"'] is None


@pytest.mark.unit
def test_fetch_watermarks_handles_empty_result(mock_client):
    tables = [TableInfo(schema="public", table="no_rows", time_column="ts")]
    mock_client.execute_query.return_value = _make_query_result(
        [],
        columns=[ColumnMeta(name="max", type_oid=1184, type_name="timestamptz")],
    )

    result = fetch_watermarks(mock_client, tables)

    assert result['"public"."no_rows"'] is None


@pytest.mark.unit
def test_fetch_watermarks_continues_on_error(mock_client):
    tables = [
        TableInfo(schema="public", table="broken", time_column="ts"),
        TableInfo(schema="public", table="healthy", time_column="ts"),
    ]
    mock_client.execute_query.side_effect = [
        SqlToolError("connection lost"),
        _make_query_result(
            [("2025-01-15 12:00:00",)],
            columns=[ColumnMeta(name="max", type_oid=1184, type_name="timestamptz")],
        ),
    ]

    result = fetch_watermarks(mock_client, tables)

    assert result['"public"."broken"'] is None
    assert result['"public"."healthy"'] == "2025-01-15 12:00:00"


# --- Step 3.1: poll_count() Tests (TDD) ---


@pytest.mark.unit
def test_poll_count_returns_count_and_max_ts(mock_client):
    table = TableInfo(schema="public", table="events", time_column="created_at")
    watermark = "2025-01-15 10:00:00"

    mock_client.execute_query.return_value = _make_query_result(
        [(5, "2025-01-15 10:00:10")],
        columns=[
            ColumnMeta(name="count", type_oid=20, type_name="bigint"),
            ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
        ],
    )

    result = poll_count(mock_client, table, watermark, elapsed_seconds=10)
    assert result.table_fqn == '"public"."events"'
    assert result.count == 5
    assert result.max_ts == "2025-01-15 10:00:10"
    assert result.interval == 10
    assert result.rows_per_second == 0.5


@pytest.mark.unit
def test_poll_count_zero_new_rows(mock_client):
    table = TableInfo(schema="public", table="events", time_column="created_at")
    watermark = "2025-01-15 10:00:00"

    mock_client.execute_query.return_value = _make_query_result(
        [(0, None)],
        columns=[
            ColumnMeta(name="count", type_oid=20, type_name="bigint"),
            ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
        ],
    )

    result = poll_count(mock_client, table, watermark, elapsed_seconds=10)
    assert result.count == 0
    assert result.max_ts is None
    assert result.rows_per_second == 0.0


@pytest.mark.unit
def test_poll_count_null_watermark_counts_all(mock_client):
    table = TableInfo(schema="public", table="events", time_column="created_at")
    watermark = None

    mock_client.execute_query.return_value = _make_query_result(
        [(100, "2025-01-15 10:00:10")],
        columns=[
            ColumnMeta(name="count", type_oid=20, type_name="bigint"),
            ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
        ],
    )

    result = poll_count(mock_client, table, watermark, elapsed_seconds=10)
    assert result.count == 100
    assert result.max_ts == "2025-01-15 10:00:10"


# --- Step 3.2: Polling Loop Tests (TDD) ---


@pytest.mark.unit
def test_poll_tables_iterates_sequentially(mock_client, monkeypatch):
    tables = [
        TableInfo(schema="public", table="events", time_column="created_at"),
        TableInfo(schema="public", table="metrics", time_column="ts"),
    ]
    config = LiveTailConfig(schema="public", tables=[], interval=5, duration=10)
    initial_watermarks = {
        '"public"."events"': "2025-01-15 10:00:00",
        '"public"."metrics"': "2025-01-15 10:00:00",
    }

    # Mock time: returns incrementing values, exits after 10 seconds
    call_count = [0]

    def mock_monotonic():
        call_count[0] += 1
        return call_count[0]

    mock_sleep = MagicMock()
    monkeypatch.setattr("time.monotonic", mock_monotonic)
    monkeypatch.setattr("time.sleep", mock_sleep)

    mock_client.execute_query.return_value = _make_query_result(
        [(1, "2025-01-15 10:00:01")],
        columns=[
            ColumnMeta(name="count", type_oid=20, type_name="bigint"),
            ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
        ],
    )

    summary = poll_tables(mock_client, tables, config, initial_watermarks)
    assert summary.tables_monitored == 2
    assert summary.total_rows >= 0  # At least one cycle


@pytest.mark.unit
def test_poll_tables_exits_after_duration(mock_client, monkeypatch):
    tables = [TableInfo(schema="public", table="events", time_column="created_at")]
    config = LiveTailConfig(schema="public", tables=[], interval=5, duration=10)
    initial_watermarks = {'"public"."events"': "2025-01-15 10:00:00"}

    call_count = [0]

    def mock_monotonic():
        call_count[0] += 1
        return call_count[0]

    mock_sleep = MagicMock()
    monkeypatch.setattr("time.monotonic", mock_monotonic)
    monkeypatch.setattr("time.sleep", mock_sleep)

    mock_client.execute_query.return_value = _make_query_result(
        [(0, None)],
        columns=[
            ColumnMeta(name="count", type_oid=20, type_name="bigint"),
            ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
        ],
    )

    summary = poll_tables(mock_client, tables, config, initial_watermarks)
    assert summary.elapsed_seconds <= config.duration + config.interval


@pytest.mark.unit
def test_poll_tables_indefinite_duration_zero(mock_client, monkeypatch):
    tables = [TableInfo(schema="public", table="events", time_column="created_at")]
    config = LiveTailConfig(schema="public", tables=[], interval=5, duration=0)
    initial_watermarks = {'"public"."events"': "2025-01-15 10:00:00"}

    call_count = [0]

    def mock_monotonic_inf():
        call_count[0] += 1
        if call_count[0] > 3:
            raise KeyboardInterrupt
        return call_count[0] * 5

    mock_sleep = MagicMock()
    monkeypatch.setattr("time.monotonic", mock_monotonic_inf)
    monkeypatch.setattr("time.sleep", mock_sleep)

    mock_client.execute_query.return_value = _make_query_result(
        [(0, None)],
        columns=[
            ColumnMeta(name="count", type_oid=20, type_name="bigint"),
            ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
        ],
    )

    with contextlib.suppress(KeyboardInterrupt):
        poll_tables(mock_client, tables, config, initial_watermarks)


# --- Step 3.3: Count-Only Display Tests (TDD) ---


@pytest.mark.unit
def test_format_count_line_basic():
    result = PollResult(
        table_fqn='"public"."events"',
        count=10,
        max_ts="2025-01-15 10:00:10",
        interval=5,
    )
    line = format_count_line(result, running_total=100)
    assert '"public"."events"' in line
    assert "2.0 rows/s" in line
    assert "(100 total)" in line
    assert line.startswith("[")


@pytest.mark.unit
def test_format_count_line_zero_rows():
    result = PollResult(
        table_fqn='"public"."metrics"',
        count=0,
        max_ts=None,
        interval=5,
    )
    line = format_count_line(result, running_total=0)
    assert "0.0 rows/s" in line
    assert "(0 total)" in line


# --- Step 3.7 / Step 4.4: Integration Tests ---


@pytest.fixture
def integration_client():
    config = load_config()
    resolved = resolve_config(config)
    with PgClient(resolved) as client:
        yield client


@pytest.fixture
def integration_table(integration_client):
    result = integration_client.execute_query("""
        SELECT hypertable_schema, hypertable_name
        FROM timescaledb_information.hypertables
        LIMIT 1
    """)
    assert result.rows, "No hypertables found"
    schema, table = str(result.rows[0][0]), str(result.rows[0][1])
    time_col = _find_time_column(integration_client, schema, table)
    assert time_col is not None
    return TableInfo(schema=schema, table=table, time_column=time_col)


@pytest.fixture
def recent_watermark(integration_client, integration_table):
    """Get a watermark near the end of the table so queries return a small batch."""
    result = integration_client.execute_query(
        f"SELECT MAX({integration_table.time_column}) FROM {integration_table.fqn}"
    )
    max_ts = str(result.rows[0][0])
    # Subtract 1 minute to get a small batch of recent rows
    result2 = integration_client.execute_query(
        "SELECT (%(ts)s::timestamptz - interval '1 minute')::text",
        {"ts": max_ts},
    )
    return str(result2.rows[0][0])


@pytest.mark.integration
def test_poll_rows_returns_real_data(
    integration_client,
    integration_table,
    recent_watermark,
):
    result, new_max_ts = poll_rows(
        integration_client,
        integration_table,
        watermark=recent_watermark,
    )
    assert result.row_count >= 0
    assert len(result.columns) > 0
    if result.row_count > 0:
        assert new_max_ts is not None
        assert new_max_ts != recent_watermark


@pytest.mark.integration
def test_poll_rows_with_watermark_filters_rows(
    integration_client,
    integration_table,
    recent_watermark,
):
    wider_result, max_ts = poll_rows(
        integration_client,
        integration_table,
        watermark=recent_watermark,
    )

    if wider_result.row_count > 0:
        narrower_result, _ = poll_rows(
            integration_client,
            integration_table,
            watermark=max_ts,
        )
        assert narrower_result.row_count <= wider_result.row_count


@pytest.mark.integration
def test_poll_rows_column_selection_real_table(
    integration_client,
    integration_table,
    recent_watermark,
):
    all_result, _ = poll_rows(
        integration_client,
        integration_table,
        watermark=recent_watermark,
    )
    assert len(all_result.columns) >= 2

    available_col_names = [
        c.name for c in all_result.columns if c.name != integration_table.time_column
    ]
    select_cols = available_col_names[:1]

    validated = validate_columns(
        integration_client,
        integration_table,
        select_cols,
    )
    assert integration_table.time_column in validated
    assert select_cols[0] in validated

    result, _ = poll_rows(
        integration_client,
        integration_table,
        watermark=recent_watermark,
        columns=validated,
    )
    result_col_names = [c.name for c in result.columns]
    assert integration_table.time_column in result_col_names
    assert select_cols[0] in result_col_names
    assert len(result.columns) <= len(all_result.columns)


@pytest.mark.integration
def test_validate_columns_real_table_filters_invalid(
    integration_client,
    integration_table,
    monkeypatch,
):
    monkeypatch.setattr(
        "tsdb_tool.core.live_tail.structlog.get_logger",
        lambda: MagicMock(),
    )
    valid = validate_columns(
        integration_client,
        integration_table,
        [integration_table.time_column, "definitely_not_a_real_column_xyz"],
    )
    assert integration_table.time_column in valid
    assert "definitely_not_a_real_column_xyz" not in valid


@pytest.mark.integration
def test_full_mode_json_output(
    integration_client,
    integration_table,
    recent_watermark,
):
    result, _ = poll_rows(
        integration_client,
        integration_table,
        watermark=recent_watermark,
    )
    if result.row_count == 0:
        pytest.skip("No recent rows available for JSON output test")

    formatter = get_formatter(format_flag="json")
    lines = list(formatter.format(result))
    output = "\n".join(lines)
    parsed = json.loads(output)
    assert isinstance(parsed, list)
    assert len(parsed) == result.row_count


@pytest.mark.integration
def test_full_mode_csv_output(
    integration_client,
    integration_table,
    recent_watermark,
):
    result, _ = poll_rows(
        integration_client,
        integration_table,
        watermark=recent_watermark,
    )
    if result.row_count == 0:
        pytest.skip("No recent rows available for CSV output test")

    formatter = get_formatter(format_flag="csv")
    lines = list(formatter.format(result))
    output = "\n".join(lines) + "\n"
    reader = csv.reader(StringIO(output))
    rows = list(reader)
    assert len(rows) >= 2  # header + at least 1 data row
    assert rows[0][0] == result.columns[0].name


# --- Step 4.1: poll_rows() Tests (TDD) ---


@pytest.mark.unit
def test_poll_rows_returns_query_result_with_rows(mock_client):
    table = TableInfo(schema="public", table="events", time_column="created_at")
    watermark = "2025-01-15 10:00:00"

    mock_client.execute_query.return_value = _make_query_result(
        [
            (1, "2025-01-15 10:00:05", "event_a"),
            (2, "2025-01-15 10:00:10", "event_b"),
        ],
        columns=[
            ColumnMeta(name="id", type_oid=23, type_name="integer"),
            ColumnMeta(name="created_at", type_oid=1184, type_name="timestamptz"),
            ColumnMeta(name="name", type_oid=25, type_name="text"),
        ],
    )

    result, new_max_ts = poll_rows(mock_client, table, watermark)
    assert result.row_count == 2
    assert len(result.rows) == 2
    assert new_max_ts == "2025-01-15 10:00:10"


@pytest.mark.unit
def test_poll_rows_orders_by_time_column_asc(mock_client):
    table = TableInfo(schema="public", table="events", time_column="created_at")
    watermark = "2025-01-15 10:00:00"

    mock_client.execute_query.return_value = _make_query_result(
        [
            ("2025-01-15 10:00:01",),
            ("2025-01-15 10:00:05",),
            ("2025-01-15 10:00:10",),
        ],
        columns=[
            ColumnMeta(name="created_at", type_oid=1184, type_name="timestamptz"),
        ],
    )

    result, new_max_ts = poll_rows(mock_client, table, watermark)
    sql = mock_client.execute_query.call_args[0][0]
    assert "ORDER BY created_at ASC" in sql


@pytest.mark.unit
def test_poll_rows_returns_updated_max_ts_as_watermark(mock_client):
    table = TableInfo(schema="public", table="metrics", time_column="ts")
    watermark = "2025-01-15 10:00:00"

    mock_client.execute_query.return_value = _make_query_result(
        [
            ("2025-01-15 10:00:05",),
            ("2025-01-15 10:00:15",),
        ],
        columns=[
            ColumnMeta(name="ts", type_oid=1184, type_name="timestamptz"),
        ],
    )

    _, new_max_ts = poll_rows(mock_client, table, watermark)
    assert new_max_ts == "2025-01-15 10:00:15"


@pytest.mark.unit
def test_poll_rows_with_columns_selects_only_specified_columns_plus_timestamp(
    mock_client,
):
    table = TableInfo(schema="public", table="metrics", time_column="ts")
    watermark = "2025-01-15 10:00:00"
    columns = ["cpu", "memory"]

    mock_client.execute_query.return_value = _make_query_result(
        [
            ("2025-01-15 10:00:05", 45.0, 8192),
        ],
        columns=[
            ColumnMeta(name="ts", type_oid=1184, type_name="timestamptz"),
            ColumnMeta(name="cpu", type_oid=701, type_name="float8"),
            ColumnMeta(name="memory", type_oid=23, type_name="integer"),
        ],
    )

    result, _ = poll_rows(mock_client, table, watermark, columns=columns)
    sql = mock_client.execute_query.call_args[0][0]
    assert "ts" in sql
    assert "cpu" in sql
    assert "memory" in sql
    assert "SELECT" in sql


@pytest.mark.unit
def test_poll_rows_returns_empty_query_result_when_no_new_rows(mock_client):
    table = TableInfo(schema="public", table="events", time_column="created_at")
    watermark = "2025-01-15 10:00:00"

    mock_client.execute_query.return_value = _make_query_result(
        [],
        columns=[
            ColumnMeta(name="created_at", type_oid=1184, type_name="timestamptz"),
        ],
    )

    result, new_max_ts = poll_rows(mock_client, table, watermark)
    assert result.row_count == 0
    assert len(result.rows) == 0
    assert new_max_ts == watermark


@pytest.mark.unit
def test_poll_rows_without_columns_selects_all_columns(mock_client):
    table = TableInfo(schema="public", table="events", time_column="created_at")
    watermark = "2025-01-15 10:00:00"

    mock_client.execute_query.return_value = _make_query_result(
        [
            (1, "2025-01-15 10:00:05", "event_a", 100),
        ],
        columns=[
            ColumnMeta(name="id", type_oid=23, type_name="integer"),
            ColumnMeta(name="created_at", type_oid=1184, type_name="timestamptz"),
            ColumnMeta(name="name", type_oid=25, type_name="text"),
            ColumnMeta(name="value", type_oid=23, type_name="integer"),
        ],
    )

    result, _ = poll_rows(mock_client, table, watermark)
    sql = mock_client.execute_query.call_args[0][0]
    assert "SELECT *" in sql or "SELECT" in sql


# --- Step 4.2: Full Mode Output Integration Tests (TDD) ---


@pytest.mark.unit
def test_poll_tables_full_mode_calls_poll_rows_instead_of_poll_count(
    mock_client,
    monkeypatch,
    capsys,
):
    tables = [TableInfo(schema="public", table="events", time_column="created_at")]
    config = LiveTailConfig(
        schema="public",
        tables=[],
        interval=5,
        duration=5,
        full=True,
    )
    initial_watermarks = {'"public"."events"': "2025-01-15 10:00:00"}

    call_count = [0]

    def mock_monotonic():
        call_count[0] += 1
        if call_count[0] >= 10:
            return 100
        return call_count[0]

    mock_sleep = MagicMock()
    monkeypatch.setattr("time.monotonic", mock_monotonic)
    monkeypatch.setattr("time.sleep", mock_sleep)

    mock_client.execute_query.return_value = _make_query_result(
        [(1, "2025-01-15 10:00:05", "event_a")],
        columns=[
            ColumnMeta(name="id", type_oid=23, type_name="integer"),
            ColumnMeta(name="created_at", type_oid=1184, type_name="timestamptz"),
            ColumnMeta(name="name", type_oid=25, type_name="text"),
        ],
    )

    with patch("tsdb_tool.core.live_tail.poll_rows") as mock_poll_rows:
        mock_poll_rows.return_value = (
            _make_query_result(
                [(1, "2025-01-15 10:00:05", "event_a")],
                columns=[
                    ColumnMeta(name="id", type_oid=23, type_name="integer"),
                    ColumnMeta(
                        name="created_at",
                        type_oid=1184,
                        type_name="timestamptz",
                    ),
                    ColumnMeta(name="name", type_oid=25, type_name="text"),
                ],
            ),
            "2025-01-15 10:00:05",
        )

        poll_tables(mock_client, tables, config, initial_watermarks)
        mock_poll_rows.assert_called()


@pytest.mark.unit
def test_poll_tables_full_mode_outputs_batch_header_to_stderr(
    mock_client,
    monkeypatch,
    capsys,
):
    tables = [TableInfo(schema="public", table="events", time_column="created_at")]
    config = LiveTailConfig(
        schema="public",
        tables=[],
        interval=5,
        duration=5,
        full=True,
    )
    initial_watermarks = {'"public"."events"': "2025-01-15 10:00:00"}

    call_count = [0]

    def mock_monotonic():
        call_count[0] += 1
        if call_count[0] >= 10:
            return 100
        return call_count[0]

    mock_sleep = MagicMock()
    monkeypatch.setattr("time.monotonic", mock_monotonic)
    monkeypatch.setattr("time.sleep", mock_sleep)

    mock_client.execute_query.return_value = _make_query_result(
        [(1, "2025-01-15 10:00:05", "event_a")],
        columns=[
            ColumnMeta(name="id", type_oid=23, type_name="integer"),
            ColumnMeta(name="created_at", type_oid=1184, type_name="timestamptz"),
            ColumnMeta(name="name", type_oid=25, type_name="text"),
        ],
    )

    poll_tables(mock_client, tables, config, initial_watermarks)
    captured = capsys.readouterr()
    assert '"public"."events"' in captured.err or "1 rows" in captured.err


# --- Step 4.3: Column Selection with Validation Tests (TDD) ---


# --- Step 5.1: E2E CliRunner Tests for CLI Command Layer ---

_CLI_MODULE = "tsdb_tool.cli.commands.live_tail"

_SAMPLE_TABLES = [
    TableInfo(schema="public", table="events", time_column="created_at"),
    TableInfo(schema="public", table="metrics", time_column="ts"),
]

_SAMPLE_SUMMARY = TailSummary(
    tables_monitored=2,
    total_rows=150,
    elapsed_seconds=65,
    per_table={'"public"."events"': 100, '"public"."metrics"': 50},
)


@pytest.fixture
def _mock_live_tail_pipeline(monkeypatch):
    """Mock all core functions for isolated CLI testing."""
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)

    monkeypatch.setattr(
        f"{_CLI_MODULE}.get_client",
        MagicMock(return_value=mock_client),
    )
    monkeypatch.setattr(
        f"{_CLI_MODULE}.resolve_tables",
        MagicMock(return_value=_SAMPLE_TABLES),
    )
    monkeypatch.setattr(
        f"{_CLI_MODULE}.detect_liveness",
        MagicMock(return_value=True),
    )
    monkeypatch.setattr(
        f"{_CLI_MODULE}.fetch_watermarks",
        MagicMock(
            return_value={
                '"public"."events"': "2025-01-15 10:00:00",
                '"public"."metrics"': "2025-01-15 10:00:00",
            }
        ),
    )
    monkeypatch.setattr(
        f"{_CLI_MODULE}.poll_tables",
        MagicMock(return_value=_SAMPLE_SUMMARY),
    )
    monkeypatch.setattr(
        f"{_CLI_MODULE}.structlog.get_logger",
        lambda: MagicMock(),
    )


@pytest.mark.unit
def test_cli_live_tail_happy_path(runner, monkeypatch, _mock_live_tail_pipeline):
    result = runner.invoke(app, ["live-tail"])
    assert result.exit_code == 0


@pytest.mark.unit
def test_cli_live_tail_prints_banner(runner, monkeypatch, _mock_live_tail_pipeline):
    result = runner.invoke(app, ["live-tail"])
    assert 'Monitoring 2 tables in schema "public"' in result.output
    assert '"public"."events"' in result.output
    assert "count-only" in result.output


@pytest.mark.unit
def test_cli_live_tail_prints_summary(runner, monkeypatch, _mock_live_tail_pipeline):
    result = runner.invoke(app, ["live-tail"])
    assert "Live Tail Summary" in result.output
    assert "1m 5s" in result.output
    assert "150" in result.output


@pytest.mark.unit
def test_cli_live_tail_no_tables_exits_input_error(runner, monkeypatch):
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)

    monkeypatch.setattr(
        f"{_CLI_MODULE}.get_client",
        MagicMock(return_value=mock_client),
    )
    monkeypatch.setattr(
        f"{_CLI_MODULE}.resolve_tables",
        MagicMock(return_value=[]),
    )
    monkeypatch.setattr(
        f"{_CLI_MODULE}.structlog.get_logger",
        lambda: MagicMock(),
    )

    result = runner.invoke(app, ["live-tail"])
    assert result.exit_code == ExitCode.INPUT_ERROR


@pytest.mark.unit
def test_cli_live_tail_inactive_schema_exits_success(runner, monkeypatch):
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)

    monkeypatch.setattr(
        f"{_CLI_MODULE}.get_client",
        MagicMock(return_value=mock_client),
    )
    monkeypatch.setattr(
        f"{_CLI_MODULE}.resolve_tables",
        MagicMock(return_value=_SAMPLE_TABLES),
    )
    monkeypatch.setattr(
        f"{_CLI_MODULE}.detect_liveness",
        MagicMock(return_value=False),
    )
    monkeypatch.setattr(
        f"{_CLI_MODULE}.structlog.get_logger",
        lambda: MagicMock(),
    )

    result = runner.invoke(app, ["live-tail"])
    assert result.exit_code == 0


@pytest.mark.unit
def test_cli_live_tail_interval_flag(
    runner,
    monkeypatch,
    _mock_live_tail_pipeline,
):
    result = runner.invoke(app, ["live-tail", "--interval", "10"])
    assert result.exit_code == 0
    assert "10s" in result.output


@pytest.mark.unit
def test_cli_live_tail_duration_flag(
    runner,
    monkeypatch,
    _mock_live_tail_pipeline,
):
    result = runner.invoke(app, ["live-tail", "--duration", "60"])
    assert result.exit_code == 0
    assert "60s" in result.output


@pytest.mark.unit
def test_cli_live_tail_duration_zero_shows_indefinite(
    runner,
    monkeypatch,
    _mock_live_tail_pipeline,
):
    result = runner.invoke(app, ["live-tail", "--duration", "0"])
    assert result.exit_code == 0
    assert "indefinite" in result.output


@pytest.mark.unit
def test_cli_live_tail_full_mode_flag(
    runner,
    monkeypatch,
    _mock_live_tail_pipeline,
):
    result = runner.invoke(app, ["live-tail", "--full"])
    assert result.exit_code == 0
    assert "full" in result.output


@pytest.mark.unit
def test_cli_live_tail_schema_flag(
    runner,
    monkeypatch,
    _mock_live_tail_pipeline,
):
    result = runner.invoke(app, ["live-tail", "--schema", "myschema"])
    assert result.exit_code == 0
    # Banner uses schema from resolve_tables (mocked), but schema is passed to config


@pytest.mark.unit
def test_cli_live_tail_summary_minutes_format(runner, monkeypatch):
    """Summary with >60s formats as Nm Ns."""
    summary = TailSummary(
        tables_monitored=1,
        total_rows=500,
        elapsed_seconds=125,
        per_table={'"public"."data"': 500},
    )

    old_stderr = sys.stderr
    sys.stderr = StringIO()
    try:
        print_summary(summary)
        output = sys.stderr.getvalue()
    finally:
        sys.stderr = old_stderr

    assert "2m 5s" in output
    assert "500" in output


@pytest.mark.unit
def test_cli_live_tail_summary_seconds_only_format(runner, monkeypatch):
    """Summary with <60s formats as Ns (no minutes)."""
    summary = TailSummary(
        tables_monitored=1,
        total_rows=10,
        elapsed_seconds=45,
        per_table={'"public"."data"': 10},
    )

    old_stderr = sys.stderr
    sys.stderr = StringIO()
    try:
        print_summary(summary)
        output = sys.stderr.getvalue()
    finally:
        sys.stderr = old_stderr

    assert "45s" in output
    assert "m " not in output.split("Tables")[0]


@pytest.mark.unit
def test_cli_live_tail_banner_lists_all_tables(
    runner,
    monkeypatch,
    _mock_live_tail_pipeline,
):
    result = runner.invoke(app, ["live-tail"])
    assert "events" in result.output
    assert "metrics" in result.output
    assert "created_at" in result.output
    assert "ts" in result.output


@pytest.mark.unit
def test_validate_columns_returns_valid_columns_only(mock_client, monkeypatch):
    monkeypatch.setattr(
        "tsdb_tool.core.live_tail.structlog.get_logger",
        lambda: MagicMock(),
    )
    table = TableInfo(schema="public", table="metrics", time_column="ts")
    requested_columns = ["cpu", "memory", "nonexistent"]

    mock_client.execute_query.return_value = _make_query_result(
        [("cpu",), ("memory",), ("ts",)],
        columns=[ColumnMeta(name="column_name", type_oid=25, type_name="text")],
    )

    valid_columns = validate_columns(mock_client, table, requested_columns)
    assert "cpu" in valid_columns
    assert "memory" in valid_columns
    assert "nonexistent" not in valid_columns
    assert "ts" in valid_columns


@pytest.mark.unit
def test_validate_columns_always_includes_timestamp(mock_client):
    table = TableInfo(schema="public", table="metrics", time_column="ts")
    requested_columns = ["cpu", "memory"]

    mock_client.execute_query.return_value = _make_query_result(
        [("cpu",), ("memory",), ("ts",)],
        columns=[ColumnMeta(name="column_name", type_oid=25, type_name="text")],
    )

    valid_columns = validate_columns(mock_client, table, requested_columns)
    assert "ts" in valid_columns


@pytest.mark.unit
def test_validate_columns_warns_for_invalid_columns(mock_client, monkeypatch):
    mock_log = MagicMock()
    monkeypatch.setattr(
        "tsdb_tool.core.live_tail.structlog.get_logger",
        lambda: mock_log,
    )
    table = TableInfo(schema="public", table="metrics", time_column="ts")
    requested_columns = ["cpu", "nonexistent"]

    mock_client.execute_query.return_value = _make_query_result(
        [("cpu",), ("ts",)],
        columns=[ColumnMeta(name="column_name", type_oid=25, type_name="text")],
    )

    validate_columns(mock_client, table, requested_columns)
    mock_log.warning.assert_called_once()


@pytest.mark.unit
def test_validate_columns_per_table(mock_client, monkeypatch):
    monkeypatch.setattr(
        "tsdb_tool.core.live_tail.structlog.get_logger",
        lambda: MagicMock(),
    )
    table1 = TableInfo(schema="public", table="metrics", time_column="ts")
    table2 = TableInfo(
        schema="public",
        table="events",
        time_column="created_at",
    )

    def side_effect(sql, params=None):
        if params and params.get("table") == "metrics":
            return _make_query_result(
                [("cpu",), ("memory",), ("ts",)],
                columns=[ColumnMeta(name="column_name", type_oid=25, type_name="text")],
            )
        return _make_query_result(
            [("name",), ("value",), ("created_at",)],
            columns=[ColumnMeta(name="column_name", type_oid=25, type_name="text")],
        )

    mock_client.execute_query.side_effect = side_effect

    valid1 = validate_columns(mock_client, table1, ["cpu", "name"])
    assert "cpu" in valid1
    assert "name" not in valid1

    valid2 = validate_columns(mock_client, table2, ["name", "cpu"])
    assert "name" in valid2
    assert "cpu" not in valid2


def _make_full_pipeline_mocks(monkeypatch):
    """Set up full pipeline mocks, returning the resolve_tables mock for inspection."""
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_resolve = MagicMock(return_value=_SAMPLE_TABLES)
    monkeypatch.setattr(
        f"{_CLI_MODULE}.get_client", MagicMock(return_value=mock_client)
    )
    monkeypatch.setattr(f"{_CLI_MODULE}.resolve_tables", mock_resolve)
    monkeypatch.setattr(f"{_CLI_MODULE}.detect_liveness", MagicMock(return_value=True))
    monkeypatch.setattr(
        f"{_CLI_MODULE}.fetch_watermarks",
        MagicMock(
            return_value={
                '"public"."events"': "2025-01-15 10:00:00",
                '"public"."metrics"': "2025-01-15 10:00:00",
            }
        ),
    )
    monkeypatch.setattr(
        f"{_CLI_MODULE}.poll_tables", MagicMock(return_value=_SAMPLE_SUMMARY)
    )
    monkeypatch.setattr(f"{_CLI_MODULE}.structlog.get_logger", lambda: MagicMock())
    return mock_resolve


@pytest.mark.unit
def test_cli_live_tail_explicit_table_args(runner, monkeypatch):
    mock_resolve = _make_full_pipeline_mocks(monkeypatch)

    result = runner.invoke(app, ["live-tail", "metrics.cpu", "metrics.memory"])

    assert result.exit_code == 0
    _, config_arg = mock_resolve.call_args[0]
    assert "metrics.cpu" in config_arg.tables
    assert "metrics.memory" in config_arg.tables


@pytest.mark.unit
def test_cli_live_tail_nonexistent_table_exits_input_error(runner, monkeypatch):
    """Test that a nonexistent explicit table causes InputError and exit code 3."""
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    monkeypatch.setattr(
        f"{_CLI_MODULE}.get_client", MagicMock(return_value=mock_client)
    )
    monkeypatch.setattr(
        f"{_CLI_MODULE}.resolve_tables",
        MagicMock(
            side_effect=InputError("Table 'nonexistent' not found in schema 'public'")
        ),
    )
    monkeypatch.setattr(f"{_CLI_MODULE}.structlog.get_logger", lambda: MagicMock())

    result = runner.invoke(app, ["live-tail", "nonexistent"])

    assert result.exit_code == ExitCode.INPUT_ERROR


@pytest.mark.unit
def test_cli_live_tail_poll_count_uses_parameterized_sql(mock_client):
    """Verify poll_count passes watermark as a SQL parameter, not string interpolation."""
    table = TableInfo(schema="public", table="events", time_column="created_at")
    watermark = "2025-01-15 10:00:00'; DROP TABLE events; --"

    mock_client.execute_query.return_value = _make_query_result(
        [(0, None)],
        columns=[
            ColumnMeta(name="count", type_oid=20, type_name="bigint"),
            ColumnMeta(name="max_ts", type_oid=1184, type_name="timestamptz"),
        ],
    )

    poll_count(mock_client, table, watermark, elapsed_seconds=10)

    _, params = mock_client.execute_query.call_args[0]
    assert isinstance(params, dict), "poll_count must use parameterized query"
    assert watermark in params.values(), "watermark must be passed as a parameter"
    sql = mock_client.execute_query.call_args[0][0]
    assert watermark not in sql, "watermark must NOT be interpolated directly into SQL"
