"""Tests for PostgreSQL service and introspection business logic (core.postgres)."""

from unittest.mock import MagicMock

import pytest

from tests.integration_config import (
    TEST_DATABASE,
    TEST_PLAIN_TABLE,
    TEST_PROFILE,
    TEST_SCHEMA,
    TEST_TABLE,
)
from tsdb_tool.core.client import PgClient
from tsdb_tool.core.config import load_config, resolve_config
from tsdb_tool.core.models import ColumnMeta, QueryResult
from tsdb_tool.core.postgres import (
    check_server,
    connections_summary,
    describe_table,
    get_time_column,
    get_timestamp_range,
    kill_backend,
    list_all_database_names,
    list_connections,
    list_databases,
    list_schemas,
    list_schemas_all_databases,
    list_tables,
    list_user_tables,
    preview_table,
    vacuum_tables,
)


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
# check_server
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_check_server_returns_all_properties(client):
    result = check_server(client)

    assert result.row_count == 4
    props = {row[0]: row[1] for row in result.rows}
    assert "version" in props
    assert "database" in props
    assert "user" in props
    assert "uptime" in props


@pytest.mark.integration
def test_check_server_result_columns(client):
    result = check_server(client)

    assert result.columns[0].name == "property"
    assert result.columns[1].name == "value"


# ---------------------------------------------------------------------------
# list_user_tables
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_user_tables_returns_table_names(client):
    tables = list_user_tables(client)

    assert isinstance(tables, list)
    assert len(tables) > 0
    for name in tables:
        assert "." in name


# ---------------------------------------------------------------------------
# list_databases
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_databases_returns_raw_result(client):
    result = list_databases(client)

    assert len(result.rows) > 0
    for row in result.rows:
        assert len(row) == 4
        assert isinstance(row[0], str)
        assert isinstance(row[3], int)


@pytest.mark.integration
def test_list_databases_sorted_by_size_desc(client):
    result = list_databases(client)

    sizes = [row[3] for row in result.rows]
    assert sizes == sorted(sizes, reverse=True)


# ---------------------------------------------------------------------------
# list_all_database_names
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_all_database_names_returns_names(client):
    names = list_all_database_names(client)

    assert isinstance(names, list)
    assert len(names) > 0
    assert "template0" not in names
    assert "template1" not in names


# ---------------------------------------------------------------------------
# list_schemas
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_schemas_basic_without_chunks(client):
    result, chunk_map = list_schemas(client)

    assert len(result.rows) > 0
    for row in result.rows:
        assert len(row) == 3
        assert isinstance(row[0], str)


@pytest.mark.integration
def test_list_schemas_with_chunk_info(client):
    result, chunk_map = list_schemas(client)

    assert len(result.rows) > 0
    if chunk_map:
        for _schema, (before_b, after_b, ht_total) in chunk_map.items():
            assert int(before_b) >= 0
            assert int(after_b) >= 0
            assert int(ht_total) >= 0


# ---------------------------------------------------------------------------
# list_schemas_all_databases
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_schemas_all_databases_single_database(client, resolved_config):
    def factory(db_name):
        cfg = resolve_config(
            load_config(),
            profile_name=TEST_PROFILE,
            database=db_name,
        )
        return PgClient(cfg)

    raw_data, _has_chunks = list_schemas_all_databases([TEST_DATABASE], factory)

    assert len(raw_data) >= 1
    assert raw_data[0][0] == TEST_DATABASE
    for row in raw_data:
        assert len(row) == 7


@pytest.mark.integration
def test_list_schemas_all_databases_with_chunks(client, resolved_config):
    def factory(db_name):
        cfg = resolve_config(
            load_config(),
            profile_name=TEST_PROFILE,
            database=db_name,
        )
        return PgClient(cfg)

    names = list_all_database_names(client)
    raw_data, _has_chunks = list_schemas_all_databases(names, factory)

    assert len(raw_data) >= 1
    for row in raw_data:
        assert len(row) == 7


def test_list_schemas_all_databases_connection_failure():
    def failing_factory(db_name):
        raise Exception("connection refused")

    raw_data, has_chunks = list_schemas_all_databases(
        ["baddb"],
        failing_factory,
    )

    assert raw_data == [("baddb", "(connection failed)", 0, 0, 0, 0, 0)]
    assert has_chunks is False


@pytest.mark.integration
def test_list_schemas_all_databases_multiple_databases(client, resolved_config):
    def factory(db_name):
        cfg = resolve_config(
            load_config(),
            profile_name=TEST_PROFILE,
            database=db_name,
        )
        return PgClient(cfg)

    names = list_all_database_names(client)
    if len(names) < 2:
        pytest.skip("Need at least 2 databases")

    raw_data, _ = list_schemas_all_databases(names[:2], factory)

    assert len(raw_data) >= 1
    db_names_in_data = {row[0] for row in raw_data}
    assert db_names_in_data.issubset(set(names[:2]))
    for row in raw_data:
        assert len(row) == 7


# ---------------------------------------------------------------------------
# list_tables
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_tables_with_schema_filter(client):
    result, _ht_map = list_tables(client, schema_filter="public")

    for row in result.rows:
        assert len(row) == 4


@pytest.mark.integration
def test_list_tables_without_schema_filter(client):
    result, _ht_map = list_tables(client)

    if result.rows:
        assert len(result.rows[0]) == 5


@pytest.mark.integration
def test_list_tables_with_hypertable_info(client):
    result, ht_map = list_tables(client, schema_filter=TEST_SCHEMA)

    assert len(ht_map) > 0


# ---------------------------------------------------------------------------
# describe_table
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_describe_table_returns_columns(client):
    result = describe_table(client, "pg_catalog", "pg_database")

    assert len(result.rows) > 0
    for row in result.rows:
        assert len(row) == 4
        assert isinstance(row[0], str)
        assert isinstance(row[1], str)


# ---------------------------------------------------------------------------
# get_time_column
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_get_time_column_returns_column_name(client):
    col = get_time_column(client, TEST_SCHEMA, TEST_TABLE)

    assert col is not None
    assert isinstance(col, str)


@pytest.mark.integration
def test_get_time_column_not_a_hypertable(client):
    col = get_time_column(client, "pg_catalog", "pg_database")

    assert col is None


# ---------------------------------------------------------------------------
# get_timestamp_range
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_get_timestamp_range_returns_range(client):
    time_col = get_time_column(client, TEST_SCHEMA, TEST_TABLE)
    if time_col is None:
        pytest.skip("No time column found")

    result = get_timestamp_range(client, TEST_SCHEMA, TEST_TABLE, time_col)

    assert len(result.rows) == 1
    assert result.rows[0][0] is not None
    assert result.rows[0][1] is not None


# ---------------------------------------------------------------------------
# preview_table
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_preview_table_head_with_time_column(client):
    time_col = get_time_column(client, TEST_SCHEMA, TEST_TABLE)

    result = preview_table(
        client,
        TEST_SCHEMA,
        TEST_TABLE,
        head=3,
        time_column=time_col,
    )

    assert result is not None
    assert len(result.rows) <= 3


@pytest.mark.integration
def test_preview_table_tail_with_time_column(client):
    time_col = get_time_column(client, TEST_SCHEMA, TEST_TABLE)

    result = preview_table(
        client,
        TEST_SCHEMA,
        TEST_TABLE,
        tail=3,
        time_column=time_col,
    )

    assert result is not None
    assert len(result.rows) <= 3


@pytest.mark.integration
def test_preview_table_sample_with_time_column(client):
    time_col = get_time_column(client, TEST_SCHEMA, TEST_TABLE)

    result = preview_table(
        client,
        TEST_SCHEMA,
        TEST_TABLE,
        sample=3,
        time_column=time_col,
    )

    assert result is not None
    assert len(result.rows) <= 3


@pytest.mark.integration
def test_preview_table_sample_without_time_column(client):
    result = preview_table(
        client,
        TEST_SCHEMA,
        TEST_PLAIN_TABLE,
        sample=3,
    )

    if result is not None:
        assert len(result.rows) <= 3


@pytest.mark.integration
def test_preview_table_head_without_time_column(client):
    result = preview_table(
        client,
        TEST_SCHEMA,
        TEST_PLAIN_TABLE,
        head=3,
    )

    if result is not None:
        assert len(result.rows) <= 3


# ---------------------------------------------------------------------------
# list_connections
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_connections_default_excludes_idle(client):
    result = list_connections(client)

    for row in result.rows:
        assert row[5] != "idle"


@pytest.mark.integration
def test_list_connections_include_all(client):
    result = list_connections(client, include_all=True)

    assert len(result.rows) >= 1
    assert len(result.rows[0]) == 12


@pytest.mark.integration
def test_list_connections_with_filters(client):
    result = list_connections(
        client,
        filter_db=TEST_DATABASE,
        include_all=True,
    )

    assert isinstance(result.rows, list)


# ---------------------------------------------------------------------------
# connections_summary
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_connections_summary_combined_output(client):
    result = connections_summary(client)

    assert result.columns[0].name == "property"
    assert result.columns[1].name == "value"
    props = [row[0] for row in result.rows]
    assert "---" in props
    assert "max_connections" in props


# ---------------------------------------------------------------------------
# vacuum_tables (unit — mutation operation)
# ---------------------------------------------------------------------------


def test_vacuum_tables_empty_list():
    mock_client = MagicMock()

    count = vacuum_tables(mock_client, [])

    assert count == 0
    mock_client.execute_query.assert_not_called()


def test_vacuum_tables_single_table():
    mock_client = MagicMock()
    mock_client.execute_query.return_value = _make_result([])

    count = vacuum_tables(mock_client, ["public.users"])

    assert count == 1
    mock_client.execute_query.assert_called_once_with("VACUUM ANALYZE public.users")


def test_vacuum_tables_full():
    mock_client = MagicMock()
    mock_client.execute_query.return_value = _make_result([])

    vacuum_tables(mock_client, ["public.users"], full=True)

    mock_client.execute_query.assert_called_once_with(
        "VACUUM FULL ANALYZE public.users"
    )


# ---------------------------------------------------------------------------
# kill_backend (unit — dangerous operation)
# ---------------------------------------------------------------------------


def test_kill_backend_terminate_success():
    mock_client = MagicMock()
    mock_client.execute_query.return_value = _make_result([(True,)])

    result = kill_backend(mock_client, 12345)

    assert result is True
    mock_client.execute_query.assert_called_once_with(
        "SELECT pg_terminate_backend(12345)"
    )


def test_kill_backend_cancel_mode():
    mock_client = MagicMock()
    mock_client.execute_query.return_value = _make_result([(True,)])

    result = kill_backend(mock_client, 12345, cancel=True)

    assert result is True
    mock_client.execute_query.assert_called_once_with("SELECT pg_cancel_backend(12345)")


def test_kill_backend_empty_result():
    mock_client = MagicMock()
    mock_client.execute_query.return_value = _make_result([])

    result = kill_backend(mock_client, 12345)

    assert result is False
