"""Tests for PgClient (Phase 3, Step 3.2).

Integration tests against real PostgreSQL using test_db profile.
"""

import pytest

from sql_tool.core.client import PgClient
from sql_tool.core.config import load_config, resolve_config
from sql_tool.core.exceptions import NetworkError, SqlToolError, TimeoutError
from sql_tool.core.models import ColumnMeta, QueryResult
from tests.integration_config import TEST_PROFILE


@pytest.fixture
def resolved_config():
    config = load_config()
    return resolve_config(config, profile_name=TEST_PROFILE)


@pytest.fixture
def client(resolved_config):
    with PgClient(resolved_config) as c:
        yield c


# -- Connection --


@pytest.mark.integration
def test_client_connects(client):
    result = client.execute_query("SELECT 1 AS n")
    assert isinstance(result, QueryResult)


@pytest.mark.integration
def test_client_reuses_connection(client):
    client.execute_query("SELECT 1")
    first_conn = client._connection
    client.execute_query("SELECT 2")
    assert client._connection is first_conn


# -- Query results --


@pytest.mark.integration
def test_execute_returns_query_result(client):
    result = client.execute_query("SELECT 1 AS num")
    assert result.row_count == 1
    assert result.rows == [(1,)]
    assert result.status_message == "SELECT 1"


@pytest.mark.integration
def test_execute_multiple_rows(client):
    result = client.execute_query("SELECT v FROM generate_series(1, 3) AS v")
    assert result.row_count == 3
    assert result.rows == [(1,), (2,), (3,)]


@pytest.mark.integration
def test_execute_multiple_columns(client):
    result = client.execute_query("SELECT 1 AS a, 'hello' AS b, true AS c")
    assert result.row_count == 1
    assert len(result.columns) == 3
    assert result.columns[0].name == "a"
    assert result.columns[1].name == "b"
    assert result.columns[2].name == "c"


@pytest.mark.integration
def test_execute_no_result_rows(client):
    result = client.execute_query("CREATE TEMP TABLE _test_no_rows (id int)")
    assert result.row_count == 0
    assert result.rows == []
    assert result.columns == []


# -- Column metadata --


@pytest.mark.integration
def test_column_meta_int(client):
    result = client.execute_query("SELECT 1 AS num")
    col = result.columns[0]
    assert isinstance(col, ColumnMeta)
    assert col.name == "num"
    assert col.type_name == "int4"


@pytest.mark.integration
def test_column_meta_text(client):
    result = client.execute_query("SELECT 'hello'::text AS greeting")
    assert result.columns[0].type_name == "text"


@pytest.mark.integration
def test_column_meta_bool(client):
    result = client.execute_query("SELECT true AS flag")
    assert result.columns[0].type_name == "bool"


# -- Error handling --


@pytest.mark.integration
def test_syntax_error_raises_sql_tool_error(client):
    with pytest.raises(SqlToolError, match="SQL error"):
        client.execute_query("SELECTT 1")


@pytest.mark.integration
def test_timeout_raises_timeout_error(resolved_config):
    short_timeout = resolved_config.model_copy(update={"default_timeout": 0.1})
    with (
        PgClient(short_timeout) as c,
        pytest.raises(TimeoutError, match="Query timed out"),
    ):
        c.execute_query("SELECT pg_sleep(5)")


@pytest.mark.integration
def test_connection_failure_raises_network_error():
    from sql_tool.core.config import ResolvedConfig

    bad_config = ResolvedConfig(host="192.0.2.1", port=9999, connect_timeout=1)
    client = PgClient(bad_config)
    with pytest.raises(NetworkError, match="Connection failed"):
        client.execute_query("SELECT 1")


# -- Lifecycle --


@pytest.mark.integration
def test_close_disconnects(client):
    client.execute_query("SELECT 1")
    assert client._connection is not None
    client.close()
    assert client._connection is None


@pytest.mark.integration
def test_close_when_not_connected(resolved_config):
    client = PgClient(resolved_config)
    client.close()  # should not raise


@pytest.mark.integration
def test_context_manager_closes(resolved_config):
    with PgClient(resolved_config) as c:
        c.execute_query("SELECT 1")
        conn = c._connection
    assert conn.closed


# -- Statement timeout --


@pytest.mark.integration
def test_sets_statement_timeout(client):
    result = client.execute_query("SHOW statement_timeout")
    timeout_val = result.rows[0][0]
    assert timeout_val == "30s"
