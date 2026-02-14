"""Tests for query result models (Phase 3, Step 3.1)."""

import pytest

from sql_tool.core.models import ColumnMeta, QueryResult


@pytest.mark.unit
def test_column_meta_basic():
    col = ColumnMeta(name="id", type_oid=23, type_name="int4")
    assert col.name == "id"
    assert col.type_oid == 23
    assert col.type_name == "int4"


@pytest.mark.unit
def test_column_meta_serializes_to_dict():
    col = ColumnMeta(name="name", type_oid=25, type_name="text")
    d = col.model_dump()
    assert d == {"name": "name", "type_oid": 25, "type_name": "text"}


@pytest.mark.unit
def test_query_result_with_rows():
    columns = [
        ColumnMeta(name="id", type_oid=23, type_name="int4"),
        ColumnMeta(name="name", type_oid=25, type_name="text"),
    ]
    rows = [(1, "alice"), (2, "bob")]
    result = QueryResult(
        columns=columns,
        rows=rows,
        row_count=2,
        status_message="SELECT 2",
    )
    assert result.row_count == 2
    assert len(result.rows) == 2
    assert result.rows[0] == (1, "alice")
    assert result.status_message == "SELECT 2"
    assert len(result.columns) == 2
    assert result.columns[0].name == "id"


@pytest.mark.unit
def test_query_result_empty():
    result = QueryResult(
        columns=[ColumnMeta(name="id", type_oid=23, type_name="int4")],
        rows=[],
        row_count=0,
        status_message="SELECT 0",
    )
    assert result.row_count == 0
    assert result.rows == []
    assert len(result.columns) == 1


@pytest.mark.unit
def test_query_result_no_columns():
    """DDL statements return no columns."""
    result = QueryResult(
        columns=[],
        rows=[],
        row_count=0,
        status_message="CREATE TABLE",
    )
    assert result.columns == []
    assert result.rows == []


@pytest.mark.unit
def test_query_result_with_none_values():
    columns = [ColumnMeta(name="val", type_oid=25, type_name="text")]
    result = QueryResult(
        columns=columns,
        rows=[(None,), ("hello",)],
        row_count=2,
        status_message="SELECT 2",
    )
    assert result.rows[0] == (None,)
    assert result.rows[1] == ("hello",)


@pytest.mark.unit
def test_query_result_serializes_to_dict():
    result = QueryResult(
        columns=[ColumnMeta(name="n", type_oid=23, type_name="int4")],
        rows=[(1,)],
        row_count=1,
        status_message="SELECT 1",
    )
    d = result.model_dump()
    assert d["row_count"] == 1
    assert d["status_message"] == "SELECT 1"
    assert len(d["columns"]) == 1
    assert d["columns"][0]["name"] == "n"
