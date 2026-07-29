from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
import pyodbc

import agent.azure_sql_executor as ase
from agent.etl_pipeline.execution_orchestrator import (
    orchestrate_sql_execution,
    build_pre_execution_counts,
)

TEST_CONN_STR = "Driver={ODBC Driver 17};Server=test;uid=user;pwd=pass;"


def test_split_sql_batches():
    # GO separators splits correctly and empty batches are excluded
    sql = "SELECT 1;\nGO\n\nSELECT 2;\n  GO  \n-- comment\nSELECT 3;"
    batches = ase._split_sql_batches(sql)
    assert batches == ["SELECT 1;", "SELECT 2;", "SELECT 3;"]

    assert ase._split_sql_batches("") == []
    assert ase._split_sql_batches("   \n   ") == []


def test_requires_approval_blocks_destructive():
    # SQL contains DELETE FROM orders
    sql = "DELETE FROM orders;"
    res = ase.check_requires_approval(sql)
    assert res["requires_approval"] is True
    assert "DELETE" in res["ops_found"]

    # Test comments are ignored
    commented_sql = "-- DELETE FROM orders;\nSELECT * FROM orders;"
    res_comment = ase.check_requires_approval(commented_sql)
    assert res_comment["requires_approval"] is False

    multi_commented_sql = "/*\nDELETE FROM orders;\n*/\nSELECT * FROM orders;"
    res_multi = ase.check_requires_approval(multi_commented_sql)
    assert res_multi["requires_approval"] is False


@patch("pyodbc.connect")
def test_get_connection_config(mock_connect):
    os.environ["DHARA_AZURE_SQL_CONN_STR"] = TEST_CONN_STR
    conn = ase.get_connection()
    assert conn is not None
    mock_connect.assert_called_once()
    
    # Check Connection Timeout default is appended
    args, kwargs = mock_connect.call_args
    assert "Connection Timeout=" in args[0]
    assert kwargs.get("autocommit") is False


@patch("pyodbc.connect")
def test_test_connection_success(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("Azure SQL Server 2022",)
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    res = ase.test_connection(TEST_CONN_STR)
    assert res["ok"] is True
    assert "Azure SQL" in res["server"]
    assert res["latency_ms"] >= 0.0
    assert res["error"] is None


@patch("pyodbc.connect")
def test_test_connection_failure(mock_connect):
    # pyodbc.connect raises OperationalError
    mock_connect.side_effect = pyodbc.OperationalError("Login failed")
    
    secret_str = "Driver={ODBC};Server=test_secret_credentials_pwd_123;uid=user;pwd=pass;"
    res = ase.test_connection(secret_str)
    assert res["ok"] is False
    assert res["server"] == ""
    assert res["error"] is not None
    assert "test_secret_credentials_pwd_123" not in res["error"]


@patch("pyodbc.connect")
def test_run_transactional_sql_success(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 5
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    sql = "SELECT 1;\nGO\nSELECT 2;"
    res = ase.run_transactional_sql(sql, connection_string=TEST_CONN_STR)

    assert res["ok"] is True
    assert res["dry_run"] is False
    assert res["transaction_committed"] is True
    assert res["total_rows_affected"] == 10
    assert len(res["batch_results"]) == 2
    assert res["rollback_reason"] is None
    assert res["error"] is None
    mock_conn.commit.assert_called_once()
    mock_conn.rollback.assert_not_called()


@patch("pyodbc.connect")
def test_run_transactional_sql_rollback_on_error(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    # Let execution fail
    mock_cursor.execute.side_effect = pyodbc.Error("Table not found")
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    sql = "SELECT 1;\nGO\nSELECT 2;"
    res = ase.run_transactional_sql(sql, connection_string=TEST_CONN_STR)

    assert res["ok"] is False
    assert res["dry_run"] is False
    assert res["transaction_committed"] is False
    assert "Table not found" in res["rollback_reason"]
    assert res["error"] is not None
    mock_conn.commit.assert_not_called()
    mock_conn.rollback.assert_called_once()


def test_dry_run_no_execution():
    with patch("pyodbc.connect") as mock_connect:
        sql = "DELETE FROM orders;"
        res = ase.run_transactional_sql(
            sql,
            connection_string=TEST_CONN_STR,
            dry_run=True,
        )
        assert res["ok"] is True
        assert res["dry_run"] is True
        assert res["requires_approval"] is True
        assert res["ops_found"] == ["DELETE"]
        assert len(res["batches"]) == 1
        mock_connect.assert_not_called()


def test_execution_disabled_env():
    os.environ["DHARA_SQL_EXECUTION_DISABLED"] = "1"
    try:
        res = ase.run_transactional_sql("SELECT 1;", connection_string=TEST_CONN_STR)
        assert res["ok"] is False
        assert res["error"] == "execution_disabled"
    finally:
        os.environ.pop("DHARA_SQL_EXECUTION_DISABLED", None)


def test_orchestrate_validation_failure():
    # Empty SQL fails basic validation
    res = orchestrate_sql_execution("", session_id="test_sess")
    assert res["ok"] is False
    assert res["stage"] == "validation"
    assert len(res["validation_errors"]) > 0


def test_orchestrate_approval_required():
    # SQL has DELETE on staging (allowed by basic validation, but needs approval)
    sql = "DELETE FROM customer_stg;"
    res = orchestrate_sql_execution(sql, session_id="test_sess", approved=False)
    assert res["ok"] is False
    assert res["stage"] == "approval_required"
    assert res["requires_approval"] is True
    assert "DELETE" in res["ops_found"]


@patch("pyodbc.connect")
def test_build_pre_execution_counts(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("dbo.customers", 100), ("orders", 100)]
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    counts = build_pre_execution_counts(["dbo.customers", "orders"], connection_string=TEST_CONN_STR)
    assert counts == {"dbo.customers": 100, "orders": 100}
    
    # Check that brackets were added
    mock_cursor.execute.assert_called_once()
    query = mock_cursor.execute.call_args[0][0]
    assert "[dbo].[customers]" in query
    assert "[orders]" in query


@patch("pyodbc.connect")
@patch("agent.azure_sql_executor.DefaultAzureCredential")
def test_get_connection_token_auth(mock_cred_cls, mock_connect):
    mock_cred = MagicMock()
    mock_token = MagicMock()
    mock_token.token = "mock_access_token_123"
    mock_cred.get_token.return_value = mock_token
    mock_cred_cls.return_value = mock_cred
    
    # Connection string without credentials to trigger token auth
    conn = ase.get_connection("Driver={ODBC Driver 17};Server=tcp:myserver.database.windows.net;")
    assert conn is not None
    mock_cred.get_token.assert_called_once_with("https://database.windows.net/.default")
    mock_connect.assert_called_once()
