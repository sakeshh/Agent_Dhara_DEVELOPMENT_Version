from __future__ import annotations
import pytest
from unittest.mock import MagicMock, patch

from agent.post_etl_validator import run_post_etl_validation
from agent.etl_pipeline.execution_orchestrator import orchestrate_sql_execution


def test_run_post_etl_validation_no_tables():
    res = run_post_etl_validation([], "dummy_conn_string")
    assert res["ok"] is True
    assert res["deltas"] == {"improvements": [], "regressions": []}


@patch("agent.post_etl_validator.get_connection")
def test_run_post_etl_validation_metrics(mock_get_connection):
    # Set up mock DB connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_connection.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    # 1. Mock columns schema query return: (COLUMN_NAME, IS_NULLABLE)
    # We will simulate order_date and total_amount columns
    mock_cursor.fetchall.side_effect = [
        [("order_date", "YES"), ("total_amount", "YES")]  # for tables[0] schema query
    ]
    
    # 2. Mock counts queries:
    # First: SELECT COUNT(*) FROM table WHERE order_date IS NULL -> returns 2 nulls (was 5 nulls in pre-assessment -> improvement!)
    # Second: SELECT COUNT(*) FROM table WHERE total_amount IS NULL -> returns 10 nulls (was 0 nulls in pre-assessment -> regression!)
    mock_cursor.fetchone.side_effect = [
        (2,),  # order_date null count
        (10,), # total_amount null count
    ]
    
    # Mock pre_assessment
    pre_assessment = {
        "datasets": {
            "sales": {
                "row_count": 100,
                "columns": {
                    "order_date": {
                        "null_percentage": 0.05  # 5 nulls
                    },
                    "total_amount": {
                        "null_percentage": 0.0   # 0 nulls
                    }
                }
            }
        }
    }
    
    res = run_post_etl_validation(
        target_tables=["sales"],
        connection_string="dummy_conn_string",
        pre_assessment=pre_assessment
    )
    
    assert res["ok"] is False  # Regressions exist
    deltas = res["deltas"]
    
    # Check improvements
    assert len(deltas["improvements"]) == 1
    imp = deltas["improvements"][0]
    assert imp["column"] == "order_date"
    assert imp["before"] == 5
    assert imp["after"] == 2
    
    # Check regressions
    assert len(deltas["regressions"]) == 1
    reg = deltas["regressions"][0]
    assert reg["column"] == "total_amount"
    assert reg["before"] == 0
    assert reg["after"] == 10


@patch("agent.etl_pipeline.execution_orchestrator.execute_plan")
@patch("agent.etl_pipeline.execution_orchestrator.validate_sql_basic")
@patch("agent.etl_pipeline.execution_orchestrator.check_requires_approval")
@patch("agent.etl_pipeline.execution_orchestrator.get_connection")
@patch("agent.post_etl_validator.get_connection")
def test_orchestrate_sql_execution_post_validation(
    mock_post_get_connection,
    mock_orchestrator_get_connection,
    mock_requires_approval,
    mock_validate_sql,
    mock_execute_plan
):
    # Mock validation and approval gates
    mock_validate_sql.return_value = (True, [], [])
    mock_requires_approval.return_value = {"requires_approval": False, "ops_found": []}
    
    # Mock execution result
    mock_result = MagicMock()
    mock_result.ok = True
    mock_result.run_id = "test_run"
    mock_result.rows_affected = 5
    mock_result.duration_ms = 120.0
    mock_result.committed = True
    mock_result.error = None
    mock_result.batch_results = []
    mock_result.artifacts = {}
    mock_execute_plan.return_value = mock_result
    
    # Mock DB connection for row counts and post-validation
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_post_get_connection.return_value = mock_conn
    mock_orchestrator_get_connection.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    # Pre-execution counts and post-execution counts queries
    mock_cursor.fetchall.side_effect = [
        [("sales", 105)],  # post-execution count union query (after = 105)
        [("id", "NO")],   # columns query in run_post_etl_validation
    ]
    mock_cursor.fetchone.return_value = (0,)  # SELECT COUNT(*) for id null count query
    
    res = orchestrate_sql_execution(
        sql="UPDATE sales SET amount = 10",
        session_id="test_session",
        connection_string="dummy_conn_string",
        pre_execution_counts={"sales": 100},
        assessment={
            "datasets": {
                "sales": {
                    "row_count": 100,
                    "columns": {
                        "id": {"null_percentage": 0.0}
                    }
                }
            }
        }
    )
    
    assert res["ok"] is True
    summary = res["post_execution_summary"]
    assert "post_etl_validation" in summary
    assert summary["post_etl_validation"]["ok"] is True
    assert summary["row_deltas"]["sales"]["delta"] == 5
