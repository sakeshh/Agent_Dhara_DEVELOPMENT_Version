from __future__ import annotations
import pytest

from agent.etl_pipeline.sql_codegen import generate_sql_etl


def test_sql_codegen_snapshot_tsql():
    """
    Lock-in test for T-SQL dialect code generation.
    Checks that core T-SQL templates (procedures, logs, and staging setup) are generated.
    """
    plan = {
        "plan_id": "snapshot_test_123",
        "datasets": {
            "dbo.sales": {
                "steps": [
                    {"action": "trim", "column": "item_name"},
                    {"action": "coerce_numeric", "column": "amount"}
                ]
            }
        }
    }
    
    assessment = {
        "datasets": {
            "dbo.sales": {
                "columns": {
                    "sale_id": {"dtype": "int", "candidate_primary_key": True},
                    "item_name": {"dtype": "varchar(100)"},
                    "amount": {"dtype": "decimal(10,2)"}
                }
            }
        }
    }
    
    sql = generate_sql_etl(plan, assessment, dialect="tsql")
    
    # Assert critical parts of generated T-SQL are present
    assert "CREATE PROCEDURE dbo.etl_clean_sales" in sql
    assert "CREATE TABLE dbo.etl_log" in sql
    assert "CREATE TABLE dbo.etl_invalid_values" in sql
    assert "BEGIN TRY" in sql
    assert "COMMIT;" in sql
    assert "END TRY" in sql
    assert "BEGIN CATCH" in sql
    assert "ROLLBACK;" in sql
