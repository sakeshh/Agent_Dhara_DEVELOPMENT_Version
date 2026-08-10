from __future__ import annotations
import pytest

from agent.etl_pipeline.sql_codegen import generate_sql_etl


def test_generated_sql_has_no_drop_table():
    """
    Contract: The generated SQL must NEVER contain DROP TABLE statements for the source/raw dataset.
    """
    plan = {
        "plan_id": "test_plan_safety",
        "datasets": {
            "dbo.customers": {
                "steps": [
                    {"action": "trim", "column": "name"}
                ]
            }
        }
    }
    
    assessment = {
        "datasets": {
            "dbo.customers": {
                "columns": {
                    "customer_id": {"dtype": "int", "candidate_primary_key": True},
                    "name": {"dtype": "varchar(100)"}
                }
            }
        }
    }
    
    sql = generate_sql_etl(plan, assessment, dialect="tsql")
    
    # Assert DROP is not applied to dbo.customers
    # We may drop stored procedures, but never the raw table
    drop_patterns = ["DROP TABLE [dbo].[customers]", "DROP TABLE dbo.customers", "DROP TABLE customers"]
    for pattern in drop_patterns:
        assert pattern not in sql


def test_generated_sql_has_no_truncate_source():
    """
    Contract: The generated SQL must NEVER truncate the source/raw tables.
    """
    plan = {
        "plan_id": "test_plan_safety",
        "datasets": {
            "dbo.customers": {
                "steps": [
                    {"action": "trim", "column": "name"}
                ],
                "scd_type": "truncate"  # truncate applies to target clean table, not source
            }
        }
    }
    
    assessment = {
        "datasets": {
            "dbo.customers": {
                "columns": {
                    "customer_id": {"dtype": "int", "candidate_primary_key": True},
                    "name": {"dtype": "varchar(100)"}
                }
            }
        }
    }
    
    sql = generate_sql_etl(plan, assessment, dialect="tsql")
    
    # Verify truncate does not target source table
    truncate_source_patterns = ["TRUNCATE TABLE [dbo].[customers]", "TRUNCATE TABLE dbo.customers", "TRUNCATE TABLE customers"]
    for pattern in truncate_source_patterns:
        assert pattern not in sql


def test_quarantine_table_always_created_before_use():
    """
    Contract: If reject or quarantine operations are used, the etl_rejects table
    must be defined before any INSERT/SELECT statements run.
    """
    plan = {
        "plan_id": "test_plan_safety",
        "business_rules": {
            "never_drop_rows": False  # allow reject/quarantine
        },
        "datasets": {
            "dbo.customers": {
                "steps": [
                    {"action": "fill_or_drop", "column": "name"}
                ]
            }
        }
    }
    
    assessment = {
        "datasets": {
            "dbo.customers": {
                "columns": {
                    "customer_id": {"dtype": "int", "candidate_primary_key": True},
                    "name": {"dtype": "varchar(100)"}
                }
            }
        }
    }
    
    sql = generate_sql_etl(plan, assessment, dialect="tsql")
    
    # Check if dbo.etl_rejects DDL is before any INSERT INTO dbo.etl_rejects
    ddl_idx = sql.find("CREATE TABLE dbo.etl_rejects")
    insert_idx = sql.find("INSERT INTO dbo.etl_rejects")
    
    assert ddl_idx != -1
    # If there is an insert, DDL must come first
    if insert_idx != -1:
        assert ddl_idx < insert_idx
