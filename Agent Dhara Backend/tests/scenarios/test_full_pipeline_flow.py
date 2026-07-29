from __future__ import annotations
import pytest
import pandas as pd

from tests.fixtures.dirty_customer_data import dirty_customer_df
from agent.intelligent_data_assessment import profile_dataframe, analyze_dataset_quality, load_dq_thresholds
from agent.etl_pipeline.planner import build_etl_plan
from agent.etl_pipeline.sql_codegen import generate_sql_etl


def test_full_pipeline_flow_scenario(dirty_customer_df):
    """
    Scenario Test: Load dirty data -> profile -> analyze quality -> build plan -> generate code.
    Ensures integration across assessment, planning, and SQL codegen works cleanly.
    """
    # 1. Profile dataframe
    profile = profile_dataframe(dirty_customer_df)
    
    # 2. Analyze dataset quality
    thresholds = load_dq_thresholds()
    dq = analyze_dataset_quality("dbo.customers", dirty_customer_df, profile, thresholds)
    profile["quality"] = dq
    
    assessment = {
        "datasets": {
            "dbo.customers": profile
        }
    }
    
    # 3. Verify that DQ issues are populated (e.g., duplicate keys, nulls)
    issues = dq.get("issues") or []
    assert len(issues) > 0
    
    # 4. Generate plan
    plan = build_etl_plan(assessment, {}, engine="sql")
    assert "datasets" in plan
    assert "dbo.customers" in plan["datasets"]
    
    # 5. Generate code
    sql = generate_sql_etl(plan, assessment, dialect="tsql")
    assert len(sql) > 0
    assert "CREATE PROCEDURE dbo.etl_clean_customers" in sql
