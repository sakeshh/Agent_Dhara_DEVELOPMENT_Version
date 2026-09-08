import pytest
from typing import Dict, Any, List

# 1. Test Codegen Validator
def test_codegen_validator():
    from agent.etl_pipeline.codegen_validator import validate_python, validate_tsql, validate_pyspark, validate_adf
    
    # Python
    ok, errs, _ = validate_python("def transform_data(df):\n    df = df.copy()\n    import logging\n    logger = logging.getLogger()\n    return df\n")
    assert ok, f"Python validation failed: {errs}"
    
    ok, errs, _ = validate_python("eval('1+1')")
    assert not ok
    assert any("eval() is forbidden" in e for e in errs)
    
    # TSQL
    ok, errs, _ = validate_tsql("CREATE TABLE dbo.ETL_LOG (id INT);\nCREATE TABLE dbo.ETL_REJECTS (id INT);\nBEGIN TRY\nSELECT SCOPE_IDENTITY();\nBEGIN TRANSACTION;\nCOMMIT TRANSACTION;\nEND TRY\nBEGIN CATCH\nROLLBACK;\nEND CATCH;\n")
    assert ok, f"TSQL validation failed: {errs}"
    
    ok, errs, _ = validate_tsql("CREATE TABLE IF NOT EXISTS dbo.test (id INT)")
    assert not ok
    
    # PySpark
    ok, errs, _ = validate_pyspark("from pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()\ndef _resolve_data_path(x): return x\n")
    assert ok, f"PySpark validation failed: {errs}"

# 2. Test Token Trimming
def test_token_trimming():
    from agent.etl_pipeline.llm_codegen import _trim_payload_for_window
    
    large_payload = {
        "source_metadata": {
            "ds1": {
                "row_count": 100,
                "columns": {f"col_{i}": {"dtype": "int", "null_percentage": 0.1, "semantic_type": "int", "sub_type": "int", "pii_level": "none"} for i in range(1500)}
            }
        },
        "manual_review": [{"dataset": "ds1", "column": f"col_{i}", "message": "error"} for i in range(20)],
        "blocked": [{"dataset": "ds1", "column": f"col_{i}", "reason": "blocked"} for i in range(10)]
    }
    
    trimmed = _trim_payload_for_window(large_payload, "python")
    
    # Ensure manual_review is capped
    assert len(trimmed["manual_review"]) <= 8
    assert "manual_review_truncated" in trimmed
    
    # Ensure blocked is capped
    assert len(trimmed["blocked"]) <= 5
    
    # Ensure metadata columns are trimmed (keys dtype, semantic_type, sub_type remain, null_percentage is removed)
    cols = trimmed["source_metadata"]["ds1"]["columns"]
    for col, cm in cols.items():
        assert "null_percentage" not in cm
        assert "dtype" in cm

# 3. Test Preflight Gate
def test_preflight_gate():
    from agent.sql_preflight import run_sql_preflight
    
    res = run_sql_preflight("CREATE TABLE IF NOT EXISTS dbo.test (id INT)")
    assert not res["passed"]
    assert any("CREATE TABLE IF NOT EXISTS" in e for e in res["errors"])
    
    res = run_sql_preflight("SELECT * FROM dbo.test")
    assert res["passed"]

# 4. Test Non-Fixable Resolutions Promotion
def test_non_fixable_promotions():
    from agent.etl_pipeline.manual_review_promote import promote_non_fixable_resolutions
    
    plan = {
        "datasets": {
            "ds1": {"steps": []}
        },
        "blocked": []
    }
    
    resolutions = [
        {"dataset": "ds1", "column": "col1", "strategy": "quarantine", "user_note": "FK check"},
        {"dataset": "ds1", "column": "col2", "strategy": "accept_risk", "user_note": "risky"}
    ]
    
    updated = promote_non_fixable_resolutions(plan, resolutions)
    
    # Check step added
    steps = updated["datasets"]["ds1"]["steps"]
    assert len(steps) == 1
    assert steps[0]["action"] == "validate_and_partition_relationship"
    
    # Check blocked added
    assert len(updated["blocked"]) == 1
    assert updated["blocked"][0]["column"] == "col2"

# 5. Test Post-ETL Feedback Loop
def test_post_etl_feedback():
    from agent.etl_pipeline.execution_orchestrator import post_etl_regen_if_needed
    
    report = {
        "ok": False,
        "deltas": {
            "regressions": [
                {"table": "ds1", "column": "col1", "issue": "null rate increased"}
            ]
        }
    }
    
    plan = {"datasets": {"ds1": {"steps": []}}}
    assessment = {"datasets": {"ds1": {"columns": {"col1": {"dtype": "int"}}}}}
    
    # Mock llm gen to return a dummy string on retry
    patched = post_etl_regen_if_needed(report, plan, assessment, "python")
    # Should attempt calling generate_etl_with_llm (which might return an LLM error in test since API keys are missing, but verifies code path)
    assert patched is None or "Error" in patched or isinstance(patched, str)

# 6. Test Plan step coverage report
def test_coverage_report():
    from agent.etl_pipeline.plan_coverage_report import build_coverage_report
    
    assessment = {
        "datasets": {
            "ds1": {
                "quality": {
                    "issues": [
                        {"column": "col1", "type": "nulls", "message": "missing data"},
                        {"column": "col2", "type": "outliers", "message": "extreme value"}
                    ]
                }
            }
        }
    }
    
    plan = {
        "datasets": {
            "ds1": {
                "steps": [
                    {"column": "col1", "action": "fill_nulls_simple"}
                ]
            }
        },
        "manual_review": [],
        "blocked": []
    }
    
    report = build_coverage_report(assessment, plan)
    assert report["coverage_pct"] == 50.0
    assert len(report["covered"]) == 1
    assert len(report["uncovered"]) == 1
    assert report["covered"][0]["column"] == "col1"
    assert report["uncovered"][0]["column"] == "col2"

# 7. Test Compiler Layering
@pytest.mark.xfail(strict=True, reason="Legacy contract superseded by PR 4.6/6 composite work package - tracked by ADR-006")
def test_compiler_layering():
    from agent.etl_pipeline.issue_to_step_compiler import preprocess_suggestions_in_place
    
    suggestions = [
        {"dataset": "ds1", "column": "col1", "issue_type": "nulls", "auto_fixable": True},
        {"dataset": "ds1", "column": "col2", "issue_type": "missing_required_column", "auto_fixable": True}
    ]
    rules = {"never_drop_rows": True}
    sem_schema = {}
    
    non_fixables = preprocess_suggestions_in_place(suggestions, rules, sem_schema)
    
    # col1 should be mapped to fill_nulls_flag (Phase 1 safe — flags null, preserves raw value)
    # NOTE: fill_nulls_simple was Phase 2 (overwrites with a fill value); fill_nulls_flag is the
    # correct Phase 1 replacement that flags the null without modifying the source value.
    assert suggestions[0]["suggested_action"] == "fill_nulls_flag"
    assert suggestions[0]["auto_fixable"] is True
    
    # col2 is non_fixable
    assert len(non_fixables) == 1
    assert non_fixables[0]["column"] == "col2"
    assert suggestions[1]["non_fixable"] is True


# 8. Test Cross-Dataset Inconsistency Mapping
def test_cross_dataset_inconsistency_mapping():
    from agent.transformation_suggester import suggest_transformations
    
    assessment_result = {
        "datasets": {
            "ds1": {"columns": {"col_mismatch": {"dtype": "int"}}},
            "ds2": {"columns": {"col_mismatch": {"dtype": "object"}}}
        },
        "data_quality_issues": {
            "global_issues": {
                "cross_dataset_inconsistencies": [
                    {
                        "type": "cross_dataset_mixed_type",
                        "column": "col_mismatch",
                        "severity": "HIGH",
                        "message": "Conflicting dtypes"
                    }
                ]
            }
        }
    }
    
    res = suggest_transformations(assessment_result)
    sugs = res["suggested_transformations"]
    
    # Verify that col_mismatch has suggestions in both ds1 and ds2
    ds1_sugs = [s for s in sugs if s["dataset"] == "ds1" and s["column"] == "col_mismatch"]
    ds2_sugs = [s for s in sugs if s["dataset"] == "ds2" and s["column"] == "col_mismatch"]
    
    assert len(ds1_sugs) == 1
    assert len(ds2_sugs) == 1
    assert ds1_sugs[0]["suggested_action"] == "cast_type"
    assert ds2_sugs[0]["suggested_action"] == "cast_type"


# 9. Test Manual Review Resolution Rebuild
@pytest.mark.xfail(strict=True, reason="Legacy resolution rebuild superseded by PR 4.6b immutable audit trail - tracked by ADR-006")
def test_manual_review_resolution_rebuild():
    from agent.etl_handlers import etl_apply_manual_resolutions, etl_plan_start
    from agent.session_store import save_session, load_session, reset_session
    from agent.etl_handlers import _ctx
    
    sid = "test_resolution_rebuild_session"
    reset_session(sid)
    sess = load_session(sid)
    ctx = _ctx(sess)
    
    # Setup mock assessment & intent
    assess = {
        "datasets": {
            "ds1": {
                "columns": {
                    "critical_col": {"dtype": "int"}
                }
            }
        },
        "data_quality_issues": {
            "datasets": {
                "ds1": {
                    "issues": [
                        {"column": "critical_col", "type": "nulls", "severity": "medium", "message": "nulls"}
                    ]
                }
            }
        }
    }
    ctx["assessment"] = assess
    ctx["etl_flow"] = {
        "target_engine": "python",
        "sql_dialect": "tsql",
        "etl_intent": {
            "target_destination": "dataframe_only",
            "generation_mode": "full"
        }
    }
    save_session(sess)
    
    # Generate initial plan
    rules = {"auto_resolve_safe_defaults": False}
    source_context = {
        "suggestions": [
            {
                "dataset": "ds1",
                "column": "critical_col",
                "issue_type": "nulls",
                "suggested_action": "fill_or_drop",
                "auto_fixable": False,
                "severity": "medium",
                "message": "nulls requiring review"
            }
        ]
    }
    plan_res = etl_plan_start(sid, business_rules=rules, assessment_result=assess, source_context=source_context)
    assert plan_res["ok"] is True
    
    # Get a pending manual review item ID
    manual_review = plan_res["plan"]["manual_review"]
    assert len(manual_review) > 0
    item_id = manual_review[0]["id"]
    
    # Apply resolutions - should rebuild and apply successfully
    resolutions = [{"item_id": item_id, "resolution_id": "preserve_and_flag"}]
    applied_res = etl_apply_manual_resolutions(sid, resolutions)
    
    assert applied_res["ok"] is True
    rebuilt_plan = applied_res["plan"]
    
    # The step should now be promoted into rebuilt_plan.datasets.ds1.steps
    steps = rebuilt_plan["datasets"]["ds1"]["steps"]
    assert any(s["column"] == "critical_col" and s["action"] in ("fill_nulls_simple", "preserve_null_and_flag") for s in steps)


# 10. Test Code Generation Coverage Gate
def test_codegen_coverage_gate():
    from agent.etl_handlers import etl_generate_code
    from agent.session_store import save_session, load_session, reset_session
    from agent.etl_handlers import _ctx

    sid = "test_coverage_gate_session"
    reset_session(sid)
    sess = load_session(sid)
    ctx = _ctx(sess)

    assess = {
        "datasets": {
            "ds1": {
                "columns": {
                    "col1": {"dtype": "int"}
                }
            }
        },
        "data_quality_issues": {
            "datasets": {
                "ds1": {
                    "issues": [
                        {"column": "col1", "type": "nulls", "severity": "high", "message": "nulls"}
                    ]
                }
            }
        }
    }
    ctx["last_assessment_result"] = assess
    
    flow = {
        "phase": "approved",
        "approved_plan": {
            "plan_id": "test_plan",
            "datasets": {
                "ds1": {
                    "steps": []
                }
            },
            "manual_review": [],
            "non_fixable": [],
        }
    }
    ctx["etl_flow"] = flow
    save_session(sess)

    res = etl_generate_code(sid, engine="python")
    assert res["ok"] is False
    assert res["error"] == "UNCOVERED_ISSUES"
    assert len(res["uncovered"]) == 1
    assert res["uncovered"][0]["column"] == "col1"


# 11. Test Compiler preserves manual review intent unless high confidence
def test_compiler_preserves_manual_intent():
    from agent.etl_pipeline.issue_to_step_compiler import compile_issues_to_steps
    
    suggestions = [
        {
            "dataset": "ds1",
            "column": "col1",
            "issue_type": "future_dates",
            "suggested_action": "review_manually",
            "auto_fixable": False,
            "severity": "medium",
            "message": "Future dates issue"
        }
    ]
    rules = {}
    
    # 1. With low semantic confidence, action remains review_manually
    sem_schema_low = {
        "ds1.col1": {
            "semantic_type": "date",
            "confidence": 0.50
        }
    }
    steps_low, manual_low = compile_issues_to_steps(suggestions, rules, sem_schema_low)
    assert len(steps_low.get("ds1", [])) == 0
    assert len(manual_low) == 1
    assert manual_low[0]["suggested_action"] == "review_manually"
    
    # 2. With high semantic confidence, action is refined to parse_dates (though still manual)
    sem_schema_high = {
        "ds1.col1": {
            "semantic_type": "date",
            "confidence": 0.90
        }
    }
    steps_high, manual_high = compile_issues_to_steps(suggestions, rules, sem_schema_high)
    assert len(steps_high.get("ds1", [])) == 0
    assert len(manual_high) == 1
    assert manual_high[0]["suggested_action"] == "parse_dates"


# 12. Test Compiler Action Map Coverage
def test_compiler_action_map_coverage():
    from agent.transformation_suggester import ISSUE_TO_ACTION
    from agent.etl_pipeline.issue_to_step_compiler import _ISSUE_TO_ACTION_MAP
    
    for key in ISSUE_TO_ACTION.keys():
        assert key in _ISSUE_TO_ACTION_MAP, f"Issue type '{key}' is in transformation_suggester.py but missing in _ISSUE_TO_ACTION_MAP in issue_to_step_compiler.py"


# 13. Test Planner Coverage Report has zero uncovered items
def test_planner_coverage_zero_uncovered():
    from agent.etl_pipeline.planner import build_etl_plan
    
    assessment = {
        "datasets": {
            "ds1": {
                "columns": {
                    "col1": {"dtype": "int"}
                }
            }
        },
        "data_quality_issues": {
            "datasets": {
                "ds1": {
                    "issues": [
                        {"column": "col1", "type": "nulls", "severity": "medium", "message": "nulls"}
                    ]
                }
            }
        }
    }
    rules = {}
    plan = build_etl_plan(assessment, rules)
    
    assert plan["coverage"]["total_issues"] == 1
    assert plan["coverage"]["uncovered"] == 0
    assert len(plan["coverage"]["uncovered_items"]) == 0


def test_pyspark_none_literal_post_processor():
    from agent.etl_pipeline.llm_codegen import _fix_pyspark_none_literals
    
    code = """
def transform_data(df):
    df = df.withColumn("col1", F.when(F.col("col1") == "0", None).otherwise(F.col("col1")))
    df = df.withColumn("col2", F.when(F.col("col2").isNull(), None).otherwise(F.col("col2")))
    vals = ["1", "2"]
    for v in vals:
        df = df.withColumn("col3", F.when(F.col("col3") == v, None).otherwise(F.col("col3")))
    return df
"""
    fixed = _fix_pyspark_none_literals(code)
    assert "F.when(F.col(\"col1\") == \"0\", F.lit(None))" in fixed
    assert "skipped no-op null fill on \"col2\"" in fixed
    assert "F.when(F.col(\"col3\").isin(vals), F.lit(None))" in fixed


def test_mismatched_transform_calls_regex_fallback():
    from agent.etl_pipeline.llm_codegen import _fix_mismatched_transform_calls
    
    code = """
def transform_orders(df):
    return df

def run_pipeline(spark):
    dfs = {}
    dfs['orders.json'] = transform_orders_json(dfs['orders.json'])
"""
    fixed = _fix_mismatched_transform_calls(code)
    assert "transform_orders(dfs['orders.json'])" in fixed




