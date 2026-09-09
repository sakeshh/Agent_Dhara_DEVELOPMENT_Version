"""
test_issue_forwarding_and_deduplication.py

Comprehensive tests for:
1. Issue deduplication with full provenance preservation (step_identity & merge_steps).
2. End-to-end issue forwarding from assessment -> planning -> codegen without caps.
3. T-SQL quality, ANSI settings, structural syntax checks, and Level 2 coverage validation.
4. LLM resolution fallback budgeting, signature deduplication, and schema validation.
5. Assessment fingerprint caching and ydata profiling control.
"""
import asyncio
import pandas as pd
import pytest

from agent.etl_pipeline.issue_identity import (
    normalize_dataset_name,
    normalize_column_name,
    normalize_action,
    normalize_issue_type,
    normalize_message_pattern,
    step_identity,
    issue_identity,
    make_step_id,
    make_issue_id,
    make_resolution_signature,
    merge_steps,
)
from agent.etl_pipeline.planner import build_etl_plan
from agent.etl_pipeline.llm_codegen import (
    _extract_assessment_dq_issues,
    _build_codegen_payload,
    _build_user_message_parts,
    _postprocess_tsql_code,
)
from agent.etl_pipeline.validate_sql import (
    validate_security,
    validate_issue_coverage,
)
from agent.etl_pipeline.llm_resolution_fallback import (
    LLMResolutionBudget,
    validate_resolution_options,
    get_cached_resolution,
    put_cached_resolution,
)
from agent.assessment_graph.nodes import (
    _compute_df_fingerprint,
    profiler,
)


def test_issue_identity_normalization():
    assert normalize_dataset_name(" [dbo].[Orders] ") == "orders"
    assert normalize_dataset_name("dbo.Customers") == "customers"
    assert normalize_dataset_name(None) == "_global"

    assert normalize_column_name(' "[User_Id]" ') == "user_id"
    assert normalize_column_name(None) == "*"

    assert normalize_action(" TRIM ") == "trim"
    assert normalize_action(None) == "noop"

    # Canonical mapping via central registry
    assert normalize_issue_type("missing_values") == "nulls"
    assert normalize_issue_type("orphan_foreign_key") == "orphan_foreign_key_rows"

    # Message pattern normalization collapses numbers, dates, literals
    msg1 = "Row 42 has date 2023-05-12 with value 'XYZ'"
    msg2 = "Row 108 has date 2024-11-20 with value 'ABC'"
    assert normalize_message_pattern(msg1) == normalize_message_pattern(msg2)


def test_merge_steps_provenance_preservation():
    step1 = {
        "dataset": "orders",
        "column": "amount",
        "action": "flag_outliers",
        "source_issue_type": "outliers",
        "severity": "medium",
        "estimated_affected_rows": 10,
        "note": "Outliers detected in amount",
        "params": {"iqr_multiplier": 1.5},
        "evidence": {"confidence": 0.8},
    }
    step2 = {
        "dataset": "orders",
        "column": "amount",
        "action": "flag_outliers",
        "source_issue_type": "high_null_percentage",
        "severity": "high",
        "estimated_affected_rows": 25,
        "note": "High null ratio on measure",
        "params": {"flag_name": "is_outlier"},
        "evidence": {"confidence": 0.95},
    }

    merged = merge_steps(step1, step2)

    # Step winner is step2 due to higher severity (high > medium) and higher affected rows
    assert merged["severity"] == "high"
    assert merged["estimated_affected_rows"] == 25
    assert merged["step_id"].startswith("STEP-")

    # Full provenance preserved
    assert set(merged["source_issue_types"]) == {"outliers", "high_null_percentage"}
    assert len(merged["source_issue_ids"]) == 2
    assert merged["params"]["iqr_multiplier"] == 1.5
    assert merged["params"]["flag_name"] == "is_outlier"


def test_planning_step_deduplication():
    # Assessment with multiple suggestions that result in the same action on the same column
    assessment = {
        "datasets": {
            "sales": {
                "columns": {
                    "customer_name": {"dtype": "string", "null_percentage": 0.0},
                },
                "quality": {
                    "issues": [
                        {"column": "customer_name", "issue_type": "whitespace", "severity": "medium", "suggested_action": "trim"},
                        {"column": "customer_name", "issue_type": "column_name_whitespace", "severity": "high", "suggested_action": "trim"},
                    ]
                }
            }
        },
        "data_quality_issues": {
            "datasets": {
                "sales": {
                    "issues": [
                        {"column": "customer_name", "issue_type": "whitespace", "severity": "medium", "suggested_action": "trim"}
                    ]
                }
            }
        }
    }
    rules = {}
    source_context = {
        "suggestions": [
            {
                "dataset": "sales",
                "column": "customer_name",
                "issue_type": "whitespace",
                "severity": "medium",
                "suggested_action": "trim",
                "auto_fixable": True,
            },
            {
                "dataset": "sales",
                "column": "customer_name",
                "issue_type": "column_name_whitespace",
                "severity": "high",
                "suggested_action": "trim",
                "auto_fixable": True,
            },
        ]
    }
    plan = build_etl_plan(assessment, rules, source_context=source_context)

    sales_steps = plan["datasets"]["sales"]["steps"]
    trim_steps = [s for s in sales_steps if s.get("action") == "trim" and s.get("column") == "customer_name"]

    # Exactly ONE merged step must exist — no duplicates!
    assert len(trim_steps) == 1
    step = trim_steps[0]
    assert "source_issue_types" in step
    assert len(step["source_issue_types"]) >= 1
    assert "step_id" in step


def test_issue_forwarding_zero_omissions():
    assessment = {
        "data_quality_issues": {
            "datasets": {
                "users": {
                    "issues": [
                        {"column": f"col_{i}", "issue_type": f"type_{i}", "severity": "medium", "message": f"Defect {i}"}
                        for i in range(50)  # 50 issues > previous 35 cap
                    ]
                }
            }
        },
        "datasets": {"users": {"columns": {f"col_{i}": {"dtype": "string"} for i in range(50)}}}
    }
    plan = {
        "plan_id": "test_plan_all_issues",
        "engine": "sql-tsql",
        "datasets": {"users": {"steps": []}},
        "manual_review": [
            {"column": f"mr_col_{i}", "issue_type": f"mr_type_{i}", "severity": "high", "message": f"MR {i}"}
            for i in range(30)  # 30 items > previous 20 cap
        ]
    }

    # 1. Extraction captures all issues from all sources
    all_extracted = _extract_assessment_dq_issues(assessment, plan)
    assert len(all_extracted) >= 80  # 50 dq + 30 manual review

    for iss in all_extracted:
        assert "issue_id" in iss
        assert iss["issue_id"].startswith("ISSUE-")

    # 2. Codegen payload includes issue_coverage_manifest
    payload = _build_codegen_payload(plan, assessment)
    assert "issue_coverage_manifest" in payload
    assert len(payload["issue_coverage_manifest"]) >= 80

    # 3. User message parts forward all issues without arbitrary [:35] or [:20] truncations
    parts = _build_user_message_parts("sql-tsql", payload)
    user_msg = "\n\n".join(parts)

    # Verify that defects from the tail of the list (e.g. col_49 and mr_col_29) are present
    assert "col_49" in user_msg
    assert "mr_col_29" in user_msg
    assert "-- COVERS: {issue_id}" in user_msg


def test_tsql_structural_checks_and_postprocessing():
    # 1. Check Level 1 GO inside BEGIN TRY detection
    bad_sql = """
    BEGIN TRY
        SELECT 1;
        GO
        SELECT 2;
    END TRY
    BEGIN CATCH
        SELECT 0;
    END CATCH
    """
    ok, issues = validate_security(bad_sql)
    assert not ok
    assert any("GO' batch separator inside BEGIN TRY" in iss for iss in issues)

    # 2. Check unreplaced template detection
    template_sql = "SELECT * FROM {{table_name}};"
    ok, issues = validate_security(template_sql)
    assert not ok
    assert any("Unreplaced template placeholder" in iss for iss in issues)

    # 3. Check postprocessing injects SET ANSI_NULLS ON and QUOTED_IDENTIFIER
    raw_tsql = "BEGIN TRY\n    SELECT 1;\nEND TRY\nBEGIN CATCH\n    THROW;\nEND CATCH"
    postprocessed = _postprocess_tsql_code(raw_tsql)
    assert "SET ANSI_NULLS ON;" in postprocessed
    assert "SET QUOTED_IDENTIFIER ON;" in postprocessed
    assert "GO" in postprocessed


def test_tsql_level2_coverage_validation():
    manifest = [
        {"issue_id": "ISSUE-1111", "column": "price", "issue_type": "negative_values", "planned_action": "flag_cross_field_violation"},
        {"issue_id": "ISSUE-2222", "column": "email", "issue_type": "invalid_email_format", "planned_action": "sanitize_email"},
    ]

    # Full coverage script
    covered_sql = """
    -- COVERS: ISSUE-1111
    UPDATE #Orders_Staging SET price_clean = NULL WHERE price < 0;

    -- COVERS: ISSUE-2222
    UPDATE #Orders_Staging SET email_clean = NULL WHERE email NOT LIKE '%_@_%._%';
    """
    res = validate_issue_coverage(covered_sql, manifest)
    assert res["valid"]
    assert res["coverage_pct"] == 100.0
    assert len(res["missing"]) == 0

    # Partial coverage script
    partial_sql = """
    -- COVERS: ISSUE-1111
    UPDATE #Orders_Staging SET price_clean = NULL WHERE price < 0;
    """
    res_part = validate_issue_coverage(partial_sql, manifest)
    assert not res_part["valid"]
    assert res_part["coverage_pct"] == 50.0
    assert len(res_part["missing"]) == 1
    assert res_part["missing"][0]["issue_id"] == "ISSUE-2222"


def test_llm_resolution_fallback_budget_and_validation():
    budget = LLMResolutionBudget(max_calls=3)

    sig1 = make_resolution_signature("unmapped_type_a", "string", "Err 1")
    sig2 = make_resolution_signature("unmapped_type_b", "integer", "Err 2")
    sig3 = make_resolution_signature("unmapped_type_c", "date", "Err 3")
    sig4 = make_resolution_signature("unmapped_type_d", "float", "Err 4")

    assert budget.can_call(sig1)
    budget.record_call(sig1)
    # Re-checking same signature should still be allowed (deduplicated)
    assert budget.can_call(sig1)

    budget.record_call(sig2)
    budget.record_call(sig3)

    # 4th unique signature exceeds budget of 3
    assert not budget.can_call(sig4)

    # Schema validation test
    raw_llm_output = [
        {"id": "fix_1", "label": "Drop entire table", "action": "DROP_TABLE_EXEC", "confidence": 0.9},
        {"id": "fix_2", "label": "Trim whitespace", "action": "trim", "confidence": 0.85, "recommended": True},
        {"id": "fix_3", "label": "Quarantine row", "action": "quarantine_row", "confidence": 0.7},
    ]
    validated = validate_resolution_options(raw_llm_output)

    # DROP_TABLE_EXEC is not in ACTION_REGISTRY, mapped to noop
    drop_opt = next(o for o in validated if o["id"] == "fix_1")
    assert drop_opt["action"] == "noop"

    # quarantine_row is destructive, so requires_human_approval must be True
    quarantine_opt = next(o for o in validated if o["id"] == "fix_3")
    assert quarantine_opt["requires_human_approval"] is True


def test_assessment_profiling_fingerprint_cache():
    df = pd.DataFrame({"id": range(10), "val": ["a", "b"] * 5})
    fp1 = _compute_df_fingerprint(df)
    fp2 = _compute_df_fingerprint(df)
    assert fp1 == fp2

    # Modified data changes fingerprint
    df_mod = pd.DataFrame({"id": range(10), "val": ["x", "y"] * 5})
    fp3 = _compute_df_fingerprint(df_mod)
    assert fp1 != fp3

    # Profiler reuses cache
    res1 = profiler({"dataset_name": "test_df", "df": df, "job_id": None})
    res2 = profiler({"dataset_name": "test_df", "df": df, "job_id": None})
    assert "test_df" in res1["metadata"]
    assert "test_df" in res2["metadata"]
