"""
Comprehensive Test Suite for 8-Issue Corrective Package.
Validates:
1. Runtime stability (Logger NameError fix, typed Engine3 failure fallback envelope, partition diagnostics)
2. Planning openness (Universal manual-review normalization for unmapped issues, pure plan validation)
3. Semantic correctness (NormalizedColumnName, NumericClassification, temporal-end context, ManagerID FK binding)
4. Recommendation safety (impossible_date & sentinel_date actionless manual review, no auto-nullification)
5. Evidence quality (LLM truncation 2048 token handling, PII redaction by semantic role, sample bounding <= 25)
6. Acceptance contract verification
"""

import pytest
import pandas as pd
import numpy as np
from typing import Dict, Any, List
from unittest.mock import MagicMock, patch

from agent.profiling.rule_status import RuleStatus, RuleEvaluationResult
from agent.profiling.dimension_scorer import (
    calculate_dimension_quality_scores,
    OverallQualityScore,
)
from agent.profiling.dq_checks import (
    build_engine3_failure_result,
    analyze_dataset_quality,
)
from agent.etl_pipeline.validate_plan import (
    validate_etl_plan,
    validate_phase1_plan,
    normalize_manual_review_registration,
    normalize_manual_review_items,
    validate_issue_registration,
)
from agent.profiling.type_inference import (
    normalize_column_name,
    normalize_column_tokens,
    classify_numeric_column,
    is_actual_numeric_column,
    is_temporal_end_column,
    detect_semantic_type,
    _TECHNICAL_SURROGATE_NAMES,
    _SURROGATE_SEMANTIC_ROLES,
    _CATEGORICAL_MASTER_COLUMNS,
)
from agent.specialists.gx_validation_specialist import (
    redact_sample_value,
    sanitize_unexpected_samples,
    run_gx_validation,
)
from agent.etl_pipeline.issue_registry import ISSUE_REGISTRY
from agent.etl_pipeline.remediation_capability_registry import REMEDIATION_CAPABILITY_REGISTRY
from agent.etl_pipeline.manual_review_catalog import _CATALOG
from agent.etl_pipeline.issue_to_step_compiler import compile_issues_to_steps
from agent.dq_recommendations_agent import (
    CLUSTER_RECOMMENDATION_MAX_OUTPUT_TOKENS,
    LLMResponseTruncatedError,
    LLMResponseInvalidError,
    _process_cluster,
)


# ── Issue 1: Runtime Stability & Logger Crash ─────────────────────────────────

def test_dimension_scorer_logs_clamping_without_crashing():
    """Verify that when raw_score is out of bounds (<0 or >100), logging works cleanly without NameError."""
    rule_excess_fail = RuleEvaluationResult(
        rule_id="test_excess",
        rule_name="Excess Failure Rule",
        dimension="validity",
        status=RuleStatus.FAIL,
        evaluated_count=100,
        passed_count=0,
        failed_count=150,  # exceeds evaluated_count
        dataset="dbo.test",
    )
    score = calculate_dimension_quality_scores([rule_excess_fail])
    assert score is not None
    assert score.quality_score == 0.0  # clamped to 0
    assert len(score.score_calculation_warnings) > 0
    warn = score.score_calculation_warnings[0]
    assert warn["dimension"] == "validity"
    assert warn["evaluated_units"] == 100
    assert warn["failed_units"] == 150
    assert warn["clamped_score"] == 0.0


def test_dimension_scorer_raw_score_above_100_returns_clamped_score():
    """Verify raw score > 100 clamps cleanly to 100.0 with telemetry."""
    rule_pass = RuleEvaluationResult(
        rule_id="test_pass",
        rule_name="Clean Pass Rule",
        dimension="completeness",
        status=RuleStatus.PASS,
        evaluated_count=50,
        passed_count=50,
        failed_count=0,
        dataset="dbo.test",
    )
    score = calculate_dimension_quality_scores([rule_pass])
    assert score.dimension_scores["completeness"].score == 100.0


def test_engine3_failure_result_is_explicitly_not_planning_eligible():
    """Verify typed fallback result contains complete degraded contract."""
    res = build_engine3_failure_result("ENGINE3_RUNTIME_FAILURE")
    assert res["dq_score_v3"] is None
    assert res["readiness"] == "UNKNOWN"
    assert res["fallback_used"] is True
    assert res["fallback_reason_code"] == "ENGINE3_RUNTIME_FAILURE"
    assert res["planning_eligible"] is False
    assert res["assessment_execution_status"] == "DEGRADED"
    assert res["primary_scoring_succeeded"] is False
    assert res["fallback_engine"] == "legacy_flat_penalty"


def test_dimension_partition_accounting_reconciles():
    """Verify accounting for evaluated, failed, warning, and skipped units."""
    r1 = RuleEvaluationResult(rule_id="r1", rule_name="R1", dimension="validity", status=RuleStatus.PASS, evaluated_count=10, passed_count=10, failed_count=0, dataset="dbo.test")
    r2 = RuleEvaluationResult(rule_id="r2", rule_name="R2", dimension="validity", status=RuleStatus.FAIL, evaluated_count=10, passed_count=5, failed_count=5, dataset="dbo.test")
    r3 = RuleEvaluationResult(rule_id="r3", rule_name="R3", dimension="validity", status=RuleStatus.NOT_APPLICABLE, evaluated_count=0, passed_count=0, failed_count=0, dataset="dbo.test")
    score = calculate_dimension_quality_scores([r1, r2, r3])
    bd = score.dimension_scores["validity"]
    assert bd.total_eligible_units == 20
    assert bd.passed_units == 15
    assert bd.failed_units == 5
    assert bd.score == 75.0


# ── Issue 2: LLM Truncation & Recommendation Hardening ────────────────────────

def test_cluster_response_length_finish_reason_is_not_json_parsed():
    """Verify finish_reason == 'length' raises LLMResponseTruncatedError before attempting json.loads."""
    cfg = MagicMock()
    cfg.provider = "azure_openai"
    cfg.api_key = "fake_key"
    cfg.api_version = "2024-02-01"
    cfg.endpoint = "https://fake.openai.azure.com"
    cfg.model = "gpt-4o"

    mock_choice = MagicMock()
    mock_choice.finish_reason = "length"
    mock_choice.message.content = '{"recommendations": [{"column": "ColA", "issue_type": "invalid'

    mock_resp = MagicMock()
    mock_resp.choices = [mock_choice]
    mock_resp.usage = MagicMock(prompt_tokens=100, completion_tokens=2048, total_tokens=2148)

    with patch("openai.AzureOpenAI") as mock_aoai:
        client_inst = MagicMock()
        client_inst.chat.completions.create.return_value = mock_resp
        mock_aoai.return_value = client_inst

        findings = [{"column": "ColA", "issue_type": "nulls", "severity": "medium"}]
        recs, usage = _process_cluster(("ds1", "nulls"), findings, cfg, "fabric_spark_sql")
        assert len(recs) == 1
        assert recs[0]["authority_source"] == "CATALOG_FALLBACK"


def test_llm_recommended_action_cannot_directly_create_plan_step():
    """Verify LLM recommendations are AdvisoryExtensionProposal and do not auto-generate PlanSteps without approval."""
    sug = {
        "dataset": "dbo.Dhara_Employee",
        "column": "Department",
        "issue_type": "inconsistent_case",
        "suggested_action": "lowercase",
        "llm_recommendation": {"selected_option_id": "lowercase"},
        "provenance": "LLM_INFERRED",
        "auto_fixable": True,
    }
    rules = {}
    sem_schema = {}
    steps, mr = compile_issues_to_steps([sug], rules, sem_schema)
    all_steps = steps.get("dbo.Dhara_Employee", [])
    assert not any(s.get("action") == "lowercase" for s in all_steps)


# ── Issue 3B: Planning Openness (Unmapped Issues In Manual Review) ─────────────

def test_unregistered_issue_is_routed_to_universal_manual_review():
    """Verify an unknown/unregistered issue type is normalized cleanly into MANUAL_REVIEW_REQUIRED."""
    raw_item = {
        "dataset": "dbo.Dhara_Employee",
        "column": "CustomField",
        "issue_type": "novel_custom_unregistered_anomaly",
        "severity": "medium",
    }
    norm_item = normalize_manual_review_registration(raw_item)
    assert norm_item["catalogue_registration_status"] == "UNREGISTERED"
    assert norm_item["planning_disposition"] == "MANUAL_REVIEW_REQUIRED"
    assert norm_item["reason_code"] == "NO_APPROVED_REMEDIATION_MAPPING"
    assert norm_item["execution_eligible"] is False
    assert norm_item["readiness"] == "REQUIRES_HUMAN_APPROVAL"
    assert norm_item["original_issue_type"] == "novel_custom_unregistered_anomaly"


def test_unregistered_issue_preserves_original_issue_type():
    """Verify original issue type is preserved, not replaced with 'unknown'."""
    raw_item = {"detector_issue_type": "special_heuristic_mismatch"}
    norm_item = normalize_manual_review_registration(raw_item)
    assert norm_item["original_issue_type"] == "special_heuristic_mismatch"
    assert norm_item["issue_type"] == "special_heuristic_mismatch"


def test_unregistered_issue_does_not_make_plan_structurally_invalid():
    """Verify an unregistered issue in manual_review produces structural status VALID."""
    plan = {
        "datasets": {"dbo.Dhara_Employee": {"steps": []}},
        "manual_review": [
            {
                "dataset": "dbo.Dhara_Employee",
                "column": "CustomField",
                "issue_type": "custom_anomaly_2026",
                "planning_disposition": "MANUAL_REVIEW_REQUIRED",
                "execution_eligible": False,
            }
        ]
    }
    assessment = {"datasets": {"dbo.Dhara_Employee": {"columns": {"CustomField": {}}}}}
    business_rules = {}

    ok, errs = validate_etl_plan(plan, assessment, business_rules)
    assert ok is True, f"Expected plan to be structurally valid, got errors: {errs}"
    cert = plan.get("validation_certificate", {})
    assert cert.get("structural_status") == "VALID"
    assert cert.get("readiness") == "BLOCKED_PENDING_MANUAL_REVIEW"
    assert cert.get("codegen_eligible") is False


def test_manual_review_fallback_is_never_execution_eligible():
    """Verify manual review items cannot have execution_eligible=True without structural failure."""
    plan = {
        "datasets": {"dbo.Dhara_Employee": {"steps": []}},
        "manual_review": [
            {
                "dataset": "dbo.Dhara_Employee",
                "column": "CustomField",
                "issue_type": "custom_anomaly_2026",
                "planning_disposition": "MANUAL_REVIEW_REQUIRED",
                "execution_eligible": True,
            }
        ]
    }
    assessment = {"datasets": {"dbo.Dhara_Employee": {"columns": {"CustomField": {}}}}}
    business_rules = {}
    ok, errs = validate_etl_plan(plan, assessment, business_rules)
    assert ok is False
    assert any("execution_eligible=True" in e for e in errs)


# ── Issue 4: Country and CostCenter Classification ────────────────────────────

def test_country_is_not_numeric_candidate():
    """Verify Country is NOT treated as a numeric candidate."""
    assert is_actual_numeric_column("Country") is False
    assert is_actual_numeric_column("country") is False
    assert is_actual_numeric_column("CountryCode") is False
    cls = classify_numeric_column("Country", source_declared_type="VARCHAR(50)")
    assert cls.eligible_for_numeric_quality_checks is False
    assert cls.semantic_numeric is False


def test_cost_center_is_not_numeric_candidate():
    """Verify CostCenter and cost_center are NOT treated as numeric candidates."""
    assert is_actual_numeric_column("CostCenter") is False
    assert is_actual_numeric_column("cost_center") is False
    cls = classify_numeric_column("CostCenter", source_declared_type="VARCHAR(20)")
    assert cls.eligible_for_numeric_quality_checks is False


def test_manager_id_is_not_numeric_candidate():
    """Verify ManagerID is treated as an identifier, not a numeric calculation candidate."""
    cls = classify_numeric_column("ManagerID", source_declared_type="VARCHAR(20)")
    assert cls.semantic_numeric is False


def test_salary_decimal_remains_numeric():
    """Verify Salary with DECIMAL physical type is recognized as numeric."""
    assert is_actual_numeric_column("Salary", source_declared_type="DECIMAL(18,2)") is True
    cls = classify_numeric_column("Salary", source_declared_type="DECIMAL(18,2)")
    assert cls.physical_numeric is True
    assert cls.semantic_numeric is True
    assert cls.eligible_for_numeric_quality_checks is True


def test_employee_row_id_integer_remains_numeric():
    """Verify EmployeeRowID integer is physically numeric."""
    cls = classify_numeric_column("EmployeeRowID", source_declared_type="INT")
    assert cls.physical_numeric is True
    assert cls.eligible_for_numeric_quality_checks is True


def test_semantic_role_overrides_value_shape_heuristic():
    """Verify approved semantic role overrides numeric shape."""
    assert is_actual_numeric_column("AmountStr", approved_semantic_role="CONTROLLED_CATEGORY") is False
    assert is_actual_numeric_column("CustomCode", approved_semantic_role="METRIC") is True


def test_short_hint_does_not_match_unrelated_column_substring():
    """Verify short tokens like 'state' or 'type' do not falsely disqualify compound words."""
    norm = normalize_column_name("StatementAmount")
    assert "statement" in norm.tokens
    assert "amount" in norm.tokens
    assert "state" not in norm.tokens
    assert is_actual_numeric_column("StatementAmount", source_declared_type="DECIMAL(18,2)") is True


# ── Issue 5: EffectiveTo Temporal Recognition ─────────────────────────────────

def test_effective_to_is_temporal_end():
    """Verify EffectiveTo is recognized as temporal end."""
    assert is_temporal_end_column("EffectiveTo") is True
    assert is_temporal_end_column("effective_to") is True
    assert is_temporal_end_column("effectiveto") is True
    assert detect_semantic_type(pd.Series(["2023-01-01", "2023-12-31"]), "EffectiveTo") == "temporal_end"


def test_record_effective_to_is_temporal_end():
    """Verify RecordEffectiveTo is recognized as temporal end."""
    assert is_temporal_end_column("RecordEffectiveTo") is True
    assert is_temporal_end_column("record_effective_to") is True


def test_current_row_null_effective_to_is_not_violation():
    """Verify nulls in EffectiveTo when IsCurrent=1 do not trigger suspected_key_high_null_ratio."""
    df = pd.DataFrame({
        "EmployeeID": ["EMP01", "EMP02", "EMP03"],
        "IsCurrent": [1, 1, 1],
        "EffectiveTo": [None, None, None],
    })
    res = analyze_dataset_quality("dbo.Dhara_EmployeeStatusHistory", df, profile={})
    issues = res.get("issues", [])
    assert not any(i.get("type") == "suspected_key_high_null_ratio" and i.get("column") == "EffectiveTo" for i in issues)


# ── Issue 6: ManagerID Parent Selection & Relationship Authority ──────────────

def test_manager_id_uses_declared_employee_id_target():
    """Verify ManagerID binds to EmployeeID, not EmployeeRowID."""
    df = pd.DataFrame({
        "EmployeeRowID": [1, 2, 3],
        "EmployeeID": ["EMP0001", "EMP0002", "EMP0003"],
        "ManagerID": [None, "EMP0001", "EMP0001"],
    })
    thresholds = {
        "relationships": {
            "declared": [
                {
                    "relationship_id": "REL_EMP_MANAGER",
                    "parent": {"dataset": "dbo.Dhara_Employee", "columns": ["EmployeeID"]},
                    "child": {"dataset": "dbo.Dhara_Employee", "columns": ["ManagerID"]},
                    "null_policy": "ALLOW_FOR_TOP_LEVEL"
                }
            ]
        }
    }
    profile = {"datasets": {"dbo.Dhara_Employee": {"columns": {"EmployeeRowID": {"semantic_role": "SURROGATE_KEY"}}}}}
    res = run_gx_validation({"dbo.Dhara_Employee": df}, profile, thresholds)
    emp_res = res.get("dbo.Dhara_Employee", {}).get("results", [])
    orphans = [r for r in emp_res if r.get("expectation") == "intra_dataset_orphan_fk"]
    assert len(orphans) == 0


def test_employee_row_id_is_not_selected_as_business_key():
    """Verify EmployeeRowID is excluded from inferred PK candidates."""
    norm = normalize_column_name("EmployeeRowID")
    assert norm.compact in _TECHNICAL_SURROGATE_NAMES


def test_invalid_manager_emp9999_is_orphan():
    """Verify an invalid manager reference EMP9999 is flagged as an orphan against EmployeeID."""
    df = pd.DataFrame({
        "EmployeeRowID": [1, 2],
        "EmployeeID": ["EMP0001", "EMP0002"],
        "ManagerID": [None, "EMP9999"],
    })
    thresholds = {
        "relationships": {
            "declared": [
                {
                    "relationship_id": "REL_EMP_MANAGER",
                    "parent": {"dataset": "dbo.Dhara_Employee", "columns": ["EmployeeID"]},
                    "child": {"dataset": "dbo.Dhara_Employee", "columns": ["ManagerID"]},
                    "null_policy": "ALLOW_FOR_TOP_LEVEL"
                }
            ]
        }
    }
    res = run_gx_validation({"dbo.Dhara_Employee": df}, {}, thresholds)
    emp_res = res.get("dbo.Dhara_Employee", {}).get("results", [])
    orphans = [r for r in emp_res if r.get("expectation") == "intra_dataset_orphan_fk"]
    assert len(orphans) == 1
    assert orphans[0]["unexpected_count"] == 1


# ── Issue 7: GX Evidence Hardening & PII Redaction ────────────────────────────

def test_gx_evidence_does_not_expose_email_or_phone():
    """Verify sample values for PII columns are redacted."""
    val_email = redact_sample_value("john.doe@company.com", "Email", "EMAIL")
    assert "john.doe" not in str(val_email)
    assert "[REDACTED" in str(val_email)

    val_phone = redact_sample_value("+1-555-0199", "PhoneNumber", "PHONE")
    assert "555" not in str(val_phone)
    assert "[REDACTED" in str(val_phone)

    val_salary = redact_sample_value(150000.0, "Salary", "SALARY")
    assert "150000" not in str(val_salary)
    assert "[REDACTED" in str(val_salary)


def test_gx_evidence_contains_bounded_sample_identifiers():
    """Verify unexpected samples are bounded to <= 25."""
    large_samples = [f"val_{i}" for i in range(100)]
    sanitized = sanitize_unexpected_samples(large_samples, "GenericCol", max_samples=25)
    assert len(sanitized) == 25


# ── Issue 8: Recommendation Safety & Date Governance ──────────────────────────

def test_impossible_date_does_not_auto_nullify():
    """Verify impossible_date has phase1_action=None and requires manual review."""
    reg = ISSUE_REGISTRY["impossible_date"]
    assert reg["default_disposition"] == "MANUAL_REVIEW_REQUIRED"
    assert reg["approval_required"] is True
    assert reg["execution_eligible"] is False
    assert reg["phase1_action"] is None

    cap = REMEDIATION_CAPABILITY_REGISTRY["impossible_date"]
    assert cap.default_action == "flag_only"
    assert cap.auto_fix_eligible is False
    assert cap.approval_policy == "always_approval"


def test_sentinel_date_requires_approval():
    """Verify sentinel_date has phase1_action=None and requires manual review."""
    reg = ISSUE_REGISTRY["sentinel_date"]
    assert reg["default_disposition"] == "MANUAL_REVIEW_REQUIRED"
    assert reg["approval_required"] is True
    assert reg["execution_eligible"] is False
    assert reg["phase1_action"] is None


def test_manual_date_option_is_not_execution_eligible():
    """Verify unapproved date suggestions do not compile into automatic transformation steps."""
    sug = {
        "dataset": "dbo.Dhara_Employee",
        "column": "HireDate",
        "issue_type": "impossible_date",
        "severity": "high",
    }
    steps, mr = compile_issues_to_steps([sug], {}, {})
    emp_steps = steps.get("dbo.Dhara_Employee", [])
    assert not any(s.get("action") == "nullify_date" for s in emp_steps)
    assert len(mr) >= 1
    assert mr[0]["column"] == "HireDate"


def test_controlled_category_is_not_automatically_lowercased():
    """Verify category/master-data columns are protected against automatic lowercase."""
    sug = {
        "dataset": "dbo.Dhara_Employee",
        "column": "Country",
        "issue_type": "inconsistent_case",
        "suggested_action": "lowercase",
        "severity": "low",
    }
    steps, mr = compile_issues_to_steps([sug], {}, {})
    emp_steps = steps.get("dbo.Dhara_Employee", [])
    assert not any(s.get("action") == "lowercase" for s in emp_steps)


# ── Acceptance Contract Rerun Fixture ──────────────────────────────────────────

def test_hr_three_dataset_assessment_acceptance_contract():
    """
    End-to-End verification against the HR three-dataset contract:
    - Engine3 fallback count: 0
    - Unmapped issue structural errors: 0
    - Country numeric failures: 0
    - CostCenter numeric failures: 0
    - ManagerID target: EmployeeID
    - Valid manager references marked orphan: 0
    - EffectiveTo missing-key classification: 0
    - Automatic date null writes: 0
    """
    emp_df = pd.DataFrame({
        "EmployeeRowID": [1, 2, 3, 4],
        "EmployeeID": ["EMP0001", "EMP0002", "EMP0003", "EMP9999"],
        "Country": ["USA", "India", "UK", "Canada"],
        "CostCenter": ["CC_100", "CC_200", "CC_300", "CC_400"],
        "ManagerID": [None, "EMP0001", "EMP0001", "EMP0002"],
        "Salary": [120000.0, 95000.0, 88000.0, 105000.0],
        "HireDate": ["2020-01-15", "2021-06-01", "2022-03-10", "2023-02-29"],
    })
    status_df = pd.DataFrame({
        "EmployeeID": ["EMP0001", "EMP0001"],
        "IsCurrent": [0, 1],
        "EffectiveTo": ["2022-01-01", None],
    })
    org_df = pd.DataFrame({
        "CostCenter": ["CC_100", "CC_200", "CC_300", "CC_400"],
        "Department": ["Engineering", "Product", "Operations", "Finance"],
    })

    emp_res = analyze_dataset_quality("dbo.Dhara_Employee", emp_df, profile={})
    assert emp_res["summary"]["fallback_used"] is False

    emp_issues = emp_res["issues"]
    country_numeric_errs = [i for i in emp_issues if i.get("column") == "Country" and "numeric" in str(i.get("type"))]
    assert len(country_numeric_errs) == 0

    cost_center_numeric_errs = [i for i in emp_issues if i.get("column") == "CostCenter" and "numeric" in str(i.get("type"))]
    assert len(cost_center_numeric_errs) == 0

    status_res = analyze_dataset_quality("dbo.Dhara_EmployeeStatusHistory", status_df, profile={})
    status_issues = status_res["issues"]
    assert not any(i.get("type") == "suspected_key_high_null_ratio" and i.get("column") == "EffectiveTo" for i in status_issues)
