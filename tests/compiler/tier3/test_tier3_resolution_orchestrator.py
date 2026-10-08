from __future__ import annotations

import json
import logging
import pytest
from typing import Dict

from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.models.tier3_resolution import (
    AstNodeType,
    ComparisonOperator,
    EvidenceAuthority,
    ResolutionDisposition,
    ResolutionStatus,
    RiskLevel,
    Tier3ResolutionProposal,
)
from agent.compiler.planners.ingestion_bridge import CanonicalIssue, IngestionBridge
from agent.compiler.resolution.resolution_orchestrator import (
    ResolutionOrchestrator,
    ResolutionResult,
)
from agent.compiler.tier3.cache import Tier3ReplayCache
from agent.compiler.tier3.eligibility import (
    Tier3EligibilityResult,
    is_tier3b_eligible,
)
from agent.compiler.tier3.model_client import MockTier3ModelClient


def _make_sample_profiles() -> Dict[str, DatasetProfile]:
    return {
        "dbo.Dhara_Employee": DatasetProfile(
            dataset_id="dbo.Dhara_Employee",
            row_count=100,
            columns={
                "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
                "EmploymentType": ColumnProfile(name="EmploymentType", dtype="string"),
                "ContractEndDate": ColumnProfile(name="ContractEndDate", dtype="date"),
                "Status": ColumnProfile(name="Status", dtype="string"),
                "Salary": ColumnProfile(name="Salary", dtype="numeric"),
                "ManagerID": ColumnProfile(name="ManagerID", dtype="int"),
            },
        )
    }


def _make_valid_auto_approvable_proposal(issue_id: str) -> str:
    """Low-risk action (TRIM or FLAG_ROW) with declared authority can be auto-approved."""
    proposal = {
        "contract_version": "1.0",
        "proposal_id": f"prop_flag_{issue_id}",
        "issue_id": issue_id,
        "dataset_name": "dbo.Dhara_Employee",
        "proposed_action_type": "FLAG_ROW",
        "target_columns": ["Status"],
        "disposition": "FLAG",
        "condition_ast": {
            "node_type": "BOOLEAN",
            "operator": "AND",
            "operands": [
                {
                    "node_type": "COMPARISON",
                    "operator": "EQ",
                    "left": {
                        "node_type": "COLUMN",
                        "dataset": "dbo.Dhara_Employee",
                        "column": "EmploymentType",
                    },
                    "right": {
                        "node_type": "LITERAL",
                        "data_type": "STRING",
                        "value": "Contractor",
                    },
                },
                {
                    "node_type": "NULL_CHECK",
                    "column": {
                        "node_type": "COLUMN",
                        "dataset": "dbo.Dhara_Employee",
                        "column": "ContractEndDate",
                    },
                    "is_null": True,
                },
            ],
        },
        "action_parameters": {"flag_column": "Is_Contractor_Missing_End_Date"},
        "evidence_authorities": ["APPROVED_BUSINESS_RULE"],
        "evidence_references": ["rule_contractor_date"],
        "confidence": 0.95,
        "risk_level": "LOW",
        "approval_required": False,
        "rationale": "Flag contractor records missing contract end date per business policy",
        "assumptions": [],
    }
    return json.dumps(proposal)


def _make_valid_approval_required_proposal(issue_id: str) -> str:
    """High-risk action (CONDITIONAL_UPDATE or NULLIFY) requires steward approval."""
    proposal = {
        "contract_version": "1.0",
        "proposal_id": f"prop_update_{issue_id}",
        "issue_id": issue_id,
        "dataset_name": "dbo.Dhara_Employee",
        "proposed_action_type": "CONDITIONAL_UPDATE",
        "target_columns": ["Status"],
        "disposition": "CORRECT",
        "condition_ast": {
            "node_type": "COMPARISON",
            "operator": "EQ",
            "left": {
                "node_type": "COLUMN",
                "dataset": "dbo.Dhara_Employee",
                "column": "EmploymentType",
            },
            "right": {
                "node_type": "LITERAL",
                "data_type": "STRING",
                "value": "Contractor",
            },
        },
        "action_parameters": {
            "set_column": "Status",
            "set_value": "Contractor_Pending_Verification",
        },
        "evidence_authorities": ["MODEL_INFERENCE"],
        "evidence_references": ["inferred_status_update"],
        "confidence": 0.91,
        "risk_level": "HIGH",
        "approval_required": True,
        "rationale": "Set status to pending verification for contractors",
        "assumptions": ["Contractors without end date need verification"],
    }
    return json.dumps(proposal)


# ==============================================================================
# 1. Eligibility Tests
# ==============================================================================

def test_is_tier3b_eligible_complex_condition():
    """Complex multi-column condition is eligible for Tier 3B LLM resolution."""
    profiles = _make_sample_profiles()
    issue = CanonicalIssue(
        issue_id="ci_emp_contract_1",
        dataset="dbo.Dhara_Employee",
        raw_dataset="Dhara_Employee",
        column="ContractEndDate",
        issue_type="conditional_consistency_violation",
        suggested_action=None,
        severity="medium",
        message="ContractEndDate is null when EmploymentType is Contractor",
    )
    result = is_tier3b_eligible(issue, profiles=profiles)
    assert result.eligible is True
    assert result.reason_code == "COMPLEX_SEMANTIC_REMEDIATION"
    assert result.requires_human_authority is False
    assert "CONDITIONAL_UPDATE" in result.supported_action_types


def test_is_tier3b_eligible_informational_avoided():
    """Informational observations (outlier flags, string length) are NOT eligible."""
    profiles = _make_sample_profiles()
    for itype in ["string_length_outlier", "all_caps_values", "binary_like_column", "flag_outliers", "planned_flag_near_duplicate_cluster"]:
        issue = CanonicalIssue(
            issue_id=f"ci_info_{itype}",
            dataset="dbo.Dhara_Employee",
            raw_dataset="Dhara_Employee",
            column="EmploymentType",
            issue_type=itype,
            suggested_action=None,
            severity="low",
            message=f"Statistical observation: {itype}",
        )
        result = is_tier3b_eligible(issue, profiles=profiles)
        assert result.eligible is False
        assert result.reason_code == "INFORMATIONAL"
        assert result.requires_human_authority is False


def test_is_tier3b_eligible_human_authority_required():
    """Missing authoritative sentinel policy or salary threshold requires human authority."""
    profiles = _make_sample_profiles()
    issue = CanonicalIssue(
        issue_id="ci_unknown_sentinel",
        dataset="dbo.Dhara_Employee",
        raw_dataset="Dhara_Employee",
        column="Salary",
        issue_type="outlier",
        suggested_action=None,
        severity="high",
        message="Salary contains -999 without declared sentinel meaning",
        resolution_path="manual_review_missing_sentinel_list",
    )
    result = is_tier3b_eligible(issue, profiles=profiles)
    assert result.eligible is False
    assert result.requires_human_authority is True
    assert result.reason_code == "MISSING_AUTHORITATIVE_SENTINEL_POLICY"


def test_is_tier3b_eligible_unsupported_primitive():
    """Operations without compiler execution primitive are classified as UNSUPPORTED."""
    profiles = _make_sample_profiles()
    issue = CanonicalIssue(
        issue_id="ci_unsupported_primitive",
        dataset="dbo.Dhara_Employee",
        raw_dataset="Dhara_Employee",
        column="EmploymentType",
        issue_type="custom_format",
        suggested_action="title_case",
        severity="medium",
        message="Convert string to title case",
    )
    result = is_tier3b_eligible(issue, profiles=profiles)
    assert result.eligible is False
    assert result.reason_code == "UNSUPPORTED_PRIMITIVE_REQUIRED"


# ==============================================================================
# 2. Resolution Orchestrator Tests
# ==============================================================================

def test_orchestrator_eligible_issue_invokes_model_and_promotes_auto_approved(caplog):
    """Eligible low-risk issue invokes model, passes validation, and is auto-approved into promoted steps."""
    caplog.set_level(logging.INFO)
    profiles = _make_sample_profiles()
    issue_id = "ci_emp_contract_auto"
    mock_client = MockTier3ModelClient(
        default_response=_make_valid_auto_approvable_proposal(issue_id)
    )
    orchestrator = ResolutionOrchestrator(model_client=mock_client)

    issue = CanonicalIssue(
        issue_id=issue_id,
        dataset="dbo.Dhara_Employee",
        raw_dataset="Dhara_Employee",
        column="ContractEndDate",
        issue_type="contract_end_date_missing",
        suggested_action=None,
        severity="medium",
        message="Flag contractors missing contract end date",
    )

    res: ResolutionResult = orchestrator.resolve(
        canonical_issues=[issue],
        dataset_profiles=profiles,
    )

    # Model was called once
    assert mock_client.call_count == 1
    # Auto-approved proposal promoted into plan steps
    assert len(res.promoted_steps) == 1
    assert res.promoted_steps[0]["action"] in ("flag_row", "conditional_update")
    assert res.promoted_steps[0]["source"] == "TIER3"
    assert issue_id in res.promoted_steps[0]["source_issue_ids"]

    # Not in manual review because it was auto-approved
    assert len(res.manual_review) == 0

    # Structured telemetry verified
    log_text = caplog.text
    assert "[Tier3Resolution]" in log_text
    assert f"issue_id={issue_id}" in log_text
    assert "model_called=true" in log_text
    assert "final_status=AUTO_APPROVED" in log_text
    assert "[Tier3Summary]" in log_text
    assert "auto_approved=1" in log_text


def test_orchestrator_approval_required_enriches_manual_review(caplog):
    """High-risk valid proposal appears in manual review enriched with recommendation and does not enter executable actions."""
    caplog.set_level(logging.INFO)
    profiles = _make_sample_profiles()
    issue_id = "ci_emp_contract_approval"
    mock_client = MockTier3ModelClient(
        default_response=_make_valid_approval_required_proposal(issue_id)
    )
    orchestrator = ResolutionOrchestrator(model_client=mock_client)

    issue = CanonicalIssue(
        issue_id=issue_id,
        dataset="dbo.Dhara_Employee",
        raw_dataset="Dhara_Employee",
        column="Status",
        issue_type="conditional_status_update",
        suggested_action=None,
        severity="medium",
        message="Update status for contractors without end date",
    )

    res: ResolutionResult = orchestrator.resolve(
        canonical_issues=[issue],
        dataset_profiles=profiles,
    )

    # Model called
    assert mock_client.call_count == 1
    # Not promoted into executable steps yet
    assert len(res.promoted_steps) == 0

    # Appears in manual review with enriched AI recommendation
    assert len(res.manual_review) == 1
    mr = res.manual_review[0]
    assert mr["status"] == "APPROVAL_REQUIRED"
    assert mr["approval_required"] is True
    assert mr["validation_status"] == "VALIDATED"
    assert "llm_recommendation" in mr
    assert mr["llm_recommendation"]["action_type"] == "CONDITIONAL_UPDATE"
    assert "resolution_options" in mr
    assert any(opt.get("is_recommended") for opt in mr["resolution_options"])

    # Telemetry verified
    assert "approval_required=1" in caplog.text


def test_orchestrator_informational_issue_avoids_model(caplog):
    """Informational issues do not invoke the LLM and are classified as non-blocking."""
    caplog.set_level(logging.INFO)
    profiles = _make_sample_profiles()
    mock_client = MockTier3ModelClient()
    orchestrator = ResolutionOrchestrator(model_client=mock_client)

    issue = CanonicalIssue(
        issue_id="ci_string_len_outlier",
        dataset="dbo.Dhara_Employee",
        raw_dataset="Dhara_Employee",
        column="EmploymentType",
        issue_type="string_length_outlier",
        suggested_action=None,
        severity="low",
        message="String length distribution anomaly",
    )

    res: ResolutionResult = orchestrator.resolve(
        canonical_issues=[issue],
        dataset_profiles=profiles,
    )

    # Model was NEVER called
    assert mock_client.call_count == 0
    assert len(res.promoted_steps) == 0
    assert len(res.informational_items) == 1
    assert res.informational_items[0]["is_blocking"] is False

    assert "model_called=false" in caplog.text
    assert "reason=INFORMATIONAL" in caplog.text


def test_orchestrator_human_authority_issue_avoids_model(caplog):
    """Human authority issues do not invoke the LLM and route to manual review."""
    caplog.set_level(logging.INFO)
    profiles = _make_sample_profiles()
    mock_client = MockTier3ModelClient()
    orchestrator = ResolutionOrchestrator(model_client=mock_client)

    issue = CanonicalIssue(
        issue_id="ci_salary_sentinel",
        dataset="dbo.Dhara_Employee",
        raw_dataset="Dhara_Employee",
        column="Salary",
        issue_type="outlier",
        suggested_action=None,
        severity="high",
        message="Salary contains -999",
        resolution_path="manual_review_missing_sentinel_list",
    )

    res: ResolutionResult = orchestrator.resolve(
        canonical_issues=[issue],
        dataset_profiles=profiles,
    )

    # Model was NEVER called
    assert mock_client.call_count == 0
    assert len(res.promoted_steps) == 0
    assert len(res.manual_review) == 1
    assert res.manual_review[0]["is_blocking"] is True
    assert res.manual_review[0]["requires_human_authority"] is True

    assert "model_called=false" in caplog.text
    assert "reason=MISSING_AUTHORITATIVE_SENTINEL_POLICY" in caplog.text


def test_orchestrator_cache_hit_avoids_duplicate_model_call(caplog):
    """Identical consecutive request uses cache without invoking model client again."""
    caplog.set_level(logging.INFO)
    profiles = _make_sample_profiles()
    issue_id = "ci_cache_test"
    mock_client = MockTier3ModelClient(
        default_response=_make_valid_auto_approvable_proposal(issue_id)
    )
    cache = Tier3ReplayCache()
    orchestrator = ResolutionOrchestrator(model_client=mock_client, cache=cache)

    issue = CanonicalIssue(
        issue_id=issue_id,
        dataset="dbo.Dhara_Employee",
        raw_dataset="Dhara_Employee",
        column="ContractEndDate",
        issue_type="contract_end_date_missing",
        suggested_action=None,
        severity="medium",
        message="Flag contractors missing contract end date",
    )

    # 1st call: cache miss, calls model
    res1 = orchestrator.resolve(canonical_issues=[issue], dataset_profiles=profiles)
    assert mock_client.call_count == 1
    assert len(res1.promoted_steps) == 1

    # 2nd call: cache hit, model client NOT called again
    res2 = orchestrator.resolve(canonical_issues=[issue], dataset_profiles=profiles)
    assert mock_client.call_count == 1
    assert len(res2.promoted_steps) == 1
    assert res2.telemetry["cache_hit"] == 1


def test_orchestrator_user_feedback_retry():
    """User natural language preference is passed in request metadata for revised proposal generation."""
    profiles = _make_sample_profiles()
    issue_id = "ci_feedback_test"
    mock_client = MockTier3ModelClient(
        default_response=_make_valid_approval_required_proposal(issue_id)
    )
    orchestrator = ResolutionOrchestrator(model_client=mock_client)

    issue = CanonicalIssue(
        issue_id=issue_id,
        dataset="dbo.Dhara_Employee",
        raw_dataset="Dhara_Employee",
        column="Status",
        issue_type="conditional_status_update",
        suggested_action=None,
        severity="medium",
        message="Review status for contractors",
    )

    feedback = "Please set status to 'Contractor_Pending_Verification' only for contractors without contract end date"
    res = orchestrator.resolve(
        canonical_issues=[issue],
        dataset_profiles=profiles,
        user_feedback_by_issue={issue_id: feedback},
    )

    assert mock_client.call_count == 1
    req = mock_client.requests_received[0]
    assert req.metadata.get("user_feedback") == feedback


def test_orchestrator_model_unavailable_fallback(caplog):
    """Model timeout / service unavailability degrades gracefully to manual review without crashing."""
    caplog.set_level(logging.INFO)
    profiles = _make_sample_profiles()
    issue_id = "ci_timeout_test"
    mock_client = MockTier3ModelClient(simulate_timeout=True)
    orchestrator = ResolutionOrchestrator(model_client=mock_client)

    issue = CanonicalIssue(
        issue_id=issue_id,
        dataset="dbo.Dhara_Employee",
        raw_dataset="Dhara_Employee",
        column="ContractEndDate",
        issue_type="contract_end_date_missing",
        suggested_action=None,
        severity="medium",
        message="Flag contractors missing contract end date",
    )

    res = orchestrator.resolve(canonical_issues=[issue], dataset_profiles=profiles)
    # Does not crash!
    assert res.ok is True
    assert len(res.promoted_steps) == 0
    assert len(res.manual_review) == 1
    assert res.manual_review[0]["status"] == "MODEL_UNAVAILABLE"
    assert "model_called=true final_status=MODEL_UNAVAILABLE" in caplog.text


# ==============================================================================
# 3. Full Planner Integration Test
# ==============================================================================

def test_planner_build_plan_tier3_enrichment():
    """Verify that build_plan enriches plan with Tier 3 telemetry and observations."""
    from agent.etl_pipeline.planner import PlanBuilder

    assessment = {
        "datasets": {
            "dbo.Dhara_Employee": {
                "row_count": 100,
                "columns": {
                    "EmployeeID": {"data_type": "int"},
                    "EmploymentType": {"data_type": "string"},
                    "ContractEndDate": {"data_type": "date"},
                    "Status": {"data_type": "string"},
                },
            }
        }
    }
    rules = {
        "rules": [],
    }

    builder = PlanBuilder(assessment=assessment, business_rules_raw=rules, engine="azure_sql")
    plan = builder.build()

    assert "manual_review" in plan
    assert "coverage" in plan
    # tier3 telemetry and observations are present in plan
    assert "tier3_telemetry" in plan
    assert "informational_observations" in plan
