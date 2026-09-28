from __future__ import annotations

from datetime import datetime, timezone
import pytest

from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.models.actions import ActionSource
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.models.tier3_approval import ApprovalDecision, ApprovalRecord
from agent.compiler.models.tier3_resolution import (
    AstNodeType,
    ColumnRef,
    ComparisonExpression,
    ComparisonOperator,
    EvidenceAuthority,
    LiteralValue,
    ResolutionDisposition,
    ResolutionStatus,
    RiskLevel,
    Tier3ResolutionProposal,
)
from agent.compiler.tier3.action_adapter import Tier3ActionAdapter
from agent.compiler.tier3.approval_gate import Tier3ApprovalGate
from agent.compiler.tier3.exceptions import (
    Tier3ApprovalError,
    Tier3IntegrationError,
)
from agent.compiler.tier3.reconciliation import IssueReconciliationTracker, ResolutionBucket
from agent.compiler.tier3.validation_result import ValidationCode
from agent.compiler.tier3.validator import Tier3ValidationOrchestrator


def _create_dataset_profiles() -> dict[str, DatasetProfile]:
    cols = {
        "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
        "FirstName": ColumnProfile(name="FirstName", dtype="string"),
        "LastName": ColumnProfile(name="LastName", dtype="string"),
        "Salary": ColumnProfile(name="Salary", dtype="float"),
        "Status": ColumnProfile(name="Status", dtype="string"),
    }
    return {"dbo.Employees": DatasetProfile(dataset_id="dbo.Employees", row_count=500, columns=cols)}


def _create_compiler_context() -> CompilerContext:
    profiles = _create_dataset_profiles()
    return CompilerContext(
        session_id="sess_tier3_e2e",
        target_engine=ExecutionEngine.AZURE_SQL,
        dataset_profiles=profiles,
    )


# ==============================================================================
# 1. Low-Risk Canonical Action: TRIM (Auto-approved by Policy)
# ==============================================================================

def test_low_risk_trim_end_to_end():
    profiles = _create_dataset_profiles()
    orchestrator = Tier3ValidationOrchestrator()
    approval_gate = Tier3ApprovalGate()
    adapter = Tier3ActionAdapter()

    proposal = Tier3ResolutionProposal(
        contract_version="1.0",
        proposal_id="prop_trim_e2e",
        issue_id="issue_trim_e2e",
        dataset_name="dbo.Employees",
        proposed_action_type="TRIM",
        target_columns=["FirstName"],
        disposition=ResolutionDisposition.CORRECT,
        action_parameters={"side": "both"},
        evidence_authorities=[EvidenceAuthority.DECLARED_SCHEMA],
        evidence_references=["SCHEMA_RULE_1"],
        confidence=1.0,
        risk_level=RiskLevel.LOW,
        approval_required=False,
        status=ResolutionStatus.PROPOSED,
    )

    # 1. Validation pipeline
    val_res = orchestrator.validate_proposal(proposal, profiles)
    assert val_res.is_valid is True
    validated_proposal = proposal.model_copy(update={"status": ResolutionStatus.VALIDATED})

    # 2. Approval Gate (Auto-approval)
    approval_eval = approval_gate.evaluate(validated_proposal)
    assert approval_eval.is_approved is True
    assert approval_eval.requires_approval is False

    # 3. Action Adapter
    action = adapter.adapt(validated_proposal, val_res)
    assert action.source == ActionSource.TIER3
    assert action.action_type == ActionType.TRIM
    assert action.metadata["tier3_proposal_id"] == "prop_trim_e2e"

    # 4. Deterministic Compiler Execution
    ctx = _create_compiler_context()
    ctx.structured_actions = [action]
    pipeline = CompilerPipeline()
    compiled_ctx, _ = pipeline.compile(ctx)

    telemetry = compiled_ctx.metadata.get("compilation_telemetry", {})
    assert telemetry["ok"] is True
    assert telemetry["executable"] is True

    # 5. Verify Lineage Survives
    logical_step = compiled_ctx.logical_plan.steps[0]
    assert logical_step.source_action_id == action.action_id
    assert logical_step.source_issue_ids == ["issue_trim_e2e"]
    assert logical_step.source == ActionSource.TIER3.value

    phys_step = compiled_ctx.physical_plan.steps[0]
    assert phys_step.source_action_id == action.action_id
    assert phys_step.source_issue_ids == ["issue_trim_e2e"]

    # 6. Verify Generated T-SQL
    combined_sql = compiled_ctx.code_artifact.content
    assert "RTRIM(LTRIM([FirstName]))" in combined_sql or "LTRIM(RTRIM([FirstName]))" in combined_sql


# ==============================================================================
# 2. Approved Semantic Action: REPLACE_SENTINELS
# ==============================================================================

def test_approved_replace_sentinels_end_to_end():
    profiles = _create_dataset_profiles()
    orchestrator = Tier3ValidationOrchestrator()
    approval_gate = Tier3ApprovalGate()
    adapter = Tier3ActionAdapter()

    proposal = Tier3ResolutionProposal(
        contract_version="1.0",
        proposal_id="prop_sentinels_e2e",
        issue_id="issue_sentinels_e2e",
        dataset_name="dbo.Employees",
        proposed_action_type="REPLACE_SENTINELS",
        target_columns=["Status"],
        disposition=ResolutionDisposition.CORRECT,
        action_parameters={"sentinels": ["UNKNOWN", "N/A"]},
        evidence_authorities=[EvidenceAuthority.APPROVED_BUSINESS_RULE],
        evidence_references=["RULE_STATUS_CODES"],
        confidence=0.98,
        risk_level=RiskLevel.MEDIUM,
        approval_required=True,
        status=ResolutionStatus.PROPOSED,
    )

    # 1. Validation
    val_res = orchestrator.validate_proposal(proposal, profiles)
    assert val_res.is_valid is True

    # 2. Approval Gate (Steward grants approval)
    approval = ApprovalRecord(
        approval_id="appr_sentinels_001",
        proposal_id=proposal.proposal_id,
        proposal_fingerprint=proposal.fingerprint,
        decision=ApprovalDecision.APPROVED,
        approver_id="steward_dan",
        approver_role="DATA_STEWARD",
        decided_at=datetime.now(timezone.utc),
        reason="Approved sentinel replacements for Status",
    )
    approval_gate.record_decision(approval)
    approval_eval = approval_gate.evaluate(proposal, approval_record=approval)
    assert approval_eval.is_approved is True

    # Transition to APPROVED
    approved_proposal = proposal.model_copy(update={"status": ResolutionStatus.APPROVED})

    # 3. Action Adapter
    action = adapter.adapt(approved_proposal, val_res, approval=approval)
    assert action.action_type == ActionType.REPLACE_SENTINELS
    assert action.metadata["approval_id"] == "appr_sentinels_001"

    # 4. Compiler Pipeline
    ctx = _create_compiler_context()
    ctx.structured_actions = [action]
    pipeline = CompilerPipeline()
    compiled_ctx, _ = pipeline.compile(ctx)

    telemetry = compiled_ctx.metadata.get("compilation_telemetry", {})
    assert telemetry["ok"] is True
    assert telemetry["rendered_transformation_count"] == 1


# ==============================================================================
# 3. Approved Range Action: VALIDATE_RANGE
# ==============================================================================

def test_approved_validate_range_end_to_end():
    profiles = _create_dataset_profiles()
    orchestrator = Tier3ValidationOrchestrator()
    approval_gate = Tier3ApprovalGate()
    adapter = Tier3ActionAdapter()

    proposal = Tier3ResolutionProposal(
        contract_version="1.0",
        proposal_id="prop_range_e2e",
        issue_id="issue_salary_range_e2e",
        dataset_name="dbo.Employees",
        proposed_action_type="VALIDATE_RANGE",
        target_columns=["Salary"],
        disposition=ResolutionDisposition.CORRECT,
        action_parameters={"min_value": 1000.0, "max_value": 500000.0},
        evidence_authorities=[EvidenceAuthority.APPROVED_BUSINESS_RULE],
        evidence_references=["RULE_SALARY_BAND"],
        confidence=0.99,
        risk_level=RiskLevel.MEDIUM,
        approval_required=True,
        status=ResolutionStatus.PROPOSED,
    )

    # 1. Validation
    val_res = orchestrator.validate_proposal(proposal, profiles)
    assert val_res.is_valid is True

    # 2. Approval
    approval = ApprovalRecord(
        approval_id="appr_range_001",
        proposal_id=proposal.proposal_id,
        proposal_fingerprint=proposal.fingerprint,
        decision=ApprovalDecision.APPROVED,
        approver_id="owner_sarah",
        approver_role="DATA_OWNER",
        decided_at=datetime.now(timezone.utc),
        reason="Approved salary band bounds",
    )
    approval_eval = approval_gate.evaluate(proposal, approval_record=approval)
    assert approval_eval.is_approved is True

    approved_proposal = proposal.model_copy(update={"status": ResolutionStatus.APPROVED})

    # 3. Adapter
    action = adapter.adapt(approved_proposal, val_res, approval=approval)
    assert action.action_type == ActionType.RANGE_CLIP

    # 4. Pipeline
    ctx = _create_compiler_context()
    ctx.structured_actions = [action]
    pipeline = CompilerPipeline()
    compiled_ctx, _ = pipeline.compile(ctx)

    combined_sql = compiled_ctx.code_artifact.content
    assert "WHEN [Salary] < 1000.0 THEN 1000.0" in combined_sql
    assert "WHEN [Salary] > 500000.0 THEN 500000.0" in combined_sql


# ==============================================================================
# 4. Blocked Unapproved QUARANTINE
# ==============================================================================

def test_blocked_unapproved_quarantine():
    profiles = _create_dataset_profiles()
    orchestrator = Tier3ValidationOrchestrator()
    approval_gate = Tier3ApprovalGate()
    adapter = Tier3ActionAdapter()

    proposal = Tier3ResolutionProposal(
        contract_version="1.0",
        proposal_id="prop_quarantine_block",
        issue_id="issue_quarantine_block",
        dataset_name="dbo.Employees",
        proposed_action_type="CONDITIONAL_UPDATE",
        target_columns=["Salary"],
        disposition=ResolutionDisposition.QUARANTINE,
        action_parameters={"new_value": None},
        evidence_authorities=[EvidenceAuthority.MODEL_INFERENCE],
        evidence_references=["MODEL_SUSPICION"],
        confidence=0.99,
        risk_level=RiskLevel.HIGH,
        approval_required=True,
        status=ResolutionStatus.VALIDATED,
    )

    val_res = orchestrator.validate_proposal(proposal, profiles)

    # 1. Approval Gate blocks evaluation without approval
    with pytest.raises(Tier3ApprovalError, match="requires approval"):
        approval_gate.evaluate(proposal, approval_record=None)

    # 2. Action Adapter blocks conversion because status is not APPROVED
    with pytest.raises(Tier3IntegrationError, match="requires approval before compiler integration"):
        adapter.adapt(proposal, val_res, approval=None)


# ==============================================================================
# 5. Blocked Schema-Invalid Proposal
# ==============================================================================

def test_blocked_schema_invalid_proposal():
    profiles = _create_dataset_profiles()
    orchestrator = Tier3ValidationOrchestrator()
    adapter = Tier3ActionAdapter()

    proposal = Tier3ResolutionProposal(
        contract_version="1.0",
        proposal_id="prop_schema_bad",
        issue_id="issue_schema_bad",
        dataset_name="dbo.Employees",
        proposed_action_type="TRIM",
        target_columns=["NonExistentColumn"],
        disposition=ResolutionDisposition.CORRECT,
        action_parameters={"side": "both"},
        evidence_authorities=[EvidenceAuthority.DECLARED_SCHEMA],
        evidence_references=["REF_BAD"],
        confidence=0.9,
        risk_level=RiskLevel.LOW,
        approval_required=False,
        status=ResolutionStatus.PROPOSED,
    )

    val_res = orchestrator.validate_proposal(proposal, profiles)
    assert val_res.is_valid is False
    assert any(f.code.value == "UNKNOWN_COLUMN" for f in val_res.findings)

    with pytest.raises(Tier3IntegrationError, match="Only validated Tier 3 proposals"):
        adapter.adapt(proposal, val_res)


# ==============================================================================
# 6. Blocked Type-Invalid Proposal
# ==============================================================================

def test_blocked_type_invalid_proposal():
    profiles = _create_dataset_profiles()
    orchestrator = Tier3ValidationOrchestrator()
    adapter = Tier3ActionAdapter()

    # Range comparison on string column (FirstName)
    bad_cond = ComparisonExpression(
        operator=ComparisonOperator.GT,
        left=ColumnRef(dataset="dbo.Employees", column="FirstName"),
        right=LiteralValue(data_type="INTEGER", value=100),
    )
    proposal = Tier3ResolutionProposal(
        contract_version="1.0",
        proposal_id="prop_type_bad",
        issue_id="issue_type_bad",
        dataset_name="dbo.Employees",
        proposed_action_type="CONDITIONAL_UPDATE",
        target_columns=["FirstName"],
        disposition=ResolutionDisposition.CORRECT,
        condition_ast=bad_cond.to_dict(),
        action_parameters={"new_value": "Over100"},
        evidence_authorities=[EvidenceAuthority.APPROVED_BUSINESS_RULE],
        evidence_references=["RULE_BAD_TYPE"],
        confidence=0.8,
        risk_level=RiskLevel.MEDIUM,
        approval_required=False,
        status=ResolutionStatus.PROPOSED,
    )

    val_res = orchestrator.validate_proposal(proposal, profiles)
    assert val_res.is_valid is False
    assert any(f.code.value in ("TYPE_MISMATCH", "INVALID_OPERATOR_FOR_TYPE") for f in val_res.findings)

    with pytest.raises(Tier3IntegrationError, match="Only validated Tier 3 proposals"):
        adapter.adapt(proposal, val_res)


# ==============================================================================
# 7. Blocked Stale Approval
# ==============================================================================

def test_blocked_stale_approval():
    profiles = _create_dataset_profiles()
    orchestrator = Tier3ValidationOrchestrator()
    approval_gate = Tier3ApprovalGate()
    adapter = Tier3ActionAdapter()

    cond = ComparisonExpression(
        operator=ComparisonOperator.LT,
        left=ColumnRef(dataset="dbo.Employees", column="Salary"),
        right=LiteralValue(data_type="INTEGER", value=0),
    )
    proposal = Tier3ResolutionProposal(
        contract_version="1.0",
        proposal_id="prop_stale",
        issue_id="issue_stale",
        dataset_name="dbo.Employees",
        proposed_action_type="CONDITIONAL_UPDATE",
        target_columns=["Salary"],
        disposition=ResolutionDisposition.NULLIFY,
        condition_ast=cond.to_dict(),
        action_parameters={"new_value": None},
        evidence_authorities=[EvidenceAuthority.APPROVED_BUSINESS_RULE],
        evidence_references=["RULE_STALE"],
        confidence=0.95,
        risk_level=RiskLevel.MEDIUM,
        approval_required=True,
        status=ResolutionStatus.APPROVED,
    )

    # Approve original proposal
    approval = ApprovalRecord(
        approval_id="appr_stale_001",
        proposal_id=proposal.proposal_id,
        proposal_fingerprint=proposal.fingerprint,
        decision=ApprovalDecision.APPROVED,
        approver_id="steward_dan",
        approver_role="DATA_STEWARD",
        decided_at=datetime.now(timezone.utc),
        reason="Approved Salary < 0",
    )

    # Modify AST (Salary < -500)
    modified_cond = ComparisonExpression(
        operator=ComparisonOperator.LT,
        left=ColumnRef(dataset="dbo.Employees", column="Salary"),
        right=LiteralValue(data_type="INTEGER", value=-500),
    )
    modified_proposal = proposal.model_copy(update={"condition_ast": modified_cond.to_dict()})

    val_res = orchestrator.validate_proposal(modified_proposal, profiles)
    assert val_res.is_valid is True

    # Blocked by ApprovalGate
    with pytest.raises(Tier3ApprovalError, match="Stale approval"):
        approval_gate.evaluate(modified_proposal, approval_record=approval)

    # Blocked by ActionAdapter
    with pytest.raises(Tier3IntegrationError, match="Approval fingerprint does not match"):
        adapter.adapt(modified_proposal, val_res, approval=approval)


# ==============================================================================
# 8. Blocked Conflicting Proposals
# ==============================================================================

def test_blocked_conflicting_proposals():
    profiles = _create_dataset_profiles()
    orchestrator = Tier3ValidationOrchestrator()

    p1 = Tier3ResolutionProposal(
        contract_version="1.0",
        proposal_id="prop_c1",
        issue_id="issue_c1",
        dataset_name="dbo.Employees",
        proposed_action_type="TRIM",
        target_columns=["FirstName"],
        disposition=ResolutionDisposition.CORRECT,
        action_parameters={"side": "both"},
        evidence_authorities=[EvidenceAuthority.DECLARED_SCHEMA],
        evidence_references=["REF_1"],
        confidence=0.9,
        risk_level=RiskLevel.LOW,
        approval_required=False,
    )
    p2 = Tier3ResolutionProposal(
        contract_version="1.0",
        proposal_id="prop_c2",
        issue_id="issue_c2",
        dataset_name="dbo.Employees",
        proposed_action_type="TRIM",
        target_columns=["FirstName"],
        disposition=ResolutionDisposition.CORRECT,
        action_parameters={"side": "left"},
        evidence_authorities=[EvidenceAuthority.DECLARED_SCHEMA],
        evidence_references=["REF_2"],
        confidence=0.9,
        risk_level=RiskLevel.LOW,
        approval_required=False,
    )

    results = orchestrator.validate_batch([p1, p2], profiles)
    assert results["prop_c1"].is_valid is False
    assert results["prop_c2"].is_valid is False
    assert any(f.code == ValidationCode.CONFLICTING_WRITE for f in results["prop_c1"].findings)


# ==============================================================================
# 9. Issue Reconciliation Accounting Invariants
# ==============================================================================

def test_issue_reconciliation_accounting_model():
    tracker = IssueReconciliationTracker()

    tracker.record_issue("issue_01", ResolutionBucket.TIER1_COMPILED)
    tracker.record_issue("issue_02", ResolutionBucket.TIER2_COMPILED)
    tracker.record_issue("issue_03", ResolutionBucket.TIER3_VALIDATION_FAILED)
    tracker.record_issue("issue_04", ResolutionBucket.TIER3_APPROVAL_REQUIRED)
    tracker.record_issue("issue_05", ResolutionBucket.TIER3_APPROVAL_REJECTED)

    # Lifecycle update: issue_06 was APPROVED then COMPILED
    tracker.record_issue("issue_06", ResolutionBucket.TIER3_APPROVED)
    tracker.record_issue("issue_06", ResolutionBucket.TIER3_COMPILED)

    tracker.record_issue("issue_07", ResolutionBucket.MANUAL_REVIEW_REQUIRED)
    tracker.record_issue("issue_08", ResolutionBucket.WAIVED)
    tracker.record_issue("issue_09", ResolutionBucket.UNSUPPORTED)
    tracker.record_issue("issue_10", ResolutionBucket.MODEL_UNAVAILABLE)

    # Invariants
    assert tracker.total_discovered == 10
    assert tracker.get_status("issue_06") == ResolutionBucket.TIER3_COMPILED
    assert tracker.verify_accounting_invariants() is True

    counts = tracker.counts_by_bucket()
    assert sum(counts.values()) == 10
    assert counts[ResolutionBucket.TIER3_COMPILED] == 1
    assert counts[ResolutionBucket.TIER3_APPROVED] == 0  # Replaced by COMPILED lifecycle stage
