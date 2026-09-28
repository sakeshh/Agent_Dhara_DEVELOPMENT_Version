from __future__ import annotations

from datetime import datetime, timezone
import pytest

from agent.compiler.models.actions import ActionSource
from agent.compiler.models.logical import ActionType
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
from agent.compiler.tier3.exceptions import Tier3IntegrationError
from agent.compiler.tier3.validation_result import Tier3ValidationResult


@pytest.fixture
def adapter() -> Tier3ActionAdapter:
    return Tier3ActionAdapter()


@pytest.fixture
def valid_trim_proposal() -> Tier3ResolutionProposal:
    return Tier3ResolutionProposal(
        contract_version="1.0",
        proposal_id="prop_trim_100",
        issue_id="issue_whitespace_100",
        dataset_name="dbo.Employees",
        proposed_action_type="TRIM",
        target_columns=["FirstName"],
        disposition=ResolutionDisposition.CORRECT,
        action_parameters={"side": "both"},
        evidence_authorities=[EvidenceAuthority.DECLARED_SCHEMA],
        evidence_references=["SCHEMA_RULE_100"],
        confidence=1.0,
        risk_level=RiskLevel.LOW,
        approval_required=False,
        status=ResolutionStatus.VALIDATED,
    )


@pytest.fixture
def valid_ast_proposal() -> Tier3ResolutionProposal:
    cond = ComparisonExpression(
        operator=ComparisonOperator.LT,
        left=ColumnRef(dataset="", column="Salary"),
        right=LiteralValue(data_type="INTEGER", value=0),
    )
    return Tier3ResolutionProposal(
        contract_version="1.0",
        proposal_id="prop_cond_200",
        issue_id="issue_negative_salary_200",
        dataset_name="dbo.Employees",
        proposed_action_type="CONDITIONAL_UPDATE",
        target_columns=["Salary"],
        disposition=ResolutionDisposition.NULLIFY,
        condition_ast=cond.to_dict(),
        action_parameters={"new_value": None},
        evidence_authorities=[EvidenceAuthority.APPROVED_BUSINESS_RULE],
        evidence_references=["RULE_SALARY_POSITIVE"],
        confidence=0.95,
        risk_level=RiskLevel.MEDIUM,
        approval_required=True,
        status=ResolutionStatus.APPROVED,
    )


def test_valid_low_risk_proposal_conversion(
    adapter: Tier3ActionAdapter,
    valid_trim_proposal: Tier3ResolutionProposal,
):
    """A validated low-risk proposal converts into a StructuredAction with complete provenance."""
    val_res = Tier3ValidationResult(
        valid=True,
        findings=[],
        normalized_proposal=valid_trim_proposal,
        proposal_fingerprint=valid_trim_proposal.fingerprint,
    )

    action = adapter.adapt(valid_trim_proposal, val_res)

    assert action.source == ActionSource.TIER3
    assert action.source_issue_ids == [valid_trim_proposal.issue_id]
    assert action.action_type == ActionType.TRIM
    assert action.dataset_name == "dbo.Employees"
    assert action.target_columns == ["FirstName"]
    assert action.parameters["side"] == "both"
    assert action.parameters["disposition"] == ResolutionDisposition.CORRECT.value
    assert action.metadata["tier3_proposal_id"] == valid_trim_proposal.proposal_id
    assert action.metadata["tier3_fingerprint"] == valid_trim_proposal.fingerprint
    assert action.metadata["contract_version"] == "1.0"


def test_invalid_proposal_is_blocked(
    adapter: Tier3ActionAdapter,
    valid_trim_proposal: Tier3ResolutionProposal,
):
    """A proposal with validation errors must not create an action."""
    val_res = Tier3ValidationResult(
        valid=False,
        findings=[],
        normalized_proposal=None,
        proposal_fingerprint=valid_trim_proposal.fingerprint,
    )
    with pytest.raises(Tier3IntegrationError, match="Only validated Tier 3 proposals"):
        adapter.adapt(valid_trim_proposal, val_res)


def test_approval_required_proposal_blocked_without_approved_status(
    adapter: Tier3ActionAdapter,
    valid_ast_proposal: Tier3ResolutionProposal,
):
    """An approval-required proposal with status VALIDATED must not enter planner."""
    unapproved_proposal = valid_ast_proposal.model_copy(update={"status": ResolutionStatus.VALIDATED})
    val_res = Tier3ValidationResult(
        valid=True,
        findings=[],
        normalized_proposal=unapproved_proposal,
        proposal_fingerprint=unapproved_proposal.fingerprint,
    )
    with pytest.raises(Tier3IntegrationError, match="requires approval before compiler integration"):
        adapter.adapt(unapproved_proposal, val_res)


def test_approved_proposal_accepted_with_matching_approval(
    adapter: Tier3ActionAdapter,
    valid_ast_proposal: Tier3ResolutionProposal,
):
    """An approved proposal is accepted when approval record matches exact fingerprint."""
    val_res = Tier3ValidationResult(
        valid=True,
        findings=[],
        normalized_proposal=valid_ast_proposal,
        proposal_fingerprint=valid_ast_proposal.fingerprint,
    )
    approval = ApprovalRecord(
        approval_id="appr_cond_200",
        proposal_id=valid_ast_proposal.proposal_id,
        proposal_fingerprint=valid_ast_proposal.fingerprint,
        decision=ApprovalDecision.APPROVED,
        approver_id="lead_carol",
        approver_role="DATA_STEWARD",
        decided_at=datetime.now(timezone.utc),
        reason="Verified business rule",
    )

    action = adapter.adapt(valid_ast_proposal, val_res, approval=approval)

    assert action.source == ActionSource.TIER3
    assert action.action_type == ActionType.CONDITIONAL_UPDATE
    assert action.metadata["approval_id"] == "appr_cond_200"
    assert action.metadata["approver_id"] == "lead_carol"


def test_stale_approval_fingerprint_mismatch_rejected(
    adapter: Tier3ActionAdapter,
    valid_ast_proposal: Tier3ResolutionProposal,
):
    """Modifying the AST after approval changes the fingerprint and rejects adapter conversion."""
    old_fingerprint = valid_ast_proposal.fingerprint
    approval = ApprovalRecord(
        approval_id="appr_old",
        proposal_id=valid_ast_proposal.proposal_id,
        proposal_fingerprint=old_fingerprint,
        decision=ApprovalDecision.APPROVED,
        approver_id="lead_carol",
        approver_role="DATA_STEWARD",
        decided_at=datetime.now(timezone.utc),
        reason="Approved initial AST",
    )

    # Modify AST (e.g. change threshold from 0 to 100)
    new_cond = ComparisonExpression(
        operator=ComparisonOperator.LT,
        left=ColumnRef(dataset="", column="Salary"),
        right=LiteralValue(data_type="INTEGER", value=100),
    )
    modified_proposal = valid_ast_proposal.model_copy(update={"condition_ast": new_cond.to_dict()})
    assert modified_proposal.fingerprint != old_fingerprint

    val_res = Tier3ValidationResult(
        valid=True,
        findings=[],
        normalized_proposal=modified_proposal,
        proposal_fingerprint=modified_proposal.fingerprint,
    )

    with pytest.raises(Tier3IntegrationError, match="Approval fingerprint does not match"):
        adapter.adapt(modified_proposal, val_res, approval=approval)


def test_ast_remains_structured_without_raw_sql(
    adapter: Tier3ActionAdapter,
    valid_ast_proposal: Tier3ResolutionProposal,
):
    """The adapter preserves condition_ast dict and never generates raw SQL strings."""
    val_res = Tier3ValidationResult(
        valid=True,
        findings=[],
        normalized_proposal=valid_ast_proposal,
        proposal_fingerprint=valid_ast_proposal.fingerprint,
    )
    approval = ApprovalRecord(
        approval_id="appr_ast_check",
        proposal_id=valid_ast_proposal.proposal_id,
        proposal_fingerprint=valid_ast_proposal.fingerprint,
        decision=ApprovalDecision.APPROVED,
        approver_id="lead_carol",
        approver_role="DATA_STEWARD",
        decided_at=datetime.now(timezone.utc),
        reason="Valid",
    )

    action = adapter.adapt(valid_ast_proposal, val_res, approval=approval)

    assert "condition_ast" in action.parameters
    assert isinstance(action.parameters["condition_ast"], dict)
    assert action.parameters["condition_ast"]["node_type"] == AstNodeType.COMPARISON.value
    # Crucial: Adapter must NOT inject raw SQL condition
    assert "condition" not in action.parameters or action.parameters.get("condition") is None


def test_unknown_action_type_is_blocked(adapter: Tier3ActionAdapter):
    """Even if an unknown action somehow bypassed validation, the adapter rejects it."""
    proposal = Tier3ResolutionProposal(
        contract_version="1.0",
        proposal_id="prop_unknown_01",
        issue_id="issue_unknown_01",
        dataset_name="dbo.Employees",
        proposed_action_type="DEDUPLICATE",
        target_columns=["EmployeeID"],
        disposition=ResolutionDisposition.CORRECT,
        evidence_authorities=[EvidenceAuthority.DECLARED_SCHEMA],
        evidence_references=["REF_01"],
        confidence=1.0,
        risk_level=RiskLevel.LOW,
        approval_required=False,
        status=ResolutionStatus.VALIDATED,
    )
    # Bypass proposed_action_type directly on instance
    object.__setattr__(proposal, "proposed_action_type", "EXEC_CUSTOM_PROCEDURE")

    val_res = Tier3ValidationResult(
        valid=True,
        findings=[],
        normalized_proposal=proposal,
        proposal_fingerprint=proposal.fingerprint,
    )
    with pytest.raises(Tier3IntegrationError, match="not a registered compiler action"):
        adapter.adapt(proposal, val_res)
