from __future__ import annotations

from datetime import datetime, timezone
import pytest

from agent.compiler.models.tier3_approval import ApprovalDecision, ApprovalRecord
from agent.compiler.models.tier3_resolution import (
    EvidenceAuthority,
    ResolutionDisposition,
    ResolutionStatus,
    RiskLevel,
    Tier3ResolutionProposal,
)
from agent.compiler.tier3.approval_gate import Tier3ApprovalGate
from agent.compiler.tier3.exceptions import Tier3ApprovalError


@pytest.fixture
def approval_gate() -> Tier3ApprovalGate:
    return Tier3ApprovalGate()


@pytest.fixture
def low_risk_proposal() -> Tier3ResolutionProposal:
    return Tier3ResolutionProposal(
        contract_version="1.0",
        proposal_id="prop_trim_001",
        issue_id="issue_whitespace_01",
        dataset_name="dbo.Employees",
        proposed_action_type="TRIM",
        target_columns=["FirstName"],
        disposition=ResolutionDisposition.CORRECT,
        action_parameters={"side": "both"},
        evidence_authorities=[EvidenceAuthority.DECLARED_SCHEMA],
        evidence_references=["SCHEMA_RULE_01"],
        confidence=0.99,
        risk_level=RiskLevel.LOW,
        approval_required=False,
        rationale="Whitespace removal supported by declared schema",
        status=ResolutionStatus.VALIDATED,
    )


@pytest.fixture
def high_risk_proposal() -> Tier3ResolutionProposal:
    return Tier3ResolutionProposal(
        contract_version="1.0",
        proposal_id="prop_quarantine_001",
        issue_id="issue_invalid_salary_01",
        dataset_name="dbo.Employees",
        proposed_action_type="VALIDATE_RANGE",
        target_columns=["Salary"],
        disposition=ResolutionDisposition.QUARANTINE,
        action_parameters={"min_value": 0.0},
        evidence_authorities=[EvidenceAuthority.MODEL_INFERENCE],
        evidence_references=["LLM_HYPOTHESIS_01"],
        confidence=0.99,
        risk_level=RiskLevel.HIGH,
        approval_required=True,
        rationale="Negative salary quarantined based on model inference",
        status=ResolutionStatus.VALIDATED,
    )


def test_automatic_approval_for_low_risk_canonical_action(
    approval_gate: Tier3ApprovalGate,
    low_risk_proposal: Tier3ResolutionProposal,
):
    """Low-risk canonical action with authoritative evidence auto-approves."""
    assert not approval_gate.is_approval_required(low_risk_proposal)
    res = approval_gate.evaluate(low_risk_proposal)
    assert res.is_approved is True
    assert res.requires_approval is False
    assert res.decision == ApprovalDecision.APPROVED


def test_high_confidence_never_overrides_approval_requirement(
    approval_gate: Tier3ApprovalGate,
    high_risk_proposal: Tier3ResolutionProposal,
):
    """A model confidence of 0.99 or 1.0 must never bypass human approval for destructive / high-risk actions."""
    assert high_risk_proposal.confidence == 0.99
    assert approval_gate.is_approval_required(high_risk_proposal) is True

    # Without approval record, evaluation MUST fail
    with pytest.raises(Tier3ApprovalError, match="requires approval"):
        approval_gate.evaluate(high_risk_proposal, approval_record=None)


def test_valid_human_approval_accepted(
    approval_gate: Tier3ApprovalGate,
    high_risk_proposal: Tier3ResolutionProposal,
):
    """Valid approval matching fingerprint and authorized role succeeds."""
    record = ApprovalRecord(
        approval_id="appr_001",
        proposal_id=high_risk_proposal.proposal_id,
        proposal_fingerprint=high_risk_proposal.fingerprint,
        decision=ApprovalDecision.APPROVED,
        approver_id="steward_alice",
        approver_role="DATA_STEWARD",
        decided_at=datetime.now(timezone.utc),
        reason="Verified business requirement that salaries must be non-negative.",
    )
    res = approval_gate.evaluate(high_risk_proposal, approval_record=record)
    assert res.is_approved is True
    assert res.requires_approval is True
    assert res.decision == ApprovalDecision.APPROVED
    assert res.approval_record == record


def test_stale_approval_rejected_on_proposal_modification(
    approval_gate: Tier3ApprovalGate,
    high_risk_proposal: Tier3ResolutionProposal,
):
    """If the proposal fingerprint changes after approval was granted, approval is rejected as stale."""
    record = ApprovalRecord(
        approval_id="appr_001",
        proposal_id=high_risk_proposal.proposal_id,
        proposal_fingerprint=high_risk_proposal.fingerprint,
        decision=ApprovalDecision.APPROVED,
        approver_id="steward_alice",
        approver_role="DATA_STEWARD",
        decided_at=datetime.now(timezone.utc),
        reason="Approved quarantine for Salary >= 0.0",
    )

    # Modify proposal target column after approval
    modified_proposal = high_risk_proposal.model_copy(update={"target_columns": ["Bonus"]})
    assert modified_proposal.fingerprint != high_risk_proposal.fingerprint

    with pytest.raises(Tier3ApprovalError, match="Stale approval"):
        approval_gate.evaluate(modified_proposal, approval_record=record)


def test_schema_fingerprint_drift_rejected(
    approval_gate: Tier3ApprovalGate,
    high_risk_proposal: Tier3ResolutionProposal,
):
    """If the underlying schema fingerprint changes after approval was granted, drift is detected."""
    record = ApprovalRecord(
        approval_id="appr_001",
        proposal_id=high_risk_proposal.proposal_id,
        proposal_fingerprint=high_risk_proposal.fingerprint,
        decision=ApprovalDecision.APPROVED,
        approver_id="steward_alice",
        approver_role="DATA_STEWARD",
        decided_at=datetime.now(timezone.utc),
        reason="Approved against schema v1",
        schema_fingerprint="schema_hash_v1",
    )

    with pytest.raises(Tier3ApprovalError, match="Schema fingerprint drifted"):
        approval_gate.evaluate(
            high_risk_proposal,
            approval_record=record,
            current_schema_fingerprint="schema_hash_v2",
        )


def test_unauthorized_approver_role_blocked(
    approval_gate: Tier3ApprovalGate,
    high_risk_proposal: Tier3ResolutionProposal,
):
    """An unauthorized approver role (e.g. INTERN) cannot grant approval."""
    record = ApprovalRecord(
        approval_id="appr_002",
        proposal_id=high_risk_proposal.proposal_id,
        proposal_fingerprint=high_risk_proposal.fingerprint,
        decision=ApprovalDecision.APPROVED,
        approver_id="bob_intern",
        approver_role="INTERN",
        decided_at=datetime.now(timezone.utc),
        reason="Looks good to me",
    )
    with pytest.raises(Tier3ApprovalError, match="unauthorized role"):
        approval_gate.evaluate(high_risk_proposal, approval_record=record)


def test_rejected_and_expired_decisions_blocked(
    approval_gate: Tier3ApprovalGate,
    high_risk_proposal: Tier3ResolutionProposal,
):
    """Decisions of REJECTED or EXPIRED raise Tier3ApprovalError."""
    rejected_record = ApprovalRecord(
        approval_id="appr_003",
        proposal_id=high_risk_proposal.proposal_id,
        proposal_fingerprint=high_risk_proposal.fingerprint,
        decision=ApprovalDecision.REJECTED,
        approver_id="steward_alice",
        approver_role="DATA_STEWARD",
        decided_at=datetime.now(timezone.utc),
        reason="Quarantine would break payroll downstream",
    )
    with pytest.raises(Tier3ApprovalError, match="was rejected"):
        approval_gate.evaluate(high_risk_proposal, approval_record=rejected_record)

    expired_record = ApprovalRecord(
        approval_id="appr_004",
        proposal_id=high_risk_proposal.proposal_id,
        proposal_fingerprint=high_risk_proposal.fingerprint,
        decision=ApprovalDecision.EXPIRED,
        approver_id="system",
        approver_role="COMPILER_ADMIN",
        decided_at=datetime.now(timezone.utc),
        reason="Time-to-compile exceeded 48h SLA",
    )
    with pytest.raises(Tier3ApprovalError, match="has expired"):
        approval_gate.evaluate(high_risk_proposal, approval_record=expired_record)


def test_approval_revocation_before_compilation(
    approval_gate: Tier3ApprovalGate,
    high_risk_proposal: Tier3ResolutionProposal,
):
    """An approved record can be revoked prior to compilation."""
    record = ApprovalRecord(
        approval_id="appr_005",
        proposal_id=high_risk_proposal.proposal_id,
        proposal_fingerprint=high_risk_proposal.fingerprint,
        decision=ApprovalDecision.APPROVED,
        approver_id="steward_alice",
        approver_role="DATA_STEWARD",
        decided_at=datetime.now(timezone.utc),
        reason="Initial sign-off",
    )
    approval_gate.record_decision(record)

    # Revoke approval
    revoked_record = approval_gate.revoke_approval("appr_005", reason="Steward discovered payroll discrepancy")
    assert revoked_record.is_revoked is True
    assert revoked_record.decision == ApprovalDecision.REVOKED

    with pytest.raises(Tier3ApprovalError, match="has been revoked"):
        approval_gate.evaluate(high_risk_proposal, approval_record=revoked_record)
