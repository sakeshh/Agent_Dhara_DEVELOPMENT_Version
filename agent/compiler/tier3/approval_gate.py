from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Set

from agent.compiler.models.tier3_approval import ApprovalDecision, ApprovalRecord
from agent.compiler.models.tier3_resolution import (
    EvidenceAuthority,
    ResolutionDisposition,
    ResolutionStatus,
    RiskLevel,
    Tier3ResolutionProposal,
)
from agent.compiler.tier3.exceptions import Tier3ApprovalError


@dataclass(frozen=True)
class ApprovalEvaluationResult:
    is_approved: bool
    requires_approval: bool
    decision: ApprovalDecision
    approval_record: Optional[ApprovalRecord] = None
    reason: Optional[str] = None


class Tier3ApprovalGate:
    """
    Evaluates policy eligibility and verifies cryptographic binding between
    human steward approvals and immutable Tier 3 proposal fingerprints.

    Guarantees:
      - Model confidence (even 1.0) NEVER overrides approval policy.
      - Approved fingerprint MUST strictly match current proposal fingerprint.
      - Stale proposals (AST, columns, schema, disposition modified) are blocked.
      - Automatic approval is strictly restricted to low-risk, authoritatively-backed canonical actions.
      - Only authorized roles can grant approvals.
    """

    AUTHORIZED_ROLES: Set[str] = {
        "DATA_STEWARD",
        "COMPILER_ADMIN",
        "DATA_OWNER",
        "LEAD_ENGINEER",
        "STEWARD",
    }

    AUTHORITATIVE_EVIDENCE: Set[EvidenceAuthority] = {
        EvidenceAuthority.DECLARED_SCHEMA,
        EvidenceAuthority.APPROVED_BUSINESS_RULE,
        EvidenceAuthority.APPROVED_CATALOG,
    }

    AUTO_APPROVABLE_ACTIONS: Set[str] = {
        "TRIM",
        "NORMALIZE_CASE",
        "FLAG_ROW",
    }

    DESTRUCTIVE_OR_HIGH_RISK_DISPOSITIONS: Set[ResolutionDisposition] = {
        ResolutionDisposition.REJECT,
        ResolutionDisposition.QUARANTINE,
        ResolutionDisposition.NULLIFY,
    }

    def __init__(self, authorized_roles: Optional[Set[str]] = None):
        self.authorized_roles = set(authorized_roles or self.AUTHORIZED_ROLES)
        self._recorded_approvals: Dict[str, ApprovalRecord] = {}

    def is_approval_required(self, proposal: Tier3ResolutionProposal) -> bool:
        """
        Determines whether a proposal strictly requires approval based on policy invariants.
        Model confidence is deliberately ignored.
        """
        # Explicit contract requirement
        if proposal.approval_required:
            return True

        # High-risk or prohibited transformations
        if proposal.risk_level in (RiskLevel.HIGH, RiskLevel.PROHIBITED):
            return True

        # Destructive dispositions
        if proposal.disposition in self.DESTRUCTIVE_OR_HIGH_RISK_DISPOSITIONS:
            return True

        # Non-canonical actions require semantic review
        act = proposal.proposed_action_type.upper().strip()
        if act not in self.AUTO_APPROVABLE_ACTIONS:
            return True

        # Requires authoritative backing
        has_authoritative = any(a in self.AUTHORITATIVE_EVIDENCE for a in proposal.evidence_authorities)
        if not has_authoritative:
            return True

        return False

    def evaluate(
        self,
        proposal: Tier3ResolutionProposal,
        approval_record: Optional[ApprovalRecord] = None,
        current_schema_fingerprint: Optional[str] = None,
    ) -> ApprovalEvaluationResult:
        """
        Evaluates whether the proposal is authorized to enter compiler planning.
        Raises Tier3ApprovalError if required approval is missing, invalid, stale, or unauthorized.
        """
        requires_approval = self.is_approval_required(proposal)

        if not requires_approval:
            # Low-risk canonical action backed by authoritative evidence: auto-approved
            return ApprovalEvaluationResult(
                is_approved=True,
                requires_approval=False,
                decision=ApprovalDecision.APPROVED,
                approval_record=None,
                reason="Auto-approved by policy: low-risk canonical action with authoritative evidence.",
            )

        if approval_record is None:
            raise Tier3ApprovalError(
                f"Proposal '{proposal.proposal_id}' requires approval before compiler integration."
            )

        # 1. Proposal ID alignment
        if approval_record.proposal_id != proposal.proposal_id:
            raise Tier3ApprovalError(
                f"Approval record proposal_id '{approval_record.proposal_id}' does not match proposal '{proposal.proposal_id}'."
            )

        # 2. Exact Fingerprint Binding
        current_fingerprint = proposal.fingerprint
        if approval_record.proposal_fingerprint != current_fingerprint:
            raise Tier3ApprovalError(
                f"Stale approval: Approved fingerprint '{approval_record.proposal_fingerprint}' "
                f"does not match current proposal fingerprint '{current_fingerprint}'."
            )

        # 3. Decision Status
        if approval_record.decision == ApprovalDecision.EXPIRED:
            raise Tier3ApprovalError(
                f"Approval record '{approval_record.approval_id}' has expired."
            )
        if approval_record.decision == ApprovalDecision.REJECTED:
            raise Tier3ApprovalError(
                f"Approval for proposal '{proposal.proposal_id}' was rejected: {approval_record.reason}"
            )
        if approval_record.decision == ApprovalDecision.CHANGES_REQUESTED:
            raise Tier3ApprovalError(
                f"Changes requested for proposal '{proposal.proposal_id}': {approval_record.reason}"
            )
        if approval_record.is_revoked or approval_record.decision == ApprovalDecision.REVOKED:
            raise Tier3ApprovalError(
                f"Approval '{approval_record.approval_id}' has been revoked."
            )
        if approval_record.decision != ApprovalDecision.APPROVED:
            raise Tier3ApprovalError(
                f"Invalid approval decision '{approval_record.decision}'."
            )

        # 4. Schema Fingerprint Alignment (detects drift after approval)
        if approval_record.schema_fingerprint and current_schema_fingerprint:
            if approval_record.schema_fingerprint != current_schema_fingerprint:
                raise Tier3ApprovalError(
                    f"Schema fingerprint drifted after approval was granted. "
                    f"Approved schema '{approval_record.schema_fingerprint}', current '{current_schema_fingerprint}'."
                )

        # 5. Role Authorization
        role = approval_record.approver_role.upper().strip()
        if role not in self.authorized_roles:
            raise Tier3ApprovalError(
                f"Approver '{approval_record.approver_id}' has unauthorized role '{approval_record.approver_role}'. "
                f"Allowed roles: {sorted(list(self.authorized_roles))}."
            )

        # 6. Audit Justification
        if not approval_record.reason or not approval_record.reason.strip():
            raise Tier3ApprovalError("Approval record requires a non-empty justification reason.")

        return ApprovalEvaluationResult(
            is_approved=True,
            requires_approval=True,
            decision=ApprovalDecision.APPROVED,
            approval_record=approval_record,
            reason=approval_record.reason,
        )

    def record_decision(self, record: ApprovalRecord) -> None:
        """Stores a steward approval decision, checking for duplicates or conflicts."""
        existing = self._recorded_approvals.get(record.approval_id)
        if existing and existing.decision != record.decision:
            raise Tier3ApprovalError(
                f"Duplicate approval record with conflicting decision: '{record.approval_id}'."
            )
        self._recorded_approvals[record.approval_id] = record

    def revoke_approval(self, approval_id: str, reason: str) -> ApprovalRecord:
        """Revokes an existing approval prior to compilation."""
        existing = self._recorded_approvals.get(approval_id)
        if not existing:
            raise Tier3ApprovalError(f"Approval '{approval_id}' not found for revocation.")
        
        revoked = ApprovalRecord(
            approval_id=existing.approval_id,
            proposal_id=existing.proposal_id,
            proposal_fingerprint=existing.proposal_fingerprint,
            decision=ApprovalDecision.REVOKED,
            approver_id=existing.approver_id,
            approver_role=existing.approver_role,
            decided_at=datetime.now(timezone.utc),
            reason=f"REVOKED: {reason}",
            policy_version=existing.policy_version,
            schema_fingerprint=existing.schema_fingerprint,
            rule_version=existing.rule_version,
            target_engine=existing.target_engine,
            is_revoked=True,
            metadata=existing.metadata,
        )
        self._recorded_approvals[approval_id] = revoked
        return revoked
