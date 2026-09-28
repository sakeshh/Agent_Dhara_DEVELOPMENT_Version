from __future__ import annotations

from typing import Any, Dict, Optional

from agent.compiler.models.actions import ActionSource, StructuredAction
from agent.compiler.models.logical import ActionType
from agent.compiler.models.tier3_approval import ApprovalDecision, ApprovalRecord
from agent.compiler.models.tier3_resolution import (
    ResolutionDisposition,
    ResolutionStatus,
    Tier3ResolutionProposal,
)
from agent.compiler.tier3.exceptions import Tier3IntegrationError
from agent.compiler.tier3.validation_result import Tier3ValidationResult


class Tier3ActionAdapter:
    """
    Dedicated adapter converting validated, approved Tier 3 proposals into StructuredAction contracts.

    Guarantees:
      - Strictly fail-closed: requires full validation and required approval binding.
      - Exact fingerprint matching: rejects any proposal modified after validation or approval.
      - Preserves typed AST in action parameters; never invents or renders raw SQL.
      - Maintains complete lineage and provenance (proposal_id, issue_id, fingerprint, evidence, approval).
      - Rejects unregistered action types.
      - Pure function: does not mutate the proposal or validation result.
    """

    REGISTERED_ACTION_MAP: Dict[str, ActionType] = {
        "TRIM": ActionType.TRIM,
        "NORMALIZE_CASE": ActionType.LOWERCASE,
        "REPLACE_SENTINELS": ActionType.REPLACE_SENTINELS,
        "VALUE_MAP": ActionType.VALUE_MAP,
        "CONDITIONAL_UPDATE": ActionType.CONDITIONAL_UPDATE,
        "VALIDATE_RANGE": ActionType.RANGE_CLIP,
        "VALIDATE_ALLOWED_VALUES": ActionType.VALUE_MAP,
        "VALIDATE_REFERENTIAL_INTEGRITY": ActionType.VALIDATE_REFERENTIAL_INTEGRITY,
        "FLAG_ROW": ActionType.CONDITIONAL_UPDATE,
        "REJECT_ROW": ActionType.CONDITIONAL_UPDATE,
        "DEDUPLICATE": ActionType.DEDUPLICATE,
        "PARSE_DATES": ActionType.PARSE_DATES,
        "CAST_TYPE": ActionType.CAST_TYPE,
        "FILL_NULLS": ActionType.FILL_NULLS,
    }

    def adapt(
        self,
        proposal: Tier3ResolutionProposal,
        validation_result: Tier3ValidationResult,
        approval: Optional[ApprovalRecord] = None,
    ) -> StructuredAction:
        """
        Converts an eligible proposal into a compiler-ready StructuredAction.
        Raises Tier3IntegrationError if any eligibility check fails.
        """
        # 1. Validation Status and Proposal Correspondence
        if not validation_result.is_valid:
            raise Tier3IntegrationError(
                "Only validated Tier 3 proposals can become structured actions."
            )
        if validation_result.proposal_fingerprint != proposal.fingerprint:
            raise Tier3IntegrationError(
                "Validation result does not match the current proposal."
            )

        # 2. Lifecycle Status Eligibility
        if proposal.status not in {
            ResolutionStatus.VALIDATED,
            ResolutionStatus.APPROVED,
        }:
            raise Tier3IntegrationError(
                f"Proposal status {proposal.status} is not compiler-eligible."
            )

        # 3. Approval Gate Requirement
        if proposal.approval_required:
            if proposal.status != ResolutionStatus.APPROVED:
                raise Tier3IntegrationError(
                    "Proposal requires approval before compiler integration."
                )
            if approval is None:
                raise Tier3IntegrationError(
                    "Approved proposal requires a valid ApprovalRecord."
                )
            if approval.decision != ApprovalDecision.APPROVED:
                raise Tier3IntegrationError(
                    f"Approval decision '{approval.decision}' cannot authorize proposal."
                )
            if approval.proposal_fingerprint != proposal.fingerprint:
                raise Tier3IntegrationError(
                    "Approval fingerprint does not match the current proposal."
                )

        # 5. Compiler Action Allowlist Check with Disposition Resolution
        resolved_action_type = self._resolve_compiler_action(
            proposal.proposed_action_type,
            proposal.disposition,
            proposal.action_parameters,
        )

        # 6. Preserve typed AST and parameters (do not create raw SQL)
        validated_parameters: Dict[str, Any] = dict(proposal.action_parameters)
        validated_parameters["disposition"] = proposal.disposition.value

        if proposal.condition_ast is not None:
            if hasattr(proposal.condition_ast, "to_dict"):
                validated_parameters["condition_ast"] = proposal.condition_ast.to_dict()
            elif hasattr(proposal.condition_ast, "model_dump"):
                validated_parameters["condition_ast"] = proposal.condition_ast.model_dump()
            else:
                validated_parameters["condition_ast"] = dict(proposal.condition_ast)

        # 7. Construct Provenance-Preserving StructuredAction
        return StructuredAction(
            action_id=f"action_{proposal.proposal_id}",
            action_type=resolved_action_type,
            dataset_name=proposal.dataset_name,
            target_columns=list(proposal.target_columns),
            parameters=validated_parameters,
            source=ActionSource.TIER3,
            source_issue_ids=[proposal.issue_id],
            requires_approval=proposal.approval_required,
            confidence=proposal.confidence,
            metadata={
                "tier3_proposal_id": proposal.proposal_id,
                "tier3_fingerprint": proposal.fingerprint,
                "evidence_authorities": [
                    authority.value if hasattr(authority, "value") else str(authority)
                    for authority in proposal.evidence_authorities
                ],
                "evidence_references": list(proposal.evidence_references),
                "approval_id": approval.approval_id if approval else None,
                "approver_id": approval.approver_id if approval else None,
                "contract_version": proposal.contract_version,
                "disposition": proposal.disposition.value,
                "risk_level": proposal.risk_level.value,
            },
        )

    def _resolve_compiler_action(
        self,
        action_str: str,
        disposition: ResolutionDisposition,
        params: Dict[str, Any],
    ) -> ActionType:
        act_key = action_str.upper().strip()
        if act_key not in self.REGISTERED_ACTION_MAP:
            raise Tier3IntegrationError(
                f"Action '{action_str}' is not a registered compiler action."
            )

        if act_key == "VALIDATE_RANGE":
            if disposition in (
                ResolutionDisposition.FLAG,
                ResolutionDisposition.REJECT,
                ResolutionDisposition.QUARANTINE,
                ResolutionDisposition.NULLIFY,
            ):
                return ActionType.CONDITIONAL_UPDATE
            return ActionType.RANGE_CLIP

        if act_key == "NORMALIZE_CASE":
            target_case = str(params.get("case", "lower")).lower()
            return ActionType.UPPERCASE if target_case == "upper" else ActionType.LOWERCASE

        return self.REGISTERED_ACTION_MAP[act_key]

