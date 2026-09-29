from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from agent.compiler.models.actions import ActionSource, StructuredAction
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import DatasetProfile
from agent.compiler.models.rules import BusinessRule
from agent.compiler.models.tier3_approval import ApprovalDecision, ApprovalRecord
from agent.compiler.models.tier3_resolution import (
    ResolutionDisposition,
    ResolutionStatus,
    Tier3ResolutionProposal,
)
from agent.compiler.planners.ingestion_bridge import CanonicalIssue, IngestionBridge
from agent.compiler.tier3.action_adapter import Tier3ActionAdapter
from agent.compiler.tier3.approval_gate import Tier3ApprovalGate
from agent.compiler.tier3.model_client import OpenAITier3ModelClient, Tier3ModelClient, Tier3ResolutionRequest
from agent.compiler.tier3.proposal_service import Tier3ProposalService
from agent.compiler.tier3.reconciliation import IssueReconciliationTracker, ResolutionBucket
from agent.compiler.tier3.validator import Tier3ValidationOrchestrator

logger = logging.getLogger("agent.compiler.tier3.resolution_service")


@dataclass
class Tier3ResolutionResult:
    """Consolidated outcome of the Tier 3 proposal, validation, and reconciliation pipeline."""
    compiler_eligible_actions: List[StructuredAction] = field(default_factory=list)
    approval_pending_proposals: List[Tier3ResolutionProposal] = field(default_factory=list)
    manual_review_issues: List[Dict[str, Any]] = field(default_factory=list)
    blocking_manual_review_issues: List[Dict[str, Any]] = field(default_factory=list)
    non_blocking_manual_review_issues: List[Dict[str, Any]] = field(default_factory=list)
    blocking_items: List[Dict[str, Any]] = field(default_factory=list)
    issue_reconciliation: List[Dict[str, Any]] = field(default_factory=list)
    tracker: IssueReconciliationTracker = field(default_factory=IssueReconciliationTracker)
    certification_status: str = "COMPLETE"  # COMPLETE | REVIEW_REQUIRED | BLOCKED
    ok: bool = True
    pending: bool = False
    executable: bool = True
    failure_stage: Optional[str] = None


class Tier3ResolutionService:
    """
    Orchestrates Tier 3 proposal generation, multi-layer validation, approval enforcement,
    and mathematical issue reconciliation between IngestionBridge and CompilerPipeline.
    """

    def __init__(
        self,
        model_client: Optional[Tier3ModelClient] = None,
        proposal_service: Optional[Tier3ProposalService] = None,
        validator: Optional[Tier3ValidationOrchestrator] = None,
        approval_gate: Optional[Tier3ApprovalGate] = None,
        action_adapter: Optional[Tier3ActionAdapter] = None,
    ):
        self.model_client = model_client or OpenAITier3ModelClient()
        self.proposal_service = proposal_service or Tier3ProposalService(client=self.model_client)
        self.validator = validator or Tier3ValidationOrchestrator()
        self.approval_gate = approval_gate or Tier3ApprovalGate()
        self.action_adapter = action_adapter or Tier3ActionAdapter()

    @staticmethod
    def is_blocking_manual_review(
        c_iss: CanonicalIssue,
        dataset_profiles: Optional[Dict[str, DatasetProfile]] = None,
        business_rules: Optional[List[Any]] = None,
    ) -> bool:
        col = (c_iss.column or "").lower()
        itype = (c_iss.issue_type or "").lower()
        msg = (c_iss.message or "").lower()
        res_p = str(c_iss.resolution_path or "").lower()

        # Non-blocking policy constraints
        if "policy" in res_p or "policy_never_drop_rows" in msg or "canonical_schema_constraint" in itype:
            return False

        # Non-blocking observational, binary-like, digits-in-text, or invalid plan steps
        if "binary_like" in res_p or "binary-like" in msg or "only_digits" in itype or "digits_in_text" in itype:
            return False
        if "invalid_plan_step" in res_p or "missing_range_bounds" in res_p or "categorical_range_clip" in res_p or "missing_target" in res_p:
            return False
        if "missing_declared_key" in res_p or "planned_flag_missing_declared_key" in itype:
            return False

        # Check self-referencing / manager hierarchies or informational observation fields
        if "manager" in col or "parent" in col or "changedby" in col or "statushistory" in col:
            return False

        # 1. Metadata-driven Key Semantics
        ds_prof = dataset_profiles.get(c_iss.dataset) if dataset_profiles else None
        from agent.compiler.models.key_semantics import classify_column_semantics, EvidenceAuthority
        key_sem = classify_column_semantics(c_iss.column, dataset_profile=ds_prof, business_rules=business_rules)

        if key_sem.is_authoritative_key:
            if any(m in itype or m in msg for m in ("null", "missing", "orphan", "integrity", "duplicate", "invalid")):
                return True

        # 2. Referential integrity or schema violations that invalidate downstream relational algebra
        if "referential_integrity" in itype or "orphan_fk" in itype:
            # Only block if foreign key relationship is authoritative or declared
            if key_sem.is_authoritative_key or not c_iss.column:
                return True
            return False

        if "schema" in itype and "policy" not in itype and "custom_observation" not in itype and "constraint" not in itype:
            return True

        # 3. Explicit invalid plan semantics on required entities
        if "invalid_plan_semantics_key" in res_p:
            return True

        # Non-blocking statistical / informational observations:
        # e.g., multivariate_outliers, all_caps_values, string_length_outlier, binary_like_column,
        # unusual_distribution, non-key missing-key observations, potential identifiers (ReasonCode, PostalCode)
        return False

    def resolve(
        self,
        canonical_issues: List[CanonicalIssue],
        deterministic_actions: List[StructuredAction],
        dataset_profiles: Dict[str, DatasetProfile],
        business_rules: Optional[List[BusinessRule]] = None,
        target_engine: ExecutionEngine = ExecutionEngine.AZURE_SQL,
        approval_records: Optional[Any] = None,
    ) -> Tier3ResolutionResult:
        tracker = IssueReconciliationTracker()
        compiler_eligible_actions: List[StructuredAction] = []
        approval_pending_proposals: List[Tier3ResolutionProposal] = []
        manual_review_issues: List[Dict[str, Any]] = []
        blocking_manual_review_issues: List[Dict[str, Any]] = []
        non_blocking_manual_review_issues: List[Dict[str, Any]] = []
        blocking_items: List[Dict[str, Any]] = []

        # 1. Reconcile deterministically compiled actions (Tier 1 & Tier 2)
        handled_issue_ids: Set[str] = set()
        for act in deterministic_actions:
            for iss_id in act.source_issue_ids:
                bucket = (
                    ResolutionBucket.TIER1_COMPILED
                    if act.source == ActionSource.CATALOG
                    else ResolutionBucket.TIER2_COMPILED
                )
                tracker.record_issue(iss_id, bucket)
                handled_issue_ids.add(iss_id)

        # 2. Extract any pre-approved records
        approvals_by_proposal: Dict[str, ApprovalRecord] = {}
        if isinstance(approval_records, dict):
            # Check for structured approval objects or dict mapping
            for k, v in approval_records.items():
                if isinstance(v, ApprovalRecord):
                    approvals_by_proposal[v.proposal_id] = v
                elif isinstance(v, dict) and "decision" in v:
                    try:
                        rec = ApprovalRecord.model_validate(v)
                        approvals_by_proposal[rec.proposal_id] = rec
                    except Exception:
                        pass
        elif isinstance(approval_records, list):
            for item in approval_records:
                if isinstance(item, ApprovalRecord):
                    approvals_by_proposal[item.proposal_id] = item

        # Convert business rules into dictionaries for prompt context
        rules_context = [
            {
                "rule_id": r.rule_id,
                "name": r.name,
                "dataset": r.dataset_name,
                "column": r.column_name,
                "description": r.description,
            }
            for r in (business_rules or [])
        ]

        # 3. Process each canonical issue
        for c_iss in canonical_issues:
            if c_iss.issue_id in handled_issue_ids:
                continue

            # Classify manual-review / statistical observation items or invalid plan steps
            if (
                IngestionBridge.is_manual_review_issue(c_iss)
                or (c_iss.resolution_path and "manual_review" in c_iss.resolution_path)
                or c_iss.resolution_path in ("unsupported_missing_target_columns", "unsupported_missing_range_bounds")
            ):
                tracker.record_issue(c_iss.issue_id, ResolutionBucket.MANUAL_REVIEW_REQUIRED)
                is_blocking = self.is_blocking_manual_review(
                    c_iss,
                    dataset_profiles=dataset_profiles,
                    business_rules=business_rules,
                )
                item_info = {
                    "issue_id": c_iss.issue_id,
                    "dataset": c_iss.dataset,
                    "column": c_iss.column,
                    "issue_type": c_iss.issue_type,
                    "message": c_iss.message,
                    "source": c_iss.source,
                    "reason": c_iss.resolution_path or "Manual review required",
                    "action_type": None,
                    "is_blocking": is_blocking,
                }
                manual_review_issues.append(item_info)
                if is_blocking:
                    blocking_manual_review_issues.append(item_info)
                    blocking_items.append(item_info)
                else:
                    non_blocking_manual_review_issues.append(item_info)

                handled_issue_ids.add(c_iss.issue_id)
                continue

            # Build Tier 3 resolution request for unresolved candidate
            ds_prof = dataset_profiles.get(c_iss.dataset)
            schema_map = (
                {c: prof.dtype for c, prof in ds_prof.columns.items()}
                if ds_prof and ds_prof.columns
                else {}
            )

            req = Tier3ResolutionRequest(
                issue_id=c_iss.issue_id,
                issue_type=c_iss.issue_type,
                issue_description=c_iss.message or f"Issue in {c_iss.dataset}.{c_iss.column or 'table'}",
                dataset_name=c_iss.dataset,
                target_columns=[c_iss.column] if c_iss.column else [],
                dataset_schema=schema_map,
                sample_values=[],
                available_rules=rules_context,
                target_engine=target_engine.value if hasattr(target_engine, "value") else str(target_engine),
            )

            # Generate structured proposal
            prop_res = self.proposal_service.generate_proposal(req, dataset_profiles)
            if not prop_res.success or not prop_res.proposal:
                if prop_res.status in ("MODEL_UNAVAILABLE", "UNAVAILABLE", "TIMEOUT"):
                    tracker.record_issue(c_iss.issue_id, ResolutionBucket.MODEL_UNAVAILABLE)
                    blocking_items.append({
                        "issue_id": c_iss.issue_id,
                        "dataset": c_iss.dataset,
                        "column": c_iss.column,
                        "reason": prop_res.error_message or "Tier 3 model unavailable or timed out",
                        "status": "MODEL_UNAVAILABLE",
                    })
                elif prop_res.status == "INVALID_PROPOSAL":
                    tracker.record_issue(c_iss.issue_id, ResolutionBucket.TIER3_VALIDATION_FAILED)
                    blocking_items.append({
                        "issue_id": c_iss.issue_id,
                        "dataset": c_iss.dataset,
                        "column": c_iss.column,
                        "reason": prop_res.error_message or "Invalid model proposal",
                        "status": "INVALID_PROPOSAL",
                    })
                else:
                    tracker.record_issue(c_iss.issue_id, ResolutionBucket.UNSUPPORTED)
                    blocking_items.append({
                        "issue_id": c_iss.issue_id,
                        "dataset": c_iss.dataset,
                        "column": c_iss.column,
                        "reason": prop_res.error_message or "No supported remediation for issue",
                        "status": "UNSUPPORTED",
                    })
                handled_issue_ids.add(c_iss.issue_id)
                continue

            proposal = prop_res.proposal

            # Validate proposal against schema, type, safety, contract
            val_res = self.validator.validate_proposal(proposal, dataset_profiles)
            if not val_res.is_valid:
                tracker.record_issue(c_iss.issue_id, ResolutionBucket.TIER3_VALIDATION_FAILED)
                findings_details = [
                    {
                        "validator": f.validator,
                        "code": f.code.value if hasattr(f.code, "value") else str(f.code),
                        "severity": f.severity.value if hasattr(f.severity, "value") else str(f.severity),
                        "message": f.message,
                        "path": f.ast_path or getattr(f, "path", None) or "target_columns",
                    }
                    for f in val_res.findings
                ]
                blocking_items.append({
                    "issue_id": c_iss.issue_id,
                    "dataset": c_iss.dataset,
                    "column": c_iss.column,
                    "reason": f"Tier 3 validation failed: {[f.message for f in val_res.errors]}",
                    "status": "TIER3_VALIDATION_FAILED",
                    "validation_findings": findings_details,
                })
                handled_issue_ids.add(c_iss.issue_id)
                continue

            validated_proposal = proposal.model_copy(update={"status": ResolutionStatus.VALIDATED})

            # Check approval gate
            approval_rec = approvals_by_proposal.get(proposal.proposal_id)
            try:
                eval_res = self.approval_gate.evaluate(validated_proposal, approval_record=approval_rec)
                if eval_res.is_approved:
                    approved_proposal = validated_proposal.model_copy(update={"status": ResolutionStatus.APPROVED})
                    s_action = self.action_adapter.adapt(
                        approved_proposal,
                        val_res,
                        approval=eval_res.approval_record,
                    )
                    compiler_eligible_actions.append(s_action)
                    tracker.record_issue(c_iss.issue_id, ResolutionBucket.TIER3_COMPILED)
                else:
                    tracker.record_issue(c_iss.issue_id, ResolutionBucket.TIER3_APPROVAL_REQUIRED)
                    approval_pending_proposals.append(validated_proposal)
                    blocking_items.append({
                        "issue_id": c_iss.issue_id,
                        "dataset": c_iss.dataset,
                        "column": c_iss.column,
                        "reason": "Requires steward approval before compilation",
                        "status": "TIER3_APPROVAL_REQUIRED",
                    })
            except Exception as app_err:
                logger.info(f"Tier 3 proposal requires steward approval: {app_err}")
                tracker.record_issue(c_iss.issue_id, ResolutionBucket.TIER3_APPROVAL_REQUIRED)
                approval_pending_proposals.append(validated_proposal)
                blocking_items.append({
                    "issue_id": c_iss.issue_id,
                    "dataset": c_iss.dataset,
                    "column": c_iss.column,
                    "reason": f"Approval gate evaluation pending: {app_err}",
                    "status": "TIER3_APPROVAL_REQUIRED",
                })

            handled_issue_ids.add(c_iss.issue_id)

        # 4. Strict Accounting Verification
        try:
            tracker.verify_accounting_invariants()
        except ValueError as inv_err:
            logger.warning(f"Reconciliation invariant check warning: {inv_err}")

        # 5. Build structured lineage reconciliation entries
        issue_reconciliation: List[Dict[str, Any]] = []
        c_map = {i.issue_id: i for i in canonical_issues}
        for iss_id, bucket in tracker._issue_states.items():
            iss_item = c_map.get(iss_id)
            is_blocking = False
            if bucket == ResolutionBucket.MANUAL_REVIEW_REQUIRED:
                is_blocking = (
                    self.is_blocking_manual_review(
                        iss_item,
                        dataset_profiles=dataset_profiles,
                        business_rules=business_rules,
                    )
                    if iss_item
                    else False
                )
            elif bucket in (
                ResolutionBucket.UNSUPPORTED,
                ResolutionBucket.TIER3_VALIDATION_FAILED,
                ResolutionBucket.TIER3_APPROVAL_REQUIRED,
                ResolutionBucket.MODEL_UNAVAILABLE,
            ):
                if iss_item and (
                    IngestionBridge.is_manual_review_issue(iss_item)
                    or (iss_item.resolution_path and "manual_review" in iss_item.resolution_path)
                    or (iss_item.resolution_path and "invalid_plan" in iss_item.resolution_path)
                    or "policy" in (iss_item.message or "").lower()
                    or (iss_item.column and ("manager" in iss_item.column.lower() or "parent" in iss_item.column.lower()))
                ):
                    is_blocking = False
                else:
                    is_blocking = True

            rec_entry = {
                "issue_id": iss_id,
                "dataset": iss_item.dataset if iss_item else None,
                "column": iss_item.column if iss_item else None,
                "final_status": bucket.value.lower(),
                "resolution_bucket": bucket.value,
                "action_type": iss_item.resolved_action.value if iss_item and iss_item.resolved_action else None,
                "reason": iss_item.resolution_path if iss_item else None,
                "blocks_execution": is_blocking,
            }
            matching_block = next((b for b in blocking_items if b.get("issue_id") == iss_id and "validation_findings" in b), None)
            if matching_block:
                rec_entry["validation_findings"] = matching_block["validation_findings"]

            issue_reconciliation.append(rec_entry)

        pending = len(approval_pending_proposals) > 0
        has_blocking = len(blocking_items) > 0

        if pending:
            certification_status = "BLOCKED"
            executable = False
            ok = False
            failure_stage = "approval_gate"
        elif has_blocking:
            certification_status = "BLOCKED"
            executable = False
            ok = False
            failure_stage = "capability_validation"
        elif non_blocking_manual_review_issues:
            certification_status = "REVIEW_REQUIRED"
            executable = True
            ok = True
            failure_stage = None
        else:
            certification_status = "COMPLETE"
            executable = True
            ok = True
            failure_stage = None

        return Tier3ResolutionResult(
            compiler_eligible_actions=compiler_eligible_actions,
            approval_pending_proposals=approval_pending_proposals,
            manual_review_issues=manual_review_issues,
            blocking_manual_review_issues=blocking_manual_review_issues,
            non_blocking_manual_review_issues=non_blocking_manual_review_issues,
            blocking_items=blocking_items,
            issue_reconciliation=issue_reconciliation,
            tracker=tracker,
            certification_status=certification_status,
            ok=ok,
            pending=pending,
            executable=executable,
            failure_stage=failure_stage,
        )
