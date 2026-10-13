from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.compiler.models.actions import ActionSource, StructuredAction
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
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
from agent.compiler.tier3.cache import Tier3ReplayCache
from agent.compiler.tier3.eligibility import (
    Tier3EligibilityResult,
    get_tier3_model_deployment,
    is_tier3_auto_resolution_enabled,
    is_tier3_cache_enabled,
    is_tier3_llm_enabled,
    is_tier3b_eligible,
)
from agent.compiler.tier3.model_client import (
    OpenAITier3ModelClient,
    Tier3ModelClient,
    Tier3ResolutionRequest,
)
from agent.compiler.tier3.proposal_service import Tier3ProposalService
from agent.compiler.tier3.reconciliation import IssueReconciliationTracker, ResolutionBucket
from agent.compiler.tier3.resolution_service import Tier3ResolutionResult, Tier3ResolutionService
from agent.compiler.tier3.validator import Tier3ValidationOrchestrator

logger = logging.getLogger("agent.compiler.resolution.resolution_orchestrator")


@dataclass
class ResolutionResult:
    """Comprehensive outcome of the multi-tier resolution orchestration."""
    promoted_steps: List[Dict[str, Any]] = field(default_factory=list)
    manual_review: List[Dict[str, Any]] = field(default_factory=list)
    informational_items: List[Dict[str, Any]] = field(default_factory=list)
    unsupported_items: List[Dict[str, Any]] = field(default_factory=list)
    structured_actions: List[StructuredAction] = field(default_factory=list)
    telemetry: Dict[str, Any] = field(default_factory=dict)
    tier3_result: Optional[Tier3ResolutionResult] = None
    ok: bool = True


class ResolutionOrchestrator:
    """
    Dedicated orchestration service responsible for coordinating:
      Canonical issues
      → Tier 1 (Deterministic Catalog)
      → Tier 2 (Deterministic Decomposition)
      → Tier 3A (Known Semantic Normalization)
      → Tier 3B Eligibility Gate (is_tier3b_eligible)
      → Tier 3B Governed LLM Proposal (with Replay Cache & User Feedback Retry)
      → Multi-layer Contract/Schema/Type/Safety/Conflict Validation
      → Policy Approval Gate (Auto-approve safe vs. Require Steward Approval)
      → Manual-Review Construction with Enriched AI Recommendations
      → Structured Action Promotion for Compiler Planning
    """

    def __init__(
        self,
        bridge: Optional[IngestionBridge] = None,
        model_client: Optional[Tier3ModelClient] = None,
        proposal_service: Optional[Tier3ProposalService] = None,
        validator: Optional[Tier3ValidationOrchestrator] = None,
        approval_gate: Optional[Tier3ApprovalGate] = None,
        action_adapter: Optional[Tier3ActionAdapter] = None,
        cache: Optional[Tier3ReplayCache] = None,
    ):
        self.bridge = bridge or IngestionBridge(default_schema="dbo")
        self.cache = cache or Tier3ReplayCache.get_instance()
        self.model_client = model_client or OpenAITier3ModelClient()
        self.proposal_service = proposal_service or Tier3ProposalService(
            client=self.model_client,
            cache=self.cache,
        )
        self.validator = validator or Tier3ValidationOrchestrator()
        self.approval_gate = approval_gate or Tier3ApprovalGate()
        self.action_adapter = action_adapter or Tier3ActionAdapter()

    def resolve(
        self,
        canonical_issues: List[CanonicalIssue],
        dataset_profiles: Dict[str, DatasetProfile],
        business_rules: Optional[List[BusinessRule]] = None,
        policies: Optional[Dict[str, Any]] = None,
        target_engine: ExecutionEngine = ExecutionEngine.AZURE_SQL,
        resolved_manual_review: Optional[List[Dict[str, Any]]] = None,
        user_feedback_by_issue: Optional[Dict[str, str]] = None,
        approval_records: Optional[Any] = None,
    ) -> ResolutionResult:
        """
        Orchestrates multi-tier resolution across all canonical issues.
        Runs Tier 3B LLM resolution on eligible unresolved items BEFORE manual review is finalized.
        """
        user_feedback_by_issue = user_feedback_by_issue or {}
        model_deployment = get_tier3_model_deployment()
        llm_enabled = is_tier3_llm_enabled()
        auto_res_enabled = is_tier3_auto_resolution_enabled()
        cache_active = is_tier3_cache_enabled()

        # Step 1: Reconcile already resolved manual review items
        resolved_mr_ids: Set[str] = set()
        resolved_mr_keys: Set[Tuple[str, Optional[str], str]] = set()
        resolved_mr_cols: Set[Tuple[str, Optional[str]]] = set()
        for mr in (resolved_manual_review or []):
            if isinstance(mr, dict):
                for id_k in ("id", "item_id", "issue_id"):
                    val = mr.get(id_k)
                    if val:
                        resolved_mr_ids.add(str(val))
                for sid in mr.get("source_issue_ids") or []:
                    if sid:
                        resolved_mr_ids.add(str(sid))
                mr_ds = str(mr.get("dataset") or "").strip().lower()
                mr_col = (
                    str(mr.get("column")).strip("[]").lower()
                    if mr.get("column") and not IngestionBridge.is_pseudo_column(mr.get("column"))
                    else None
                )
                mr_it = str(mr.get("issue_type") or "").strip().lower()
                if mr_ds:
                    resolved_mr_keys.add((mr_ds, mr_col, mr_it))
                    resolved_mr_cols.add((mr_ds, mr_col))

        # Extract approval records by proposal ID
        approvals_by_proposal: Dict[str, ApprovalRecord] = {}
        if isinstance(approval_records, dict):
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

        # Convert business rules to context dicts
        rules_context = [
            {
                "rule_id": r.rule_id if hasattr(r, "rule_id") else r.get("rule_id", ""),
                "name": r.name if hasattr(r, "name") else r.get("name", ""),
                "dataset": r.dataset_name if hasattr(r, "dataset_name") else r.get("dataset", ""),
                "column": r.column_name if hasattr(r, "column_name") else r.get("column", ""),
                "description": r.description if hasattr(r, "description") else r.get("description", ""),
            }
            for r in (business_rules or [])
        ]

        # Step 2: Try Tier 1, Tier 2, and Tier 3A deterministic mapping
        detected_issues, deterministic_actions = self.bridge.bridge_to_compiler_models(canonical_issues)

        handled_issue_ids: Set[str] = set()
        for act in deterministic_actions:
            for iid in act.source_issue_ids:
                handled_issue_ids.add(iid)

        # Collect unresolved issues
        unresolved_issues: List[CanonicalIssue] = []
        for c_iss in canonical_issues:
            # Check if covered by already resolved manual review
            is_mr_resolved = (
                c_iss.issue_id in resolved_mr_ids
                or any(mid in resolved_mr_ids for mid in getattr(c_iss, "merged_issue_ids", []))
                or (c_iss.dataset.lower(), (c_iss.column or "").lower() if c_iss.column else None, c_iss.issue_type.lower()) in resolved_mr_keys
                or (c_iss.dataset.lower(), (c_iss.column or "").lower() if c_iss.column else None) in resolved_mr_cols
                or c_iss.resolution_path == "resolved_by_manual_review"
            )
            if is_mr_resolved or c_iss.issue_id in handled_issue_ids:
                continue
            unresolved_issues.append(c_iss)

        # Step 3: Classify unresolved issues using eligibility gate
        tier3_candidates: List[CanonicalIssue] = []
        manual_review_items: List[Dict[str, Any]] = []
        informational_items: List[Dict[str, Any]] = []
        unsupported_items: List[Dict[str, Any]] = []

        unresolved_received = len(unresolved_issues)
        eligible_count = 0
        informational_count = 0
        human_authority_count = 0
        unsupported_primitive_count = 0

        for issue in unresolved_issues:
            eligibility: Tier3EligibilityResult = is_tier3b_eligible(
                issue=issue,
                profiles=dataset_profiles,
                rules=business_rules,
            )

            if eligibility.eligible:
                tier3_candidates.append(issue)
                eligible_count += 1
            elif eligibility.reason_code == "INFORMATIONAL":
                informational_count += 1
                item_info = {
                    "issue_id": issue.issue_id,
                    "dataset": issue.dataset,
                    "column": issue.column,
                    "issue_type": issue.issue_type,
                    "message": issue.message,
                    "reason": "Informational observation",
                    "reason_code": eligibility.reason_code,
                    "is_blocking": False,
                    "status": "INFORMATIONAL",
                }
                informational_items.append(item_info)
                logger.info(
                    "[Tier3Resolution] issue_id=%s eligible=false reason=%s model_called=false",
                    issue.issue_id,
                    eligibility.reason_code,
                )
            elif eligibility.requires_human_authority:
                human_authority_count += 1
                item_info = {
                    "id": f"mr_{issue.dataset}_{issue.column or 'table'}_{issue.issue_type}",
                    "issue_id": issue.issue_id,
                    "dataset": issue.dataset,
                    "column": issue.column,
                    "issue_type": issue.issue_type,
                    "message": issue.message,
                    "reason": issue.resolution_path or "Missing authoritative business semantics",
                    "reason_code": eligibility.reason_code,
                    "is_blocking": True,
                    "status": "MANUAL_REVIEW_REQUIRED",
                    "requires_human_authority": True,
                }
                manual_review_items.append(item_info)
                logger.info(
                    "[Tier3Resolution] issue_id=%s eligible=false reason=%s model_called=false",
                    issue.issue_id,
                    eligibility.reason_code,
                )
            else:
                unsupported_primitive_count += 1
                item_info = {
                    "issue_id": issue.issue_id,
                    "dataset": issue.dataset,
                    "column": issue.column,
                    "issue_type": issue.issue_type,
                    "message": issue.message,
                    "reason": issue.resolution_path or "Unsupported compiler primitive",
                    "reason_code": eligibility.reason_code,
                    "is_blocking": False,
                    "status": "UNSUPPORTED",
                }
                unsupported_items.append(item_info)
                logger.info(
                    "[Tier3Resolution] issue_id=%s eligible=false reason=%s model_called=false",
                    issue.issue_id,
                    eligibility.reason_code,
                )

        # Telemetry: Orchestrator triage distribution
        logger.info(
            "[Tier3Orchestrator] canonical_unresolved=%d eligible=%d informational=%d manual_authority_required=%d unsupported_primitive=%d",
            unresolved_received,
            eligible_count,
            informational_count,
            human_authority_count,
            unsupported_primitive_count,
        )

        # Step 4: Process eligible Tier 3B candidates
        model_called_count = 0
        cache_hit_count = 0
        validated_count = 0
        auto_approved_count = 0
        approval_required_count = 0
        validation_failed_count = 0

        promoted_structured_actions: List[StructuredAction] = []
        approval_pending_proposals: List[Tier3ResolutionProposal] = []

        for c_iss in tier3_candidates:
            if not llm_enabled:
                # LLM disabled by config, graceful fallback to manual review without crash
                mr_item = {
                    "id": f"mr_{c_iss.dataset}_{c_iss.column or 'table'}_{c_iss.issue_type}",
                    "issue_id": c_iss.issue_id,
                    "dataset": c_iss.dataset,
                    "column": c_iss.column,
                    "issue_type": c_iss.issue_type,
                    "message": c_iss.message,
                    "reason": "Tier 3 LLM resolution disabled in environment",
                    "reason_code": "TIER3_LLM_DISABLED",
                    "is_blocking": True,
                    "status": "MANUAL_REVIEW_REQUIRED",
                }
                manual_review_items.append(mr_item)
                logger.info(
                    "[Tier3Resolution] issue_id=%s eligible=true model_called=false reason=llm_disabled",
                    c_iss.issue_id,
                )
                continue

            # Build resolution request
            ds_prof = dataset_profiles.get(c_iss.dataset)
            if not ds_prof:
                for dname, prof in dataset_profiles.items():
                    if dname.lower().strip("[]") == c_iss.dataset.lower().strip("[]"):
                        ds_prof = prof
                        break

            schema_map = (
                {c: prof.dtype for c, prof in ds_prof.columns.items()}
                if ds_prof and ds_prof.columns
                else {}
            )

            # Check if user provided custom feedback/instruction for this issue
            user_feedback = user_feedback_by_issue.get(c_iss.issue_id)
            meta: Dict[str, Any] = {}
            if user_feedback:
                meta["user_feedback"] = user_feedback

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
                metadata=meta,
            )

            # Check cache
            cached_proposal: Optional[Tier3ResolutionProposal] = None
            if cache_active:
                cached_proposal = self.cache.get(req, dataset_profiles)

            req_key = self.cache.compute_request_key(req) if self.cache else "none"
            if cached_proposal is not None:
                cache_hit_count += 1
                prop_res = self.proposal_service.generate_proposal(req, dataset_profiles)
                logger.info(
                    "[Tier3ModelClient] issue_id=%s cache_hit=true model_called=false request_fingerprint=sha256:%s",
                    c_iss.issue_id,
                    req_key[:16],
                )
                logger.info(
                    "[Tier3Resolution] issue_id=%s eligible=true cache_hit=true model_called=false proposal_status=%s",
                    c_iss.issue_id,
                    cached_proposal.status.value if hasattr(cached_proposal.status, "value") else str(cached_proposal.status),
                )
            else:
                model_called_count += 1
                logger.info(
                    "[Tier3ModelClient] issue_id=%s cache_hit=false model_called=true request_fingerprint=sha256:%s",
                    c_iss.issue_id,
                    req_key[:16],
                )
                logger.info(
                    "[Tier3Resolution] issue_id=%s eligible=true cache_hit=false model_called=true model=%s request_fingerprint=%s",
                    c_iss.issue_id,
                    model_deployment,
                    req_key[:16],
                )
                prop_res = self.proposal_service.generate_proposal(req, dataset_profiles)

            # Evaluate proposal generation result
            if not prop_res.success or not prop_res.proposal:
                if prop_res.status in ("MODEL_UNAVAILABLE", "UNAVAILABLE", "TIMEOUT"):
                    manual_review_items.append({
                        "id": f"mr_{c_iss.dataset}_{c_iss.column or 'table'}_{c_iss.issue_type}",
                        "issue_id": c_iss.issue_id,
                        "dataset": c_iss.dataset,
                        "column": c_iss.column,
                        "issue_type": c_iss.issue_type,
                        "message": c_iss.message,
                        "reason": prop_res.error_message or "Tier 3 model service unavailable or timed out",
                        "status": "MODEL_UNAVAILABLE",
                        "is_blocking": True,
                    })
                    logger.info(
                        "[Tier3Resolution] issue_id=%s eligible=true model_called=true final_status=MODEL_UNAVAILABLE",
                        c_iss.issue_id,
                    )
                else:
                    validation_failed_count += 1
                    manual_review_items.append({
                        "id": f"mr_{c_iss.dataset}_{c_iss.column or 'table'}_{c_iss.issue_type}",
                        "issue_id": c_iss.issue_id,
                        "dataset": c_iss.dataset,
                        "column": c_iss.column,
                        "issue_type": c_iss.issue_type,
                        "message": c_iss.message,
                        "reason": prop_res.error_message or "Invalid model proposal syntax or contract",
                        "status": "INVALID_PROPOSAL",
                        "is_blocking": True,
                    })
                    logger.info(
                        "[Tier3Resolution] issue_id=%s eligible=true proposal_received=false final_status=INVALID_PROPOSAL",
                        c_iss.issue_id,
                    )
                continue

            proposal = prop_res.proposal

            # Multi-layer validation (contract, schema, type, safety, conflict)
            val_res = self.validator.validate_proposal(proposal, dataset_profiles)
            conflict_free = not getattr(val_res, "has_conflict_errors", False)
            logger.info(
                "[Tier3Validation] issue_id=%s contract_valid=%s schema_valid=%s type_valid=%s safety_valid=%s conflict_free=%s",
                c_iss.issue_id,
                str(val_res.contract_valid).lower(),
                str(val_res.schema_valid).lower(),
                str(val_res.type_valid).lower(),
                str(val_res.safety_valid).lower(),
                str(conflict_free).lower(),
            )
            if not val_res.is_valid:
                validation_failed_count += 1
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
                manual_review_items.append({
                    "id": f"mr_{c_iss.dataset}_{c_iss.column or 'table'}_{c_iss.issue_type}",
                    "issue_id": c_iss.issue_id,
                    "dataset": c_iss.dataset,
                    "column": c_iss.column,
                    "issue_type": c_iss.issue_type,
                    "message": c_iss.message,
                    "reason": f"Tier 3 validation failed: {[f.message for f in val_res.errors]}",
                    "status": "TIER3_VALIDATION_FAILED",
                    "validation_findings": findings_details,
                    "is_blocking": True,
                })
                logger.info(
                    "[Tier3Resolution] issue_id=%s proposal_received=true contract_valid=%s schema_valid=%s type_valid=%s safety_valid=%s final_status=INVALID",
                    c_iss.issue_id,
                    val_res.contract_valid,
                    val_res.schema_valid,
                    val_res.type_valid,
                    val_res.safety_valid,
                )
                continue

            validated_count += 1
            validated_proposal = proposal.model_copy(update={"status": ResolutionStatus.VALIDATED})

            # Check approval gate
            approval_rec = approvals_by_proposal.get(proposal.proposal_id)
            is_req = self.approval_gate.is_approval_required(validated_proposal)

            if not is_req and auto_res_enabled:
                eval_res = self.approval_gate.evaluate(validated_proposal, approval_record=approval_rec)
                if eval_res.is_approved:
                    approved_proposal = validated_proposal.model_copy(update={"status": ResolutionStatus.APPROVED})
                    s_action = self.action_adapter.adapt(
                        approved_proposal,
                        val_res,
                        approval=eval_res.approval_record,
                    )
                    promoted_structured_actions.append(s_action)
                    auto_approved_count += 1
                    logger.info(
                        "[Tier3Resolution] issue_id=%s proposal_received=true validated=true approval_required=false final_status=AUTO_APPROVED",
                        c_iss.issue_id,
                    )
                    continue

            # Check if pre-existing approval_record approves it
            if approval_rec is not None:
                try:
                    eval_res = self.approval_gate.evaluate(validated_proposal, approval_record=approval_rec)
                    if eval_res.is_approved:
                        approved_proposal = validated_proposal.model_copy(update={"status": ResolutionStatus.APPROVED})
                        s_action = self.action_adapter.adapt(
                            approved_proposal,
                            val_res,
                            approval=eval_res.approval_record,
                        )
                        promoted_structured_actions.append(s_action)
                        auto_approved_count += 1
                        logger.info(
                            "[Tier3Resolution] issue_id=%s proposal_received=true validated=true approval_required=true final_status=APPROVED",
                            c_iss.issue_id,
                        )
                        continue
                except Exception as eval_err:
                    logger.info("Approval record rejected or invalid: %s", eval_err)

            # Requires steward approval
            approval_required_count += 1
            approval_pending_proposals.append(validated_proposal)
            cond_dict = (
                validated_proposal.condition_ast.to_dict()
                if hasattr(validated_proposal.condition_ast, "to_dict")
                else validated_proposal.condition_ast
            )
            mr_entry = {
                "id": f"mr_{c_iss.dataset}_{c_iss.column or 'table'}_{c_iss.issue_type}",
                "issue_id": c_iss.issue_id,
                "dataset": c_iss.dataset,
                "column": c_iss.column,
                "issue_type": c_iss.issue_type,
                "message": c_iss.message,
                "status": "APPROVAL_REQUIRED",
                "is_blocking": True,
                "llm_recommendation": {
                    "proposal_id": validated_proposal.proposal_id,
                    "action_type": validated_proposal.proposed_action_type,
                    "parameters": validated_proposal.action_parameters,
                    "condition_ast": cond_dict,
                    "disposition": validated_proposal.disposition.value,
                    "rationale": validated_proposal.rationale,
                    "confidence": validated_proposal.confidence,
                    "risk_level": validated_proposal.risk_level.value,
                },
                "validation_status": "VALIDATED",
                "approval_required": True,
                "allowed_user_actions": [
                    "APPROVE",
                    "MODIFY",
                    "REJECT",
                    "DESCRIBE_ALTERNATIVE",
                    "WAIVE",
                ],
                "resolution_options": [
                    {
                        "id": f"approve_{validated_proposal.proposed_action_type.lower()}",
                        "action": validated_proposal.proposed_action_type.lower(),
                        "label": f"Approve AI recommendation: {validated_proposal.proposed_action_type}",
                        "description": validated_proposal.rationale or f"Apply {validated_proposal.proposed_action_type} with validated condition",
                        "recommended": True,
                        "is_recommended": True,
                        "proposal_id": validated_proposal.proposal_id,
                    },
                    {
                        "id": "waive",
                        "action": "noop",
                        "label": "Waive issue (accept risk)",
                        "description": "Ignore this finding and continue execution without transformation",
                        "recommended": False,
                        "is_recommended": False,
                    },
                    {
                        "id": "modify",
                        "action": "noop",
                        "label": "Modify parameters or condition",
                        "description": "Adjust action parameters or condition before execution",
                        "recommended": False,
                        "is_recommended": False,
                    },
                ],
            }
            manual_review_items.append(mr_entry)
            logger.info(
                "[Tier3Resolution] issue_id=%s proposal_received=true validated=true approval_required=true final_status=APPROVAL_REQUIRED",
                c_iss.issue_id,
            )

        # Step 5: Summary Telemetry Logging
        logger.info(
            "[Tier3Summary] eligible=%d model_calls=%d cache_hits=%d validated=%d auto_approved=%d approval_required=%d invalid=%d",
            eligible_count,
            model_called_count,
            cache_hit_count,
            validated_count,
            auto_approved_count,
            approval_required_count,
            validation_failed_count,
        )
        logger.info(
            "[Tier3SummaryDetailed] unresolved_received=%d eligible=%d informational=%d human_authority_required=%d unsupported_primitive=%d model_called=%d cache_hit=%d validated=%d auto_approved=%d approval_required=%d validation_failed=%d",
            unresolved_received,
            eligible_count,
            informational_count,
            human_authority_count,
            unsupported_primitive_count,
            model_called_count,
            cache_hit_count,
            validated_count,
            auto_approved_count,
            approval_required_count,
            validation_failed_count,
        )

        telemetry_summary = {
            "unresolved_received": unresolved_received,
            "eligible": eligible_count,
            "informational": informational_count,
            "human_authority_required": human_authority_count,
            "unsupported_primitive": unsupported_primitive_count,
            "model_called": model_called_count,
            "cache_hit": cache_hit_count,
            "validated": validated_count,
            "auto_approved": auto_approved_count,
            "approval_required": approval_required_count,
            "validation_failed": validation_failed_count,
        }

        # Step 6: Convert auto-approved StructuredActions into plan steps
        promoted_steps: List[Dict[str, Any]] = []
        for s_act in promoted_structured_actions:
            cols = s_act.target_columns or []
            primary_col = cols[0] if cols else None
            note_text = s_act.metadata.get("rationale") or s_act.metadata.get("note") or "Auto-approved Tier 3B governed remediation"
            cond_ast = s_act.parameters.get("condition_ast") or s_act.metadata.get("condition_ast")
            act_id_short = str(s_act.action_id or "act")[:8]
            step_dict = {
                "step_id": f"step_{s_act.dataset_name}_{primary_col or 'global'}_{s_act.action_type.value.lower()}_{act_id_short}",
                "dataset": s_act.dataset_name,
                "column": primary_col,
                "action": s_act.action_type.value.lower(),
                "source_issue_type": "tier3_llm_resolution",
                "source_issue_types": ["tier3_llm_resolution"],
                "source_issue_ids": list(s_act.source_issue_ids),
                "source": "TIER3",
                "severity": "medium",
                "priority": 60,
                "note": note_text,
                "params": s_act.parameters,
                "condition_ast": cond_ast,
                "evidence": {
                    "why_this_action": note_text,
                    "confidence": s_act.confidence or 0.95,
                    "source": "TIER3_LLM",
                },
                "message": note_text,
            }
            promoted_steps.append(step_dict)

        all_structured = [*deterministic_actions, *promoted_structured_actions]

        return ResolutionResult(
            promoted_steps=promoted_steps,
            manual_review=manual_review_items,
            informational_items=informational_items,
            unsupported_items=unsupported_items,
            structured_actions=all_structured,
            telemetry=telemetry_summary,
            ok=True,
        )

    def resolve_for_plan(
        self,
        assessment: Dict[str, Any],
        suggestions: List[Dict[str, Any]],
        rules: Dict[str, Any],
        semantic_schema: Optional[Dict[str, Any]] = None,
        target_engine: ExecutionEngine = ExecutionEngine.AZURE_SQL,
        resolved_manual_review: Optional[List[Dict[str, Any]]] = None,
        user_feedback_by_issue: Optional[Dict[str, str]] = None,
        approval_records: Optional[Any] = None,
    ) -> ResolutionResult:
        """
        Convenience lifecycle hook for planner.py.
        Extracts dataset profiles and canonical issues from raw assessment & plan inputs,
        then invokes full resolution orchestration.
        """
        # Build dataset profiles from assessment
        profiles: Dict[str, DatasetProfile] = {}
        for ds_name, ds_info in (assessment.get("datasets") or {}).items():
            cols = {}
            for col_name, c_info in (ds_info.get("columns") or {}).items():
                if isinstance(c_info, dict):
                    dtype = c_info.get("data_type") or c_info.get("type") or "string"
                    cols[col_name] = ColumnProfile(name=col_name, dtype=dtype)
                else:
                    cols[col_name] = ColumnProfile(name=col_name, dtype="string")
            row_count = int(ds_info.get("row_count") or 0)
            profiles[ds_name] = DatasetProfile(dataset_id=ds_name, row_count=row_count, columns=cols)

        # Normalize business rules
        b_rules: List[BusinessRule] = []
        if isinstance(rules, dict):
            for r_item in rules.get("rules") or []:
                if isinstance(r_item, dict):
                    b_rules.append(
                        BusinessRule(
                            rule_id=r_item.get("rule_id") or f"rule_{len(b_rules)}",
                            name=r_item.get("name") or "business_rule",
                            dataset_name=r_item.get("dataset", ""),
                            column_name=r_item.get("column"),
                            description=r_item.get("description", ""),
                        )
                    )

        # Extract and normalize canonical issues
        known_ds = set(profiles.keys())
        mock_plan = {
            "business_rules": rules,
            "datasets": {k: {"steps": []} for k in known_ds},
            "resolved_manual_review": resolved_manual_review or [],
        }
        canonical_issues, _, _ = self.bridge.extract_and_normalize(
            assess=assessment,
            plan=mock_plan,
            known_datasets=known_ds,
            resolved_manual_review=resolved_manual_review,
        )

        return self.resolve(
            canonical_issues=canonical_issues,
            dataset_profiles=profiles,
            business_rules=b_rules,
            policies=rules,
            target_engine=target_engine,
            resolved_manual_review=resolved_manual_review,
            user_feedback_by_issue=user_feedback_by_issue,
            approval_records=approval_records,
        )
