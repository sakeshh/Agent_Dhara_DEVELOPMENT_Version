"""
assessment_planning_bridge.py — Evidence-Planning Reconciliation Bridge (Phase 1).

Enforces bidirectional reconciliation between assessment evidence and ETL plan steps:
1. Every planning-relevant assessment evidence item receives an explicit disposition.
2. Every generated plan step traces back to an evidence record or canonical rule.
3. Duplicate consolidation graphs are validated as acyclic directed trees.
4. No planning-relevant issue can disappear silently between assessment and planning.
"""
from __future__ import annotations

from enum import Enum
import logging
from typing import Any, Dict, List, Optional, Set, Tuple
from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger("agent.etl_pipeline.assessment_planning_bridge")


class EvidencePlanningRelevance(str, Enum):
    PLANNING_RELEVANT = "planning_relevant"
    INFORMATIONAL = "informational"
    ASSESSMENT_DIAGNOSTIC = "assessment_diagnostic"


class AssessmentPlanningDisposition(str, Enum):
    PLAN_STEP_CREATED = "plan_step_created"
    VALIDATION_ONLY = "validation_only"
    NO_REMEDIATION_REQUESTED = "no_remediation_requested"
    POLICY_BLOCKED = "policy_blocked"
    UNSUPPORTED_ACTION = "unsupported_action"
    MANUAL_REVIEW_REQUIRED = "manual_review_required"
    DUPLICATE_CONSOLIDATED = "duplicate_consolidated"
    OUT_OF_SCOPE = "out_of_scope"


class EvidencePlanningRecord(BaseModel):
    evidence_id: str
    issue_type: str
    disposition: AssessmentPlanningDisposition
    supporting_rule_ids: Tuple[str, ...] = ()
    generated_step_ids: Tuple[str, ...] = ()
    review_item_id: Optional[str] = None
    advisory_proposal_id: Optional[str] = None
    policy_decision_id: Optional[str] = None
    canonical_surviving_evidence_id: Optional[str] = None
    rationale: Optional[str] = None
    relevance: EvidencePlanningRelevance = EvidencePlanningRelevance.PLANNING_RELEVANT

    @model_validator(mode="after")
    def validate_disposition_contracts(self) -> "EvidencePlanningRecord":
        if self.disposition == AssessmentPlanningDisposition.PLAN_STEP_CREATED:
            if not self.generated_step_ids:
                raise ValueError(
                    f"Evidence {self.evidence_id} marked PLAN_STEP_CREATED must have at least one generated_step_id"
                )
        elif self.disposition == AssessmentPlanningDisposition.MANUAL_REVIEW_REQUIRED:
            if not self.review_item_id and not self.advisory_proposal_id and not self.rationale:
                raise ValueError(
                    f"Evidence {self.evidence_id} marked MANUAL_REVIEW_REQUIRED must provide review_item_id, advisory_proposal_id, or rationale"
                )
        elif self.disposition == AssessmentPlanningDisposition.UNSUPPORTED_ACTION:
            if not self.advisory_proposal_id and not self.rationale:
                raise ValueError(
                    f"Evidence {self.evidence_id} marked UNSUPPORTED_ACTION must provide advisory_proposal_id or rationale"
                )
        elif self.disposition == AssessmentPlanningDisposition.DUPLICATE_CONSOLIDATED:
            if not self.canonical_surviving_evidence_id:
                raise ValueError(
                    f"Evidence {self.evidence_id} marked DUPLICATE_CONSOLIDATED must provide canonical_surviving_evidence_id"
                )
            if self.canonical_surviving_evidence_id == self.evidence_id:
                raise ValueError(
                    f"Evidence {self.evidence_id} cannot consolidate onto itself"
                )
        return self


class ReconciliationResult(BaseModel):
    reconciled: bool
    total_evidence_count: int
    planning_relevant_count: int
    reconciled_count: int
    unreconciled_evidence_ids: Tuple[str, ...] = ()
    orphan_step_ids: Tuple[str, ...] = ()
    records: Dict[str, EvidencePlanningRecord] = Field(default_factory=dict)
    diagnostics: List[str] = Field(default_factory=list)


def validate_acyclic_duplicate_graph(consolidation_map: Dict[str, str]) -> None:
    """
    Validates that duplicate consolidation references form an acyclic directed forest.
    Raises ValueError on cycle or self-reference.
    """
    for node, parent in consolidation_map.items():
        if node == parent:
            raise ValueError(f"Self-referential duplicate consolidation detected on '{node}'")
        visited: Set[str] = {node}
        curr = parent
        while curr in consolidation_map:
            if curr in visited:
                cycle_str = " -> ".join(list(visited) + [curr])
                raise ValueError(f"Circular duplicate consolidation detected: {cycle_str}")
            visited.add(curr)
            curr = consolidation_map[curr]


def classify_evidence_relevance(raw_issue: Dict[str, Any]) -> EvidencePlanningRelevance:
    """
    Classifies raw assessment evidence into PLANNING_RELEVANT, INFORMATIONAL, or ASSESSMENT_DIAGNOSTIC.
    Informational/diagnostic issues (e.g. summary profiling counts, memory footprints) do not require plan steps.
    """
    explicit = raw_issue.get("planning_relevance") or raw_issue.get("relevance")
    if explicit:
        if isinstance(explicit, EvidencePlanningRelevance):
            return explicit
        str_val = str(explicit).strip().lower()
        if str_val in ("planning_relevant", "relevant"):
            return EvidencePlanningRelevance.PLANNING_RELEVANT
        if str_val in ("informational", "info"):
            return EvidencePlanningRelevance.INFORMATIONAL
        if str_val in ("assessment_diagnostic", "diagnostic"):
            return EvidencePlanningRelevance.ASSESSMENT_DIAGNOSTIC

    cat = str(raw_issue.get("category") or "").lower()
    itype = str(raw_issue.get("issue_type") or raw_issue.get("type") or "").lower()

    if cat in ("profiling_summary", "metadata", "informational"):
        return EvidencePlanningRelevance.INFORMATIONAL
    if cat in ("system_diagnostic", "diagnostic") or itype in ("dataset_profiled", "column_profile_complete", "row_count_statistic", "profiling_runtime"):
        return EvidencePlanningRelevance.ASSESSMENT_DIAGNOSTIC
    return EvidencePlanningRelevance.PLANNING_RELEVANT


def reconcile_assessment_to_plan(
    assessment_issues: List[Dict[str, Any]],
    plan_steps: List[Dict[str, Any]],
    manual_review_items: Optional[List[Dict[str, Any]]] = None,
    advisory_proposals: Optional[List[Dict[str, Any]]] = None,
    consolidation_map: Optional[Dict[str, str]] = None,
    rule_ids: Optional[Set[str]] = None,
) -> ReconciliationResult:
    """
    Performs bidirectional reconciliation between assessment evidence and plan steps:
    1. Forward: Every planning-relevant evidence must receive a disposition.
    2. Reverse: Every plan step must link back to an evidence item or an explicit business rule.
    3. Bidirectional lineage: Step references evidence, and evidence references step.
    4. Duplicate graph acyclicity validation.
    """
    if consolidation_map:
        validate_acyclic_duplicate_graph(consolidation_map)

    manual_items = manual_review_items or []
    proposals = advisory_proposals or []
    known_rules = set(rule_ids or [])

    # Index known evidence items
    known_evidence_ids: Set[str] = set()
    for issue in assessment_issues:
        eid = str(issue.get("id") or issue.get("evidence_id") or issue.get("issue_id") or "").strip()
        if eid:
            known_evidence_ids.add(eid)

    # Index plan steps by evidence_id
    steps_by_evidence: Dict[str, List[str]] = {}
    for st in plan_steps:
        sid = str(st.get("id") or st.get("step_id") or "step")
        ev_id = st.get("evidence_id") or st.get("source_evidence_id") or st.get("issue_id")
        if ev_id:
            steps_by_evidence.setdefault(str(ev_id), []).append(sid)

    # Index manual review items
    review_by_evidence: Dict[str, str] = {}
    for item in manual_items:
        ev_id = item.get("evidence_id") or item.get("issue_id")
        rid = str(item.get("id") or item.get("review_item_id") or "rev")
        if ev_id:
            review_by_evidence[str(ev_id)] = rid

    # Index advisory proposals
    prop_by_evidence: Dict[str, str] = {}
    for prop in proposals:
        ev_id = prop.get("issue_id") or prop.get("evidence_id")
        pid = str(prop.get("proposal_id") or prop.get("id") or "prop")
        if ev_id:
            prop_by_evidence[str(ev_id)] = pid

    records: Dict[str, EvidencePlanningRecord] = {}
    unreconciled: List[str] = []
    diagnostics: List[str] = []
    planning_relevant_count = 0

    for issue in assessment_issues:
        ev_id = str(issue.get("id") or issue.get("evidence_id") or issue.get("issue_id") or "").strip()
        if not ev_id:
            ev_id = f"ev_{len(records)+1}"
        itype = str(issue.get("issue_type") or issue.get("type") or "unknown")
        relevance = classify_evidence_relevance(issue)

        if relevance != EvidencePlanningRelevance.PLANNING_RELEVANT:
            records[ev_id] = EvidencePlanningRecord(
                evidence_id=ev_id,
                issue_type=itype,
                disposition=AssessmentPlanningDisposition.OUT_OF_SCOPE,
                relevance=relevance,
                rationale=f"Relevance classified as {relevance.value}",
            )
            continue

        planning_relevant_count += 1

        # Check duplicate consolidation
        if consolidation_map and ev_id in consolidation_map:
            surviving_id = consolidation_map[ev_id]
            records[ev_id] = EvidencePlanningRecord(
                evidence_id=ev_id,
                issue_type=itype,
                disposition=AssessmentPlanningDisposition.DUPLICATE_CONSOLIDATED,
                canonical_surviving_evidence_id=surviving_id,
                relevance=relevance,
                rationale=f"Consolidated into canonical evidence '{surviving_id}'",
            )
            continue

        # Check if plan steps exist
        if ev_id in steps_by_evidence:
            step_ids = tuple(steps_by_evidence[ev_id])
            records[ev_id] = EvidencePlanningRecord(
                evidence_id=ev_id,
                issue_type=itype,
                disposition=AssessmentPlanningDisposition.PLAN_STEP_CREATED,
                generated_step_ids=step_ids,
                relevance=relevance,
            )
            continue

        # Check if in manual review
        if ev_id in review_by_evidence:
            records[ev_id] = EvidencePlanningRecord(
                evidence_id=ev_id,
                issue_type=itype,
                disposition=AssessmentPlanningDisposition.MANUAL_REVIEW_REQUIRED,
                review_item_id=review_by_evidence[ev_id],
                relevance=relevance,
            )
            continue

        # Check if in advisory proposals
        if ev_id in prop_by_evidence:
            records[ev_id] = EvidencePlanningRecord(
                evidence_id=ev_id,
                issue_type=itype,
                disposition=AssessmentPlanningDisposition.UNSUPPORTED_ACTION,
                advisory_proposal_id=prop_by_evidence[ev_id],
                relevance=relevance,
            )
            continue

        # Check if validation-only or policy blocked
        disposition_override = issue.get("planning_disposition")
        if disposition_override and disposition_override in AssessmentPlanningDisposition.__members__.values():
            records[ev_id] = EvidencePlanningRecord(
                evidence_id=ev_id,
                issue_type=itype,
                disposition=AssessmentPlanningDisposition(disposition_override),
                relevance=relevance,
                rationale=issue.get("rationale") or "Explicit disposition override",
            )
            continue

        # Unreconciled!
        unreconciled.append(ev_id)
        diagnostics.append(f"Planning-relevant evidence '{ev_id}' ({itype}) has no disposition or linked plan step")

    # Reverse pass: check for orphan steps that have no evidence and no supporting rule,
    # or that link to non-existent or non-planning-relevant evidence
    orphan_steps: List[str] = []
    for st in plan_steps:
        sid = str(st.get("id") or st.get("step_id") or "step")
        ev_id = st.get("evidence_id") or st.get("source_evidence_id") or st.get("issue_id")
        rule_id = st.get("rule_id") or st.get("business_rule_id")
        if ev_id:
            ev_str = str(ev_id)
            if ev_str not in known_evidence_ids:
                orphan_steps.append(sid)
                diagnostics.append(f"Plan step '{sid}' references non-existent evidence '{ev_str}'")
            elif ev_str in records and records[ev_str].relevance != EvidencePlanningRelevance.PLANNING_RELEVANT:
                orphan_steps.append(sid)
                diagnostics.append(f"Plan step '{sid}' references non-planning-relevant evidence '{ev_str}'")
        elif not (rule_id and rule_id in known_rules):
            orphan_steps.append(sid)
            diagnostics.append(f"Plan step '{sid}' has neither valid evidence link nor known supporting rule ID")

    reconciled_ok = len(unreconciled) == 0 and len(orphan_steps) == 0

    return ReconciliationResult(
        reconciled=reconciled_ok,
        total_evidence_count=len(assessment_issues),
        planning_relevant_count=planning_relevant_count,
        reconciled_count=len(records) - len(unreconciled),
        unreconciled_evidence_ids=tuple(unreconciled),
        orphan_step_ids=tuple(orphan_steps),
        records=records,
        diagnostics=diagnostics,
    )
