"""
Remediation Router for Agent Dhara.
Evaluates data quality findings against the Capability Registry,
enforces objective safety & impact gates, and assigns auditable lifecycle states.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from agent.etl_pipeline.issue_registry import ISSUE_TYPE_ALIASES, canonicalize_issue_type
from agent.etl_pipeline.remediation_capability_registry import (
    REMEDIATION_CAPABILITY_REGISTRY,
    UNKNOWN_CAPABILITY_FALLBACK,
    get_capability_entry,
)
from agent.etl_pipeline.remediation_models import (
    CapabilityEntry,
    RecommendationCategory,
    RoutingResult,
)

logger = logging.getLogger("agent.remediation_router")

# ── Reason Codes ─────────────────────────────────────────────────────────────
REASON_STANDARD_LOW_RISK = "STANDARD_LOW_RISK_TRANSFORMATION"
REASON_UNKNOWN_ISSUE_TYPE = "UNKNOWN_ISSUE_TYPE_REQUIRES_REVIEW"
REASON_LOW_DETECTION_CERTAINTY = "LOW_DETECTION_CERTAINTY_REQUIRES_INVESTIGATION"
REASON_CONTEXTUAL_REMEDIATION = "CONTEXTUAL_REMEDIATION_REQUIRES_DECISION"
REASON_IMPACT_PERCENTAGE_EXCEEDED = "IMPACT_PERCENTAGE_EXCEEDS_SAFETY_LIMIT"
REASON_IMPACT_ROWS_EXCEEDED = "IMPACT_ROWS_EXCEEDS_SAFETY_LIMIT"
REASON_HIGH_RISK_ACTION = "HIGH_OR_CRITICAL_RISK_REQUIRES_APPROVAL"
REASON_DESTRUCTIVE_ACTION = "ROW_DESTRUCTIVE_ACTION_REQUIRES_APPROVAL"
REASON_MISSING_METRICS = "MISSING_AFFECTED_METRICS_REQUIRES_CONFIRMATION"


def route_finding(
    finding: Dict[str, Any],
    dataset: Optional[str] = None,
    total_rows: Optional[int] = None,
) -> RoutingResult:
    """
    Evaluates a DQ finding and routes it to standard remediation, decision required,
    or manual investigation with objective auto-fix gating.
    """
    raw_issue_type = str(finding.get("issue_type") or finding.get("type") or "unknown")
    canonical_type = canonicalize_issue_type(raw_issue_type)
    ds = dataset or str(finding.get("dataset") or "default_dataset")
    col = finding.get("column")
    col_str = str(col) if col is not None else None
    issue_id = str(finding.get("id") or f"{ds}_{col_str}_{canonical_type}")

    # 1. Capability Lookup with safe unknown fallback
    is_registered = canonical_type in REMEDIATION_CAPABILITY_REGISTRY
    capability = get_capability_entry(canonical_type)

    reason_codes: List[str] = []

    # 2. Extract metrics
    affected_rows = finding.get("affected_rows")
    if affected_rows is None:
        affected_rows = finding.get("count") or finding.get("row_count_affected")
    if affected_rows is not None:
        try:
            affected_rows = int(affected_rows)
        except (ValueError, TypeError):
            affected_rows = None

    affected_pct = finding.get("affected_percentage") or finding.get("pct")
    if affected_pct is not None:
        try:
            affected_pct = float(affected_pct)
        except (ValueError, TypeError):
            affected_pct = None
    elif affected_rows is not None and total_rows and total_rows > 0:
        affected_pct = round((affected_rows / total_rows) * 100.0, 2)

    # 3. Determine Recommendation Category & Baseline Statuses
    if not is_registered:
        rec_category: RecommendationCategory = "decision_required"
        rec_status = "ai_options_available"
        approval_status = "pending"
        auto_fix_eligible = False
        reason_codes.append(REASON_UNKNOWN_ISSUE_TYPE)

    elif capability.detection_certainty in ("low", "unknown"):
        rec_category = "manual_investigation"
        rec_status = "manual_investigation_required"
        approval_status = "pending"
        auto_fix_eligible = False
        reason_codes.append(REASON_LOW_DETECTION_CERTAINTY)

    elif capability.remediation_certainty == "high" and capability.risk_level == "low":
        rec_category = "standard_remediation"
        rec_status = "standard_available"
        auto_fix_eligible = capability.auto_fix_eligible
        approval_status = "not_required" if auto_fix_eligible else "pending"
        reason_codes.append(REASON_STANDARD_LOW_RISK)

    else:
        rec_category = "decision_required"
        rec_status = "ai_options_available"
        approval_status = "pending"
        auto_fix_eligible = False
        reason_codes.append(REASON_CONTEXTUAL_REMEDIATION)

    # 4. Check Risk and Destructive Invariants
    if capability.risk_level in ("high", "critical"):
        auto_fix_eligible = False
        if REASON_HIGH_RISK_ACTION not in reason_codes:
            reason_codes.append(REASON_HIGH_RISK_ACTION)

    if capability.row_destructive:
        auto_fix_eligible = False
        if REASON_DESTRUCTIVE_ACTION not in reason_codes:
            reason_codes.append(REASON_DESTRUCTIVE_ACTION)

    # 5. Objective Impact Boundary Gates (dual boundary: % or absolute rows)
    limit_pct = capability.impact_policy.max_affected_percentage
    limit_rows = capability.impact_policy.max_affected_rows

    # Check percentage limit
    if affected_pct is not None and affected_pct > limit_pct:
        auto_fix_eligible = False
        approval_status = "pending"
        if REASON_IMPACT_PERCENTAGE_EXCEEDED not in reason_codes:
            reason_codes.append(REASON_IMPACT_PERCENTAGE_EXCEEDED)

    # Check absolute row count limit
    if affected_rows is not None and affected_rows > limit_rows:
        auto_fix_eligible = False
        approval_status = "pending"
        if REASON_IMPACT_ROWS_EXCEEDED not in reason_codes:
            reason_codes.append(REASON_IMPACT_ROWS_EXCEEDED)

    # If metrics are completely missing for an issue, require review
    # (Proactive hygiene transforms such as proactive_trim operate on clean columns)
    is_proactive = raw_issue_type.startswith("proactive_")
    if affected_rows is None and affected_pct is None:
        if not is_proactive:
            auto_fix_eligible = False
            approval_status = "pending"
            if REASON_MISSING_METRICS not in reason_codes:
                reason_codes.append(REASON_MISSING_METRICS)
        else:
            affected_rows = 0
            affected_pct = 0.0

    # 6. Determine Primary Reason Code Precedence
    if REASON_UNKNOWN_ISSUE_TYPE in reason_codes:
        primary_reason = REASON_UNKNOWN_ISSUE_TYPE
    elif REASON_HIGH_RISK_ACTION in reason_codes:
        primary_reason = REASON_HIGH_RISK_ACTION
    elif REASON_DESTRUCTIVE_ACTION in reason_codes:
        primary_reason = REASON_DESTRUCTIVE_ACTION
    elif REASON_LOW_DETECTION_CERTAINTY in reason_codes:
        primary_reason = REASON_LOW_DETECTION_CERTAINTY
    elif REASON_IMPACT_PERCENTAGE_EXCEEDED in reason_codes:
        primary_reason = REASON_IMPACT_PERCENTAGE_EXCEEDED
    elif REASON_IMPACT_ROWS_EXCEEDED in reason_codes:
        primary_reason = REASON_IMPACT_ROWS_EXCEEDED
    elif REASON_CONTEXTUAL_REMEDIATION in reason_codes:
        primary_reason = REASON_CONTEXTUAL_REMEDIATION
    elif REASON_MISSING_METRICS in reason_codes:
        primary_reason = REASON_MISSING_METRICS
    else:
        primary_reason = REASON_STANDARD_LOW_RISK

    # 7. Build Stable Cluster Key: (dataset, cluster_group)
    cluster_key: Tuple[str, str] = (ds, capability.cluster_group)

    return RoutingResult(
        issue_id=issue_id,
        raw_issue_type=raw_issue_type,
        canonical_issue_type=canonical_type,
        dataset=ds,
        column=col_str,
        recommendation_category=rec_category,
        recommendation_status=rec_status,
        approval_status=approval_status,
        codegen_status="not_started",
        validation_status="not_started",
        execution_status="not_started",
        auto_fix_eligible=auto_fix_eligible,
        reason_codes=reason_codes,
        primary_reason_code=primary_reason,
        registry_version="v1.0",
        capability=capability,
        cluster_key=cluster_key,
        affected_rows=affected_rows,
        affected_percentage=affected_pct,
        applied_impact_limit_pct=limit_pct,
        applied_impact_limit_rows=limit_rows,
    )
