"""
Classify plan steps and manifest issues into AUTO / REVIEW / BLOCKED buckets.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from agent.etl_pipeline.action_registry import (
    ACTION_REGISTRY,
    AUTO_ELIGIBLE_ACTIONS as _AUTO_ACTIONS,
    ALWAYS_REVIEW_ACTIONS as _REVIEW_ACTIONS,
    get_action_meta,
)

# Export lists for backward compatibility tests
_AUTO_ACTIONS = _AUTO_ACTIONS
_REVIEW_ACTIONS = _REVIEW_ACTIONS


def classify_step_bucket(
    action: str,
    *,
    severity: str = "medium",
    null_percentage: Optional[float] = None,
    never_drop_rows: bool = False,
) -> dict[str, str]:
    """
    Returns: {"bucket": "auto" | "review" | "blocked", "phase": "cleanse" | "transform"}
    """
    act = (action or "").strip().lower()
    sev = (severity or "medium").lower()
    meta = get_action_meta(act)

    if meta.get("invalid"):
        return {"bucket": "blocked", "phase": "invalid"}

    # Special rules:
    # 1. drop_rows
    if act == "drop_rows":
        bucket = "blocked" if never_drop_rows else "review"
    # 2. fill_or_drop or other conditional auto actions
    elif act == "fill_or_drop" and null_percentage is not None and null_percentage > 30:
        bucket = "review"
    elif meta.get("requires_review") or sev == "high":
        bucket = "review"
    elif meta.get("auto_eligible") is True:
        bucket = "auto"
    elif meta.get("auto_eligible") == "CONDITIONAL":
        # Safe defaults for conditional actions
        bucket = "auto"
    else:
        bucket = "review"

    phase_num = meta.get("phase")
    if phase_num == 1:
        phase = "cleanse"
    elif phase_num == 2:
        phase = "transform"
    else:
        phase = "transform"

    return {"bucket": bucket, "phase": phase}


def tag_plan_step_buckets(plan: Dict[str, Any], business_rules: Dict[str, Any]) -> Dict[str, Any]:
    """Add bucket and phase fields to every step in plan.datasets."""
    never_drop = bool(business_rules.get("never_drop_rows"))
    for _ds, block in (plan.get("datasets") or {}).items():
        if not isinstance(block, dict):
            continue
        for st in block.get("steps") or []:
            if not isinstance(st, dict):
                continue

            # User-promoted steps (from manual review resolution) are explicitly approved
            # by the user — count them as 'auto' so readiness % reflects the resolution.
            evidence = st.get("evidence") or {}
            user_approved = (
                bool(evidence.get("rule_override"))
                or st.get("source") in ("non_fixable_user_resolution",)
                or bool(evidence.get("auto_resolved"))
            )
            if user_approved:
                st["bucket"] = "auto"
                st["phase"] = st.get("phase") or "cleanse"
                continue

            sev = str(st.get("severity") or "medium")
            res = classify_step_bucket(
                str(st.get("action") or ""),
                severity=sev,
                null_percentage=st.get("null_percentage"),
                never_drop_rows=never_drop,
            )
            st["bucket"] = res["bucket"]
            st["phase"] = res["phase"]
    return plan
