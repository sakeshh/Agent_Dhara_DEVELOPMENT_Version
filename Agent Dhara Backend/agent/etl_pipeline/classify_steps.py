"""
Classify plan steps and manifest issues into AUTO / REVIEW / BLOCKED buckets.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

_AUTO_ACTIONS = frozenset(
    {
        "trim",
        "lowercase",
        "uppercase",
        "fill_nulls_simple",
        "fill_or_drop",
        "cast_type",
        "coerce_numeric",
        "parse_dates",
        "parse_dates_safe",        # safe date parse with audit flag
        "flag_domain_violation",   # domain violation flagging (no data loss)
        "fill_nulls_flag",         # flag + preserve invalid numeric values
        "sanitize_email",
        "normalize_phone",
        "regex_replace",
        "range_clip",
        "clip_or_flag",
        "flag_outliers",
        "clip_outliers",
        "cap_outliers",
        "standardize_boolean",
        "replace_values",
        "zero_to_null",
        "deduplicate",
        "join_datasets",
        "hash_phone",
        "mask_phone",
        "drop_column",
        "exclude_column",
        "nullify_future_dates",
        "nullify_punctuation",
        "nullify_dummy_dates",
        "noop",
        "replace_sentinel_values",
    }
)

_REVIEW_ACTIONS = frozenset({"review_manually", "validate_referential_integrity_or_stage"})


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

    if act == "drop_rows":
        bucket = "blocked" if never_drop_rows else "review"
    elif act == "fill_or_drop" and null_percentage is not None and null_percentage > 30:
        bucket = "review"
    elif act in _REVIEW_ACTIONS or sev == "high":
        bucket = "review"
    elif act in _AUTO_ACTIONS:
        bucket = "auto"
    else:
        bucket = "review"

    # Define cleanse-phase vs transform-phase actions
    cleanse_actions = {
        "trim", "lowercase", "uppercase", "fill_nulls_simple", "fill_or_drop",
        "cast_type", "coerce_numeric", "parse_dates", "sanitize_email",
        "normalize_phone", "range_clip", "clip_or_flag", "flag_outliers",
        "clip_outliers", "cap_outliers", "standardize_boolean", "zero_to_null",
        "deduplicate", "drop_column", "exclude_column", "nullify_future_dates",
        "nullify_punctuation", "nullify_dummy_dates",
        "noop", "drop_rows"
    }
    phase = "cleanse" if act in cleanse_actions else "transform"

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
