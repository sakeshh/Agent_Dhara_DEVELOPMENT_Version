"""
Issue-to-Step Compiler: 3-pass layering engine translating data quality issues
to rule-validated, semantics-refined ETL plan steps, manual review queues, and non-fixable items.
"""
from typing import Any, Dict, List, Tuple
import logging

logger = logging.getLogger("agent.etl_pipeline.issue_to_step_compiler")

from agent.transformation_suggester import ISSUE_TO_ACTION
_ISSUE_TO_ACTION_MAP = dict(ISSUE_TO_ACTION)
_ISSUE_TO_ACTION_MAP.update({
    "nulls": "fill_or_drop",
    "null_values": "fill_or_drop",
    "type_mismatch": "cast_type",
    "invalid_type": "cast_type",
    "outliers": "clip_or_flag",
    "numeric_outliers": "clip_or_flag",
    "duplicate": "deduplicate",
    "duplicates": "deduplicate",
    "near_duplicate_rows": "deduplicate",
    "numeric_outliers_iqr": "clip_or_flag",
    # duplicate_primary_key is a COMPLEX issue — always routes to manual review,
    # never auto-deduped without explicit user acknowledgment
    "duplicate_primary_key": "review_manually",
    "invalid_gstin": "regex_replace",
    "invalid_pan": "regex_replace",
    "invalid_aadhaar": "regex_replace",
    "invalid_ifsc": "regex_replace",
    "invalid_cin": "regex_replace",
    "invalid_url": "regex_replace",
    "repeated_token_in_string": "regex_replace",
    "encoding_corruption": "regex_replace",
    "sentinel_numeric_value": "replace_sentinel_values",
    "string_length_outlier": "flag_outliers",
    "custom_rule_violation": "review_manually",
    # String quality
    # mixed_case / inconsistent_case: use trim-only — applying lowercase to master-data
    # fields (customer_name, machine_name, location) destroys semantic value.
    # Columns needing full lowercase normalization should be configured explicitly.
    "leading_trailing_whitespace":        "trim",
    "mixed_case":                         "trim",
    "inconsistent_case":                  "trim",
    "special_characters":                 "regex_replace",
    "html_tags":                          "regex_replace",
    "control_characters":                 "regex_replace",
    "unicode_normalization":              "regex_replace",
    # Placeholders (N/A, ???, unknown, TBD) → nullify punctuation-only values
    "placeholder_detected":               "nullify_punctuation",
    # Categorical column contains only-digit strings — flag it, do NOT cast to numeric
    "string_with_only_digits_in_text_column": "flag_domain_violation",
    # Numeric
    "negative_values":               "range_clip",
    "zero_values":                   "zero_to_null",
    "impossible_values":             "range_clip",
    "precision_loss":                "cast_type",
    # Date/time
    # invalid_date_format: use parse_dates_safe which emits both a flag column
    # and the timestamp parse — preventing silent null propagation
    "future_dates":                  "nullify_future_dates",
    "dummy_dates":                   "nullify_dummy_dates",
    "invalid_date_format":           "parse_dates_safe",
    "date_range_violation":          "nullify_future_dates",
    # Boolean/categorical
    "boolean_inconsistency":         "standardize_boolean",
    "invalid_category":              "replace_values",
    "enum_violation":                "replace_values",
    "inconsistent_boolean":          "standardize_boolean",
    # PII / privacy
    "pii_email":                     "sanitize_email",
    "pii_phone":                     "hash_phone",
    "pii_sensitive":                 "hash_phone",
    # Structural
    "schema_drift":                  "cast_type",
    "column_rename":                 "cast_type",
    "extra_whitespace_in_name":      "trim",
    # Invalid numerics in named numeric columns (e.g. amount='error') — flag + leave raw
    "invalid_numeric_values":        "fill_nulls_flag",
    # Sentinel date strings (00/00/0000, 99/99/9999) → nullify
    "invalid_date_format":           "parse_dates_safe",
})


_NON_FIXABLE_ISSUE_TYPES = frozenset({
    "missing_required_column",
    "very_wide_table",
    "empty_dataset",
    "orphan_foreign_keys",          # FK violation — needs source fix
    "schema_mismatch",              # target schema incompatible
    # encoding_corruption REMOVED — fixable with regex_replace in most cases
    "referential_integrity_violation",
})

# These CAN be partially addressed but carry high business risk — need explicit acknowledgment
_COMPLEX_ISSUE_TYPES = frozenset({
    "custom_rule_violation",
    "business_key_duplicate",
    "duplicate_primary_key",              # always manual — dedup of PKs requires business sign-off
    "dq_gate_warning",
    "high_null_percentage",               # >50% null — may indicate structural problem
    "dominant_value_skew",               # may indicate sentinel fill
    "multivariate_outliers",             # cross-column — needs domain judgment
    "string_with_only_digits_in_text_column",  # domain violation — flag only, never cast
    "invalid_numeric_values",            # text-encoded non-numerics — flag, preserve original
})


def _apply_three_pass_overrides(
    sug: Dict[str, Any],
    action: str,
    rules: Dict[str, Any],
    sem_schema: Dict[str, Any],
) -> Tuple[str, bool, str, Dict[str, Any]]:
    """
    Applies Pass 1 (baseline mapping), Pass 2 (business rules), Pass 3 (semantic refinement).
    Returns (resolved_action, auto_fixable, note, params).
    """
    never_drop_rows = bool(rules.get("never_drop_rows"))
    outlier_strategy = str(rules.get("outlier_strategy") or "flag").lower().strip()
    non_nullable_cols = [str(x).lower().strip() for x in (rules.get("non_nullable") or [])]

    ds = sug.get("dataset") or "global"
    col = sug.get("column") or "*"
    it = sug.get("issue_type") or "unknown"
    was_manual = (sug.get("auto_fixable") is False) or (sug.get("suggested_action") == "review_manually")
    note = f"Baseline issue mapping for {it}"
    params = dict(sug.get("params") or {})
    if action == "replace_sentinel_values" and "sentinel_values" not in params:
        params["sentinel_values"] = [
            -999.0,
            -9999.0,
            999.0,
            9999.0,
            99999.0,
            999999.0,
            "N/A",
            "NA",
            "NULL",
            "UNKNOWN",
            "UNSPECIFIED",
            "??",
            "---",
        ]

    # Pass 1: Issue Baseline Mapping
    if not action or action == "noop":
        action = _ISSUE_TO_ACTION_MAP.get(it) or "noop"

    if it == "encoding_corruption":
        # Try regex fix first; if pattern confidence < 0.5, escalate to manual_review
        if sug.get("pattern_confidence", 1.0) >= 0.5:
            action = "regex_replace"
        else:
            action = "review_manually"

    if action in ("review_manually", "noop"):
        action = "review_manually"

    # Pass 2: Business Rules Overrides
    if never_drop_rows and action == "fill_or_drop":
        action = "fill_nulls_simple"
        note = "never_drop_rows override: changed from fill_or_drop to fill_nulls_simple"
        params["fill_strategy"] = "value"
        params["fill_value"] = None

    if action == "clip_or_flag":
        action = {"clip": "clip_outliers", "cap": "cap_outliers"}.get(outlier_strategy, "flag_outliers")
        note = f"outlier_strategy={outlier_strategy} override"

    col_key = col.lower()
    if (col_key in non_nullable_cols or f"{ds}.{col_key}" in non_nullable_cols) and action == "fill_or_drop":
        action = "fill_nulls_simple"
        note = "non_nullable override: using fill-only instead of drop/fill choice"
        params["fill_strategy"] = "value"
        params["fill_value"] = None

    # Pass 3: Semantic Refinement
    sem_desc = sem_schema.get(f"{ds}.{col}") or {}
    sem_type = str(sem_desc.get("semantic_type") or "").lower().strip()
    sub_type = str(sem_desc.get("sub_type") or "").lower().strip()
    pii_level = str(sem_desc.get("pii_level") or "").lower().strip()
    sem_confidence = float(sem_desc.get("confidence", 1.0))

    if sem_type == "date" and action in ("cast_type", "noop", "review_manually"):
        action = "parse_dates"
        note = "Semantic refinement: parse_dates for date semantic_type"
    elif sem_type == "email" and action in ("trim", "lowercase", "noop", "review_manually"):
        action = "sanitize_email"
        note = "Semantic refinement: sanitize_email for email semantic_type"
    elif sem_type == "phone" and action in ("trim", "noop", "review_manually"):
        if pii_level in ("high", "medium"):
            action = "hash_phone"
            note = "Semantic refinement: hash_phone for high/medium PII phone number"
        else:
            action = "normalize_phone"
            note = "Semantic refinement: normalize_phone for phone semantic_type"
    elif sub_type == "currency" and action in ("clip_or_flag", "clip_outliers", "review_manually"):
        action = "range_clip"
        params["lower_bound"] = 0.0
        note = "Semantic refinement: range_clip (lower_bound=0) for currency sub_type"
    elif sub_type == "boolean_int" and action in ("coerce_numeric", "review_manually"):
        action = "standardize_boolean"
        note = "Semantic refinement: standardize_boolean for boolean_int sub_type"

    # Enforce manual intent preservation if semantic confidence is low
    if was_manual and action != "review_manually" and sem_confidence < 0.85:
        action = "review_manually"
        note = f"Preserved manual review: semantic confidence {sem_confidence:.2f} < 0.85"

    # Determine auto_fixable
    is_risky = action in ("drop_column", "exclude_column", "clip_outliers", "cap_outliers", "review_manually")
    auto_fixable = sug.get("auto_fixable")
    if auto_fixable is None:
        auto_fixable = action not in ("review_manually", "noop")
    if is_risky:
        auto_fixable = False

    # Check LLM confidence if inferred suggestion
    from agent.etl_pipeline.rule_provenance import RuleProvenance
    is_llm_inferred = sug.get("llm_recommendation") is not None and sug.get("provenance") != RuleProvenance.AUTO_DETECTED
    if is_llm_inferred:
        confidence = sug.get("llm_confidence") or 1.0
        if confidence < 0.65:
            auto_fixable = False

    return action, auto_fixable, note, params


def compile_issues_to_steps(
    suggestions: List[Dict[str, Any]],
    rules: Dict[str, Any],
    sem_schema: Dict[str, Any]
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    datasets_steps = {}
    manual_review = []
    seen_mr = set()
    
    from agent.etl_pipeline.manual_review_catalog import enrich_manual_review_item, manual_review_item_id
    
    # Process suggestions
    for sug in suggestions:
        ds = sug.get("dataset") or "global"
        col = sug.get("column") or "*"
        it = sug.get("issue_type") or "unknown"
        
        action, auto_fixable, note, params = _apply_three_pass_overrides(
            sug, sug.get("suggested_action"), rules, sem_schema
        )
        
        is_noop = (action == "noop")
        
        # Save step or route to manual_review/non_fixable
        if auto_fixable and action != "review_manually" and not is_noop:
            # Add to steps
            datasets_steps.setdefault(ds, [])
            max_order = max((s.get("order", 0) for s in datasets_steps[ds]), default=0)
            
            datasets_steps[ds].append({
                "order": max_order + 1,
                "column": col,
                "action": action,
                "bucket": "cleanse",
                "phase": sug.get("phase", "cleanse"),
                "priority": 80,
                "params": params,
                "note": f"{sug.get('message') or note} ({note})",
                "source": "issue_to_step_compiler",
                "auto_fixable": True,
                "source_issue_type": it,
                "severity": sug.get("severity") or "medium",
                "estimated_affected_rows": sug.get("row_count_affected"),
                "llm_recommendation": sug.get("llm_recommendation"),
                "message": sug.get("message"),
            })
        else:
            # For noops, only retain if they have associated validation/LLM context or severity is not trivial
            if is_noop:
                has_context = bool(sug.get("llm_recommendation") or sug.get("validation_errors") or sug.get("message"))
                if not has_context:
                    continue

            # Uniqueness check based on (dataset, column, issue_type, action)
            mr_key = (ds, col, it, action)
            if mr_key in seen_mr:
                continue
            seen_mr.add(mr_key)

            # Determine risk tier
            if it in _NON_FIXABLE_ISSUE_TYPES:
                risk_tier = "non_fixable"
            elif it in _COMPLEX_ISSUE_TYPES or (
                sug.get("severity", "").lower() == "high" and not auto_fixable
            ):
                risk_tier = "complex"
            else:
                risk_tier = "standard"

            mr_item = {
                "id": manual_review_item_id(ds if ds != "global" else None, col, it),
                "dataset": ds if ds != "global" else None,
                "column": col,
                "issue_type": it,
                "risk_tier": risk_tier,
                "severity": sug.get("severity") or "medium",
                "message": sug.get("message") or f"Review required for {it}",
                "guidance": sug.get("manual_guidance") or f"Configure action for {col}",
                "suggested_action": action,
                "auto_fixable": False,
            }
            if "llm_recommendation" in sug:
                mr_item["llm_recommendation"] = sug["llm_recommendation"]
            if "llm_confidence" in sug:
                mr_item["llm_confidence"] = sug["llm_confidence"]

            # ALL tiers go to manual_review — no separate non_fixable list
            manual_review.append(enrich_manual_review_item(
                mr_item,
                llm_recommendation=mr_item.get("llm_recommendation")
            ))
            
    # Deduplicate steps by (action, column) to avoid redundant operations
    deduplicated_datasets_steps = {}
    for ds, steps in datasets_steps.items():
        seen = set()
        deduped = []
        for step in steps:
            key = (step.get("action"), step.get("column"))
            if key not in seen:
                seen.add(key)
                deduped.append(step)
        for idx, step in enumerate(deduped):
            step["order"] = idx + 1
        deduplicated_datasets_steps[ds] = deduped
            
    return deduplicated_datasets_steps, manual_review


def preprocess_suggestions_in_place(
    suggestions: List[Dict[str, Any]],
    rules: Dict[str, Any],
    sem_schema: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Modifies suggestions in-place to apply the 3-pass compiler logic.
    Returns the list of non_fixable issues.
    """
    non_fixables = []

    for sug in suggestions:
        ds = sug.get("dataset") or "global"
        col = sug.get("column") or "*"
        it = sug.get("issue_type") or "unknown"
        
        action, auto_fixable, _, _ = _apply_three_pass_overrides(
            sug, sug.get("suggested_action"), rules, sem_schema
        )
            
        sug["suggested_action"] = action
        sug["auto_fixable"] = auto_fixable
        
        # Check non_fixable
        is_non_fixable = it in _NON_FIXABLE_ISSUE_TYPES
        if is_non_fixable:
            sug["non_fixable"] = True
            sug["risk_tier"] = "non_fixable"
            from agent.etl_pipeline.manual_review_catalog import manual_review_item_id
            nf_item = {
                "id": manual_review_item_id(ds if ds != "global" else None, col, it),
                "dataset": ds if ds != "global" else None,
                "column": col,
                "issue_type": it,
                "risk_tier": "non_fixable",
                "severity": sug.get("severity") or "medium",
                "message": sug.get("message") or f"Manual review required for {it}",
                "guidance": sug.get("manual_guidance") or f"Configure action for {col}",
                "suggested_action": action,
                "auto_fixable": False
            }
            if "llm_recommendation" in sug:
                nf_item["llm_recommendation"] = sug["llm_recommendation"]
            if "llm_confidence" in sug:
                nf_item["llm_confidence"] = sug["llm_confidence"]
            non_fixables.append(nf_item)
            
    return non_fixables
