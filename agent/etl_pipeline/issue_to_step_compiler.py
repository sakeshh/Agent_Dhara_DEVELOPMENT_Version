"""
Issue-to-Step Compiler: 3-pass layering engine translating data quality issues
to rule-validated, semantics-refined ETL plan steps, manual review queues, and non-fixable items.
"""
from typing import Any, Dict, List, Tuple
import logging

from agent.etl_pipeline.issue_identity import (
    make_step_id,
    make_issue_id,
    step_identity,
    merge_steps,
)

logger = logging.getLogger("agent.etl_pipeline.issue_to_step_compiler")

from agent.transformation_suggester import ISSUE_TO_ACTION
_ISSUE_TO_ACTION_MAP = dict(ISSUE_TO_ACTION)
_ISSUE_TO_ACTION_MAP.update({
    # Null handling: optional fields flag + noop, key fields flag_missing_key via governance override
    "nulls": "fill_nulls_flag",
    "null_values": "fill_nulls_flag",
    "type_mismatch": "cast_type",
    "invalid_type": "cast_type",
    "outliers": "flag_outliers",
    "numeric_outliers": "flag_outliers",
    "duplicate": "flag_duplicate_candidates",
    "duplicates": "flag_duplicate_candidates",
    "near_duplicate_rows": "flag_near_duplicate_cluster",
    "numeric_outliers_iqr": "flag_outliers",
    # duplicate_primary_key is a COMPLEX issue — always routes to manual review,
    # never auto-deduped without explicit user acknowledgment
    "duplicate_primary_key": "flag_business_key_duplicate",
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
    # Numeric — flag violations, do NOT clip in Phase 1
    "negative_values":               "flag_range_violation",
    "zero_values":                   "zero_to_null",
    "impossible_values":             "flag_range_violation",
    "precision_loss":                "cast_type",
    # Date/time
    # invalid_date_format: use parse_dates_safe which emits both a flag column
    # and the timestamp parse — preventing silent null propagation
    "future_dates":                  "nullify_future_dates",
    "dummy_dates":                   "nullify_dummy_dates",
    "invalid_date_format":           "parse_dates_safe",
    "date_range_violation":          "nullify_future_dates",
    # Boolean/categorical — flag, do NOT replace values in Phase 1
    "boolean_inconsistency":         "standardize_boolean",
    "invalid_category":              "flag_domain_violation",
    "enum_violation":                "flag_domain_violation",
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
    "near_duplicate_rows":           "flag_near_duplicate_cluster",
    "exact_duplicate_rows":          "flag_duplicate_candidates",
    "declared_key_null_violation":   "flag_missing_declared_key",
    "orphan_foreign_key_rows":       "validate_and_partition_relationship",
    "cross_field_non_negative":      "flag_cross_field_violation",
    "numeric_value_serialized_as_string": "coerce_numeric",
})


_BUSINESS_KEY_PATTERNS = ("id", "key", "code", "ref", "num", "no", "nbr", "uuid", "guid", "sku", "ean", "isbn", "asin")
_NAME_FIELD_PATTERNS = ("name", "title", "description", "label", "machine", "customer", "vendor", "supplier", "employee", "owner")

def _is_business_key(col: str) -> bool:
    col_l = str(col).lower()
    return any(k in col_l for k in _BUSINESS_KEY_PATTERNS)

def _is_name_field(col: str) -> bool:
    col_l = str(col).lower()
    return any(k in col_l for k in _NAME_FIELD_PATTERNS)


# Flag-only issues that must never auto-dedup
_FLAG_ONLY_ISSUE_TYPES = frozenset({
    "near_duplicate_rows",
    "exact_duplicate_rows",
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

    col_l = str(col).lower().strip()
    is_currency_or_price = (sub_type == "currency") or any(k in col_l for k in ("price", "cost", "fee", "amount", "budget", "salary", "revenue", "sales", "fare", "balance"))
    is_age = (col_l == "age") or "age" in it
    is_date = (sem_type == "date") or any(k in col_l for k in ("date", "created", "timestamp", "time", "dob", "birth"))
    is_phone = (sem_type == "phone") or "phone" in col_l or "phone" in it
    is_email = (sem_type == "email") or "email" in col_l or "email" in it

    is_approved = bool(
        sug.get("approved")
        or str(sug.get("approval_status", "")).lower() == "approved"
        or str(sug.get("status", "")).lower() == "approved"
        or sug.get("authority_source") == "HUMAN_APPROVED"
        or sug.get("selected_action")
        or sug.get("approved_remediation")
    )

    if is_currency_or_price and (it in ("negative_values", "impossible_values") or action in ("flag_range_violation", "clip_or_flag", "clip_outliers", "review_manually", "noop")):
        action = "range_clip"
        params["lower_bound"] = 0.0
        params["min_value"] = 0.0
        note = "Semantic refinement: range_clip (lower_bound=0) for price/currency column"
    elif is_age and (it in ("negative_values", "impossible_values", "outliers", "numeric_outliers") or action in ("flag_range_violation", "flag_outliers", "clip_or_flag", "review_manually", "noop")):
        action = "range_clip"
        params["lower_bound"] = 0
        params["upper_bound"] = 120
        params["min_value"] = 0
        params["max_value"] = 120
        note = "Semantic refinement: range_clip (0-120) for age column"
    elif is_date and (it in ("impossible_date", "sentinel_date")):
        if is_approved:
            action = action or ("nullify_date" if it == "impossible_date" else "map_to_null")
            note = f"Approved remediation: {action} for {it}"
        else:
            action = "flag_only"
            auto_fixable = False
            note = f"Governance guard: {it} requires human review; automated mutation prohibited"
    elif is_date and (it in ("invalid_date_format", "dummy_dates", "future_dates", "date_range_violation") or action in ("cast_type", "parse_dates_safe", "nullify_future_dates", "nullify_dummy_dates", "noop", "review_manually")):
        action = "parse_dates"
        params["filter_impossible"] = False
        params["sentinel_values"] = ["9999-12-31", "1900-01-01", "0001-01-01", "N/A", "NA", "???", "NULL"]
        note = "Semantic refinement: parse_dates for date column"
    elif is_email and action in ("trim", "lowercase", "noop", "review_manually", "flag_domain_violation"):
        action = "sanitize_email"
        params["regex"] = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
        note = "Semantic refinement: sanitize_email for email semantic_type"
    elif is_phone and action in ("trim", "noop", "review_manually", "flag_domain_violation"):
        if pii_level in ("high", "medium"):
            action = "hash_phone"
            note = "Semantic refinement: hash_phone for high/medium PII phone number"
        else:
            action = "normalize_phone"
            params["min_digits"] = 7
            params["max_digits"] = 15
            note = "Semantic refinement: normalize_phone for phone semantic_type"
    elif (it in ("placeholder_detected", "sentinel_value", "sentinel_string") or (action in ("nullify_punctuation", "review_manually", "noop") and col_l in ("customername", "city", "name", "address", "state", "country", "title", "description"))) and not _is_business_key(col):
        action = "replace_sentinel_values"
        params["sentinel_values"] = ["???", "n/a", "na", "null", "none", "unknown", "dummy", "temp", "test", "###", "-", "unspecified"]
        note = "Semantic refinement: replace_sentinel_values for text placeholders"
    elif sub_type == "boolean_int" and action in ("coerce_numeric", "review_manually"):
        action = "standardize_boolean"
        note = "Semantic refinement: standardize_boolean for boolean_int sub_type"
    # Pass 4: Governance guards
    from agent.profiling.type_inference import normalize_column_name, _CATEGORICAL_MASTER_COLUMNS
    col_norm = normalize_column_name(col)
    is_master_data = col_norm.canonical in _CATEGORICAL_MASTER_COLUMNS or col_norm.compact in _CATEGORICAL_MASTER_COLUMNS
    if action == "lowercase" and (_is_name_field(col) or _is_business_key(col) or is_master_data) and it not in ("duplicate_insensitive_values", "inconsistent_case", "mixed_case"):
        action = "trim_preserve_case"
        note = f"Governance override: replaced lowercase with trim_preserve_case for master data/name/key column {col}"
        
    if it == "declared_key_null_violation":
        action = "flag_missing_declared_key"
        note = f"Governance override: replaced null imputation with flag_missing_declared_key for declared key {col}"
    elif it in ("exact_duplicate_rows", "duplicate_primary_key", "near_duplicate_rows") or action == "deduplicate":
        if it == "near_duplicate_rows":
            action = "flag_near_duplicate_cluster"
            note = "Governance override: replaced deduplicate with flag_near_duplicate_cluster for near duplicates"
        elif it == "duplicate_primary_key":
            action = "flag_business_key_duplicate"
            note = "Governance override: replaced deduplicate with flag_business_key_duplicate for primary/business key duplicates"
        else:
            action = "flag_duplicate_candidates"
            note = "Governance override: replaced deduplicate with flag_duplicate_candidates for exact duplicates"
    elif action in ("fill_nulls_simple", "fill_or_drop") and (_is_business_key(col) or sub_type in ("identifier", "key")):
        action = "flag_missing_key"
        note = f"Governance override: replaced null imputation with flag_missing_key for key column {col}"

    # Enforce manual intent preservation if semantic confidence is low
    if was_manual and action != "review_manually" and sem_confidence < 0.85:
        action = "review_manually"
        note = f"Preserved manual review: semantic confidence {sem_confidence:.2f} < 0.85"

    # Determine auto_fixable
    from agent.etl_pipeline.action_registry import ACTION_REGISTRY
    meta = ACTION_REGISTRY.get(action) or {}
    is_risky = (
        action in ("drop_column", "exclude_column", "clip_outliers", "cap_outliers", "review_manually")
        or meta.get("invalid")
        or meta.get("auto_eligible") is False
    )
    auto_fixable = sug.get("auto_fixable")
    if auto_fixable is None:
        auto_fixable = (
            action not in ("review_manually", "noop")
            and not meta.get("invalid")
            and meta.get("auto_eligible") is not False
        )
    if is_risky:
        auto_fixable = False

    # Check LLM confidence if inferred suggestion
    from agent.etl_pipeline.rule_provenance import RuleProvenance
    is_approved = bool(
        sug.get("approved")
        or sug.get("approval_status") == "approved"
        or str(sug.get("status", "")).lower() == "approved"
        or sug.get("authority_source") == "HUMAN_APPROVED"
    )
    is_llm_inferred = sug.get("llm_recommendation") is not None and sug.get("provenance") != RuleProvenance.AUTO_DETECTED

    safe_remediation_actions = {
        "range_clip", "replace_sentinel_values", "parse_dates",
        "parse_dates_safe", "normalize_phone", "sanitize_email",
        "flag_duplicate_candidates", "trim", "trim_preserve_case",
        "lowercase", "uppercase", "regex_replace"
    }

    if is_approved:
        auto_fixable = True
        sug["authority_source"] = "HUMAN_APPROVED"
        sug["approval_status"] = "approved"
    elif is_llm_inferred and not (sug.get("auto_fixable") and action in safe_remediation_actions):
        # LLM suggestions are advisory unless explicitly approved or safe field remediation
        auto_fixable = False
        sug["authority_source"] = "LLM_ADVISORY"

    # ── PR 4 Objective Safety & Complete Authorization Gates ──────────────────
    from agent.etl_pipeline.remediation_router import route_finding
    routing = route_finding(sug, dataset=sug.get("dataset"))
    sug["routing_result"] = routing
    sug["lifecycle"] = routing.lifecycle
    sug["reason_codes"] = routing.reason_codes
    sug["recommendation_category"] = routing.recommendation_category

    cap = routing.capability
    # Complete Authorization Formula:
    # 1. Capability auto-fix eligible
    # 2. Risk level is strictly low
    # 3. Remediation certainty is strictly high
    # 4. Action is supported in capability registry
    # 5. Non-destructive (row_destructive is False)
    # 6. Generator is available
    # 7. Impact within action-specific limits (both % and row count)
    # 8. No key/relationship impact or business rule conflict
    generator_available = bool(cap.generator_ids and any(g for g in cap.generator_ids.values() if g != "noop"))
    action_supported = action in cap.supported_actions or (action in ("trim_preserve_case", "trim") and "trim" in cap.supported_actions)
    is_key_col = _is_business_key(col) or _is_name_field(col)

    if not is_approved:
        if (
            not routing.auto_fix_eligible
            or cap.risk_level != "low"
            or cap.remediation_certainty != "high"
            or cap.row_destructive
            or not generator_available
            or not action_supported
            or (is_key_col and action in ("drop_column", "exclude_column", "fill_or_drop", "review_manually"))
        ):
            if not (sug.get("auto_fixable") and action in safe_remediation_actions):
                auto_fixable = False

    # Populate required action params with safe Phase 1 defaults
    if action == "cast_type":
        params.setdefault("target_type", sug.get("target_type") or sug.get("target_dtype") or "string")
        params.setdefault("preserve_raw", True)
    elif action == "coerce_numeric":
        params.setdefault("target_type", sug.get("target_type") or sug.get("target_dtype") or "double")
        params.setdefault("preserve_raw", True)
    elif action in ("parse_dates", "parse_dates_safe"):
        params.setdefault("accepted_formats", ["yyyy-MM-dd", "yyyy/MM/dd", "MM/dd/yyyy", "dd-MM-yyyy"])
        params.setdefault("preserve_raw", True)
    elif action in ("range_clip", "clip_outliers"):
        params.setdefault("min_value", 0.0)
        params.setdefault("lower_bound", 0.0)
        params.setdefault("preserve_raw", True)
    elif action == "replace_sentinel_values":
        params.setdefault("sentinel_values", [-999.0, -9999.0, 999.0, 9999.0, 99999.0, 999999.0, "N/A", "NA", "NULL", "UNKNOWN", "UNSPECIFIED", "??", "---"])
        params.setdefault("preserve_raw", True)

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
        if str(col).strip().lower() in ("unknown", "none", "null", "all", "table", "dataset", "-", "[table-level]", "[row-level]"):
            col = "*"
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
            
            sid = make_step_id(ds, col, action)
            iid = make_issue_id(ds, col, it)
            datasets_steps[ds].append({
                "step_id": sid,
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
                "capability_auto_fix_eligible": True,
                "planning_gate_passed": True,
                "runtime_auto_fix_authorized": False,  # Dry-run validation required before runtime execution (PR 6)
                "validation_status": "not_started",
                "lifecycle": {
                    "recommendation_status": "standard_available",
                    "approval_status": "not_required",
                    "codegen_status": "generated",
                    "validation_status": "not_started",
                    "execution_status": "not_started",
                },
                "source_issue_type": it,
                "source_issue_types": [it] if it else [],
                "source_issue_ids": [iid],
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
                "capability_auto_fix_eligible": bool(sug.get("routing_result") and sug["routing_result"].auto_fix_eligible),
                "planning_gate_passed": False,
                "runtime_auto_fix_authorized": False,
                "validation_status": "not_started",
                "lifecycle": sug.get("lifecycle") or {
                    "recommendation_status": "ai_options_available",
                    "approval_status": "pending",
                    "codegen_status": "not_started",
                    "validation_status": "not_started",
                    "execution_status": "not_started",
                },
                "reason_codes": sug.get("reason_codes", []),
                "recommendation_category": sug.get("recommendation_category", "decision_required"),
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
            
    # Deduplicate using merge_steps preserving full issue provenance, then sort by priority
    from agent.etl_pipeline.planner import _ACTION_PRIORITY
    deduplicated_datasets_steps = {}
    for ds, steps in datasets_steps.items():
        merged_by_ident: Dict[str, Dict[str, Any]] = {}
        for step in steps:
            s_ident = step_identity(ds, step.get("column"), step.get("action"))
            if s_ident in merged_by_ident:
                merged_by_ident[s_ident] = merge_steps(merged_by_ident[s_ident], step)
            else:
                merged_by_ident[s_ident] = step

        sorted_steps = sorted(
            merged_by_ident.values(),
            key=lambda s: (_ACTION_PRIORITY.get(str(s.get("action") or ""), 80), str(s.get("column") or ""))
        )
        for idx, step in enumerate(sorted_steps):
            step["order"] = idx + 1
        deduplicated_datasets_steps[ds] = sorted_steps
            
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
