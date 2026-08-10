"""
Plan step coverage report: cross-references assessment issues against plan steps.
"""
from typing import Any, Dict, List

RESOLVING_ACTIONS = {
    "whitespace": {"trim", "regex_replace"},
    "nulls": {"fill_or_drop", "fill_nulls_simple", "cast_type"},
    "null_values": {"fill_or_drop", "fill_nulls_simple", "cast_type"},
    "high_null_percentage": {"fill_or_drop", "fill_nulls_simple", "cast_type", "drop_column", "exclude_column"},
    "null_rate_drift": {"fill_or_drop", "fill_nulls_simple", "cast_type", "trim", "lowercase", "review_manually", "noop"},
    "invalid_date_format": {"parse_dates", "parse_dates_safe", "nullify_dummy_dates", "nullify_future_dates"},
    "ancient_dates": {"parse_dates", "parse_dates_safe", "nullify_dummy_dates"},
    "future_dates": {"nullify_future_dates", "parse_dates", "parse_dates_safe"},
    "dummy_dates": {"nullify_dummy_dates", "parse_dates", "parse_dates_safe"},
    "invalid_email": {"sanitize_email", "regex_replace"},
    "pii_email": {"sanitize_email", "regex_replace"},
    "proactive_sanitize_email": {"sanitize_email", "regex_replace"},
    "invalid_phone": {"normalize_phone", "hash_phone", "mask_phone", "regex_replace"},
    "pii_phone": {"normalize_phone", "hash_phone", "mask_phone", "regex_replace"},
    "invalid_numeric": {"coerce_numeric", "cast_type", "fill_nulls_flag"},
    "invalid_numeric_values": {"fill_nulls_flag", "coerce_numeric", "cast_type"},
    "mixed_types": {"coerce_numeric", "cast_type"},
    "integer_stored_as_float": {"cast_type"},
    "negative_values": {"clip_or_flag", "flag_outliers", "clip_outliers", "cap_outliers", "range_clip"},
    "custom_range": {"clip_or_flag", "flag_outliers", "clip_outliers", "cap_outliers", "range_clip"},
    "suspicious_zero": {"zero_to_null", "replace_sentinel_values"},
    "zero_values": {"zero_to_null", "replace_sentinel_values"},
    "duplicate_rows": {"deduplicate"},
    "duplicate": {"deduplicate"},
    "duplicates": {"deduplicate"},
    "near_duplicate_rows": {"deduplicate"},
    "duplicate_primary_key": {"deduplicate", "review_manually", "validate_referential_integrity_or_stage"},
    "business_key_duplicate": {"deduplicate", "review_manually", "validate_referential_integrity_or_stage"},
    "duplicate_insensitive_values": {"deduplicate", "lowercase"},
    "sentinel_numeric_value": {"replace_sentinel_values", "zero_to_null"},
    "placeholder_detected": {"nullify_punctuation", "replace_sentinel_values", "zero_to_null", "fill_or_drop"},
    "punctuation_only_value": {"nullify_punctuation"},
    "string_with_only_digits_in_text_column": {"flag_domain_violation", "cast_type", "review_manually"},
    "binary_like_column": {"standardize_boolean"},
    "boolean_inconsistency": {"standardize_boolean"},
    "casing_inconsistency": {"lowercase", "uppercase", "capitalize", "titlecase", "trim"},
    "mixed_case": {"lowercase", "uppercase", "capitalize", "titlecase", "trim"},
    "mixed_casing": {"lowercase", "uppercase", "capitalize", "titlecase", "trim"},
    "inconsistent_case": {"lowercase", "uppercase", "capitalize", "titlecase", "trim"},
    "inconsistent_casing": {"lowercase", "uppercase", "capitalize", "titlecase", "trim"},
    "outliers": {"flag_outliers", "clip_outliers", "cap_outliers", "range_clip", "clip_or_flag"},
    "outlier": {"flag_outliers", "clip_outliers", "cap_outliers", "range_clip", "clip_or_flag"},
    "outlier_detected": {"flag_outliers", "clip_outliers", "cap_outliers", "range_clip", "clip_or_flag"},
    "numeric_outliers": {"flag_outliers", "clip_outliers", "cap_outliers", "range_clip", "clip_or_flag"},
    "numeric_outliers_iqr": {"flag_outliers", "clip_outliers", "cap_outliers", "range_clip", "clip_or_flag"},
    "numeric_outliers_zscore": {"flag_outliers", "clip_outliers", "cap_outliers", "range_clip", "clip_or_flag"},
    "schema_drift": {"cast_type", "trim", "lowercase", "drop_column", "exclude_column", "review_manually", "validate_referential_integrity_or_stage", "noop"},
    "schema_mismatch": {"cast_type", "review_manually", "validate_referential_integrity_or_stage"},
    "orphan_foreign_keys": {"validate_referential_integrity_or_stage", "review_manually"},
    "referential_integrity_violation": {"validate_referential_integrity_or_stage", "review_manually"},
}

def build_coverage_report(assessment: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compares the quality issues identified in the assessment against the steps
    and manual review/blocked items defined in the ETL plan, validating action-level coverage.
    """
    import re
    covered = []
    uncovered = []
    
    def _clean_ds(ds):
        d = str(ds or "").strip().lower()
        if not d or d == "global":
            return "_global"
        for ext in (".csv", ".tsv", ".json", ".parquet", ".xml", ".xlsx", ".xls", ".jsonl"):
            if d.endswith(ext):
                d = d[:-len(ext)]
                break
        return re.sub(r"[^a-z0-9_]", "", d)

    def _clean_col(col):
        c = str(col or "").strip()
        return c.lower() if c else "*"

    # 1. Collect all columns with steps in the plan mapped to their actions
    planned_steps_map = {}
    ds_has_steps = set()
    datasets = (plan or {}).get("datasets") or {}
    for ds_name, ds_block in datasets.items():
        ds_clean = _clean_ds(ds_name)
        steps = ds_block.get("steps") or []
        if steps:
            ds_has_steps.add(ds_clean)
        for step in steps:
            col = step.get("column")
            act = step.get("action")
            if act:
                key = (ds_clean, _clean_col(col))
                planned_steps_map.setdefault(key, set()).add(str(act).lower())
                
    # 2. Collect columns in manual review, resolved_manual_review, blocked, non_fixable
    mr_cols = set()
    manual_review = (plan or {}).get("manual_review") or []
    for item in manual_review:
        if isinstance(item, dict):
            mr_cols.add((_clean_ds(item.get("dataset")), _clean_col(item.get("column"))))
            # Also add by issue_type key
            it = item.get("issue_type") or item.get("type")
            if it:
                mr_cols.add((_clean_ds(item.get("dataset")), str(it).lower()))
            
    resolved_manual = (plan or {}).get("resolved_manual_review") or []
    for item in resolved_manual:
        if isinstance(item, dict):
            mr_cols.add((_clean_ds(item.get("dataset")), _clean_col(item.get("column"))))
            it = item.get("issue_type") or item.get("type")
            if it:
                mr_cols.add((_clean_ds(item.get("dataset")), str(it).lower()))
            
    blocked_cols = set()
    blocked = (plan or {}).get("blocked") or []
    for item in blocked:
        if isinstance(item, dict):
            blocked_cols.add((_clean_ds(item.get("dataset")), _clean_col(item.get("column"))))

    nf_cols = set()
    non_fixable = (plan or {}).get("non_fixable") or []
    for item in non_fixable:
        if isinstance(item, dict):
            nf_cols.add((_clean_ds(item.get("dataset")), _clean_col(item.get("column"))))
            
    # 3. Iterate over quality issues in the assessment
    ass_datasets = (assessment or {}).get("datasets") or {}
    total_issues = 0
    
    for ds_name, ds_meta in ass_datasets.items():
        ds_clean = _clean_ds(ds_name)
        dq_issues = list((ds_meta.get("quality") or {}).get("issues") or [])
        legacy_issues = (assessment or {}).get("data_quality_issues", {}).get("datasets", {}).get(ds_name, {}).get("issues", [])
        if legacy_issues:
            dq_issues.extend(legacy_issues)
            
        for issue in dq_issues:
            if not isinstance(issue, dict):
                continue
            col = issue.get("column")
            it = issue.get("issue_type") or issue.get("type") or issue.get("issue") or issue.get("kind") or "unknown"
            total_issues += 1
            
            issue_detail = {
                "dataset": ds_name,
                "column": col or "",
                "issue_type": it,
                "message": issue.get("message"),
                "severity": issue.get("severity") or "MEDIUM",
            }
            
            cleaned_col = _clean_col(col)
            issue_key = (ds_clean, cleaned_col)
            it_key = (ds_clean, str(it).lower())
            
            is_covered = (
                issue_key in mr_cols or 
                issue_key in blocked_cols or 
                issue_key in nf_cols or
                it_key in mr_cols
            )
            
            if not is_covered:
                actions_for_col = planned_steps_map.get(issue_key, set())
                it_str = str(it or "").lower()
                resolving = RESOLVING_ACTIONS.get(it) or RESOLVING_ACTIONS.get(it_str) or set()

                if cleaned_col == "*" and ds_clean in ds_has_steps:
                    # Dataset-level issue (e.g. schema_drift, null_rate_drift) on a dataset that has steps planned
                    is_covered = True
                elif it_str.startswith(("profile_heuristic", "heuristic", "profile", "column_profile")):
                    if actions_for_col or ds_clean in ds_has_steps:
                        is_covered = True
                elif actions_for_col & resolving:
                    is_covered = True
                elif not resolving and actions_for_col:
                    is_covered = True
            
            if is_covered:
                covered.append(issue_detail)
            else:
                uncovered.append(issue_detail)
                
    coverage_pct = 100.0
    if total_issues > 0:
        coverage_pct = round((len(covered) / total_issues) * 100, 2)
        
    return {
        "covered": covered,
        "uncovered": uncovered,
        "coverage_pct": coverage_pct
    }

