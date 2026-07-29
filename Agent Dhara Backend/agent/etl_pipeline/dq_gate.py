"""
DQ Gate: Calculates dataset-level data quality scores and determines if they pass the Phase 2 transformations threshold.
"""
from __future__ import annotations

from typing import Any, Dict

def calculate_dataset_dq_score(assessment: Dict[str, Any], ds_name: str) -> Dict[str, Any]:
    """
    Calculates a DQ Score (0-100) using weighted metrics:
    - Null Rate: 30%
    - Type Mismatches: 30%
    - Duplicates: 20%
    - Outliers: 20%
    """
    ds_info = (assessment or {}).get("datasets", {}).get(ds_name, {})
    columns = ds_info.get("columns") or {}
    
    # 1. Null Score (30%)
    null_score = 100.0
    if columns:
        null_pcts = []
        for col in columns.values():
            if isinstance(col, dict):
                null_pcts.append(col.get("null_percentage") or col.get("null_pct") or 0.0)
        if null_pcts:
            avg_null = sum(null_pcts) / len(null_pcts)
            # Quadratic penalty: high null rates are penalized more aggressively
            null_score = max(0.0, 100.0 * ((1.0 - avg_null) ** 2))

    # 2. Type Mismatch, Duplicate, Outlier Score Calculation (Row-Weighted & Deduplicated)
    dq_issues = list((ds_info.get("quality") or {}).get("issues") or [])

    # Deduplicate issues based on (column, type, message)
    seen_issues = set()
    dedup_dq_issues = []
    for issue in dq_issues:
        k = (str(issue.get("column") or ""), str(issue.get("type") or issue.get("issue_type") or "").strip().lower(), str(issue.get("message") or ""))
        if k not in seen_issues:
            seen_issues.add(k)
            dedup_dq_issues.append(issue)
    dq_issues = dedup_dq_issues

    row_count = max(1, int(ds_info.get("row_count") or 1))

    def affected_rows(issue_type_filter):
        # Sum unexpected counts or default to 1 row per unique issue type if missing/0
        return sum(max(1, int(i.get("count") or 0)) for i in dq_issues if issue_type_filter(str(i.get("type") or i.get("issue_type") or "").strip().lower()))

    # Type Score (30%)
    type_affected = affected_rows(lambda t: t in ("type_mismatch", "invalid_date_format", "invalid_email", "invalid_phone", "invalid_uuid", "invalid_url", "mixed_scalar_types", "invalid_numeric", "parse_dates"))
    type_score = max(0.0, 100.0 * (1.0 - type_affected / row_count))

    # Duplicate Score (20%)
    dup_affected = affected_rows(lambda t: "duplicate" in t or "dup" in t)
    # Plus business key duplicate confirmations if they ran
    llm_ds_hints = ds_info.get("llm_hints") or {}
    dup_info = llm_ds_hints.get("business_key_confirmation") or {}
    if isinstance(dup_info, dict):
        dup_affected += int(dup_info.get("business_key_duplicate_count") or 0)
    dup_score = max(0.0, 100.0 * (1.0 - min(row_count, dup_affected) / row_count))

    # Outlier Score (20%)
    outlier_affected = affected_rows(lambda t: "outlier" in t or t in ("range_clip", "clip_or_flag", "flag_outliers", "clip_outliers", "cap_outliers"))
    outlier_score = max(0.0, 100.0 * (1.0 - outlier_affected / row_count))

    # Weighted DQ Score
    dq_score = (0.30 * null_score) + (0.30 * type_score) + (0.20 * dup_score) + (0.20 * outlier_score)
    if null_score < 10.0:
        dq_score = max(0.0, dq_score - 15.0)
    
    return {
        "score": round(dq_score, 2),
        "details": {
            "null_score": round(null_score, 2),
            "type_score": round(type_score, 2),
            "duplicate_score": round(dup_score, 2),
            "outlier_score": round(outlier_score, 2)
        }
    }

def check_dq_gate(
    assessment: Dict[str, Any],
    ds_name: str,
    threshold: float = 70.0,
    force_unlock: bool = False,
    sem_schema: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Determines if a dataset passes the DQ gate for Phase 2 transformations."""
    res = calculate_dataset_dq_score(assessment, ds_name)
    score = res["score"]
    
    # Check for high-PII columns belonging to this dataset in semantic schema
    has_high_pii = False
    if sem_schema:
        for key, desc in sem_schema.items():
            if key.startswith(f"{ds_name}.") and isinstance(desc, dict) and desc.get("pii_level") == "high":
                has_high_pii = True
                break
                
    effective_threshold = min(threshold + 15.0, 95.0) if has_high_pii else threshold
    passed = score >= effective_threshold or force_unlock
    
    return {
        "passed": passed,
        "score": score,
        "threshold": effective_threshold,
        "force_unlocked": force_unlock,
        "details": res["details"],
        "has_high_pii": has_high_pii,
    }


def evaluate_dq_gate(
    assessment: Dict[str, Any],
    rules: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Evaluates the data quality gate for all datasets in the assessment.
    Returns:
        Dict: {
            "passed": bool,
            "blocking_issues": List[Dict[str, Any]],
            "warnings": List[Dict[str, Any]]
        }
    """
    datasets = (assessment or {}).get("datasets") or {}
    rules = rules or {}
    threshold = float(rules.get("dq_threshold") or 70.0)
    force_unlock_list = list(rules.get("force_unlock") or [])
    sem_schema = rules.get("semantic_overrides") or {}

    blocking_issues = []
    warnings = []
    all_passed = True

    for ds_name in datasets.keys():
        force = ds_name in force_unlock_list
        res = check_dq_gate(assessment, ds_name, threshold, force, sem_schema)
        score = res.get("score", 100.0)
        
        # Calculate if we passed without force_unlock
        passed_without_force = score >= res.get("threshold", threshold)
        
        if not res.get("passed", True):
            all_passed = False
            # Find the lowest quality score details to explain why it blocked
            blocking_issues.append({
                "dataset": ds_name,
                "score": score,
                "threshold": res.get("threshold", threshold),
                "reason": f"Data quality score ({score}%) is below the required threshold ({res.get('threshold', threshold)}%).",
                "details": res.get("details", {})
            })
        elif not passed_without_force and force:
            warnings.append({
                "dataset": ds_name,
                "score": score,
                "threshold": res.get("threshold", threshold),
                "reason": f"Dataset passed quality gate via force_unlock override. Raw score: {score}%."
            })
            
    return {
        "passed": all_passed,
        "blocking_issues": blocking_issues,
        "warnings": warnings,
    }


def check_dq_gate_post_cleanse(
    post_cleanse_assessment: Dict[str, Any],
    ds_name: str,
    threshold: float = 70.0,
    force_unlock: bool = False,
    sem_schema: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Determines if a dataset passes the DQ gate for Phase 2 using post-cleanse metrics."""
    return check_dq_gate(
        post_cleanse_assessment,
        ds_name,
        threshold=threshold,
        force_unlock=force_unlock,
        sem_schema=sem_schema
    )

