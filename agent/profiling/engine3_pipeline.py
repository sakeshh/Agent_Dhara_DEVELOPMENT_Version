"""
Engine 3.0 Post-Processing Pipeline for Agent Dhara.
This is the PRIMARY integration point that makes Engine 3.0 run in production.

Pipeline stages (in order):
  1. Semantic inference enrichment of column metadata
  2. PII / Privacy separation  (removes PII from DQ issue list)
  3. Finding deduplication  (prevents double-scoring same defect)
  4. GX result translation  (maps GX results to 7-state model)
  5. Dimension scoring  (replaces legacy flat-penalty formula)
  6. Component manifest attachment

Called from analyze_dataset_quality() in dq_checks.py after GX results are collected.
"""
from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from agent.feature_flags import get_flags
from agent.runtime_component_manifest import build_component_manifest, log_component_startup
from agent.profiling.gx_capability_registry import (
    PRIVACY_ISSUE_TYPES,
    is_privacy_issue,
    canonical_rule_id_for_issue,
)
from agent.profiling.rule_status import RuleStatus, RuleEvaluationResult, RuleCoverageSummary
from agent.profiling.dimension_scorer import (
    calculate_dimension_quality_scores,
    OverallQualityScore,
)

logger = logging.getLogger("agent.engine3_pipeline")

log_component_startup("Engine3Pipeline", "3.1.0")


# ── Issue-type → DQ dimension mapping ────────────────────────────────────────────
_ISSUE_TYPE_TO_DIMENSION: Dict[str, str] = {
    # Completeness
    "nulls":                           "completeness",
    "empty_string_values":             "completeness",
    "declared_key_null_violation":     "completeness",
    "declared_key_high_null_ratio":    "completeness",
    "suspected_key_high_null_ratio":   "completeness",
    "placeholder_detected":            "completeness",
    # Uniqueness
    "exact_duplicate_rows":            "uniqueness",
    "declared_key_violation":          "uniqueness",
    "candidate_key_violation":         "uniqueness",
    "duplicate_primary_key":           "uniqueness",
    "near_duplicate_rows":             "uniqueness",
    "duplicate_insensitive_values":    "uniqueness",
    # Validity
    "invalid_numeric":                 "validity",
    "mixed_scalar_types":              "validity",
    "invalid_date_format":             "validity",
    "mixed_date_formats":              "validity",
    "negative_values":                 "validity",
    "negative_values_in_non_negative_measure": "validity",
    "negative_values_review_required": "validity",
    "cross_field_non_negative":        "validity",
    "punctuation_only_value":          "validity",
    # Accuracy (real-world correctness, plausibility, anomaly detection)
    "implausible_age":                 "accuracy",
    "implausible_percentage":          "accuracy",
    "sentinel_numeric_value":          "accuracy",
    "suspicious_zero":                 "accuracy",
    "numeric_outliers_iqr":            "accuracy",
    "numeric_outliers_zscore":         "accuracy",
    "multivariate_outliers":           "accuracy",
    "out_of_range":                    "accuracy",
    # Conformity
    "invalid_email":                   "conformity",
    "invalid_phone":                   "conformity",
    "invalid_uuid":                    "conformity",
    "invalid_url":                     "conformity",
    "invalid_lookup_value":            "conformity",
    "custom_one_of":                   "conformity",
    "string_with_only_digits_in_text_column": "conformity",
    "dtype_drift":                     "conformity",
    # Consistency
    "formula_rule_violation":          "consistency",
    "custom_rule_violation":           "consistency",
    "functional_dependency_violation": "consistency",
    "conditional_not_null":            "consistency",
    # Referential integrity
    "orphan_foreign_key_rows":         "referential_integrity",
    "intra_dataset_orphan_fk":         "referential_integrity",
    # Timeliness
    "future_dates":                    "timeliness",
    "ancient_dates":                   "timeliness",
    "date_range_violation":            "timeliness",
    "weekend_date_anomaly":            "timeliness",
    "timezone_inconsistency":          "timeliness",
}

# Severity rank for mutually-exclusive row counting
_SEV_RANK: Dict[str, int] = {"high": 3, "medium": 2, "low": 1}

# GX expectation_type → DQ dimension (replicated from gx_result_translator for local use)
_GX_EXP_TO_DIM: Dict[str, str] = {
    "expect_column_values_to_not_be_null":           "completeness",
    "expect_column_values_to_be_unique":             "uniqueness",
    "expect_compound_columns_to_be_unique":          "uniqueness",
    "expect_column_values_to_be_in_set":             "conformity",
    "expect_column_values_to_match_regex":           "conformity",
    "expect_column_values_to_be_between":            "validity",
    "expect_column_value_lengths_to_be_between":     "validity",
    "expect_table_columns_to_match_ordered_list":    "conformity",
    "expect_table_columns_to_match_set":             "conformity",
    "expect_table_row_count_to_be_between":          "completeness",
    "expect_column_values_to_be_dateutil_parseable": "validity",
    "expect_column_values_to_be_in_type_list":       "validity",
    "expect_column_values_to_not_be_in_set":         "validity",
}


def separate_privacy_findings(
    issues: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Stage 1: Split the raw issue list into quality issues and privacy findings.
    Privacy findings MUST NOT be included in DQ scores, high-severity counts,
    release decisions, or remediation prioritization.

    Returns: (quality_issues, privacy_findings)
    """
    quality_issues: List[Dict[str, Any]] = []
    privacy_findings: List[Dict[str, Any]] = []

    for issue in issues:
        issue_type = issue.get("type") or issue.get("issue_type") or ""
        if is_privacy_issue(issue_type):
            pf = {**issue, "pipeline": "privacy", "score_family": "privacy"}
            privacy_findings.append(pf)
            logger.debug("[DQ3-Privacy] Separated privacy finding: type=%s col=%s",
                         issue_type, issue.get("column"))
        else:
            quality_issues.append(issue)

    logger.info("[DQ3-Privacy] Separated %d quality issues, %d privacy findings",
                len(quality_issues), len(privacy_findings))
    return quality_issues, privacy_findings


def deduplicate_quality_issues(
    issues: List[Dict[str, Any]],
    dataset_id: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Stage 2: Deduplicate issues using canonical (dataset, column, rule_id) identity.
    Merges evidence from duplicate findings. Returns (deduped_issues, diagnostics).
    """
    finding_map: Dict[str, Dict[str, Any]] = {}
    diagnostics: List[Dict[str, Any]] = []

    for issue in issues:
        col = issue.get("column") or "DATASET"
        issue_type = issue.get("type") or issue.get("issue_type") or "unknown"
        rule_id = canonical_rule_id_for_issue(issue_type)

        key = hashlib.sha256(
            f"{dataset_id}::{col}::{rule_id}".encode()
        ).hexdigest()[:16]

        issue["finding_id"] = key
        issue["canonical_rule_id"] = rule_id

        if key not in finding_map:
            finding_map[key] = issue
        else:
            # Merge evidence into existing finding
            existing = finding_map[key]
            if issue.get("count") and existing.get("count"):
                # Use max count to avoid over-counting same defect
                existing["count"] = max(existing["count"], issue["count"])
            existing_rows = existing.get("row_indexes") or []
            new_rows = issue.get("row_indexes") or []
            merged_rows = list(set(existing_rows + new_rows))[:100]
            existing["row_indexes"] = merged_rows
            existing_samples = existing.get("sample_values") or []
            new_samples = issue.get("sample_values") or []
            merged_samples = list(dict.fromkeys(existing_samples + new_samples))[:10]
            existing["sample_values"] = merged_samples
            diagnostics.append({
                "action": "merged_duplicate_finding",
                "finding_id": key,
                "rule_id": rule_id,
                "issue_type": issue_type,
            })
            logger.debug("[DQ3-Dedup] Merged duplicate: rule=%s col=%s key=%s",
                         rule_id, col, key)

    deduped = list(finding_map.values())
    logger.info("[DQ3-Dedup] %d issues → %d deduplicated (merged %d)",
                len(issues), len(deduped), len(diagnostics))
    return deduped, diagnostics


def build_rule_results_from_issues(
    quality_issues: List[Dict[str, Any]],
    gx_results_raw: Optional[List[Any]],
    dataset_id: str,
    n_rows: int,
) -> List[RuleEvaluationResult]:
    """
    Stage 3: Convert deduplicated DQ issues + translated GX results into
    canonical RuleEvaluationResult objects for the dimension scorer.
    """
    flags = get_flags()
    rule_results: List[RuleEvaluationResult] = []

    # Convert legacy DQ issues to RuleEvaluationResult
    for issue in quality_issues:
        issue_type = issue.get("type") or issue.get("issue_type") or "unknown"
        rule_id = issue.get("canonical_rule_id") or canonical_rule_id_for_issue(issue_type)
        dimension = _ISSUE_TYPE_TO_DIMENSION.get(issue_type, "validity")
        severity_str = str(issue.get("severity") or "medium").upper()
        count = int(issue.get("count") or 0)
        rows = issue.get("row_indexes") or []
        eval_units = max(1, n_rows)
        failed_units = min(count, eval_units)

        status = RuleStatus.FAIL if count > 0 else RuleStatus.PASS

        rr = RuleEvaluationResult(
            rule_id=rule_id,
            rule_version="3.1.0",
            status=status,
            dataset=dataset_id,
            column=issue.get("column"),
            dimension=dimension,
            severity=severity_str,
            confidence=1.0,
            evaluated_count=eval_units,
            failed_count=failed_units,
            failure_rate=round(failed_units / eval_units, 4),
            affected_rows=rows[:100],
            samples=(issue.get("sample_values") or [])[:25],
            error_message=None,
            evidence={
                "issue_type": issue_type,
                "gating": issue.get("gating", True),
                "provider": issue.get("provider", "dhara_native"),
            },
        )
        rule_results.append(rr)

    # Add translated GX results if available and flag enabled
    if flags.gx_result_translator and gx_results_raw:
        try:
            from agent.profiling.gx_result_translator import translate_gx_result_list
            import great_expectations as gx
            gx_ver = getattr(gx, "__version__", "unknown")
            translated = translate_gx_result_list(
                gx_results_raw,
                dataset_id=dataset_id,
                gx_provider_version=gx_ver,
            )
            # Deduplicate GX results against existing rule_results by rule_id+column
            existing_keys = {
                (r.rule_id, str(r.column or ""))
                for r in rule_results
            }
            for tr in translated:
                key = (tr.rule_id, str(tr.column or ""))
                if key not in existing_keys:
                    rule_results.append(tr)
                    existing_keys.add(key)
                    logger.debug("[DQ3-GX] Added translated GX result: rule=%s col=%s status=%s",
                                 tr.rule_id, tr.column, tr.status.value)
        except Exception as e:
            logger.warning("[DQ3-GX] GX result translation failed: %s", e)

    return rule_results


def run_engine3_pipeline(
    *,
    name: str,
    df: pd.DataFrame,
    raw_issues: List[Dict[str, Any]],
    gx_results_raw: Optional[List[Any]] = None,
    profile: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
    relationship_issues: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Master entry point for the Engine 3.0 post-processing pipeline.
    Called from analyze_dataset_quality() AFTER GX has run and produced raw issues.

    Returns a dict compatible with the existing analyze_dataset_quality() return schema,
    enriched with Engine 3.0 fields.
    """
    flags = get_flags()
    n = len(df)
    start_ts = time.monotonic()

    logger.info("[DQ3] Engine3Pipeline starting for dataset=%s rows=%d issues=%d",
                name, n, len(raw_issues))

    # ── Stage 1: Privacy separation ──────────────────────────────────────────
    if flags.privacy_score_separation:
        quality_issues, privacy_findings = separate_privacy_findings(raw_issues)
    else:
        quality_issues = raw_issues
        privacy_findings = []
        logger.debug("[DQ3] Privacy separation disabled (legacy mode)")

    # ── Stage 2: Finding deduplication ───────────────────────────────────────
    if flags.finding_deduplicator_v2:
        deduped_issues, dedup_diagnostics = deduplicate_quality_issues(quality_issues, name)
    else:
        deduped_issues = quality_issues
        dedup_diagnostics = []
        logger.debug("[DQ3] Finding deduplicator disabled (legacy mode)")

    # ── Stage 3: Build canonical rule results ─────────────────────────────────
    rule_results = build_rule_results_from_issues(
        deduped_issues,
        gx_results_raw=gx_results_raw,
        dataset_id=name,
        n_rows=n,
    )

    # ── Stage 4: Dimension scoring ────────────────────────────────────────────
    if flags.dimension_scoring_v2:
        has_missing_pk = any(
            (i.get("type") or "") in {"declared_key_null_violation", "declared_key_violation"}
            and int(i.get("count") or 0) > 0
            for i in deduped_issues
        )
        quality_score_obj: OverallQualityScore = calculate_dimension_quality_scores(
            rule_results,
            has_missing_primary_key=has_missing_pk,
            custom_weights=_extract_weight_config(thresholds),
        )
        dq_score = quality_score_obj.quality_score
        readiness = quality_score_obj.readiness
        gating_blockers = quality_score_obj.gating_blockers
        score_coverage = quality_score_obj.score_coverage
        score_methodology = "dimension_scorer_v3"

        dim_scores_dict = {
            dim_name: {
                "status": bd.status,
                "score": bd.score,
                "configured_weight": bd.configured_weight,
                "effective_weight": bd.effective_weight,
                "evaluated_rules_count": bd.evaluated_rules_count,
                "passed_rules_count": bd.passed_rules_count,
                "failed_rules_count": bd.failed_rules_count,
            }
            for dim_name, bd in quality_score_obj.dimension_scores.items()
        }
        assessment_confidence = quality_score_obj.assessment_confidence

        logger.info("[DQ3-Score] DimensionScorer: score=%s coverage=%.2f readiness=%s confidence=%.1f",
                    str(dq_score), score_coverage, readiness, assessment_confidence)
    else:
        # Fallback: use legacy score from the raw issues
        dq_score = None
        score_coverage = 0.0
        readiness = "UNKNOWN"
        gating_blockers = []
        score_methodology = "legacy_flat_penalty"
        dim_scores_dict = {}
        assessment_confidence = 100.0

    # ── Stage 5: Coverage summary ─────────────────────────────────────────────
    coverage = _build_coverage_summary(rule_results)

    # ── Stage 6: Component manifest & GX Metadata ────────────────────────────
    pipeline_time_ms = round((time.monotonic() - start_ts) * 1000, 1)
    component_manifest = build_component_manifest()

    gx_exec_summary = {
        "enabled": True,
        "adapter_version": "3.1.0",
        "gx_core_version": "0.18.0",
        "suites_compiled": 1 if len(rule_results) > 0 else 0,
        "expectations_planned": len(rule_results),
        "expectations_executed": len([r for r in rule_results if r.status in (RuleStatus.PASS, RuleStatus.FAIL)]),
        "passed": len([r for r in rule_results if r.status == RuleStatus.PASS]),
        "failed": len([r for r in rule_results if r.status == RuleStatus.FAIL]),
        "not_evaluated": len([r for r in rule_results if r.status == RuleStatus.NOT_EVALUATED]),
        "configuration_errors": len([r for r in rule_results if r.status == RuleStatus.CONFIGURATION_ERROR]),
        "execution_errors": len([r for r in rule_results if r.status == RuleStatus.EXECUTION_ERROR]),
    }

    return {
        # Core issue lists
        "quality_issues":   deduped_issues,
        "privacy_findings": privacy_findings,

        # Engine 3.0 scoring
        "dq_score_v3":           dq_score,
        "score_coverage":        score_coverage,
        "readiness":             readiness,
        "gating_blockers":       gating_blockers,
        "assessment_confidence": assessment_confidence,
        "score_methodology":     score_methodology,
        "dimension_breakdown_v3": dim_scores_dict,
        "gx_execution_summary":  gx_exec_summary,

        # Rule results (for downstream use)

        "rule_results": [r.model_dump() for r in rule_results],

        # Coverage
        "rule_coverage": coverage,

        # Deduplication diagnostics
        "deduplication_diagnostics": dedup_diagnostics,

        # Component manifest
        "component_manifest": component_manifest,

        # Execution timing
        "pipeline_time_ms": pipeline_time_ms,
    }


def _extract_weight_config(thresholds: Optional[Dict[str, Any]]) -> Optional[Dict[str, float]]:
    if not thresholds:
        return None
    score_cfg = thresholds.get("dq_score") or {}
    weight_cfg = score_cfg.get("dimension_weights")
    if isinstance(weight_cfg, dict):
        return {k: float(v) for k, v in weight_cfg.items()}
    return None


def _build_coverage_summary(rule_results: List[RuleEvaluationResult]) -> Dict[str, Any]:
    total = len(rule_results)
    if total == 0:
        return {
            "total_rules": 0,
            "evaluated": 0,
            "passed": 0,
            "failed": 0,
            "not_applicable": 0,
            "not_evaluated": 0,
            "execution_errors": 0,
            "configuration_errors": 0,
            "coverage_percentage": 100.0,
        }

    by_status: Dict[str, int] = {}
    for r in rule_results:
        s = r.status.value
        by_status[s] = by_status.get(s, 0) + 1

    evaluated = by_status.get("PASS", 0) + by_status.get("FAIL", 0)
    coverage_pct = round(100.0 * evaluated / total, 1) if total else 100.0

    return {
        "total_rules":         total,
        "evaluated":           evaluated,
        "passed":              by_status.get("PASS", 0),
        "failed":              by_status.get("FAIL", 0),
        "not_applicable":      by_status.get("NOT_APPLICABLE", 0),
        "not_evaluated":       by_status.get("NOT_EVALUATED", 0),
        "execution_errors":    by_status.get("EXECUTION_ERROR", 0),
        "configuration_errors": by_status.get("CONFIGURATION_ERROR", 0),
        "insufficient_evidence": by_status.get("INSUFFICIENT_EVIDENCE", 0),
        "coverage_percentage": coverage_pct,
    }
