"""build_etl_plan - Builds an ETL plan from assessment data."""

from __future__ import annotations
import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.etl_pipeline.models import (
    ETLPlan, DatasetPlan, PlanStep, CrossDatasetStep,
    ManualReviewItem, CleaningPlan, CoverageReport,
    ReconciliationReport, PlanMetadata, PlanStatus,
    GenerationMode, ReviewResult
)
from agent.assessment.models import (
    AssessmentResult, DatasetProfile, ColumnProfile,
    QualityIssue, IssueSeverity, IssueCategory,
    SemanticProfile, Relationship
)
from agent.core.interfaces import MemoryStore
from agent.logging_setup import get_logger, get_performance_logger

logger = get_logger("etl_pipeline.build_plan")


def build_etl_plan(
    assessment: Dict[str, Any],
    business_rules: Dict[str, Any] = None,
    engine: str = "sql",
    feature_flags: Optional[Dict[str, bool]] = None
) -> Any:
    """Build an ETL plan from assessment data.

    Converts assessment DQ issues into an executable ETL plan
    with steps, cross-dataset dependencies, and manual review items.
    """

    business_rules = business_rules or {}
    feature_flags = feature_flags or {}

    plan_id = str(uuid.uuid4().hex[:8])

    # Initialize plan structure
    plan: Dict[str, Any] = {
        "plan_id": plan_id,
        "assessment_signature": "",
        "datasets": {},
        "cross_dataset_steps": [],
        "manual_review": [],
        "cleaning_plan": None,
        "coverage_report": None,
        "reconciliation": None,
        "engine": engine,
        "generation_mode": "full",
        "status": "draft",
        "business_rules": business_rules,
        "blocked": False,
        "blocking_reasons": [],
        "metadata": {
            "created_at": datetime.utcnow().isoformat(),
            "engine": engine,
        },
    }

    # Process per-dataset plans
    total_issues = 0
    covered_issues = 0

    for ds_name, ds_data in assessment.get("datasets", {}).items():
        # Get columns info
        columns = ds_data.get("columns", {})

        # Get quality issues for this dataset
        dq_issues = _extract_dq_issues(assessment, ds_name)
        total_issues += len(dq_issues)

        if not dq_issues and columns:
            # No issues but has columns - generate a confirm step
            plan["datasets"][ds_name] = {
                "steps": [
                    {
                        "step_id": f"confirm_clean_{ds_name}",
                        "action": "noop",
                        "reason": "No quality issues detected",
                        "confidence": 1.0,
                        "metadata": {"source": "assessment"},
                    }
                ],
                "complexity": "low",
            }
            continue

        # Group issues by category
        issue_groups = _group_issues_by_category(dq_issues)

        steps: List[Dict[str, Any]] = []
        ds_covered = 0

        for category, issues in issue_groups.items():
            for issue in issues:
                step = _issue_to_step(issue, ds_name, columns, plan_id)
                if step and step["confidence"] >= 0.5:
                    steps.append(step)
                    covered_issues += 1

        # Add incremental step if needed
        if _should_use_incremental(ds_name, ds_data, business_rules):
            steps.append({
                "step_id": f"incremental_{ds_name}",
                "action": "incremental_read",
                "reason": "Enable incremental processing",
                "confidence": 0.9,
                "metadata": {"source": "incremental_config"},
            })

        # Add semantic enrichment if needed
        if _needs_semantic_enrichment(assessment, ds_name):
            steps.append({
                "step_id": f"semantic_enrich_{ds_name}",
                "action": "refine_semantic_type",
                "reason": "Enrich semantic types based on assessment",
                "confidence": 0.7,
                "metadata": {"source": "semantic"},
            })

        plan["datasets"][ds_name] = {
            "steps": steps,
            "complexity": _compute_complexity(steps, len(dq_issues)),
        }

    # Generate cross-dataset steps
    cross_steps = _generate_cross_dataset_steps(assessment, plan)
    plan["cross_dataset_steps"] = cross_steps

    # Generate manual review items
    manual_review = _generate_manual_review(assessment, plan)
    plan["manual_review"] = manual_review

    # Create cleaning plan
    cleaning_plan = _create_cleaning_plan(assessment, plan)
    plan["cleaning_plan"] = cleaning_plan

    # Create coverage report
    coverage_report = _create_coverage_report(assessment, plan)
    plan["coverage_report"] = coverage_report

    # Create reconciliation
    reconciliation = _create_reconciliation(assessment, plan)
    plan["reconciliation"] = reconciliation

    # Set status based on coverage
    coverage_pct = coverage_report["coverage_pct"] if coverage_report else 1.0
    if coverage_pct >= 0.8:
        plan["status"] = "approved"
    elif coverage_pct >= 0.5:
        plan["status"] = "in_review"
    else:
        plan["status"] = "blocked"

    # Add assessment signature
    assessment_result = assessment.get("assessment_result")
    if assessment_result and isinstance(assessment_result, dict):
        plan["assessment_signature"] = assessment_result.get("signature", "")

    logger.info(f"Plan built: {plan_id}, coverage: {coverage_pct:.1f}%, issues: {total_issues}")

    return plan


def _extract_dq_issues(assessment: Dict[str, Any], ds_name: str) -> Any:
    """Extract quality issues for a dataset from assessment."""

    # From assessment.data_quality_issues
    dq_issues = (assessment.get("data_quality_issues") or {}).get("datasets") or {}

    for dsk, block in dq_issues.items():
        # Match dataset name variations
        ds_clean = ds_name.replace("dbo.", "").strip().lower()
        dsk_match = dsk.replace("dbo.", "").strip().lower()
        if dsk == ds_name or dsk == ds_clean or dsk_match == ds_clean or dsk.replace("dbo.", "") == ds_name.replace("dbo.", ""):
            issues = block.get("issues") or []
            if issues:
                return issues

    # From assessment.datasets.<ds>.quality.issues
    ds_info = assessment.get("datasets", {}).get(ds_name)
    if ds_info:
        q = ds_info.get("quality") or {}
        issues = q.get("issues") or []
        if issues:
            return issues

    return []


def _group_issues_by_category(issues: Any) -> Any:
    """Group issues by category and severity."""

    groups: Dict[str, List[Dict[str, Any]]] = {}

    for issue in issues:
        itype = str(issue.get("issue_type") or "").lower()
        if itype not in groups:
            groups[itype] = []
        groups[itype].append(issue)

    # Sort within groups by severity
    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    for key in groups:
        groups[key].sort(key=lambda i: severity_order.get(str(i.get("severity") or "low").lower(), 3))

    return groups


def _issue_to_step(issue: Any, ds_name: str, columns: Any, plan_id: str) -> Any:
    """Convert a DQ issue to an ETL plan step."""

    col = issue.get("column")
    itype = str(issue.get("issue_type") or "").lower()
    sev = str(issue.get("severity") or "low").lower()
    msg = issue.get("message") or ""

    if not col:
        return None

    # Map issue type to action
    action_map: Dict[str, str] = {
        # Null handling
        "missing_values": "fill_nulls_simple",
        "null_exceeded": "fill_nulls_simple",
        "invalid_email": "sanitize_email",
        "invalid_phone": "normalize_phone",
        # Format
        "format_mismatch": "lowercase",
        "date_format_inconsistency": "parse_dates",
        "email_format_inconsistency": "sanitize_email",
        # Outliers
        "numeric_outliers_iqr": "range_clip",
        "negative_values": "range_clip",
        "implausible_age": "range_clip",
        "outlier": "range_clip",
        # Business rules
        "business_rule": "replace_values",
        "replace_sentinel": "replace_sentinel_values",
        "placeholder_detected": "replace_sentinel_values",
        # Type casting
        "type_mismatch": "cast_type",
        "coerce_numeric": "coerce_numeric",
        # Date handling
        "impossible_date": "parse_dates",
        "date_format": "parse_dates",
        # Validation
        "invalid_value": "validate_value",
        "missing_value": "fill_nulls_simple",
    }

    action = action_map.get(itype)
    if not action:
        # Default based on issue type
        if "null" in itype:
            action = "fill_nulls_simple"
        elif "price" in ds_name.lower() and "negative" in itype:
            action = "range_clip"
        else:
            action = "review_manually"

    # Get params based on action and issue characteristics
    params: Dict[str, Any] = {
        "issue_id": issue.get("id") or str(uuid.uuid4().hex[:4]),
        "category": itype,
        "severity": sev,
        "column": col,
        "dataset": ds_name,
    }

    # Add action-specific params
    if action == "fill_nulls_simple":
        # Get recommended fill value from evidence if available
        evidence = issue.get("evidence") or {}
        fill_val = evidence.get("recommended_fill")
        if fill_val:
            params["fill_value"] = fill_val
        else:
            # Default strategy based on dtype
            dtype = columns.get(col, {}).get("dtype", "string")
            if "int" in str(dtype).lower():
                params["fill_strategy"] = "mean"
            elif "float" in str(dtype).lower():
                params["fill_strategy"] = "mean"
            else:
                params["fill_strategy"] = "unknown"

    elif action == "range_clip":
        params["lower_bound"] = 0.0
        params["min_value"] = 0.0
        # Price columns get lower bound 0
        if any(k in ds_name.lower() for k in ("price", "cost", "revenue")):
            params["lower_bound"] = 0.0
            params["min_value"] = 0.0

    elif action == "sanitize_email":
        params["target_format"] = "canonical"

    elif action == "normalize_phone":
        params["strategy"] = "digits"

    elif action == "cast_type":
        # Infer target type from semantic type
        semantic = columns.get(col, {}).get("semantic_type", "")
        if semantic and "date" in semantic.lower():
            params["target_type"] = "DATE"
        elif semantic and "id" in semantic.lower():
            params["target_type"] = "BIGINT"
        elif semantic and "float" in semantic.lower():
            params["target_type"] = "DECIMAL(18, 2)"
        else:
            params["target_type"] = "NVARCHAR(256)"

    elif action == "coerce_numeric":
        # Check if column is price/rate/cost etc.
        col_lower = col.lower()
        is_price = any(k in col_lower for k in ("price", "cost", "amount", "rate", "total", "balance", "tax"))
        params["cast_target"] = "DECIMAL(18, 2)" if is_price else "BIGINT"

    elif action == "parse_dates":
        params["filter_impossible"] = True

    elif action == "review_manually":
        # Generate manual review item
        manual_item: Dict[str, Any] = {
            "id": f"review_{issue.get('id')}",
            "type": "blocker",
            "dataset": ds_name,
            "column": col,
            "issue_type": itype,
            "severity": sev,
            "message": msg,
            "guidance": f"Review {itype} on {col}",
            "resolution_options": [
                {"label": "Auto-fix", "action": "auto"},
                {"label": "Manual fix", "action": "manual"},
                {"label": "Skip", "action": "skip"},
            ],
        }
        plan["manual_review"].append(manual_item)

    step: Dict[str, Any] = {
        "step_id": f"fix_{itype}_{ds_name}_{col.strip('[]').lower()}",
        "action": action,
        "reason": msg or f"Fix {itype} on {col}",
        "confidence": 0.8,
        "params": params,
        "source_issue_type": itype,
        "source_issue_id": issue.get("id"),
    }

    return step


def _should_use_incremental(ds_name: str, ds_data: Any, business_rules: Any) -> Any:
    """Determine if incremental processing should be used."""

    # Check data size - incremental for large datasets
    row_count = ds_data.get("row_count", 0) if ds_data else 0
    if row_count > 10000:
        return True

    # Check for watermark column
    columns = ds_data.get("columns", {}) if ds_data else {}
    if any(
        c.get("name") == "watermark" or c.get("name") == "last_modified"
        for c in columns.values()
    ):
        return True

    # Check business rules config
    if business_rules.get("enable_incremental", False):
        return True

    return False


def _needs_semantic_enrichment(assessment: Any, ds_name: str) -> Any:
    """Determine if semantic enrichment is needed."""

    # Check for many 'unknown' semantic types
    ds_info = assessment.get("datasets", {}).get(ds_name)
    if ds_info:
        columns = ds_info.get("columns", {})
        unknown_count = sum(1 for c in columns.values() if str(c.get("semantic_type", "")) == "unknown")
        total_cols = len(columns)

        if total_cols > 0 and unknown_count / total_cols > 0.3:
            return True

    return False


def _compute_complexity(steps: Any, issue_count: Any) -> Any:
    """Compute complexity rating."""

    if not steps or issue_count <= 1:
        return "low"
    elif issue_count <= 5 and len(steps) <= 3:
        return "medium"
    else:
        return "high"


def _generate_cross_dataset_steps(assessment: Any, plan: Any) -> Any:
    """Generate cross-dataset steps from relationships."""

    steps: List[Dict[str, Any]] = []

    # Get relationships from assessment
    relationships = assessment.get("relationships") or []

    for rel in relationships:
        rel_str = str(rel).lower()
        # Skip if already in plan steps
        step_ids = {s.get("step_id", "") for s in plan.get("datasets", {}).values() if s.get("steps")}

        step = {
            "step_id": f"validate_rel_{rel.get('id', uuid.uuid4().hex[:4])}",
            "action": "validate_relationship",
            "datasets": [rel.get("parent_dataset"), rel.get("child_dataset")],
            "join_keys": {
                rel.get("parent_dataset", ""): rel.get("parent_column", ""),
                rel.get("child_dataset", ""): rel.get("child_column", ""),
            },
            "order": 1,
            "params": {
                "relationship_id": rel.get("id", ""),
                "confidence": rel.get("confidence", 0.5),
            },
        }
        steps.append(step)

    # Add cross-dataset aggregation if multiple datasets
    datasets = list(plan.get("datasets", {}).keys())
    if len(datasets) > 1:
        steps.append({
            "step_id": "cross_dataset_aggregation",
            "action": "cross_dataset_agg",
            "datasets": datasets,
            "order": 1,
            "params": {"type": "cross_dataset_summary"},
        })

    return steps


def _generate_manual_review(assessment: Any, plan: Any) -> Any:
    """Generate manual review items."""

    items: List[Dict[str, Any]] = []

    # High-severity issues need review
    dq_issues = _extract_dq_issues(assessment, "")

    for issue in dq_issues:
        sev = str(issue.get("severity") or "low").lower()
        if sev in ["critical", "high"]:
            col = issue.get("column")
            itype = str(issue.get("issue_type") or "").lower()
            msg = issue.get("message") or ""

            item: Dict[str, Any] = {
                "id": f"review_{issue.get('id')}",
                "type": "blocker",
                "dataset": issue.get("dataset_name", ""),
                "column": col,
                "issue_type": itype,
                "severity": sev,
                "message": msg,
                "guidance": f"Review {itype} on {col}",
                "resolution_options": [
                    {"label": "Apply", "action": "apply"},
                    {"label": "Reject", "action": "reject"},
                    {"label": "Modify", "action": "modify"},
                ],
            }
            items.append(item)

    # Add review for destructive operations
    for ds_name, ds_data in plan.get("datasets", {}).items():
        for step in ds_data.get("steps", []):
            if step.get("action") in ["drop_table", "drop_column", "purge_data"]:
                item: Dict[str, Any] = {
                    "id": f"destructive_{step.get('step_id')}",
                    "type": "destructive_operation",
                    "dataset": ds_name,
                    "column": step.get("column"),
                    "issue_type": "destructive_operation",
                    "severity": "high",
                    "message": f"Step {step.get('step_id')} performs destructive operation",
                    "guidance": "Review before executing destructive operation",
                    "resolution_options": [
                        {"label": "Approve", "action": "approve"},
                        {"label": "Reject", "action": "reject"},
                    ],
                }
                items.append(item)

    return items


def _create_cleaning_plan(assessment: Any, plan: Any) -> Any:
    """Create cleaning plan from assessment."""

    if not assessment.get("data_quality_issues"):
        return None

    cleaning_mode = "cleanse_with_audit"
    business_keys: Dict[str, List[str]] = {}
    duplicate_policies: Dict[str, Dict[str, Any]] = {}
    output_contracts: Dict[str, Any] = {}

    for ds_name, ds_data in assessment.get("datasets", {}).items():
        business_keys[ds_name] = []
        duplicate_policies[ds_name] = {}

        dq_issues = _extract_dq_issues(assessment, ds_name)

        for issue in dq_issues:
            col = issue.get("column")
            if col:
                # Collect business keys for dedup
                itype = str(issue.get("issue_type") or "").lower()
                if itype in ["duplicate", "exact_duplicate_rows", "business_key_duplicate"]:
                    if col not in business_keys[ds_name]:
                        business_keys[ds_name].append(col)

                # Duplicate handling policy
                if itype == "duplicate":
                    duplicate_policies[ds_name][col] = {
                        "policy": "deduplicate",
                        "strategy": "keep_last",
                    }

                # Format standardization
                if itype in ["invalid_email", "format_mismatch", "date_format_inconsistency"] and col:
                    output_contracts[f"{ds_name}_{col}_format"] = {
                        "target_format": "canonical",
                        "strict": True,
                    }

    return {
        "cleaning_mode": cleaning_mode,
        "business_keys": business_keys,
        "duplicate_policies": duplicate_policies,
        "output_contracts": output_contracts,
    }


def _create_coverage_report(assessment: Any, plan: Any) -> Any:
    """Create coverage report."""

    total_issues = 0
    covered_issues = 0
    per_dataset: Dict[str, Any] = {}
    unmatched_issues: List[Dict[str, Any]] = []

    for ds_name, ds_plan in plan.get("datasets", {}).items():
        dq_issues = _extract_dq_issues(assessment, ds_name)
        ds_total = len(dq_issues)
        ds_covered = len([s for s in ds_plan.get("steps", []) if s.get("evidence_id")])

        total_issues += ds_total
        covered_issues += ds_covered

        # Find unmatched issues
        matched_step_ids = {s.get("step_id") for s in ds_plan.get("steps", []) if s.get("evidence_id")}
        for issue in dq_issues:
            if issue.get("id") not in matched_step_ids:
                unmatched_issues.append({
                    "issue_id": issue.get("id"),
                    "category": issue.get("issue_type"),
                    "severity": issue.get("severity"),
                    "column": issue.get("column"),
                    "dataset": ds_name,
                })

        per_dataset[ds_name] = {
            "total_issues": ds_total,
            "covered_issues": ds_covered,
            "coverage_pct": ds_covered / ds_total if ds_total > 0 else 1.0,
            "step_count": len(ds_plan.get("steps", [])),
        }

    overall_pct = covered_issues / total_issues if total_issues > 0 else 1.0

    return {
        "total_issues": total_issues,
        "covered_issues": covered_issues,
        "coverage_pct": overall_pct,
        "per_dataset": per_dataset,
        "unmatched_issues": unmatched_issues,
    }


def _create_reconciliation(assessment: Any, plan: Any) -> Any:
    """Reconcile assessment issues with plan steps."""
    
    total_issues = 0
    total_steps = 0
    covered_issues = 0
    per_dataset: Dict[str, Any] = {}
    unmatched_issues: List[Dict[str, Any]] = []

    for ds_name, ds_plan in plan.get("datasets", {}).items():
        dq_issues = _extract_dq_issues(assessment, ds_name)
        ds_total = len(dq_issues)
        ds_steps = len(ds_plan.get("steps", []))

        total_issues += ds_total
        total_steps += ds_steps

        # Find unmatched issues
        matched_issue_ids = {s.get("source_issue_id") for s in ds_plan.get("steps", []) if s.get("source_issue_id")}
        for issue in dq_issues:
            if issue.get("id") not in matched_issue_ids:
                unmatched_issues.append({
                    "issue_id": issue.get("id"),
                    "category": issue.get("issue_type"),
                    "severity": issue.get("severity"),
                    "column": issue.get("column"),
                    "dataset": ds_name,
                })
            else:
                covered_issues += 1

        per_dataset[ds_name] = {
            "total_issues": ds_total,
            "total_steps": ds_steps,
            "coverage_pct": len([s for s in ds_plan.get("steps", []) if s.get("source_issue_id")]) / ds_steps if ds_steps > 0 else 1.0,
        }

    overall_pct = covered_issues / total_issues if total_issues > 0 else 1.0

    return {
        "total_issues": total_issues,
        "total_steps": total_steps,
        "coverage_pct": overall_pct,
        "per_dataset": per_dataset,
        "unmatched_issues": unmatched_issues,
    }