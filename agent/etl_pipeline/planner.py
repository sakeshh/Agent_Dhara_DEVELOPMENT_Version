"""PlannerAgent - Transforms assessment results into executable ETL plans."""

from __future__ import annotations
import asyncio
import logging
import uuid
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.assessment.models import (
    AssessmentResult, DatasetProfile, ColumnProfile, QualityIssue,
    Relationship, SemanticProfile, IssueSeverity, IssueCategory,
    FixabilityTier, SemanticType
)
from agent.etl_pipeline.models import (
    ETLPlan, DatasetPlan, PlanStep, CrossDatasetStep,
    ManualReviewItem, CleaningPlan, CoverageReport,
    ReconciliationReport, PlanMetadata, PlanStatus, GenerationMode,
    ReviewResult, GeneratedCode, ExecutionResult
)
from agent.core.interfaces import MemoryStore
from agent.logging_setup import get_logger, get_performance_logger

logger = get_logger("etl_pipeline.planner")


class PlanningContext:
    """Context for planning decisions."""
    
    def __init__(self, dataset_name: str, dataset_profile: DatasetProfile,
                 assessment_result: Optional[AssessmentResult] = None,
                 memory: Optional[MemoryStore] = None):
        self.dataset_name = dataset_name
        self.dataset_profile = dataset_profile
        self.assessment_result = assessment_result
        self.memory = memory
        
        # Planning state
        self.blockers: List[str] = []
        self.business_keys: List[str] = []
        self.incremental = False
        self.incremental_config: Optional[IncrementalConfig] = None
        self.target_table: Optional[str] = None
        self.staging_table: Optional[str] = None
        self.watermark_column: Optional[str] = None
        self.cross_dataset_dependencies: Dict[str, str] = {}


@dataclass
class PlanSuggestion:
    """A suggested plan step with reasoning."""
    step_id: str
    action: str
    reason: str
    confidence: float
    estimated_rows_impact: int
    metadata: Dict[str, Any] = field(default_factory=dict)


class PlannerAgent:
    """Transfers assessment findings into a structured ETL plan.
    
    Converts QualityIssue -> PlanStep mapping, discovers incremental
    strategy, and generates cross-dataset steps from relationships.
    """
    
    def __init__(self, config: Any, memory: Optional[MemoryStore] = None):
        self.config = config
        self.memory = memory
        self.min_confidence = getattr(config, 'planner_min_confidence', 0.5)
        self.min_issue_coverage = getattr(config, 'planner_issue_coverage', 0.7)
    
    async def plan_from_assessment(self, assessment_result, dataset_profiles, datasets, feature_flags=None):
        """Create an ETL plan from assessment results."""
        feature_flags = feature_flags or {}
        
        # Initialize plan
        plan_id = f"plan_{uuid.uuid4().hex[:8]}"
        plan = {
            "plan_id": plan_id,
            "assessment_signature": "",
            "datasets": {},
            "cross_dataset_steps": [],
            "manual_review": [],
            "cleaning_plan": None,
            "coverage_report": None,
            "reconciliation": None,
            "engine": "sql",
            "generation_mode": "full",
            "status": "draft",
            "business_rules": {},
            "blocked": False,
            "blocking_reasons": [],
            "metadata": {
                "created_at": datetime.utcnow().isoformat(),
                "engine": "sql",
            },
        }
        
        # Process per-dataset plans
        total_issues = 0
        covered_issues = 0
        
        for ds_name, profile in dataset_profiles.items():
            ds_steps, ds_covered, ds_total = await self._plan_dataset(
                ds_name, profile, assessment_result, datasets, plan
            )
            plan["datasets"][ds_name] = {"steps": ds_steps, "complexity": "low"}
            total_issues += ds_total
            covered_issues += ds_covered
        
        # Generate cross-dataset steps from relationships
        cross_steps = await self._plan_cross_dataset(assessment_result, dataset_profiles, plan)
        plan["cross_dataset_steps"] = cross_steps
        
        # Generate manual review items
        manual_review = await self._generate_review_items(assessment_result, dataset_profiles, plan)
        plan["manual_review"] = manual_review
        
        # Create cleaning plan
        plan["cleaning_plan"] = await self._create_cleaning_plan(assessment_result, dataset_profiles, plan)
        
        # Create coverage report
        plan["coverage_report"] = await self._create_coverage_report(assessment_result, dataset_profiles, plan)
        
        # Create reconciliation
        plan["reconciliation"] = await self._create_reconciliation(assessment_result, dataset_profiles, plan)
        
        # Set status based on coverage
        coverage_pct = plan["coverage_report"]["coverage_pct"] if plan["coverage_report"] else 1.0
        if coverage_pct >= 0.8:
            plan["status"] = "approved"
        elif coverage_pct >= 0.5:
            plan["status"] = "in_review"
        else:
            plan["status"] = "blocked"
        
        # Add assessment signature
        if isinstance(assessment_result, dict) and assessment_result.get("signature"):
            plan["assessment_signature"] = assessment_result["signature"]
        
        logger.info(f"Plan built: {plan_id}, coverage: {coverage_pct:.1f}%, issues: {total_issues}")
        return plan
    
    def _plan_dataset(self, ds_name, profile, assessment_result, datasets, plan):
        """Plan transformations for a single dataset."""
        steps = []
        covered = 0
        total = len(assessment_result.get("quality_issues", [])) if assessment_result else 0
        
        if total == 0:
            steps.append({
                "step_id": f"confirm_clean_{ds_name}",
                "action": "noop",
                "reason": "No quality issues detected",
                "confidence": 1.0,
                "metadata": {"source": "assessment"}
            })
            return steps, 0, 0
        
        # Group issues by category
        issue_groups = {}
        for issue in assessment_result.get("quality_issues", []):
            cat = issue.get("category", "unknown")
            if cat not in issue_groups:
                issue_groups[cat] = []
            issue_groups[cat].append(issue)
        
        # Process each group
        for category, issues in issue_groups.items():
            for issue in issues:
                step = self._issue_to_step(issue, ds_name, profile)
                if step and step.get("confidence", 0) >= self.min_confidence:
                    steps.append(step)
                    covered += 1
        
        # Add incremental processing if configured
        if self._should_use_incremental(ds_name, profile):
            steps.append({
                "step_id": f"incremental_{ds_name}",
                "action": "incremental_read",
                "reason": "Enable incremental processing",
                "confidence": 0.9,
                "metadata": {"source": "incremental_config"}
            })
        
        # Add semantic enrichment step if needed
        if self._needs_semantic_enrichment(assessment_result, profile):
            steps.append({
                "step_id": f"semantic_enrich_{ds_name}",
                "action": "refine_semantic_type",
                "reason": "Enrich semantic types based on assessment",
                "confidence": 0.7,
                "metadata": {"source": "semantic"}
            })
        
        return steps, covered, total
    
    def _issue_to_step(self, issue, ds_name, profile):
        """Convert a quality issue to a plan step."""
        col = issue.get("column")
        itype = str(issue.get("issue_type", "")).lower()
        sev = str(issue.get("severity", "low")).lower()
        
        if not col:
            return None
        
        # Map issue type to action
        action_map = {
            "missing_values": "fill_nulls_simple",
            "null_exceeded": "fill_nulls_simple",
            "invalid_email": "sanitize_email",
            "invalid_phone": "normalize_phone",
            "format_mismatch": "lowercase",
            "date_format_inconsistency": "parse_dates",
            "email_format_inconsistency": "sanitize_email",
            "numeric_outliers_iqr": "range_clip",
            "negative_values": "range_clip",
            "implausible_age": "range_clip",
            "outlier": "range_clip",
            "business_rule": "replace_values",
            "replace_sentinel": "replace_sentinel_values",
            "placeholder_detected": "replace_sentinel_values",
            "type_mismatch": "cast_type",
            "coerce_numeric": "coerce_numeric",
            "impossible_date": "parse_dates",
            "date_format": "parse_dates",
            "invalid_value": "validate_value",
            "missing_value": "fill_nulls_simple",
        }
        
        action = action_map.get(itype)
        if not action:
            if "null" in itype:
                action = "fill_nulls_simple"
            elif "price" in ds_name.lower() and "negative" in itype:
                action = "range_clip"
            else:
                action = "review_manually"
        
        # Build params
        params = {
            "issue_id": issue.get("id", ""),
            "category": itype,
            "severity": sev,
            "column": col,
            "dataset": ds_name,
        }
        
        if action == "fill_nulls_simple":
            evidence = issue.get("evidence", {})
            fill_val = evidence.get("recommended_fill")
            if fill_val:
                params["fill_value"] = fill_val
            else:
                dtype = profile.get(col, {}).get("dtype", "string") if profile else "string"
                if "int" in str(dtype).lower():
                    params["fill_strategy"] = "mean"
                elif "float" in str(dtype).lower():
                    params["fill_strategy"] = "mean"
                else:
                    params["fill_strategy"] = "unknown"
        
        elif action == "range_clip":
            params["lower_bound"] = 0.0
            params["min_value"] = 0.0
            if any(k in ds_name.lower() for k in ("price", "cost", "revenue")):
                params["lower_bound"] = 0.0
                params["min_value"] = 0.0
        
        elif action == "sanitize_email":
            params["target_format"] = "canonical"
        
        elif action == "normalize_phone":
            params["strategy"] = "digits"
        
        elif action == "cast_type":
            semantic = profile.get(col, {}).get("semantic_type", "") if profile else ""
            if semantic and "date" in semantic.lower():
                params["target_type"] = "DATE"
            elif semantic and "id" in semantic.lower():
                params["target_type"] = "BIGINT"
            elif semantic and "float" in semantic.lower():
                params["target_type"] = "DECIMAL(18, 2)"
            else:
                params["target_type"] = "NVARCHAR(256)"
        
        elif action == "coerce_numeric":
            col_lower = col.lower()
            is_price = any(k in col_lower for k in ("price", "cost", "amount", "rate", "total", "balance", "tax"))
            params["cast_target"] = "DECIMAL(18, 2)" if is_price else "BIGINT"
        
        elif action == "parse_dates":
            params["filter_impossible"] = True
        
        elif action == "review_manually":
            # Generate manual review item
            pass
        
        step = {
            "step_id": f"fix_{itype}_{ds_name}_{col.strip('[]').lower()}",
            "action": action,
            "reason": issue.get("message", f"Fix {itype} on {col}"),
            "confidence": 0.8,
            "params": params,
            "source_issue_type": itype,
            "source_issue_id": issue.get("id"),
            "evidence_id": issue.get("id"),
        }
        
        return step
    
    def _should_use_incremental(self, ds_name, ds_data):
        """Determine if incremental processing should be used."""
        row_count = ds_data.get("row_count", 0) if ds_data else 0
        if row_count > 10000:
            return True
        
        columns = ds_data.get("columns", {}) if ds_data else {}
        if any(c.get("name") in ("watermark", "last_modified") for c in columns.values()):
            return True
        
        return False
    
    def _needs_semantic_enrichment(self, assessment_result, ds_name):
        """Determine if semantic enrichment is needed."""
        ds_info = assessment_result.get("datasets", {}).get(ds_name) if assessment_result else {}
        if ds_info:
            columns = ds_info.get("columns", {})
            unknown_count = sum(1 for c in columns.values() if str(c.get("semantic_type", "")) == "unknown")
            total_cols = len(columns)
            if total_cols > 0 and unknown_count / total_cols > 0.3:
                return True
        return False
    
    async def _plan_cross_dataset(self, assessment_result, dataset_profiles, plan):
        """Generate cross-dataset steps from relationships."""
        steps = []
        relationships = assessment_result.get("relationships", []) if assessment_result else []
        
        for rel in relationships:
            rel_str = str(rel).lower()
            step = {
                "step_id": f"validate_rel_{uuid.uuid4().hex[:4]}",
                "action": "validate_relationship",
                "datasets": [rel.get("parent_dataset", ""), rel.get("child_dataset", "")],
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
        
        datasets = list(dataset_profiles.keys()) if dataset_profiles else []
        if len(datasets) > 1:
            steps.append({
                "step_id": "cross_dataset_aggregation",
                "action": "cross_dataset_agg",
                "datasets": datasets,
                "order": 1,
                "params": {"type": "cross_dataset_summary"},
            })
        
        return steps
    
    async def _generate_review_items(self, assessment_result, dataset_profiles, plan):
        """Generate manual review items."""
        items = []
        dq_issues = assessment_result.get("data_quality_issues", {}).get("datasets", {}) if assessment_result else {}
        
        for issue in dq_issues.values():
            for issue_item in issue.get("issues", []):
                sev = str(issue_item.get("severity", "low")).lower()
                if sev in ["critical", "high"]:
                    col = issue_item.get("column", "")
                    itype = str(issue_item.get("issue_type", "")).lower()
                    msg = issue_item.get("message", "")
                    items.append({
                        "id": f"review_{issue_item.get('id')}",
                        "type": "blocker",
                        "dataset": issue_item.get("dataset_name", ""),
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
                    })
        
        for ds_name, ds_plan in plan.get("datasets", {}).items():
            for step in ds_plan.get("steps", []):
                if step.get("action") in ["drop_table", "drop_column", "purge_data"]:
                    items.append({
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
                    })
        
        return items
    
    async def _create_cleaning_plan(self, assessment_result, dataset_profiles, plan):
        """Create cleaning plan from assessment."""
        if not assessment_result or not assessment_result.get("data_quality_issues"):
            return None
        
        cleaning_mode = "cleanse_with_audit"
        business_keys = {}
        duplicate_policies = {}
        output_contracts = {}
        
        for ds_name, ds_data in dataset_profiles.items():
            business_keys[ds_name] = []
            duplicate_policies[ds_name] = {}
            
            dq_issues = assessment_result.get("data_quality_issues", {}).get("datasets", {}).get(ds_name, {}).get("issues", [])
            
            for issue in dq_issues:
                col = issue.get("column")
                if col:
                    itype = str(issue.get("issue_type", "")).lower()
                    if itype in ["duplicate", "exact_duplicate_rows", "business_key_duplicate"]:
                        if col not in business_keys[ds_name]:
                            business_keys[ds_name].append(col)
                    
                    if itype == "duplicate":
                        duplicate_policies[ds_name][col] = {
                            "policy": "deduplicate",
                            "strategy": "keep_last",
                        }
                    
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
    
    async def _create_coverage_report(self, assessment_result, dataset_profiles, plan):
        """Create coverage report."""
        total_issues = 0
        covered_issues = 0
        per_dataset = {}
        unmatched_issues = []
        
        for ds_name, ds_plan in plan.get("datasets", {}).items():
            dq_issues = assessment_result.get("data_quality_issues", {}).get("datasets", {}).get(ds_name, {}).get("issues", []) if assessment_result else []
            ds_total = len(dq_issues)
            ds_covered = len([s for s in ds_plan.get("steps", []) if s.get("evidence_id")])
            
            total_issues += ds_total
            covered_issues += ds_covered
            
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
    
    async def _create_reconciliation(self, assessment_result, dataset_profiles, plan):
        """Reconcile assessment issues with plan steps."""
        total_issues = 0
        total_steps = 0
        per_dataset = {}
        unmatched_issues = []
        
        for ds_name, ds_plan in plan.get("datasets", {}).items():
            dq_issues = assessment_result.get("data_quality_issues", {}).get("datasets", {}).get(ds_name, {}).get("issues", []) if assessment_result else []
            ds_total = len(dq_issues)
            ds_steps = len(ds_plan.get("steps", []))
            
            total_issues += ds_total
            total_steps += ds_steps
            
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