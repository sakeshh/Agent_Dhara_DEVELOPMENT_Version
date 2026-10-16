"""ReviewerAgent - Human-in-the-loop review and approval of ETL plans."""

from __future__ import annotations
import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from agent.etl_pipeline.models import (
    ETLPlan, ManualReviewItem, PlanStatus, ReviewResult
)
from agent.core.interfaces import MemoryStore
from agent.logging_setup import get_logger, get_performance_logger

logger = get_logger("etl_pipeline.reviewer")


class ReviewResult:
    """Result of plan review."""
    coverage: Dict[str, Any]
    traceability: Dict[str, Any]
    risk_assessment: Dict[str, Any]
    best_practices: Dict[str, Any]
    performance: Dict[str, Any]
    suggestions: List[str]
    can_proceed: bool
    overall_score: float
    review_items: List[Dict[str, Any]]
    updated_plan: ETLPlan


class ReviewGateStatus:
    """Status of the review gate."""
    
    def __init__(self, approved: bool, reason: str, blocking_items: List[str],
                 conditions: Optional[List[str]] = None):
        self.approved = approved
        self.reason = reason
        self.blocking_items = blocking_items
        self.conditions = conditions or []


class ReviewerAgent:
    """Human-in-the-loop reviewer for ETL plans.
    
    Presents plan to human reviewer, tracks decisions, and determines
    whether plan can proceed to code generation.
    """
    
    def __init__(self, config: Any, memory: Optional[MemoryStore] = None):
        self.config = config
        self.memory = memory
        self.review_timeout_minutes = getattr(config, 'review_timeout_minutes', 30)
        self.auto_approve_low_risk = getattr(config, 'auto_approve_low_risk', True)
    
    async def review_plan(self, plan: Any) -> Any:
        """Review an ETL plan and produce review result."""
        
        with get_performance_logger("plan_review") as perf:
            
            # Analyze plan for risk
            risk_assessment = await self._assess_risk(plan)
            
            # Generate coverage traceability
            traceability = await self._generate_traceability(plan)
            
            # Generate best practices
            best_practices = await self._generate_best_practices(plan)
            
            # Generate performance estimate
            performance = await self._estimate_performance(plan)
            
            # Generate suggestions
            suggestions = await self._generate_suggestions(plan)
            
            # Determine if plan can proceed
            can_proceed = await self._determine_proceed(plan, risk_assessment)
            
            # Create review items summary
            review_items = self._create_review_item_summary(plan.manual_review)
            
            # Overall coverage score
            coverage = await self._compute_coverage(plan)
            overall_score = risk_assessment.get("overall_score", 0.0)
            
            result = ReviewResult(
                coverage=coverage,
                traceability=traceability,
                risk_assessment=risk_assessment,
                best_practices=best_practices,
                performance=performance,
                suggestions=suggestions,
                can_proceed=can_proceed,
                overall_score=overall_score,
                updated_plan=plan,
            )
            
            perf.log_metric("overall_score", overall_score)
            perf.log_metric("can_proceed", can_proceed)
            perf.log_metric("risk_score", overall_score)
            
            return result
    
    async def _assess_risk(self, plan: Any) -> Any:
        """Assess risk of the ETL plan."""
        
        risk_metrics: Dict[str, Any] = {
            "overall_score": 0.0,
            "severity_breakdown": {"critical": 0, "high": 0, "medium": 0, "low": 0},
            "blocking_items": [],
            "destructive_operations": 0,
            "cross_dataset_risks": 0,
        }
        
        total_score = 0.0
        count = 0
        
        # Review manual review items
        for item in plan.manual_review:
            sev = item.severity.lower() if isinstance(item.severity, str) else item.severity.value.lower()
            risk_metrics["severity_breakdown"][sev] = risk_metrics["severity_breakdown"].get(sev, 0) + 1
            
            if item.status == "pending" and sev in ["critical", "high"]:
                risk_metrics["blocking_items"].append(f"{item.type}: {item.message}")
                total_score += 0.3 if sev == "critical" else 0.2
            
            if item.type == "destructive_operation":
                risk_metrics["destructive_operations"] += 1
                total_score += 0.1
        
        # Review plan steps for risks
        for ds_name, ds_plan in plan.datasets.items():
            for step in ds_plan.steps:
                # High-risk actions
                if step.action in ["drop_table", "drop_column", "purge_data"]:
                    risk_metrics["destructive_operations"] += 1
                    total_score += 0.2
                    risk_metrics["blocking_items"].append(f"step {step.step_id}: destructive operation")
                
                # Medium-risk actions
                if step.action in ["standardize_format", "rebalance_distribution"]:
                    total_score += 0.05
        
        # Cross-dataset steps risk
        for cds in plan.cross_dataset_steps:
            if cds.action in ["join", "union"]:
                risk_metrics["cross_dataset_risks"] += 1
                total_score += 0.1
        
        # Cap overall score at 1.0
        overall_score = min(total_score, 1.0)
        risk_metrics["overall_score"] = overall_score
        
        return risk_metrics
    
    async def _generate_traceability(self, plan: Any) -> Any:
        """Generate traceability mapping from assessment to plan."""
        
        traceability: Dict[str, Any] = {
            "issue_to_step": {},
            "step_to_issue": {},
            "coverage_by_category": {},
        }
        
        # Map issues to steps
        if plan.manual_review:
            for item in plan.manual_review:
                if item.issue_type:
                    traceability["issue_to_step"][item.issue_type] = \
                        traceability["issue_to_step"].get(item.issue_type, 0) + 1
        
        for ds_name, ds_plan in plan.datasets.items():
            category_counts: Dict[str, int] = {}
            
            for step in ds_plan.steps:
                if step.evidence_id:
                    # Find the corresponding issue
                    category = step.params.get("category", "unknown") if step.params else "unknown"
                    category_counts[category] = category_counts.get(category, 0) + 1
                    traceability["step_to_issue"][step.step_id] = category
            
            traceability["coverage_by_category"][ds_name] = category_counts
        
        return traceability
    
    async def _generate_best_practices(self, plan: Any) -> Any:
        """Generate best practice recommendations."""
        
        best_practices: List[str] = []
        
        # Check for good patterns
        total_issues = sum(
            len(issues) for issues in plan.coverage_report.per_dataset.values()
        ) if plan.coverage_report else 0
        
        # Recommendations
        if plan.coverage_report and plan.coverage_report.coverage_pct >= 0.8:
            best_practices.append("High coverage achieved - plan can proceed")
        else:
            best_practices.append("Consider additional issue coverage before proceeding")
        
        # Check incremental config
        if any(
            step.action == "incremental_read"
            for ds_plan in plan.datasets.values()
            for step in ds_plan.steps
        ):
            best_practices.append("Incremental processing enabled - monitor watermark")
        
        # Check relationships
        if plan.cross_dataset_steps:
            best_practices.append("Cross-dataset steps present - verify join/union logic")
        
        # Check for risky operations
        risky = [
            s for s in self._flatten_steps(plan)
            if s.action in ["drop_table", "drop_column", "purge_data"]
        ]
        if risky:
            best_practices.append("Review destructive operations before execution")
        
        return {"best_practices": best_practices, "count": len(best_practices)}
    
    async def _estimate_performance(self, plan: Any) -> Any:
        """Estimate performance characteristics of the plan."""
        
        estimated_runtime = 0
        estimated_cost = 0.0
        step_count = 0
        
        for ds_name, ds_plan in plan.datasets.items():
            for step in ds_plan.steps:
                step_count += 1
                # Rough runtime estimate
                if step.action in ["fill_nulls", "standardize_format"]:
                    estimated_runtime += 5  # seconds
                elif step.action in ["clip_outliers", "deduplicate"]:
                    estimated_runtime += 10
                elif step.action in ["drop_table", "purge_data"]:
                    estimated_runtime += 1
                else:
                    estimated_runtime += 3
                
                # Cost estimate
                if step.action in ["semantic_refinement"]:
                    estimated_cost += 0.1  # LLM cost
                elif step.action in ["fill_nulls", "standardize_format"]:
                    estimated_cost += 0.01
        
        # Cross-dataset steps
        for cds in plan.cross_dataset_steps:
            step_count += 1
            estimated_runtime += 10
            estimated_cost += 0.05
        
        return {
            "estimated_runtime_seconds": estimated_runtime,
            "estimated_cost_usd": estimated_cost,
            "step_count": step_count,
        }
    
    async def _generate_suggestions(self, plan: Any) -> Any:
        """Generate suggestions for plan improvement."""
        
        suggestions: List[str] = []
        
        # Coverage suggestions
        if plan.coverage_report and plan.coverage_report.coverage_pct < 0.7:
            suggestions.append("Increase issue coverage before plan approval")
        
        # Incremental suggestions
        if not any(
            step.action == "incremental_read"
            for ds_plan in plan.datasets.values()
            for step in ds_plan.steps
        ):
            suggestions.append("Consider incremental processing for large datasets")
        
        # Relationship suggestions
        if not plan.cross_dataset_steps and len(plan.datasets) > 1:
            suggestions.append("Consider cross-dataset relationships for integration")
        
        # Business rule suggestions
        if plan.business_rules and len(plan.business_rules) < 3:
            suggestions.append("Consider reviewing uncovered business rules")
        
        return suggestions
    
    async def _determine_proceed(self, plan: Any, risk_assessment: Any) -> Any:
        """Determine if the plan can proceed to code generation."""
        
        # Check for blocking items
        blocking = risk_assessment.get("blocking_items", [])
        if blocking:
            return False, "Blocked by pending high/critical review items"
        
        # Check plan status
        if plan.status != PlanStatus.APPROVED and plan.status != PlanStatus.GENERATING:
            return False, "Plan not in approved/generating state"
        
        # Check coverage
        if plan.coverage_report and plan.coverage_report.coverage_pct < self.min_issue_coverage:
            return False, f"Coverage {plan.coverage_report.coverage_pct:.2f} below threshold {self.min_issue_coverage}"
        
        # Check manual review items
        for item in plan.manual_review:
            if item.status == "pending" and item.severity in ["critical", "high"]:
                return False, f"Pending {item.type}: {item.message}"
        
        return True, "Plan can proceed to code generation"
    
    def _create_review_item_summary(self, manual_review: Any) -> Any:
        """Create summary of manual review items."""
        
        summary: List[Dict[str, Any]] = []
        
        for item in manual_review:
            summary.append({
                "id": item.id,
                "type": item.type,
                "dataset": item.dataset,
                "column": item.column,
                "issue_type": item.issue_type,
                "severity": item.severity,
                "status": item.status,
                "message": item.message,
                "resolution": item.resolution,
            })
        
        return summary
    
    def _flatten_steps(self, plan: Any) -> Any:
        """Flatten all dataset steps into a single list."""
        
        steps: List[Any] = []
        for ds_plan in plan.datasets.values():
            steps.extend(ds_plan.steps)
        steps.extend(plan.cross_dataset_steps)
        return steps