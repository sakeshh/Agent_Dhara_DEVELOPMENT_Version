"""ETL Pipeline Models - Core data structures for the pipeline."""
from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from typing import Union

from agent.assessment.models import QualityIssue, IssueSeverity

# Generated ETL code artifact - used by CodegenAgent to represent generated code
@dataclass
class GeneratedCode:
    """Generated ETL code artifact."""
    id: str = field(default_factory=lambda: f"code_{uuid.uuid4().hex[:8]}")
    language: str = "python"  # python, sql, spark, etc.
    code: str = ""
    dialect: str = "tsql"  # target database/warehouse dialect
    plan_id: str = ""
    step_ids: List[str] = field(default_factory=list)
    evidence_ids: List[str] = field(default_factory=list)
    validation_errors: List[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "language": self.language,
            "code_len": len(self.code),
            "dialect": self.dialect,
            "plan_id": self.plan_id,
            "step_count": len(self.step_ids),
            "generated_at": self.generated_at.isoformat(),
        }

# Execution result for code execution tracking
@dataclass
class ExecutionResult:
    """Result of code execution."""
    plan_id: str
    execution_id: str = field(default_factory=lambda: f"exec_{uuid.uuid4().hex[:8]}")
    code_id: Optional[str] = None
    status: str = "pending"  # pending, running, completed, failed, cancelled
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None

    # Results
    success: Optional[bool] = None
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None

    # Performance
    runtime_seconds: Optional[float] = None
    records_processed: Optional[int] = None
    records_written: Optional[int] = None

    # Per-step results
    step_results: List[Dict[str, Any]] = field(default_factory=list)

    # Log context
    log_context_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "plan_id": self.plan_id,
            "code_id": self.code_id,
            "status": self.status,
            "success": self.success,
            "runtime_seconds": self.runtime_seconds,
            "records_processed": self.records_processed,
            "records_written": self.records_written,
            "error": self.error,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "step_results": self.step_results,
        }

# Pipeline scheduling
@dataclass
class ScheduledRun:
    """A scheduled pipeline run."""
    pipeline_id: str
    schedule_name: str = ""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    cron_expression: Optional[str] = None
    trigger: str = "one_time"  # "cron", "interval", "one_time", "event_based"
    status: str = "pending"  # "pending", "running", "completed", "failed", "cancelled"
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    last_completed: Optional[datetime] = None
    run_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    configured: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "pipeline_id": self.pipeline_id,
            "schedule_name": self.schedule_name,
            "trigger": self.trigger,
            "status": self.status,
            "cron_expression": self.cron_expression,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "last_completed": self.last_completed.isoformat() if self.last_completed else None,
            "run_count": self.run_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
        }

from agent.core.interfaces import IncrementalConfig


class GenerationMode(Enum):
    FULL = "full"           # Cleanse + Transform
    CLEANSE_ONLY = "cleanse_only"
    TRANSFORM_ONLY = "transform_only"


class PlanStatus(Enum):
    DRAFT = "draft"
    IN_REVIEW = "in_review"
    APPROVED = "approved"
    GENERATING = "generating"
    VALIDATING = "validating"
    CODE_READY = "code_ready"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class PlanStep:
    """A single transformation step in the plan."""
    step_id: str
    dataset: str
    column: Optional[str]
    action: str
    source_issue_id: Optional[str] = None
    source_issue_type: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)
    evidence_id: Optional[str] = None
    confidence: float = 1.0
    depends_on: List[str] = field(default_factory=list)  # step_ids
    order: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DatasetPlan:
    """Plan for a single dataset."""
    dataset: str
    steps: List[PlanStep] = field(default_factory=list)
    estimated_rows: int = 0
    complexity: str = "medium"  # low, medium, high
    incremental_config: Optional[IncrementalConfig] = None
    source_type: str = "unknown"
    target_table: Optional[str] = None
    staging_table: Optional[str] = None
    watermark_column: Optional[str] = None


@dataclass
class CrossDatasetStep:
    """A step involving multiple datasets."""
    step_id: str
    action: str  # join, lookup, aggregation_check, union
    datasets: List[str]
    join_keys: Dict[str, str] = field(default_factory=dict)  # dataset -> column
    params: Dict[str, Any] = field(default_factory=dict)
    order: int = 0
    depends_on: List[str] = field(default_factory=list)


@dataclass
class ManualReviewItem:
    """An item requiring human review."""
    id: str
    type: str  # blocker, ambiguous_action, destructive_operation, business_rule_review
    dataset: str
    column: Optional[str]
    issue_type: str
    severity: str
    message: str
    guidance: str = ""
    resolution_options: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "pending"  # pending, approved, rejected, resolved
    resolution: Optional[str] = None
    justification: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[str] = None


@dataclass
class CleaningPlan:
    """Cleaning plan for a dataset."""
    dataset: str
    cleaning_mode: str = "cleanse_with_audit"  # audit_only, cleanse_with_audit, strict
    business_keys: Dict[str, List[str]] = field(default_factory=dict)  # dataset -> key columns
    duplicate_policies: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    output_contracts: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CoverageReport:
    """Assessment-to-plan coverage report."""
    total_issues: int
    covered_issues: int
    coverage_pct: float
    per_dataset: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    unmatched_issues: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ReconciliationReport:
    """Assessment-to-plan reconciliation."""
    total_issues: int
    total_steps: int
    coverage_pct: float
    per_dataset: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    unmatched_issues: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class PlanMetadata:
    """Metadata about the plan."""
    plan_id: str
    assessment_signature: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    created_by: Optional[str] = None
    version: str = "1.0"
    generation_mode: GenerationMode = GenerationMode.FULL
    status: PlanStatus = PlanStatus.DRAFT
    engine_recommendation: Dict[str, Any] = field(default_factory=dict)
    estimated_runtime_seconds: int = 0
    estimated_cost_usd: float = 0.0


@dataclass
class ETLPlan:
    """Complete ETL plan."""
    plan_id: str = field(default_factory=lambda: f"plan_{uuid.uuid4().hex[:8]}")
    assessment_signature: str = ""
    datasets: Dict[str, DatasetPlan] = field(default_factory=dict)
    cross_dataset_steps: List[CrossDatasetStep] = field(default_factory=list)
    manual_review: List[ManualReviewItem] = field(default_factory=list)
    cleaning_plan: Optional[CleaningPlan] = None
    coverage_report: Optional[CoverageReport] = None
    reconciliation: Optional[ReconciliationReport] = None
    engine_recommendation: Dict[str, Any] = field(default_factory=dict)
    business_rules: Dict[str, Any] = field(default_factory=dict)
    generation_mode: GenerationMode = GenerationMode.FULL
    status: PlanStatus = PlanStatus.DRAFT
    metadata: PlanMetadata = field(default_factory=PlanMetadata)
    blocked: bool = False
    blocking_reasons: List[str] = field(default_factory=list)
    
    def total_steps(self) -> int:
        return sum(len(dp.steps) for dp in self.datasets.values()) + len(self.cross_dataset_steps)
    
    def get_step(self, step_id: str) -> Optional[PlanStep]:
        for dp in self.datasets.values():
            for step in dp.steps:
                if step.step_id == step_id:
                    return step
        for cds in self.cross_dataset_steps:
            if cds.step_id == step_id:
                return cds
        return None
    
    def get_dataset_steps(self, dataset: str) -> List[PlanStep]:
        return self.datasets.get(dataset, DatasetPlan(dataset=dataset)).steps


@dataclass
class ReviewResult:
    """Result of plan review."""
    coverage: Dict[str, Any]  # CoverageReport as dict
    traceability: Dict[str, Any]  # TraceabilityReport as dict
    risk_assessment: Dict[str, Any]  # RiskAssessment as dict
    best_practices: Dict[str, Any]
    performance: Dict[str, Any]
    suggestions: List[str]
    can_proceed: bool
    overall_score: float = 0.0
    review_items: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class PipelineSchedule:
    """A pipeline schedule configuration."""
    plan_id: str
    id: str = field(default_factory=lambda: f"schedule_{uuid.uuid4().hex[:8]}")
    schedule_name: str = ""
    cron_expression: Optional[str] = None
    trigger: str = "one_time"  # "cron", "interval", "one_time", "event_based"
    status: str = "pending"  # "pending", "running", "completed", "failed", "cancelled"
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    last_completed: Optional[datetime] = None
    run_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    configured: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "plan_id": self.plan_id,
            "schedule_name": self.schedule_name,
            "trigger": self.trigger,
            "status": self.status,
            "cron_expression": self.cron_expression.isoformat() if self.cron_expression else None,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "last_completed": self.last_completed.isoformat() if self.last_completed else None,
            "run_count": self.run_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
        }
class ValidationResult:
    """Result of code validation."""
    plan_id: str
    step_id: Optional[str] = None
    checks_passed: int = 0
    checks_total: int = 0
    severity: str = "info"
    details: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    execution_time_ms: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "step_id": self.step_id,
            "checks_passed": self.checks_passed,
            "checks_total": self.checks_total,
            "severity": self.severity,
            "details": self.details,
            "error_message": self.error_message,
            "execution_time_ms": self.execution_time_ms,
            "timestamp": self.timestamp.isoformat(),
        }
