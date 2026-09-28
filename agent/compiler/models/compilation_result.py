from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class CodeGenerationError(RuntimeError):
    """Raised when compilation fails a validation gate."""
    def __init__(
        self,
        message: str,
        telemetry: Optional[Dict[str, Any]] = None,
        failure_stage: str = "compilation_gate",
    ):
        super().__init__(message)
        self.message = message
        self.telemetry = telemetry or {}
        self.failure_stage = failure_stage

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": False,
            "executable": False,
            "error": self.message,
            "failure_stage": self.failure_stage,
            **self.telemetry,
        }


@dataclass
class StepCompilationResult:
    """Detailed compilation status for an individual physical step with full lineage."""
    step_id: str
    dataset_name: str
    action_type: str
    strategy_name: Optional[str]
    template_name: Optional[str]
    status: str  # RENDERED | UNSUPPORTED | TEMPLATE_MISSING | PARAMETER_MISSING | RENDER_FAILED | SKIPPED
    sql: Optional[str] = None
    error: Optional[str] = None
    source_logical_step_id: Optional[str] = None
    source_action_id: Optional[str] = None
    source_issue_ids: List[str] = field(default_factory=list)
    is_executable_transformation: bool = False


@dataclass
class CompilationTelemetry:
    """Summary metrics of the compilation and code generation process."""
    ok: bool = True
    executable: bool = True
    partial: bool = False
    pipeline_type: str = "transformation"  # "transformation" | "pass_through"
    message: Optional[str] = None

    # Ingestion accounting
    assessment_issue_count: int = 0
    mapped_issue_count: int = 0
    unsupported_issue_count: int = 0
    manual_review_issue_count: int = 0

    # Planner metrics
    structured_action_count: int = 0
    logical_step_count: int = 0
    physical_step_count: int = 0

    # Transformation execution metrics
    rendered_transformation_count: int = 0
    failed_transformation_count: int = 0
    template_missing_count: int = 0
    parameter_missing_count: int = 0
    render_failed_count: int = 0
    skipped_step_count: int = 0
    unsupported_step_count: int = 0
    manual_review_step_count: int = 0

    # Infrastructure SQL fragment metrics (never counted as transformations)
    infrastructure_statement_count: int = 0
    projection_statement_count: int = 0
    fallback_projection_used: bool = False

    failure_stage: Optional[str] = None
    planner_input_source: str = "structured_actions"
    coverage: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    step_results: List[StepCompilationResult] = field(default_factory=list)
    issue_reconciliation: List[Dict[str, Any]] = field(default_factory=list)

    # Backward compatibility properties
    @property
    def mapped_action_count(self) -> int:
        return self.mapped_issue_count

    @mapped_action_count.setter
    def mapped_action_count(self, v: int) -> None:
        self.mapped_issue_count = v

    @property
    def planned_step_count(self) -> int:
        return self.physical_step_count

    @planned_step_count.setter
    def planned_step_count(self, v: int) -> None:
        self.physical_step_count = v

    @property
    def rendered_step_count(self) -> int:
        return self.rendered_transformation_count

    @rendered_step_count.setter
    def rendered_step_count(self, v: int) -> None:
        self.rendered_transformation_count = v

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "executable": self.executable,
            "partial": self.partial,
            "pipeline_type": self.pipeline_type,
            "message": self.message,
            "assessment_issue_count": self.assessment_issue_count,
            "mapped_issue_count": self.mapped_issue_count,
            "mapped_action_count": self.mapped_action_count,
            "unsupported_issue_count": self.unsupported_issue_count,
            "manual_review_issue_count": self.manual_review_issue_count,
            "structured_action_count": self.structured_action_count,
            "logical_step_count": self.logical_step_count,
            "physical_step_count": self.physical_step_count,
            "planned_step_count": self.planned_step_count,
            "rendered_transformation_count": self.rendered_transformation_count,
            "rendered_step_count": self.rendered_step_count,
            "failed_transformation_count": self.failed_transformation_count,
            "template_missing_count": self.template_missing_count,
            "parameter_missing_count": self.parameter_missing_count,
            "render_failed_count": self.render_failed_count,
            "skipped_step_count": self.skipped_step_count,
            "unsupported_step_count": self.unsupported_step_count,
            "manual_review_step_count": self.manual_review_step_count,
            "infrastructure_statement_count": self.infrastructure_statement_count,
            "projection_statement_count": self.projection_statement_count,
            "fallback_projection_used": self.fallback_projection_used,
            "failure_stage": self.failure_stage,
            "planner_input_source": self.planner_input_source,
            "coverage": self.coverage,
            "warnings": self.warnings,
            "issue_reconciliation": self.issue_reconciliation,
        }
