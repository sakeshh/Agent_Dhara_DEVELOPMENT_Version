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
    """Detailed compilation status for an individual physical step."""
    step_id: str
    dataset_name: str
    action_type: str
    strategy_name: Optional[str]
    template_name: Optional[str]
    status: str  # RENDERED | UNSUPPORTED | TEMPLATE_MISSING | PARAMETER_MISSING | RENDER_FAILED | SKIPPED
    sql: Optional[str] = None
    error: Optional[str] = None


@dataclass
class CompilationTelemetry:
    """Summary metrics of the compilation and code generation process."""
    ok: bool = True
    executable: bool = True
    partial: bool = False
    pipeline_type: str = "transformation"  # "transformation" | "pass_through"
    message: Optional[str] = None
    assessment_issue_count: int = 0
    mapped_action_count: int = 0
    unsupported_issue_count: int = 0
    planned_step_count: int = 0
    rendered_step_count: int = 0
    template_missing_count: int = 0
    parameter_missing_count: int = 0
    render_failed_count: int = 0
    fallback_projection_used: bool = False
    failure_stage: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    step_results: List[StepCompilationResult] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "executable": self.executable,
            "partial": self.partial,
            "pipeline_type": self.pipeline_type,
            "message": self.message,
            "assessment_issue_count": self.assessment_issue_count,
            "mapped_action_count": self.mapped_action_count,
            "unsupported_issue_count": self.unsupported_issue_count,
            "planned_step_count": self.planned_step_count,
            "rendered_step_count": self.rendered_step_count,
            "template_missing_count": self.template_missing_count,
            "parameter_missing_count": self.parameter_missing_count,
            "render_failed_count": self.render_failed_count,
            "fallback_projection_used": self.fallback_projection_used,
            "failure_stage": self.failure_stage,
            "warnings": self.warnings,
        }
