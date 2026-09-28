from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from agent.compiler.models.logical import ActionType


class SafetyStatus(str, Enum):
    PASSED = "passed"
    WARNING = "warning"
    BLOCKED = "blocked"
    QUARANTINED = "quarantined"
    SKIPPED = "skipped"


class ConversionFailure(BaseModel):
    """Tracks a single silent type conversion anomaly or failed cast."""
    column: str
    original_value: Any
    target_type: str
    row_index: int
    reason: str


class StepSafetyMetrics(BaseModel):
    """Defensive metrics collected per transformation step."""
    step_id: str
    action_type: ActionType
    target_columns: List[str] = Field(default_factory=list)
    input_rows: int = 0
    passed_rows: int = 0
    failed_rows: int = 0
    failure_percentage: float = 0.0
    unexpected_nulls: int = 0
    quarantined_rows: int = 0
    status: SafetyStatus = SafetyStatus.PASSED
    conversion_failures: List[ConversionFailure] = Field(default_factory=list)
    violations: List[str] = Field(default_factory=list)


class DryRunReport(BaseModel):
    """Comprehensive report produced by pre-flight validation on sample data."""
    pipeline_id: str
    status: SafetyStatus = SafetyStatus.PASSED
    is_safe_to_execute: bool = True
    step_metrics: List[StepSafetyMetrics] = Field(default_factory=list)
    total_input_rows: int = 0
    total_output_rows: int = 0
    total_failed_rows: int = 0
    circuit_breaker_triggered: bool = False
    violations: List[str] = Field(default_factory=list)
    quarantined_records: List[Dict[str, Any]] = Field(default_factory=list)
    clean_records: List[Dict[str, Any]] = Field(default_factory=list)
