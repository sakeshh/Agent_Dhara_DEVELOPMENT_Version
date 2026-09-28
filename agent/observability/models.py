from __future__ import annotations

import time
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class RouteMetrics(BaseModel):
    """Metrics captured during the sub-millisecond intent routing stage."""
    route_type: str = "compiler_direct"  # compiler_direct | llm_ir_planner | multi_agent_planner
    route_latency_ms: float = 0.0
    planner_type: str = "deterministic"
    confidence: float = 1.0


class IRMetrics(BaseModel):
    """Metrics captured during canonical IR extraction, normalization, and self-healing."""
    validation_count: int = 1
    repair_count: int = 0
    repair_success: bool = True
    semantic_failures: int = 0
    fixer_latency_ms: float = 0.0


class CompilerMetrics(BaseModel):
    """Metrics captured during target compilation and emission."""
    target: str = "sql"  # sql | pyspark | adf | fabric | databricks | python
    compile_time: float = 0.0
    artifact_size: int = 0
    success: bool = True
    step_count: int = 0


class ApprovalMetrics(BaseModel):
    """Metrics captured during enterprise human approval evaluation."""
    approval_tier: str = "auto_approved"  # auto_approved | warning_required | mandatory_human_approval
    approver: Optional[str] = None
    decision_time: float = 0.0
    risk_level: str = "low"
    requires_human_gate: bool = False


class ExecutionMetrics(BaseModel):
    """Metrics captured post-generation and pre/post physical pipeline run."""
    runtime: float = 0.0
    rows_processed: int = 0
    cost_estimate: str = "low"
    execution_success: bool = True
    shuffle_operations: int = 0
    confidence_score: float = 1.0


class DharaTelemetrySpan(BaseModel):
    """Unified OpenTelemetry span encapsulating an entire compiler lifecycle."""
    trace_id: str
    span_id: str
    session_id: str
    timestamp: float = Field(default_factory=time.time)
    route: RouteMetrics = Field(default_factory=RouteMetrics)
    ir: IRMetrics = Field(default_factory=IRMetrics)
    compiler: CompilerMetrics = Field(default_factory=CompilerMetrics)
    approval: ApprovalMetrics = Field(default_factory=ApprovalMetrics)
    execution: ExecutionMetrics = Field(default_factory=ExecutionMetrics)
    tags: Dict[str, str] = Field(default_factory=dict)
