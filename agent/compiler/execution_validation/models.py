from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class CostEstimate(BaseModel):
    estimated_cost: str = "low"  # "low" | "medium" | "high"
    shuffle_operations: int = 0
    broadcast_recommended: bool = False
    estimated_runtime: str = "< 5s"
    estimated_memory_mb: int = 512
    complexity_score: float = 1.0
    cost_breakdown: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)
    recommendations: List[str] = Field(default_factory=list)


class ExecutionValidationResult(BaseModel):
    valid: bool = True
    target_engine: str = "sql"
    explain_type: str = "simulated"  # "simulated" | "actual"
    confidence_score: float = 1.0
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    recommendations: List[str] = Field(default_factory=list)
    optimization_suggestions: List[str] = Field(default_factory=list)
    explain_plan: str = ""
    cost_estimate: CostEstimate = Field(default_factory=CostEstimate)
    validation_latency_ms: float = 0.0
