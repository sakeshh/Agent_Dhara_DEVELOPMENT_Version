from enum import Enum
import time
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from agent.compiler.execution_validation.models import CostEstimate
from agent.ir.models import IrPlan, RepairRecord


class StabilityTier(str, Enum):
    GOLD = "gold"          # >= 0.95
    SILVER = "silver"      # >= 0.90
    BRONZE = "bronze"      # >= 0.80
    PROVISIONAL = "provisional"  # < 0.80 or < 3 executions


class PatternExecutionRecord(BaseModel):
    """
    Core memory entity for Phase 8 Pattern Learning Engine.
    Persists:
    - User Prompt
    - Canonical IR Plan (Never raw code)
    - Repair History (Fixer audit trail)
    - Cost Estimate & Complexity
    - Validation Result & Confidence
    - Execution Outcome Telemetry
    - User Feedback & Ratings
    """
    pattern_id: str
    session_id: str
    prompt: str
    ir_plan: IrPlan
    repair_history: List[RepairRecord] = Field(default_factory=list)
    cost_estimate: CostEstimate = Field(default_factory=CostEstimate)
    validation_result: Optional[Dict[str, Any]] = None
    target_engine: str = "sql"
    compilation_success: bool = True

    # Execution telemetry feedback loop
    execution_success: Optional[bool] = None
    execution_runtime_ms: Optional[float] = None
    execution_error: Optional[str] = None
    total_executions: int = 0
    successful_executions: int = 0

    # User feedback loop
    user_rating: Optional[float] = None  # 1.0 to 5.0
    total_ratings: int = 0
    average_rating: float = 5.0
    feedback_comments: List[str] = Field(default_factory=list)
    user_action: Optional[str] = None  # "approved" | "rejected" | "modified"

    # Dynamic composite ranking score & enterprise stability
    rank_score: float = 1.0
    stability_score: float = 1.0
    stability_tier: StabilityTier = StabilityTier.PROVISIONAL
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)


class LearningSummary(BaseModel):
    """Aggregated analytics on learned patterns."""
    total_patterns_learned: int = 0
    total_executions_recorded: int = 0
    overall_execution_success_rate: float = 1.0
    average_user_rating: float = 5.0
    top_domains: List[str] = Field(default_factory=list)
    high_confidence_patterns: int = 0
