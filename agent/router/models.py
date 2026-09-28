from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from agent.ir.models import IntentComplexity


class RouteTarget(str, Enum):
    COMPILER_DIRECT = "compiler_direct"          # Simple: deterministic fast path, zero LLM, <10ms
    LLM_IR_PLANNER = "llm_ir_planner"            # Moderate: schema-grounded LLM IR planning
    MULTI_AGENT_PLANNER = "multi_agent_planner"  # Complex: SCD2, CDC, incremental merge, ADF orchestration


class IntentClassification(BaseModel):
    intent: str
    complexity: IntentComplexity
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    route: RouteTarget
    reasoning: str = ""
    detected_operations: List[str] = Field(default_factory=list)
    matched_patterns: List[str] = Field(default_factory=list)
    latency_ms: float = 0.0
