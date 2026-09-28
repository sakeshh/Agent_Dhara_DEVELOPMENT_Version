from __future__ import annotations

import time
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class CompilerMetrics(BaseModel):
    planner_latency_ms: float = 0.0
    validation_latency_ms: float = 0.0
    repair_latency_ms: float = 0.0
    compile_latency_ms: float = 0.0
    total_compilation_time_ms: float = 0.0
    total_latency_ms: float = 0.0
    target_language: str = "sql"
    route_taken: str = "compiler_direct"  # "compiler_direct" | "llm_ir_planner" | "multi_agent_planner"
    retries: int = 0
    success: bool = True
    repairs_count: int = 0
    timestamp: float = Field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()
