from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import time
from typing import Optional

from agent.compiler.models.context import CompilerContext


@dataclass(frozen=True)
class AgentMetrics:
    agent_name: str
    execution_ms: float
    success: bool
    retries: int = 0
    error_message: Optional[str] = None


class BaseAgent(ABC):
    """Base abstract class for all compiler agents with standardized execution and metrics."""

    @abstractmethod
    def execute(self, context: CompilerContext) -> CompilerContext:
        """Execute the agent's core task synchronously."""
        pass

    def run_with_metrics(self, context: CompilerContext) -> tuple[CompilerContext, AgentMetrics]:
        """Runs execute() and captures latency, success, and error telemetry."""
        start = time.perf_counter()
        agent_name = self.__class__.__name__
        try:
            ctx = self.execute(context)
            elapsed = (time.perf_counter() - start) * 1000
            metrics = AgentMetrics(
                agent_name=agent_name,
                execution_ms=round(elapsed, 2),
                success=True,
            )
            # Store metrics in context metadata history
            history = ctx.metadata.setdefault("agent_metrics_history", [])
            history.append({
                "agent": metrics.agent_name,
                "execution_ms": metrics.execution_ms,
                "success": metrics.success,
            })
            return ctx, metrics
        except Exception as e:
            elapsed = (time.perf_counter() - start) * 1000
            metrics = AgentMetrics(
                agent_name=agent_name,
                execution_ms=round(elapsed, 2),
                success=False,
                error_message=str(e),
            )
            history = context.metadata.setdefault("agent_metrics_history", [])
            history.append({
                "agent": metrics.agent_name,
                "execution_ms": metrics.execution_ms,
                "success": metrics.success,
                "error": str(e),
            })
            raise
