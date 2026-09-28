"""
Agent Dhara Compiler Runtime Safety & Defensive Execution (Phase 4).
Includes:
- Conversion Error Tracking (replacing blind TRY_CAST)
- Circuit Breaker threshold enforcement
- In-memory dry run / pre-flight validation on sample data
- Defensive coercion (division-by-zero, bounds checking)
"""

from agent.compiler.safety.models import (
    ConversionFailure,
    DryRunReport,
    SafetyStatus,
    StepSafetyMetrics,
)
from agent.compiler.safety.circuit_breaker import CircuitBreaker
from agent.compiler.safety.dry_run import InMemoDryRunSimulator

__all__ = [
    "ConversionFailure",
    "DryRunReport",
    "SafetyStatus",
    "StepSafetyMetrics",
    "CircuitBreaker",
    "InMemoDryRunSimulator",
]
