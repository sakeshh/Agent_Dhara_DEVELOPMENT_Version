"""
Registry for target execution engines.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Type

from agent.execution.execution_target import ExecutionEngine, ExecutionPlan, ExecutionResult, Executor

logger = logging.getLogger("agent.execution.registry")

_REGISTRY: Dict[ExecutionEngine, Type[Executor]] = {}


def register_executor(engine: ExecutionEngine, executor_cls: Type[Executor]) -> None:
    """Register an executor class for an execution engine."""
    _REGISTRY[engine] = executor_cls
    logger.info("Registered executor %s for engine %s", executor_cls.__name__, engine.value)


def get_executor(engine: ExecutionEngine, **kwargs) -> Executor:
    """Get an instantiated executor for the engine."""
    if engine not in _REGISTRY:
        raise ValueError(f"No executor registered for engine: {engine}")
    return _REGISTRY[engine](**kwargs)


def execute_plan(plan: ExecutionPlan, **kwargs) -> ExecutionResult:
    """Dispatches execution to the registered executor for the plan's engine."""
    executor = get_executor(plan.engine, **kwargs)
    return executor.execute(plan, **kwargs)
