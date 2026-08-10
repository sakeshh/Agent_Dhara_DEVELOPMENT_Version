"""
Abstract definitions and models for Execution targets and executors.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class ExecutionEngine(str, Enum):
    AZURE_SQL = "AZURE_SQL"
    FABRIC_WAREHOUSE = "FABRIC_WAREHOUSE"
    FABRIC_PYSPARK = "FABRIC_PYSPARK"
    FABRIC_DELTA = "FABRIC_DELTA"
    LOCAL_PYTHON = "LOCAL_PYTHON"


class ExecutionPlan(BaseModel):
    plan_id: str
    engine: ExecutionEngine
    code: str
    dialect: str = "tsql"
    target_tables: List[str] = []
    requires_approval: bool = False
    destructive_ops: List[str] = []
    metadata: Dict[str, Any] = {}


class ExecutionResult(BaseModel):
    ok: bool
    engine: ExecutionEngine
    run_id: str
    rows_affected: int = 0
    duration_ms: float = 0.0
    committed: bool = False
    error: Optional[str] = None
    batch_results: List[Dict[str, Any]] = []
    artifacts: Dict[str, Any] = {}


class Executor(ABC):
    def __init__(self, **kwargs) -> None:
        pass

    @abstractmethod
    def engine_type(self) -> ExecutionEngine:
        """Return the target execution engine type."""
        pass

    @abstractmethod
    def execute(self, plan: ExecutionPlan, **kwargs) -> ExecutionResult:
        """Run the execution plan and return standardized ExecutionResult."""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Returns True if connection is verified, False otherwise."""
        pass
