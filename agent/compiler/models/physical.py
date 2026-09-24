from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ExecutionEngine(str, Enum):
    PYTHON = "python"
    AZURE_SQL = "sql"
    PYSPARK = "pyspark"
    ADF = "adf"
    FABRIC = "fabric"
    DATABRICKS = "databricks"


class PhysicalStep(BaseModel):
    step_id: str
    logical_step_id: str
    dataset_name: str
    strategy_name: str
    template_name: str
    target_columns: List[str] = Field(default_factory=list)
    template_parameters: Dict[str, Any] = Field(default_factory=dict)
    execution_order: int = 0
    intermediate_identifier: Optional[str] = None
    quarantine_table_or_df: Optional[str] = None
    description: Optional[str] = None


class PhysicalPlan(BaseModel):
    plan_id: str
    logical_plan_id: str
    target_engine: ExecutionEngine
    dataset_names: List[str] = Field(default_factory=list)
    steps: List[PhysicalStep] = Field(default_factory=list)
    io_config: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def get_steps_for_dataset(self, dataset_name: str) -> List[PhysicalStep]:
        return [s for s in self.steps if s.dataset_name == dataset_name]
