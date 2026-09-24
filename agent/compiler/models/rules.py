from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class RuleScope(str, Enum):
    GLOBAL = "global"
    DATASET = "dataset"
    COLUMN = "column"


class BusinessRule(BaseModel):
    rule_id: str
    name: str
    scope: RuleScope = RuleScope.DATASET
    dataset_name: Optional[str] = None
    column_name: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True
    never_drop_rows: bool = False
    priority: int = 100
    description: Optional[str] = None
