from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ActionType(str, Enum):
    TRIM = "trim"
    LOWERCASE = "lowercase"
    UPPERCASE = "uppercase"
    ZERO_TO_NULL = "zero_to_null"
    REPLACE_SENTINELS = "replace_sentinels"
    CAST_TYPE = "cast_type"
    COERCE_NUMERIC = "coerce_numeric"
    PARSE_DATES = "parse_dates"
    SANITIZE_EMAIL = "sanitize_email"
    NORMALIZE_PHONE = "normalize_phone"
    MASK_PHONE = "mask_phone"
    RANGE_CLIP = "range_clip"
    CAP_OUTLIERS = "cap_outliers"
    STANDARDIZE_BOOLEAN = "standardize_boolean"
    FILL_NULLS = "fill_nulls"
    DROP_COLUMN = "drop_column"
    DEDUPLICATE = "deduplicate"
    VALIDATE_REFERENTIAL_INTEGRITY = "validate_referential_integrity"
    CUSTOM_EXPRESSION = "custom_expression"


class DataEffect(str, Enum):
    TRANSFORM_IN_PLACE = "transform_in_place"
    CREATE_COLUMN = "create_column"
    DROP_COLUMN = "drop_column"
    FILTER_ROWS = "filter_rows"
    PARTITION_ROWS = "partition_rows"
    QUARANTINE_ROWS = "quarantine_rows"


class LogicalStep(BaseModel):
    step_id: str
    dataset_name: str
    action: ActionType
    target_columns: List[str] = Field(default_factory=list)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    effect: DataEffect = DataEffect.TRANSFORM_IN_PLACE
    priority: int = 100
    confidence: float = 1.0  # 0.0 - 1.0
    requires_user_approval: bool = False
    source_issue_ids: List[str] = Field(default_factory=list)
    description: Optional[str] = None
    fused_operations: List[ActionType] = Field(default_factory=list)


class LogicalPlan(BaseModel):
    plan_id: str
    dataset_names: List[str] = Field(default_factory=list)
    steps: List[LogicalStep] = Field(default_factory=list)
    invariants: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def get_steps_for_dataset(self, dataset_name: str) -> List[LogicalStep]:
        return [s for s in self.steps if s.dataset_name == dataset_name]
