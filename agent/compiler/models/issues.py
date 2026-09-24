from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class IssueSeverity(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class IssueCategory(str, Enum):
    MISSING_VALUE = "missing_value"
    TYPE_MISMATCH = "type_mismatch"
    FORMAT_ANOMALY = "format_anomaly"
    OUTLIER = "outlier"
    DUPLICATE = "duplicate"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    PII_EXPOSURE = "pii_exposure"
    UNTRIMMED_WHITESPACE = "untrimmed_whitespace"
    OTHER = "other"


class Issue(BaseModel):
    issue_id: str
    dataset_name: str
    column_name: Optional[str] = None
    category: IssueCategory = IssueCategory.OTHER
    severity: IssueSeverity = IssueSeverity.MEDIUM
    message: str
    affected_rows: int = 0
    affected_percentage: float = 0.0
    detected_values: List[Any] = Field(default_factory=list)
    suggested_action: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
