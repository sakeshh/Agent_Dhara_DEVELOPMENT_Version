from __future__ import annotations

from enum import Enum
import time
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class RagEntityType(str, Enum):
    BUSINESS_RULE = "business_rule"
    DATA_CONTRACT = "data_contract"
    IR_PATTERN = "ir_pattern"
    DQ_RULE = "dq_rule"
    COMPANY_STANDARD = "company_standard"


class RagItem(BaseModel):
    id: str
    entity_type: RagEntityType
    title: str
    content: str
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    vector: Optional[List[float]] = None
    created_at: float = Field(default_factory=time.time)


class RagSearchResult(BaseModel):
    item: RagItem
    score: float
    relevance_reason: str = ""
