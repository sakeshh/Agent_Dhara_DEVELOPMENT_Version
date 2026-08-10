from __future__ import annotations
from enum import IntEnum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

class RuleProvenance(IntEnum):
    BUSINESS_RULE = 1
    SEMANTIC_LAYER = 2
    AUTO_DETECTED = 3

class TaggedRule(BaseModel):
    dataset: str
    column: str
    issue_type: str
    action: str
    provenance: RuleProvenance
    source_detail: str = ""
    confidence: float = 1.0
    metadata: Dict[str, Any] = Field(default_factory=dict)

class RuleConflict(BaseModel):
    dataset: str
    column: str
    issue_type: str
    rules: List[TaggedRule]
    auto_resolved: bool = False
    resolution: Optional[str] = None
