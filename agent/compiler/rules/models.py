from __future__ import annotations

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field

from agent.compiler.models.actions import StructuredAction


class DecompositionStatus(str, Enum):
    RESOLVED = "resolved"        # High-confidence parsed transformation(s)
    AMBIGUOUS = "ambiguous"      # Missing business parameters / under-specified rule
    UNSUPPORTED = "unsupported"  # Proprietary algorithm / out-of-scope for compiler
    INVALID = "invalid"          # Violates schema or data type contracts


class RuleDecompositionResult(BaseModel):
    """Result of decomposing a business rule into validated transformation semantics."""
    status: DecompositionStatus
    actions: List[StructuredAction] = Field(default_factory=list)
    extraction_method: str = Field(default="none", description="deterministic | llm | none")
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    diagnostics: List[str] = Field(default_factory=list)
    original_rule: str = Field(..., description="The raw business rule text")
    missing_information: List[str] = Field(default_factory=list)
