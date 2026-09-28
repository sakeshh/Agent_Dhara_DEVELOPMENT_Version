from __future__ import annotations

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


class KeySource(str, Enum):
    USER_APPROVED = "user_approved"
    DATA_CONTRACT = "data_contract"
    DATABASE_PK = "database_pk"
    UNIQUE_INDEX = "unique_index"
    KNOWLEDGE_BASE = "knowledge_base"
    INFERRED = "inferred"
    HUMAN_APPROVAL_PENDING = "human_approval_pending"


class KeyCandidate(BaseModel):
    columns: List[str]
    source: KeySource
    confidence: float
    null_ratio: float = 0.0
    uniqueness_ratio: float = 0.0
    is_composite: bool = False
    evidence: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class KeyResolution(BaseModel):
    columns: List[str]
    source: KeySource
    confidence: float
    approved: bool
    evidence: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)

    @property
    def is_usable_for_destructive_dedup(self) -> bool:
        """Destructive deduplication requires either explicit approval or >= 95% confidence from authoritative source."""
        if self.approved:
            return True
        if self.confidence >= 95.0 and self.source in (
            KeySource.USER_APPROVED,
            KeySource.DATA_CONTRACT,
            KeySource.DATABASE_PK,
            KeySource.UNIQUE_INDEX,
        ):
            return True
        return False
