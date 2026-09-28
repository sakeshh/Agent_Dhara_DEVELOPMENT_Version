from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class DuplicateType(str, Enum):
    EXACT_ROW = "exact_row"
    BUSINESS_KEY = "business_key"
    NEAR_DUPLICATE = "near_duplicate"
    CROSS_SOURCE = "cross_source"


class DuplicateDisposition(str, Enum):
    FLAG = "flag"
    QUARANTINE = "quarantine"
    REMOVE = "remove"
    REMOVE_REDUNDANT = "remove_redundant"
    MERGE = "merge"
    REQUIRE_APPROVAL = "require_approval"


class SurvivorStrategy(str, Enum):
    MOST_COMPLETE = "most_complete"
    LATEST = "latest"
    NEWEST = "newest"
    EARLIEST = "earliest"
    OLDEST = "oldest"
    HIGHEST_QUALITY = "highest_quality"
    SOURCE_PRIORITY = "source_priority"
    USER_DEFINED = "user_defined"


class DuplicatePolicy(BaseModel):
    dataset_name: str
    duplicate_type: DuplicateType = DuplicateType.BUSINESS_KEY
    keys: List[str] = Field(default_factory=list)
    approved_key: Optional[List[str]] = None
    disposition: DuplicateDisposition = DuplicateDisposition.REMOVE
    survivor_strategy: SurvivorStrategy = SurvivorStrategy.MOST_COMPLETE
    timestamp_column: Optional[str] = None
    expected_redundant_rows: int = 0
    requires_approval: bool = False
    approved: bool = False

    def validate(self) -> tuple[bool, List[str]]:
        errors = []
        effective_keys = self.approved_key or self.keys
        disp_val = self.disposition.value if hasattr(self.disposition, "value") else str(self.disposition)
        if disp_val in ("remove", "remove_redundant", "merge"):
            if not effective_keys and self.duplicate_type != DuplicateType.EXACT_ROW:
                errors.append(f"Destructive deduplication on '{self.dataset_name}' requires an approved business key.")
        return (len(errors) == 0, errors)

    def validate_executable(self) -> tuple[bool, List[str]]:
        return self.validate()


# Backward compatibility
DuplicateResolutionPlan = DuplicatePolicy
