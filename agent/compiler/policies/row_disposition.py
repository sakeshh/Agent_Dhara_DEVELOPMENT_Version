from __future__ import annotations

from enum import Enum


class RowDisposition(str, Enum):
    """Declares the physical data effect of an action on row cardinality."""
    RETAIN = "retain"
    TRANSFORM = "transform"
    REMOVE = "remove"
    QUARANTINE = "quarantine"
    MERGE = "merge"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
