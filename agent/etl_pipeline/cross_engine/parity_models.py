"""
agent/etl_pipeline/cross_engine/parity_models.py

Data models for category-specific parity comparison results and
cross-engine parity execution reporting.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from agent.etl_pipeline.cross_engine.capability_models import EngineCapabilityKey


class ParityComparatorType(str, Enum):
    """Semantic category defining the comparator policy."""
    EXACT_BYTE = "exact_byte"
    DECIMAL = "decimal"
    FLOATING_TOLERANCE = "floating_tolerance"
    TEMPORAL = "temporal"
    BOOLEAN = "boolean"
    SCHEMA = "schema"
    ROW_OCCURRENCE = "row_occurrence"
    VALIDATION_METRIC = "validation_metric"
    FAILURE = "failure"


class ComparatorResult(BaseModel):
    """Granular comparison outcome for a single field or invariant."""
    comparator_type: ParityComparatorType
    passed: bool
    field_name: str
    expected: Any = None
    actual: Any = None
    deviation_id: Optional[str] = None
    details: Dict[str, Any] = Field(default_factory=dict)


class CrossEngineParityReport(BaseModel):
    """
    Standardized multi-engine certification report separating
    specification conformance from differential reference parity.
    """
    fixture_id: str
    graph_hash: str
    reference_engine: str = "pandas"
    target_engine: str
    capability_key: EngineCapabilityKey
    specification_conformance: bool
    differential_parity: bool
    comparator_results: Tuple[ComparatorResult, ...] = Field(default_factory=tuple)
    expected_deviations: Tuple[str, ...] = Field(default_factory=tuple)
    unexpected_deviations: Tuple[str, ...] = Field(default_factory=tuple)

    @property
    def overall_passed(self) -> bool:
        """Overall pass requires both specification conformance and reference parity without unexpected deviations."""
        return (
            self.specification_conformance
            and self.differential_parity
            and len(self.unexpected_deviations) == 0
        )
