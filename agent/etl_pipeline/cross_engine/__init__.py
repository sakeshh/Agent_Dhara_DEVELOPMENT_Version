"""
agent/etl_pipeline/cross_engine/__init__.py

Package initialization for cross-engine compilation, capability registration,
and semantic parity governance.
"""
from agent.etl_pipeline.cross_engine.capability_models import (
    CapabilityDecision,
    CapabilityStatus,
    EngineCapabilityKey,
    ExpectedEngineDeviation,
    FailureCategory,
    NormalizedFailure,
)
from agent.etl_pipeline.cross_engine.runtime_profiles import (
    AdfRuntimeProfile,
    EngineRuntimeProfile,
    PandasRuntimeProfile,
    SparkRuntimeProfile,
    SqlRuntimeProfile,
    compute_runtime_profile_hash,
)
from agent.etl_pipeline.cross_engine.capability_registry import (
    EngineCapabilityRegistry,
    get_global_capability_registry,
)
from agent.etl_pipeline.cross_engine.parity_models import (
    ComparatorResult,
    CrossEngineParityReport,
    ParityComparatorType,
)

__all__ = [
    "CapabilityDecision",
    "CapabilityStatus",
    "EngineCapabilityKey",
    "ExpectedEngineDeviation",
    "FailureCategory",
    "NormalizedFailure",
    "EngineRuntimeProfile",
    "SparkRuntimeProfile",
    "SqlRuntimeProfile",
    "AdfRuntimeProfile",
    "PandasRuntimeProfile",
    "compute_runtime_profile_hash",
    "EngineCapabilityRegistry",
    "get_global_capability_registry",
    "ParityComparatorType",
    "ComparatorResult",
    "CrossEngineParityReport",
]
