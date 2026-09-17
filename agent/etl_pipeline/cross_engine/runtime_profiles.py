"""
agent/etl_pipeline/cross_engine/runtime_profiles.py

Deterministic, versioned runtime profile models and canonical profile hashing.
Enforces the invariant: Any runtime setting capable of altering execution
semantics MUST alter the runtime_profile_hash.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class EngineRuntimeProfile(BaseModel):
    """Base runtime profile defining core common execution environment settings."""
    engine_name: str = "generic"
    engine_version: str = "1.0.0"
    canonicalization_version: str = "CANONICAL_SCALAR_V1"
    session_timezone: str = "UTC"
    case_sensitive: bool = True
    extra_semantics: Dict[str, Any] = Field(default_factory=dict)

    def to_canonical_dict(self) -> Dict[str, Any]:
        """Convert profile to an alphabetically sorted, JSON-serializable dictionary."""
        d = self.model_dump()
        return d

    def compute_hash(self) -> str:
        """Compute deterministic SHA-256 hash of canonical JSON representation."""
        canonical_str = json.dumps(self.to_canonical_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()


class PandasRuntimeProfile(EngineRuntimeProfile):
    """Runtime profile capturing Pandas execution environment settings."""
    engine_name: str = "pandas"
    engine_version: str = "2.3.3"
    pandas_version: str = "2.3.3"
    python_version: str = "3.13"
    numpy_version: str = "2.3.5"
    copy_on_write: bool = False
    datetime_unit: str = "ns"


class SparkRuntimeProfile(EngineRuntimeProfile):
    """Runtime profile capturing PySpark / Spark SQL Catalyst execution environment settings."""
    engine_name: str = "pyspark"
    engine_version: str = "3.5.1"
    spark_version: str = "3.5.1"
    python_version: str = "3.13"

    ansi_enabled: bool = True
    legacy_time_parser_policy: str = "CORRECTED"
    adaptive_query_execution: bool = True
    decimal_allow_precision_loss: bool = False
    arrow_enabled: bool = True
    regex_engine_profile: str = "JAVA_RESTRICTED_V1"


class SqlRuntimeProfile(EngineRuntimeProfile):
    """Runtime profile capturing relational SQL dialect execution settings."""
    engine_name: str = "sql"
    dialect: str = "spark_sql"           # "spark_sql", "tsql", "ansi_standard"
    database_engine_version: str = "3.5.1"
    compatibility_level: Optional[str] = None
    collation: str = "UTF-8"
    ansi_nulls_enabled: bool = True
    ansi_warnings_enabled: bool = True
    decimal_overflow_mode: str = "ERROR" # "ERROR", "NULL", "CLAMP"
    regex_capabilities: str = "STANDARD"


class AdfRuntimeProfile(EngineRuntimeProfile):
    """Runtime profile capturing Azure Data Factory Mapping Data Flow environment settings."""
    engine_name: str = "adf"
    runtime_type: str = "mapping_data_flow"
    dataflow_version: str = "2024-01"
    compute_type: str = "General"
    core_count: int = 8
    orchestration_only_mode: bool = False


def compute_runtime_profile_hash(profile: EngineRuntimeProfile) -> str:
    """Convenience helper to compute the deterministic SHA-256 profile hash."""
    return profile.compute_hash()
