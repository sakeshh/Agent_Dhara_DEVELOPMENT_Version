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


class AzureSqlDatabaseRuntimeProfile(EngineRuntimeProfile):
    """Runtime profile capturing authoritative Azure SQL Database execution settings."""
    engine_name: str = "azure_sql_database"
    engine_version: str = "12.0.2000.8"               # SERVERPROPERTY('ProductVersion')
    engine_edition: str = "5"                         # SERVERPROPERTY('EngineEdition') (5 = SQL Database)
    database_engine_version: str = "Microsoft SQL Azure (RTM) - 12.0.2000.8" # @@VERSION
    dialect: str = "tsql"
    compatibility_level: str = "160"                  # sys.databases compatibility_level
    database_collation: str = "SQL_Latin1_General_CP1_CI_AS" # sys.databases collation_name
    server_collation: str = "SQL_Latin1_General_CP1_CI_AS"   # SERVERPROPERTY('Collation')
    ansi_nulls_enabled: bool = True
    ansi_warnings_enabled: bool = True
    session_timezone: str = "UTC"
    case_sensitive: bool = False                      # Derived from database_collation
    three_valued_logic: bool = True
    decimal_overflow_mode: str = "ERROR"              # ARITHABORT ON
    regex_capabilities: str = "LIKE_WILDCARDS"         # LIKE, PATINDEX; no native regex

    @classmethod
    def from_captured_evidence(
        cls,
        engine_version: str,
        engine_edition: str,
        database_engine_version: str,
        compatibility_level: str,
        database_collation: str,
        server_collation: Optional[str] = None,
        ansi_nulls_enabled: bool = True,
        ansi_warnings_enabled: bool = True,
        session_timezone: str = "UTC",
        three_valued_logic: bool = True,
        decimal_overflow_mode: str = "ERROR",
    ) -> AzureSqlDatabaseRuntimeProfile:
        """Construct a validated profile dynamically from captured SQL query evidence."""
        case_sens = "_CS_" in database_collation.upper() or "_BIN" in database_collation.upper()
        return cls(
            engine_name="azure_sql_database",
            engine_version=str(engine_version),
            engine_edition=str(engine_edition),
            database_engine_version=str(database_engine_version),
            dialect="tsql",
            compatibility_level=str(compatibility_level),
            database_collation=str(database_collation),
            server_collation=str(server_collation or database_collation),
            ansi_nulls_enabled=ansi_nulls_enabled,
            ansi_warnings_enabled=ansi_warnings_enabled,
            session_timezone=session_timezone,
            case_sensitive=case_sens,
            three_valued_logic=three_valued_logic,
            decimal_overflow_mode=decimal_overflow_mode,
        )


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
