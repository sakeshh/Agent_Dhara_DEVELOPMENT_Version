from __future__ import annotations

from enum import Enum


class AgentErrorCode(str, Enum):
    # Source load/connectivity errors
    SOURCE_LOAD_FAILED = "SOURCE_LOAD_FAILED"
    CONNECTION_FAILED = "CONNECTION_FAILED"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    PERMISSION_DENIED = "PERMISSION_DENIED"

    # Assessment stage errors
    PROFILING_FAILED = "PROFILING_FAILED"
    GX_VALIDATION_FAILED = "GX_VALIDATION_FAILED"
    SCHEMA_ENRICHMENT_FAILED = "SCHEMA_ENRICHMENT_FAILED"

    # ETL & SQL generation/execution errors
    ETL_PLAN_FAILED = "ETL_PLAN_FAILED"
    ETL_GENERATION_FAILED = "ETL_GENERATION_FAILED"
    SQL_VALIDATION_FAILED = "SQL_VALIDATION_FAILED"
    SQL_EXECUTION_FAILED = "SQL_EXECUTION_FAILED"
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
    INVALID_PHASE_TRANSITION = "INVALID_PHASE_TRANSITION"
    PATH_TRAVERSAL_BLOCKED = "PATH_TRAVERSAL_BLOCKED"
    VERSION_CONFLICT = "VERSION_CONFLICT"
    DQ_GATE_BLOCKER = "DQ_GATE_BLOCKER"
    SEMANTIC_OVERRIDE_CONFLICT = "SEMANTIC_OVERRIDE_CONFLICT"

    # Infrastructure errors
    LLM_CALL_FAILED = "LLM_CALL_FAILED"
    TIMEOUT = "TIMEOUT"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class AgentError(Exception):
    """
    Unified base exception for all Agent Dhara pipeline and ETL orchestrator errors.
    """
    def __init__(self, code: AgentErrorCode, message: str, recoverable: bool = False):
        super().__init__(message)
        self.code = code
        self.message = message
        self.recoverable = recoverable

    def to_dict(self) -> dict:
        return {
            "code": self.code.value,
            "message": self.message,
            "recoverable": self.recoverable,
        }

    def __str__(self) -> str:
        return f"[{self.code.value}] {self.message} (recoverable={self.recoverable})"


class ConnectorConfigError(Exception):
    """Raised when a connector is referenced but credentials are missing."""
    pass

