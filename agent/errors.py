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


class CompilationBlockedError(AgentError):
    """Raised when pipeline compilation or execution is blocked by compiler validation diagnostics."""
    def __init__(self, message: str, details: dict | None = None):
        super().__init__(AgentErrorCode.ETL_GENERATION_FAILED, message, recoverable=False)
        self.details = details or {}


class UnsupportedCapabilityError(CompilationBlockedError):
    """Raised when an operation semantic cannot be executed natively by the physical engine."""
    def __init__(self, message: str, engine: str | None = None, action: str | None = None):
        super().__init__(message, details={"engine": engine, "action": action})
        self.engine = engine
        self.action = action


class CircuitBreakerError(CompilationBlockedError):
    """Raised when runtime safety or dry-run validation exceeds configured failure thresholds."""
    def __init__(
        self,
        message: str,
        step_id: str | None = None,
        failed_count: int = 0,
        threshold: int | float = 0,
        action: str | None = None,
    ):
        super().__init__(
            message,
            details={
                "step_id": step_id,
                "failed_count": failed_count,
                "threshold": threshold,
                "action": action,
            },
        )
        self.step_id = step_id
        self.failed_count = failed_count
        self.threshold = threshold
        self.action = action


class ReconciliationError(CompilationBlockedError):
    """Raised when mathematical row-count reconciliation fails (input != clean + quarantine + dropped + merged)."""
    def __init__(self, message: str, input_rows: int = 0, accounted_rows: int = 0, delta: int = 0):
        super().__init__(
            message,
            details={
                "input_rows": input_rows,
                "accounted_rows": accounted_rows,
                "delta": delta,
            },
        )
        self.input_rows = input_rows
        self.accounted_rows = accounted_rows
        self.delta = delta


class GeneratedCodeIncompleteError(Exception):
    """Exception raised when generated code has incomplete transform functions."""
    def __init__(self, code: str, missing_functions: list[str]):
        self.code = code
        self.missing_functions = missing_functions
        super().__init__(f"Generated code is missing transform functions: {missing_functions}")


class MechanicalRepairViolation(Exception):
    """Raised when a post-processor/fixup removes transform functions or markers."""
    def __init__(self, message: str, fixup_name: str):
        super().__init__(message)
        self.fixup_name = fixup_name

    def to_dict(self) -> dict:
        return {
            "error": "MECHANICAL_REPAIR_VIOLATION",
            "fixup_name": self.fixup_name,
            "message": str(self),
        }


