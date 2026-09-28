from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class DiagnosticSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    BLOCKING = "blocking"


class ValidationIssueType(str, Enum):
    SYNTAX = "SYNTAX"
    IMPORT = "IMPORT"
    REFERENCE = "REFERENCE"
    SECURITY = "SECURITY"
    DIALECT = "DIALECT"
    PERFORMANCE = "PERFORMANCE"
    GOVERNANCE = "GOVERNANCE"
    SCHEMA_CONTRACT = "SCHEMA_CONTRACT"
    DESTRUCTIVE_UNAPPROVED = "DESTRUCTIVE_UNAPPROVED"


class DiagnosticIssue(BaseModel):
    issue_code: str
    issue_type: ValidationIssueType = ValidationIssueType.SYNTAX
    severity: DiagnosticSeverity = DiagnosticSeverity.ERROR
    message: str
    line_number: Optional[int] = None
    column_number: Optional[int] = None
    node_type: Optional[str] = None
    problematic_code: Optional[str] = None
    suggested_fix: Optional[str] = None


class ValidationResult(BaseModel):
    is_valid: bool
    target_engine: str
    issues: List[DiagnosticIssue] = Field(default_factory=list)
    has_security_hazards: bool = False
    has_performance_anti_patterns: bool = False
    invariants_satisfied: bool = True
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity in (DiagnosticSeverity.ERROR, DiagnosticSeverity.BLOCKING))

    @property
    def has_blocking_errors(self) -> bool:
        return any(i.severity == DiagnosticSeverity.BLOCKING for i in self.issues)
