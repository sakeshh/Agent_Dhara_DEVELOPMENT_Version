from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from agent.compiler.models.tier3_resolution import Tier3ResolutionProposal


class ValidationSeverity(str, Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class ValidationCode(str, Enum):
    # Schema & Identifiers
    UNKNOWN_DATASET = "UNKNOWN_DATASET"
    UNKNOWN_COLUMN = "UNKNOWN_COLUMN"
    AMBIGUOUS_DATASET = "AMBIGUOUS_DATASET"
    INTERNAL_COLUMN_PROTECTED = "INTERNAL_COLUMN_PROTECTED"

    # Type & Operator Compatibility
    TYPE_MISMATCH = "TYPE_MISMATCH"
    INVALID_OPERATOR_FOR_TYPE = "INVALID_OPERATOR_FOR_TYPE"
    INVALID_LITERAL = "INVALID_LITERAL"
    LOSSY_CONVERSION_UNAPPROVED = "LOSSY_CONVERSION_UNAPPROVED"
    EMPTY_VALUE_LIST = "EMPTY_VALUE_LIST"
    INVALID_RANGE_BOUNDS = "INVALID_RANGE_BOUNDS"

    # Contract & Parameters
    MISSING_PARAMETER = "MISSING_PARAMETER"
    ACTION_TYPE_INCOMPATIBLE = "ACTION_TYPE_INCOMPATIBLE"

    # Safety & Resource Bounds
    AST_DEPTH_EXCEEDED = "AST_DEPTH_EXCEEDED"
    AST_NODE_COUNT_EXCEEDED = "AST_NODE_COUNT_EXCEEDED"
    IN_LIST_SIZE_EXCEEDED = "IN_LIST_SIZE_EXCEEDED"
    EXCESSIVE_TARGET_COLUMNS = "EXCESSIVE_TARGET_COLUMNS"
    PROHIBITED_DISPOSITION = "PROHIBITED_DISPOSITION"
    UNAUTHORIZED_DESTRUCTIVE_ACTION = "UNAUTHORIZED_DESTRUCTIVE_ACTION"

    # Conflicts & Graph
    CONFLICTING_WRITE = "CONFLICTING_WRITE"
    DUPLICATE_ACTION = "DUPLICATE_ACTION"
    DEPENDENCY_CYCLE = "DEPENDENCY_CYCLE"
    OVERLAPPING_DESTRUCTIVE_ACTION = "OVERLAPPING_DESTRUCTIVE_ACTION"
    READ_WRITE_HAZARD = "READ_WRITE_HAZARD"
    POTENTIAL_CONFLICT_REQUIRES_REVIEW = "POTENTIAL_CONFLICT_REQUIRES_REVIEW"


@dataclass(frozen=True)
class ValidationFinding:
    validator: str
    code: ValidationCode
    severity: ValidationSeverity
    message: str
    proposal_id: Optional[str] = None
    dataset: Optional[str] = None
    column: Optional[str] = None
    ast_path: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Tier3ValidationResult:
    valid: bool
    findings: List[ValidationFinding] = field(default_factory=list)
    normalized_proposal: Optional[Tier3ResolutionProposal] = None
    proposal_fingerprint: Optional[str] = None

    @property
    def is_valid(self) -> bool:
        return self.valid and not self.has_errors

    @property
    def has_errors(self) -> bool:
        return any(f.severity == ValidationSeverity.ERROR for f in self.findings)

    @property
    def errors(self) -> List[ValidationFinding]:
        return [f for f in self.findings if f.severity == ValidationSeverity.ERROR]

    @property
    def warnings(self) -> List[ValidationFinding]:
        return [f for f in self.findings if f.severity == ValidationSeverity.WARNING]
