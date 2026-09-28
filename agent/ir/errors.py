from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class IrErrorCode(str, Enum):
    MISSING_TABLE = "MISSING_TABLE"
    MISSING_COLUMN = "MISSING_COLUMN"
    INVALID_JOIN = "INVALID_JOIN"
    INVALID_DATATYPE = "INVALID_DATATYPE"
    INVALID_RULE = "INVALID_RULE"
    UNSUPPORTED_OPERATION = "UNSUPPORTED_OPERATION"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    VERSION_INCOMPATIBLE = "VERSION_INCOMPATIBLE"


class IrError(BaseModel):
    error_code: IrErrorCode
    message: str
    target_table: Optional[str] = None
    target_column: Optional[str] = None
    suggestions: List[str] = Field(default_factory=list)
    details: Dict[str, Any] = Field(default_factory=dict)
    severity: str = "error"  # "error" | "warning"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_code": self.error_code.value,
            "message": self.message,
            "target_table": self.target_table,
            "target_column": self.target_column,
            "suggestions": self.suggestions,
            "details": self.details,
            "severity": self.severity,
        }


class IrValidationResult(BaseModel):
    valid: bool = True
    errors: List[IrError] = Field(default_factory=list)
    warnings: List[IrError] = Field(default_factory=list)

    def add_error(
        self,
        error_code: IrErrorCode,
        message: str,
        target_table: Optional[str] = None,
        target_column: Optional[str] = None,
        suggestions: Optional[List[str]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.valid = False
        self.errors.append(
            IrError(
                error_code=error_code,
                message=message,
                target_table=target_table,
                target_column=target_column,
                suggestions=suggestions or [],
                details=details or {},
                severity="error",
            )
        )

    def add_warning(
        self,
        error_code: IrErrorCode,
        message: str,
        target_table: Optional[str] = None,
        target_column: Optional[str] = None,
        suggestions: Optional[List[str]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.warnings.append(
            IrError(
                error_code=error_code,
                message=message,
                target_table=target_table,
                target_column=target_column,
                suggestions=suggestions or [],
                details=details or {},
                severity="warning",
            )
        )
