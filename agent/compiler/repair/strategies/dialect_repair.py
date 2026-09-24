from __future__ import annotations

import re
from typing import Optional

from agent.compiler.base.repair import BaseRepairStrategy
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.validation import DiagnosticIssue, ValidationIssueType


class DialectRepair(BaseRepairStrategy):
    """Repairs SQL/Spark dialect incompatibilities and translates vendor-specific functions."""

    def can_handle(self, diagnostic: DiagnosticIssue) -> bool:
        return diagnostic.issue_type == ValidationIssueType.DIALECT or "DIALECT" in diagnostic.issue_code

    def patch(self, code: str, diagnostic: DiagnosticIssue, context: CompilerContext) -> Optional[str]:
        patched = code
        modified = False

        # 1. Replace MySQL IFNULL with T-SQL ISNULL or COALESCE
        if "IFNULL" in diagnostic.issue_code or "IFNULL" in code.upper():
            patched = re.sub(r"\bIFNULL\s*\(", "COALESCE(", patched, flags=re.IGNORECASE)
            modified = True

        # 2. Replace Oracle NVL with COALESCE
        if "NVL" in diagnostic.issue_code or "NVL" in code.upper():
            patched = re.sub(r"\bNVL\s*\(", "COALESCE(", patched, flags=re.IGNORECASE)
            modified = True

        return patched if modified else None
