from __future__ import annotations

import re
from typing import Optional

from agent.compiler.base.repair import BaseRepairStrategy
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.validation import DiagnosticIssue, ValidationIssueType


class SyntaxRepair(BaseRepairStrategy):
    """Repairs syntax anomalies such as unbalanced brackets, mismatched parens, and syntax errors."""

    def can_handle(self, diagnostic: DiagnosticIssue) -> bool:
        return diagnostic.issue_type == ValidationIssueType.SYNTAX or "SYNTAX" in diagnostic.issue_code

    def patch(self, code: str, diagnostic: DiagnosticIssue, context: CompilerContext) -> Optional[str]:
        # Case 1: Unbalanced square brackets in SQL
        if diagnostic.issue_code == "SQL_BRACKET_MISMATCH":
            open_count = code.count("[")
            close_count = code.count("]")
            if open_count > close_count:
                # Missing closing bracket after unclosed identifier: e.g. '[column_name FROM' -> '[column_name] FROM'
                patched = re.sub(r"\[([a-zA-Z0-9_]+)(?=\s+(?:FROM|AS|JOIN|ON|WHERE|SET|,|\)|;))", r"[\1]", code)
                if patched.count("[") == patched.count("]"):
                    return patched
                # Or append closing bracket
                return code + ("]" * (open_count - close_count))

        # Case 2: Unbalanced parentheses
        if diagnostic.issue_code == "SQL_PAREN_MISMATCH":
            open_count = code.count("(")
            close_count = code.count(")")
            if open_count > close_count:
                return code.rstrip() + ("\n)" * (open_count - close_count))

        # Case 3: Python syntax error on specific line
        if diagnostic.issue_code == "PY_SYNTAX_ERROR" and diagnostic.line_number is not None:
            lines = code.splitlines()
            idx = diagnostic.line_number - 1
            if 0 <= idx < len(lines):
                target_line = lines[idx]
                # Fix missing colon on def/if/for
                if re.match(r"^\s*(?:def|if|elif|else|for|while|with|try|except|finally)\b.*[^:]$", target_line):
                    lines[idx] = target_line + ":"
                    return "\n".join(lines)
                # Fix unclosed bracket on the line
                if target_line.count("[") > target_line.count("]"):
                    lines[idx] = target_line + ("]" * (target_line.count("[") - target_line.count("]")))
                    return "\n".join(lines)

        return None
