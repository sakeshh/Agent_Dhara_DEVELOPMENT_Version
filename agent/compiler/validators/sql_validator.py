from __future__ import annotations

import re
from typing import List

from agent.compiler.base.validation import BaseValidationAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.validation import DiagnosticIssue, DiagnosticSeverity, ValidationIssueType, ValidationResult


class SQLValidator(BaseValidationAgent):
    """Linter and semantic validator for generated Azure SQL / T-SQL scripts."""

    def validate(self, artifact: CodeArtifact, context: CompilerContext) -> ValidationResult:
        issues: List[DiagnosticIssue] = []
        sql = artifact.content
        has_security = False

        # 1. Delimiter & Bracket balance check
        open_brackets = sql.count("[")
        close_brackets = sql.count("]")
        if open_brackets != close_brackets:
            issues.append(
                DiagnosticIssue(
                    issue_code="SQL_BRACKET_MISMATCH",
                    issue_type=ValidationIssueType.SYNTAX,
                    severity=DiagnosticSeverity.ERROR,
                    message=f"Unbalanced square brackets in T-SQL script: {open_brackets} '[' vs {close_brackets} ']'",
                    suggested_fix="Ensure all identifiers wrapped in [ ... ] are properly closed.",
                )
            )

        # Parentheses balance (ignoring comments)
        lines = [line.split("--")[0] for line in sql.splitlines()]
        clean_code = " \n ".join(lines)
        open_parens = clean_code.count("(")
        close_parens = clean_code.count(")")
        if open_parens != close_parens:
            issues.append(
                DiagnosticIssue(
                    issue_code="SQL_PAREN_MISMATCH",
                    issue_type=ValidationIssueType.SYNTAX,
                    severity=DiagnosticSeverity.ERROR,
                    message=f"Unbalanced parentheses in T-SQL script: {open_parens} '(' vs {close_parens} ')'",
                    suggested_fix="Check subqueries, function calls, and CTE parenthesization.",
                )
            )

        # 2. Block and Transaction balance
        begin_try = len(re.findall(r"\bBEGIN\s+TRY\b", clean_code, re.IGNORECASE))
        end_try = len(re.findall(r"\bEND\s+TRY\b", clean_code, re.IGNORECASE))
        begin_catch = len(re.findall(r"\bBEGIN\s+CATCH\b", clean_code, re.IGNORECASE))
        end_catch = len(re.findall(r"\bEND\s+CATCH\b", clean_code, re.IGNORECASE))

        if begin_try != end_try or begin_catch != end_catch:
            issues.append(
                DiagnosticIssue(
                    issue_code="SQL_TRY_CATCH_MISMATCH",
                    issue_type=ValidationIssueType.SYNTAX,
                    severity=DiagnosticSeverity.ERROR,
                    message="Unbalanced TRY/CATCH blocks in stored procedure.",
                )
            )

        # 3. Dialect checks: MySQL/Oracle functions in Azure SQL
        if re.search(r"\bIFNULL\s*\(", clean_code, re.IGNORECASE):
            issues.append(
                DiagnosticIssue(
                    issue_code="SQL_DIALECT_IFNULL",
                    issue_type=ValidationIssueType.DIALECT,
                    severity=DiagnosticSeverity.ERROR,
                    message="Unsupported function IFNULL() in Azure SQL. Use ISNULL() or COALESCE().",
                    suggested_fix="Replace IFNULL(...) with ISNULL(...) or COALESCE(...).",
                )
            )

        if re.search(r"\bNVL\s*\(", clean_code, re.IGNORECASE):
            issues.append(
                DiagnosticIssue(
                    issue_code="SQL_DIALECT_NVL",
                    issue_type=ValidationIssueType.DIALECT,
                    severity=DiagnosticSeverity.ERROR,
                    message="Unsupported Oracle function NVL() in Azure SQL. Use COALESCE().",
                    suggested_fix="Replace NVL(...) with COALESCE(...).",
                )
            )

        # 4. Security checks
        if "xp_cmdshell" in clean_code.lower():
            has_security = True
            issues.append(
                DiagnosticIssue(
                    issue_code="SQL_SEC_XP_CMDSHELL",
                    issue_type=ValidationIssueType.SECURITY,
                    severity=DiagnosticSeverity.ERROR,
                    message="Prohibited system procedure xp_cmdshell detected in SQL script.",
                )
            )

        # 5. Anti-Pattern: Unconditional DELETE without WHERE
        unconditional_deletes = re.findall(r"\bDELETE\s+FROM\s+([\[\]\w\.]+)\s*;", clean_code, re.IGNORECASE)
        for tbl in unconditional_deletes:
            if "staging" not in tbl.lower() and "reject" not in tbl.lower():
                issues.append(
                    DiagnosticIssue(
                        issue_code="SQL_UNSAFE_DELETE",
                        issue_type=ValidationIssueType.SECURITY,
                        severity=DiagnosticSeverity.ERROR,
                        message=f"Dangerous unconditional DELETE detected on table '{tbl}' without WHERE clause.",
                        suggested_fix="Add WHERE clause or route to staging table.",
                    )
                )

        # 6. Invariant check: never_drop_rows
        never_drop = context.options.get("never_drop_rows", False)
        invariants_ok = True
        if never_drop:
            if "DELETE FROM [DedupCTE]" in clean_code and "Rejection_Log" not in clean_code:
                invariants_ok = False
                issues.append(
                    DiagnosticIssue(
                        issue_code="SQL_INVARIANT_VIOLATION",
                        issue_type=ValidationIssueType.GOVERNANCE,
                        severity=DiagnosticSeverity.ERROR,
                        message="Violated 'never_drop_rows' invariant: records deleted without quarantine logging.",
                    )
                )

        is_valid = len([i for i in issues if i.severity == DiagnosticSeverity.ERROR]) == 0

        result = ValidationResult(
            is_valid=is_valid,
            target_engine="sql",
            issues=issues,
            has_security_hazards=has_security,
            invariants_satisfied=invariants_ok,
            metadata={"total_issues": len(issues)},
        )

        EventBus.get_instance().emit(
            event_type=CompilerEventType.VALIDATION_PASSED if is_valid else CompilerEventType.VALIDATION_FAILED,
            session_id=context.session_id,
            payload={"is_valid": is_valid, "error_count": result.error_count},
            source_agent=self.__class__.__name__,
        )

        return result
