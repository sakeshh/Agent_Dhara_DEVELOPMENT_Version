from __future__ import annotations

import ast
from typing import List

from agent.compiler.base.validation import BaseValidationAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.validation import DiagnosticIssue, DiagnosticSeverity, ValidationIssueType, ValidationResult


class PythonValidator(BaseValidationAgent):
    """AST-based validator for generated Python / Pandas pipelines."""

    DANGEROUS_FUNCTIONS = {"eval", "exec", "__import__"}
    DANGEROUS_MODULES = {"subprocess", "shutil"}

    def validate(self, artifact: CodeArtifact, context: CompilerContext) -> ValidationResult:
        issues: List[DiagnosticIssue] = []
        has_security = False
        parsed_tree = None

        # 1. AST Syntax Check
        try:
            parsed_tree = ast.parse(artifact.content)
        except SyntaxError as e:
            issues.append(
                DiagnosticIssue(
                    issue_code="PY_SYNTAX_ERROR",
                    issue_type=ValidationIssueType.SYNTAX,
                    severity=DiagnosticSeverity.ERROR,
                    message=f"SyntaxError: {e.msg}",
                    line_number=e.lineno,
                    column_number=e.offset,
                    problematic_code=e.text,
                    suggested_fix="Check brackets, commas, or indentation near the error line.",
                )
            )

        # 2. Security & Anti-Pattern Check
        if parsed_tree is not None:
            for node in ast.walk(parsed_tree):
                # Call security checks
                if isinstance(node, ast.Call):
                    func_name = None
                    if isinstance(node.func, ast.Name):
                        func_name = node.func.id
                    elif isinstance(node.func, ast.Attribute):
                        func_name = node.func.attr
                    
                    if func_name in self.DANGEROUS_FUNCTIONS:
                        has_security = True
                        issues.append(
                            DiagnosticIssue(
                                issue_code="PY_SEC_HAZARD",
                                issue_type=ValidationIssueType.SECURITY,
                                severity=DiagnosticSeverity.ERROR,
                                message=f"Dangerous function execution detected: '{func_name}'",
                                line_number=getattr(node, "lineno", None),
                                suggested_fix=f"Remove call to '{func_name}'",
                            )
                        )

                # Import security checks
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name in self.DANGEROUS_MODULES:
                            has_security = True
                            issues.append(
                                DiagnosticIssue(
                                    issue_code="PY_UNSAFE_IMPORT",
                                    issue_type=ValidationIssueType.SECURITY,
                                    severity=DiagnosticSeverity.ERROR,
                                    message=f"Unsafe module import: '{alias.name}'",
                                    line_number=getattr(node, "lineno", None),
                                )
                            )

            # Check required imports
            imported_names = set()
            for node in ast.walk(parsed_tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imported_names.add(alias.asname or alias.name)
                elif isinstance(node, ast.ImportFrom):
                    for alias in node.names:
                        imported_names.add(alias.asname or alias.name)

            if "pd" not in imported_names and "pandas" in artifact.content:
                issues.append(
                    DiagnosticIssue(
                        issue_code="PY_MISSING_PANDAS_IMPORT",
                        issue_type=ValidationIssueType.IMPORT,
                        severity=DiagnosticSeverity.ERROR,
                        message="Missing required import: 'import pandas as pd'",
                        suggested_fix="Add 'import pandas as pd' to the top of the file.",
                    )
                )

        # 3. Invariants Check: never_drop_rows
        never_drop = context.options.get("never_drop_rows", False)
        invariants_ok = True
        if never_drop and parsed_tree is not None:
            for node in ast.walk(parsed_tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    if node.func.attr == "dropna":
                        invariants_ok = False
                        issues.append(
                            DiagnosticIssue(
                                issue_code="PY_INVARIANT_VIOLATION",
                                issue_type=ValidationIssueType.GOVERNANCE,
                                severity=DiagnosticSeverity.ERROR,
                                message="Violated 'never_drop_rows' invariant: unconditional dropna() found.",
                                line_number=getattr(node, "lineno", None),
                                suggested_fix="Route dropped rows to quarantine DataFrame instead.",
                            )
                        )

        is_valid = len([i for i in issues if i.severity == DiagnosticSeverity.ERROR]) == 0

        result = ValidationResult(
            is_valid=is_valid,
            target_engine="python",
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
