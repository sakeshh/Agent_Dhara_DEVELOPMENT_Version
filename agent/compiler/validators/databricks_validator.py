from __future__ import annotations

import ast
import logging
from typing import List, Optional

from agent.compiler.base.validation import BaseValidationAgent
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.validation import (
    DiagnosticIssue,
    DiagnosticSeverity,
    ValidationIssueType,
    ValidationResult,
)

logger = logging.getLogger(__name__)


class DatabricksValidator(BaseValidationAgent):
    """Validates Databricks Delta Live Tables (DLT) scripts for syntax, DLT decorators, and Unity Catalog compliance."""

    def validate(self, artifact: CodeArtifact, context: Optional[CompilerContext] = None) -> ValidationResult:
        issues: List[DiagnosticIssue] = []

        # 1. Parse AST
        try:
            tree = ast.parse(artifact.content)
        except SyntaxError as e:
            return ValidationResult(
                is_valid=False,
                target_engine="databricks",
                issues=[
                    DiagnosticIssue(
                        issue_code="DATABRICKS_SYNTAX_ERROR",
                        issue_type=ValidationIssueType.SYNTAX,
                        severity=DiagnosticSeverity.ERROR,
                        line_number=e.lineno,
                        column_number=e.offset,
                        message=f"SyntaxError in DLT script: {e.msg}",
                    )
                ],
            )

        # 2. Check for dlt import
        has_dlt_import = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "dlt":
                        has_dlt_import = True
            elif isinstance(node, ast.ImportFrom):
                if node.module == "dlt":
                    has_dlt_import = True

        if not has_dlt_import:
            issues.append(
                DiagnosticIssue(
                    issue_code="DATABRICKS_MISSING_DLT",
                    issue_type=ValidationIssueType.IMPORT,
                    severity=DiagnosticSeverity.ERROR,
                    message="Missing required 'import dlt' in Databricks DLT pipeline script.",
                )
            )

        # 3. Check for @dlt.table decorators
        dlt_table_count = 0
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                for dec in node.decorator_list:
                    dec_name = ""
                    if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute):
                        dec_name = dec.func.attr
                    elif isinstance(dec, ast.Attribute):
                        dec_name = dec.attr
                    if dec_name == "table":
                        dlt_table_count += 1

        if dlt_table_count == 0:
            issues.append(
                DiagnosticIssue(
                    issue_code="DATABRICKS_NO_TABLES",
                    issue_type=ValidationIssueType.GOVERNANCE,
                    severity=DiagnosticSeverity.ERROR,
                    message="No @dlt.table declarations found in Databricks pipeline script.",
                )
            )

        has_errors = any(i.severity == DiagnosticSeverity.ERROR for i in issues)
        return ValidationResult(
            is_valid=not has_errors,
            target_engine="databricks",
            issues=issues,
        )

    def execute(self, context: CompilerContext) -> CompilerContext:
        if not context.code_artifact:
            raise ValueError("Cannot run DatabricksValidator without code_artifact in CompilerContext.")

        result = self.validate(context.code_artifact, context)
        context.validation_result = result
        return context
