from __future__ import annotations

import ast
import json
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


class FabricValidator(BaseValidationAgent):
    """Validates Microsoft Fabric Lakehouse Notebooks (.ipynb) structure, cell syntax, and Delta write policies."""

    def validate(self, artifact: CodeArtifact, context: Optional[CompilerContext] = None) -> ValidationResult:
        issues: List[DiagnosticIssue] = []

        # 1. JSON parse notebook
        try:
            notebook = json.loads(artifact.content)
        except json.JSONDecodeError as e:
            return ValidationResult(
                is_valid=False,
                target_engine="fabric",
                issues=[
                    DiagnosticIssue(
                        issue_code="FABRIC_JSON_ERROR",
                        issue_type=ValidationIssueType.SYNTAX,
                        severity=DiagnosticSeverity.ERROR,
                        message=f"Invalid notebook JSON format: {str(e)}",
                    )
                ],
            )

        # 2. Check notebook schema
        if "nbformat" not in notebook or "cells" not in notebook:
            issues.append(
                DiagnosticIssue(
                    issue_code="FABRIC_SCHEMA_ERROR",
                    issue_type=ValidationIssueType.SYNTAX,
                    severity=DiagnosticSeverity.ERROR,
                    message="Missing required notebook fields 'nbformat' or 'cells'",
                )
            )

        cells = notebook.get("cells", [])
        has_delta_write = False
        has_spark_import = False

        for idx, cell in enumerate(cells):
            cell_type = cell.get("cell_type")
            source = "".join(cell.get("source", []))

            if cell_type == "code":
                # Validate Python syntax in each code cell
                try:
                    ast.parse(source)
                except SyntaxError as syn_err:
                    issues.append(
                        DiagnosticIssue(
                            issue_code="FABRIC_SYNTAX_ERROR",
                            issue_type=ValidationIssueType.SYNTAX,
                            severity=DiagnosticSeverity.ERROR,
                            line_number=syn_err.lineno,
                            message=f"SyntaxError in cell {idx}: {syn_err.msg}",
                        )
                    )

                if "pyspark" in source or "spark" in source:
                    has_spark_import = True
                if "saveAsTable" in source or "format(\"delta\")" in source:
                    has_delta_write = True

        if not has_delta_write:
            issues.append(
                DiagnosticIssue(
                    issue_code="FABRIC_MISSING_DELTA",
                    issue_type=ValidationIssueType.GOVERNANCE,
                    severity=DiagnosticSeverity.WARNING,
                    message="Notebook does not explicitly write to a Delta Lake table.",
                )
            )

        has_errors = any(i.severity == DiagnosticSeverity.ERROR for i in issues)
        return ValidationResult(
            is_valid=not has_errors,
            target_engine="fabric",
            issues=issues,
        )

    def execute(self, context: CompilerContext) -> CompilerContext:
        if not context.code_artifact:
            raise ValueError("Cannot run FabricValidator without code_artifact in CompilerContext.")

        result = self.validate(context.code_artifact, context)
        context.validation_result = result
        return context
