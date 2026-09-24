from __future__ import annotations

import ast
from typing import List

from agent.compiler.base.validation import BaseValidationAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.validation import DiagnosticIssue, DiagnosticSeverity, ValidationIssueType, ValidationResult


class SparkValidator(BaseValidationAgent):
    """AST-based validator and linter for generated PySpark / Fabric pipelines."""

    RISKY_SPARK_METHODS = {"collect": "collect() triggers full dataset download to driver memory", "toPandas": "toPandas() bypasses distributed execution"}

    def validate(self, artifact: CodeArtifact, context: CompilerContext) -> ValidationResult:
        issues: List[DiagnosticIssue] = []
        has_perf = False
        parsed_tree = None

        # 1. AST Syntax Check
        try:
            parsed_tree = ast.parse(artifact.content)
        except SyntaxError as e:
            issues.append(
                DiagnosticIssue(
                    issue_code="SPARK_SYNTAX_ERROR",
                    issue_type=ValidationIssueType.SYNTAX,
                    severity=DiagnosticSeverity.ERROR,
                    message=f"SyntaxError in PySpark script: {e.msg}",
                    line_number=e.lineno,
                    column_number=e.offset,
                    suggested_fix="Check brackets or syntax near error line.",
                )
            )

        # 2. Performance Anti-Pattern checks
        if parsed_tree is not None:
            for node in ast.walk(parsed_tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    method_name = node.func.attr
                    if method_name in self.RISKY_SPARK_METHODS:
                        has_perf = True
                        issues.append(
                            DiagnosticIssue(
                                issue_code="SPARK_PERF_HAZARD",
                                issue_type=ValidationIssueType.PERFORMANCE,
                                severity=DiagnosticSeverity.WARNING,
                                message=f"Performance warning: {self.RISKY_SPARK_METHODS[method_name]}",
                                line_number=getattr(node, "lineno", None),
                                suggested_fix=f"Avoid calling {method_name}() in production distributed ETL pipelines.",
                            )
                        )

        # 3. Invariants Check
        never_drop = context.options.get("never_drop_rows", False)
        invariants_ok = True
        if never_drop and parsed_tree is not None:
            for node in ast.walk(parsed_tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    if node.func.attr == "dropna":
                        invariants_ok = False
                        issues.append(
                            DiagnosticIssue(
                                issue_code="SPARK_INVARIANT_VIOLATION",
                                issue_type=ValidationIssueType.GOVERNANCE,
                                severity=DiagnosticSeverity.ERROR,
                                message="Violated 'never_drop_rows' invariant: unconditional dropna() found.",
                                line_number=getattr(node, "lineno", None),
                            )
                        )

        is_valid = len([i for i in issues if i.severity == DiagnosticSeverity.ERROR]) == 0

        result = ValidationResult(
            is_valid=is_valid,
            target_engine="pyspark",
            issues=issues,
            has_performance_anti_patterns=has_perf,
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
