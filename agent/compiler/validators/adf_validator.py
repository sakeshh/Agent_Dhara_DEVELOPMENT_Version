from __future__ import annotations

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


class ADFValidator(BaseValidationAgent):
    """Validates Azure Data Factory (ADF) pipeline JSON schema, activity DAG dependencies, and linked resources."""

    def validate(self, artifact: CodeArtifact, context: Optional[CompilerContext] = None) -> ValidationResult:
        issues: List[DiagnosticIssue] = []

        # 1. JSON parse
        try:
            bundle = json.loads(artifact.content)
        except json.JSONDecodeError as e:
            return ValidationResult(
                is_valid=False,
                target_engine="adf",
                issues=[
                    DiagnosticIssue(
                        issue_code="ADF_JSON_ERROR",
                        issue_type=ValidationIssueType.SYNTAX,
                        severity=DiagnosticSeverity.ERROR,
                        message=f"Invalid ADF JSON structure: {str(e)}",
                    )
                ],
            )

        # 2. Check Pipeline root
        pipeline = bundle.get("pipeline")
        if not pipeline or "name" not in pipeline or "properties" not in pipeline:
            issues.append(
                DiagnosticIssue(
                    issue_code="ADF_ROOT_ERROR",
                    issue_type=ValidationIssueType.SYNTAX,
                    severity=DiagnosticSeverity.ERROR,
                    message="ADF artifact missing required 'pipeline' or 'pipeline.properties' key.",
                )
            )
            return ValidationResult(
                is_valid=False,
                target_engine="adf",
                issues=issues,
            )

        activities = pipeline.get("properties", {}).get("activities", [])
        if not activities:
            issues.append(
                DiagnosticIssue(
                    issue_code="ADF_NO_ACTIVITIES",
                    issue_type=ValidationIssueType.GOVERNANCE,
                    severity=DiagnosticSeverity.ERROR,
                    message="ADF Pipeline contains no activities.",
                )
            )

        # Verify activity names and dependencies
        activity_names = {act.get("name") for act in activities if act.get("name")}
        for act in activities:
            if not act.get("name"):
                issues.append(
                    DiagnosticIssue(
                        issue_code="ADF_ACTIVITY_NO_NAME",
                        issue_type=ValidationIssueType.SYNTAX,
                        severity=DiagnosticSeverity.ERROR,
                        message="ADF activity missing 'name'.",
                    )
                )
            if not act.get("type"):
                issues.append(
                    DiagnosticIssue(
                        issue_code="ADF_ACTIVITY_NO_TYPE",
                        issue_type=ValidationIssueType.SYNTAX,
                        severity=DiagnosticSeverity.ERROR,
                        message=f"ADF activity '{act.get('name')}' missing 'type'.",
                    )
                )

            # Check dependencies exist
            for dep in act.get("dependsOn", []):
                dep_act = dep.get("activity")
                if dep_act and dep_act not in activity_names:
                    issues.append(
                        DiagnosticIssue(
                            issue_code="ADF_MISSING_DEPENDENCY",
                            issue_type=ValidationIssueType.REFERENCE,
                            severity=DiagnosticSeverity.ERROR,
                            message=f"ADF activity '{act.get('name')}' depends on non-existent activity '{dep_act}'.",
                        )
                    )

        # Linked services verification
        if not bundle.get("linked_services"):
            issues.append(
                DiagnosticIssue(
                    issue_code="ADF_NO_LINKED_SERVICES",
                    issue_type=ValidationIssueType.GOVERNANCE,
                    severity=DiagnosticSeverity.WARNING,
                    message="No linked services configured in ADF deployment package.",
                )
            )

        has_errors = any(i.severity == DiagnosticSeverity.ERROR for i in issues)
        return ValidationResult(
            is_valid=not has_errors,
            target_engine="adf",
            issues=issues,
        )

    def execute(self, context: CompilerContext) -> CompilerContext:
        if not context.code_artifact:
            raise ValueError("Cannot run ADFValidator without code_artifact in CompilerContext.")

        result = self.validate(context.code_artifact, context)
        context.validation_result = result
        return context
