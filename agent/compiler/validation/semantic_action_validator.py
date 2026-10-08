from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Set

from agent.compiler.models.actions import StructuredAction
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType, LogicalStep
from agent.compiler.models.validation import (
    DiagnosticIssue,
    DiagnosticSeverity,
    ValidationIssueType,
)
from agent.semantic_context import ColumnRole, EntityDefinition, SemanticCleaningPlan

logger = logging.getLogger("agent.compiler.validation.semantic")


class SemanticActionValidator:
    """
    Deterministic, zero-LLM semantic validator.
    Enforces the ColumnRole × ActionType matrix and business-key deduplication rules.
    Runs prior to LogicalPlanner and Manifest Freeze.
    """

    ID_REGEX = re.compile(r"(^|_)(id|key|uuid|guid|pk|fk|code|ref)($|_)", re.I)
    METRIC_REGEX = re.compile(r"(salary|wage|amount|price|cost|fee|limit|balance|revenue|tax|total|count|qty|quantity|rate)", re.I)
    DATE_REGEX = re.compile(r"(date|time|timestamp|dob|created|updated|birth|expiry|due)", re.I)
    STATUS_REGEX = re.compile(r"(status|state|category|type|flag|phase|tier|level)", re.I)
    EMAIL_REGEX = re.compile(r"(email|e_mail|mail)", re.I)
    PHONE_REGEX = re.compile(r"(phone|mobile|cell|fax|tel)", re.I)

    # Core Role x Action Forbidden Matrix
    FORBIDDEN_ACTIONS_BY_ROLE: Dict[ColumnRole, Set[ActionType]] = {
        ColumnRole.IDENTIFIER: {
            ActionType.PARSE_DATES,
            ActionType.RANGE_CLIP,
            ActionType.CAP_OUTLIERS,
            ActionType.COERCE_NUMERIC,
        },
        ColumnRole.PRIMARY_KEY: {
            ActionType.PARSE_DATES,
            ActionType.RANGE_CLIP,
            ActionType.CAP_OUTLIERS,
            ActionType.ZERO_TO_NULL,
            ActionType.FILL_NULLS,
            ActionType.COERCE_NUMERIC,
        },
        ColumnRole.FOREIGN_KEY: {
            ActionType.PARSE_DATES,
            ActionType.RANGE_CLIP,
            ActionType.CAP_OUTLIERS,
            ActionType.COERCE_NUMERIC,
        },
        ColumnRole.BUSINESS_KEY: {
            ActionType.PARSE_DATES,
            ActionType.RANGE_CLIP,
            ActionType.CAP_OUTLIERS,
            ActionType.ZERO_TO_NULL,
            ActionType.COERCE_NUMERIC,
        },
        ColumnRole.METRIC: {
            ActionType.LOWERCASE,
            ActionType.UPPERCASE,
            ActionType.TRIM,
            ActionType.PARSE_DATES,
            ActionType.SANITIZE_EMAIL,
            ActionType.NORMALIZE_PHONE,
            ActionType.MASK_PHONE,
        },
        ColumnRole.DATE: {
            ActionType.SANITIZE_EMAIL,
            ActionType.NORMALIZE_PHONE,
            ActionType.MASK_PHONE,
            ActionType.LOWERCASE,
            ActionType.UPPERCASE,
            ActionType.COERCE_NUMERIC,
        },
        ColumnRole.TIMESTAMP: {
            ActionType.SANITIZE_EMAIL,
            ActionType.NORMALIZE_PHONE,
            ActionType.MASK_PHONE,
            ActionType.LOWERCASE,
            ActionType.UPPERCASE,
            ActionType.COERCE_NUMERIC,
        },
        ColumnRole.CATEGORICAL: {
            ActionType.PARSE_DATES,
            ActionType.RANGE_CLIP,
            ActionType.CAP_OUTLIERS,
            ActionType.COERCE_NUMERIC,
        },
        ColumnRole.FLAG: {
            ActionType.PARSE_DATES,
            ActionType.RANGE_CLIP,
            ActionType.CAP_OUTLIERS,
            ActionType.COERCE_NUMERIC,
        },
        ColumnRole.EMAIL: {
            ActionType.RANGE_CLIP,
            ActionType.CAP_OUTLIERS,
            ActionType.COERCE_NUMERIC,
            ActionType.PARSE_DATES,
            ActionType.STANDARDIZE_BOOLEAN,
        },
        ColumnRole.PHONE: {
            ActionType.RANGE_CLIP,
            ActionType.CAP_OUTLIERS,
            ActionType.COERCE_NUMERIC,
            ActionType.PARSE_DATES,
            ActionType.STANDARDIZE_BOOLEAN,
        },
    }

    # Custom explanatory messages for deterministic diagnostics
    EXPLANATION_TEMPLATES: Dict[tuple[ColumnRole, ActionType], str] = {
        (ColumnRole.IDENTIFIER, ActionType.PARSE_DATES): "Identifiers/IDs cannot be parsed as dates.",
        (ColumnRole.PRIMARY_KEY, ActionType.PARSE_DATES): "Primary keys cannot be parsed as dates.",
        (ColumnRole.FOREIGN_KEY, ActionType.PARSE_DATES): "Foreign keys cannot be parsed as dates.",
        (ColumnRole.METRIC, ActionType.LOWERCASE): "Numeric metrics/currency amounts cannot have LOWERCASE applied.",
        (ColumnRole.METRIC, ActionType.UPPERCASE): "Numeric metrics/currency amounts cannot have UPPERCASE applied.",
        (ColumnRole.METRIC, ActionType.TRIM): (
            "Numeric metrics cannot have standalone string TRIM applied. "
            "Trimming is only permitted internally during string-to-numeric coercion."
        ),
        (ColumnRole.METRIC, ActionType.PARSE_DATES): "Numeric metrics/currency amounts cannot be parsed as dates.",
        (ColumnRole.CATEGORICAL, ActionType.PARSE_DATES): "Categorical/status columns cannot be parsed as dates.",
        (ColumnRole.CATEGORICAL, ActionType.RANGE_CLIP): "Categorical/status columns cannot be range-clipped.",
        (ColumnRole.FLAG, ActionType.PARSE_DATES): "Boolean flag columns cannot be parsed as dates.",
        (ColumnRole.PRIMARY_KEY, ActionType.ZERO_TO_NULL): "Primary keys cannot be converted to NULL.",
    }

    @classmethod
    def infer_role_heuristic(cls, col_name: str, dtype: Optional[str] = None) -> ColumnRole:
        """Fallback deterministic role inference based on naming patterns and physical dtype."""
        c_low = col_name.strip().lower()
        d_low = (dtype or "").lower()

        if cls.ID_REGEX.search(c_low):
            return ColumnRole.PRIMARY_KEY if c_low in ("id", "pk") else ColumnRole.IDENTIFIER
        if cls.METRIC_REGEX.search(c_low) or any(t in d_low for t in ("decimal", "numeric", "float", "double", "money")):
            return ColumnRole.METRIC
        if cls.DATE_REGEX.search(c_low) or any(t in d_low for t in ("date", "time")):
            return ColumnRole.DATE
        if cls.EMAIL_REGEX.search(c_low):
            return ColumnRole.EMAIL
        if cls.PHONE_REGEX.search(c_low):
            return ColumnRole.PHONE
        if cls.STATUS_REGEX.search(c_low):
            return ColumnRole.CATEGORICAL
        if any(t in d_low for t in ("bool", "bit")):
            return ColumnRole.FLAG

        return ColumnRole.FREE_TEXT

    @classmethod
    def get_column_role(
        cls,
        dataset_name: str,
        column_name: str,
        semantic_plan: Optional[SemanticCleaningPlan] = None,
        context: Optional[CompilerContext] = None,
    ) -> ColumnRole:
        """Resolves the ColumnRole from the SemanticCleaningPlan, dataset profile, or deterministic heuristics."""
        if semantic_plan and hasattr(semantic_plan, "entities"):
            contracts = semantic_plan.get_cleaning_contracts(dataset_name)
            if column_name in contracts:
                return contracts[column_name].role

            # Check if column matches canonical_key
            dedup_key = semantic_plan.get_dedup_key(dataset_name)
            if dedup_key and dedup_key.lower() == column_name.lower():
                return ColumnRole.PRIMARY_KEY

        # Fallback to dataset profile
        dtype = None
        if context and context.dataset_profiles:
            prof = context.dataset_profiles.get(dataset_name)
            if prof:
                col_p = prof.get_column(column_name)
                if col_p:
                    dtype = col_p.dtype
                    # If column profile already has semantic hints
                    if hasattr(col_p, "semantic_type") and col_p.semantic_type:
                        st = col_p.semantic_type.lower()
                        if st in ("id", "identifier", "uuid", "key"):
                            return ColumnRole.IDENTIFIER
                        if st in ("metric", "currency", "amount", "numeric"):
                            return ColumnRole.METRIC
                        if st in ("date", "datetime"):
                            return ColumnRole.DATE
                        if st in ("categorical", "status"):
                            return ColumnRole.CATEGORICAL
                        if st in ("email",):
                            return ColumnRole.EMAIL
                        if st in ("phone",):
                            return ColumnRole.PHONE

        return cls.infer_role_heuristic(column_name, dtype)

    @classmethod
    def validate_action(
        cls,
        action: StructuredAction,
        semantic_plan: Optional[SemanticCleaningPlan] = None,
        context: Optional[CompilerContext] = None,
    ) -> List[DiagnosticIssue]:
        """Validates a single StructuredAction against semantic constraints."""
        issues: List[DiagnosticIssue] = []
        dataset = action.dataset_name
        act_type = action.action_type
        target_cols = [c for c in action.target_columns if str(c).strip().strip("[]").lower() not in ("row-level", "table-level", "dataset-level", "*", "")]

        # 1. Deduplication Admission Control
        if act_type == ActionType.DEDUPLICATE:
            dedup_issues = cls.validate_deduplication(action, semantic_plan, context)
            issues.extend(dedup_issues)
            return issues

        # 2. Field-level Role x Action Compatibility
        for col in target_cols:
            role = cls.get_column_role(dataset, col, semantic_plan, context)

            # Check IDENTIFIER Casing Policy
            if role in (ColumnRole.IDENTIFIER, ColumnRole.PRIMARY_KEY, ColumnRole.FOREIGN_KEY, ColumnRole.BUSINESS_KEY):
                if act_type in (ActionType.LOWERCASE, ActionType.UPPERCASE):
                    allow_id_casing = False
                    if context and context.options.get("identifier_normalization_policy"):
                        allow_id_casing = True
                    if action.parameters.get("normalize_casing") is True:
                        allow_id_casing = True

                    if not allow_id_casing:
                        issues.append(
                            DiagnosticIssue(
                                issue_code="SEMANTIC_ROLE_VIOLATION",
                                issue_type=ValidationIssueType.SCHEMA_CONTRACT,
                                severity=DiagnosticSeverity.BLOCKING,
                                message=(
                                    f"Cannot apply '{act_type.value}' to identifier column '{col}' ({role.value}). "
                                    f"Identifier casing normalization is denied by default unless 'identifier_normalization_policy' is enabled."
                                ),
                                suggested_fix="Enable identifier_normalization_policy or remove casing action on identifier.",
                            )
                        )
                        continue

            # Check general forbidden matrix
            forbidden_set = cls.FORBIDDEN_ACTIONS_BY_ROLE.get(role, set())
            if act_type in forbidden_set:
                reason = cls.EXPLANATION_TEMPLATES.get(
                    (role, act_type),
                    f"Action '{act_type.value}' is semantically incompatible with column '{col}' having role '{role.value}'."
                )
                issues.append(
                    DiagnosticIssue(
                        issue_code="SEMANTIC_ROLE_VIOLATION",
                        issue_type=ValidationIssueType.SCHEMA_CONTRACT,
                        severity=DiagnosticSeverity.BLOCKING,
                        message=reason,
                        suggested_fix=f"Remove or replace '{act_type.value}' with an operator compatible with {role.value}.",
                    )
                )

        return issues

    @classmethod
    def validate_deduplication(
        cls,
        action: StructuredAction,
        semantic_plan: Optional[SemanticCleaningPlan] = None,
        context: Optional[CompilerContext] = None,
    ) -> List[DiagnosticIssue]:
        """
        Deduplication Admission Control:
        1. Forbids low-cardinality/categorical columns from acting as sole deduplication key.
        2. Blocks deduplication if no primary key, canonical key, or verified business key is supplied.
        3. Enforces deterministic survivor ordering.
        """
        issues: List[DiagnosticIssue] = []
        dataset = action.dataset_name
        target_cols = [c for c in action.target_columns if str(c).strip().strip("[]").lower() not in ("row-level", "table-level", "dataset-level", "*", "")]

        # Determine approved business key from semantic plan
        canonical_key = None
        if semantic_plan:
            canonical_key = semantic_plan.get_dedup_key(dataset)

        # Check: Categorical / Status columns cannot be the sole deduplication key
        if target_cols:
            all_categorical = True
            for col in target_cols:
                role = cls.get_column_role(dataset, col, semantic_plan, context)
                if role not in (ColumnRole.CATEGORICAL, ColumnRole.FLAG):
                    all_categorical = False
                    break

            if all_categorical:
                issues.append(
                    DiagnosticIssue(
                        issue_code="DEDUP_INVALID_CATEGORICAL_KEY",
                        issue_type=ValidationIssueType.SCHEMA_CONTRACT,
                        severity=DiagnosticSeverity.BLOCKING,
                        message=(
                            f"Deduplication partitioned solely by categorical/status columns {target_cols} is prohibited. "
                            f"This would delete valid business records that share the same status."
                        ),
                        suggested_fix="Deduplication requires a verified unique primary key or composite business key.",
                    )
                )
                return issues

        # Check: If no target columns and no canonical key -> Unsafe dataset-wide deduplication
        if not target_cols and not canonical_key:
            issues.append(
                DiagnosticIssue(
                    issue_code="DEDUP_MISSING_BUSINESS_KEY",
                    issue_type=ValidationIssueType.SCHEMA_CONTRACT,
                    severity=DiagnosticSeverity.BLOCKING,
                    message=(
                        f"Dataset '{dataset}' has no approved canonical key or target key columns specified for deduplication. "
                        f"Automated deduplication cannot proceed safely."
                    ),
                    suggested_fix="Route to MANUAL_REVIEW_REQUIRED for data steward approval of business key.",
                )
            )
            return issues

        # Check survivor ordering
        survivor_order = action.parameters.get("survivor_order") or action.parameters.get("order_by_clause")
        if survivor_order and "select 1" in str(survivor_order).lower():
            issues.append(
                DiagnosticIssue(
                    issue_code="DEDUP_NON_DETERMINISTIC_SURVIVOR",
                    issue_type=ValidationIssueType.SYNTAX,
                    severity=DiagnosticSeverity.BLOCKING,
                    message="Deduplication with non-deterministic 'ORDER BY (SELECT 1)' is strictly prohibited.",
                    suggested_fix="Provide an explicit survivor tie-breaker (e.g. '[UpdatedAt] DESC', '[_DharaStagingRowId] ASC').",
                )
            )

        return issues

    @classmethod
    def validate_plan(
        cls,
        steps: List[LogicalStep],
        semantic_plan: Optional[SemanticCleaningPlan] = None,
        context: Optional[CompilerContext] = None,
    ) -> List[DiagnosticIssue]:
        """
        Cross-step Plan-Level Semantic Audit:
        Checks for lifecycle contradictions across steps for the same dataset and column.
        """
        issues: List[DiagnosticIssue] = []
        dataset_column_actions: Dict[tuple[str, str], List[ActionType]] = {}

        for step in steps:
            for col in step.target_columns:
                key = (step.dataset_name, col)
                dataset_column_actions.setdefault(key, []).append(step.action)

        for (ds, col), acts in dataset_column_actions.items():
            role = cls.get_column_role(ds, col, semantic_plan, context)

            # Check 1: COERCE_NUMERIC followed later by PARSE_DATES
            if ActionType.COERCE_NUMERIC in acts and ActionType.PARSE_DATES in acts:
                coerce_idx = acts.index(ActionType.COERCE_NUMERIC)
                date_idx = acts.index(ActionType.PARSE_DATES)
                if date_idx > coerce_idx:
                    issues.append(
                        DiagnosticIssue(
                            issue_code="PLAN_CONTRADICTORY_STEPS",
                            issue_type=ValidationIssueType.LOGICAL,
                            severity=DiagnosticSeverity.BLOCKING,
                            message=f"Contradictory plan: Column '{col}' in '{ds}' is coerced to numeric and then parsed as a date.",
                            suggested_fix="Remove conflicting PARSE_DATES action after COERCE_NUMERIC.",
                        )
                    )

            # Check 2: LOWERCASE after numeric coercion or on metric
            if role == ColumnRole.METRIC and ActionType.LOWERCASE in acts:
                issues.append(
                    DiagnosticIssue(
                        issue_code="PLAN_INVALID_METRIC_TRANSFORMATION",
                        issue_type=ValidationIssueType.LOGICAL,
                        severity=DiagnosticSeverity.BLOCKING,
                        message=f"Plan attempts string LOWERCASE on metric column '{col}' in '{ds}'.",
                        suggested_fix="Remove LOWERCASE step on numeric metric.",
                    )
                )

        return issues

    @classmethod
    def validate_context(cls, context: CompilerContext) -> tuple[bool, List[DiagnosticIssue]]:
        """
        Primary entry point called by CompilerPipeline admission gate before planning.
        Validates all structured actions in the context.
        """
        all_issues: List[DiagnosticIssue] = []
        semantic_plan = context.semantic_plan

        # Validate all structured actions
        for act in context.structured_actions:
            act_obj = act
            if isinstance(act, dict):
                act_obj = StructuredAction.model_validate(act)
            elif not isinstance(act, StructuredAction):
                continue

            act_issues = cls.validate_action(act_obj, semantic_plan, context)
            all_issues.extend(act_issues)

        # If logical plan already constructed, also validate plan-level
        if context.logical_plan and context.logical_plan.steps:
            plan_issues = cls.validate_plan(context.logical_plan.steps, semantic_plan, context)
            all_issues.extend(plan_issues)

        is_valid = not any(
            i.severity in (DiagnosticSeverity.ERROR, DiagnosticSeverity.BLOCKING)
            for i in all_issues
        )
        return is_valid, all_issues
