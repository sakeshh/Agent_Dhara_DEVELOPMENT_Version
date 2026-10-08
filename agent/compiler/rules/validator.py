from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.compiler.rules.normalized_rules import (
    ColumnBusinessRule,
    ConstraintType,
    NormalizedBusinessRules,
    ViolationDisposition,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RuleValidationIssue:
    """Diagnostic issue produced by BusinessRuleValidator."""
    rule_id: str
    code: str
    severity: str  # "BLOCKING" | "WARNING"
    dataset: str
    column: Optional[str] = None
    message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "code": self.code,
            "severity": self.severity,
            "dataset": self.dataset,
            "column": self.column,
            "message": self.message,
        }


@dataclass
class BusinessRuleValidationResult:
    """Aggregated validation output for business rules."""
    is_valid: bool
    issues: List[RuleValidationIssue] = field(default_factory=list)

    @property
    def blocking_issues(self) -> List[RuleValidationIssue]:
        return [i for i in self.issues if i.severity == "BLOCKING"]

    @property
    def error_messages(self) -> List[str]:
        return [i.message for i in self.blocking_issues]


class BusinessRuleValidator:
    """
    Validates business rules for schema consistency, internal consistency, policy conflicts,
    and remediation contradictions before action compilation.
    Enforces fail-closed safety semantics.
    """

    @classmethod
    def validate(
        cls,
        rules: NormalizedBusinessRules,
        dataset_profiles: Optional[Dict[str, Any]] = None,
        semantic_plan: Optional[Any] = None,
    ) -> BusinessRuleValidationResult:
        issues: List[RuleValidationIssue] = []

        # 1. Policy Conflicts: never_drop_rows vs drop/reject rules
        cls._validate_policy_conflicts(rules, issues)

        # 2. Schema Conflicts: required_columns missing from dataset profile
        cls._validate_schema_conflicts(rules, dataset_profiles, issues)

        # 3. Range Sanity: minimum > maximum
        cls._validate_range_sanity(rules, issues)

        # 4. Null / Default Value Conflicts
        cls._validate_null_default_conflicts(rules, issues)

        # 5. Allowed-Values / Value-Map Conflicts
        cls._validate_allowed_values_conflicts(rules, issues)

        # 6. Conflicting Remediations for same column
        cls._validate_conflicting_remediations(rules, issues)

        # 7. Semantic Role Conflicts
        cls._validate_semantic_conflicts(rules, semantic_plan, issues)

        has_blocking = any(i.severity == "BLOCKING" for i in issues)
        return BusinessRuleValidationResult(
            is_valid=not has_blocking,
            issues=issues,
        )

    @classmethod
    def _validate_policy_conflicts(
        cls,
        rules: NormalizedBusinessRules,
        issues: List[RuleValidationIssue],
    ) -> None:
        """
        Detects conflicts between global never_drop_rows and dropping policies.
        E.g. never_drop_rows = true + drop_invalid_rows = true -> BUSINESS_RULE_CONFLICT
        """
        never_drop = rules.never_drop_rows
        # Check policy rules
        for p in rules.policy_rules:
            if not p.enabled:
                continue
            if p.name == "drop_invalid_rows" and bool(p.value) and never_drop:
                issues.append(
                    RuleValidationIssue(
                        rule_id=p.rule_id,
                        code="BUSINESS_RULE_CONFLICT",
                        severity="BLOCKING",
                        dataset=p.dataset or "global",
                        message="Policy conflict: 'never_drop_rows=True' conflicts with 'drop_invalid_rows=True'.",
                    )
                )

    @classmethod
    def _validate_schema_conflicts(
        cls,
        rules: NormalizedBusinessRules,
        dataset_profiles: Optional[Dict[str, Any]],
        issues: List[RuleValidationIssue],
    ) -> None:
        """
        Validates that required_columns actually exist in dataset profiles.
        If a required source column is absent: REQUIRED_COLUMN_MISSING -> compilation blocked.
        """
        if not dataset_profiles:
            return

        # Build column lookup by dataset
        ds_cols: Dict[str, Set[str]] = {}
        for ds_name, prof in dataset_profiles.items():
            norm_name = ds_name.strip("[]").lower()
            if hasattr(prof, "columns"):
                ds_cols[norm_name] = {c.strip("[]").lower() for c in prof.columns.keys()}
            elif isinstance(prof, dict) and "columns" in prof:
                ds_cols[norm_name] = {c.strip("[]").lower() for c in prof["columns"].keys()}

        # Check required columns
        for col_name in rules.required_columns:
            col_lower = col_name.strip("[]").lower()
            # If rules specify dataset on column rules for this required col:
            matching_col_rules = [r for r in rules.column_rules if r.column.lower() == col_lower]
            target_ds = matching_col_rules[0].dataset if matching_col_rules else None

            if target_ds:
                norm_ds = target_ds.strip("[]").lower()
                if norm_ds in ds_cols and col_lower not in ds_cols[norm_ds]:
                    issues.append(
                        RuleValidationIssue(
                            rule_id=f"REQ-COL-{col_name.upper()}",
                            code="REQUIRED_COLUMN_MISSING",
                            severity="BLOCKING",
                            dataset=target_ds,
                            column=col_name,
                            message=f"Required column '{col_name}' does not exist in dataset '{target_ds}'.",
                        )
                    )
            else:
                # Check if it exists in any dataset
                all_known_cols = {c for cols in ds_cols.values() for c in cols}
                if all_known_cols and col_lower not in all_known_cols:
                    issues.append(
                        RuleValidationIssue(
                            rule_id=f"REQ-COL-{col_name.upper()}",
                            code="REQUIRED_COLUMN_MISSING",
                            severity="BLOCKING",
                            dataset="global",
                            column=col_name,
                            message=f"Required column '{col_name}' does not exist in any profiled dataset.",
                        )
                    )

    @classmethod
    def _validate_range_sanity(
        cls,
        rules: NormalizedBusinessRules,
        issues: List[RuleValidationIssue],
    ) -> None:
        """
        Range conflicts: minimum > maximum -> INVALID_RANGE_RULE (BLOCKING).
        """
        for r in rules.column_rules:
            if not r.enabled or r.constraint_type != ConstraintType.RANGE:
                continue
            min_v = r.parameters.get("minimum")
            max_v = r.parameters.get("maximum")
            if min_v is not None and max_v is not None:
                try:
                    if float(min_v) > float(max_v):
                        issues.append(
                            RuleValidationIssue(
                                rule_id=r.rule_id,
                                code="INVALID_RANGE_RULE",
                                severity="BLOCKING",
                                dataset=r.dataset,
                                column=r.column,
                                message=f"Range minimum ({min_v}) cannot exceed maximum ({max_v}) for column '{r.column}'.",
                            )
                        )
                except (ValueError, TypeError):
                    pass

    @classmethod
    def _validate_null_default_conflicts(
        cls,
        rules: NormalizedBusinessRules,
        issues: List[RuleValidationIssue],
    ) -> None:
        """
        Null/default conflicts:
        Salary > 0 + default Salary = 0 -> BUSINESS_RULE_CONFLICT
        """
        # Group rules by dataset + column
        for r in rules.column_rules:
            if not r.enabled:
                continue
            # If rule has a default fill value
            default_val = r.parameters.get("default_value")
            if default_val is not None:
                try:
                    def_num = float(default_val)
                    # Check against range constraints on same column
                    for other in rules.get_rules_for_column(r.dataset, r.column):
                        if other.constraint_type == ConstraintType.RANGE:
                            min_v = other.parameters.get("minimum")
                            if min_v is not None and def_num < float(min_v):
                                issues.append(
                                    RuleValidationIssue(
                                        rule_id=r.rule_id,
                                        code="BUSINESS_RULE_CONFLICT",
                                        severity="BLOCKING",
                                        dataset=r.dataset,
                                        column=r.column,
                                        message=f"Default fill value '{default_val}' violates minimum range threshold '{min_v}' for column '{r.column}'.",
                                    )
                                )
                except (ValueError, TypeError):
                    pass

    @classmethod
    def _validate_allowed_values_conflicts(
        cls,
        rules: NormalizedBusinessRules,
        issues: List[RuleValidationIssue],
    ) -> None:
        """
        Allowed-value conflicts:
        allowed values = ACTIVE, INACTIVE + map ACTIVE -> DELETED -> BUSINESS_RULE_CONFLICT
        """
        for r in rules.column_rules:
            if not r.enabled or r.constraint_type != ConstraintType.ALLOWED_VALUES:
                continue
            allowed_vals = {str(v).strip().upper() for v in r.parameters.get("allowed_values", [])}
            aliases = r.parameters.get("aliases", {})
            if allowed_vals and aliases:
                for src_val, tgt_val in aliases.items():
                    src_upper = str(src_val).strip().upper()
                    tgt_upper = str(tgt_val).strip().upper()
                    # 1. If mapping source was an allowed value, but target is NOT an allowed value
                    if src_upper in allowed_vals and tgt_upper not in allowed_vals:
                        issues.append(
                            RuleValidationIssue(
                                rule_id=r.rule_id,
                                code="BUSINESS_RULE_CONFLICT",
                                severity="BLOCKING",
                                dataset=r.dataset,
                                column=r.column,
                                message=f"Value mapping re-maps valid allowed value '{src_val}' to invalid value '{tgt_val}' for column '{r.column}'.",
                            )
                        )
                    # 2. If alias target itself is not in allowed values
                    elif tgt_upper not in allowed_vals:
                        issues.append(
                            RuleValidationIssue(
                                rule_id=r.rule_id,
                                code="BUSINESS_RULE_CONFLICT",
                                severity="BLOCKING",
                                dataset=r.dataset,
                                column=r.column,
                                message=f"Value mapping target '{tgt_val}' is not in allowed values list {list(allowed_vals)} for column '{r.column}'.",
                            )
                        )

    @classmethod
    def _validate_conflicting_remediations(
        cls,
        rules: NormalizedBusinessRules,
        issues: List[RuleValidationIssue],
    ) -> None:
        """
        Duplicate/conflicting remediation rules:
        Rule A: null Salary -> 0, Rule B: null Salary -> median -> CONFLICTING_REMEDIATIONS
        """
        remediation_map: Dict[Tuple[str, str, str], List[ColumnBusinessRule]] = {}
        for r in rules.column_rules:
            if not r.enabled or not r.explicit_remediation_action:
                continue
            key = (r.dataset.lower(), r.column.lower(), r.explicit_remediation_action.lower())
            remediation_map.setdefault(key, []).append(r)

        for (ds, col, act), r_list in remediation_map.items():
            if len(r_list) > 1:
                # Check if parameters conflict
                distinct_params = {str(sorted(r.parameters.items())) for r in r_list}
                if len(distinct_params) > 1:
                    issues.append(
                        RuleValidationIssue(
                            rule_id=r_list[0].rule_id,
                            code="CONFLICTING_REMEDIATIONS",
                            severity="BLOCKING",
                            dataset=r_list[0].dataset,
                            column=r_list[0].column,
                            message=f"Two enabled rules define conflicting remediation strategies for column '{r_list[0].column}'.",
                        )
                    )

    @classmethod
    def _validate_semantic_conflicts(
        cls,
        rules: NormalizedBusinessRules,
        semantic_plan: Optional[Any],
        issues: List[RuleValidationIssue],
    ) -> None:
        """
        Semantic conflicts:
        Prefix Salary with 'INR-' -> SEMANTIC_ROLE_VIOLATION
        """
        if not semantic_plan:
            return

        from agent.compiler.validation.semantic_action_validator import SemanticActionValidator

        for tr in rules.transformation_rules:
            if not tr.enabled:
                continue
            role = SemanticActionValidator.get_column_role(tr.dataset, tr.column, semantic_plan=semantic_plan)
            role_val = getattr(role, "value", str(role)).upper()

            # PREFIX_ADD on METRIC or IDENTIFIER
            if tr.action_type in ("prefix_add", "suffix_add"):
                if role_val in ("METRIC", "MEASURE", "NUMERIC"):
                    issues.append(
                        RuleValidationIssue(
                            rule_id=tr.rule_id,
                            code="SEMANTIC_ROLE_VIOLATION",
                            severity="BLOCKING",
                            dataset=tr.dataset,
                            column=tr.column,
                            message=f"Cannot apply string transformation '{tr.action_type}' to numeric/metric column '{tr.column}' (role={role_val}).",
                        )
                    )
            # PAD on DATE or METRIC
            elif tr.action_type == "pad":
                if role_val in ("DATE", "DATETIME", "TIMESTAMP"):
                    issues.append(
                        RuleValidationIssue(
                            rule_id=tr.rule_id,
                            code="SEMANTIC_ROLE_VIOLATION",
                            severity="BLOCKING",
                            dataset=tr.dataset,
                            column=tr.column,
                            message=f"Cannot apply padding transformation to temporal column '{tr.column}' (role={role_val}).",
                        )
                    )
