from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from agent.compiler.models.actions import (
    ActionSource,
    StructuredAction,
)
from agent.compiler.models.logical import ActionType
from agent.compiler.rules.normalized_rules import (
    ColumnBusinessRule,
    ConstraintType,
    CrossFieldBusinessRule,
    NormalizedBusinessRules,
    RuleAuthority,
    TransformationBusinessRule,
    ViolationDisposition,
)

logger = logging.getLogger(__name__)


class BusinessRuleCompiler:
    """
    Compiles canonical NormalizedBusinessRules into executable StructuredActions.
    
    SAFETY INVARIANT:
    A business constraint tells what must be true. It does NOT automatically authorize
    mutating data.
    - Pure constraints -> validation actions (CONDITIONAL_UPDATE with disposition FLAG/REJECT)
    - Explicit approved remediations -> mutating actions (RANGE_CLIP, FILL_NULLS, VALUE_MAP)
    """

    @classmethod
    def compile(
        cls,
        rules: NormalizedBusinessRules,
        semantic_plan: Optional[Any] = None,
    ) -> List[StructuredAction]:
        actions: List[StructuredAction] = []

        # 1. Compile ColumnBusinessRules
        for rule in rules.column_rules:
            if not rule.enabled:
                continue

            if rule.is_constraint_only:
                act = cls._compile_constraint_action(rule)
            elif rule.defines_explicit_remediation:
                act = cls._compile_remediation_action(rule, semantic_plan=semantic_plan)
            else:
                logger.warning("[BusinessRuleCompiler] Rule '%s' has ambiguous remediation; routing to manual review.", rule.rule_id)
                act = None

            if act:
                actions.append(act)

        # 2. Compile CrossFieldBusinessRules (pure assertions)
        for cf_rule in rules.cross_field_rules:
            if not cf_rule.enabled:
                continue
            act = cls._compile_cross_field_action(cf_rule)
            if act:
                actions.append(act)

        # 3. Compile TransformationBusinessRules (explicit domain operations)
        for tr_rule in rules.transformation_rules:
            if not tr_rule.enabled:
                continue
            act = cls._compile_transformation_action(tr_rule)
            if act:
                actions.append(act)

        return actions

    @classmethod
    def _compile_constraint_action(cls, rule: ColumnBusinessRule) -> Optional[StructuredAction]:
        """
        Translates a pure constraint rule into a non-mutating validation action.
        Condition identifies violating records. Disposition is FLAG (or REJECT if specified).
        """
        col = rule.column
        col_ref = f"[{col}]"
        disp = rule.on_violation_disposition.value

        if rule.constraint_type == ConstraintType.RANGE:
            min_v = rule.parameters.get("minimum")
            max_v = rule.parameters.get("maximum")
            conds = []
            if min_v is not None:
                conds.append(f"{col_ref} < {min_v}")
            if max_v is not None:
                conds.append(f"{col_ref} > {max_v}")
            if not conds:
                return None
            sql_condition = " OR ".join(conds)

            return StructuredAction(
                action_id=f"action_val_range_{rule.rule_id}",
                action_type=ActionType.CONDITIONAL_UPDATE,
                dataset_name=rule.dataset,
                target_columns=[col],
                parameters={
                    "condition": sql_condition,
                    "disposition": disp,
                    "new_value": None,
                    "else_value": None,
                },
                source=ActionSource.BUSINESS_RULE,
                source_reference=rule.rule_id,
                source_issue_ids=[rule.rule_id],
                extraction_method="deterministic_constraint",
                confidence=1.0,
                requires_approval=False,
                metadata={
                    "source_rule_id": rule.rule_id,
                    "source_rule_version": "1.0",
                    "source_authority": rule.authority.value,
                    "priority": rule.priority,
                    "is_constraint_only": True,
                    "disposition": disp,
                    "rule_description": rule.description,
                },
            )

        elif rule.constraint_type == ConstraintType.NOT_NULL:
            return StructuredAction(
                action_id=f"action_val_not_null_{rule.rule_id}",
                action_type=ActionType.CONDITIONAL_UPDATE,
                dataset_name=rule.dataset,
                target_columns=[col],
                parameters={
                    "condition": f"{col_ref} IS NULL",
                    "disposition": disp,
                    "new_value": None,
                    "else_value": None,
                },
                source=ActionSource.BUSINESS_RULE,
                source_reference=rule.rule_id,
                source_issue_ids=[rule.rule_id],
                extraction_method="deterministic_constraint",
                confidence=1.0,
                requires_approval=False,
                metadata={
                    "source_rule_id": rule.rule_id,
                    "source_rule_version": "1.0",
                    "source_authority": rule.authority.value,
                    "priority": rule.priority,
                    "is_constraint_only": True,
                    "disposition": disp,
                    "rule_description": rule.description,
                },
            )

        elif rule.constraint_type == ConstraintType.ALLOWED_VALUES:
            allowed = rule.parameters.get("allowed_values") or []
            if not allowed:
                return None
            allowed_quoted = ", ".join(f"'{v}'" for v in allowed)
            sql_condition = f"{col_ref} IS NOT NULL AND {col_ref} NOT IN ({allowed_quoted})"

            return StructuredAction(
                action_id=f"action_val_allowed_{rule.rule_id}",
                action_type=ActionType.CONDITIONAL_UPDATE,
                dataset_name=rule.dataset,
                target_columns=[col],
                parameters={
                    "condition": sql_condition,
                    "disposition": disp,
                    "new_value": None,
                    "else_value": None,
                },
                source=ActionSource.BUSINESS_RULE,
                source_reference=rule.rule_id,
                source_issue_ids=[rule.rule_id],
                extraction_method="deterministic_constraint",
                confidence=1.0,
                requires_approval=False,
                metadata={
                    "source_rule_id": rule.rule_id,
                    "source_rule_version": "1.0",
                    "source_authority": rule.authority.value,
                    "priority": rule.priority,
                    "is_constraint_only": True,
                    "disposition": disp,
                    "rule_description": rule.description,
                },
            )

        elif rule.constraint_type == ConstraintType.ASSERTION:
            expr = rule.parameters.get("expression") or ""
            if not expr:
                return None
            sql_condition = f"NOT ({expr})"

            return StructuredAction(
                action_id=f"action_val_assertion_{rule.rule_id}",
                action_type=ActionType.CONDITIONAL_UPDATE,
                dataset_name=rule.dataset,
                target_columns=[col],
                parameters={
                    "condition": sql_condition,
                    "disposition": disp,
                    "new_value": None,
                    "else_value": None,
                },
                source=ActionSource.BUSINESS_RULE,
                source_reference=rule.rule_id,
                source_issue_ids=[rule.rule_id],
                extraction_method="deterministic_constraint",
                confidence=1.0,
                requires_approval=False,
                metadata={
                    "source_rule_id": rule.rule_id,
                    "source_rule_version": "1.0",
                    "source_authority": rule.authority.value,
                    "priority": rule.priority,
                    "is_constraint_only": True,
                    "disposition": disp,
                    "rule_description": rule.description,
                },
            )

        return None

    @classmethod
    def _compile_remediation_action(
        cls,
        rule: ColumnBusinessRule,
        semantic_plan: Optional[Any] = None,
    ) -> Optional[StructuredAction]:
        """
        Translates an explicit remediation rule into a mutating action.
        Only called when rule.defines_explicit_remediation is True.
        """
        action_name = str(rule.explicit_remediation_action or "").lower()
        col = rule.column

        if action_name == "clip" or rule.constraint_type == ConstraintType.RANGE:
            min_v = rule.parameters.get("minimum")
            max_v = rule.parameters.get("maximum")
            return StructuredAction(
                action_id=f"action_rem_clip_{rule.rule_id}",
                action_type=ActionType.RANGE_CLIP,
                dataset_name=rule.dataset,
                target_columns=[col],
                parameters={
                    "min_value": float(min_v) if min_v is not None else None,
                    "max_value": float(max_v) if max_v is not None else None,
                },
                source=ActionSource.BUSINESS_RULE,
                source_reference=rule.rule_id,
                source_issue_ids=[rule.rule_id],
                extraction_method="explicit_remediation",
                confidence=1.0,
                requires_approval=rule.approval_required,
                metadata={
                    "source_rule_id": rule.rule_id,
                    "source_rule_version": "1.0",
                    "source_authority": rule.authority.value,
                    "priority": rule.priority,
                    "is_constraint_only": False,
                    "rule_description": rule.description,
                },
            )

        elif action_name == "fill" or rule.constraint_type == ConstraintType.NOT_NULL:
            # Check if column is a primary key: automatic fill on primary key is prohibited!
            if semantic_plan and hasattr(semantic_plan, "column_roles"):
                role = semantic_plan.column_roles.get(col) or semantic_plan.column_roles.get(col.lower())
                role_val = getattr(role, "value", str(role)).upper()
                if role_val in ("PRIMARY_KEY", "SURROGATE_KEY", "BUSINESS_KEY"):
                    logger.error("[BusinessRuleCompiler] Prohibiting automatic fill on key column '%s'.", col)
                    return None

            fill_val = rule.parameters.get("default_value")
            if fill_val is None:
                fill_val = rule.parameters.get("fill_value")

            return StructuredAction(
                action_id=f"action_rem_fill_{rule.rule_id}",
                action_type=ActionType.FILL_NULLS,
                dataset_name=rule.dataset,
                target_columns=[col],
                parameters={"fill_value": fill_val},
                source=ActionSource.BUSINESS_RULE,
                source_reference=rule.rule_id,
                source_issue_ids=[rule.rule_id],
                extraction_method="explicit_remediation",
                confidence=1.0,
                requires_approval=rule.approval_required,
                metadata={
                    "source_rule_id": rule.rule_id,
                    "source_rule_version": "1.0",
                    "source_authority": rule.authority.value,
                    "priority": rule.priority,
                    "is_constraint_only": False,
                    "rule_description": rule.description,
                },
            )

        elif action_name == "map" or rule.constraint_type == ConstraintType.ALLOWED_VALUES:
            aliases = rule.parameters.get("aliases") or {}
            if not aliases:
                logger.warning("[BusinessRuleCompiler] Cannot compile VALUE_MAP without explicit aliases mapping.")
                return None

            return StructuredAction(
                action_id=f"action_rem_vmap_{rule.rule_id}",
                action_type=ActionType.VALUE_MAP,
                dataset_name=rule.dataset,
                target_columns=[col],
                parameters={"mapping": dict(aliases)},
                source=ActionSource.BUSINESS_RULE,
                source_reference=rule.rule_id,
                source_issue_ids=[rule.rule_id],
                extraction_method="explicit_remediation",
                confidence=1.0,
                requires_approval=rule.approval_required,
                metadata={
                    "source_rule_id": rule.rule_id,
                    "source_rule_version": "1.0",
                    "source_authority": rule.authority.value,
                    "priority": rule.priority,
                    "is_constraint_only": False,
                    "rule_description": rule.description,
                },
            )

        return None

    @classmethod
    def _compile_cross_field_action(cls, rule: CrossFieldBusinessRule) -> Optional[StructuredAction]:
        disp = rule.on_violation_disposition.value
        sql_condition = f"NOT ({rule.expression})"

        return StructuredAction(
            action_id=f"action_cross_{rule.rule_id}",
            action_type=ActionType.CONDITIONAL_UPDATE,
            dataset_name=rule.dataset,
            target_columns=list(rule.columns),
            parameters={
                "condition": sql_condition,
                "disposition": disp,
                "new_value": None,
                "else_value": None,
            },
            source=ActionSource.BUSINESS_RULE,
            source_reference=rule.rule_id,
            source_issue_ids=[rule.rule_id],
            extraction_method="deterministic_cross_field",
            confidence=1.0,
            requires_approval=rule.approval_required,
            metadata={
                "source_rule_id": rule.rule_id,
                "source_rule_version": "1.0",
                "source_authority": rule.authority.value,
                "priority": rule.priority,
                "is_constraint_only": True,
                "disposition": disp,
                "rule_description": rule.description,
            },
        )

    @classmethod
    def _compile_transformation_action(cls, rule: TransformationBusinessRule) -> Optional[StructuredAction]:
        act_type_str = rule.action_type.lower()
        act_enum = None
        for a in ActionType:
            if a.value == act_type_str:
                act_enum = a
                break

        if not act_enum:
            logger.warning("[BusinessRuleCompiler] Unknown action type '%s' in transformation rule '%s'.", act_type_str, rule.rule_id)
            return None

        return StructuredAction(
            action_id=f"action_tr_{rule.rule_id}",
            action_type=act_enum,
            dataset_name=rule.dataset,
            target_columns=[rule.column],
            parameters=dict(rule.parameters),
            source=ActionSource.BUSINESS_RULE,
            source_reference=rule.rule_id,
            source_issue_ids=[rule.rule_id],
            extraction_method="deterministic_transformation",
            confidence=1.0,
            metadata={
                "source_rule_id": rule.rule_id,
                "source_rule_version": "1.0",
                "source_authority": rule.authority.value,
                "priority": rule.priority,
                "is_transformation": True,
                "rule_description": rule.description,
            },
        )
