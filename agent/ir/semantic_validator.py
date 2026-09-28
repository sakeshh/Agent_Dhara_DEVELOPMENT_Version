from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from agent.compiler.models.profile import DatasetProfile
from agent.ir.errors import IrErrorCode, IrValidationResult
from agent.ir.models import IrPlan
from agent.ir.rule_expression import RuleExpression, RuleType

logger = logging.getLogger("agent.ir.semantic_validator")


class BusinessRuleValidator:
    """
    Validates semantic correctness and cross-field domain consistency of an IrPlan.
    Catches logical contradictions, impossible filters, and domain rule violations
    that would pass basic schema syntax checks.
    """

    @classmethod
    def validate_semantics(
        cls,
        plan: IrPlan,
        dataset_profiles: Dict[str, DatasetProfile],
        known_business_constraints: Optional[List[Dict[str, Any]]] = None,
    ) -> IrValidationResult:
        result = IrValidationResult()

        # 1. Collect all filter conditions per column
        column_filters: Dict[str, List[Dict[str, Any]]] = {}
        for step in plan.logical_plan:
            if step.filter_node and step.filter_node.column:
                col = step.filter_node.column
                if col not in column_filters:
                    column_filters[col] = []
                column_filters[col].append({
                    "operator": step.filter_node.operator,
                    "value": step.filter_node.value,
                    "step_id": step.step_id,
                })

        # 2. Check for Contradictory Numeric Bounds (e.g. Score > 80 AND Score < 50)
        for col, conds in column_filters.items():
            min_bound = None
            max_bound = None
            for c in conds:
                op = c.get("operator")
                val = c.get("value")
                if isinstance(val, (int, float)):
                    if op in ("gt", "gte", ">", ">="):
                        min_bound = val if min_bound is None else max(min_bound, val)
                    elif op in ("lt", "lte", "<", "<="):
                        max_bound = val if max_bound is None else min(max_bound, val)

            if min_bound is not None and max_bound is not None and min_bound > max_bound:
                result.add_error(
                    error_code=IrErrorCode.INVALID_RULE,
                    message=f"Contradictory filter range on '{col}': lower bound ({min_bound}) exceeds upper bound ({max_bound}).",
                    target_column=col,
                    details={"min_bound": min_bound, "max_bound": max_bound},
                )

        # 3. Check for Contradictory Equality Conditions (e.g. Status == 'Certified' AND Status == 'Failed')
        for col, conds in column_filters.items():
            eq_values = [
                c.get("value") for c in conds
                if c.get("operator") in ("eq", "=", "==") and c.get("value") is not None
            ]
            if len(set(eq_values)) > 1:
                result.add_error(
                    error_code=IrErrorCode.INVALID_RULE,
                    message=f"Mutually exclusive equality filters on '{col}': {eq_values}",
                    target_column=col,
                    details={"conflicting_values": eq_values},
                )

        # 4. Cross-Field Semantic Rule Inconsistencies
        # Example: Certified implies Score >= 60
        cert_filter = None
        score_filter = None
        for col, conds in column_filters.items():
            if "cert" in col.lower():
                for c in conds:
                    if str(c.get("value", "")).lower() == "certified":
                        cert_filter = c
            if "score" in col.lower():
                for c in conds:
                    op = c.get("operator")
                    val = c.get("value")
                    if isinstance(val, (int, float)) and op in ("lt", "lte", "<", "<="):
                        score_filter = (op, val)

        if cert_filter and score_filter:
            op, val = score_filter
            if val < 60:
                result.add_warning(
                    error_code=IrErrorCode.INVALID_RULE,
                    message=f"Potential semantic conflict: 'Certified' status typically requires Score >= 60, but filter requires {op} {val}.",
                    target_column="Score",
                    details={"certification": cert_filter, "score_bound": score_filter},
                )

        return result
