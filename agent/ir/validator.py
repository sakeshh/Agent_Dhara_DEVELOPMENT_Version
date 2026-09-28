from __future__ import annotations

import difflib
import logging
from typing import Dict, List, Optional
from agent.compiler.models.profile import DatasetProfile
from agent.ir.errors import IrErrorCode, IrValidationResult
from agent.ir.models import IrPlan, JoinType
from agent.ir.rule_expression import RuleExpression, RuleType

logger = logging.getLogger("agent.ir.validator")


class IrValidator:
    """
    Validates an IrPlan against concrete DatasetProfiles before it can enter the compiler.
    Verifies table existence, column existence, data type alignment, join viability, and rule definitions.
    """

    @staticmethod
    def validate(
        plan: IrPlan,
        dataset_profiles: Dict[str, DatasetProfile],
    ) -> IrValidationResult:
        result = IrValidationResult()
        available_tables = list(dataset_profiles.keys())

        if not available_tables:
            result.add_warning(
                error_code=IrErrorCode.MISSING_TABLE,
                message="No dataset profiles provided for validation; skipping strict schema checks.",
            )
            return result

        # 1. Source Nodes Validation
        for src in plan.sources:
            if src.table not in dataset_profiles:
                suggestions = difflib.get_close_matches(src.table, available_tables, n=3, cutoff=0.5)
                result.add_error(
                    error_code=IrErrorCode.MISSING_TABLE,
                    message=f"Source table '{src.table}' does not exist in dataset catalog.",
                    target_table=src.table,
                    suggestions=suggestions,
                )
            else:
                profile = dataset_profiles[src.table]
                avail_cols = list(profile.columns.keys())
                for col in src.columns:
                    if col not in profile.columns:
                        suggestions = difflib.get_close_matches(col, avail_cols, n=3, cutoff=0.5)
                        if plan.validation.strict_schema:
                            result.add_error(
                                error_code=IrErrorCode.MISSING_COLUMN,
                                message=f"Column '{col}' does not exist in table '{src.table}'.",
                                target_table=src.table,
                                target_column=col,
                                suggestions=suggestions,
                            )
                        else:
                            result.add_warning(
                                error_code=IrErrorCode.MISSING_COLUMN,
                                message=f"Column '{col}' does not exist in table '{src.table}'.",
                                target_table=src.table,
                                target_column=col,
                                suggestions=suggestions,
                            )

        # Helper to get available columns for a table or fallback
        def get_table_columns(tbl_name: Optional[str]) -> List[str]:
            if tbl_name and tbl_name in dataset_profiles:
                return list(dataset_profiles[tbl_name].columns.keys())
            # If table is unspecified but only 1 profile exists, use it
            if len(dataset_profiles) == 1:
                return list(next(iter(dataset_profiles.values())).columns.keys())
            return []

        # 2. Logical Steps Validation
        for step in plan.logical_plan:
            # Join check
            if step.join_node:
                jn = step.join_node
                for tbl in (jn.left_table, jn.right_table):
                    if tbl not in dataset_profiles:
                        suggestions = difflib.get_close_matches(tbl, available_tables, n=3, cutoff=0.5)
                        result.add_error(
                            error_code=IrErrorCode.MISSING_TABLE,
                            message=f"Join table '{tbl}' does not exist.",
                            target_table=tbl,
                            suggestions=suggestions,
                        )

                left_cols = get_table_columns(jn.left_table)
                right_cols = get_table_columns(jn.right_table)
                for lk in jn.left_keys:
                    if left_cols and lk not in left_cols:
                        suggestions = difflib.get_close_matches(lk, left_cols, n=3, cutoff=0.5)
                        result.add_error(
                            error_code=IrErrorCode.INVALID_JOIN,
                            message=f"Left join key '{lk}' not found in '{jn.left_table}'.",
                            target_table=jn.left_table,
                            target_column=lk,
                            suggestions=suggestions,
                        )
                for rk in jn.right_keys:
                    if right_cols and rk not in right_cols:
                        suggestions = difflib.get_close_matches(rk, right_cols, n=3, cutoff=0.5)
                        result.add_error(
                            error_code=IrErrorCode.INVALID_JOIN,
                            message=f"Right join key '{rk}' not found in '{jn.right_table}'.",
                            target_table=jn.right_table,
                            target_column=rk,
                            suggestions=suggestions,
                        )

            # Filter check
            if step.filter_node:
                fn = step.filter_node
                if fn.table and fn.table not in dataset_profiles:
                    suggestions = difflib.get_close_matches(fn.table, available_tables, n=3, cutoff=0.5)
                    result.add_error(
                        error_code=IrErrorCode.MISSING_TABLE,
                        message=f"Filter table '{fn.table}' does not exist.",
                        target_table=fn.table,
                        suggestions=suggestions,
                    )
                cols = get_table_columns(fn.table)
                if fn.column and cols and fn.column not in cols:
                    suggestions = difflib.get_close_matches(fn.column, cols, n=3, cutoff=0.5)
                    result.add_error(
                        error_code=IrErrorCode.MISSING_COLUMN,
                        message=f"Filter column '{fn.column}' does not exist.",
                        target_table=fn.table,
                        target_column=fn.column,
                        suggestions=suggestions,
                    )

            # Aggregate check
            if step.aggregate_node:
                an = step.aggregate_node
                cols = get_table_columns(an.table)
                if cols:
                    for g_col in an.group_by:
                        if g_col not in cols:
                            suggestions = difflib.get_close_matches(g_col, cols, n=3, cutoff=0.5)
                            result.add_error(
                                error_code=IrErrorCode.MISSING_COLUMN,
                                message=f"Group-by column '{g_col}' does not exist.",
                                target_table=an.table,
                                target_column=g_col,
                                suggestions=suggestions,
                            )
                    for agg in an.aggregations:
                        if agg.column not in cols and agg.column != "*":
                            suggestions = difflib.get_close_matches(agg.column, cols, n=3, cutoff=0.5)
                            result.add_error(
                                error_code=IrErrorCode.MISSING_COLUMN,
                                message=f"Aggregate column '{agg.column}' does not exist.",
                                target_table=an.table,
                                target_column=agg.column,
                                suggestions=suggestions,
                            )

            # Projection check
            if step.projection_node:
                pn = step.projection_node
                cols = get_table_columns(pn.table)
                if cols:
                    for p_col in pn.columns:
                        if p_col not in cols and p_col != "*":
                            suggestions = difflib.get_close_matches(p_col, cols, n=3, cutoff=0.5)
                            result.add_error(
                                error_code=IrErrorCode.MISSING_COLUMN,
                                message=f"Projection column '{p_col}' does not exist.",
                                target_table=pn.table,
                                target_column=p_col,
                                suggestions=suggestions,
                            )

        # 3. Business Rules Validation
        for rule in plan.business_rules:
            cols = get_table_columns(rule.target_table)
            if rule.column and cols and rule.column not in cols:
                suggestions = difflib.get_close_matches(rule.column, cols, n=3, cutoff=0.5)
                result.add_error(
                    error_code=IrErrorCode.INVALID_RULE,
                    message=f"Business rule targets non-existent column '{rule.column}'.",
                    target_table=rule.target_table,
                    target_column=rule.column,
                    suggestions=suggestions,
                )

        return result
