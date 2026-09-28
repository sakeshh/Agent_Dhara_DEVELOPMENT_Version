from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union
from agent.compiler.models.profile import DatasetProfile
from agent.ir.models import IrPlan
from agent.ir.rule_expression import ComparisonOperator

logger = logging.getLogger("agent.ir.normalizer")

# Canonical operator map
_OPERATOR_MAP = {
    "=": "eq",
    "==": "eq",
    "eq": "eq",
    "equals": "eq",
    "!=": "neq",
    "<>": "neq",
    "neq": "neq",
    "not_equals": "neq",
    ">": "gt",
    "gt": "gt",
    "greater_than": "gt",
    ">=": "gte",
    "gte": "gte",
    "greater_than_or_equal": "gte",
    "<": "lt",
    "lt": "lt",
    "less_than": "lt",
    "<=": "lte",
    "lte": "lte",
    "less_than_or_equal": "lte",
    "like": "like",
    "in": "in",
    "is_null": "is_null",
    "not_null": "not_null",
}


class IrNormalizer:
    """
    Normalizes an IrPlan before it reaches the Builder and Compiler:
    1. Resolves table and column names to canonical casing from schema profiles (e.g. score -> Score).
    2. Standardizes filter and comparison operators (e.g. == -> eq, > -> gt).
    3. Trims whitespace and strips redundant syntax.
    4. Normalizes lineage and rule targets.
    """

    @staticmethod
    def normalize(
        plan: IrPlan,
        dataset_profiles: Dict[str, DatasetProfile],
    ) -> IrPlan:
        # Build case-insensitive lookup dictionaries
        tbl_lookup: Dict[str, str] = {t.lower(): t for t in dataset_profiles.keys()}
        col_lookup: Dict[str, Dict[str, str]] = {}
        for t_name, profile in dataset_profiles.items():
            col_lookup[t_name.lower()] = {c.lower(): c for c in profile.columns.keys()}

        def canonical_table(tbl: Optional[str]) -> Optional[str]:
            if not tbl:
                return tbl
            return tbl_lookup.get(tbl.lower(), tbl)

        def canonical_column(tbl: Optional[str], col: Optional[str]) -> Optional[str]:
            if not col or col == "*":
                return col
            # If table is known, check table's columns
            if tbl and tbl.lower() in col_lookup:
                return col_lookup[tbl.lower()].get(col.lower(), col)
            # If only 1 table in schema, check it
            if len(col_lookup) == 1:
                single_tbl = next(iter(col_lookup.values()))
                return single_tbl.get(col.lower(), col)
            # Search across all tables
            for cols in col_lookup.values():
                if col.lower() in cols:
                    return cols[col.lower()]
            return col

        def normalize_operator(op: Optional[str]) -> str:
            if not op:
                return "eq"
            return _OPERATOR_MAP.get(op.strip().lower(), op.strip().lower())

        # 1. Normalize Sources
        for src in plan.sources:
            canon_tbl = canonical_table(src.table)
            if canon_tbl:
                src.table = canon_tbl
            src.columns = [canonical_column(src.table, c) or c for c in src.columns]

        # 2. Normalize Logical Plan
        for step in plan.logical_plan:
            if step.source_node:
                s = step.source_node
                s.table = canonical_table(s.table) or s.table
                s.columns = [canonical_column(s.table, c) or c for c in s.columns]

            if step.filter_node:
                fn = step.filter_node
                fn.table = canonical_table(fn.table)
                fn.column = canonical_column(fn.table, fn.column)
                fn.operator = normalize_operator(fn.operator)
                if fn.rule:
                    fn.rule.target_table = canonical_table(fn.rule.target_table)
                    fn.rule.column = canonical_column(fn.rule.target_table, fn.rule.column)

            if step.join_node:
                jn = step.join_node
                jn.left_table = canonical_table(jn.left_table) or jn.left_table
                jn.right_table = canonical_table(jn.right_table) or jn.right_table
                jn.left_keys = [canonical_column(jn.left_table, k) or k for k in jn.left_keys]
                jn.right_keys = [canonical_column(jn.right_table, k) or k for k in jn.right_keys]

            if step.aggregate_node:
                an = step.aggregate_node
                an.table = canonical_table(an.table)
                an.group_by = [canonical_column(an.table, g) or g for g in an.group_by]
                for agg in an.aggregations:
                    agg.column = canonical_column(an.table, agg.column) or agg.column

            if step.projection_node:
                pn = step.projection_node
                pn.table = canonical_table(pn.table)
                pn.columns = [canonical_column(pn.table, c) or c for c in pn.columns]
                new_aliases = {}
                for c, alias in pn.aliases.items():
                    canon_c = canonical_column(pn.table, c) or c
                    new_aliases[canon_c] = alias.strip()
                pn.aliases = new_aliases

        # 3. Normalize Business Rules
        for rule in plan.business_rules:
            rule.target_table = canonical_table(rule.target_table)
            rule.column = canonical_column(rule.target_table, rule.column)

        # 4. Normalize Lineage
        plan.lineage.source_tables = [
            canonical_table(t) or t for t in plan.lineage.source_tables
        ]
        plan.lineage.target_tables = [
            canonical_table(t) or t for t in plan.lineage.target_tables
        ]

        return plan
