from __future__ import annotations

from typing import List, Optional
from agent.compiler.deduplication.duplicate_policy import (
    DuplicateDisposition,
    DuplicateResolutionPlan,
    DuplicateType,
    SurvivorStrategy,
)


class SurvivorSelector:
    """
    Translates high-level SurvivorStrategy into physical ordering clauses for
    SQL, Python, and PySpark compilers.
    """

    @staticmethod
    def get_sql_order_clause(plan: DuplicateResolutionPlan, all_columns: Optional[List[str]] = None) -> str:
        if plan.survivor_strategy == SurvivorStrategy.LATEST and plan.timestamp_column:
            return f"[{plan.timestamp_column}] DESC"
        elif plan.survivor_strategy == SurvivorStrategy.EARLIEST and plan.timestamp_column:
            return f"[{plan.timestamp_column}] ASC"
        elif plan.survivor_strategy == SurvivorStrategy.MOST_COMPLETE and all_columns:
            # Order by non-null count descending
            case_sums = " + ".join([f"CASE WHEN [{c}] IS NOT NULL THEN 1 ELSE 0 END" for c in all_columns])
            return f"({case_sums}) DESC"
        return "(SELECT NULL)"

    @staticmethod
    def get_python_sort_criteria(plan: DuplicateResolutionPlan) -> tuple[List[str], List[bool]]:
        if plan.survivor_strategy == SurvivorStrategy.LATEST and plan.timestamp_column:
            return ([plan.timestamp_column], [False])
        elif plan.survivor_strategy == SurvivorStrategy.EARLIEST and plan.timestamp_column:
            return ([plan.timestamp_column], [True])
        return ([], [])
