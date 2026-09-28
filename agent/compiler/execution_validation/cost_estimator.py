from __future__ import annotations

import logging
from typing import List
from agent.compiler.execution_validation.models import CostEstimate
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.physical import ExecutionEngine

logger = logging.getLogger("agent.compiler.execution_validation.cost")


class RuntimeCostEstimator:
    """
    Analyzes CompilerContext row volumes, join topologies, and shuffle operations
    to produce deterministic pre-execution cost, memory, and runtime estimates.
    """

    @classmethod
    def estimate(cls, context: CompilerContext) -> CostEstimate:
        total_rows = sum(p.row_count for p in context.dataset_profiles.values())
        steps = context.logical_plan.steps if context.logical_plan else []

        shuffle_ops = 0
        warnings: List[str] = []

        # Configuration thresholds with enterprise defaults
        cost_cfg = context.metadata.get("cost_estimator_config") or {}
        broadcast_small_limit = cost_cfg.get("broadcast_small_table_rows", 50000)
        broadcast_large_limit = cost_cfg.get("broadcast_large_table_rows", 500000)

        # Count operations requiring network shuffle or heavy sorting
        for s in steps:
            action = s.action.value.lower()
            if action in ("deduplicate", "group_by", "aggregate"):
                shuffle_ops += 1
            elif "join" in action:
                shuffle_ops += 2

        recommendations: List[str] = []

        # Check broadcast join feasibility
        broadcast_rec = False
        profiles = list(context.dataset_profiles.values())
        if len(profiles) >= 2:
            small_table = any(p.row_count > 0 and p.row_count <= broadcast_small_limit for p in profiles)
            large_table = any(p.row_count >= broadcast_large_limit for p in profiles)
            if small_table and large_table:
                broadcast_rec = True

        # Tier classification
        if total_rows <= 100000 and shuffle_ops <= 1:
            cost_tier = "low"
            est_runtime = "< 5s"
            memory_mb = 512
            complexity = 1.0
        elif total_rows <= 2000000 and shuffle_ops <= 3:
            cost_tier = "medium"
            est_runtime = "15s - 45s"
            memory_mb = 2048
            complexity = 2.5
        else:
            cost_tier = "high"
            est_runtime = "> 1m"
            memory_mb = 8192
            complexity = 4.5
            warnings.append(f"High-volume data processing ({total_rows:,} rows); cluster scaling or partition pruning recommended.")

        if shuffle_ops > 2 and context.target_engine in (ExecutionEngine.PYSPARK, ExecutionEngine.DATABRICKS):
            warnings.append(f"Execution involves {shuffle_ops} shuffle operations; ensure spark.sql.shuffle.partitions is tuned.")

        if broadcast_rec and context.target_engine in (ExecutionEngine.PYSPARK, ExecutionEngine.DATABRICKS):
            recommendations.append("Broadcast join feasible for smaller lookup table; broadcast hint recommended.")

        return CostEstimate(
            estimated_cost=cost_tier,
            shuffle_operations=shuffle_ops,
            broadcast_recommended=broadcast_rec,
            estimated_runtime=est_runtime,
            estimated_memory_mb=memory_mb,
            complexity_score=complexity,
            cost_breakdown={
                "total_rows": total_rows,
                "shuffle_operations": shuffle_ops,
                "table_count": len(profiles),
                "step_count": len(steps),
                "engine": context.target_engine.value,
            },
            warnings=warnings,
            recommendations=recommendations,
        )
