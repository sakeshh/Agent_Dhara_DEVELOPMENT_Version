from __future__ import annotations

import math
from typing import Dict, Optional
from agent.compiler.base.agent import BaseAgent
from agent.compiler.models.artifacts import CostEstimate
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType
from agent.compiler.models.physical import ExecutionEngine


class CostEstimator(BaseAgent):
    """Estimates computational runtime, memory requirements, and cloud costs before pipeline deployment."""

    # Hourly rate estimates ($ / hour)
    ENGINE_HOURLY_RATES: Dict[ExecutionEngine, float] = {
        ExecutionEngine.PYTHON: 0.08,    # Standard VM / Container app
        ExecutionEngine.AZURE_SQL: 0.35, # Azure SQL General Purpose vCore
        ExecutionEngine.PYSPARK: 0.85,   # Small Spark cluster (Driver + 2 Workers)
        ExecutionEngine.FABRIC: 0.75,    # Fabric Capacity Unit (CU)
        ExecutionEngine.DATABRICKS: 1.20,# Databricks DBU + Compute
        ExecutionEngine.ADF: 0.25,       # ADF Data Pipeline DIU rate
    }

    def estimate(self, context: CompilerContext) -> CostEstimate:
        # 1. Total row and column count
        total_rows = 0
        total_cols = 0
        for ds in context.dataset_profiles.values():
            total_rows += ds.row_count
            total_cols += max(ds.column_count, len(ds.columns), 1)

        total_rows = max(total_rows, 1000)
        total_cols = max(total_cols, 5)

        # 2. Complexity multiplier based on operations
        complexity_factor = 1.0
        if context.logical_plan:
            for step in context.logical_plan.steps:
                if step.action == ActionType.DEDUPLICATE:
                    complexity_factor += 0.4  # Sorting / hashing overhead
                elif step.action == ActionType.VALIDATE_REFERENTIAL_INTEGRITY:
                    complexity_factor += 0.5  # Cross-dataset join
                elif step.action in (ActionType.SANITIZE_EMAIL, ActionType.MASK_PHONE):
                    complexity_factor += 0.2  # Regex scanning

        # 3. Engine-specific throughput (rows per second)
        engine = context.target_engine
        if engine == ExecutionEngine.PYTHON:
            throughput = 120_000
            overhead_sec = 1.0
        elif engine == ExecutionEngine.AZURE_SQL:
            throughput = 350_000
            overhead_sec = 2.0
        elif engine in (ExecutionEngine.PYSPARK, ExecutionEngine.FABRIC, ExecutionEngine.DATABRICKS):
            throughput = 1_500_000
            overhead_sec = 8.0  # Spark initialization / DAG planning overhead
        else:
            throughput = 100_000
            overhead_sec = 2.0

        estimated_runtime_sec = overhead_sec + (total_rows * complexity_factor) / throughput
        estimated_runtime_sec = round(estimated_runtime_sec, 2)

        # 4. Memory estimation (MB)
        # Average 120 bytes per row per 10 columns * 2.5 safety headroom
        bytes_per_row = (total_cols / 10.0) * 120
        raw_data_mb = (total_rows * bytes_per_row) / (1024 * 1024)
        estimated_memory_mb = max(256.0, round(raw_data_mb * 2.5, 2))

        # 5. Cloud cost estimation ($)
        hourly_rate = self.ENGINE_HOURLY_RATES.get(engine, 0.25)
        cost_usd = (estimated_runtime_sec / 3600.0) * hourly_rate
        cost_usd = round(max(cost_usd, 0.001), 4)

        return CostEstimate(
            estimated_runtime_seconds=estimated_runtime_sec,
            estimated_memory_mb=estimated_memory_mb,
            estimated_cloud_cost_usd=cost_usd,
            notes=f"Based on {total_rows:,} rows across {total_cols} columns using {engine.value.upper()}.",
        )

    def execute(self, context: CompilerContext) -> CompilerContext:
        estimate = self.estimate(context)
        context.metadata["cost_estimate"] = estimate.model_dump()
        return context
