from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from agent.compiler.models.context import CompilerContext


class GenerationMetrics(BaseModel):
    generation_time_ms: float = 0.0
    validation_time_ms: float = 0.0
    repair_time_ms: float = 0.0
    total_compilation_time_ms: float = 0.0


class RepairMetrics(BaseModel):
    repair_attempted: bool = False
    repair_success: bool = True
    attempts: int = 0
    repair_success_pct: float = 100.0
    dialect_failures: Dict[str, int] = Field(default_factory=dict)


class PlanningMetrics(BaseModel):
    initial_step_count: int = 0
    optimized_step_count: int = 0
    optimization_reduction_pct: float = 0.0
    operator_fusion_count: int = 0
    dead_step_count: int = 0


class QualityMetrics(BaseModel):
    tests_generated: int = 0
    contracts_generated: bool = False
    lineage_coverage_pct: float = 100.0


class CompilerMetricsReport(BaseModel):
    generation: GenerationMetrics = Field(default_factory=GenerationMetrics)
    repair: RepairMetrics = Field(default_factory=RepairMetrics)
    planning: PlanningMetrics = Field(default_factory=PlanningMetrics)
    quality: QualityMetrics = Field(default_factory=QualityMetrics)


class CompilerMetricsCollector:
    """Collects and aggregates compiler telemetry across Generation, Repair, Planning, and Quality dimensions."""

    def collect(self, context: CompilerContext) -> CompilerMetricsReport:
        history = list(context.metadata.get("agent_metrics_history") or [])
        report = CompilerMetricsReport()

        # 1. Generation timings
        total_ms = 0.0
        for entry in history:
            agent = entry.get("agent_name", "")
            ms = entry.get("execution_ms", 0.0)
            total_ms += ms

            if any(k in agent for k in ["CodegenAgent", "LanguageRouter"]):
                report.generation.generation_time_ms += ms
            elif any(k in agent for k in ["Validator", "Validation"]):
                report.generation.validation_time_ms += ms
            elif "Repair" in agent:
                report.generation.repair_time_ms += ms

        report.generation.total_compilation_time_ms = round(total_ms, 2)
        report.generation.generation_time_ms = round(report.generation.generation_time_ms, 2)
        report.generation.validation_time_ms = round(report.generation.validation_time_ms, 2)
        report.generation.repair_time_ms = round(report.generation.repair_time_ms, 2)

        # 2. Repair telemetry
        repair_analytics = context.metadata.get("repair_analytics")
        if repair_analytics:
            report.repair.repair_attempted = True
            report.repair.attempts = repair_analytics.get("total_repairs", 0)
            report.repair.repair_success = (context.validation_result.is_valid if context.validation_result else True)
            report.repair.repair_success_pct = 100.0 if report.repair.repair_success else 0.0

        # 3. Planning metrics
        if context.logical_plan:
            opt_meta = context.logical_plan.metadata
            fused = opt_meta.get("fused_count", 0)
            initial_steps = opt_meta.get("initial_steps", len(context.logical_plan.steps))
            current_steps = len(context.logical_plan.steps)

            if fused == 0:
                fused = sum(len(s.fused_operations) for s in context.logical_plan.steps)

            report.planning.initial_step_count = initial_steps
            report.planning.optimized_step_count = current_steps
            report.planning.operator_fusion_count = fused
            report.planning.dead_step_count = max(0, initial_steps - current_steps - fused)
            if initial_steps > 0:
                reduction = ((initial_steps - current_steps) / initial_steps) * 100.0
                report.planning.optimization_reduction_pct = round(reduction, 1)

        # 4. Quality metrics
        comp_test = context.metadata.get("companion_test_artifact", {})
        if comp_test:
            meta = comp_test.get("metadata", {})
            report.quality.tests_generated = meta.get("assertion_count", 1)

        report.quality.contracts_generated = bool(context.metadata.get("data_contract_yaml"))

        lineage_entries = context.metadata.get("lineage_entries")
        if lineage_entries is not None:
            report.quality.lineage_coverage_pct = 100.0

        return report
