from __future__ import annotations

from typing import Any, Dict, List
from agent.compiler.base.agent import BaseAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType, LogicalStep
from agent.compiler.models.physical import ExecutionEngine, PhysicalPlan, PhysicalStep


class PhysicalPlanner(BaseAgent):
    """Translates engine-agnostic LogicalSteps into engine-specific PhysicalSteps."""

    # Python translation strategies
    PYTHON_STRATEGIES: Dict[ActionType, tuple[str, str]] = {
        ActionType.TRIM: ("str_strip", "str_strip.j2"),
        ActionType.LOWERCASE: ("str_lower", "str_lower.j2"),
        ActionType.UPPERCASE: ("str_upper", "str_upper.j2"),
        ActionType.CAST_TYPE: ("astype_or_to_numeric", "cast_type.j2"),
        ActionType.COERCE_NUMERIC: ("to_numeric_coerce", "coerce_numeric.j2"),
        ActionType.PARSE_DATES: ("to_datetime", "parse_dates.j2"),
        ActionType.SANITIZE_EMAIL: ("email_clean_regex", "sanitize_email.j2"),
        ActionType.NORMALIZE_PHONE: ("phone_normalize_digits", "normalize_phone.j2"),
        ActionType.MASK_PHONE: ("phone_mask_last4", "mask_phone.j2"),
        ActionType.RANGE_CLIP: ("clip_min_max", "range_clip.j2"),
        ActionType.CAP_OUTLIERS: ("quantile_clip", "cap_outliers.j2"),
        ActionType.STANDARDIZE_BOOLEAN: ("map_boolean_values", "standardize_boolean.j2"),
        ActionType.FILL_NULLS: ("fillna", "fill_nulls.j2"),
        ActionType.ZERO_TO_NULL: ("replace_zeros_with_nan", "zero_to_null.j2"),
        ActionType.REPLACE_SENTINELS: ("replace_sentinels", "replace_sentinels.j2"),
        ActionType.DROP_COLUMN: ("drop_columns", "drop_column.j2"),
        ActionType.DEDUPLICATE: ("drop_duplicates", "deduplicate.j2"),
        ActionType.VALIDATE_REFERENTIAL_INTEGRITY: ("merge_quarantine_orphans", "ri_check.j2"),
        ActionType.CUSTOM_EXPRESSION: ("eval_expression", "custom.j2"),
    }

    # SQL translation strategies
    SQL_STRATEGIES: Dict[ActionType, tuple[str, str]] = {
        ActionType.TRIM: ("tsql_trim", "trim.j2"),
        ActionType.LOWERCASE: ("tsql_lower", "lower.j2"),
        ActionType.UPPERCASE: ("tsql_upper", "upper.j2"),
        ActionType.CAST_TYPE: ("tsql_try_cast", "cast_type.j2"),
        ActionType.COERCE_NUMERIC: ("tsql_try_cast_numeric", "coerce_numeric.j2"),
        ActionType.PARSE_DATES: ("tsql_try_convert_date", "parse_dates.j2"),
        ActionType.SANITIZE_EMAIL: ("tsql_regex_email", "sanitize_email.j2"),
        ActionType.NORMALIZE_PHONE: ("tsql_clean_digits", "normalize_phone.j2"),
        ActionType.MASK_PHONE: ("tsql_mask_phone", "mask_phone.j2"),
        ActionType.RANGE_CLIP: ("tsql_case_when_clip", "range_clip.j2"),
        ActionType.STANDARDIZE_BOOLEAN: ("tsql_case_boolean", "standardize_boolean.j2"),
        ActionType.FILL_NULLS: ("tsql_coalesce", "fill_nulls.j2"),
        ActionType.ZERO_TO_NULL: ("tsql_nullif_zero", "zero_to_null.j2"),
        ActionType.REPLACE_SENTINELS: ("tsql_nullif_sentinels", "replace_sentinels.j2"),
        ActionType.DEDUPLICATE: ("tsql_row_number_partition", "deduplicate.j2"),
        ActionType.VALIDATE_REFERENTIAL_INTEGRITY: ("tsql_left_anti_join", "ri_check.j2"),
    }

    # Spark translation strategies
    SPARK_STRATEGIES: Dict[ActionType, tuple[str, str]] = {
        ActionType.TRIM: ("spark_trim", "str_strip.j2"),
        ActionType.LOWERCASE: ("spark_lower", "str_lower.j2"),
        ActionType.UPPERCASE: ("spark_upper", "str_upper.j2"),
        ActionType.CAST_TYPE: ("spark_cast", "cast_type.j2"),
        ActionType.PARSE_DATES: ("spark_to_date", "parse_dates.j2"),
        ActionType.FILL_NULLS: ("spark_fillna", "fill_nulls.j2"),
        ActionType.ZERO_TO_NULL: ("spark_zero_to_null", "zero_to_null.j2"),
        ActionType.RANGE_CLIP: ("spark_range_clip", "range_clip.j2"),
        ActionType.DEDUPLICATE: ("spark_drop_duplicates", "deduplicate.j2"),
        ActionType.VALIDATE_REFERENTIAL_INTEGRITY: ("spark_broadcast_join", "ri_check.j2"),
    }

    def _map_step(self, step: LogicalStep, engine: ExecutionEngine, order: int) -> PhysicalStep:
        if engine == ExecutionEngine.PYTHON:
            strat, tmpl = self.PYTHON_STRATEGIES.get(step.action, ("custom_python", "custom.j2"))
        elif engine == ExecutionEngine.AZURE_SQL:
            strat, tmpl = self.SQL_STRATEGIES.get(step.action, ("custom_sql", "custom.j2"))
        elif engine == ExecutionEngine.PYSPARK:
            strat, tmpl = self.SPARK_STRATEGIES.get(step.action, ("spark_" + step.action.value, f"{step.action.value}.j2"))
        elif engine == ExecutionEngine.FABRIC:
            strat, tmpl = self.SPARK_STRATEGIES.get(step.action, ("fabric_" + step.action.value, f"{step.action.value}.j2"))
        elif engine == ExecutionEngine.DATABRICKS:
            strat, tmpl = self.SPARK_STRATEGIES.get(step.action, ("dlt_" + step.action.value, f"{step.action.value}.j2"))
        elif engine == ExecutionEngine.ADF:
            strat, tmpl = (f"adf_{step.action.value}", "adf_activity.j2")
        else:
            strat, tmpl = ("generic_strategy", f"{step.action.value}.j2")

        # Handle fused operations
        template_params = dict(step.parameters)
        template_params["columns"] = step.target_columns
        template_params["dataset_name"] = step.dataset_name
        template_params["fused_operations"] = [op.value for op in step.fused_operations]

        return PhysicalStep(
            step_id=f"phys_{step.step_id}",
            logical_step_id=step.step_id,
            dataset_name=step.dataset_name,
            strategy_name=strat,
            template_name=tmpl,
            target_columns=step.target_columns,
            template_parameters=template_params,
            execution_order=order,
            description=step.description,
        )

    def execute(self, context: CompilerContext) -> CompilerContext:
        if not context.logical_plan:
            raise ValueError("Cannot run PhysicalPlanner without a LogicalPlan in CompilerContext.")

        physical_steps: List[PhysicalStep] = []
        for order, step in enumerate(context.logical_plan.steps):
            phys = self._map_step(step, context.target_engine, order)
            physical_steps.append(phys)

        physical_plan = PhysicalPlan(
            plan_id=f"phys_{context.logical_plan.plan_id}",
            logical_plan_id=context.logical_plan.plan_id,
            target_engine=context.target_engine,
            dataset_names=context.logical_plan.dataset_names,
            steps=physical_steps,
            io_config=dict(context.options.get("io_config") or {}),
            metadata={"step_count": len(physical_steps)},
        )

        context.physical_plan = physical_plan

        EventBus.get_instance().emit(
            event_type=CompilerEventType.PHYSICAL_PLAN_COMPILED,
            session_id=context.session_id,
            payload={
                "physical_plan_id": physical_plan.plan_id,
                "target_engine": context.target_engine.value,
                "step_count": len(physical_steps),
            },
            source_agent=self.__class__.__name__,
        )

        return context
