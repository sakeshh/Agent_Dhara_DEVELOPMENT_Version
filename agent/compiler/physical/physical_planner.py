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
        # Level 2 Composable Operators
        ActionType.PREFIX_ADD: ("tsql_prefix_add", "prefix_add.j2"),
        ActionType.SUFFIX_ADD: ("tsql_suffix_add", "suffix_add.j2"),
        ActionType.PATTERN_REPLACE: ("tsql_pattern_replace", "pattern_replace.j2"),
        ActionType.VALUE_MAP: ("tsql_value_map", "value_map.j2"),
        ActionType.PAD: ("tsql_pad", "pad.j2"),
        ActionType.CONDITIONAL_UPDATE: ("tsql_conditional_update", "conditional_update.j2"),
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
        from agent.errors import UnsupportedCapabilityError
        import re

        template_params = dict(step.parameters)
        template_params["columns"] = step.target_columns
        template_params["dataset_name"] = step.dataset_name
        template_params["fused_operations"] = [op.value for op in step.fused_operations]
        step_meta = getattr(step, "metadata", {}) or {}
        if "max_failed_rows" not in template_params and step_meta.get("max_failed_rows") is not None:
            template_params["max_failed_rows"] = step_meta["max_failed_rows"]
        if "track_failures" not in template_params and step_meta.get("track_failures") is not None:
            template_params["track_failures"] = step_meta["track_failures"]

        if engine == ExecutionEngine.PYTHON:
            strat, tmpl = self.PYTHON_STRATEGIES.get(step.action, ("custom_python", "custom.j2"))
        elif engine == ExecutionEngine.AZURE_SQL:
            strat, tmpl = self.SQL_STRATEGIES.get(step.action, ("custom_sql", "custom.j2"))

            # Physical capability enforcement for Azure SQL:
            if step.action == ActionType.PATTERN_REPLACE:
                pat = str(step.parameters.get("pattern") or "")
                is_regex = bool(step.parameters.get("regex", True))
                if is_regex:
                    regex_tokens = [r"\d", r"\w", r"\s", r"\D", r"\W", r"\S", ".*", ".+", "^", "$", "(", ")", "[0-9]", "[a-z]"]
                    if any(t in pat for t in regex_tokens) or re.search(r"[\^\$\*\+\?\{\}\[\]\(\)\|\\]", pat):
                        raise UnsupportedCapabilityError(
                            f"Azure SQL does not support POSIX regular expression replacement for pattern '{pat}'. "
                            f"Regular expression replacements require Python or PySpark engine, or literal replacement.",
                            engine="sql",
                            action="pattern_replace",
                        )

            elif step.action == ActionType.CONDITIONAL_UPDATE:
                from agent.compiler.templates.engine import render_literal
                raw_cond = step.parameters.get("condition")

                if isinstance(raw_cond, dict):
                    col = str(raw_cond.get("column") or "").strip().replace("[", "").replace("]", "")
                    raw_op = raw_cond.get("operator") or "EQ"
                    if hasattr(raw_op, "value"):
                        op = str(raw_op.value).upper().strip()
                    elif "." in str(raw_op):
                        op = str(raw_op).split(".")[-1].upper().strip()
                    else:
                        op = str(raw_op).upper().strip()
                    val = raw_cond.get("value")
                    op_map = {
                        "EQ": "=", "NEQ": "<>", "IS_NULL": "IS NULL", "IS_NOT_NULL": "IS NOT NULL",
                        "GT": ">", "LT": "<", "GTE": ">=", "LTE": "<=", "LIKE": "LIKE"
                    }
                    if not col or op not in op_map:
                        raise UnsupportedCapabilityError(
                            f"Invalid structured condition operator '{op}' or missing column in conditional_update.",
                            engine="sql", action="conditional_update"
                        )
                    sql_op = op_map[op]
                    if sql_op in ("IS NULL", "IS NOT NULL"):
                        sql_cond = f"[{col}] {sql_op}"
                    else:
                        sql_cond = f"[{col}] {sql_op} {render_literal(val)}"
                elif isinstance(raw_cond, str):
                    forbidden = [";", "--", "/*", "DROP", "ALTER", "EXEC", "DELETE", "TRUNCATE", "INSERT"]
                    if any(f in raw_cond.upper() for f in forbidden):
                        raise UnsupportedCapabilityError(
                            f"Azure SQL conditional_update rejects arbitrary unvalidated expression '{raw_cond}'.",
                            engine="sql", action="conditional_update"
                        )
                    safe_pat = r"^\s*\[?([a-zA-Z0-9_]+)\]?\s+(IS\s+NULL|IS\s+NOT\s+NULL|=|<>|!=|>|<|>=|<=|LIKE)\s*(.*)$"
                    m = re.match(safe_pat, raw_cond.strip(), re.IGNORECASE)
                    if not m:
                        raise UnsupportedCapabilityError(
                            f"Azure SQL conditional_update rejects arbitrary expression '{raw_cond}'. "
                            f"Use structured condition {{column, operator, value}} or simple predicate '[col] = val'.",
                            engine="sql", action="conditional_update"
                        )
                    col_name = m.group(1)
                    op_str = m.group(2).upper()
                    val_part = m.group(3).strip()
                    if op_str in ("IS NULL", "IS NOT NULL"):
                        sql_cond = f"[{col_name}] {op_str}"
                    else:
                        if (val_part.startswith("'") and val_part.endswith("'")) or (val_part.startswith('"') and val_part.endswith('"')):
                            val_to_render = val_part[1:-1]
                        elif re.match(r"^-?\d+$", val_part):
                            val_to_render = int(val_part)
                        elif re.match(r"^-?\d+\.\d+$", val_part):
                            val_to_render = float(val_part)
                        else:
                            val_to_render = val_part
                        sql_cond = f"[{col_name}] {op_str} {render_literal(val_to_render)}"
                else:
                    raise UnsupportedCapabilityError("Conditional update requires a valid condition.", engine="sql", action="conditional_update")

                template_params["sql_condition"] = sql_cond
                template_params["rendered_new_value"] = render_literal(step.parameters.get("new_value"))
                else_val = step.parameters.get("else_value")
                template_params["rendered_else_value"] = render_literal(else_val) if else_val is not None else ""

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

        return PhysicalStep(
            step_id=f"phys_{step.step_id}",
            logical_step_id=step.step_id,
            source_action_id=getattr(step, "source_action_id", None),
            source_issue_ids=list(getattr(step, "source_issue_ids", []) or []),
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
