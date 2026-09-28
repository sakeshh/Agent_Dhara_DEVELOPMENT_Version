from __future__ import annotations

import logging
from typing import Dict, List, Optional
from agent.compiler.models.cleaning import CleaningMode
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.metrics import CompilerMetrics
from agent.compiler.models.logical import ActionType, DataEffect, LogicalPlan, LogicalStep
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.models.rules import BusinessRule, RuleScope
from agent.ir.models import IrPlan, IrStep
from agent.ir.rule_expression import RuleExpression, RuleType

logger = logging.getLogger("agent.ir.builder")


class IrBuilder:
    """
    Transforms an IrPlan into a CompilerContext.
    This is the ONLY boundary module aware of both the IR model and the Compiler model.
    """

    @staticmethod
    def build(
        ir_plan: IrPlan,
        session_id: str = "default",
        target_engine: ExecutionEngine = ExecutionEngine.AZURE_SQL,
        existing_profiles: Optional[Dict[str, DatasetProfile]] = None,
        cleaning_mode: CleaningMode = CleaningMode.CLEANSE_WITH_AUDIT,
    ) -> CompilerContext:
        profiles: Dict[str, DatasetProfile] = dict(existing_profiles or {})

        # Ensure all sources in IR have corresponding profiles
        for src in ir_plan.sources:
            if src.table not in profiles:
                cols = {c: ColumnProfile(name=c, dtype="string") for c in src.columns}
                profiles[src.table] = DatasetProfile(
                    dataset_id=src.table,
                    dataset_name=src.table,
                    schema_name=src.schema_name or "dbo",
                    columns=cols,
                )

        primary_dataset = (
            ir_plan.sources[0].table
            if ir_plan.sources
            else (list(profiles.keys())[0] if profiles else "default_dataset")
        )

        # 1. Translate Business Rules from RuleExpression DSL
        compiled_rules: List[BusinessRule] = []
        for idx, rule in enumerate(ir_plan.business_rules):
            ds_name = rule.target_table or primary_dataset
            sql_cond = rule.to_sql_condition()
            spark_cond = rule.to_pyspark_condition()
            gx_spec = rule.to_gx_expectation()

            b_rule = BusinessRule(
                rule_id=f"ir_rule_{idx + 1}",
                name=f"{rule.rule_type.value}_{rule.column or 'dataset'}",
                scope=RuleScope.COLUMN if rule.column else RuleScope.DATASET,
                dataset_name=ds_name,
                column_name=rule.column,
                parameters={
                    "rule_type": rule.rule_type.value,
                    "sql_condition": sql_cond,
                    "pyspark_condition": spark_cond,
                    "gx_expectation": gx_spec,
                    "rule_expression": rule.model_dump(exclude_none=True),
                },
                description=rule.description or f"IR business rule: {rule.rule_type.value}",
            )
            compiled_rules.append(b_rule)

        # 2. Translate IrSteps to LogicalPlan Steps
        logical_steps: List[LogicalStep] = []
        dataset_names = list(profiles.keys())

        for idx, step in enumerate(ir_plan.logical_plan):
            step_id = step.step_id or f"ir_step_{idx + 1}"
            ds_name = primary_dataset

            if step.step_type == "filter" and step.filter_node:
                fn = step.filter_node
                ds_name = fn.table or primary_dataset
                col = fn.column or ""
                rule_cond = fn.rule.to_sql_condition() if fn.rule else f"{col} {fn.operator} {fn.value}"
                logical_steps.append(
                    LogicalStep(
                        step_id=step_id,
                        dataset_name=ds_name,
                        action=ActionType.CUSTOM_EXPRESSION,
                        target_columns=[col] if col else [],
                        parameters={"expression": rule_cond, "filter_operator": fn.operator, "filter_value": fn.value},
                        effect=DataEffect.FILTER_ROWS,
                        priority=100 + idx,
                        description=step.description or f"Filter {ds_name} on {col}",
                    )
                )
            elif step.step_type == "deduplicate":
                target_cols = step.parameters.get("subset", [])
                logical_steps.append(
                    LogicalStep(
                        step_id=step_id,
                        dataset_name=ds_name,
                        action=ActionType.DEDUPLICATE,
                        target_columns=target_cols,
                        parameters=step.parameters,
                        effect=DataEffect.FILTER_ROWS,
                        priority=50,
                        description=step.description or f"Deduplicate records in {ds_name}",
                    )
                )
            elif step.step_type == "projection" and step.projection_node:
                pn = step.projection_node
                ds_name = pn.table or primary_dataset
                logical_steps.append(
                    LogicalStep(
                        step_id=step_id,
                        dataset_name=ds_name,
                        action=ActionType.CUSTOM_EXPRESSION,
                        target_columns=pn.columns,
                        parameters={"aliases": pn.aliases, "computed": pn.computed},
                        effect=DataEffect.TRANSFORM_IN_PLACE,
                        priority=200 + idx,
                        description=step.description or f"Project columns for {ds_name}",
                    )
                )
            else:
                # Generic custom expression / transformation
                logical_steps.append(
                    LogicalStep(
                        step_id=step_id,
                        dataset_name=ds_name,
                        action=ActionType.CUSTOM_EXPRESSION,
                        target_columns=step.parameters.get("columns", []),
                        parameters=step.parameters,
                        effect=DataEffect.TRANSFORM_IN_PLACE,
                        priority=100 + idx,
                        description=step.description or f"Execute {step.step_type}",
                    )
                )

        logical_plan = LogicalPlan(
            plan_id=ir_plan.metadata.plan_id or f"plan_{session_id}",
            dataset_names=dataset_names,
            steps=logical_steps,
            invariants={},
            metadata={"planner": ir_plan.metadata.planner_model},
        )

        ir_metrics = ir_plan.metadata.repair_metrics
        compiler_metrics = CompilerMetrics(
            planner_latency_ms=0.0,
            repair_latency_ms=ir_metrics.repair_duration_ms if ir_metrics else 0.0,
            target_language=target_engine.value,
            repairs_count=len(ir_plan.metadata.repair_history),
            route_taken=ir_plan.metadata.tags[0] if ir_plan.metadata.tags else "llm_ir_planner",
        )

        # 3. Assemble CompilerContext
        ctx = CompilerContext(
            session_id=session_id,
            target_engine=target_engine,
            cleaning_mode=cleaning_mode,
            preserve_source_schema=True,
            include_operational_metadata=False,
            dataset_profiles=profiles,
            business_rules=compiled_rules,
            detected_issues=[],
            logical_plan=logical_plan,
            metrics=compiler_metrics,
            options={
                "physical_hints": ir_plan.physical_hints.model_dump(),
                "validation": ir_plan.validation.model_dump(),
            },
            metadata={
                "ir_version": ir_plan.ir_version,
                "ir_intent": ir_plan.intent.model_dump(),
                "ir_lineage": ir_plan.lineage.model_dump(),
                "ir_metadata": ir_plan.metadata.model_dump(),
            },
        )

        return ctx
