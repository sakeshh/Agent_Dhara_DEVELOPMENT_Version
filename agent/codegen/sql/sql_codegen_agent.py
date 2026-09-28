from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Dict, List

from agent.compiler.base.codegen import BaseCodegenAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.physical import PhysicalPlan
from agent.compiler.schema.sql_identifier import parse_table_identifier
from agent.compiler.templates.engine import TemplateEngine


class SQLCodegenAgent(BaseCodegenAgent):
    """Generates production-grade T-SQL stored procedures and CTE transformations using Jinja2 templates."""

    def __init__(self, template_engine: TemplateEngine | None = None):
        self.template_engine = template_engine or TemplateEngine.get_instance()

    def generate(self, physical_plan: PhysicalPlan, context: CompilerContext) -> CodeArtifact:
        dataset_transforms: Dict[str, str] = {}
        never_drop = context.options.get("never_drop_rows", False)

        for ds_name in physical_plan.dataset_names:
            table_ident = parse_table_identifier(ds_name)
            steps = physical_plan.get_steps_for_dataset(ds_name)
            rendered_steps: List[str] = []

            # Add staging table preparation header
            staging_table = table_ident.staging_name
            source_table = table_ident.qualified_name
            prep_ddl = (
                f"-- Ensure Staging table exists\n"
                f"IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL DROP TABLE {staging_table};\n"
                f"SELECT * INTO {staging_table} FROM {source_table};"
            )
            rendered_steps.append(prep_ddl)

            for step in steps:
                template_path = f"sql/{step.template_name}"
                params = dict(step.template_parameters)
                params["step_id"] = step.step_id
                params["description"] = step.description or step.strategy_name
                params["columns"] = step.target_columns
                params["dataset_name"] = table_ident.table
                params["schema_name"] = table_ident.schema
                params["staging_table"] = table_ident.staging_name
                params["source_table"] = table_ident.qualified_name
                params["clean_table"] = table_ident.clean_name
                if "parent_dataset" in params and params["parent_dataset"]:
                    try:
                        p_ident = parse_table_identifier(params["parent_dataset"])
                        params["parent_dataset"] = p_ident.table
                        params["parent_qualified_name"] = p_ident.qualified_name
                    except Exception:
                        pass
                params["never_drop_rows"] = never_drop
                params["pipeline_id"] = physical_plan.plan_id

                try:
                    step_code = self.template_engine.render(template_path, **params)
                    rendered_steps.append(step_code)
                except Exception:
                    rendered_steps.append(f"-- Step {step.step_id}: {step.strategy_name}")
            # Retrieve output contract for this dataset
            output_contracts = context.metadata.get("output_contracts") or {}
            contract_data = output_contracts.get(ds_name) or {}
            clean_columns = contract_data.get("clean_columns") or []

            clean_table = table_ident.clean_name
            if clean_columns:
                cols_quoted = ",\n    ".join(f"[{c}]" for c in clean_columns)
                projection_sql = (
                    f"-- Strict Final Clean Projection (Output Contract Conformance)\n"
                    f"IF OBJECT_ID('{clean_table}', 'U') IS NOT NULL DROP TABLE {clean_table};\n"
                    f"SELECT\n    {cols_quoted}\nINTO {clean_table}\nFROM {staging_table};"
                )
                rendered_steps.append(projection_sql)

            dataset_transforms[ds_name] = "\n\n".join(rendered_steps) if rendered_steps else "-- No transformations"

        # Sanitize proc id
        clean_plan_id = re.sub(r"[^a-zA-Z0-9_]", "_", physical_plan.plan_id)

        sql_code = self.template_engine.render(
            "sql/stored_procedure.j2",
            pipeline_id=clean_plan_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            dataset_transforms=dataset_transforms,
        )

        artifact = CodeArtifact(
            artifact_id=f"art_{physical_plan.plan_id}_sql",
            name=f"sp_etl_{clean_plan_id}.sql",
            language="sql",
            content=sql_code,
            file_extension=".sql",
            is_entrypoint=True,
            metadata={
                "plan_id": physical_plan.plan_id,
                "step_count": len(physical_plan.steps),
                "engine": "sql",
            },
        )

        EventBus.get_instance().emit(
            event_type=CompilerEventType.CODE_GENERATED,
            session_id=context.session_id,
            payload={
                "artifact_id": artifact.artifact_id,
                "language": artifact.language,
                "bytes": len(sql_code),
            },
            source_agent=self.__class__.__name__,
        )

        return artifact
