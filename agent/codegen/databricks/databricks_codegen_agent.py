from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from agent.compiler.base.codegen import BaseCodegenAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.physical import PhysicalPlan
from agent.compiler.templates.engine import TemplateEngine


class DatabricksCodegenAgent(BaseCodegenAgent):
    """Generates enterprise Databricks Delta Live Tables (DLT) scripts with Unity Catalog governance and workflow job specs."""

    def __init__(self, template_engine: Optional[TemplateEngine] = None):
        self.template_engine = template_engine or TemplateEngine()

    def generate(self, physical_plan: PhysicalPlan) -> CodeArtifact:
        clean_plan_id = re.sub(r"[^a-zA-Z0-9_]", "_", physical_plan.plan_id)
        now_iso = datetime.now(timezone.utc).isoformat()

        sections: List[str] = [
            '"""',
            f"Databricks Delta Live Tables (DLT) Pipeline: {clean_plan_id}",
            f"Generated: {now_iso}",
            "Target: Unity Catalog Managed Tables with Declarative Quality Expectations",
            '"""',
            "from __future__ import annotations",
            "",
            "import dlt",
            "from pyspark.sql import functions as F",
            "from pyspark.sql.types import *",
            "",
        ]

        # Ingestion & DLT Tables
        for ds in physical_plan.dataset_names:
            clean_ds = re.sub(r"[^a-zA-Z0-9_]", "_", ds)
            steps = physical_plan.get_steps_for_dataset(ds)

            # Bronze / Raw Table
            sections.extend([
                f"# ===========================================================================",
                f"# Bronze / Raw Ingestion Table: {clean_ds}_raw",
                f"# ===========================================================================",
                f"@dlt.table(",
                f"    name=\"{clean_ds}_raw\",",
                f"    comment=\"Raw ingested staging data for {clean_ds} from cloud storage\",",
                f"    table_properties={{",
                f"        \"quality\": \"bronze\",",
                f"        \"pipelines.autoOptimize.zOrderCols\": \"id\"",
                f"    }}",
                f")",
                f"def {clean_ds}_raw():",
                f"    # Ingest from cloud object storage via Auto Loader or managed table",
                f"    return (",
                f"        spark.readStream.format(\"cloudFiles\")",
                f"        .option(\"cloudFiles.format\", \"parquet\")",
                f"        .load(f\"/mnt/datalake/landing/{clean_ds}\")",
                f"    )",
                "",
            ])

            # Silver / Cleaned Table with DLT expectations
            expectations_code: List[str] = []
            # Extract target columns to generate real DLT expectations
            for s in steps:
                for col in s.target_columns:
                    expectations_code.append(
                        f"@dlt.expect_or_drop(\"valid_{col}\", \"{col} IS NOT NULL\")"
                    )

            if not expectations_code:
                expectations_code.append(
                    f"@dlt.expect(\"valid_record\", \"1 = 1\")"
                )

            rendered_steps: List[str] = []
            for step in steps:
                tmpl_name = step.template_name
                template_path = tmpl_name if tmpl_name.startswith("spark/") else f"spark/{tmpl_name}"
                params = dict(step.template_parameters)
                params["dataset_name"] = ds
                params["pipeline_id"] = physical_plan.plan_id
                try:
                    step_code = self.template_engine.render(template_path, **params)
                    rendered_steps.append(step_code)
                except Exception:
                    rendered_steps.append(f"# Step: {step.step_id} ({step.strategy_name})")

            indented_steps = []
            for step_code in rendered_steps:
                indented = "\n".join("    " + line if line.strip() else "" for line in step_code.splitlines())
                indented_steps.append(indented)

            silver_transform = "\n".join(indented_steps) if indented_steps else "    # Pass-through clean data"

            sections.extend([
                f"# ===========================================================================",
                f"# Silver / Cleaned Curated Table: {clean_ds}_curated (Unity Catalog)",
                f"# ===========================================================================",
                *expectations_code,
                f"@dlt.table(",
                f"    name=\"{clean_ds}_curated\",",
                f"    comment=\"Cleansed, validated, and normalized records for {clean_ds}\",",
                f"    table_properties={{",
                f"        \"quality\": \"silver\",",
                f"        \"delta.enableChangeDataFeed\": \"true\"",
                f"    }}",
                f")",
                f"def {clean_ds}_curated():",
                f"    df = dlt.read_stream(\"{clean_ds}_raw\")",
                f"    df_{clean_ds} = df",
                silver_transform,
                f"    return df_{clean_ds}",
                "",
            ])

        dlt_script = "\n".join(sections)

        # Companion Databricks Workflow Job definition
        workflow_job = {
            "name": f"dlt_job_{clean_plan_id}",
            "tasks": [
                {
                    "task_key": "run_dlt_pipeline",
                    "pipeline_task": {
                        "pipeline_id": f"dlt_{clean_plan_id}",
                        "full_refresh": False
                    },
                    "timeout_seconds": 3600
                }
            ],
            "schedule": {
                "quartz_cron_expression": "0 0 2 * * ?",
                "timezone_id": "UTC",
                "pause_status": "UNPAUSED"
            },
            "parameters": [
                {"name": "catalog", "default": "production_catalog"},
                {"name": "schema", "default": "data_engineering"}
            ]
        }

        artifact = CodeArtifact(
            artifact_id=f"art_{physical_plan.plan_id}_databricks",
            name=f"dlt_pipeline_{clean_plan_id}.py",
            language="python",
            content=dlt_script,
            file_extension=".py",
            is_entrypoint=True,
            metadata={
                "engine": "databricks",
                "dialect": "dlt",
                "unity_catalog": True,
                "workflow_job": workflow_job,
            }
        )
        return artifact

    def execute(self, context: CompilerContext) -> CompilerContext:
        if not context.physical_plan:
            raise ValueError("Cannot execute DatabricksCodegenAgent without a PhysicalPlan.")

        artifact = self.generate(context.physical_plan)
        context.code_artifact = artifact

        EventBus.get_instance().emit(
            event_type=CompilerEventType.CODE_GENERATED,
            session_id=context.session_id,
            payload={
                "artifact_id": artifact.artifact_id,
                "name": artifact.name,
                "engine": "databricks",
                "dialect": "dlt",
            },
            source_agent=self.__class__.__name__,
        )
        return context
