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


class FabricCodegenAgent(BaseCodegenAgent):
    """Generates Microsoft Fabric Lakehouse Notebooks (.ipynb) and Fabric Synapse Warehouse scripts."""

    def __init__(self, template_engine: Optional[TemplateEngine] = None):
        self.template_engine = template_engine or TemplateEngine()

    def generate(self, physical_plan: PhysicalPlan, context: Optional[CompilerContext] = None) -> CodeArtifact:
        clean_plan_id = re.sub(r"[^a-zA-Z0-9_]", "_", physical_plan.plan_id)
        now_iso = datetime.now(timezone.utc).isoformat()

        # Build Notebook Cells
        cells: List[Dict[str, Any]] = []

        # Markdown header cell
        cells.append({
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                f"# Microsoft Fabric Lakehouse ETL Notebook\n",
                f"**Pipeline ID:** `{clean_plan_id}`  \n",
                f"**Generated:** {now_iso}  \n",
                f"**Engine:** Fabric Spark Runtime (Delta Lake)  \n\n",
                "Automated end-to-end data cleansing, quality enforcement, and Delta table materialization.\n"
            ]
        })

        # Cell 1: Environment & Lakehouse Setup
        cells.append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Setup & Imports for Fabric Lakehouse\n",
                "from pyspark.sql import functions as F\n",
                "from pyspark.sql.types import *\n",
                "from delta.tables import DeltaTable\n\n",
                "# Fabric workspace & lakehouse session initialization\n",
                "spark.conf.set(\"spark.sql.parquet.vorder.enabled\", \"true\")\n",
                "spark.conf.set(\"spark.microsoft.delta.optimizeWrite.enabled\", \"true\")\n",
                "print(\"Fabric Lakehouse Spark session initialized successfully.\")\n"
            ]
        })

        # Transformations per dataset
        for ds_name in physical_plan.dataset_names:
            clean_ds = re.sub(r"[^a-zA-Z0-9_]", "_", ds_name)
            steps = physical_plan.get_steps_for_dataset(ds_name)

            # Ingestion Cell
            cells.append({
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    f"# Ingest Source Dataset: {ds_name}\n",
                    f"try:\n",
                    f"    df_{clean_ds} = spark.read.table(\"{clean_ds}\")\n",
                    f"except Exception:\n",
                    f"    # Fallback to Files/ directory if not registered as managed table\n",
                    f"    df_{clean_ds} = spark.read.format(\"parquet\").load(f\"Files/{clean_ds}\")\n\n",
                    f"print(f\"Loaded {clean_ds} with {{df_{clean_ds}.count()}} rows.\")\n"
                ]
            })

            # Transformation Code
            rendered_steps: List[str] = []
            for step in steps:
                tmpl_name = step.template_name
                template_path = tmpl_name if tmpl_name.startswith("spark/") else f"spark/{tmpl_name}"
                params = dict(step.template_parameters)
                params["dataset_name"] = ds_name
                params["pipeline_id"] = physical_plan.plan_id
                try:
                    step_code = self.template_engine.render(template_path, **params)
                    rendered_steps.append(step_code)
                except Exception:
                    rendered_steps.append(f"# Step: {step.step_id} ({step.strategy_name})")

            # Retrieve output contract for this dataset
            output_contracts = (context.metadata.get("output_contracts") if context else {}) or {}
            contract_data = output_contracts.get(ds_name) or {}
            clean_columns = contract_data.get("clean_columns") or []
            if clean_columns:
                rendered_steps.append(
                    f"# Strict Final Projection (Output Contract Conformance)\n"
                    f"clean_cols = [c for c in {repr(clean_columns)} if c in df_{clean_ds}.columns]\n"
                    f"df_{clean_ds} = df_{clean_ds}.select(*clean_cols)"
                )

            transform_code = "\n".join(rendered_steps) if rendered_steps else "# No active transformation steps"
            cells.append({
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    f"# Cleanse & Transform: {clean_ds}\n",
                    transform_code + "\n"
                ]
            })

            # Materialization / Sink Cell
            cells.append({
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    f"# Materialize clean Delta table to Fabric Lakehouse\n",
                    f"target_table = \"{clean_ds}_curated\"\n",
                    f"df_{clean_ds}.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\").saveAsTable(target_table)\n",
                    f"print(f\"Successfully materialized {{target_table}} to Fabric OneLake.\")\n"
                ]
            })

        # Assemble standard Jupyter Notebook JSON format
        notebook_data = {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {
                "language_info": {"name": "python"},
                "kernel_info": {"name": "synapse_pyspark"},
                "microsoft_fabric": {
                    "pipeline_id": clean_plan_id,
                    "target": "lakehouse",
                }
            },
            "cells": cells
        }

        notebook_json = json.dumps(notebook_data, indent=2)

        # Also generate Synapse Warehouse T-SQL Companion Script
        warehouse_script = f"""-- Microsoft Fabric Synapse Data Warehouse DDL/DML Script
-- Pipeline ID: {clean_plan_id}
-- Timestamp: {now_iso}

CREATE SCHEMA IF NOT EXISTS curated;
GO

"""
        for ds in physical_plan.dataset_names:
            clean_d = re.sub(r"[^a-zA-Z0-9_]", "_", ds)
            warehouse_script += f"""-- Fabric Warehouse CTAS View/Table for {clean_d}
IF OBJECT_ID('curated.{clean_d}_warehouse', 'U') IS NOT NULL
    DROP TABLE curated.{clean_d}_warehouse;
GO

CREATE TABLE curated.{clean_d}_warehouse
AS
SELECT * FROM [lakehouse].[dbo].[{clean_d}_curated];
GO
"""

        artifact = CodeArtifact(
            artifact_id=f"art_{physical_plan.plan_id}_fabric",
            name=f"fabric_notebook_{clean_plan_id}.ipynb",
            language="json",
            content=notebook_json,
            file_extension=".ipynb",
            is_entrypoint=True,
            metadata={
                "engine": "fabric",
                "format": "ipynb",
                "cell_count": len(cells),
                "warehouse_sql": warehouse_script,
            }
        )
        return artifact

    def execute(self, context: CompilerContext) -> CompilerContext:
        if not context.physical_plan:
            raise ValueError("Cannot execute FabricCodegenAgent without a PhysicalPlan.")

        artifact = self.generate(context.physical_plan)
        context.code_artifact = artifact

        EventBus.get_instance().emit(
            event_type=CompilerEventType.CODE_GENERATED,
            session_id=context.session_id,
            payload={
                "artifact_id": artifact.artifact_id,
                "name": artifact.name,
                "engine": "fabric",
                "cells": artifact.metadata.get("cell_count", 0),
            },
            source_agent=self.__class__.__name__,
        )
        return context
