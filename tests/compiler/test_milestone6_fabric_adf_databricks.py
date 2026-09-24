from __future__ import annotations

import ast
import json
import pytest

from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.models import (
    ColumnProfile,
    CompilerContext,
    DatasetProfile,
    ExecutionEngine,
    Issue,
    IssueCategory,
)
from agent.compiler.validators.fabric_validator import FabricValidator
from agent.compiler.validators.adf_validator import ADFValidator
from agent.compiler.validators.databricks_validator import DatabricksValidator


def _build_context(engine: ExecutionEngine, session_id: str) -> CompilerContext:
    customer_cols = {
        "customer_id": ColumnProfile(name="customer_id", dtype="int", is_primary_key_candidate=True),
        "email": ColumnProfile(name="email", dtype="string"),
        "phone": ColumnProfile(name="phone", dtype="string"),
        "customer_code": ColumnProfile(name="customer_code", dtype="string"),
    }
    dataset = DatasetProfile(
        dataset_name="dim_customer",
        row_count=100_000,
        column_count=4,
        columns=customer_cols,
    )

    issues = [
        Issue(
            issue_id="iss_1",
            dataset_name="dim_customer",
            column_name="email",
            category=IssueCategory.MISSING_VALUE,
            suggested_action="fillna",
            message="Null email values detected",
            parameters={"fill_value": "unknown@domain.com"},
        ),
        Issue(
            issue_id="iss_2",
            dataset_name="dim_customer",
            column_name="customer_code",
            category=IssueCategory.FORMAT_ANOMALY,
            suggested_action="lowercase",
            message="Mixed casing in customer_code",
        ),
        Issue(
            issue_id="iss_3",
            dataset_name="dim_customer",
            column_name="customer_id",
            category=IssueCategory.DUPLICATE,
            suggested_action="deduplicate",
            message="Duplicate customer IDs found",
        ),
    ]

    return CompilerContext(
        session_id=session_id,
        target_engine=engine,
        dataset_profiles={"dim_customer": dataset},
        detected_issues=issues,
    )


def test_fabric_codegen_and_validation():
    pipeline = CompilerPipeline()
    context = _build_context(ExecutionEngine.FABRIC, "sess_fabric_test")

    ctx, snapshot = pipeline.compile(context)

    # 1. Verify code artifact
    artifact = ctx.code_artifact
    assert artifact is not None
    assert artifact.file_extension == ".ipynb"
    assert artifact.language == "json"
    assert artifact.metadata["engine"] == "fabric"

    # 2. Parse Notebook JSON
    nb = json.loads(artifact.content)
    assert nb["nbformat"] == 4
    assert len(nb["cells"]) >= 4

    # 3. Check Python syntax in code cells
    for cell in nb["cells"]:
        if cell["cell_type"] == "code":
            source = "".join(cell["source"])
            ast.parse(source)

    # 4. Verify Fabric Validator passed
    validator = FabricValidator()
    val_res = validator.validate(artifact)
    assert val_res.is_valid
    assert val_res.error_count == 0

    # 5. Verify deployment package manifest includes Fabric notebook
    manifest = snapshot.metadata.get("deployment_manifest")
    assert manifest is not None
    assert manifest["artifacts"]["code"]["name"].startswith("fabric_notebook_")
    assert "warehouse_sql" in artifact.metadata


def test_adf_codegen_and_validation():
    pipeline = CompilerPipeline()
    context = _build_context(ExecutionEngine.ADF, "sess_adf_test")

    ctx, snapshot = pipeline.compile(context)

    # 1. Verify code artifact
    artifact = ctx.code_artifact
    assert artifact is not None
    assert artifact.file_extension == ".json"
    assert artifact.language == "json"
    assert artifact.metadata["engine"] == "adf"

    # 2. Parse ADF JSON bundle
    bundle = json.loads(artifact.content)
    assert "pipeline" in bundle
    assert "linked_services" in bundle
    assert "datasets" in bundle
    assert "triggers" in bundle

    pipeline_obj = bundle["pipeline"]
    activities = pipeline_obj["properties"]["activities"]
    assert len(activities) >= 3

    # Check activities have correct types
    act_types = [a["type"] for a in activities]
    assert "Validation" in act_types
    assert "Copy" in act_types
    assert "SqlServerStoredProcedure" in act_types

    # 3. Verify ADF Validator passed
    validator = ADFValidator()
    val_res = validator.validate(artifact)
    assert val_res.is_valid
    assert val_res.error_count == 0

    # 4. Verify deployment package
    manifest = snapshot.metadata.get("deployment_manifest")
    assert manifest is not None
    assert manifest["artifacts"]["code"]["name"].startswith("adf_pipeline_")


def test_databricks_codegen_and_validation():
    pipeline = CompilerPipeline()
    context = _build_context(ExecutionEngine.DATABRICKS, "sess_databricks_test")

    ctx, snapshot = pipeline.compile(context)

    # 1. Verify code artifact
    artifact = ctx.code_artifact
    assert artifact is not None
    assert artifact.file_extension == ".py"
    assert artifact.language == "python"
    assert artifact.metadata["engine"] == "databricks"
    assert artifact.metadata["dialect"] == "dlt"

    # 2. Parse Python AST
    tree = ast.parse(artifact.content)
    assert tree is not None
    assert "@dlt.table" in artifact.content
    assert "@dlt.expect_or_drop" in artifact.content
    assert "workflow_job" in artifact.metadata

    # 3. Verify Databricks Validator passed
    validator = DatabricksValidator()
    val_res = validator.validate(artifact)
    assert val_res.is_valid
    assert val_res.error_count == 0

    # 4. Verify deployment package
    manifest = snapshot.metadata.get("deployment_manifest")
    assert manifest is not None
    assert manifest["artifacts"]["code"]["name"].startswith("dlt_pipeline_")


def test_all_six_engines_compile_successfully():
    """Verify that Python, SQL, PySpark, Fabric, ADF, and Databricks all compile end-to-end cleanly."""
    engines = [
        ExecutionEngine.PYTHON,
        ExecutionEngine.AZURE_SQL,
        ExecutionEngine.PYSPARK,
        ExecutionEngine.FABRIC,
        ExecutionEngine.ADF,
        ExecutionEngine.DATABRICKS,
    ]

    pipeline = CompilerPipeline()

    for eng in engines:
        ctx = _build_context(eng, f"sess_multi_{eng.value}")
        out_ctx, snapshot = pipeline.compile(ctx)

        assert out_ctx.code_artifact is not None
        assert snapshot is not None
        assert snapshot.target_engine == eng
        manifest = snapshot.metadata.get("deployment_manifest")
        assert manifest is not None
        assert manifest["pipeline_id"] == snapshot.pipeline_id
