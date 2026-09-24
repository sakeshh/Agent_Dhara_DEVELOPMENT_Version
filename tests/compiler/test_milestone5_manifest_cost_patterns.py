from __future__ import annotations

import pytest
from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.manifests import ArtifactManifestBuilder, DeploymentPackage
from agent.compiler.models import (
    ColumnProfile,
    CompilerContext,
    DatasetProfile,
    ExecutionEngine,
    Issue,
    IssueCategory,
)
from agent.compiler.patterns import PatternLibrary
from agent.compiler.planners.cost_estimator import CostEstimator


def _build_test_context() -> CompilerContext:
    customer_cols = {
        "customer_id": ColumnProfile(name="customer_id", dtype="int", is_primary_key_candidate=True),
        "email": ColumnProfile(name="email", dtype="string"),
        "phone": ColumnProfile(name="phone", dtype="string"),
        "first_name": ColumnProfile(name="first_name", dtype="string"),
        "last_name": ColumnProfile(name="last_name", dtype="string"),
    }
    dataset = DatasetProfile(
        dataset_name="customers",
        row_count=500_000,
        column_count=5,
        columns=customer_cols,
    )
    return CompilerContext(
        session_id="sess_m5",
        target_engine=ExecutionEngine.PYSPARK,
        dataset_profiles={"customers": dataset},
        detected_issues=[
            Issue(
                issue_id="iss_1",
                dataset_name="customers",
                column_name="email",
                category=IssueCategory.UNTRIMMED_WHITESPACE,
                suggested_action="trim",
                message="Untrimmed whitespace",
            ),
            Issue(
                issue_id="iss_2",
                dataset_name="customers",
                column_name="customer_id",
                category=IssueCategory.DUPLICATE,
                suggested_action="deduplicate",
                message="Duplicates found",
            ),
        ],
    )


def test_pattern_library_matches_customer_master():
    pattern_lib = PatternLibrary()
    cols = {"customer_id", "email", "phone", "first_name", "last_name"}
    match = pattern_lib.match_pattern(cols)

    assert match is not None
    pattern, confidence = match
    assert pattern.pattern_name == "CustomerMaster"
    assert pattern.domain == "CRM"
    assert confidence == 1.0


def test_pattern_library_matches_orders():
    pattern_lib = PatternLibrary()
    cols = {"order_id", "customer_id", "order_date", "total_amount"}
    match = pattern_lib.match_pattern(cols)

    assert match is not None
    pattern, confidence = match
    assert pattern.pattern_name == "OrderProcessing"
    assert pattern.domain == "E-Commerce / ERP"
    assert confidence >= 0.80


def test_cost_estimator_spark():
    estimator = CostEstimator()
    context = _build_test_context()
    estimate = estimator.estimate(context)

    assert estimate.estimated_runtime_seconds > 0
    assert estimate.estimated_memory_mb >= 256.0
    assert estimate.estimated_cloud_cost_usd > 0
    assert "500,000 rows" in estimate.notes


def test_artifact_manifest_builder():
    pipeline = CompilerPipeline()
    context = _build_test_context()
    context, snapshot = pipeline.compile(context)

    # Verify deployment manifest is present in context metadata
    assert "deployment_manifest" in context.metadata
    manifest_data = context.metadata["deployment_manifest"]
    manifest = DeploymentPackage(**manifest_data)

    # 1. Structure
    assert manifest.manifest_version == "2.0.0"
    assert manifest.target_engine == "pyspark"

    # 2. Bundled artifacts
    assert "code" in manifest.artifacts
    assert manifest.artifacts["code"]["language"] == "pyspark"
    assert "tests" in manifest.artifacts
    assert "contract" in manifest.artifacts
    assert "lineage" in manifest.artifacts

    # 3. Statuses
    assert manifest.validation["status"] == "passed"
    assert manifest.cost_estimate is not None
    assert manifest.cost_estimate.estimated_runtime_seconds > 0

    # 4. JSON Serialization
    json_output = manifest.to_json()
    assert '"manifest_version": "2.0.0"' in json_output
    assert '"target_engine": "pyspark"' in json_output
