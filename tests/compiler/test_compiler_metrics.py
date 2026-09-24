from __future__ import annotations

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


def test_compiler_metrics_collection():
    customer_cols = {
        "customer_id": ColumnProfile(name="customer_id", dtype="int", is_primary_key_candidate=True),
        "email": ColumnProfile(name="email", dtype="string"),
    }
    dataset = DatasetProfile(
        dataset_name="dim_customer",
        row_count=50_000,
        column_count=2,
        columns=customer_cols,
    )

    issues = [
        Issue(
            issue_id="iss_1",
            dataset_name="dim_customer",
            column_name="email",
            category=IssueCategory.UNTRIMMED_WHITESPACE,
            suggested_action="trim",
            message="Untrimmed whitespace",
        ),
        Issue(
            issue_id="iss_2",
            dataset_name="dim_customer",
            column_name="email",
            category=IssueCategory.FORMAT_ANOMALY,
            suggested_action="lowercase",
            message="Mixed casing",
        ),
    ]

    context = CompilerContext(
        session_id="sess_metrics_eval",
        target_engine=ExecutionEngine.PYTHON,
        dataset_profiles={"dim_customer": dataset},
        detected_issues=issues,
    )

    pipeline = CompilerPipeline()
    ctx, snapshot = pipeline.compile(context)

    # 1. Verify compiler_metrics in context metadata
    metrics = ctx.metadata.get("compiler_metrics")
    assert metrics is not None

    # Check Generation
    gen = metrics["generation"]
    assert "generation_time_ms" in gen
    assert "validation_time_ms" in gen
    assert "total_compilation_time_ms" in gen
    assert gen["total_compilation_time_ms"] >= 0

    # Check Planning
    plan = metrics["planning"]
    assert "operator_fusion_count" in plan
    # Untrimmed whitespace + lowercase should be fused
    assert plan["operator_fusion_count"] >= 1

    # Check Quality
    qual = metrics["quality"]
    assert qual["contracts_generated"] is True
    assert qual["lineage_coverage_pct"] == 100.0

    # Check manifest metadata includes compiler_metrics
    manifest = snapshot.metadata.get("deployment_manifest")
    assert manifest is not None
    assert "compiler_metrics" in manifest["metadata"]
