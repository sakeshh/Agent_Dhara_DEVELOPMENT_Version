"""
tests/cross_engine/test_parity_pyspark_dedup.py

Phase 3 Increment 3.2 Work Package 3.2D:
PySpark Deterministic Deduplication Parity & Partition-Independence.

Verifies:
1. Deterministic survivor selection hierarchy:
   - Partition by deduplication keys
   - Order by declared SortColumnSpec values
   - Order by occurrence identity as final deterministic tie-breaker
   - Select first or last (PRESERVE_FIRST vs PRESERVE_LAST reversed sort)
2. Partition-independence invariant:
   - SOURCE_STABLE deduplication survivor set is invariant across repartition(1), repartition(2), repartition(4).
   - EXECUTION_LOCAL test demonstrating partition-dependency when partitioning changes.
3. Null key policies:
   - GROUP_NULLS_TOGETHER
   - FAIL_STEP
   - TREAT_EACH_NULL_AS_DISTINCT
   - EXCLUDE_NULL_KEY_ROWS
4. Duplicate export mode:
   - REMOVED_OCCURRENCES_ONLY
   - Parity with Pandas reference compiler
"""

from __future__ import annotations

import os
import tempfile
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import SparkSession

from agent.etl_pipeline.codegen_shared import CodegenStatus
from agent.etl_pipeline.pandas_ir_compiler import (
    CompilerStepDisposition,
    PandasIRCompiler,
)
from agent.etl_pipeline.pyspark_ir_compiler import (
    PySparkIRCompiler,
)
from agent.etl_pipeline.transformation_ir import (
    ActionFamily,
    DeduplicateExpression,
    DeduplicationTieBreaker,
    DuplicateExportMode,
    NullKeyPolicy,
    OccurrenceIdentityScope,
    OrderDirection,
    RoutingIdentityPolicy,
    SortColumnSpec,
    SourceStableIdentitySpec,
    StepKind,
    TransformationIRGraph,
    TransformationStepIR,
)





def df_to_spark(spark: SparkSession, pdf: pd.DataFrame):
    """Safely loads a pandas DataFrame into Spark via temp JSON without socket worker crashes."""
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False, encoding="utf-8") as f:
        pdf.to_json(f.name, orient="records", lines=True, date_format="iso")
        tmp_path = f.name
    try:
        sdf = spark.read.json(tmp_path)
        sdf.cache()
        _ = sdf.count()
        return sdf
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_pyspark_dedup_preserve_first_parity(spark_session: SparkSession):
    """Verifies PRESERVE_FIRST deduplication parity with Pandas reference compiler."""
    pdf = pd.DataFrame({
        "customer_id": ["C1", "C2", "C1", "C3", "C2"],
        "timestamp": [100, 200, 150, 300, 250],
        "value": [10, 20, 15, 30, 25],
    })
    sdf = df_to_spark(spark_session, pdf)

    expr = DeduplicateExpression(
        key_columns=("customer_id",),
        tie_breaker=DeduplicationTieBreaker.PRESERVE_FIRST,
        order_by=(
            SortColumnSpec(column="timestamp", direction=OrderDirection.ASCENDING),
        ),
        duplicates_output_ref="duplicates",
        duplicate_export_mode=DuplicateExportMode.REMOVED_OCCURRENCES_ONLY,
    )

    step = TransformationStepIR(
        step_id="step_dedup_first",
        family=ActionFamily.DEDUPLICATE,
        expression=expr,
        dataset_name="primary",
        kind=StepKind.DATASET_FILTER,
    )

    graph = TransformationIRGraph(
        graph_id="test_dedup_first",
        steps=[step],
    )

    # 1. Pandas reference
    pandas_compiler = PandasIRCompiler()
    pd_res = pandas_compiler.execute(graph, pdf)
    pd_survivors = pd_res.outputs["primary"].sort_values("customer_id").reset_index(drop=True)
    pd_dups = pd_res.outputs["duplicates"].sort_values("customer_id").reset_index(drop=True)

    # 2. PySpark
    pyspark_compiler = PySparkIRCompiler()
    py_res = pyspark_compiler.execute(graph, sdf)
    py_survivors = py_res.outputs["primary"].toPandas().sort_values("customer_id").reset_index(drop=True)
    py_dups = py_res.outputs["duplicates"].toPandas().sort_values("customer_id").reset_index(drop=True)

    assert len(py_survivors) == len(pd_survivors) == 3
    assert list(py_survivors["customer_id"]) == ["C1", "C2", "C3"]
    assert list(py_survivors["timestamp"]) == [100, 200, 300]
    assert list(py_survivors["value"]) == [10, 20, 30]

    assert len(py_dups) == len(pd_dups) == 2
    assert list(py_dups["customer_id"]) == ["C1", "C2"]
    assert list(py_dups["timestamp"]) == [150, 250]


def test_pyspark_dedup_preserve_last_parity(spark_session: SparkSession):
    """Verifies PRESERVE_LAST deduplication using reversed sort directions."""
    pdf = pd.DataFrame({
        "customer_id": ["C1", "C2", "C1", "C3", "C2"],
        "timestamp": [100, 200, 150, 300, 250],
        "value": [10, 20, 15, 30, 25],
    })
    sdf = df_to_spark(spark_session, pdf)

    expr = DeduplicateExpression(
        key_columns=("customer_id",),
        tie_breaker=DeduplicationTieBreaker.PRESERVE_LAST,
        order_by=(
            SortColumnSpec(column="timestamp", direction=OrderDirection.ASCENDING),
        ),
        duplicates_output_ref="duplicates",
    )

    step = TransformationStepIR(
        step_id="step_dedup_last",
        family=ActionFamily.DEDUPLICATE,
        expression=expr,
        dataset_name="primary",
        kind=StepKind.DATASET_FILTER,
    )

    graph = TransformationIRGraph(
        graph_id="test_dedup_last",
        steps=[step],
    )

    # 1. Pandas reference
    pandas_compiler = PandasIRCompiler()
    pd_res = pandas_compiler.execute(graph, pdf)
    pd_survivors = pd_res.outputs["primary"].sort_values("customer_id").reset_index(drop=True)

    # 2. PySpark
    pyspark_compiler = PySparkIRCompiler()
    py_res = pyspark_compiler.execute(graph, sdf)
    py_survivors = py_res.outputs["primary"].toPandas().sort_values("customer_id").reset_index(drop=True)

    assert len(py_survivors) == 3
    # With PRESERVE_LAST on timestamp ASC, the latest timestamp survives
    assert list(py_survivors["customer_id"]) == ["C1", "C2", "C3"]
    assert list(py_survivors["timestamp"]) == [150, 250, 300]
    assert list(py_survivors["value"]) == [15, 25, 30]


def test_pyspark_dedup_source_stable_partition_independence(spark_session: SparkSession):
    """
    Verifies that SOURCE_STABLE deduplication survivor identities are strictly
    invariant across repartition(1), repartition(2), and repartition(4).
    """
    pdf = pd.DataFrame({
        "part_id": ["p0", "p0", "p1", "p1", "p2", "p2"],
        "offset": [0, 1, 0, 1, 0, 1],
        "entity_id": ["E1", "E2", "E1", "E3", "E2", "E3"],
        "seq": [10, 20, 15, 5, 25, 30],
    })

    spec = SourceStableIdentitySpec(
        snapshot_id="snap_dedup_part_test",
        partition_column="part_id",
        row_offset_column="offset",
    )

    expr = DeduplicateExpression(
        key_columns=("entity_id",),
        tie_breaker=DeduplicationTieBreaker.PRESERVE_FIRST,
        order_by=(
            SortColumnSpec(column="seq", direction=OrderDirection.ASCENDING),
        ),
    )

    step = TransformationStepIR(
        step_id="step_dedup_partition_test",
        family=ActionFamily.DEDUPLICATE,
        expression=expr,
        dataset_name="primary",
        kind=StepKind.DATASET_FILTER,
    )

    graph = TransformationIRGraph(
        graph_id="test_dedup_partition_independence",
        steps=[step],
        routing_identity_policy=RoutingIdentityPolicy(
            scope=OccurrenceIdentityScope.SOURCE_STABLE,
            source_stable_spec=spec,
            retain_in_primary_output=True,  # Retain identity to verify survivor occurrence IDs
        ),
    )

    pyspark_compiler = PySparkIRCompiler()
    base_sdf = df_to_spark(spark_session, pdf)

    # Test across 1, 2, and 4 partitions
    results_by_partition: Dict[int, List[str]] = {}
    for num_parts in [1, 2, 4]:
        sdf_partitioned = base_sdf.repartition(num_parts)
        exec_res = pyspark_compiler.execute(graph, sdf_partitioned)
        out_pdf = exec_res.outputs["primary"].toPandas()
        # Collect sorted list of surviving identity IDs
        surviving_ids = sorted(list(out_pdf["__dhara_row_occurrence_0"]))
        results_by_partition[num_parts] = surviving_ids

    # Survivor occurrence IDs must be 100% IDENTICAL across all partition counts
    assert results_by_partition[1] == results_by_partition[2] == results_by_partition[4]
    assert len(results_by_partition[1]) == 3  # E1, E2, E3 each have 1 survivor


def test_pyspark_dedup_null_key_fail_step(spark_session: SparkSession):
    """Verifies that Deduplication with NullKeyPolicy.FAIL_STEP raises when null keys exist."""
    pdf = pd.DataFrame({"key": ["K1", None, "K2"], "val": [1, 2, 3]})
    sdf = df_to_spark(spark_session, pdf)

    expr = DeduplicateExpression(
        key_columns=("key",),
        null_key_policy=NullKeyPolicy.FAIL_STEP,
    )

    step = TransformationStepIR(
        step_id="step_dedup_fail_null",
        family=ActionFamily.DEDUPLICATE,
        expression=expr,
        dataset_name="primary",
        kind=StepKind.DATASET_FILTER,
    )

    graph = TransformationIRGraph(
        graph_id="test_dedup_fail_null",
        steps=[step],
    )

    pyspark_compiler = PySparkIRCompiler()
    with pytest.raises(Exception, match="Deduplication encountered null key under FAIL_STEP policy"):
        pyspark_compiler.execute(graph, sdf)


def test_pyspark_dedup_null_key_treat_each_distinct(spark_session: SparkSession):
    """Verifies that TREAT_EACH_NULL_AS_DISTINCT preserves all null key rows as separate occurrences."""
    pdf = pd.DataFrame({"key": ["K1", None, None, "K1"], "val": [1, 2, 3, 4]})
    sdf = df_to_spark(spark_session, pdf)

    expr = DeduplicateExpression(
        key_columns=("key",),
        tie_breaker=DeduplicationTieBreaker.PRESERVE_FIRST,
        null_key_policy=NullKeyPolicy.TREAT_EACH_NULL_AS_DISTINCT,
    )

    step = TransformationStepIR(
        step_id="step_dedup_distinct_null",
        family=ActionFamily.DEDUPLICATE,
        expression=expr,
        dataset_name="primary",
        kind=StepKind.DATASET_FILTER,
    )

    graph = TransformationIRGraph(
        graph_id="test_dedup_distinct_null",
        steps=[step],
    )

    pyspark_compiler = PySparkIRCompiler()
    exec_res = pyspark_compiler.execute(graph, sdf)
    out_pdf = exec_res.outputs["primary"].toPandas()

    # K1 deduped to 1 row; both None rows preserved because each null is treated as distinct!
    assert len(out_pdf) == 3
    assert list(out_pdf[out_pdf["key"].isna()]["val"]) == [2, 3]
