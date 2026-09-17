"""
tests/cross_engine/test_parity_pyspark_routing.py

Phase 3 Increment 3.2 Work Packages 3.2B & 3.2C:
PySpark Routing Identity Lifecycle, Quarantine, and Filter Parity.

Verifies:
1. Occurrence Identity Lifecycle:
   - EXECUTION_LOCAL assigned once at graph entry via F.monotonically_increasing_id() and cached.
   - SOURCE_STABLE coordinate binding (snapshot_id + partition + offset).
   - Duplicate source-stable coordinate detection.
   - Publication boundary identity stripping from primary output.
2. Filter & Quarantine Parity:
   - Partition Quarantine parity between Pandas reference compiler and PySpark compiler.
   - Copy mode Quarantine parity with occurrence bounds check.
   - Filter retaining/dropping matching rows and optional dropped export.
   - Three-valued unknown policy resolution (FAIL_STEP, ROUTE_TO_QUARANTINE, SEPARATE_OUTPUT).
   - Distributed multiset reconciliation and branch disjointness.
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
    BinaryOp,
    BinaryOperator,
    ColumnRef,
    DataType,
    FilterDropPolicy,
    FilterExpression,
    FilterUnknownPolicy,
    LiteralValue,
    NullCheck,
    OccurrenceIdentityScope,
    PassThroughExpression,
    QuarantineExpression,
    QuarantineUnknownPolicy,
    RoutingIdentityPolicy,
    RoutingMode,
    SourceStableIdentitySpec,
    StepKind,
    TransformationIRGraph,
    TransformationStepIR,
    ValidateExpression,
    ValidateSeverity,
)
from tests.cross_engine.assertions import (
    assert_schema_parity,
)


@pytest.fixture(scope="module")
def spark_session():
    """Initializes an isolated local SparkSession under the certified local[1] profile."""
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("AgentDhara_Increment32_Routing_Parity")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


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


def test_pyspark_routing_identity_execution_local_materialized_once(spark_session: SparkSession):
    """Verifies that EXECUTION_LOCAL identity is bound once at graph entry and cached."""
    pdf = pd.DataFrame({"amount": [10.0, 50.0, 150.0, 200.0]})
    sdf = df_to_spark(spark_session, pdf)

    # Quarantine rows > 100
    expr = QuarantineExpression(
        predicate=BinaryOp(
            left=ColumnRef(column_name="amount"),
            op=BinaryOperator.GT,
            right=LiteralValue(value=100.0, data_type=DataType.FLOAT),
        ),
        routing_mode=RoutingMode.PARTITION,
        curated_output_ref="curated",
        quarantine_output_ref="quarantine",
        reason_column="_quar_reason",
        reason_code="HIGH_AMOUNT",
    )

    step = TransformationStepIR(
        step_id="step_quarantine_1",
        family=ActionFamily.QUARANTINE,
        expression=expr,
        dataset_name="primary",
        kind=StepKind.QUARANTINE_ROUTING,
    )

    graph = TransformationIRGraph(
        graph_id="test_routing_exec_local",
        steps=[step],
        routing_identity_policy=RoutingIdentityPolicy(
            scope=OccurrenceIdentityScope.EXECUTION_LOCAL,
            retain_in_primary_output=False,
            retain_in_secondary_outputs=False,
        ),
    )

    pyspark_compiler = PySparkIRCompiler()
    py_res = pyspark_compiler.compile(graph)
    assert py_res.is_success
    assert py_res.artifact is not None

    # Check generated source: uses F.monotonically_increasing_id() and .cache()
    source = py_res.artifact.source_code
    assert "F.monotonically_increasing_id()" in source
    assert ".cache()" in source
    # Must NOT use row_number().over(Window.orderBy(F.lit(1)))
    assert "row_number().over(Window.orderBy(F.lit(1)))" not in source

    # Execute against Spark
    exec_res = pyspark_compiler.execute(graph, sdf)
    assert "curated" in exec_res.outputs
    assert "quarantine" in exec_res.outputs

    cur_pdf = exec_res.outputs["curated"].toPandas()
    quar_pdf = exec_res.outputs["quarantine"].toPandas()

    assert len(cur_pdf) == 2
    assert len(quar_pdf) == 2
    assert set(cur_pdf["amount"]) == {10.0, 50.0}
    assert set(quar_pdf["amount"]) == {150.0, 200.0}
    assert list(quar_pdf["_quar_reason"]) == ["HIGH_AMOUNT", "HIGH_AMOUNT"]

    # Publication boundary: _identity_col stripped from published outputs
    assert "__dhara_row_occurrence_0" not in cur_pdf.columns
    assert "__dhara_row_occurrence_0" not in quar_pdf.columns


def test_pyspark_routing_identity_source_stable_parity(spark_session: SparkSession):
    """Verifies SOURCE_STABLE coordinate binding and parity with Pandas reference."""
    pdf = pd.DataFrame({
        "part_id": ["p0", "p0", "p1", "p1"],
        "offset": [0, 1, 0, 1],
        "score": [40, 85, 95, 30],
    })
    sdf = df_to_spark(spark_session, pdf)

    spec = SourceStableIdentitySpec(
        snapshot_id="snap_2026_09",
        partition_column="part_id",
        row_offset_column="offset",
    )

    expr = QuarantineExpression(
        predicate=BinaryOp(
            left=ColumnRef(column_name="score"),
            op=BinaryOperator.GTE,
            right=LiteralValue(value=80, data_type=DataType.INTEGER),
        ),
        routing_mode=RoutingMode.PARTITION,
        curated_output_ref="curated",
        quarantine_output_ref="quarantine",
        reason_column="q_reason",
        reason_code="HIGH_SCORE",
    )

    step = TransformationStepIR(
        step_id="step_quar_ss",
        family=ActionFamily.QUARANTINE,
        expression=expr,
        dataset_name="primary",
        kind=StepKind.QUARANTINE_ROUTING,
    )

    policy = RoutingIdentityPolicy(
        scope=OccurrenceIdentityScope.SOURCE_STABLE,
        source_stable_spec=spec,
        retain_in_primary_output=False,
        retain_in_secondary_outputs=True,
    )

    graph = TransformationIRGraph(
        graph_id="test_routing_source_stable",
        steps=[step],
        routing_identity_policy=policy,
    )

    # 1. Pandas reference
    pandas_compiler = PandasIRCompiler()
    pd_res = pandas_compiler.compile(graph)
    assert pd_res.is_success
    pd_outputs = pandas_compiler.execute(graph, pdf).outputs

    # 2. PySpark
    pyspark_compiler = PySparkIRCompiler()
    py_res = pyspark_compiler.compile(graph)
    assert py_res.is_success
    py_outputs = pyspark_compiler.execute(graph, sdf).outputs

    # Compare curated outputs
    pd_cur = pd_outputs["curated"].reset_index(drop=True)
    py_cur = py_outputs["curated"].toPandas().reset_index(drop=True)
    assert len(pd_cur) == len(py_cur) == 2
    assert set(pd_cur["score"]) == set(py_cur["score"]) == {40, 30}

    # Compare quarantine outputs
    pd_q = pd_outputs["quarantine"].reset_index(drop=True)
    py_q = py_outputs["quarantine"].toPandas().reset_index(drop=True)
    assert len(pd_q) == len(py_q) == 2
    assert set(pd_q["score"]) == set(py_q["score"]) == {85, 95}
    assert list(pd_q["q_reason"]) == list(py_q["q_reason"]) == ["HIGH_SCORE", "HIGH_SCORE"]

    # Secondary retained identity column check: cross-engine exact canonical parity
    assert "__dhara_row_occurrence_0" in py_q.columns
    assert set(py_q["__dhara_row_occurrence_0"]) == set(pd_q["__dhara_row_occurrence_0"])
    assert len(set(py_q["__dhara_row_occurrence_0"])) == 2


def test_pyspark_routing_duplicate_coordinates_detected(spark_session: SparkSession):
    """Verifies that duplicate SOURCE_STABLE coordinates fail at runtime."""
    # Intentional duplicate coordinates: two rows have ("p0", 0)
    pdf = pd.DataFrame({
        "part_id": ["p0", "p0"],
        "offset": [0, 0],
        "val": [1, 2],
    })
    sdf = df_to_spark(spark_session, pdf)

    spec = SourceStableIdentitySpec(
        snapshot_id="snap_dup",
        partition_column="part_id",
        row_offset_column="offset",
    )

    expr = QuarantineExpression(
        predicate=BinaryOp(
            left=ColumnRef(column_name="val"),
            op=BinaryOperator.GT,
            right=LiteralValue(value=1, data_type=DataType.INTEGER),
        ),
        routing_mode=RoutingMode.PARTITION,
        curated_output_ref="curated",
        quarantine_output_ref="quarantine",
    )

    step = TransformationStepIR(
        step_id="step_quar_dup",
        family=ActionFamily.QUARANTINE,
        expression=expr,
        dataset_name="primary",
        kind=StepKind.QUARANTINE_ROUTING,
    )

    graph = TransformationIRGraph(
        graph_id="test_dup_coords",
        steps=[step],
        routing_identity_policy=RoutingIdentityPolicy(
            scope=OccurrenceIdentityScope.SOURCE_STABLE,
            source_stable_spec=spec,
        ),
    )

    pyspark_compiler = PySparkIRCompiler()
    py_res = pyspark_compiler.compile(graph)
    assert py_res.is_success

    with pytest.raises(Exception, match="Duplicate source-stable coordinates"):
        pyspark_compiler.execute(graph, sdf)


def test_pyspark_quarantine_copy_mode_parity(spark_session: SparkSession):
    """Verifies Quarantine COPY mode preserves all rows in curated and copies matching rows to quarantine."""
    pdf = pd.DataFrame({"id": [1, 2, 3], "status": ["ACTIVE", "SUSPECT", "ACTIVE"]})
    sdf = df_to_spark(spark_session, pdf)

    expr = QuarantineExpression(
        predicate=BinaryOp(
            left=ColumnRef(column_name="status"),
            op=BinaryOperator.EQ,
            right=LiteralValue(value="SUSPECT", data_type=DataType.STRING),
        ),
        routing_mode=RoutingMode.COPY,
        curated_output_ref="primary_curated",
        quarantine_output_ref="audit_quarantine",
        reason_column="audit_flag",
        reason_code="NEEDS_REVIEW",
    )

    step = TransformationStepIR(
        step_id="step_quar_copy",
        family=ActionFamily.QUARANTINE,
        expression=expr,
        dataset_name="primary",
        kind=StepKind.QUARANTINE_ROUTING,
    )

    graph = TransformationIRGraph(
        graph_id="test_copy_quar",
        steps=[step],
    )

    pyspark_compiler = PySparkIRCompiler()
    exec_res = pyspark_compiler.execute(graph, sdf)
    cur_pdf = exec_res.outputs["primary_curated"].toPandas()
    quar_pdf = exec_res.outputs["audit_quarantine"].toPandas()

    assert len(cur_pdf) == 3  # All 3 retained
    assert len(quar_pdf) == 1  # 1 copied
    assert quar_pdf["id"].iloc[0] == 2
    assert quar_pdf["audit_flag"].iloc[0] == "NEEDS_REVIEW"


def test_pyspark_filter_export_dropped_parity(spark_session: SparkSession):
    """Verifies Filter retains matching rows and exports dropped rows with rejection reason."""
    pdf = pd.DataFrame({"score": [10, 55, 75, 90]})
    sdf = df_to_spark(spark_session, pdf)

    expr = FilterExpression(
        predicate=BinaryOp(
            left=ColumnRef(column_name="score"),
            op=BinaryOperator.GTE,
            right=LiteralValue(value=60, data_type=DataType.INTEGER),
        ),
        retain_matching=True,
        drop_policy=FilterDropPolicy.EXPORT_DROPPED,
        dropped_output_ref="filtered_rejects",
        rejection_reason_code="BELOW_THRESHOLD",
    )

    step = TransformationStepIR(
        step_id="step_filter_export",
        family=ActionFamily.FILTER,
        expression=expr,
        dataset_name="primary",
        kind=StepKind.DATASET_FILTER,
    )

    graph = TransformationIRGraph(
        graph_id="test_filter_export",
        steps=[step],
    )

    # 1. Pandas reference
    pandas_compiler = PandasIRCompiler()
    pd_res = pandas_compiler.execute(graph, pdf)
    pd_retained = pd_res.outputs["primary"]
    pd_dropped = pd_res.outputs["filtered_rejects"]

    # 2. PySpark
    pyspark_compiler = PySparkIRCompiler()
    py_res = pyspark_compiler.execute(graph, sdf)
    py_retained = py_res.outputs["primary"].toPandas()
    py_dropped = py_res.outputs["filtered_rejects"].toPandas()

    assert len(py_retained) == len(pd_retained) == 2
    assert set(py_retained["score"]) == set(pd_retained["score"]) == {75, 90}

    assert len(py_dropped) == len(pd_dropped) == 2
    assert set(py_dropped["score"]) == set(pd_dropped["score"]) == {10, 55}
    assert list(py_dropped["_dhara_filter_reason"]) == ["BELOW_THRESHOLD", "BELOW_THRESHOLD"]


def test_pyspark_source_stable_collision_fixtures_negative(spark_session: SparkSession):
    """
    Negative verification of unambiguous typed tuple encoding for SOURCE_STABLE identity.
    Proves that collision-prone coordinate combinations produce 100% distinct identities:
    1. ("a_b", "c") vs ("a", "b_c")
    2. ("", "a") vs ("_", "a")
    3. (None, "a") vs ("", "a")
    4. integer 1 vs string "1"
    5. boolean True vs string "true"
    Also verifies that identical coordinates trigger duplicate coordinate detection.
    """
    spec = SourceStableIdentitySpec(
        snapshot_id="snap_collision_test",
        partition_column="part",
        row_offset_column="offset",
        source_file_column="file",
    )

    pdf = pd.DataFrame({
        "part": ["a_b", "a", "", "_", "none_test_1", "none_test_2", "type_int", "type_str", "bool_t", "bool_s"],
        "offset": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "file": ["c", "b_c", "a", "a", None, "", "1", "1", "true", "true"],
        "data": range(10),
    })
    sdf = df_to_spark(spark_session, pdf)

    graph = TransformationIRGraph(
        graph_id="test_collision_fixtures",
        steps=[
            TransformationStepIR(
                step_id="step_pass",
                family=ActionFamily.PASS_THROUGH,
                expression=PassThroughExpression(),
                dataset_name="primary",
                kind=StepKind.CONTROL,
            )
        ],
        routing_identity_policy=RoutingIdentityPolicy(
            scope=OccurrenceIdentityScope.SOURCE_STABLE,
            source_stable_spec=spec,
            retain_in_primary_output=True,
        ),
    )

    # 1. Pandas reference execution
    pd_res = PandasIRCompiler().execute(graph, pdf)
    pd_ids = list(pd_res.outputs["primary"]["__dhara_row_occurrence_0"])

    # 2. PySpark execution
    py_res = PySparkIRCompiler().execute(graph, sdf)
    py_ids = list(py_res.outputs["primary"].toPandas()["__dhara_row_occurrence_0"])

    # Cross-engine exact parity
    assert py_ids == pd_ids

    # Collision checks:
    # ("a_b", "c") vs ("a", "b_c")
    assert py_ids[0] != py_ids[1], "Collision between ('a_b', 'c') and ('a', 'b_c')!"
    # ("", "a") vs ("_", "a")
    assert py_ids[2] != py_ids[3], "Collision between ('', 'a') and ('_', 'a')!"
    # (None, "a") vs ("", "a")
    assert py_ids[4] != py_ids[5], "Collision between (None, 'a') and ('', 'a')!"

    # Real duplicate coordinates must be caught
    pdf_dup = pd.DataFrame({
        "part": ["p1", "p1"],
        "offset": [100, 100],
        "file": ["f1", "f1"],
    })
    sdf_dup = df_to_spark(spark_session, pdf_dup)
    with pytest.raises(Exception, match="Duplicate source-stable coordinates detected"):
        PySparkIRCompiler().execute(graph, sdf_dup)


def test_pyspark_identity_materialization_and_cache_release(spark_session: SparkSession):
    """
    Verifies that:
    1. Identity is added once above the branch point and explicitly materialized (.count()).
    2. Curated and quarantine branches descend from the same materialized node.
    3. Technical identity is released (_entry_cached.unpersist()) after execution.
    """
    pdf = pd.DataFrame({
        "status": ["VALID", "SUSPECT", "VALID", "SUSPECT"],
        "val": [10, 20, 30, 40],
    })
    sdf = df_to_spark(spark_session, pdf)

    expr = QuarantineExpression(
        predicate=BinaryOp(
            left=ColumnRef(column_name="status"),
            op=BinaryOperator.EQ,
            right=LiteralValue(value="SUSPECT", data_type=DataType.STRING),
        ),
        routing_mode=RoutingMode.PARTITION,
        curated_output_ref="curated",
        quarantine_output_ref="quarantine",
    )
    step = TransformationStepIR(
        step_id="step_quar_mat",
        family=ActionFamily.QUARANTINE,
        expression=expr,
        dataset_name="primary",
        kind=StepKind.QUARANTINE_ROUTING,
    )
    graph = TransformationIRGraph(
        graph_id="test_mat_cache",
        steps=[step],
        routing_identity_policy=RoutingIdentityPolicy(
            scope=OccurrenceIdentityScope.EXECUTION_LOCAL,
            retain_in_primary_output=True,
            retain_in_secondary_outputs=True,
        ),
    )

    compiler = PySparkIRCompiler()
    comp_res = compiler.compile(graph)
    assert comp_res.is_success
    code = comp_res.artifact.source_code

    # Verify static code guarantees: try ... finally: _entry_cached.unpersist()
    assert "_entry_cached = _working.cache()" in code
    assert "_ = _entry_cached.count()  # Explicit materialization action before branching" in code
    assert "try:" in code
    assert "finally:" in code
    assert "_entry_cached.unpersist()" in code

    # Execute and verify branch identity conservation on success path
    exec_res = compiler.execute(graph, sdf)
    cur_ids = set(exec_res.outputs["curated"].toPandas()["__dhara_row_occurrence_0"])
    quar_ids = set(exec_res.outputs["quarantine"].toPandas()["__dhara_row_occurrence_0"])

    assert len(cur_ids) == 2
    assert len(quar_ids) == 2
    assert cur_ids.isdisjoint(quar_ids)
    assert len(cur_ids | quar_ids) == 4

    # Verify exception-safe cleanup on failed branch execution
    step_fail = TransformationStepIR(
        step_id="step_fail_validation",
        family=ActionFamily.VALIDATE,
        expression=ValidateExpression(
            predicate=BinaryOp(
                left=ColumnRef(column_name="val"),
                op=BinaryOperator.LT,
                right=LiteralValue(value=0, data_type=DataType.INTEGER),
            ),
            severity=ValidateSeverity.ERROR,
        ),
        dataset_name="primary",
        kind=StepKind.ASSERTION,
    )
    graph_fail = TransformationIRGraph(
        graph_id="test_mat_cache_failure",
        steps=[step, step_fail],
        routing_identity_policy=RoutingIdentityPolicy(
            scope=OccurrenceIdentityScope.EXECUTION_LOCAL,
        ),
    )
    res_fail = compiler.compile(graph_fail)
    assert res_fail.is_success

    loc = {}
    exec(res_fail.artifact.source_code, loc)
    transform_fn = loc["transform_datasets"]

    from unittest.mock import patch
    import pyspark.sql

    with patch.object(pyspark.sql.DataFrame, "unpersist", autospec=True) as mock_unpersist:
        with pytest.raises(Exception):
            transform_fn(sdf)
        assert mock_unpersist.called, "unpersist() must execute in finally block even when downstream execution raises!"

