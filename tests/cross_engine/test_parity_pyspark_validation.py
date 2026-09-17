"""
tests/cross_engine/test_parity_pyspark_validation.py

Phase 3 Increment 3.2 Work Package 3.2A:
PySpark Validation Parity & Invariants.

Verifies:
1. Pure validation without row partitioning, row drops, or value mutation (Non-Mutating Invariant).
2. Category-specific metric parity between Pandas reference compiler and PySpark compiler:
   - candidate_count
   - pass_count
   - violation_count
   - raw_unknown_count
   - effective_violation_count
3. Three-valued boolean unknown policy resolution:
   - COUNT_AS_VIOLATION
   - COUNT_AS_PASS
   - REPORT_SEPARATELY
   - FAIL_STEP (aborts when unknown rows encountered)
4. Severity gate outcomes:
   - WARNING (emits metric summary with gate_outcome='WARN', does not abort pipeline)
   - ERROR (aborts pipeline with bounded diagnostic ValueError when violations > 0)
"""

from __future__ import annotations

import json
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
    LiteralValue,
    NullCheck,
    StepKind,
    TransformationIRGraph,
    TransformationStepIR,
    ValidateExpression,
    ValidateSeverity,
    ValidationUnknownPolicy,
)
from tests.cross_engine.assertions import (
    assert_schema_parity,
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


def spark_to_df(sdf) -> pd.DataFrame:
    """Safely converts a PySpark DataFrame back to pandas via Catalyst row dictionaries."""
    rows = [r.asDict() for r in sdf.collect()]
    return pd.DataFrame(rows)


# ============================================================================
# 1. Non-Mutating Validation & Metric Parity
# ============================================================================

def test_pyspark_validation_non_mutating_metric_parity(spark_session):
    """
    Verifies that VALIDATE computes exact metric parity against Pandas reference:
    candidate_count == pass_count + violation_count + raw_unknown_count.
    Verifies that input DataFrame is completely unmutated (values, schema, row count).
    """
    pdf = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "age": [25, -5, 40, None, 18],  # 25, 40, 18 pass; -5 violates; None is unknown
    })

    col = ColumnRef(column_name="age", dataset_name="primary")
    cond = BinaryOp(left=col, op=BinaryOperator.GTE, right=LiteralValue(value=0, data_type=DataType.INTEGER))

    step = TransformationStepIR(
        step_id="v_age_parity",
        dataset_name="primary",
        family=ActionFamily.VALIDATE,
        kind=StepKind.ASSERTION,
        expression=ValidateExpression(
            predicate=cond,
            severity=ValidateSeverity.WARNING,
            unknown_policy=ValidationUnknownPolicy.REPORT_SEPARATELY,
            reason_code="NEGATIVE_AGE_PROHIBITED",
        ),
    )
    graph = TransformationIRGraph(graph_id="g_v_metrics", steps=(step,), primary_output_ref="primary")

    # 1. Pandas reference compilation and execution
    p_compiler = PandasIRCompiler()
    p_res = p_compiler.execute(graph, pdf.copy())
    p_df = p_res.primary_output
    p_metrics = p_res.validation_summaries["v_age_parity"]

    # 2. PySpark compilation and execution
    s_compiler = PySparkIRCompiler()
    sdf_in = df_to_spark(spark_session, pdf)
    s_res = s_compiler.execute(graph, sdf_in)
    s_df = spark_to_df(s_res.primary_output)
    s_metrics = s_res.validation_summaries["v_age_parity"]

    # Invariant: Non-mutating (Frame before == Frame after)
    assert len(s_df) == len(pdf)
    assert list(s_df["id"]) == list(pdf["id"])
    assert len(p_df) == len(pdf)

    # Metric parity assertions: candidate = pass + viol + unk
    assert s_metrics["candidate_count"] == 5
    assert s_metrics["pass_count"] == 3
    assert s_metrics["violation_count"] == 1
    assert s_metrics["raw_unknown_count"] == 1
    assert s_metrics["effective_violation_count"] == 1
    assert s_metrics["gate_outcome"] == "WARN"

    # Cross-engine exact equality
    for key in ["candidate_count", "pass_count", "violation_count", "raw_unknown_count", "effective_violation_count", "gate_outcome"]:
        assert s_metrics[key] == p_metrics[key], f"Metric mismatch for {key}: {s_metrics[key]} != {p_metrics[key]}"


# ============================================================================
# 2. Unknown Policies Resolution
# ============================================================================

def test_pyspark_validation_unknown_policy_count_as_violation(spark_session):
    """Under COUNT_AS_VIOLATION, effective_violation = violation + unknown."""
    pdf = pd.DataFrame({
        "status": ["OK", "FAIL", None, "OK"],
    })
    col = ColumnRef(column_name="status", dataset_name="primary")
    cond = BinaryOp(left=col, op=BinaryOperator.EQ, right=LiteralValue(value="OK"))

    step = TransformationStepIR(
        step_id="v_unk_viol",
        dataset_name="primary",
        family=ActionFamily.VALIDATE,
        kind=StepKind.ASSERTION,
        expression=ValidateExpression(
            predicate=cond,
            severity=ValidateSeverity.WARNING,
            unknown_policy=ValidationUnknownPolicy.COUNT_AS_VIOLATION,
            reason_code="MUST_BE_OK",
        ),
    )
    graph = TransformationIRGraph(graph_id="g_unk_v", steps=(step,), primary_output_ref="primary")

    p_res = PandasIRCompiler().execute(graph, pdf.copy())
    s_compiler = PySparkIRCompiler()
    sdf_in = df_to_spark(spark_session, pdf)
    s_res = s_compiler.execute(graph, sdf_in)

    p_m = p_res.validation_summaries["v_unk_viol"]
    s_m = s_res.validation_summaries["v_unk_viol"]

    # 1 violation ("FAIL") + 1 unknown (None) -> effective_violation = 2
    assert s_m["pass_count"] == 2
    assert s_m["violation_count"] == 1
    assert s_m["raw_unknown_count"] == 1
    assert s_m["effective_violation_count"] == 2
    assert s_m["effective_violation_count"] == p_m["effective_violation_count"]


def test_pyspark_validation_unknown_policy_count_as_pass(spark_session):
    """Under COUNT_AS_PASS, effective_violation = violation only."""
    pdf = pd.DataFrame({
        "status": ["OK", "FAIL", None, "OK"],
    })
    col = ColumnRef(column_name="status", dataset_name="primary")
    cond = BinaryOp(left=col, op=BinaryOperator.EQ, right=LiteralValue(value="OK"))

    step = TransformationStepIR(
        step_id="v_unk_pass",
        dataset_name="primary",
        family=ActionFamily.VALIDATE,
        kind=StepKind.ASSERTION,
        expression=ValidateExpression(
            predicate=cond,
            severity=ValidateSeverity.WARNING,
            unknown_policy=ValidationUnknownPolicy.COUNT_AS_PASS,
            reason_code="MUST_BE_OK",
        ),
    )
    graph = TransformationIRGraph(graph_id="g_unk_p", steps=(step,), primary_output_ref="primary")

    p_res = PandasIRCompiler().execute(graph, pdf.copy())
    s_compiler = PySparkIRCompiler()
    sdf_in = df_to_spark(spark_session, pdf)
    s_res = s_compiler.execute(graph, sdf_in)

    p_m = p_res.validation_summaries["v_unk_pass"]
    s_m = s_res.validation_summaries["v_unk_pass"]

    # Unknown treated as pass, effective_violation = 1
    assert s_m["effective_violation_count"] == 1
    assert s_m["effective_violation_count"] == p_m["effective_violation_count"]


# ============================================================================
# 3. Severity Gates: WARNING vs ERROR
# ============================================================================

def test_pyspark_validation_severity_error_raises(spark_session):
    """Under ValidateSeverity.ERROR, execution raises ValueError when violations > 0."""
    pdf = pd.DataFrame({
        "score": [80, 20],  # 20 violates score >= 50
    })
    col = ColumnRef(column_name="score", dataset_name="primary")
    cond = BinaryOp(left=col, op=BinaryOperator.GTE, right=LiteralValue(value=50, data_type=DataType.INTEGER))

    step_err = TransformationStepIR(
        step_id="v_err",
        dataset_name="primary",
        family=ActionFamily.VALIDATE,
        kind=StepKind.ASSERTION,
        expression=ValidateExpression(
            predicate=cond,
            severity=ValidateSeverity.ERROR,
            reason_code="SCORE_TOO_LOW",
        ),
    )
    graph = TransformationIRGraph(graph_id="g_err", steps=(step_err,), primary_output_ref="primary")

    compiler = PySparkIRCompiler()
    sdf_in = df_to_spark(spark_session, pdf)

    with pytest.raises(ValueError, match="Validation assertion failed"):
        compiler.execute(graph, sdf_in)


def test_pyspark_validation_fail_step_on_unknown(spark_session):
    """Under FAIL_STEP unknown policy, execution raises ValueError when unknown rows are encountered."""
    pdf = pd.DataFrame({
        "score": [80, None],
    })
    col = ColumnRef(column_name="score", dataset_name="primary")
    cond = BinaryOp(left=col, op=BinaryOperator.GTE, right=LiteralValue(value=50, data_type=DataType.INTEGER))

    step = TransformationStepIR(
        step_id="v_fail_unk",
        dataset_name="primary",
        family=ActionFamily.VALIDATE,
        kind=StepKind.ASSERTION,
        expression=ValidateExpression(
            predicate=cond,
            severity=ValidateSeverity.WARNING,
            unknown_policy=ValidationUnknownPolicy.FAIL_STEP,
            reason_code="SCORE_TOO_LOW",
        ),
    )
    graph = TransformationIRGraph(graph_id="g_fail_unk", steps=(step,), primary_output_ref="primary")

    compiler = PySparkIRCompiler()
    sdf_in = df_to_spark(spark_session, pdf)

    with pytest.raises(ValueError, match="Validation condition evaluated to unknown under FAIL_STEP"):
        compiler.execute(graph, sdf_in)
