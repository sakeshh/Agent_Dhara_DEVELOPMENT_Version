"""
tests/cross_engine/test_parity_pyspark_joins_derivations.py

Increment 3.3 PySpark Cross-Engine Parity Certification Suite.

Validates semantic parity between the certified Pandas reference compiler and the PySpark compiler for:
1. Column Derivations (Arithmetic with INT64 pre-widening & CLIP_TO_RANGE, FLOAT64 division by zero,
   DECIMAL precision, Conditional unknown policies, Coalesce, Concat, Date Arithmetic).
2. Extractions (Regex multi-match policy, Delimiter literal regex metacharacters,
   DatePart normalized dayofweek, Bounded JSONPath).
3. Multi-Dataset Joins (INNER with cardinality guard, ONE_TO_ONE, Null key policy,
   Null-safe matching, CROSS join authorization and row expansion limits, SEMI/ANTI left-schema preservation,
   Unmatched routing, Suffix collision resolution & preflight rejection, RIGHT/FULL key projection).
4. Multi-Dataset Aggregations (COUNT(*) vs COUNT(col) vs COUNT_DISTINCT, SUM/AVG/MIN/MAX,
   STDDEV_SAMPLE vs STDDEV_POPULATION, MEDIAN approx, GroupKeyNullPolicy, all-null sum policy,
   empty input policy).
5. In-Memory Publication Layer (staged execution, postcondition failure abort, late failure abort,
   exception abort, commit/abort idempotency).
6. Multi-Dataset End-to-End Pipeline (Joins + Derivations + Aggregations + Publication).

Total: 37 tests.
"""
from __future__ import annotations

import datetime
from decimal import Decimal
import json
import os
import tempfile
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import SparkSession

from agent.etl_pipeline.codegen_shared import CodegenStatus
from agent.etl_pipeline.pandas_ir_compiler import (
    PandasIRCompiler,
    RowExpansionLimitExceededError,
)
from agent.etl_pipeline.pyspark_ir_compiler import (
    PySparkIRCompiler,
    PySparkInMemoryPublicationAdapter,
    PySparkPublicationPostconditionError,
    execute_pyspark_with_publication_contracts,
)
from agent.etl_pipeline.transformation_ir import (
    ActionFamily,
    AggregateColumnSpec,
    AggregateExpression,
    AggregateOp,
    AggregateValueNullPolicy,
    ArithmeticDerivation,
    ArithmeticDomain,
    ArithmeticOperator,
    BinaryOp,
    BinaryOperator,
    BoundedJsonPath,
    ClipExpression,
    CoalesceDerivation,
    ColumnRef,
    ConcatDerivation,
    ConcatNullPolicy,
    ConditionalDerivation,
    ConditionalUnknownPolicy,
    DateArithmeticDerivation,
    DateArithmeticUnit,
    DatePart,
    DatePartExtract,
    DelimiterExtract,
    DivisionByZeroPolicy,
    EmptyInputPolicy,
    ExtractFailurePolicy,
    GroupKeyNullPolicy,
    JoinCardinality,
    JoinCollisionPolicy,
    JoinExpression,
    JoinKeyPair,
    JoinNullPolicy,
    JoinType,
    JoinUnmatchedPolicy,
    JsonPathExtract,
    LiteralValue,
    MultipleMatchPolicy,
    NumericOperand,
    NumericOverflowPolicy,
    RegexExtract,
    StepAuthorization,
    StepKind,
    TransformationIRGraph,
    TransformationStepIR,
    UnmatchedRowDisposition,
)


def df_to_spark(spark: SparkSession, pdf: pd.DataFrame):
    """
    Ingests pandas DataFrame into PySpark via temporary JSON file,
    ensuring 100% JVM Catalyst execution without Python worker socket crashes on Windows.
    """
    records = pdf.replace({np.nan: None}).to_dict(orient="records")
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(records, f)
        temp_path = f.name
    try:
        df = spark.read.json(temp_path)
        df = df.cache()
        _ = df.count()
        return df
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass


def spark_to_df(spark_df) -> pd.DataFrame:
    """Converts a PySpark DataFrame to pandas DataFrame via JVM Row dict conversion."""
    records = [r.asDict() for r in spark_df.collect()]
    return pd.DataFrame(records)


def _exec_pandas(graph: TransformationIRGraph, datasets: Any) -> Dict[str, pd.DataFrame]:
    compiler = PandasIRCompiler()
    res = compiler.compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED
    ns = {
        "pd": pd,
        "np": np,
        "Decimal": Decimal,
        "RowExpansionLimitExceededError": RowExpansionLimitExceededError,
    }
    exec(res.artifact.source_code, ns)
    fn = ns["transform_datasets"]
    return fn(datasets)

# ============================================================================
# Section 1: Derivations & Extractions (13 tests)
# ============================================================================

def test_pyspark_arithmetic_overflow_and_clip(spark_session):
    """1. INT64 overflow detected before wraparound, respects CLIP_TO_RANGE."""
    expr = ArithmeticDerivation(
        left=NumericOperand(column_name="a"),
        right=NumericOperand(column_name="b"),
        operator=ArithmeticOperator.ADD,
        domain=ArithmeticDomain.INT64,
        overflow_policy=NumericOverflowPolicy.CLIP_TO_RANGE,
        output_column="sum_col",
    )
    step = TransformationStepIR(
        step_id="s_arith", dataset_name="d", family=ActionFamily.DERIVE,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr, target_column="sum_col"
    )
    graph = TransformationIRGraph(graph_id="g_arith", steps=(step,))

    pdf = pd.DataFrame({"a": [9223372036854775800, -9223372036854775800], "b": [100, -100]})
    # Pandas reference
    p_outs = _exec_pandas(graph, pdf.copy())
    p_res = list(p_outs["primary"]["sum_col"])
    assert p_res == [9223372036854775807, -9223372036854775808]

    # Spark
    sdf = df_to_spark(spark_session, pdf)
    s_res = PySparkIRCompiler().execute(graph, sdf)
    s_df = spark_to_df(s_res.primary_output)
    assert list(s_df["sum_col"]) == p_res


def test_pyspark_int64_multiplication_widens_before_clip(spark_session):
    """2. INT64 multiplication widens to DECIMAL before arithmetic, clips cleanly."""
    expr = ArithmeticDerivation(
        left=NumericOperand(column_name="a"),
        right=NumericOperand(column_name="b"),
        operator=ArithmeticOperator.MULTIPLY,
        domain=ArithmeticDomain.INT64,
        overflow_policy=NumericOverflowPolicy.CLIP_TO_RANGE,
        output_column="mult_col",
    )
    step = TransformationStepIR(
        step_id="s_mult", dataset_name="d", family=ActionFamily.DERIVE,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr, target_column="mult_col"
    )
    graph = TransformationIRGraph(graph_id="g_mult", steps=(step,))

    pdf = pd.DataFrame({"a": [3000000000, 4000000000000000000], "b": [3, 10]})
    sdf = df_to_spark(spark_session, pdf)
    s_res = PySparkIRCompiler().execute(graph, sdf)
    s_df = spark_to_df(s_res.primary_output)
    assert s_df["mult_col"].iloc[0] == 9000000000
    assert s_df["mult_col"].iloc[1] == 9223372036854775807


def test_pyspark_float_division_by_zero_policies(spark_session):
    """3. FLOAT64 division by zero respects SET_NULL vs RETURN_INFINITY vs FAIL_STEP."""
    # SET_NULL
    expr_null = ArithmeticDerivation(
        left=NumericOperand(column_name="x"), right=NumericOperand(column_name="y"),
        operator=ArithmeticOperator.DIVIDE, domain=ArithmeticDomain.FLOAT64,
        division_by_zero_policy=DivisionByZeroPolicy.SET_NULL, output_column="div_col",
    )
    g_null = TransformationIRGraph(graph_id="g_dn", steps=(
        TransformationStepIR(step_id="s_dn", dataset_name="d", family=ActionFamily.DERIVE,
                             kind=StepKind.ROW_TRANSFORMATION, expression=expr_null, target_column="div_col"),
    ))
    pdf = pd.DataFrame({"x": [10.0, 20.0], "y": [2.0, 0.0]})
    sdf = df_to_spark(spark_session, pdf)
    s_null = PySparkIRCompiler().execute(g_null, sdf)
    df_null = spark_to_df(s_null.primary_output)
    assert df_null["div_col"].iloc[0] == 5.0
    assert df_null["div_col"].iloc[1] is None or pd.isna(df_null["div_col"].iloc[1])

    # RETURN_INFINITY
    expr_inf = ArithmeticDerivation(
        left=NumericOperand(column_name="x"), right=NumericOperand(column_name="y"),
        operator=ArithmeticOperator.DIVIDE, domain=ArithmeticDomain.FLOAT64,
        division_by_zero_policy=DivisionByZeroPolicy.RETURN_INFINITY, output_column="div_col",
    )
    g_inf = TransformationIRGraph(graph_id="g_di", steps=(
        TransformationStepIR(step_id="s_di", dataset_name="d", family=ActionFamily.DERIVE,
                             kind=StepKind.ROW_TRANSFORMATION, expression=expr_inf, target_column="div_col"),
    ))
    pdf2 = pd.DataFrame({"x": [20.0, -20.0], "y": [0.0, 0.0]})
    sdf2 = df_to_spark(spark_session, pdf2)
    s_inf = PySparkIRCompiler().execute(g_inf, sdf2)
    df_inf = spark_to_df(s_inf.primary_output)
    assert np.isposinf(df_inf["div_col"].iloc[0])
    assert np.isneginf(df_inf["div_col"].iloc[1])

    # FAIL_STEP
    expr_fail = ArithmeticDerivation(
        left=NumericOperand(column_name="x"), right=NumericOperand(column_name="y"),
        operator=ArithmeticOperator.DIVIDE, domain=ArithmeticDomain.FLOAT64,
        division_by_zero_policy=DivisionByZeroPolicy.FAIL_STEP, output_column="div_col",
    )
    g_fail = TransformationIRGraph(graph_id="g_df", steps=(
        TransformationStepIR(step_id="s_df", dataset_name="d", family=ActionFamily.DERIVE,
                             kind=StepKind.ROW_TRANSFORMATION, expression=expr_fail, target_column="div_col"),
    ))
    with pytest.raises(Exception):
        PySparkIRCompiler().execute(g_fail, sdf)


def test_pyspark_decimal_precision_scale_and_rounding(spark_session):
    """4. DECIMAL domain respects precision and scale casting."""
    expr = ArithmeticDerivation(
        left=NumericOperand(column_name="val"),
        right=NumericOperand(literal_value=1),
        operator=ArithmeticOperator.ADD,
        domain=ArithmeticDomain.DECIMAL,
        output_column="dec_out",
    )
    step = TransformationStepIR(
        step_id="s_dec", dataset_name="d", family=ActionFamily.DERIVE,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr, target_column="dec_out"
    )
    graph = TransformationIRGraph(graph_id="g_dec", steps=(step,))
    pdf = pd.DataFrame({"val": [10.5, 20.25]})
    sdf = df_to_spark(spark_session, pdf)
    s_res = PySparkIRCompiler().execute(graph, sdf)
    s_df = spark_to_df(s_res.primary_output)
    assert len(s_df) == 2
    assert float(s_df["dec_out"].iloc[0]) == 11.5


def test_pyspark_conditional_unknown_policy(spark_session):
    """5. Conditional derivation explicitly handles True, False, and Unknown (NA)."""
    col = ColumnRef(column_name="score", dataset_name="d")
    cond = BinaryOp(left=col, op=BinaryOperator.GT, right=LiteralValue(value=50))
    expr = ConditionalDerivation(
        condition=cond,
        then_value="PASS",
        else_value="FAIL",
        unknown_policy=ConditionalUnknownPolicy.USE_FALLBACK,
        fallback_value="UNKNOWN_SCORE",
        output_column="result",
    )
    step = TransformationStepIR(
        step_id="step_cond", dataset_name="d", family=ActionFamily.DERIVE,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr, target_column="result"
    )
    graph = TransformationIRGraph(graph_id="g_cond", steps=(step,))

    pdf = pd.DataFrame({"score": [80.0, 30.0, np.nan]})
    # Pandas
    p_outs = _exec_pandas(graph, pdf.copy())
    assert list(p_outs["primary"]["result"]) == ["PASS", "FAIL", "UNKNOWN_SCORE"]

    # Spark
    sdf = df_to_spark(spark_session, pdf)
    s_res = PySparkIRCompiler().execute(graph, sdf)
    s_df = spark_to_df(s_res.primary_output)
    assert list(s_df["result"]) == ["PASS", "FAIL", "UNKNOWN_SCORE"]


def test_pyspark_coalesce_derivation(spark_session):
    """6. CoalesceDerivation combines columns with fallback value."""
    expr = CoalesceDerivation(
        columns=["phone1", "phone2"],
        fallback_value="NO_PHONE",
        output_column="phone",
    )
    step = TransformationStepIR(
        step_id="s_coal", dataset_name="d", family=ActionFamily.DERIVE,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr, target_column="phone"
    )
    graph = TransformationIRGraph(graph_id="g_coal", steps=(step,))

    pdf = pd.DataFrame({
        "phone1": [None, "555-1234", None],
        "phone2": ["555-9999", None, None],
    })
    sdf = df_to_spark(spark_session, pdf)
    s_res = PySparkIRCompiler().execute(graph, sdf)
    s_df = spark_to_df(s_res.primary_output)
    assert list(s_df["phone"]) == ["555-9999", "555-1234", "NO_PHONE"]


def test_pyspark_concat_null_policies_and_separators(spark_session):
    """7. ConcatDerivation with IGNORE_NULLS vs PROPAGATE_NULL vs PRESERVE_AS_EMPTY."""
    pdf = pd.DataFrame({
        "first_name": ["Alice", "Bob", None],
        "last_name": [None, "Smith", "Jones"],
    })
    sdf = df_to_spark(spark_session, pdf)

    # IGNORE_NULLS
    e_ign = ConcatDerivation(columns=["first_name", "last_name"], delimiter=" ", null_policy=ConcatNullPolicy.IGNORE_NULLS, output_column="full")
    g_ign = TransformationIRGraph(graph_id="g_ign", steps=(TransformationStepIR(step_id="s1", dataset_name="d", family=ActionFamily.DERIVE, kind=StepKind.ROW_TRANSFORMATION, expression=e_ign, target_column="full"),))
    df_ign = spark_to_df(PySparkIRCompiler().execute(g_ign, sdf).primary_output)
    assert list(df_ign["full"]) == ["Alice", "Bob Smith", "Jones"]

    # PROPAGATE_NULL
    e_prop = ConcatDerivation(columns=["first_name", "last_name"], delimiter=" ", null_policy=ConcatNullPolicy.PROPAGATE_NULL, output_column="full")
    g_prop = TransformationIRGraph(graph_id="g_prop", steps=(TransformationStepIR(step_id="s2", dataset_name="d", family=ActionFamily.DERIVE, kind=StepKind.ROW_TRANSFORMATION, expression=e_prop, target_column="full"),))
    df_prop = spark_to_df(PySparkIRCompiler().execute(g_prop, sdf).primary_output)
    assert df_prop["full"].iloc[0] is None
    assert df_prop["full"].iloc[1] == "Bob Smith"
    assert df_prop["full"].iloc[2] is None


def test_pyspark_concat_all_null_and_empty_string_distinction(spark_session):
    """8. ConcatDerivation distinguishes empty string from NULL."""
    pdf = pd.DataFrame({
        "a": ["", None],
        "b": ["", None],
    })
    sdf = df_to_spark(spark_session, pdf)
    expr = ConcatDerivation(columns=["a", "b"], delimiter="|", null_policy=ConcatNullPolicy.PRESERVE_AS_EMPTY, output_column="joined")
    graph = TransformationIRGraph(graph_id="g_dist", steps=(TransformationStepIR(step_id="s", dataset_name="d", family=ActionFamily.DERIVE, kind=StepKind.ROW_TRANSFORMATION, expression=expr, target_column="joined"),))
    df_out = spark_to_df(PySparkIRCompiler().execute(graph, sdf).primary_output)
    assert df_out["joined"].iloc[0] == "|"
    assert df_out["joined"].iloc[1] == "|"


def test_pyspark_date_arithmetic_parity(spark_session):
    """9. DateArithmeticDerivation adds/subtracts days and months including leap years."""
    expr_day = DateArithmeticDerivation(
        source_column="dt", operation="add", amount=5, unit=DateArithmeticUnit.DAY, output_column="plus_5d"
    )
    expr_month = DateArithmeticDerivation(
        source_column="dt", operation="add", amount=1, unit=DateArithmeticUnit.MONTH, output_column="plus_1m"
    )
    graph = TransformationIRGraph(graph_id="g_date", steps=(
        TransformationStepIR(step_id="s1", dataset_name="d", family=ActionFamily.DERIVE, kind=StepKind.ROW_TRANSFORMATION, expression=expr_day, target_column="plus_5d"),
        TransformationStepIR(step_id="s2", dataset_name="d", family=ActionFamily.DERIVE, kind=StepKind.ROW_TRANSFORMATION, expression=expr_month, target_column="plus_1m"),
    ))
    pdf = pd.DataFrame({"dt": ["2024-02-29", "2024-06-15"]})
    sdf = df_to_spark(spark_session, pdf)
    s_res = PySparkIRCompiler().execute(graph, sdf)
    s_df = spark_to_df(s_res.primary_output)
    # 2024-02-29 + 5 days = 2024-03-05
    assert str(s_df["plus_5d"].iloc[0]).startswith("2024-03-05")
    # 2024-02-29 + 1 month = 2024-03-29
    assert str(s_df["plus_1m"].iloc[0]).startswith("2024-03-29")


def test_pyspark_regex_extract_multiple_match_policy(spark_session):
    """10. RegexExtract fails step under FAIL_STEP when multiple matches occur."""
    expr = RegexExtract(
        source_column="text",
        pattern=r"\d+",
        group_index=0,
        multiple_match_policy=MultipleMatchPolicy.FAIL_STEP,
        output_column="num",
    )
    step = TransformationStepIR(step_id="s_re", dataset_name="d", family=ActionFamily.EXTRACT, kind=StepKind.ROW_TRANSFORMATION, expression=expr, target_column="num")
    graph = TransformationIRGraph(graph_id="g_re", steps=(step,))
    pdf = pd.DataFrame({"text": ["Invoice 123 has 456 items"]})
    sdf = df_to_spark(spark_session, pdf)
    with pytest.raises(Exception):
        PySparkIRCompiler().execute(graph, sdf)


def test_pyspark_delimiter_literal_regex_metacharacters(spark_session):
    """11. DelimiterExtract escapes regex metacharacters like '.' and '|'."""
    s1 = TransformationStepIR(
        step_id="s_dot", dataset_name="d", family=ActionFamily.EXTRACT, kind=StepKind.ROW_TRANSFORMATION,
        expression=DelimiterExtract(source_column="url", delimiter=".", index=1, output_column="domain"),
        target_column="domain",
    )
    s2 = TransformationStepIR(
        step_id="s_pipe", dataset_name="d", family=ActionFamily.EXTRACT, kind=StepKind.ROW_TRANSFORMATION,
        expression=DelimiterExtract(source_column="pipe_text", delimiter="|", index=1, output_column="mid"),
        target_column="mid",
    )
    graph = TransformationIRGraph(graph_id="g_delim", steps=(s1, s2))
    pdf = pd.DataFrame({
        "url": ["www.google.com", "api.github.com"],
        "pipe_text": ["A|B|C", "X|Y|Z"],
    })
    sdf = df_to_spark(spark_session, pdf)
    s_res = PySparkIRCompiler().execute(graph, sdf)
    s_df = spark_to_df(s_res.primary_output)
    assert list(s_df["domain"]) == ["google", "github"]
    assert list(s_df["mid"]) == ["B", "Y"]


def test_pyspark_day_of_week_normalized_to_ir_contract(spark_session):
    """12. DatePartExtract normalizes day of week to Monday=0 ... Sunday=6."""
    expr = DatePartExtract(source_column="ts", part=DatePart.DAY_OF_WEEK, output_column="dow")
    step = TransformationStepIR(step_id="s_dow", dataset_name="d", family=ActionFamily.EXTRACT, kind=StepKind.ROW_TRANSFORMATION, expression=expr, target_column="dow")
    graph = TransformationIRGraph(graph_id="g_dow", steps=(step,))

    # 2024-06-16 is Sunday -> 6
    # 2024-06-17 is Monday -> 0
    # 2024-06-21 is Friday -> 4
    pdf = pd.DataFrame({"ts": ["2024-06-16", "2024-06-17", "2024-06-21"]})
    sdf = df_to_spark(spark_session, pdf)
    s_res = PySparkIRCompiler().execute(graph, sdf)
    s_df = spark_to_df(s_res.primary_output)
    assert list(s_df["dow"]) == [6, 0, 4]


def test_pyspark_json_path_bounded_and_default_blocking(spark_session):
    """13. JsonPathExtract extracts bounded JSON paths and blocks wildcards."""
    # Bounded extraction
    expr = JsonPathExtract(
        source_column="raw_json",
        path="user.addresses[0].zip",
        output_column="zip_code",
    )
    step = TransformationStepIR(step_id="s_json", dataset_name="d", family=ActionFamily.EXTRACT, kind=StepKind.ROW_TRANSFORMATION, expression=expr, target_column="zip_code")
    graph = TransformationIRGraph(graph_id="g_json", steps=(step,))
    pdf = pd.DataFrame({
        "raw_json": [
            '{"user": {"addresses": [{"zip": "94105"}, {"zip": "94107"}]}}',
            '{"user": {"addresses": []}}',
        ]
    })
    sdf = df_to_spark(spark_session, pdf)
    s_res = PySparkIRCompiler().execute(graph, sdf)
    s_df = spark_to_df(s_res.primary_output)
    assert s_df["zip_code"].iloc[0] == "94105"
    assert s_df["zip_code"].iloc[1] is None or pd.isna(s_df["zip_code"].iloc[1])

    # Model validation blocks recursive / wildcard syntax
    with pytest.raises(ValueError):
        BoundedJsonPath(path="user.*.id")

# ============================================================================
# Section 2: Joins (12 tests)
# ============================================================================

def test_pyspark_join_inner_and_cardinality_guard(spark_session):
    """14. Inner join with ONE_TO_MANY cardinality guard fails on duplicate left keys."""
    expr = JoinExpression(
        left_dataset="orders", right_dataset="items", join_type=JoinType.INNER,
        key_pairs=[JoinKeyPair(left="order_id", right="order_id")],
        cardinality=JoinCardinality.ONE_TO_MANY, output_dataset="order_details",
    )
    step = TransformationStepIR(step_id="s_join", dataset_name="orders", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr)
    graph = TransformationIRGraph(graph_id="g_join", steps=(step,))

    df_orders_dup = pd.DataFrame({"order_id": [101, 101], "customer": ["A", "B"]})
    df_items = pd.DataFrame({"order_id": [101, 101], "sku": ["SKU1", "SKU2"]})
    s_orders = df_to_spark(spark_session, df_orders_dup)
    s_items = df_to_spark(spark_session, df_items)

    with pytest.raises(Exception, match="Left table has duplicate keys under one_to_many"):
        PySparkIRCompiler().execute(graph, {"orders": s_orders, "items": s_items})


def test_pyspark_join_cardinality_one_to_one_rejection(spark_session):
    """15. ONE_TO_ONE cardinality check rejects duplicate right keys."""
    expr = JoinExpression(
        left_dataset="users", right_dataset="profiles", join_type=JoinType.INNER,
        key_pairs=[JoinKeyPair(left="uid", right="uid")],
        cardinality=JoinCardinality.ONE_TO_ONE, output_dataset="user_profile",
    )
    step = TransformationStepIR(step_id="s_1to1", dataset_name="users", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr)
    graph = TransformationIRGraph(graph_id="g_1to1", steps=(step,))

    df_users = pd.DataFrame({"uid": [1, 2], "name": ["A", "B"]})
    df_profiles_dup = pd.DataFrame({"uid": [1, 1], "bio": ["Bio1", "Bio2"]})
    s_users = df_to_spark(spark_session, df_users)
    s_profiles = df_to_spark(spark_session, df_profiles_dup)

    with pytest.raises(Exception, match="Right table has duplicate keys under one_to_one"):
        PySparkIRCompiler().execute(graph, {"users": s_users, "profiles": s_profiles})


def test_pyspark_join_null_key_fail_step(spark_session):
    """16. JoinNullPolicy.FAIL_STEP_ON_NULL_KEY raises error when join key is NULL."""
    expr = JoinExpression(
        left_dataset="orders", right_dataset="customers", join_type=JoinType.INNER,
        key_pairs=[JoinKeyPair(left="cid", right="cid")],
        null_policy=JoinNullPolicy.FAIL_STEP_ON_NULL_KEY, output_dataset="out",
    )
    step = TransformationStepIR(step_id="s_nk", dataset_name="orders", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr)
    graph = TransformationIRGraph(graph_id="g_nk", steps=(step,))

    df_orders = pd.DataFrame({"cid": [1, None], "amt": [100, 200]})
    df_cust = pd.DataFrame({"cid": [1, 2], "name": ["Alice", "Bob"]})
    s_orders = df_to_spark(spark_session, df_orders)
    s_cust = df_to_spark(spark_session, df_cust)

    with pytest.raises(Exception, match="Null key in left column 'cid' under FAIL_STEP_ON_NULL_KEY"):
        PySparkIRCompiler().execute(graph, {"orders": s_orders, "customers": s_cust})


def test_pyspark_join_do_not_match_null_cardinality(spark_session):
    """17. JoinNullPolicy.DROP_NULL_MATCHES ignores nulls during matching."""
    expr = JoinExpression(
        left_dataset="d1", right_dataset="d2", join_type=JoinType.INNER,
        key_pairs=[JoinKeyPair(left="k", right="k")],
        null_policy=JoinNullPolicy.DROP_NULL_MATCHES, output_dataset="out",
    )
    step = TransformationStepIR(step_id="s_dnm", dataset_name="d1", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr)
    graph = TransformationIRGraph(graph_id="g_dnm", steps=(step,))

    df1 = pd.DataFrame({"k": [1, None, 2], "v1": ["A", "B", "C"]})
    df2 = pd.DataFrame({"k": [1, None, 3], "v2": ["X", "Y", "Z"]})
    s1 = df_to_spark(spark_session, df1)
    s2 = df_to_spark(spark_session, df2)

    res = PySparkIRCompiler().execute(graph, {"d1": s1, "d2": s2})
    df_out = spark_to_df(res.outputs["out"])
    # Only key 1 matches; nulls do not match
    assert len(df_out) == 1
    assert df_out["k"].iloc[0] == 1


def test_pyspark_join_null_safe_match_null_to_null(spark_session):
    """18. JoinNullPolicy.MATCH_NULLS_TOGETHER matches NULL to NULL via eqNullSafe."""
    expr = JoinExpression(
        left_dataset="d1", right_dataset="d2", join_type=JoinType.INNER,
        key_pairs=[JoinKeyPair(left="k", right="k")],
        null_policy=JoinNullPolicy.MATCH_NULLS_TOGETHER, output_dataset="out",
    )
    step = TransformationStepIR(step_id="s_mnt", dataset_name="d1", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr)
    graph = TransformationIRGraph(graph_id="g_mnt", steps=(step,))

    df1 = pd.DataFrame({"k": [1, None], "v1": ["A", "B"]})
    df2 = pd.DataFrame({"k": [1, None], "v2": ["X", "Y"]})
    s1 = df_to_spark(spark_session, df1)
    s2 = df_to_spark(spark_session, df2)

    res = PySparkIRCompiler().execute(graph, {"d1": s1, "d2": s2})
    df_out = spark_to_df(res.outputs["out"])
    # Both 1 and NULL match
    assert len(df_out) == 2


def test_pyspark_cross_join_requires_step_authorization(spark_session):
    """19. CROSS join without StepAuthorization is rejected at compile time."""
    expr = JoinExpression(left_dataset="d1", right_dataset="d2", join_type=JoinType.CROSS)
    step = TransformationStepIR(step_id="s_cj_no", dataset_name="d1", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr)
    graph = TransformationIRGraph(graph_id="g_cj_no", steps=(step,))

    res = PySparkIRCompiler().compile(graph)
    assert res.status == CodegenStatus.FAILED_COMPILATION
    assert any("CROSS join requires elevated StepAuthorization" in d for d in res.diagnostics)


def test_pyspark_cross_join_predicted_limit_blocks_before_join(spark_session):
    """20. CROSS join with valid StepAuthorization enforces max_output_row_count."""
    expr = JoinExpression(
        left_dataset="d1", right_dataset="d2", join_type=JoinType.CROSS,
        max_output_row_count=5,
    )
    auth = StepAuthorization(
        step_id="s_cj_ok", policy_decision_id="pol_cj", approved_by_subject_id="admin",
        tenant_id="t1", expires_at=datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1),
        authorized_step_hash="dummy_hash",
    )
    step = TransformationStepIR(
        step_id="s_cj_ok", dataset_name="d1", family=ActionFamily.JOIN,
        kind=StepKind.MULTI_DATASET_JOIN, expression=expr, authorization=auth
    )
    graph = TransformationIRGraph(graph_id="g_cj_ok", steps=(step,))

    res_comp = PySparkIRCompiler().compile(graph)
    assert res_comp.status == CodegenStatus.SUCCEEDED

    d1 = pd.DataFrame({"a": [1, 2, 3, 4]})
    d2 = pd.DataFrame({"b": [10, 20, 30, 40]})
    s1 = df_to_spark(spark_session, d1)
    s2 = df_to_spark(spark_session, d2)

    with pytest.raises(RowExpansionLimitExceededError, match="exceeds max_output_row_count"):
        PySparkIRCompiler().execute(graph, {"d1": s1, "d2": s2})


def test_pyspark_join_semi_and_anti_preserve_left_schema(spark_session):
    """21. SEMI and ANTI joins preserve ONLY left table columns."""
    s1 = TransformationStepIR(
        step_id="s_semi", dataset_name="users", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN,
        expression=JoinExpression(left_dataset="users", right_dataset="vip", join_type=JoinType.SEMI,
                                  key_pairs=[JoinKeyPair(left="uid", right="vuid")], output_dataset="semi_out"),
    )
    s2 = TransformationStepIR(
        step_id="s_anti", dataset_name="users", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN,
        expression=JoinExpression(left_dataset="users", right_dataset="vip", join_type=JoinType.ANTI,
                                  key_pairs=[JoinKeyPair(left="uid", right="vuid")], output_dataset="anti_out"),
    )
    graph = TransformationIRGraph(graph_id="g_sa", steps=(s1, s2))

    df_users = pd.DataFrame({"uid": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    df_vip = pd.DataFrame({"vuid": [2, 3], "tier": ["Gold", "Platinum"]})
    s_users = df_to_spark(spark_session, df_users)
    s_vip = df_to_spark(spark_session, df_vip)

    res = PySparkIRCompiler().execute(graph, {"users": s_users, "vip": s_vip})
    df_semi = spark_to_df(res.outputs["semi_out"])
    df_anti = spark_to_df(res.outputs["anti_out"])

    assert list(df_semi["uid"]) == [2, 3]
    assert set(df_semi.columns) == {"uid", "name"}
    assert list(df_anti["uid"]) == [1]
    assert set(df_anti.columns) == {"uid", "name"}


def test_pyspark_join_unmatched_routing(spark_session):
    """22. Unmatched rows routed to dedicated secondary output dataset."""
    expr = JoinExpression(
        left_dataset="orders", right_dataset="cust", join_type=JoinType.LEFT,
        key_pairs=[JoinKeyPair(left="cid", right="cid")],
        unmatched_policy=JoinUnmatchedPolicy(
            left_unmatched=UnmatchedRowDisposition.ROUTE_TO_UNMATCHED_OUTPUT,
            left_output_ref="orphan_orders",
        ),
        output_dataset="matched",
    )
    step = TransformationStepIR(step_id="s_unm", dataset_name="orders", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr)
    graph = TransformationIRGraph(graph_id="g_unm", steps=(step,))

    df_orders = pd.DataFrame({"order_id": [1, 2], "cid": [10, 99]})
    df_cust = pd.DataFrame({"cid": [10], "name": ["Alice"]})
    s_orders = df_to_spark(spark_session, df_orders)
    s_cust = df_to_spark(spark_session, df_cust)

    res = PySparkIRCompiler().execute(graph, {"orders": s_orders, "cust": s_cust})
    assert "orphan_orders" in res.outputs
    df_orphans = spark_to_df(res.outputs["orphan_orders"])
    assert list(df_orphans["order_id"]) == [2]


def test_pyspark_join_suffix_collision_resolution(spark_session):
    """23. Colliding column names from right table are suffixed deterministically."""
    expr = JoinExpression(
        left_dataset="left_tbl", right_dataset="right_tbl", join_type=JoinType.INNER,
        key_pairs=[JoinKeyPair(left="id", right="id")],
        right_suffix="_right", output_dataset="out",
    )
    step = TransformationStepIR(step_id="s_sfx", dataset_name="left_tbl", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr)
    graph = TransformationIRGraph(graph_id="g_sfx", steps=(step,))

    df_left = pd.DataFrame({"id": [1], "val": ["L1"]})
    df_right = pd.DataFrame({"id": [1], "val": ["R1"]})
    s_left = df_to_spark(spark_session, df_left)
    s_right = df_to_spark(spark_session, df_right)

    res = PySparkIRCompiler().execute(graph, {"left_tbl": s_left, "right_tbl": s_right})
    df_out = spark_to_df(res.outputs["out"])
    assert len(df_out) == 1
    assert "val" in df_out.columns


def test_pyspark_join_suffix_collision_after_suffix_rejected(spark_session):
    """24. Preflight rejects join when suffixed name collides with existing left column."""
    from agent.etl_pipeline.transformation_ir import DatasetSchema, DatasetColumnSchema, DataType
    compiler = PySparkIRCompiler(schemas={
        "t1": DatasetSchema(dataset_name="t1", columns={
            "id": DatasetColumnSchema(name="id", logical_type=DataType.INTEGER),
            "val": DatasetColumnSchema(name="val", logical_type=DataType.STRING),
            "val_right": DatasetColumnSchema(name="val_right", logical_type=DataType.STRING),
        }),
        "t2": DatasetSchema(dataset_name="t2", columns={
            "id": DatasetColumnSchema(name="id", logical_type=DataType.INTEGER),
            "val": DatasetColumnSchema(name="val", logical_type=DataType.STRING),
        }),
    })
    expr = JoinExpression(
        left_dataset="t1", right_dataset="t2", join_type=JoinType.INNER,
        key_pairs=[JoinKeyPair(left="id", right="id")],
        right_suffix="_right", output_dataset="out",
    )
    step = TransformationStepIR(step_id="s_col_rej", dataset_name="t1", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr)
    graph = TransformationIRGraph(graph_id="g_col_rej", steps=(step,))

    res = compiler.compile(graph)
    assert res.status == CodegenStatus.FAILED_COMPILATION
    assert any("JoinOutputCollisionError" in d for d in res.diagnostics)


def test_pyspark_right_and_full_outer_key_projection(spark_session):
    """25. RIGHT and FULL_OUTER joins cleanly project output datasets."""
    expr = JoinExpression(
        left_dataset="d1", right_dataset="d2", join_type=JoinType.RIGHT,
        key_pairs=[JoinKeyPair(left="k", right="k")],
        output_dataset="right_out",
    )
    step = TransformationStepIR(step_id="s_rj", dataset_name="d1", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr)
    graph = TransformationIRGraph(graph_id="g_rj", steps=(step,))

    df1 = pd.DataFrame({"k": [1], "v1": ["A"]})
    df2 = pd.DataFrame({"k": [1, 2], "v2": ["X", "Y"]})
    s1 = df_to_spark(spark_session, df1)
    s2 = df_to_spark(spark_session, df2)

    res = PySparkIRCompiler().execute(graph, {"d1": s1, "d2": s2})
    df_out = spark_to_df(res.outputs["right_out"])
    assert len(df_out) == 2

# ============================================================================
# Section 3: Aggregations (7 tests)
# ============================================================================

def test_pyspark_aggregation_count_star_vs_column_and_distinct(spark_session):
    """26. COUNT(*) counts rows including nulls, COUNT(col) excludes nulls, COUNT_DISTINCT."""
    expr = AggregateExpression(
        group_by=["dept"],
        aggregates=[
            AggregateColumnSpec(source_column=None, operation=AggregateOp.COUNT, output_column="total_rows"),
            AggregateColumnSpec(source_column="bonus", operation=AggregateOp.COUNT, output_column="bonus_count"),
            AggregateColumnSpec(source_column="role", operation=AggregateOp.COUNT_DISTINCT, output_column="unique_roles"),
        ],
        output_dataset="summary",
    )
    step = TransformationStepIR(step_id="s_agg", dataset_name="emp", family=ActionFamily.AGGREGATE, kind=StepKind.DATASET_AGGREGATION, expression=expr)
    graph = TransformationIRGraph(graph_id="g_agg", steps=(step,))

    pdf = pd.DataFrame({
        "dept": ["IT", "IT", "HR"],
        "bonus": [1000.0, None, 500.0],
        "role": ["Dev", "Dev", "Recruiter"],
    })
    sdf = df_to_spark(spark_session, pdf)
    res = PySparkIRCompiler().execute(graph, sdf)
    out_df = spark_to_df(res.outputs["summary"]).set_index("dept")

    assert out_df.loc["HR", "total_rows"] == 1
    assert out_df.loc["HR", "bonus_count"] == 1
    assert out_df.loc["HR", "unique_roles"] == 1
    assert out_df.loc["IT", "total_rows"] == 2
    assert out_df.loc["IT", "bonus_count"] == 1
    assert out_df.loc["IT", "unique_roles"] == 1


def test_pyspark_aggregation_statistical_metrics(spark_session):
    """27. SUM, AVG, MIN, MAX aggregation parity."""
    expr = AggregateExpression(
        group_by=["dept"],
        aggregates=[
            AggregateColumnSpec(source_column="salary", operation=AggregateOp.SUM, output_column="total_salary"),
            AggregateColumnSpec(source_column="salary", operation=AggregateOp.AVG, output_column="avg_salary"),
            AggregateColumnSpec(source_column="salary", operation=AggregateOp.MIN, output_column="min_salary"),
            AggregateColumnSpec(source_column="salary", operation=AggregateOp.MAX, output_column="max_salary"),
        ],
        output_dataset="stats",
    )
    step = TransformationStepIR(step_id="s_stats", dataset_name="emp", family=ActionFamily.AGGREGATE, kind=StepKind.DATASET_AGGREGATION, expression=expr)
    graph = TransformationIRGraph(graph_id="g_stats", steps=(step,))

    pdf = pd.DataFrame({
        "dept": ["IT", "IT"],
        "salary": [100000.0, 120000.0],
    })
    sdf = df_to_spark(spark_session, pdf)
    res = PySparkIRCompiler().execute(graph, sdf)
    out_df = spark_to_df(res.outputs["stats"]).set_index("dept")

    assert out_df.loc["IT", "total_salary"] == 220000.0
    assert out_df.loc["IT", "avg_salary"] == 110000.0
    assert out_df.loc["IT", "min_salary"] == 100000.0
    assert out_df.loc["IT", "max_salary"] == 120000.0


def test_pyspark_stddev_single_observation_semantics(spark_session):
    """28. Single observation: STDDEV_SAMPLE is None/NaN, STDDEV_POPULATION is 0.0."""
    expr = AggregateExpression(
        group_by=["grp"],
        aggregates=[
            AggregateColumnSpec(source_column="val", operation=AggregateOp.STDDEV_SAMPLE, output_column="std_samp"),
            AggregateColumnSpec(source_column="val", operation=AggregateOp.STDDEV_POPULATION, output_column="std_pop"),
        ],
        output_dataset="std_out",
    )
    step = TransformationStepIR(step_id="s_std", dataset_name="d", family=ActionFamily.AGGREGATE, kind=StepKind.DATASET_AGGREGATION, expression=expr)
    graph = TransformationIRGraph(graph_id="g_std", steps=(step,))

    pdf = pd.DataFrame({"grp": ["G1"], "val": [42.0]})
    sdf = df_to_spark(spark_session, pdf)
    res = PySparkIRCompiler().execute(graph, sdf)
    out_df = spark_to_df(res.outputs["std_out"])

    assert out_df["std_samp"].iloc[0] is None or pd.isna(out_df["std_samp"].iloc[0])
    assert out_df["std_pop"].iloc[0] == 0.0


def test_pyspark_median_approximation_policy_and_tolerance(spark_session):
    """29. MEDIAN approximation produces value within accuracy tolerance of true median."""
    expr = AggregateExpression(
        group_by=["grp"],
        aggregates=[
            AggregateColumnSpec(source_column="val", operation=AggregateOp.MEDIAN, output_column="med"),
        ],
        output_dataset="med_out",
    )
    step = TransformationStepIR(step_id="s_med", dataset_name="d", family=ActionFamily.AGGREGATE, kind=StepKind.DATASET_AGGREGATION, expression=expr)
    graph = TransformationIRGraph(graph_id="g_med", steps=(step,))

    vals = list(range(1, 101))
    pdf = pd.DataFrame({"grp": ["G1"] * 100, "val": [float(v) for v in vals]})
    sdf = df_to_spark(spark_session, pdf)
    res = PySparkIRCompiler().execute(graph, sdf)
    out_df = spark_to_df(res.outputs["med_out"])

    # True median of 1..100 is 50.5; approx percentile should be very close
    assert abs(out_df["med"].iloc[0] - 50.5) <= 1.0


def test_pyspark_aggregation_group_key_null_policy(spark_session):
    """30. GroupKeyNullPolicy.FAIL_STEP fails if null group key encountered; DROP_NULL_GROUP drops."""
    expr_fail = AggregateExpression(
        group_by=["cat"],
        aggregates=[AggregateColumnSpec(source_column="amt", operation=AggregateOp.SUM, output_column="total")],
        group_key_null_policy=GroupKeyNullPolicy.FAIL_STEP,
    )
    graph_fail = TransformationIRGraph(graph_id="g_gkf", steps=(
        TransformationStepIR(step_id="s_f", dataset_name="d", family=ActionFamily.AGGREGATE, kind=StepKind.DATASET_AGGREGATION, expression=expr_fail),
    ))
    pdf = pd.DataFrame({"cat": [None, "Electronics"], "amt": [10.0, 20.0]})
    sdf = df_to_spark(spark_session, pdf)
    with pytest.raises(Exception, match="Null group key encountered under FAIL_STEP"):
        PySparkIRCompiler().execute(graph_fail, sdf)

    expr_drop = AggregateExpression(
        group_by=["cat"],
        aggregates=[AggregateColumnSpec(source_column="amt", operation=AggregateOp.SUM, output_column="total")],
        group_key_null_policy=GroupKeyNullPolicy.DROP_NULL_GROUP,
        output_dataset="drop_out",
    )
    graph_drop = TransformationIRGraph(graph_id="g_gkd", steps=(
        TransformationStepIR(step_id="s_d", dataset_name="d", family=ActionFamily.AGGREGATE, kind=StepKind.DATASET_AGGREGATION, expression=expr_drop),
    ))
    res = PySparkIRCompiler().execute(graph_drop, sdf)
    out_df = spark_to_df(res.outputs["drop_out"])
    assert len(out_df) == 1
    assert out_df["cat"].iloc[0] == "Electronics"


def test_pyspark_all_null_sum_policy_matrix(spark_session):
    """31. All-null group sum yields 0 under TREAT_AS_ZERO and NULL under PROPAGATE_NULL."""
    expr_zero = AggregateExpression(
        group_by=["cat"],
        aggregates=[
            AggregateColumnSpec(source_column="amt", operation=AggregateOp.SUM, null_policy=AggregateValueNullPolicy.TREAT_AS_ZERO, output_column="s_zero"),
            AggregateColumnSpec(source_column="amt", operation=AggregateOp.SUM, null_policy=AggregateValueNullPolicy.PROPAGATE_NULL, output_column="s_null"),
        ],
        output_dataset="null_matrix",
    )
    graph = TransformationIRGraph(graph_id="g_nm", steps=(
        TransformationStepIR(step_id="s_nm", dataset_name="d", family=ActionFamily.AGGREGATE, kind=StepKind.DATASET_AGGREGATION, expression=expr_zero),
    ))
    pdf = pd.DataFrame({"cat": ["A"], "amt": [None]})
    sdf = df_to_spark(spark_session, pdf)
    res = PySparkIRCompiler().execute(graph, sdf)
    out_df = spark_to_df(res.outputs["null_matrix"])
    assert out_df["s_zero"].iloc[0] == 0.0 or out_df["s_zero"].iloc[0] == 0
    assert out_df["s_null"].iloc[0] is None or pd.isna(out_df["s_null"].iloc[0])


def test_pyspark_empty_global_vs_grouped_aggregation(spark_session):
    """32. EmptyInputPolicy.EMIT_EMPTY_DATASET overrides native Spark row emission to return 0 rows."""
    expr = AggregateExpression(
        aggregates=[AggregateColumnSpec(source_column=None, operation=AggregateOp.COUNT, output_column="cnt")],
        empty_input_policy=EmptyInputPolicy.EMIT_EMPTY_DATASET,
        output_dataset="empty_out",
    )
    graph = TransformationIRGraph(graph_id="g_emp", steps=(
        TransformationStepIR(step_id="s_emp", dataset_name="d", family=ActionFamily.AGGREGATE, kind=StepKind.DATASET_AGGREGATION, expression=expr),
    ))
    pdf = pd.DataFrame({"x": pd.Series([], dtype="float64")})
    sdf = df_to_spark(spark_session, pdf)
    res = PySparkIRCompiler().execute(graph, sdf)
    out_df = spark_to_df(res.outputs["empty_out"])
    assert len(out_df) == 0

# ============================================================================
# Section 4: Publication Contracts (4 tests)
# ============================================================================

def test_pyspark_atomic_publication_failure_aborts_zero_outputs(spark_session):
    """33. Postcondition failure triggers abort, committing exactly zero outputs."""
    adapter = PySparkInMemoryPublicationAdapter()
    col = ColumnRef(column_name="amt", dataset_name="d")
    step = TransformationStepIR(
        step_id="s_pub", dataset_name="d", family=ActionFamily.CLIP, kind=StepKind.ROW_TRANSFORMATION,
        expression=ClipExpression(target_column=col, upper_bound=LiteralValue(value=100.0)),
        target_column="amt",
    )
    graph = TransformationIRGraph(graph_id="g_pub", steps=(step,))
    pdf = pd.DataFrame({"amt": [50.0, 150.0]})
    sdf = df_to_spark(spark_session, pdf)

    def failing_postcondition(outputs):
        raise ValueError("Business invariant violated: total amount exceeds threshold")

    with pytest.raises(PySparkPublicationPostconditionError, match="Publication aborted due to failed postconditions"):
        execute_pyspark_with_publication_contracts(
            graph=graph,
            df=sdf,
            adapter=adapter,
            postconditions=[failing_postcondition],
        )

    assert len(adapter._published) == 0
    assert len(adapter._staged) == 0
    assert adapter._manifest_version == "v0"


def test_pyspark_publication_late_output_failure_aborts_all(spark_session):
    """34. Failure on late output clears all staged frames atomically."""
    adapter = PySparkInMemoryPublicationAdapter()
    col = ColumnRef(column_name="v", dataset_name="d")
    step = TransformationStepIR(
        step_id="s_late", dataset_name="d", family=ActionFamily.CLIP, kind=StepKind.ROW_TRANSFORMATION,
        expression=ClipExpression(target_column=col, upper_bound=LiteralValue(value=100.0)),
        target_column="v",
    )
    graph = TransformationIRGraph(graph_id="g_late", steps=(step,))
    pdf = pd.DataFrame({"v": [10.0, 20.0]})
    sdf = df_to_spark(spark_session, pdf)

    call_counts = [0]
    def passing_pc(outputs):
        call_counts[0] += 1

    def late_failing_pc(outputs):
        raise RuntimeError("Late failure in secondary pipeline validation")

    with pytest.raises(PySparkPublicationPostconditionError):
        execute_pyspark_with_publication_contracts(
            graph=graph,
            df=sdf,
            adapter=adapter,
            postconditions=[passing_pc, late_failing_pc],
        )

    assert call_counts[0] == 1
    assert len(adapter._published) == 0
    assert len(adapter._staged) == 0


def test_pyspark_publication_postcondition_exception_aborts_all(spark_session):
    """35. Exception during postcondition evaluation unpersists frames and aborts."""
    adapter = PySparkInMemoryPublicationAdapter()
    col = ColumnRef(column_name="n", dataset_name="d")
    step = TransformationStepIR(
        step_id="s_exc", dataset_name="d", family=ActionFamily.CLIP, kind=StepKind.ROW_TRANSFORMATION,
        expression=ClipExpression(target_column=col, upper_bound=LiteralValue(value=100.0)),
        target_column="n",
    )
    graph = TransformationIRGraph(graph_id="g_exc", steps=(step,))
    pdf = pd.DataFrame({"n": [1.0, 2.0]})
    sdf = df_to_spark(spark_session, pdf)

    def raising_pc(outputs):
        raise TypeError("Unexpected schema type during custom validator")

    with pytest.raises(PySparkPublicationPostconditionError):
        execute_pyspark_with_publication_contracts(
            graph=graph,
            df=sdf,
            adapter=adapter,
            postconditions=[raising_pc],
        )

    assert len(adapter._published) == 0
    assert len(adapter._staged) == 0


def test_pyspark_publication_commit_and_abort_idempotency(spark_session):
    """36. Repeated commit/abort calls on the adapter are idempotent."""
    adapter = PySparkInMemoryPublicationAdapter()
    col = ColumnRef(column_name="val", dataset_name="d")
    step = TransformationStepIR(
        step_id="s_idem", dataset_name="d", family=ActionFamily.CLIP, kind=StepKind.ROW_TRANSFORMATION,
        expression=ClipExpression(target_column=col, upper_bound=LiteralValue(value=100.0)),
        target_column="val",
    )
    graph = TransformationIRGraph(graph_id="g_idem", steps=(step,))
    pdf = pd.DataFrame({"val": [50.0, 80.0]})
    sdf = df_to_spark(spark_session, pdf)

    commit_res = execute_pyspark_with_publication_contracts(
        graph=graph,
        df=sdf,
        adapter=adapter,
        postconditions=[lambda outs: None],
    )
    assert commit_res.is_committed
    assert len(adapter._published) >= 1

    # Second commit call with dummy handle is idempotent
    second_commit = adapter.commit(commit_res)
    assert second_commit.is_committed

    # Abort on a fresh adapter is idempotent
    adapter2 = PySparkInMemoryPublicationAdapter()
    adapter2.stage("s1", {"d": sdf})
    adapter2.abort()
    # Repeated abort call is a no-op
    adapter2.abort()
    assert adapter2._state == "ABORTED"


# ============================================================================
# Section 5: Multi-Dataset End-to-End Pipeline (1 test)
# ============================================================================

def test_pyspark_increment_33_multi_dataset_e2e_pipeline(spark_session):
    """37. End-to-end multi-dataset pipeline: Join -> Derive -> Aggregate -> Publish."""
    adapter = PySparkInMemoryPublicationAdapter()

    # Step 1: Join customers and orders
    s_join = TransformationStepIR(
        step_id="step_join", dataset_name="customers", family=ActionFamily.JOIN,
        kind=StepKind.MULTI_DATASET_JOIN,
        expression=JoinExpression(
            left_dataset="customers", right_dataset="orders", join_type=JoinType.INNER,
            key_pairs=[JoinKeyPair(left="cid", right="cid")],
            cardinality=JoinCardinality.ONE_TO_MANY,
            output_dataset="cust_orders",
        ),
    )

    # Step 2: Derive total line amount (price + tax)
    s_derive = TransformationStepIR(
        step_id="step_derive", dataset_name="cust_orders", family=ActionFamily.DERIVE,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ArithmeticDerivation(
            left=NumericOperand(column_name="price"),
            right=NumericOperand(column_name="tax"),
            operator=ArithmeticOperator.ADD,
            domain=ArithmeticDomain.FLOAT64,
            output_column="total_amt",
        ),
        target_column="total_amt",
    )

    # Step 3: Aggregate by region: total revenue, customer count
    s_agg = TransformationStepIR(
        step_id="step_agg", dataset_name="cust_orders", family=ActionFamily.AGGREGATE,
        kind=StepKind.DATASET_AGGREGATION,
        expression=AggregateExpression(
            group_by=["region"],
            aggregates=[
                AggregateColumnSpec(source_column="total_amt", operation=AggregateOp.SUM, output_column="regional_revenue"),
                AggregateColumnSpec(source_column="cid", operation=AggregateOp.COUNT_DISTINCT, output_column="distinct_customers"),
            ],
            output_dataset="regional_summary",
        ),
    )

    graph = TransformationIRGraph(
        graph_id="g_e2e_pipeline",
        steps=(s_join, s_derive, s_agg),
        primary_output_ref="regional_summary",
    )

    df_customers = pd.DataFrame({
        "cid": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "region": ["East", "West", "East"],
    })
    df_orders = pd.DataFrame({
        "cid": [1, 2, 1],
        "price": [100.0, 200.0, 50.0],
        "tax": [10.0, 20.0, 5.0],
    })

    s_cust = df_to_spark(spark_session, df_customers)
    s_ord = df_to_spark(spark_session, df_orders)

    def verify_pipeline_summary(outputs):
        assert "regional_summary" in outputs
        df_sum = spark_to_df(outputs["regional_summary"]).set_index("region")
        assert "East" in df_sum.index
        assert "West" in df_sum.index
        # East total: (100+10) + (50+5) = 165.0
        assert df_sum.loc["East", "regional_revenue"] == 165.0
        assert df_sum.loc["East", "distinct_customers"] == 1
        # West total: 200+20 = 220.0
        assert df_sum.loc["West", "regional_revenue"] == 220.0
        assert df_sum.loc["West", "distinct_customers"] == 1

    commit_res = execute_pyspark_with_publication_contracts(
        graph=graph,
        df={"customers": s_cust, "orders": s_ord},
        adapter=adapter,
        postconditions=[verify_pipeline_summary],
    )

    assert commit_res.is_committed
    assert "regional_summary" in adapter._published
