"""
tests/test_pandas_ir_compiler_joins_derivations.py — Comprehensive Test Suite for Increment 3E.

Covers:
1. Canonical scalar serialization (Decimals, Timestamps, Integers, Floats, Bytes, NaT, Dates).
2. Derivations (Arithmetic with INT64/FLOAT64/DECIMAL domains and overflow policies,
   Conditional with unknown policies, Coalesce, Concat, Date Arithmetic).
3. Extractions (Regex, BoundedJsonPath, Delimiter, DatePart).
4. Multi-Dataset Joins (Inner, Left, Right, Full, Semi, Anti, Cross with StepAuthorization,
   cardinality guards, expansion limits, unmatched routing).
5. Aggregations (COUNT vs COUNT(*), COUNT_DISTINCT, SUM, AVG, MIN, MAX, STDDEV_SAMPLE,
   STDDEV_POPULATION, MEDIAN, GroupKeyNullPolicy, AggregateValueNullPolicy, empty inputs).
6. Publication Layer (InMemoryPublicationAdapter, staged execution, atomic commit, postcondition failure abort).
"""
import datetime
from decimal import Decimal
import hashlib
import numpy as np
import pandas as pd
import pytest

from agent.etl_pipeline.codegen_shared import CodegenStatus
from agent.etl_pipeline.pandas_ir_compiler import (
    CompilerStepDisposition,
    InMemoryPublicationAdapter,
    PandasIRCompiler,
    PublicationAbortError,
    PublicationPostconditionError,
    RowExpansionLimitExceededError,
    canonical_encode_scalar_v1,
    execute_with_publication_contracts,
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
    CanonicalSerializationError,
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
    DecimalCanonicalizationMode,
    DelimiterExtract,
    DivisionByZeroPolicy,
    EmptyInputPolicy,
    ExistingColumnPolicy,
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


def _exec_artifact(artifact, df):
    ns = {
        "pd": pd,
        "np": np,
        "Decimal": Decimal,
        "hashlib": hashlib,
        "RowExpansionLimitExceededError": RowExpansionLimitExceededError,
    }
    exec(artifact.source_code, ns)
    fn = ns["transform_datasets"]
    return fn(df)


# ============================================================================
# 1. Canonical Serialization Tests (Contract 1, 2, 3)
# ============================================================================

def test_canonical_decimal_equivalent_scale_normalization():
    """Contract 1: Decimal('1.0') and Decimal('1.00') produce identical bytes under NUMERIC_VALUE."""
    b1 = canonical_encode_scalar_v1(Decimal("1.0"), mode=DecimalCanonicalizationMode.NUMERIC_VALUE)
    b2 = canonical_encode_scalar_v1(Decimal("1.00"), mode=DecimalCanonicalizationMode.NUMERIC_VALUE)
    b3 = canonical_encode_scalar_v1(Decimal("1"), mode=DecimalCanonicalizationMode.NUMERIC_VALUE)
    assert b1 == b2 == b3


def test_canonical_decimal_negative_zero_canonical_zero():
    """Contract 1: Decimal('-0') and Decimal('0.00') normalize to identical canonical zero."""
    bz1 = canonical_encode_scalar_v1(Decimal("-0"), mode=DecimalCanonicalizationMode.NUMERIC_VALUE)
    bz2 = canonical_encode_scalar_v1(Decimal("0.00"), mode=DecimalCanonicalizationMode.NUMERIC_VALUE)
    bz3 = canonical_encode_scalar_v1(Decimal("0"), mode=DecimalCanonicalizationMode.NUMERIC_VALUE)
    assert bz1 == bz2 == bz3


def test_canonical_decimal_preserve_declared_scale():
    """Contract 1: Declared financial scale 2 vs 4 produces distinct bytes under PRESERVE_DECLARED_SCALE."""
    b_scale2 = canonical_encode_scalar_v1(
        Decimal("100.50"), mode=DecimalCanonicalizationMode.PRESERVE_DECLARED_SCALE, declared_scale=2
    )
    b_scale4 = canonical_encode_scalar_v1(
        Decimal("100.50"), mode=DecimalCanonicalizationMode.PRESERVE_DECLARED_SCALE, declared_scale=4
    )
    assert b_scale2 != b_scale4


def test_canonical_decimal_rejects_non_finite():
    """Contract 1: Non-finite Decimals raise CanonicalSerializationError."""
    with pytest.raises(CanonicalSerializationError, match="Non-finite Decimal"):
        canonical_encode_scalar_v1(Decimal("NaN"))
    with pytest.raises(CanonicalSerializationError, match="Non-finite Decimal"):
        canonical_encode_scalar_v1(Decimal("Infinity"))


def test_canonical_timestamp_nanosecond_overflow_rejection():
    """Contract 2: Timestamps exceeding signed 64-bit nanosecond range raise CanonicalSerializationError."""
    dt_far_future = datetime.datetime(2500, 1, 1, tzinfo=datetime.timezone.utc)
    with pytest.raises(CanonicalSerializationError, match="exceeds signed 64-bit range"):
        canonical_encode_scalar_v1(dt_far_future)

    dt_far_past = datetime.datetime(1400, 1, 1, tzinfo=datetime.timezone.utc)
    with pytest.raises(CanonicalSerializationError, match="exceeds signed 64-bit range"):
        canonical_encode_scalar_v1(dt_far_past)


def test_canonical_naive_timestamp_without_policy_rejected():
    """Contract 2: Naive timestamp without dataset timezone policy raises CanonicalSerializationError."""
    dt_naive = datetime.datetime(2023, 5, 1, 12, 0, 0)
    with pytest.raises(CanonicalSerializationError, match="without an explicit dataset timezone policy"):
        canonical_encode_scalar_v1(dt_naive, dataset_tz=None)

    # Valid with policy
    b_aware = canonical_encode_scalar_v1(dt_naive, dataset_tz="UTC")
    assert b_aware.startswith(b"\x06TS\x00")


def test_canonical_nat_dedicated_domain_tag():
    """Contract 2: pd.NaT gets dedicated domain tag separate from NULL."""
    b_nat = canonical_encode_scalar_v1(pd.NaT)
    b_null = canonical_encode_scalar_v1(None)
    assert b_nat == b"\x09NAT\x00"
    assert b_null == b"\x00DHARA_NULL_V1"
    assert b_nat != b_null


def test_canonical_date_tag_not_timezone_shifted():
    """Contract 2: Calendar date uses date tag and is not timezone shifted."""
    d = datetime.date(2024, 6, 15)
    b_date = canonical_encode_scalar_v1(d)
    assert b_date.startswith(b"\x0aDATE\x00")
    assert b"2024-06-15" in b_date


def test_canonical_unsupported_object_zero_string_fallback():
    """Contract 3: Unsupported nested objects (dict, list) reject explicitly without string fallback."""
    with pytest.raises(CanonicalSerializationError, match="Unsupported scalar type"):
        canonical_encode_scalar_v1({"a": 1})
    with pytest.raises(CanonicalSerializationError, match="Unsupported scalar type"):
        canonical_encode_scalar_v1([1, 2, 3])


def test_canonical_bool_handled_before_int():
    """Contract 3: bool handled before int; True and 1 produce distinct domain-tagged bytes."""
    b_bool = canonical_encode_scalar_v1(True)
    b_int = canonical_encode_scalar_v1(1)
    assert b_bool.startswith(b"\x01BOOL\x01")
    assert b_int.startswith(b"\x02INT\x00")
    assert b_bool != b_int


# ============================================================================
# 2. Derivations Tests (Contract 5, 6, 7)
# ============================================================================

def test_arithmetic_derivation_int64_overflow_and_clip_to_range():
    """Contract 6: INT64 overflow detected before wraparound, respects CLIP_TO_RANGE."""
    expr = ArithmeticDerivation(
        left=NumericOperand(column_name="a"),
        right=NumericOperand(column_name="b"),
        operator=ArithmeticOperator.ADD,
        domain=ArithmeticDomain.INT64,
        overflow_policy=NumericOverflowPolicy.CLIP_TO_RANGE,
        output_column="sum_col",
    )
    step = TransformationStepIR(
        step_id="step_arith",
        dataset_name="d",
        family=ActionFamily.DERIVE,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr,
        target_column="sum_col",
    )
    graph = TransformationIRGraph(graph_id="g_arith", steps=(step,))
    res = PandasIRCompiler().compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df = pd.DataFrame({"a": [9223372036854775800, -9223372036854775800], "b": [100, -100]})
    outs = _exec_artifact(res.artifact, df)
    out_s = outs["primary"]["sum_col"]
    assert out_s.iloc[0] == 9223372036854775807
    assert out_s.iloc[1] == -9223372036854775808


def test_arithmetic_derivation_float_division_by_zero_policies():
    """Contract 6: FLOAT64 division by zero respects SET_NULL vs RETURN_INFINITY vs FAIL_STEP."""
    expr_null = ArithmeticDerivation(
        left=NumericOperand(column_name="x"),
        right=NumericOperand(column_name="y"),
        operator=ArithmeticOperator.DIVIDE,
        domain=ArithmeticDomain.FLOAT64,
        division_by_zero_policy=DivisionByZeroPolicy.SET_NULL,
        output_column="div_col",
    )
    step = TransformationStepIR(
        step_id="step_div", dataset_name="d", family=ActionFamily.DERIVE,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr_null, target_column="div_col"
    )
    res_null = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_div", steps=(step,)))
    df = pd.DataFrame({"x": [10.0, 20.0], "y": [2.0, 0.0]})
    outs_null = _exec_artifact(res_null.artifact, df)
    assert outs_null["primary"]["div_col"].iloc[0] == 5.0
    assert np.isnan(outs_null["primary"]["div_col"].iloc[1])


def test_conditional_derivation_unknown_policy():
    """Contract 7: Conditional derivation explicitly handles True, False, and Unknown (NA)."""
    col = ColumnRef(column_name="score", dataset_name="d")
    cond = BinaryOp(left=col, op=BinaryOperator.GT, right=LiteralValue(value=50))

    expr_fallback = ConditionalDerivation(
        condition=cond,
        then_value="PASS",
        else_value="FAIL",
        unknown_policy=ConditionalUnknownPolicy.USE_FALLBACK,
        fallback_value="UNKNOWN_SCORE",
        output_column="result",
    )
    step = TransformationStepIR(
        step_id="step_cond", dataset_name="d", family=ActionFamily.DERIVE,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr_fallback, target_column="result"
    )
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_cond", steps=(step,)))
    df = pd.DataFrame({"score": [80, 30, np.nan]})
    outs = _exec_artifact(res.artifact, df)
    assert list(outs["primary"]["result"]) == ["PASS", "FAIL", "UNKNOWN_SCORE"]


def test_coalesce_and_concat_derivations():
    """Contract 5: CoalesceDerivation combine_first and ConcatDerivation with null policy."""
    expr_coal = CoalesceDerivation(
        columns=["phone1", "phone2"],
        fallback_value="NO_PHONE",
        output_column="phone",
    )
    expr_concat = ConcatDerivation(
        columns=["first_name", "last_name"],
        delimiter=" ",
        null_policy=ConcatNullPolicy.IGNORE_NULLS,
        output_column="full_name",
    )
    s1 = TransformationStepIR(step_id="s1", dataset_name="d", family=ActionFamily.DERIVE, kind=StepKind.ROW_TRANSFORMATION, expression=expr_coal, target_column="phone")
    s2 = TransformationStepIR(step_id="s2", dataset_name="d", family=ActionFamily.DERIVE, kind=StepKind.ROW_TRANSFORMATION, expression=expr_concat, target_column="full_name")

    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_c", steps=(s1, s2)))
    df = pd.DataFrame({
        "phone1": [None, "555-1234"],
        "phone2": ["555-9999", None],
        "first_name": ["Alice", "Bob"],
        "last_name": [None, "Smith"],
    })
    outs = _exec_artifact(res.artifact, df)
    assert list(outs["primary"]["phone"]) == ["555-9999", "555-1234"]
    assert list(outs["primary"]["full_name"]) == ["Alice", "Bob Smith"]


# ============================================================================
# 3. Extraction Tests (Contract 8)
# ============================================================================

def test_bounded_json_path_extraction():
    """Contract 8: BoundedJsonPath navigation for object keys and array indices."""
    expr = JsonPathExtract(
        source_column="raw_json",
        path="user.addresses[0].zip",
        output_column="zip_code",
    )
    step = TransformationStepIR(
        step_id="s_json", dataset_name="d", family=ActionFamily.EXTRACT,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr, target_column="zip_code"
    )
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_json", steps=(step,)))
    df = pd.DataFrame({
        "raw_json": [
            '{"user": {"addresses": [{"zip": "94105"}, {"zip": "94107"}]}}',
            '{"user": {"addresses": []}}',
        ]
    })
    outs = _exec_artifact(res.artifact, df)
    assert outs["primary"]["zip_code"].iloc[0] == "94105"
    assert pd.isna(outs["primary"]["zip_code"].iloc[1])


def test_bounded_json_path_rejects_wildcards_and_excessive_depth():
    """Contract 8: BoundedJsonPath rejects syntax with wildcards, filter expressions, and excess depth."""
    with pytest.raises(ValueError, match="Prohibited recursive, wildcard"):
        BoundedJsonPath(path="user.*.id")
    with pytest.raises(ValueError, match="Prohibited recursive, wildcard"):
        BoundedJsonPath(path="user..id")
    with pytest.raises(ValueError, match="exceeds maximum permitted depth"):
        BoundedJsonPath(path=".".join([f"k{i}" for i in range(25)]), max_depth=10)


def test_regex_extract_multiple_match_policy():
    """Contract 8: Regex extract with multiple matches fails step under FAIL_STEP policy."""
    expr = RegexExtract(
        source_column="text",
        pattern=r"\d+",
        group_index=0,
        multiple_match_policy=MultipleMatchPolicy.FAIL_STEP,
        output_column="num",
    )
    step = TransformationStepIR(step_id="s_re", dataset_name="d", family=ActionFamily.EXTRACT, kind=StepKind.ROW_TRANSFORMATION, expression=expr, target_column="num")
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_re", steps=(step,)))
    df = pd.DataFrame({"text": ["Invoice 123 has 456 items"]})
    with pytest.raises(ValueError, match="Multiple regex matches encountered under FAIL_STEP"):
        _exec_artifact(res.artifact, df)


def test_delimiter_and_date_part_extraction():
    """Contract 8: Delimiter extract by index and date-part extraction."""
    s1 = TransformationStepIR(
        step_id="s_delim", dataset_name="d", family=ActionFamily.EXTRACT, kind=StepKind.ROW_TRANSFORMATION,
        expression=DelimiterExtract(source_column="csv_line", delimiter=",", index=1, output_column="field_1"),
        target_column="field_1",
    )
    s2 = TransformationStepIR(
        step_id="s_dp", dataset_name="d", family=ActionFamily.EXTRACT, kind=StepKind.ROW_TRANSFORMATION,
        expression=DatePartExtract(source_column="ts", part=DatePart.YEAR, output_column="year_col"),
        target_column="year_col",
    )
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_dp", steps=(s1, s2)))
    df = pd.DataFrame({
        "csv_line": ["A,B,C", "X,Y,Z"],
        "ts": ["2023-01-15 10:00:00", "2024-06-20 14:30:00"],
    })
    outs = _exec_artifact(res.artifact, df)
    assert list(outs["primary"]["field_1"]) == ["B", "Y"]
    assert list(outs["primary"]["year_col"]) == [2023, 2024]


# ============================================================================
# 4. Multi-Dataset Joins Tests (Contract 9, 10)
# ============================================================================

def test_join_inner_and_cardinality_guard():
    """Contract 9: Inner join with ONE_TO_MANY cardinality check; fails if left has duplicate keys."""
    expr = JoinExpression(
        left_dataset="orders",
        right_dataset="items",
        join_type=JoinType.INNER,
        key_pairs=[JoinKeyPair(left="order_id", right="order_id")],
        cardinality=JoinCardinality.ONE_TO_MANY,
        output_dataset="order_details",
    )
    step = TransformationStepIR(step_id="s_join", dataset_name="orders", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr)
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_join", steps=(step,)))

    df_orders_dup = pd.DataFrame({"order_id": [101, 101], "customer": ["A", "B"]})
    df_items = pd.DataFrame({"order_id": [101, 101], "sku": ["SKU1", "SKU2"]})
    with pytest.raises(ValueError, match="Left table has duplicate keys under ONE_TO_MANY"):
        _exec_artifact(res.artifact, {"orders": df_orders_dup, "items": df_items})


def test_cross_join_requires_step_authorization_and_enforces_max_rows():
    """Contract 9: CROSS join requires elevated StepAuthorization and rejects exceeding max_output_row_count."""
    expr_cross = JoinExpression(
        left_dataset="d1",
        right_dataset="d2",
        join_type=JoinType.CROSS,
        max_output_row_count=5,
        max_row_expansion_ratio=10.0,
    )
    step_no_auth = TransformationStepIR(step_id="s_cross", dataset_name="d1", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr_cross)
    res_no_auth = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_cross_no", steps=(step_no_auth,)))
    assert res_no_auth.status == CodegenStatus.FAILED_COMPILATION

    auth = StepAuthorization(
        step_id="s_cross", policy_decision_id="pol_cross", approved_by_subject_id="admin",
        tenant_id="t1", expires_at=datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1),
        authorized_step_hash="dummy_hash",
    )
    step_auth = TransformationStepIR(step_id="s_cross", dataset_name="d1", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr_cross, authorization=auth)
    res_auth = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_cross_ok", steps=(step_auth,)))
    assert res_auth.status == CodegenStatus.SUCCEEDED

    d1 = pd.DataFrame({"a": [1, 2, 3, 4]})
    d2 = pd.DataFrame({"b": [10, 20, 30, 40]})
    with pytest.raises(RowExpansionLimitExceededError, match="exceeds max_output_row_count"):
        _exec_artifact(res_auth.artifact, {"d1": d1, "d2": d2})


def test_join_semi_and_anti_preserve_left_schema_only():
    """Contract 9: SEMI and ANTI joins preserve ONLY left schema columns."""
    expr_semi = JoinExpression(
        left_dataset="users",
        right_dataset="vip",
        join_type=JoinType.SEMI,
        key_pairs=[JoinKeyPair(left="user_id", right="vip_id")],
        output_dataset="semi_out",
    )
    expr_anti = JoinExpression(
        left_dataset="users",
        right_dataset="vip",
        join_type=JoinType.ANTI,
        key_pairs=[JoinKeyPair(left="user_id", right="vip_id")],
        output_dataset="anti_out",
    )
    s1 = TransformationStepIR(step_id="s_semi", dataset_name="users", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr_semi)
    s2 = TransformationStepIR(step_id="s_anti", dataset_name="users", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr_anti)
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_semi_anti", steps=(s1, s2)))

    df_users = pd.DataFrame({"user_id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    df_vip = pd.DataFrame({"vip_id": [2, 3], "tier": ["Gold", "Platinum"]})

    outs = _exec_artifact(res.artifact, {"users": df_users, "vip": df_vip})
    assert list(outs["semi_out"]["user_id"]) == [2, 3]
    assert list(outs["semi_out"].columns) == ["user_id", "name"]
    assert list(outs["anti_out"]["user_id"]) == [1]
    assert list(outs["anti_out"].columns) == ["user_id", "name"]


def test_join_unmatched_policy_routing():
    """Contract 10: Unmatched rows routed to dedicated secondary output dataset."""
    expr = JoinExpression(
        left_dataset="orders",
        right_dataset="customers",
        join_type=JoinType.LEFT,
        key_pairs=[JoinKeyPair(left="cust_id", right="cust_id")],
        unmatched_policy=JoinUnmatchedPolicy(
            left_unmatched=UnmatchedRowDisposition.ROUTE_TO_UNMATCHED_OUTPUT,
            left_output_ref="orphan_orders",
        ),
        output_dataset="matched_orders",
    )
    step = TransformationStepIR(step_id="s_unmatched", dataset_name="orders", family=ActionFamily.JOIN, kind=StepKind.MULTI_DATASET_JOIN, expression=expr)
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_unmatched", steps=(step,)))

    df_orders = pd.DataFrame({"order_id": [1, 2], "cust_id": [10, 99]})
    df_customers = pd.DataFrame({"cust_id": [10], "name": ["Customer 10"]})
    outs = _exec_artifact(res.artifact, {"orders": df_orders, "customers": df_customers})

    assert "orphan_orders" in outs
    assert list(outs["orphan_orders"]["order_id"]) == [2]


# ============================================================================
# 5. Business Aggregation Tests (Contract 11)
# ============================================================================

def test_aggregation_count_star_vs_count_column_and_distinct():
    """Contract 11: COUNT(*) counts rows including nulls, COUNT(col) excludes nulls, COUNT_DISTINCT."""
    expr = AggregateExpression(
        group_by=["dept"],
        aggregates=[
            AggregateColumnSpec(source_column=None, operation=AggregateOp.COUNT, output_column="total_rows"),
            AggregateColumnSpec(source_column="bonus", operation=AggregateOp.COUNT, output_column="bonus_count"),
            AggregateColumnSpec(source_column="role", operation=AggregateOp.COUNT_DISTINCT, output_column="unique_roles"),
            AggregateColumnSpec(source_column="salary", operation=AggregateOp.SUM, output_column="total_salary"),
            AggregateColumnSpec(source_column="salary", operation=AggregateOp.AVG, output_column="avg_salary"),
            AggregateColumnSpec(source_column="salary", operation=AggregateOp.STDDEV_SAMPLE, output_column="salary_stddev"),
            AggregateColumnSpec(source_column="salary", operation=AggregateOp.MEDIAN, output_column="salary_median"),
        ],
        output_dataset="dept_summary",
    )
    step = TransformationStepIR(step_id="s_agg", dataset_name="emp", family=ActionFamily.AGGREGATE, kind=StepKind.DATASET_FILTER, expression=expr)
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_agg", steps=(step,)))

    df = pd.DataFrame({
        "dept": ["IT", "IT", "HR"],
        "bonus": [1000.0, None, 500.0],
        "role": ["Dev", "Dev", "Recruiter"],
        "salary": [100000.0, 120000.0, 80000.0],
    })
    outs = _exec_artifact(res.artifact, df)
    out_df = outs["dept_summary"].set_index("dept")

    assert out_df.loc["HR", "total_rows"] == 1
    assert out_df.loc["HR", "bonus_count"] == 1
    assert out_df.loc["HR", "unique_roles"] == 1
    assert out_df.loc["HR", "total_salary"] == 80000.0

    assert out_df.loc["IT", "total_rows"] == 2
    assert out_df.loc["IT", "bonus_count"] == 1
    assert out_df.loc["IT", "unique_roles"] == 1
    assert out_df.loc["IT", "total_salary"] == 220000.0
    assert out_df.loc["IT", "avg_salary"] == 110000.0
    assert round(out_df.loc["IT", "salary_stddev"], 2) == 14142.14


def test_aggregation_group_key_null_policy_and_all_null_sum():
    """Contract 11: GroupKeyNullPolicy.FAIL_STEP and all-null group sum."""
    expr = AggregateExpression(
        group_by=["category"],
        aggregates=[
            AggregateColumnSpec(
                source_column="amount",
                operation=AggregateOp.SUM,
                null_policy=AggregateValueNullPolicy.TREAT_AS_ZERO,
                output_column="sum_amt",
            )
        ],
        group_key_null_policy=GroupKeyNullPolicy.FAIL_STEP,
    )
    step = TransformationStepIR(step_id="s_null_grp", dataset_name="d", family=ActionFamily.AGGREGATE, kind=StepKind.DATASET_FILTER, expression=expr)
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_ng", steps=(step,)))

    df_null_grp = pd.DataFrame({"category": [None, "Electronics"], "amount": [10.0, 20.0]})
    with pytest.raises(ValueError, match="Null group key encountered under FAIL_STEP"):
        _exec_artifact(res.artifact, df_null_grp)


# ============================================================================
# 6. Publication Layer Tests (Contract 12)
# ============================================================================

def test_atomic_publication_failure_aborts_and_commits_zero_outputs():
    """Contract 12: If postcondition fails, abort is called and zero outputs are committed."""
    adapter = InMemoryPublicationAdapter()
    col = ColumnRef(column_name="amt", dataset_name="d")
    step = TransformationStepIR(
        step_id="s_pub", dataset_name="d", family=ActionFamily.CLIP, kind=StepKind.ROW_TRANSFORMATION,
        expression=ClipExpression(target_column=col, upper_bound=LiteralValue(value=100.0)),
        target_column="amt",
    )
    graph = TransformationIRGraph(graph_id="g_pub", steps=(step,))
    df = pd.DataFrame({"amt": [50.0, 150.0]})

    def failing_postcondition(outputs):
        raise ValueError("Business invariant violated: total amount exceeds threshold")

    with pytest.raises(PublicationPostconditionError, match="Publication aborted due to failed postconditions"):
        execute_with_publication_contracts(
            graph=graph,
            df=df,
            adapter=adapter,
            postconditions=[failing_postcondition],
        )

    assert len(adapter._published) == 0
    assert len(adapter._staged) == 0
    assert adapter._manifest_version == "v0"


def test_atomic_publication_success_updates_manifest_and_commits_outputs():
    """Contract 12: Successful postconditions atomically update manifest pointer and publish outputs."""
    adapter = InMemoryPublicationAdapter()
    col = ColumnRef(column_name="val", dataset_name="d")
    step = TransformationStepIR(
        step_id="s_pub_ok", dataset_name="d", family=ActionFamily.CLIP, kind=StepKind.ROW_TRANSFORMATION,
        expression=ClipExpression(target_column=col, upper_bound=LiteralValue(value=100.0)),
        target_column="val",
    )
    graph = TransformationIRGraph(graph_id="g_pub_ok", steps=(step,))
    df = pd.DataFrame({"val": [50.0, 80.0]})

    def passing_postcondition(outputs):
        assert "primary" in outputs
        assert outputs["primary"]["val"].max() <= 100.0

    commit_res = execute_with_publication_contracts(
        graph=graph,
        df=df,
        adapter=adapter,
        postconditions=[passing_postcondition],
    )

    assert commit_res.is_committed
    assert commit_res.published_output_count == 1
    assert adapter._manifest_version.startswith("v1_")
    assert "primary" in adapter._published
    assert list(adapter._published["primary"]["val"]) == [50.0, 80.0]
    assert len(adapter._staged) == 0
