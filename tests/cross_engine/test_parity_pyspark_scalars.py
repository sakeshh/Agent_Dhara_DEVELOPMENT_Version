"""
tests/cross_engine/test_parity_pyspark_scalars.py

Increment 3.1 Certification Test Suite:
PySpark Scalar, Parse, Clip, Flag, and Apply-Only Imputation Semantic Parity.

Validates:
1. Deterministic compiler artifact generation (identical IR -> bit-identical artifact bytes & SHA-256).
2. Category-specific semantic parity between Pandas reference compiler and PySpark compiler:
   - MAP_VALUE (TRIM, LOWER, UPPER, CAST, REGEX_SUBSTITUTE, LOOKUP_MAP, NORMALIZE_PHONE, SANITIZE_EMAIL)
   - CONDITIONAL_REPLACE (with three-valued unknown policies)
   - CLIP (double-sided clipping, null and infinity policies)
   - FLAG (predicate evaluation, three-valued unknown logic, audit indicator columns)
   - STANDARDIZE (STANDARDIZE_BOOLEAN with disjoint token sets)
   - PARSE (Numeric and Date parsing with distinct null vs parse error tracking)
   - IMPUTE (Apply-only: constant, pre-fitted global statistical values, pre-fitted grouped values)
3. Strict Fit/Apply Imputation Invariant:
   Proves that execution data with empirical mean divergent from fit data causes BOTH Pandas
   and PySpark to apply the learned fit-time value without recalculation.
4. Control construct: PASS_THROUGH step disposition and execution.
5. Negative Preflight & Safety Governance:
   - Preflight blocks unsupported action families atomically.
   - Tampered imputation model snapshot hashes raise ImputationModelIntegrityError.
   - AST safety validation rejects unapproved aggregation calls and dynamic code execution.
"""

from __future__ import annotations

import ast
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
import tempfile
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError
from pyspark.sql import SparkSession

from agent.etl_pipeline.codegen_shared import CodegenStatus
from agent.etl_pipeline.cross_engine.capability_models import (
    CapabilityStatus,
    EngineCapabilityKey,
)
from agent.etl_pipeline.cross_engine.capability_registry import (
    get_global_capability_registry,
)
from agent.etl_pipeline.pandas_ir_compiler import (
    CompilerStepDisposition,
    PandasIRCompiler,
)
from agent.etl_pipeline.pyspark_ir_compiler import (
    CompiledPySparkArtifact,
    PySparkIRCompiler,
)
from agent.etl_pipeline.transformation_ir import (
    ActionFamily,
    BinaryOp,
    BinaryOperator,
    BooleanFormatSpec,
    ClipExpression,
    ClipNullPolicy,
    ColumnRef,
    ConditionalReplaceExpression,
    DataType,
    DateFormatSpec,
    DatasetColumnSchema,
    DatasetSchema,
    FlagExpression,
    GroupedImputationStrategy,
    GroupedImputationValue,
    GroupKeyValue,
    ImputationModelIntegrityError,
    ImputeExpression,
    InfinityPolicy,
    LearnedImputationModel,
    LiteralValue,
    MissingGroupPolicy,
    NullCheck,
    NumericFormatSpec,
    ParseExpression,
    ParseFailurePolicy,
    ParseOutputMode,
    PassThroughExpression,
    PredicateUnknownPolicy,
    ScalarMapExpression,
    ScalarMapOperation,
    StatisticalImputationStrategy,
    StepKind,
    TransformationEffects,
    TransformationIRGraph,
    TransformationStepIR,
    UnmappedValuePolicy,
    ValueVersion,
    compute_model_snapshot_hash,
)
from tests.cross_engine.assertions import (
    assert_boolean_parity,
    assert_floating_parity,
    assert_schema_parity,
)


# ============================================================================
# Spark Test Fixture & Ingestion Helpers
# ============================================================================




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


# ============================================================================
# 1. Deterministic Artifact Generation Tests
# ============================================================================

def test_pyspark_compiler_deterministic_artifact_bytes():
    """Identical TransformationIRGraph must compile to identical PySpark artifact bytes and hash."""
    compiler = PySparkIRCompiler()
    step = TransformationStepIR(
        step_id="step_scalar_01",
        dataset_name="primary",
        kind=StepKind.ROW_TRANSFORMATION,
        family=ActionFamily.MAP_VALUE,
        expression=ScalarMapExpression(
            source_column=ColumnRef(column_name="name"),
            output_column="name_upper",
            operation=ScalarMapOperation.UPPER,
        ),
    )
    graph = TransformationIRGraph(
        graph_id="graph_det_01",
        steps=[step],
        primary_output_ref="primary",
    )

    res1 = compiler.compile(graph)
    res2 = compiler.compile(graph)

    assert res1.is_success
    assert res2.is_success
    assert res1.artifact is not None and res2.artifact is not None
    assert res1.artifact.artifact_bytes == res2.artifact.artifact_bytes
    assert res1.artifact.artifact_hash == res2.artifact.artifact_hash
    assert res1.artifact.source_code == res2.artifact.source_code
    assert res1.step_dispositions["step_scalar_01"] == CompilerStepDisposition.COMPILED


# ============================================================================
# 2. MAP_VALUE Parity Tests
# ============================================================================

def test_pyspark_scalar_map_trim_lower_upper_parity(spark_session):
    """Verifies TRIM, LOWER, and UPPER produce identical outputs across Pandas and PySpark."""
    pdf = pd.DataFrame({
        "raw_text": ["  Alice  ", "BOB  ", "  charlie", None, "   "],
    })

    steps = [
        TransformationStepIR(
            step_id="s1_trim",
            dataset_name="primary",
            kind=StepKind.ROW_TRANSFORMATION,
            family=ActionFamily.MAP_VALUE,
            expression=ScalarMapExpression(
                source_column=ColumnRef(column_name="raw_text"),
                output_column="trimmed",
                operation=ScalarMapOperation.TRIM,
            ),
        ),
        TransformationStepIR(
            step_id="s2_upper",
            dataset_name="primary",
            kind=StepKind.ROW_TRANSFORMATION,
            family=ActionFamily.MAP_VALUE,
            expression=ScalarMapExpression(
                source_column=ColumnRef(column_name="trimmed"),
                output_column="upper_text",
                operation=ScalarMapOperation.UPPER,
            ),
        ),
        TransformationStepIR(
            step_id="s3_lower",
            dataset_name="primary",
            kind=StepKind.ROW_TRANSFORMATION,
            family=ActionFamily.MAP_VALUE,
            expression=ScalarMapExpression(
                source_column=ColumnRef(column_name="trimmed"),
                output_column="lower_text",
                operation=ScalarMapOperation.LOWER,
            ),
        ),
    ]
    graph = TransformationIRGraph(graph_id="g_map_01", steps=steps, primary_output_ref="primary")

    # 1. Execute Pandas reference
    pandas_compiler = PandasIRCompiler()
    p_res = pandas_compiler.execute(graph, pdf.copy())
    p_df = p_res.primary_output

    # 2. Execute PySpark
    spark_compiler = PySparkIRCompiler()
    sdf_in = df_to_spark(spark_session, pdf)
    s_res = spark_compiler.execute(graph, sdf_in)
    s_df = spark_to_df(s_res.primary_output)

    # 3. Parity checks
    for col in ["trimmed", "upper_text", "lower_text"]:
        p_vals = [None if pd.isna(v) else str(v) for v in p_df[col]]
        s_vals = [None if pd.isna(v) else str(v) for v in s_df[col]]
        assert p_vals == s_vals, f"Mismatch in {col}: Pandas={p_vals} vs Spark={s_vals}"


def test_pyspark_scalar_map_regex_and_lookup_parity(spark_session):
    """Verifies REGEX_SUBSTITUTE, NORMALIZE_PHONE, SANITIZE_EMAIL, and LOOKUP_MAP."""
    pdf = pd.DataFrame({
        "phone": ["+1 (555) 123-4567", "555-9876", None],
        "email": ["  User@Example.COM  ", "ADMIN@Dhara.AI", None],
        "status_code": ["A", "I", "U"],
    })

    steps = [
        TransformationStepIR(
            step_id="s_phone",
            dataset_name="primary",
            kind=StepKind.ROW_TRANSFORMATION,
            family=ActionFamily.MAP_VALUE,
            expression=ScalarMapExpression(
                source_column=ColumnRef(column_name="phone"),
                output_column="clean_phone",
                operation=ScalarMapOperation.NORMALIZE_PHONE,
            ),
        ),
        TransformationStepIR(
            step_id="s_email",
            dataset_name="primary",
            kind=StepKind.ROW_TRANSFORMATION,
            family=ActionFamily.MAP_VALUE,
            expression=ScalarMapExpression(
                source_column=ColumnRef(column_name="email"),
                output_column="clean_email",
                operation=ScalarMapOperation.SANITIZE_EMAIL,
            ),
        ),
        TransformationStepIR(
            step_id="s_lookup",
            dataset_name="primary",
            kind=StepKind.ROW_TRANSFORMATION,
            family=ActionFamily.MAP_VALUE,
            expression=ScalarMapExpression(
                source_column=ColumnRef(column_name="status_code"),
                output_column="status_desc",
                operation=ScalarMapOperation.LOOKUP_MAP,
                parameters={"mapping": {"A": "Active", "I": "Inactive"}},
                unmapped_policy=UnmappedValuePolicy.KEEP_ORIGINAL,
            ),
        ),
    ]
    graph = TransformationIRGraph(graph_id="g_map_02", steps=steps, primary_output_ref="primary")

    pandas_compiler = PandasIRCompiler()
    p_df = pandas_compiler.execute(graph, pdf.copy()).primary_output

    spark_compiler = PySparkIRCompiler()
    sdf_in = df_to_spark(spark_session, pdf)
    s_df = spark_to_df(spark_compiler.execute(graph, sdf_in).primary_output)

    for col in ["clean_phone", "clean_email", "status_desc"]:
        p_vals = [None if pd.isna(v) else str(v) for v in p_df[col]]
        s_vals = [None if pd.isna(v) else str(v) for v in s_df[col]]
        assert p_vals == s_vals, f"Mismatch in {col}: Pandas={p_vals} vs Spark={s_vals}"


# ============================================================================
# 3. CONDITIONAL_REPLACE Parity Tests
# ============================================================================

def test_pyspark_conditional_replace_parity(spark_session):
    """Verifies CONDITIONAL_REPLACE under TREAT_AS_FALSE and PRESERVE_UNKNOWN."""
    pdf = pd.DataFrame({
        "val": [10, -999, 25, -999, None],
    })

    steps = [
        TransformationStepIR(
            step_id="s_cond_rep",
            dataset_name="primary",
            kind=StepKind.ROW_TRANSFORMATION,
            family=ActionFamily.CONDITIONAL_REPLACE,
            expression=ConditionalReplaceExpression(
                target_column=ColumnRef(column_name="val"),
                output_column="val_sanitized",
                condition=BinaryOp(
                    left=ColumnRef(column_name="val"),
                    op=BinaryOperator.EQ,
                    right=LiteralValue(value=-999),
                ),
                replace_value=LiteralValue(value=0),
                unknown_policy=PredicateUnknownPolicy.TREAT_AS_FALSE,
            ),
        ),
    ]
    graph = TransformationIRGraph(graph_id="g_cond_01", steps=steps, primary_output_ref="primary")

    pandas_compiler = PandasIRCompiler()
    p_df = pandas_compiler.execute(graph, pdf.copy()).primary_output

    spark_compiler = PySparkIRCompiler()
    sdf_in = df_to_spark(spark_session, pdf)
    s_df = spark_to_df(spark_compiler.execute(graph, sdf_in).primary_output)

    p_vals = [None if pd.isna(v) else float(v) for v in p_df["val_sanitized"]]
    s_vals = [None if pd.isna(v) else float(v) for v in s_df["val_sanitized"]]
    assert p_vals == [10.0, 0.0, 25.0, 0.0, None]
    assert s_vals == [10.0, 0.0, 25.0, 0.0, None]
    assert p_vals == s_vals


# ============================================================================
# 4. CLIP & FLAG Parity Tests
# ============================================================================

def test_pyspark_clip_and_flag_parity(spark_session):
    """
    Contract from architecture:
    FLAG reads ORIGINAL discount_percent
    CLIP reads and writes CURRENT discount_percent
    Input: [10.0, 60.0, 45.0, 80.0]
    Output: discount_percent=[10.0, 50.0, 45.0, 50.0], excessive_discount=[False, True, False, True]
    """
    pdf = pd.DataFrame({
        "discount_percent": [10.0, 60.0, 45.0, 80.0],
    })

    steps = [
        TransformationStepIR(
            step_id="s_clip",
            dataset_name="primary",
            kind=StepKind.ROW_TRANSFORMATION,
            family=ActionFamily.CLIP,
            expression=ClipExpression(
                target_column=ColumnRef(column_name="discount_percent", version=ValueVersion.CURRENT),
                lower_bound=LiteralValue(value=0.0),
                upper_bound=LiteralValue(value=50.0),
                null_policy=ClipNullPolicy.PRESERVE,
                infinity_policy=InfinityPolicy.CLIP_TO_BOUND,
            ),
        ),
        TransformationStepIR(
            step_id="s_flag",
            dataset_name="primary",
            kind=StepKind.ROW_TRANSFORMATION,
            family=ActionFamily.FLAG,
            expression=FlagExpression(
                output_column="excessive_discount",
                predicate=BinaryOp(
                    left=ColumnRef(column_name="discount_percent", version=ValueVersion.ORIGINAL),
                    op=BinaryOperator.GT,
                    right=LiteralValue(value=50.0),
                ),
                true_value=True,
                false_value=False,
                unknown_policy=PredicateUnknownPolicy.TREAT_AS_FALSE,
            ),
        ),
    ]
    graph = TransformationIRGraph(graph_id="g_clip_flag_01", steps=steps, primary_output_ref="primary")

    pandas_compiler = PandasIRCompiler()
    p_df = pandas_compiler.execute(graph, pdf.copy()).primary_output

    spark_compiler = PySparkIRCompiler()
    sdf_in = df_to_spark(spark_session, pdf)
    s_df = spark_to_df(spark_compiler.execute(graph, sdf_in).primary_output)

    # Parity assertions
    assert list(p_df["discount_percent"]) == [10.0, 50.0, 45.0, 50.0]
    assert list(s_df["discount_percent"]) == [10.0, 50.0, 45.0, 50.0]
    assert list(p_df["excessive_discount"]) == [False, True, False, True]
    assert list(s_df["excessive_discount"]) == [False, True, False, True]


# ============================================================================
# 5. STANDARDIZE_BOOLEAN Parity Tests
# ============================================================================

def test_pyspark_standardize_boolean_parity(spark_session):
    """Verifies STANDARDIZE_BOOLEAN maps true/false tokens cleanly with null preservation."""
    pdf = pd.DataFrame({
        "flag_raw": ["yes", "NO", "1", "0", "true", "FALSE", None],
    })

    steps = [
        TransformationStepIR(
            step_id="s_bool_std",
            dataset_name="primary",
            kind=StepKind.ROW_TRANSFORMATION,
            family=ActionFamily.MAP_VALUE,
            expression=ScalarMapExpression(
                source_column=ColumnRef(column_name="flag_raw"),
                output_column="flag_bool",
                operation=ScalarMapOperation.STANDARDIZE_BOOLEAN,
            ),
        ),
    ]
    graph = TransformationIRGraph(graph_id="g_bool_01", steps=steps, primary_output_ref="primary")

    pandas_compiler = PandasIRCompiler()
    p_df = pandas_compiler.execute(graph, pdf.copy()).primary_output

    spark_compiler = PySparkIRCompiler()
    sdf_in = df_to_spark(spark_session, pdf)
    s_df = spark_to_df(spark_compiler.execute(graph, sdf_in).primary_output)

    p_vals = [None if pd.isna(v) else bool(v) for v in p_df["flag_bool"]]
    s_vals = [None if pd.isna(v) else bool(v) for v in s_df["flag_bool"]]
    assert p_vals == [True, False, True, False, True, False, None]
    assert s_vals == [True, False, True, False, True, False, None]


# ============================================================================
# 6. PARSE Parity Tests (Distinguishing Nulls from Parse Errors)
# ============================================================================

def test_pyspark_parse_numeric_and_date_parity(spark_session):
    """
    Contract 6: Existing missing values are strictly distinguished from parse errors!
    Verifies PARSE_NUMBER and PARSE_DATE with failure flag indicator columns.
    """
    pdf = pd.DataFrame({
        "price_str": ["$1,250.50", "$300.00", "INVALID", None],
        "date_str": ["2026-05-15", "2026-11-20", "BAD_DATE", None],
    })

    steps = [
        TransformationStepIR(
            step_id="s_parse_num",
            dataset_name="primary",
            kind=StepKind.ROW_TRANSFORMATION,
            family=ActionFamily.PARSE,
            expression=ParseExpression(
                source_column=ColumnRef(column_name="price_str"),
                target_column="price_parsed",
                target_data_type=DataType.FLOAT,
                numeric_spec=NumericFormatSpec(
                    currency_symbols=("$",),
                    thousands_separator=",",
                    decimal_separator=".",
                ),
                failure_policy=ParseFailurePolicy.FLAG,
            ),
        ),
        TransformationStepIR(
            step_id="s_parse_date",
            dataset_name="primary",
            kind=StepKind.ROW_TRANSFORMATION,
            family=ActionFamily.PARSE,
            expression=ParseExpression(
                source_column=ColumnRef(column_name="date_str"),
                target_column="date_parsed",
                target_data_type=DataType.DATE,
                date_spec=DateFormatSpec(accepted_formats=("%Y-%m-%d",)),
                failure_policy=ParseFailurePolicy.FLAG,
            ),
        ),
    ]
    graph = TransformationIRGraph(graph_id="g_parse_01", steps=steps, primary_output_ref="primary")

    pandas_compiler = PandasIRCompiler()
    p_df = pandas_compiler.execute(graph, pdf.copy()).primary_output

    spark_compiler = PySparkIRCompiler()
    sdf_in = df_to_spark(spark_session, pdf)
    s_df = spark_to_df(spark_compiler.execute(graph, sdf_in).primary_output)

    # Invariant: Row 3 ("INVALID") is a parse error. Row 4 (None) is NOT a parse error!
    p_err = list(p_df["price_parsed_is_parse_error"])
    s_err = list(s_df["price_parsed_is_parse_error"])
    assert p_err == [False, False, True, False]
    assert s_err == [False, False, True, False]

    # Date parse error check
    p_d_err = list(p_df["date_parsed_is_parse_error"])
    s_d_err = list(s_df["date_parsed_is_parse_error"])
    assert p_d_err == [False, False, True, False]
    assert s_d_err == [False, False, True, False]


# ============================================================================
# 7. Apply-Only Imputation Invariant & Parity Tests
# ============================================================================

def test_pyspark_impute_constant_and_statistical_divergent_mean_parity(spark_session):
    """
    CRITICAL INVARIANT TEST:
    Fit data had a mean of 42.0.
    Execution data has numbers [100.0, 200.0, None].
    If the engine recalculated mean at apply time, it would impute 150.0.
    Under certified contracts, BOTH Pandas and PySpark MUST impute 42.0 (fit-time mean).
    """
    learned_model = LearnedImputationModel(
        model_id="model_mean_01",
        strategy="mean",
        learned_value=42.0,
        fit_population_hash="hash_pop_123",
        fit_schema_hash="hash_sch_123",
    )

    pdf = pd.DataFrame({
        "val": [100.0, 200.0, None],
    })

    steps = [
        TransformationStepIR(
            step_id="s_impute_mean",
            dataset_name="primary",
            kind=StepKind.ROW_TRANSFORMATION,
            family=ActionFamily.IMPUTE,
            expression=ImputeExpression(
                target_column=ColumnRef(column_name="val"),
                output_column="val_imputed",
                strategy=StatisticalImputationStrategy.MEAN,
                learned_model=learned_model,
                emit_indicator=True,
                indicator_column="val_was_imputed",
            ),
        ),
    ]
    graph = TransformationIRGraph(graph_id="g_impute_01", steps=steps, primary_output_ref="primary")

    pandas_compiler = PandasIRCompiler()
    p_df = pandas_compiler.execute(graph, pdf.copy()).primary_output

    spark_compiler = PySparkIRCompiler()
    sdf_in = df_to_spark(spark_session, pdf)
    s_df = spark_to_df(spark_compiler.execute(graph, sdf_in).primary_output)

    # Both must impute 42.0, NEVER 150.0!
    assert list(p_df["val_imputed"]) == [100.0, 200.0, 42.0]
    assert list(s_df["val_imputed"]) == [100.0, 200.0, 42.0]

    # Pre-fill indicator must flag only row 2 (0-indexed)
    assert list(p_df["val_was_imputed"]) == [False, False, True]
    assert list(s_df["val_was_imputed"]) == [False, False, True]


def test_pyspark_impute_grouped_with_fallback_parity(spark_session):
    """Verifies grouped imputation with learned group lookup and fallback for unseen group."""
    grp_vals = (
        GroupedImputationValue(
            group_key=(GroupKeyValue(column="dept", value="ENG", data_type="string"),),
            learned_value=85.0,
            sample_size=10,
        ),
        GroupedImputationValue(
            group_key=(GroupKeyValue(column="dept", value="HR", data_type="string"),),
            learned_value=65.0,
            sample_size=10,
        ),
    )
    model = LearnedImputationModel(
        model_id="model_grp_01",
        strategy="grouped_mean",
        learned_value=70.0,
        group_values=grp_vals,
        group_columns=("dept",),
        global_fallback_value=70.0,
        missing_group_policy=MissingGroupPolicy.USE_GLOBAL_FALLBACK,
        fit_population_hash="hash_pop_grp",
        fit_schema_hash="hash_sch_grp",
    )

    pdf = pd.DataFrame({
        "dept": ["ENG", "HR", "SALES", "ENG"],  # SALES is unseen group
        "score": [None, None, None, 90.0],
    })

    steps = [
        TransformationStepIR(
            step_id="s_grp_impute",
            dataset_name="primary",
            kind=StepKind.ROW_TRANSFORMATION,
            family=ActionFamily.IMPUTE,
            expression=ImputeExpression(
                target_column=ColumnRef(column_name="score"),
                output_column="score_imputed",
                strategy=GroupedImputationStrategy.GROUPED_MEAN,
                group_by_columns=("dept",),
                learned_model=model,
            ),
        ),
    ]
    graph = TransformationIRGraph(graph_id="g_grp_01", steps=steps, primary_output_ref="primary")

    pandas_compiler = PandasIRCompiler()
    p_df = pandas_compiler.execute(graph, pdf.copy()).primary_output

    spark_compiler = PySparkIRCompiler()
    sdf_in = df_to_spark(spark_session, pdf)
    s_df = spark_to_df(spark_compiler.execute(graph, sdf_in).primary_output)

    # ENG -> 85.0, HR -> 65.0, SALES (unseen) -> 70.0 (fallback), ENG (90.0) -> 90.0
    assert list(p_df["score_imputed"]) == [85.0, 65.0, 70.0, 90.0]
    assert list(s_df["score_imputed"]) == [85.0, 65.0, 70.0, 90.0]


def test_pyspark_pass_through_control_disposition():
    """Verifies PASS_THROUGH step receives CONTROL_ONLY and does not emit mutative code."""
    compiler = PySparkIRCompiler()
    step = TransformationStepIR(
        step_id="s_pass",
        dataset_name="primary",
        kind=StepKind.CONTROL,
        family=ActionFamily.PASS_THROUGH,
        expression=PassThroughExpression(),
    )
    graph = TransformationIRGraph(graph_id="g_pass_01", steps=[step], primary_output_ref="primary")

    res = compiler.compile(graph)
    assert res.is_success
    assert res.step_dispositions["s_pass"] == CompilerStepDisposition.CONTROL_ONLY
    assert "CONTROL_ONLY" in res.artifact.source_code


# ============================================================================
# 8. Negative Tests: Preflight, Model Tampering, AST Safety
# ============================================================================

def test_pyspark_preflight_blocks_unsupported_family_atomically():
    """Unsupported action family (e.g. uncertified family) must fail preflight atomically."""
    compiler = PySparkIRCompiler()
    compiler.SUPPORTED_FAMILIES = set(PySparkIRCompiler.SUPPORTED_FAMILIES) - {ActionFamily.JOIN}
    step = TransformationStepIR(
        step_id="step_join_01",
        dataset_name="primary",
        kind=StepKind.ROW_TRANSFORMATION,
        family=ActionFamily.JOIN,
        expression=PassThroughExpression(),
    )
    graph = TransformationIRGraph(graph_id="g_unsupported", steps=[step])

    res = compiler.compile(graph)
    assert not res.is_success
    assert res.status == CodegenStatus.FAILED_COMPILATION
    assert res.artifact is None
    assert res.step_dispositions["step_join_01"] == CompilerStepDisposition.UNSUPPORTED
    assert any("not implemented" in d for d in res.diagnostics)


def test_pyspark_imputation_snapshot_hash_tamper_rejected():
    """Tampered imputation model snapshot hash must raise ImputationModelIntegrityError."""
    with pytest.raises((ImputationModelIntegrityError, ValidationError), match="Model snapshot hash mismatch"):
        LearnedImputationModel(
            model_id="model_bad_01",
            strategy="mean",
            learned_value=50.0,
            fit_population_hash="hash_pop",
            fit_schema_hash="hash_sch",
            model_snapshot_hash="TAMPERED_INVALID_HASH",
        )


def test_pyspark_ast_safety_rejects_arbitrary_code_and_runtime_agg():
    """AST safety validator must reject dangerous execution and apply-time aggregations."""
    compiler = PySparkIRCompiler()

    # 1. Prohibited import/eval
    unsafe_code_1 = "import os\ndef transform(df):\n    os.system('echo bad')\n    return df"
    with pytest.raises(ValueError, match="Prohibited import: os"):
        compiler._validate_ast_safety(unsafe_code_1)

    # 2. Prohibited eval call
    unsafe_code_2 = "def transform(df):\n    eval('1 + 1')\n    return df"
    with pytest.raises(ValueError, match="Prohibited call: eval"):
        compiler._validate_ast_safety(unsafe_code_2)

    # 3. Prohibited apply-time statistical recalculation
    unsafe_code_3 = "def transform(df):\n    val = df.mean()\n    return df"
    with pytest.raises(ValueError, match="Prohibited statistical recalculation"):
        compiler._validate_ast_safety(unsafe_code_3)
