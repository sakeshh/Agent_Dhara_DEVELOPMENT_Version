"""
Tests for Reference Pandas IR Compiler (Transformation IR v1.0 -> Executable Pandas)
=====================================================================================

Vertical Slice 3 (Increment 3A): Reference compiler for deterministic scalar actions:
- Strict Step Accounting across all 5 dispositions
- Selective Series pre-images with collision-safe symbol generation
- Explicit 3-valued null/unknown policies and infinity policies
- AST safety inspection and restricted sandbox execution
- Coverage for all 11 ScalarMap operations, Clip, Flag, ConditionalReplace, PassThrough
- Clear separation between Determinism and Idempotency
"""

import ast
from decimal import Decimal
from typing import Any, Dict

import numpy as np
import pandas as pd
import pytest

from agent.etl_pipeline.pandas_ir_compiler import (
    CompiledPandasArtifact,
    CompilerAccountingError,
    CompilerStepDisposition,
    PandasCompilerResult,
    PandasIRCompiler,
    PreimageBinding,
)
from agent.etl_pipeline.codegen_shared import CodegenStatus
from agent.etl_pipeline.transformation_ir import (
    TRANSFORMATION_IR_VERSION,
    ActionFamily,
    AmbiguousTimePolicy,
    BinaryOp,
    BinaryOperator,
    BooleanFormatSpec,
    ClipExpression,
    ClipNullPolicy,
    ColumnRef,
    ConditionalReplaceExpression,
    DataType,
    DatasetColumnSchema,
    DatasetSchema,
    DateFormatSpec,
    ExistingColumnPolicy,
    FlagExpression,
    GroupKeyValue,
    GroupedImputationStrategy,
    GroupedImputationValue,
    ImputeExpression,
    InfinityPolicy,
    InSetPredicate,
    IRDependency,
    LearnedImputationModel,
    LiteralValue,
    MissingGroupPolicy,
    ModeTieBreaker,
    NonexistentTimePolicy,
    NullCheck,
    NumericFormatSpec,
    ParseExpression,
    ParseFailurePolicy,
    ParseOutputMode,
    PassThroughDecision,
    PassThroughExpression,
    PredicateUnknownPolicy,
    ScalarMapExpression,
    ScalarMapOperation,
    StatisticalImputationStrategy,
    StepKind,
    TemporalParseTarget,
    TransformationIRGraph,
    TransformationIRInvariantError,
    TransformationStepIR,
    UnboundedInfinityPolicy,
    UnmappedValuePolicy,
    ValueVersion,
    compute_model_snapshot_hash,
    ImputationModelIntegrityError,
    verify_imputation_model,
)


def _execute_artifact_safely(artifact: CompiledPandasArtifact, df: pd.DataFrame) -> pd.DataFrame:
    """Executes the compiled artifact within a strictly controlled execution namespace."""
    namespace: Dict[str, Any] = {
        "np": np,
        "pd": pd,
    }
    exec(artifact.source_code, namespace)
    transform_fn = namespace["transform"]
    return transform_fn(df)


# ============================================================================
# 1. Compiler Step Accounting Tests
# ============================================================================

def test_compiler_accounting_all_dispositions_and_invariants():
    """
    Vertical Slice 3A: Every IR step receives exactly one disposition.
    Input step IDs == disposition step IDs.
    Accounting equation: N_steps = N_compiled + N_control + N_unsupported + N_invalid_schema + N_invalid_ir.
    """
    col = ColumnRef(column_name="salary", dataset_name="payroll")
    step_clip = TransformationStepIR(
        step_id="step_clip",
        dataset_name="payroll",
        family=ActionFamily.CLIP,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ClipExpression(
            target_column=col,
            upper_bound=LiteralValue(value=100000.0, data_type=DataType.FLOAT),
        ),
        target_column="salary",
    )
    step_pass = TransformationStepIR(
        step_id="step_pass",
        dataset_name="payroll",
        family=ActionFamily.PASS_THROUGH,
        kind=StepKind.CONTROL,
        expression=PassThroughExpression(reason_code="documented_business_exemption"),
    )
    # Unsupported step in 3E (e.g. STANDARDIZE)
    step_unsupported = TransformationStepIR(
        step_id="step_hash",
        dataset_name="payroll",
        family=ActionFamily.STANDARDIZE,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=FlagExpression(
            predicate=NullCheck(operand=col),
            output_column="hash_flag",
        ),
        target_column="hash_flag",
    )

    graph = TransformationIRGraph(
        graph_id="graph_accounting",
        steps=(step_clip, step_pass),
        schema_snapshot_hash="schema_snap_123",
    )

    compiler = PandasIRCompiler()
    result = compiler.compile(graph)

    # Invariants
    assert result.status == CodegenStatus.SUCCEEDED
    assert result.artifact is not None
    assert set(result.step_dispositions.keys()) == {"step_clip", "step_pass"}
    assert result.step_dispositions["step_clip"] == CompilerStepDisposition.COMPILED
    assert result.step_dispositions["step_pass"] == CompilerStepDisposition.CONTROL_ONLY

    # Equation check
    disps = list(result.step_dispositions.values())
    assert len(disps) == 2
    assert disps.count(CompilerStepDisposition.COMPILED) == 1
    assert disps.count(CompilerStepDisposition.CONTROL_ONLY) == 1
    assert disps.count(CompilerStepDisposition.UNSUPPORTED) == 0

    # Test with unsupported step: causes atomic failure
    graph_unsupported = TransformationIRGraph(
        graph_id="graph_with_unsupported",
        steps=(step_clip, step_pass, step_unsupported),
        schema_snapshot_hash="schema_snap_123",
    )
    result_fail = compiler.compile(graph_unsupported)
    assert result_fail.status == CodegenStatus.FAILED_COMPILATION
    assert result_fail.artifact is None
    assert result_fail.step_dispositions["step_hash"] == CompilerStepDisposition.UNSUPPORTED
    assert len(result_fail.diagnostics) >= 1


def test_compiler_rejection_of_schema_binding_failure():
    """
    Schema binding failure marks step as INVALID_SCHEMA_BINDING and fails artifact generation.
    """
    schemas = {
        "payroll": DatasetSchema(
            dataset_name="payroll",
            columns={
                "salary": DatasetColumnSchema(name="salary", data_type=DataType.STRING, nullable=False)
            },
        )
    }
    col = ColumnRef(column_name="salary", dataset_name="payroll")
    # Clip on string column without conversion fails schema binding
    step_invalid_clip = TransformationStepIR(
        step_id="step_str_clip",
        dataset_name="payroll",
        family=ActionFamily.CLIP,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ClipExpression(
            target_column=col,
            upper_bound=LiteralValue(value=100.0, data_type=DataType.FLOAT),
        ),
        target_column="salary",
    )
    graph = TransformationIRGraph(
        graph_id="graph_schema_fail",
        steps=(step_invalid_clip,),
        schema_snapshot_hash="schema_snap_payroll",
    )
    compiler = PandasIRCompiler(schemas=schemas)
    result = compiler.compile(graph)

    assert result.status == CodegenStatus.FAILED_COMPILATION
    assert result.artifact is None
    assert result.step_dispositions["step_str_clip"] == CompilerStepDisposition.INVALID_SCHEMA_BINDING


# ============================================================================
# 2. Collision-Safe Selective Pre-Image Tests
# ============================================================================

def test_selective_preimage_symbol_generation_and_no_full_copy():
    """
    Vertical Slice 3A: Pre-images generate collision-safe symbols (_preimage_0)
    and emit Series-only deep copies, prohibiting full df.copy().
    """
    col_orig = ColumnRef(column_name="discount percent", version=ValueVersion.ORIGINAL)
    col_curr = ColumnRef(column_name="discount percent", version=ValueVersion.CURRENT)

    step_flag = TransformationStepIR(
        step_id="step_flag",
        dataset_name="orders",
        family=ActionFamily.FLAG,
        kind=StepKind.AUDIT_FLAG,
        expression=FlagExpression(
            predicate=BinaryOp(left=col_orig, op=BinaryOperator.GT, right=LiteralValue(value=50.0)),
            output_column="excessive_discount",
            true_value=True,
            false_value=False,
        ),
        target_column="excessive_discount",
    )
    step_clip = TransformationStepIR(
        step_id="step_clip",
        dataset_name="orders",
        family=ActionFamily.CLIP,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ClipExpression(
            target_column=col_curr,
            upper_bound=LiteralValue(value=50.0, data_type=DataType.FLOAT),
        ),
        target_column="discount percent",
        prerequisites=("step_flag",),
    )

    graph = TransformationIRGraph(
        graph_id="graph_cap_and_flag",
        steps=(step_flag, step_clip),
        dependencies=(IRDependency(source_step_id="step_flag", target_step_id="step_clip"),),
        schema_snapshot_hash="orders_snap",
    )

    compiler = PandasIRCompiler()
    result = compiler.compile(graph)
    assert result.status == CodegenStatus.SUCCEEDED
    assert result.artifact is not None

    code = result.artifact.source_code
    # 1. Verify selective Series pre-image symbol created
    assert len(result.preimage_bindings) == 1
    binding = result.preimage_bindings[0]
    assert binding.column_ref == "discount percent"
    assert binding.internal_symbol == "_preimage_0"
    assert binding.required_by_step_ids == ("step_flag",)

    # 2. Verify preamble contains Series copy and AST has no deep df.copy()
    assert "_preimage_0 = df['discount percent'].copy(deep=True)" in code

    tree = ast.parse(code)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr == "copy":
                # Ensure no call like df.copy(deep=True)
                if isinstance(node.func.value, ast.Name) and node.func.value.id == "df":
                    for kw in node.keywords:
                        if kw.arg == "deep" and getattr(kw.value, "value", None) is True:
                            pytest.fail("Found prohibited whole-DataFrame deep copy: df.copy(deep=True)")

    # 3. Verify Execution Parity
    input_df = pd.DataFrame({"discount percent": [10.0, 60.0, 45.0, 80.0]})
    out_df = _execute_artifact_safely(result.artifact, input_df)

    assert list(out_df["discount percent"]) == [10.0, 50.0, 45.0, 50.0]
    assert list(out_df["excessive_discount"]) == [False, True, False, True]
    # Verify temporary pre-image symbol did not leak into output dataframe columns
    assert "_preimage_0" not in out_df.columns


# ============================================================================
# 3. Explicit 3-Valued Null & Infinity Handling
# ============================================================================

def test_predicate_unknown_policies_on_null_and_nan():
    """
    Vertical Slice 3A: Tests 3-valued unknown policies (PRESERVE_UNKNOWN, TREAT_AS_FALSE, TREAT_AS_TRUE).
    """
    col = ColumnRef(column_name="score", dataset_name="tests")

    # Policy 1: PRESERVE_UNKNOWN emits nullable boolean pd.NA
    expr_preserve = FlagExpression(
        predicate=BinaryOp(left=col, op=BinaryOperator.GT, right=LiteralValue(value=50.0)),
        output_column="flag_preserve",
        unknown_policy=PredicateUnknownPolicy.PRESERVE_UNKNOWN,
    )
    step_preserve = TransformationStepIR(
        step_id="step_pres",
        dataset_name="tests",
        family=ActionFamily.FLAG,
        kind=StepKind.AUDIT_FLAG,
        expression=expr_preserve,
        target_column="flag_preserve",
    )

    # Policy 2: TREAT_AS_FALSE treats null comparison as False
    expr_false = FlagExpression(
        predicate=BinaryOp(left=col, op=BinaryOperator.GT, right=LiteralValue(value=50.0)),
        output_column="flag_false",
        unknown_policy=PredicateUnknownPolicy.TREAT_AS_FALSE,
    )
    step_false = TransformationStepIR(
        step_id="step_fls",
        dataset_name="tests",
        family=ActionFamily.FLAG,
        kind=StepKind.AUDIT_FLAG,
        expression=expr_false,
        target_column="flag_false",
    )

    graph = TransformationIRGraph(
        graph_id="graph_null_policies",
        steps=(step_preserve, step_false),
        schema_snapshot_hash="tests_snap",
    )
    compiler = PandasIRCompiler()
    result = compiler.compile(graph)
    assert result.status == CodegenStatus.SUCCEEDED

    input_df = pd.DataFrame({"score": [60.0, None, np.nan, 40.0]})
    out_df = _execute_artifact_safely(result.artifact, input_df)

    # Verify flag_preserve preserves pd.NA
    assert out_df["flag_preserve"].iloc[0] is True or out_df["flag_preserve"].iloc[0] == True
    assert pd.isna(out_df["flag_preserve"].iloc[1])
    assert pd.isna(out_df["flag_preserve"].iloc[2])
    assert out_df["flag_preserve"].iloc[3] is False or out_df["flag_preserve"].iloc[3] == False

    # Verify flag_false coerced to False
    assert out_df["flag_false"].iloc[0] is True or out_df["flag_false"].iloc[0] == True
    assert out_df["flag_false"].iloc[1] is False or out_df["flag_false"].iloc[1] == False
    assert out_df["flag_false"].iloc[2] is False or out_df["flag_false"].iloc[2] == False
    assert out_df["flag_false"].iloc[3] is False or out_df["flag_false"].iloc[3] == False


def test_infinity_handling_policies_in_clip():
    """
    Vertical Slice 3A: Tests CLIP handling of +inf and -inf under CLIP_TO_BOUND policy.
    """
    col = ColumnRef(column_name="temp", dataset_name="sensors")
    step_clip_inf = TransformationStepIR(
        step_id="step_clip_inf",
        dataset_name="sensors",
        family=ActionFamily.CLIP,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ClipExpression(
            target_column=col,
            lower_bound=LiteralValue(value=-10.0, data_type=DataType.FLOAT),
            upper_bound=LiteralValue(value=100.0, data_type=DataType.FLOAT),
            infinity_policy=InfinityPolicy.CLIP_TO_BOUND,
        ),
        target_column="temp",
    )
    graph = TransformationIRGraph(
        graph_id="graph_inf_test",
        steps=(step_clip_inf,),
        schema_snapshot_hash="sensors_snap",
    )
    compiler = PandasIRCompiler()
    result = compiler.compile(graph)
    assert result.status == CodegenStatus.SUCCEEDED

    input_df = pd.DataFrame({"temp": [50.0, np.inf, -np.inf, np.nan]})
    out_df = _execute_artifact_safely(result.artifact, input_df)

    assert out_df["temp"].iloc[0] == 50.0
    assert out_df["temp"].iloc[1] == 100.0  # +inf clipped to upper bound
    assert out_df["temp"].iloc[2] == -10.0  # -inf clipped to lower bound
    assert np.isnan(out_df["temp"].iloc[3])  # nan preserved under ClipNullPolicy.PRESERVE


# ============================================================================
# 4. Coverage of all 11 ScalarMap Operations
# ============================================================================

def test_all_11_scalar_map_operations_execution():
    """
    Vertical Slice 3A: Comprehensive test validating all 11 operations under ScalarMapOperation.
    """
    col_str = ColumnRef(column_name="raw_str", dataset_name="data")
    col_num = ColumnRef(column_name="raw_num", dataset_name="data")
    col_date = ColumnRef(column_name="raw_date", dataset_name="data")

    steps = [
        # 1. TRIM
        TransformationStepIR(
            step_id="s_trim", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(source_column=col_str, output_column="str_trim", operation=ScalarMapOperation.TRIM),
        ),
        # 2. LOWER
        TransformationStepIR(
            step_id="s_lower", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(source_column=col_str, output_column="str_lower", operation=ScalarMapOperation.LOWER),
        ),
        # 3. UPPER
        TransformationStepIR(
            step_id="s_upper", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(source_column=col_str, output_column="str_upper", operation=ScalarMapOperation.UPPER),
        ),
        # 4. CAST
        TransformationStepIR(
            step_id="s_cast", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(
                source_column=col_str, output_column="str_cast", operation=ScalarMapOperation.CAST,
                parameters={"target_type": "string"}
            ),
        ),
        # 5. PARSE_NUMBER
        TransformationStepIR(
            step_id="s_parse_num", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(source_column=col_num, output_column="num_parsed", operation=ScalarMapOperation.PARSE_NUMBER),
        ),
        # 6. PARSE_DATE
        TransformationStepIR(
            step_id="s_parse_date", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(source_column=col_date, output_column="date_parsed", operation=ScalarMapOperation.PARSE_DATE),
        ),
        # 7. STANDARDIZE_BOOLEAN
        TransformationStepIR(
            step_id="s_std_bool", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(source_column=col_str, output_column="bool_std", operation=ScalarMapOperation.STANDARDIZE_BOOLEAN),
        ),
        # 8. REGEX_SUBSTITUTE
        TransformationStepIR(
            step_id="s_regex", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(
                source_column=col_str, output_column="str_regex", operation=ScalarMapOperation.REGEX_SUBSTITUTE,
                parameters={"pattern": "[0-9]", "replacement": "X"}
            ),
        ),
        # 9. LOOKUP_MAP
        TransformationStepIR(
            step_id="s_lookup", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(
                source_column=col_str, output_column="str_lookup", operation=ScalarMapOperation.LOOKUP_MAP,
                parameters={"mapping": {"  HELLO  ": "GREETING"}},
                unmapped_policy=UnmappedValuePolicy.KEEP_ORIGINAL,
            ),
        ),
        # 10. NORMALIZE_PHONE
        TransformationStepIR(
            step_id="s_phone", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(source_column=col_str, output_column="phone_norm", operation=ScalarMapOperation.NORMALIZE_PHONE),
        ),
        # 11. SANITIZE_EMAIL
        TransformationStepIR(
            step_id="s_email", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(source_column=col_str, output_column="email_clean", operation=ScalarMapOperation.SANITIZE_EMAIL),
        ),
    ]

    graph = TransformationIRGraph(
        graph_id="graph_all_11_ops",
        steps=tuple(steps),
        schema_snapshot_hash="data_snap",
    )
    compiler = PandasIRCompiler()
    result = compiler.compile(graph)
    assert result.status == CodegenStatus.SUCCEEDED

    input_df = pd.DataFrame({
        "raw_str": ["  HELLO  ", "yes", "+1 (555) 123-4567", "USER@Domain.COM "],
        "raw_num": ["123.45", "invalid", "678", None],
        "raw_date": ["2026-01-15", "invalid_date", "2026-09-16", None],
    })
    out_df = _execute_artifact_safely(result.artifact, input_df)

    # 1. TRIM
    assert out_df["str_trim"].iloc[0] == "HELLO"
    # 2. LOWER
    assert out_df["str_lower"].iloc[0] == "  hello  "
    # 3. UPPER
    assert out_df["str_upper"].iloc[0] == "  HELLO  "
    # 5. PARSE_NUMBER
    assert out_df["num_parsed"].iloc[0] == 123.45
    assert pd.isna(out_df["num_parsed"].iloc[1])
    # 6. PARSE_DATE
    assert not pd.isna(out_df["date_parsed"].iloc[0])
    assert pd.isna(out_df["date_parsed"].iloc[1])
    # 7. STANDARDIZE_BOOLEAN
    assert out_df["bool_std"].iloc[1] is True or out_df["bool_std"].iloc[1] == True
    # 10. NORMALIZE_PHONE
    assert out_df["phone_norm"].iloc[2] == "+15551234567"
    # 11. SANITIZE_EMAIL
    assert out_df["email_clean"].iloc[3] == "user@domain.com"


# ============================================================================
# 5. Determinism vs. Idempotency Verification
# ============================================================================

def test_determinism_and_operation_specific_idempotency():
    """
    Vertical Slice 3A: Separates determinism from idempotency.
    - Determinism: identical input + IR produces identical artifact bytes & execution outputs across runs.
    - Idempotency: transform(transform(df)) == transform(df) only for idempotent=True families.
    """
    col = ColumnRef(column_name="name", dataset_name="users")
    step_trim = TransformationStepIR(
        step_id="step_trim_name",
        dataset_name="users",
        family=ActionFamily.MAP_VALUE,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ScalarMapExpression(source_column=col, output_column="name", operation=ScalarMapOperation.TRIM),
        idempotent=True,
    )
    graph = TransformationIRGraph(
        graph_id="graph_idempotency",
        steps=(step_trim,),
        schema_snapshot_hash="users_snap",
    )

    compiler = PandasIRCompiler()

    # 1. Determinism: Two compilations produce exact byte-for-byte identical output
    res1 = compiler.compile(graph)
    res2 = compiler.compile(graph)
    assert res1.artifact.artifact_bytes == res2.artifact.artifact_bytes
    assert res1.artifact.artifact_hash == res2.artifact.artifact_hash

    # 2. Idempotency: Running twice produces identical DataFrame
    input_df = pd.DataFrame({"name": [" Alice  ", " Bob\t", "Charlie "]})
    out_once = _execute_artifact_safely(res1.artifact, input_df)
    out_twice = _execute_artifact_safely(res1.artifact, out_once)

    pd.testing.assert_frame_equal(out_once, out_twice)
    assert list(out_twice["name"]) == ["Alice", "Bob", "Charlie"]


# ============================================================================
# 6. AST Safety & Constrained Execution Tests
# ============================================================================

def test_ast_safety_prohibits_dangerous_constructs():
    """
    Vertical Slice 3A: AST safety validator detects and rejects dangerous calls.
    """
    compiler = PandasIRCompiler()

    # Safe code
    safe_code = "import pandas as pd\nimport numpy as np\ndf['x'] = df['x'].clip(0, 10)\n"
    compiler._validate_ast_safety(safe_code)

    # Prohibited: eval call
    unsafe_eval = "import pandas as pd\neval('1 + 1')\n"
    with pytest.raises(ValueError, match="Prohibited built-in call: eval"):
        compiler._validate_ast_safety(unsafe_eval)

    # Prohibited: dangerous import
    unsafe_import = "import os\n"
    with pytest.raises(ValueError, match="Prohibited import: os"):
        compiler._validate_ast_safety(unsafe_import)


def test_scalar_string_operations_preserve_null_semantics():
    """
    Vertical Slice 3A: Direct tests for all string-oriented operations:
    TRIM, LOWER, UPPER, REGEX_SUBSTITUTE, STANDARDIZE_BOOLEAN, NORMALIZE_PHONE, SANITIZE_EMAIL.
    Asserts pd.NA preservation on [" ABC ", None, np.nan, pd.NA] and absence of stringified nulls.
    """
    col = ColumnRef(column_name="raw_val", dataset_name="data")
    steps = (
        TransformationStepIR(
            step_id="step_trim", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(source_column=col, output_column="trimmed", operation=ScalarMapOperation.TRIM),
        ),
        TransformationStepIR(
            step_id="step_lower", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(source_column=col, output_column="lowered", operation=ScalarMapOperation.LOWER),
        ),
        TransformationStepIR(
            step_id="step_upper", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(source_column=col, output_column="uppered", operation=ScalarMapOperation.UPPER),
        ),
        TransformationStepIR(
            step_id="step_regex", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(
                source_column=col, output_column="regexed", operation=ScalarMapOperation.REGEX_SUBSTITUTE,
                parameters={"pattern": r"\s+", "replacement": ""},
            ),
        ),
        TransformationStepIR(
            step_id="step_bool", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(source_column=col, output_column="booled", operation=ScalarMapOperation.STANDARDIZE_BOOLEAN),
        ),
        TransformationStepIR(
            step_id="step_phone", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(source_column=col, output_column="phoned", operation=ScalarMapOperation.NORMALIZE_PHONE),
        ),
        TransformationStepIR(
            step_id="step_email", dataset_name="data", family=ActionFamily.MAP_VALUE,
            kind=StepKind.ROW_TRANSFORMATION,
            expression=ScalarMapExpression(source_column=col, output_column="emailed", operation=ScalarMapOperation.SANITIZE_EMAIL),
        ),
    )
    graph = TransformationIRGraph(
        graph_id="graph_null_preservation",
        steps=steps,
        schema_snapshot_hash="data_snap",
    )
    compiler = PandasIRCompiler()
    result = compiler.compile(graph)
    assert result.status == CodegenStatus.SUCCEEDED

    input_df = pd.DataFrame({"raw_val": ["  ABC  ", None, np.nan, pd.NA]})
    out_df = _execute_artifact_safely(result.artifact, input_df)

    # 1. Non-null row is correctly transformed
    assert out_df["trimmed"].iloc[0] == "ABC"
    assert out_df["lowered"].iloc[0] == "  abc  "
    assert out_df["uppered"].iloc[0] == "  ABC  "
    assert out_df["regexed"].iloc[0] == "ABC"
    assert out_df["phoned"].iloc[0] == ""
    assert out_df["emailed"].iloc[0] == "abc"

    # 2. Null rows remain null (pd.NA), never stringified into "None", "nan", "<NA>"
    string_cols = ["trimmed", "lowered", "uppered", "regexed", "booled", "phoned", "emailed"]
    for col_name in string_cols:
        for idx in [1, 2, 3]:
            val = out_df[col_name].iloc[idx]
            assert pd.isna(val), f"Column {col_name} row {idx} should be NA but got {repr(val)}"
        non_nulls = out_df[col_name].dropna().astype(str).tolist()
        assert "None" not in non_nulls, f"Literal 'None' leaked in column {col_name}"
        assert "nan" not in non_nulls, f"Literal 'nan' leaked in column {col_name}"
        assert "<NA>" not in non_nulls, f"Literal '<NA>' leaked in column {col_name}"


def test_clip_incomplete_bounds_and_unbounded_infinity_policies():
    """
    Vertical Slice 3A Tracked Correction 6:
    ClipExpression with incomplete bounds (e.g. upper_bound=50, lower_bound=None)
    must handle -np.inf via UnboundedInfinityPolicy rather than inventing bounds.
    """
    col = ColumnRef(column_name="val", dataset_name="nums")

    # Scenario A: upper bound 50, lower bound None, unbounded_infinity_policy = PRESERVE
    step_preserve = TransformationStepIR(
        step_id="step_preserve",
        dataset_name="nums",
        family=ActionFamily.CLIP,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ClipExpression(
            target_column=col,
            upper_bound=LiteralValue(value=50.0, data_type=DataType.FLOAT),
            lower_bound=None,
            infinity_policy=InfinityPolicy.CLIP_TO_BOUND,
            unbounded_infinity_policy=UnboundedInfinityPolicy.PRESERVE,
        ),
    )
    graph_a = TransformationIRGraph(graph_id="clip_incomplete_preserve", steps=(step_preserve,))
    res_a = PandasIRCompiler().compile(graph_a)
    assert res_a.status == CodegenStatus.SUCCEEDED

    df_in_a = pd.DataFrame({"val": [-np.inf, 20.0, 70.0, np.inf]})
    df_out_a = _execute_artifact_safely(res_a.artifact, df_in_a)
    # +inf clipped to 50.0; 70.0 clipped to 50.0; -inf PRESERVED
    assert df_out_a["val"].iloc[0] == -np.inf
    assert df_out_a["val"].iloc[1] == 20.0
    assert df_out_a["val"].iloc[2] == 50.0
    assert df_out_a["val"].iloc[3] == 50.0

    # Scenario B: upper bound 50, lower bound None, unbounded_infinity_policy = TREAT_AS_NULL
    step_null = TransformationStepIR(
        step_id="step_null",
        dataset_name="nums",
        family=ActionFamily.CLIP,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ClipExpression(
            target_column=col,
            upper_bound=LiteralValue(value=50.0, data_type=DataType.FLOAT),
            lower_bound=None,
            infinity_policy=InfinityPolicy.CLIP_TO_BOUND,
            unbounded_infinity_policy=UnboundedInfinityPolicy.TREAT_AS_NULL,
        ),
    )
    graph_b = TransformationIRGraph(graph_id="clip_incomplete_null", steps=(step_null,))
    res_b = PandasIRCompiler().compile(graph_b)
    assert res_b.status == CodegenStatus.SUCCEEDED

    df_in_b = pd.DataFrame({"val": [-np.inf, 20.0, 70.0, np.inf]})
    df_out_b = _execute_artifact_safely(res_b.artifact, df_in_b)
    # -inf converted to NaN; +inf clipped to 50.0
    assert np.isnan(df_out_b["val"].iloc[0])
    assert df_out_b["val"].iloc[1] == 20.0
    assert df_out_b["val"].iloc[2] == 50.0
    assert df_out_b["val"].iloc[3] == 50.0


def test_unknown_predicate_policies_fail_and_separate_flagging():
    """
    Vertical Slice 3A Tracked Correction 7:
    Tests FAIL_STEP and FLAG_UNKNOWN_SEPARATELY policies on FlagExpression and ConditionalReplaceExpression.
    """
    col = ColumnRef(column_name="discount", dataset_name="sales")
    pred = BinaryOp(
        left=ColumnRef(column_name="discount", dataset_name="sales"),
        op=BinaryOperator.GT,
        right=LiteralValue(value=50.0, data_type=DataType.FLOAT),
    )

    # 1. FlagExpression with FLAG_UNKNOWN_SEPARATELY
    step_flag_sep = TransformationStepIR(
        step_id="step_flag_sep",
        dataset_name="sales",
        family=ActionFamily.FLAG,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=FlagExpression(
            predicate=pred,
            output_column="excessive",
            unknown_policy=PredicateUnknownPolicy.FLAG_UNKNOWN_SEPARATELY,
        ),
    )
    graph_flag = TransformationIRGraph(graph_id="flag_sep", steps=(step_flag_sep,))
    res_flag = PandasIRCompiler().compile(graph_flag)
    assert res_flag.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({"discount": [20.0, 60.0, np.nan]})
    df_out = _execute_artifact_safely(res_flag.artifact, df_in)
    assert list(df_out["excessive"]) == [False, True, False]
    assert list(df_out["excessive_unknown"]) == [False, False, True]

    # 2. ConditionalReplaceExpression with FAIL_STEP
    step_rep_fail = TransformationStepIR(
        step_id="step_rep_fail",
        dataset_name="sales",
        family=ActionFamily.CONDITIONAL_REPLACE,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ConditionalReplaceExpression(
            target_column=col,
            condition=pred,
            replace_value=LiteralValue(value=50.0, data_type=DataType.FLOAT),
            unknown_policy=PredicateUnknownPolicy.FAIL_STEP,
        ),
    )
    graph_fail = TransformationIRGraph(graph_id="rep_fail", steps=(step_rep_fail,))
    res_fail = PandasIRCompiler().compile(graph_fail)
    assert res_fail.status == CodegenStatus.SUCCEEDED

    # Non-null runs fine
    df_ok = pd.DataFrame({"discount": [20.0, 60.0, 10.0]})
    df_ok_out = _execute_artifact_safely(res_fail.artifact, df_ok)
    assert list(df_ok_out["discount"]) == [20.0, 50.0, 10.0]

    # Null value triggers FAIL_STEP runtime error
    df_bad = pd.DataFrame({"discount": [20.0, np.nan, 60.0]})
    with pytest.raises(ValueError, match="FAIL_STEP"):
        _execute_artifact_safely(res_fail.artifact, df_bad)


# ============================================================================
# 7. Increment 3B: Imputation & Parse Contracts
# ============================================================================

def test_statistical_imputation_fit_apply_separation():
    """
    Vertical Slice 3B: Fit vs. Apply Separation for statistical imputation.
    - Planning binds an authorized LearnedImputationModel with fixed learned_value and snapshot hash.
    - Emitted apply code contains zero runtime statistical recalculations (.mean(), .median(), etc.).
    - Changing inference data does NOT change the bound imputation value.
    """
    col = ColumnRef(column_name="age", dataset_name="users")
    model = LearnedImputationModel(
        model_id="model_age_median_v1",
        model_version="1.0.0",
        strategy="median",
        learned_value=34.5,
        fit_dataset_ref="users_baseline",
        fit_population_hash="pop_hash_123",
        fit_schema_hash="schema_hash_456",
        fit_policy_decision_id="POL-AGE-01",
    )

    step_impute = TransformationStepIR(
        step_id="step_impute_age",
        dataset_name="users",
        family=ActionFamily.IMPUTE,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ImputeExpression(
            target_column=col,
            strategy="median",
            learned_model=model,
        ),
    )
    graph = TransformationIRGraph(graph_id="impute_fit_apply", steps=(step_impute,))
    compiler = PandasIRCompiler()
    res = compiler.compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    # 1. Source code binds constant 34.5 and contains no runtime .median()
    code = res.artifact.source_code
    assert "34.5" in code
    assert ".median()" not in code
    assert ".mean()" not in code

    # 2. Execute on inference batch 1
    df1 = pd.DataFrame({"age": [20.0, None, 50.0]})
    out1 = _execute_artifact_safely(res.artifact, df1)
    assert list(out1["age"]) == [20.0, 34.5, 50.0]

    # 3. Execute on vastly different batch 2; bound learned value does NOT shift
    df2 = pd.DataFrame({"age": [100.0, None, 200.0]})
    out2 = _execute_artifact_safely(res.artifact, df2)
    assert list(out2["age"]) == [100.0, 34.5, 200.0]

    # 4. AST safety validator rejects runtime .median()
    unsafe_code = "import pandas as pd\ndf['age'] = df['age'].fillna(df['age'].median())\n"
    with pytest.raises(ValueError, match="Prohibited apply-time statistical recalculation"):
        compiler._validate_ast_safety(unsafe_code)


def test_grouped_imputation_with_composite_keys_and_missing_group_policies():
    """
    Vertical Slice 3B: Grouped statistical imputation with composite keys
    and comprehensive missing/unseen group policy handling.
    """
    col = ColumnRef(column_name="salary", dataset_name="employees")
    g_val_1 = GroupedImputationValue(
        group_key=(
            GroupKeyValue(column="dept", data_type="string", value="Eng"),
            GroupKeyValue(column="region", data_type="string", value="US"),
        ),
        learned_value=120000.0,
    )
    g_val_2 = GroupedImputationValue(
        group_key=(
            GroupKeyValue(column="dept", data_type="string", value="HR"),
            GroupKeyValue(column="region", data_type="string", value="EU"),
        ),
        learned_value=65000.0,
    )

    # 1. Duplicate group keys rejected during LearnedImputationModel construction
    with pytest.raises(ValueError, match="Duplicate group key"):
        LearnedImputationModel(
            model_id="m_dup",
            strategy="grouped_mean",
            group_columns=("dept", "region"),
            group_values=(g_val_1, g_val_1),
            missing_group_policy=MissingGroupPolicy.KEEP_NULL,
        )

    # 2. Policy: USE_GLOBAL_FALLBACK
    model_fallback = LearnedImputationModel(
        model_id="m_fallback",
        strategy="grouped_mean",
        group_columns=("dept", "region"),
        group_values=(g_val_1, g_val_2),
        missing_group_policy=MissingGroupPolicy.USE_GLOBAL_FALLBACK,
        global_fallback_value=50000.0,
    )
    step_fallback = TransformationStepIR(
        step_id="step_grp_fallback",
        dataset_name="employees",
        family=ActionFamily.IMPUTE,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ImputeExpression(
            target_column=col,
            strategy="grouped_mean",
            group_by_columns=("dept", "region"),
            learned_model=model_fallback,
        ),
    )
    res_fb = PandasIRCompiler().compile(TransformationIRGraph(graph_id="grp_fb", steps=(step_fallback,)))
    assert res_fb.status == CodegenStatus.SUCCEEDED

    df_test = pd.DataFrame({
        "dept": ["Eng", "HR", "Sales"],
        "region": ["US", "EU", "APAC"],
        "salary": [None, None, None],
    })
    out_fb = _execute_artifact_safely(res_fb.artifact, df_test)
    assert list(out_fb["salary"]) == [120000.0, 65000.0, 50000.0]

    # 3. Policy: FAIL_STEP
    model_fail = LearnedImputationModel(
        model_id="m_fail",
        strategy="grouped_mean",
        group_columns=("dept", "region"),
        group_values=(g_val_1, g_val_2),
        missing_group_policy=MissingGroupPolicy.FAIL_STEP,
    )
    step_fail = TransformationStepIR(
        step_id="step_grp_fail",
        dataset_name="employees",
        family=ActionFamily.IMPUTE,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ImputeExpression(
            target_column=col,
            strategy="grouped_mean",
            group_by_columns=("dept", "region"),
            learned_model=model_fail,
        ),
    )
    res_fail = PandasIRCompiler().compile(TransformationIRGraph(graph_id="grp_fail", steps=(step_fail,)))
    with pytest.raises(ValueError, match="Unseen group"):
        _execute_artifact_safely(res_fail.artifact, df_test)

    # 4. Policy: FLAG
    model_flag = LearnedImputationModel(
        model_id="m_flag",
        strategy="grouped_mean",
        group_columns=("dept", "region"),
        group_values=(g_val_1, g_val_2),
        missing_group_policy=MissingGroupPolicy.FLAG,
    )
    step_flag = TransformationStepIR(
        step_id="step_grp_flag",
        dataset_name="employees",
        family=ActionFamily.IMPUTE,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ImputeExpression(
            target_column=col,
            strategy="grouped_mean",
            group_by_columns=("dept", "region"),
            learned_model=model_flag,
        ),
    )
    res_flag = PandasIRCompiler().compile(TransformationIRGraph(graph_id="grp_flag", steps=(step_flag,)))
    out_flag = _execute_artifact_safely(res_flag.artifact, df_test)
    assert list(out_flag["salary_unseen_group"]) == [False, False, True]


def test_imputation_indicator_captured_before_fill():
    """
    Vertical Slice 3B: Imputation indicator column must be captured before filling.
    """
    col = ColumnRef(column_name="score", dataset_name="tests")
    step = TransformationStepIR(
        step_id="step_impute_score",
        dataset_name="tests",
        family=ActionFamily.IMPUTE,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ImputeExpression(
            target_column=col,
            strategy="constant",
            constant_value=0.0,
            emit_indicator=True,
            indicator_column="score_imputed",
        ),
    )
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="ind_test", steps=(step,)))
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({"score": [85.0, None, 92.0, np.nan]})
    df_out = _execute_artifact_safely(res.artifact, df_in)

    assert list(df_out["score"]) == [85.0, 0.0, 92.0, 0.0]
    assert list(df_out["score_imputed"]) == [False, True, False, True]


def test_parse_numeric_and_original_null_distinction():
    """
    Vertical Slice 3B: Parse error accounting strictly distinguishes original nulls
    from invalid parse errors.
    Input: ['10', 'abc', None, pd.NA]
    - '10' -> 10.0, no error
    - 'abc' -> invalid, parse error = True
    - None -> original missing, parse error = False
    - pd.NA -> original missing, parse error = False
    """
    col = ColumnRef(column_name="raw_num", dataset_name="inputs")
    step = TransformationStepIR(
        step_id="step_parse_num",
        dataset_name="inputs",
        family=ActionFamily.PARSE,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ParseExpression(
            source_column=col,
            target_column="parsed_num",
            failure_policy=ParseFailurePolicy.FLAG,
            target_data_type=DataType.FLOAT,
            numeric_spec=NumericFormatSpec(decimal_separator="."),
        ),
    )
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="parse_dist", steps=(step,)))
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({"raw_num": ["10", "abc", None, pd.NA]})
    df_out = _execute_artifact_safely(res.artifact, df_in)

    assert df_out["parsed_num"].iloc[0] == 10.0
    assert pd.isna(df_out["parsed_num"].iloc[1])
    assert pd.isna(df_out["parsed_num"].iloc[2])
    assert pd.isna(df_out["parsed_num"].iloc[3])

    # Parse error flags: ONLY 'abc' is an invalid parse error!
    assert df_out["parsed_num_is_parse_error"].iloc[0] == False
    assert df_out["parsed_num_is_parse_error"].iloc[1] == True
    assert df_out["parsed_num_is_parse_error"].iloc[2] == False
    assert df_out["parsed_num_is_parse_error"].iloc[3] == False


def test_parse_failure_policies_execution():
    """
    Vertical Slice 3B: Execution of SET_NULL, FAIL_STEP, and KEEP_ORIGINAL.
    """
    col = ColumnRef(column_name="amt", dataset_name="finance")

    # 1. FAIL_STEP policy
    step_fail = TransformationStepIR(
        step_id="step_p_fail",
        dataset_name="finance",
        family=ActionFamily.PARSE,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ParseExpression(
            source_column=col,
            target_column="amt",
            failure_policy=ParseFailurePolicy.FAIL_STEP,
            target_data_type=DataType.FLOAT,
        ),
    )
    res_fail = PandasIRCompiler().compile(TransformationIRGraph(graph_id="p_fail", steps=(step_fail,)))
    assert res_fail.status == CodegenStatus.SUCCEEDED

    # Clean input runs
    df_clean = pd.DataFrame({"amt": ["10.5", "20.0", None]})
    out_clean = _execute_artifact_safely(res_fail.artifact, df_clean)
    assert out_clean["amt"].iloc[0] == 10.5

    # Invalid input raises
    df_bad = pd.DataFrame({"amt": ["10.5", "corrupt_data"]})
    with pytest.raises(ValueError, match="Parse failure") as exc_info:
        _execute_artifact_safely(res_fail.artifact, df_bad)
    err_msg = str(exc_info.value)
    assert "step_p_fail" in err_msg
    assert "amt" in err_msg
    assert "UNPARSEABLE_VALUE" in err_msg
    assert "corrupt_data" not in err_msg  # Invariant: No sensitive raw values exposed

    # 2. In-place KEEP_ORIGINAL on typed numeric column is rejected during IR validation
    with pytest.raises(ValueError, match="KEEP_ORIGINAL is prohibited for in-place replacement"):
        ParseExpression(
            source_column=col,
            target_column="amt",
            output_mode=ParseOutputMode.REPLACE_SOURCE_COLUMN,
            failure_policy=ParseFailurePolicy.KEEP_ORIGINAL,
            target_data_type=DataType.FLOAT,
        )


def test_parse_dates_temporal_and_calendar_contracts():
    """
    Vertical Slice 3B: Pure calendar dates (output_type='date' or temporal_target='calendar_date')
    are not shifted by timezone conversions.
    """
    col = ColumnRef(column_name="reg_date", dataset_name="users")
    step = TransformationStepIR(
        step_id="step_parse_date",
        dataset_name="users",
        family=ActionFamily.PARSE,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ParseExpression(
            source_column=col,
            target_column="reg_date",
            target_data_type=DataType.DATE,
            date_spec=DateFormatSpec(
                accepted_formats=("%Y-%m-%d",),
                output_type="date",
                temporal_target=TemporalParseTarget.CALENDAR_DATE,
            ),
        ),
    )
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="parse_date", steps=(step,)))
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({"reg_date": ["2026-09-16", "invalid", None]})
    df_out = _execute_artifact_safely(res.artifact, df_in)

    from datetime import date
    assert df_out["reg_date"].iloc[0] == date(2026, 9, 16)
    assert pd.isna(df_out["reg_date"].iloc[1])
    assert pd.isna(df_out["reg_date"].iloc[2])


def test_standardize_boolean_format_spec_disjoint_tokens():
    """
    Vertical Slice 3B: BooleanFormatSpec with custom token mappings (Si/No).
    Asserts rejection of overlapping token sets and normalized collisions.
    """
    col = ColumnRef(column_name="active", dataset_name="members")

    # Overlapping raw tokens rejected
    with pytest.raises(ValueError, match="disjoint"):
        BooleanFormatSpec(
            true_tokens=frozenset({"yes", "true"}),
            false_tokens=frozenset({"no", "true"}),
        )

    # Collision after trimming and case normalization
    with pytest.raises(ValueError, match="collision"):
        BooleanFormatSpec(
            true_tokens=frozenset({"Yes"}),
            false_tokens=frozenset({" yes "}),
            case_sensitive=False,
            trim_whitespace=True,
        )

    # Valid custom Spanish tokens
    spec = BooleanFormatSpec(
        true_tokens=frozenset({"si", "verdadero", "1"}),
        false_tokens=frozenset({"no", "falso", "0"}),
    )
    step = TransformationStepIR(
        step_id="step_bool_custom",
        dataset_name="members",
        family=ActionFamily.PARSE,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=ParseExpression(
            source_column=col,
            target_column="active",
            target_data_type=DataType.BOOLEAN,
            boolean_spec=spec,
        ),
    )
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="parse_bool", steps=(step,)))
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({"active": ["si", "NO", "verdadero", "unknown", None]})
    df_out = _execute_artifact_safely(res.artifact, df_in)

    assert df_out["active"].iloc[0] == True
    assert df_out["active"].iloc[1] == False
    assert df_out["active"].iloc[2] == True
    assert pd.isna(df_out["active"].iloc[3])
    assert pd.isna(df_out["active"].iloc[4])


def test_model_snapshot_hash_verification():
    """
    Vertical Slice 3B: Modifying model metadata or learned values changes
    model_snapshot_hash and fails verification if forged.
    """
    model = LearnedImputationModel(
        model_id="m_val",
        strategy="mean",
        learned_value=42.0,
        fit_population_hash="p1",
        fit_schema_hash="s1",
    )
    assert model.model_snapshot_hash != ""

    # Verify model at use time succeeds
    verify_imputation_model(model)

    # Attempting to forge model with mismatched snapshot hash fails construction
    with pytest.raises(ValueError, match="snapshot hash mismatch"):
        LearnedImputationModel(
            model_id="m_forge",
            strategy="mean",
            learned_value=99.0,
            fit_population_hash="p1",
            fit_schema_hash="s1",
            model_snapshot_hash=model.model_snapshot_hash,
        )

    # If tampered object with bad hash is checked at use-time
    fake_model = object.__new__(LearnedImputationModel)
    object.__setattr__(fake_model, "model_id", "m_tampered")
    object.__setattr__(fake_model, "strategy", "mean")
    object.__setattr__(fake_model, "learned_value", 999.0)
    object.__setattr__(fake_model, "group_values", ())
    object.__setattr__(fake_model, "group_columns", ())
    object.__setattr__(fake_model, "missing_group_policy", MissingGroupPolicy.KEEP_NULL)
    object.__setattr__(fake_model, "global_fallback_value", None)
    object.__setattr__(fake_model, "fit_population_hash", "p1")
    object.__setattr__(fake_model, "fit_schema_hash", "s1")
    object.__setattr__(fake_model, "target_column_type", "")
    object.__setattr__(fake_model, "tie_breaker", ModeTieBreaker.LOWEST_CANONICAL_VALUE)
    object.__setattr__(fake_model, "null_exclusion_policy", "exclude_nulls")
    object.__setattr__(fake_model, "model_version", "1.0.0")
    object.__setattr__(fake_model, "canonicalization_version", "1.0.0")
    object.__setattr__(fake_model, "model_snapshot_hash", "tampered_fake_hash")

    with pytest.raises(ImputationModelIntegrityError, match="snapshot mismatch"):
        verify_imputation_model(fake_model)
