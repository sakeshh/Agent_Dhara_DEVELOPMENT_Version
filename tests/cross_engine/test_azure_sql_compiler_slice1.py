"""
tests/cross_engine/test_azure_sql_compiler_slice1.py

Layer A: Automated local compiler and semantic validation tests for AzureSqlTsqlIRCompiler.
Verifies deterministic T-SQL generation, identifier quoting, original-vs-current value preservation
across CTE stages, static apply-only imputation checks, three-way validation accounting,
granular parse subtypes, and strict default-denial governance.
"""
import hashlib
from typing import Any, Dict, List, Optional, Tuple
import pytest

from agent.etl_pipeline.azure_sql_ir_compiler import (
    AzureSqlCompilationError,
    AzureSqlCompilationResult,
    AzureSqlSchemaBindingError,
    AzureSqlTsqlIRCompiler,
    AzureSqlUnsupportedCapabilityError,
    quote_tsql_identifier,
    quote_tsql_literal,
    register_azure_sql_slice1_capabilities,
)
from agent.etl_pipeline.cross_engine.capability_models import (
    CapabilityStatus,
    FailureCategory,
)
from agent.etl_pipeline.cross_engine.capability_registry import (
    EngineCapabilityRegistry,
)
from agent.etl_pipeline.cross_engine.runtime_profiles import (
    AzureSqlDatabaseRuntimeProfile,
)
from agent.etl_pipeline.transformation_ir import (
    ActionFamily,
    BinaryOp,
    BinaryOperator,
    ClipExpression,
    ColumnRef,
    ConditionalReplaceExpression,
    DataType,
    FlagExpression,
    ImputeExpression,
    LearnedImputationModel,
    LiteralValue,
    NullCheck,
    ParseExpression,
    ParseFailurePolicy,
    PredicateUnknownPolicy,
    ScalarMapExpression,
    ScalarMapOperation,
    StepKind,
    TransformationStepIR,
    UnaryOp,
    UnaryOperator,
    ValidateExpression,
    ValidationUnknownPolicy,
    ValueVersion,
)


@pytest.fixture
def test_registry():
    """Provides an isolated capability registry with verified slice 1 capabilities for compiler tests."""
    reg = EngineCapabilityRegistry()
    prof = AzureSqlDatabaseRuntimeProfile()
    register_azure_sql_slice1_capabilities(
        registry=reg,
        profile=prof,
        status=CapabilityStatus.VERIFIED,
        certification_commit="slice1_compiler_test",
        verified_test_ids=("tests/cross_engine/test_azure_sql_compiler_slice1.py",),
    )
    return reg


@pytest.fixture
def compiler(test_registry):
    prof = AzureSqlDatabaseRuntimeProfile()
    return AzureSqlTsqlIRCompiler(runtime_profile=prof, capability_registry=test_registry)


def _make_step(step_id: str, family: ActionFamily, expr: Any) -> TransformationStepIR:
    return TransformationStepIR(
        step_id=step_id,
        dataset_name="primary",
        kind=StepKind.ROW_TRANSFORMATION,
        family=family,
        expression=expr,
    )


# ============================================================================
# 1. Identifier Quoting & Literal Formatting
# ============================================================================

def test_identifier_quoting():
    assert quote_tsql_identifier("customer_id") == "[customer_id]"
    assert quote_tsql_identifier("[already_bracketed]") == "[already_bracketed]"
    assert quote_tsql_identifier("col]with]brackets") == "[col]]with]]brackets]"
    assert quote_tsql_identifier("   spaced_name   ") == "[spaced_name]"


def test_literal_formatting():
    assert quote_tsql_literal(None) == "NULL"
    assert quote_tsql_literal(True) == "CAST(1 AS BIT)"
    assert quote_tsql_literal(False) == "CAST(0 AS BIT)"
    assert quote_tsql_literal(42) == "42"
    assert quote_tsql_literal(3.1415) == "3.1415"
    assert quote_tsql_literal("O'Connor") == "'O''Connor'"


# ============================================================================
# 2. Dynamic Runtime Profile Evidence Construction
# ============================================================================

def test_runtime_profile_from_captured_evidence():
    prof = AzureSqlDatabaseRuntimeProfile.from_captured_evidence(
        engine_version="12.0.2000.8",
        engine_edition="5",
        database_engine_version="Microsoft SQL Azure (RTM) - 12.0.2000.8",
        compatibility_level="160",
        database_collation="Latin1_General_100_CI_AS_SC_UTF8",
        server_collation="SQL_Latin1_General_CP1_CI_AS",
        ansi_nulls_enabled=True,
        ansi_warnings_enabled=True,
    )
    assert prof.engine_name == "azure_sql_database"
    assert prof.dialect == "tsql"
    assert prof.compatibility_level == "160"
    assert prof.case_sensitive is False # CI = Case Insensitive
    assert prof.compute_hash() is not None

    # Test Case Sensitive collation derivation
    cs_prof = AzureSqlDatabaseRuntimeProfile.from_captured_evidence(
        engine_version="12.0.2000.8",
        engine_edition="5",
        database_engine_version="Microsoft SQL Azure (RTM) - 12.0.2000.8",
        compatibility_level="160",
        database_collation="Latin1_General_100_CS_AS_SC_UTF8",
    )
    assert cs_prof.case_sensitive is True # CS = Case Sensitive


# ============================================================================
# 3. Scalar Map Compilation (TRIM, UPPER, LOWER)
# ============================================================================

def test_scalar_map_trim_upper_lower(compiler):
    step_trim = _make_step(
        step_id="step_trim",
        family=ActionFamily.MAP_VALUE,
        expr=ScalarMapExpression(
            operation=ScalarMapOperation.TRIM,
            source_column=ColumnRef(column_name="name"),
            output_column="name_trimmed",
        ),
    )
    step_upper = _make_step(
        step_id="step_upper",
        family=ActionFamily.MAP_VALUE,
        expr=ScalarMapExpression(
            operation=ScalarMapOperation.UPPER,
            source_column=ColumnRef(column_name="code"),
            output_column="code_upper",
        ),
    )
    step_lower = _make_step(
        step_id="step_lower",
        family=ActionFamily.MAP_VALUE,
        expr=ScalarMapExpression(
            operation=ScalarMapOperation.LOWER,
            source_column=ColumnRef(column_name="email"),
            output_column="email_lower",
        ),
    )

    res = compiler.compile_plan([step_trim, step_upper, step_lower], input_columns=["name", "code", "email"])
    sql = res.query_sql

    assert "TRIM([source_stage].[name]) AS [name_trimmed]" in sql
    assert "UPPER([step_001_step_trim].[code]) AS [code_upper]" in sql
    assert "LOWER([step_002_step_upper].[email]) AS [email_lower]" in sql
    assert "SELECT" in sql and "FROM [step_003_step_lower]" in sql


# ============================================================================
# 4. Conditional Replace with 3-Valued Logic
# ============================================================================

def test_conditional_replace_three_valued_logic(compiler):
    step = _make_step(
        step_id="step_cond_rep",
        family=ActionFamily.CONDITIONAL_REPLACE,
        expr=ConditionalReplaceExpression(
            target_column=ColumnRef(column_name="status"),
            condition=BinaryOp(
                left=ColumnRef(column_name="status"),
                op=BinaryOperator.EQ,
                right=LiteralValue(value="DISCONTINUED"),
            ),
            replace_value=LiteralValue(value="ARCHIVED"),
            unknown_policy=PredicateUnknownPolicy.TREAT_AS_FALSE,
        ),
    )
    res = compiler.compile_plan([step], input_columns=["id", "status"])
    sql = res.query_sql

    assert "CASE WHEN ([source_stage].[status] = 'DISCONTINUED') THEN 'ARCHIVED' ELSE [source_stage].[status] END AS [status]" in sql


# ============================================================================
# 5. Numerical Clip and Audit Companion Flag
# ============================================================================

def test_clip_and_flag_compilation(compiler):
    step_flag = _make_step(
        step_id="flag_excessive_discount",
        family=ActionFamily.FLAG,
        expr=FlagExpression(
            predicate=BinaryOp(
                left=ColumnRef(column_name="discount"),
                op=BinaryOperator.GT,
                right=LiteralValue(value=0.5),
            ),
            output_column="is_excessive_discount",
            true_value=1,
            false_value=0,
            unknown_policy=PredicateUnknownPolicy.TREAT_AS_FALSE,
        ),
    )
    step_clip = _make_step(
        step_id="clip_discount",
        family=ActionFamily.CLIP,
        expr=ClipExpression(
            target_column=ColumnRef(column_name="discount"),
            lower_bound=LiteralValue(value=0.0),
            upper_bound=LiteralValue(value=0.5),
        ),
    )

    res = compiler.compile_plan([step_flag, step_clip], input_columns=["order_id", "discount"])
    sql = res.query_sql

    assert "CASE WHEN ([source_stage].[discount] > 0.5) THEN 1 ELSE 0 END AS [is_excessive_discount]" in sql
    assert "CASE WHEN [step_001_flag_excessive_discount].[discount] < 0.0 THEN 0.0 WHEN [step_001_flag_excessive_discount].[discount] > 0.5 THEN 0.5 ELSE [step_001_flag_excessive_discount].[discount] END AS [discount]" in sql


# ============================================================================
# 6. Granular Parsing Subtypes & Style Codes
# ============================================================================

def test_parse_subtypes_and_styles(compiler):
    step_int = _make_step(
        step_id="parse_int",
        family=ActionFamily.PARSE,
        expr=ParseExpression(
            source_column=ColumnRef(column_name="int_str"),
            target_data_type=DataType.INTEGER,
            target_column="int_val",
            failure_policy=ParseFailurePolicy.FLAG,
        ),
    )
    step_date = _make_step(
        step_id="parse_date",
        family=ActionFamily.PARSE,
        expr=ParseExpression(
            source_column=ColumnRef(column_name="date_str"),
            target_data_type=DataType.DATE,
            target_column="date_val",
            failure_policy=ParseFailurePolicy.SET_NULL,
        ),
    )
    step_dt = _make_step(
        step_id="parse_datetime",
        family=ActionFamily.PARSE,
        expr=ParseExpression(
            source_column=ColumnRef(column_name="dt_str"),
            target_data_type=DataType.TIMESTAMP,
            target_column="dt_val",
            failure_policy=ParseFailurePolicy.SET_NULL,
        ),
    )

    res = compiler.compile_plan([step_int, step_date, step_dt], input_columns=["int_str", "date_str", "dt_str"])
    sql = res.query_sql

    assert "TRY_CAST([source_stage].[int_str] AS INT) AS [int_val]" in sql
    assert "int_val_is_parse_error" in sql
    assert "TRY_CONVERT(DATE, [step_001_parse_int].[date_str], 23) AS [date_val]" in sql
    assert "TRY_CONVERT(DATETIME2, [step_002_parse_date].[dt_str], 126) AS [dt_val]" in sql


# ============================================================================
# 7. Apply-Only Imputation: Static & Pre-Fitted Divergent Fixture
# ============================================================================

def test_impute_constant_and_learned_model(compiler):
    # 1. Constant imputation
    step_const = _make_step(
        step_id="impute_const",
        family=ActionFamily.IMPUTE,
        expr=ImputeExpression(
            target_column=ColumnRef(column_name="category"),
            strategy="constant",
            constant_value="UNKNOWN",
            emit_indicator=True,
            indicator_column="category_was_imputed",
        ),
    )

    # 2. Pre-fitted learned mean (divergent fixture: pre-fitted mean 50, execution mean 1000)
    learned = LearnedImputationModel(
        model_id="model_mean_01",
        strategy="mean",
        learned_value=50.0,
        fit_population_hash="hash_pop_123",
        fit_schema_hash="hash_sch_123",
    )
    step_mean = _make_step(
        step_id="impute_mean",
        family=ActionFamily.IMPUTE,
        expr=ImputeExpression(
            target_column=ColumnRef(column_name="score"),
            strategy="mean",
            learned_model=learned,
        ),
    )

    res = compiler.compile_plan([step_const, step_mean], input_columns=["category", "score"])
    sql = res.query_sql

    assert "COALESCE([source_stage].[category], 'UNKNOWN') AS [category]" in sql
    assert "category_was_imputed" in sql
    assert "COALESCE([step_001_impute_const].[score], 50.0) AS [score]" in sql


def test_impute_rejects_aggregate_learning_and_tampered_hash(compiler):
    # 1. Reject tampered model snapshot hash (caught at construction or compiler pre-flight)
    valid_model = LearnedImputationModel(
        model_id="bad_model",
        strategy="mean",
        learned_value=42.0,
        fit_population_hash="hash_pop_123",
        fit_schema_hash="hash_sch_123",
    )
    object.__setattr__(valid_model, "learned_value", 999.0) # Tamper value after creation!

    with pytest.raises((AzureSqlCompilationError, ValueError)):
        step_bad = _make_step(
            step_id="impute_tampered",
            family=ActionFamily.IMPUTE,
            expr=ImputeExpression(
                target_column=ColumnRef(column_name="score"),
                strategy="mean",
                learned_model=valid_model,
            ),
        )
        compiler.compile_plan([step_bad], input_columns=["score"])

    # 2. Reject aggregate tokens in apply artifact
    step_agg = _make_step(
        step_id="impute_agg",
        family=ActionFamily.IMPUTE,
        expr=ImputeExpression(
            target_column=ColumnRef(column_name="score"),
            strategy="constant",
            constant_value="AVG([score])",
        ),
    )
    with pytest.raises(AzureSqlCompilationError) as exc:
        compiler.compile_plan([step_agg], input_columns=["score"])
    assert exc.value.reason_code == "DYNAMIC_FIT_PROHIBITED"


# ============================================================================
# 8. Original vs. Current Value Pre-Image Semantics in CTE Stages
# ============================================================================

def test_original_vs_current_pre_image_preservation(compiler):
    """
    Verifies that mutating a column preserves its ORIGINAL value for downstream steps
    that explicitly request ValueVersion.ORIGINAL.
    Step 1: Mutate [amount] (multiply by 2)
    Step 2: Flag comparing CURRENT [amount] vs ORIGINAL [amount]
    """
    step1_mutate = _make_step(
        step_id="mutate_amount",
        family=ActionFamily.CONDITIONAL_REPLACE,
        expr=ConditionalReplaceExpression(
            target_column=ColumnRef(column_name="amount"),
            condition=LiteralValue(value=True),
            replace_value=BinaryOp(
                left=ColumnRef(column_name="amount", version=ValueVersion.CURRENT),
                op=BinaryOperator.MUL,
                right=LiteralValue(value=2),
            ),
        ),
    )
    step2_flag = _make_step(
        step_id="flag_growth",
        family=ActionFamily.FLAG,
        expr=FlagExpression(
            predicate=BinaryOp(
                left=ColumnRef(column_name="amount", version=ValueVersion.CURRENT),
                op=BinaryOperator.GT,
                right=ColumnRef(column_name="amount", version=ValueVersion.ORIGINAL),
            ),
            output_column="amount_doubled",
            true_value=1,
            false_value=0,
        ),
    )

    res = compiler.compile_plan([step1_mutate, step2_flag], input_columns=["id", "amount"])
    sql = res.query_sql

    # Step 1 should project shadow __orig_amount from source_stage
    assert "[source_stage].[amount] AS [__orig_amount]" in sql
    # Step 2 should reference CURRENT amount and shadow __orig_amount
    assert "[step_001_mutate_amount].[amount] > [step_001_mutate_amount].[__orig_amount]" in sql
    # Final projection must omit internal shadow column
    assert "__orig_amount" not in res.output_columns
    assert "amount_doubled" in res.output_columns


# ============================================================================
# 9. Three-Way Validation Accounting
# ============================================================================

def test_three_way_validation_query_generation(compiler):
    step_val = _make_step(
        step_id="val_non_negative_balance",
        family=ActionFamily.VALIDATE,
        expr=ValidateExpression(
            predicate=BinaryOp(
                left=ColumnRef(column_name="balance"),
                op=BinaryOperator.GTE,
                right=LiteralValue(value=0),
            ),
            unknown_policy=ValidationUnknownPolicy.FAIL_STEP,
        ),
    )
    res = compiler.compile_plan([step_val], input_columns=["account_id", "balance"])
    assert len(res.validation_queries) == 1
    vq = res.validation_queries[0]

    assert "COUNT_BIG(*) AS candidate_count" in vq
    assert "SUM(CASE WHEN ([source_stage].[balance] >= 0) THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS pass_count" in vq
    assert "SUM(CASE WHEN NOT ([source_stage].[balance] >= 0) THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS violation_count" in vq
    assert "SUM(CASE WHEN ([source_stage].[balance] >= 0) IS NULL THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS raw_unknown_count" in vq


# ============================================================================
# 10. Separation of Compiler Artifact & Certification Harness Script
# ============================================================================

def test_certification_harness_generation(compiler):
    step = _make_step(
        step_id="trim_name",
        family=ActionFamily.MAP_VALUE,
        expr=ScalarMapExpression(
            operation=ScalarMapOperation.TRIM,
            source_column=ColumnRef(column_name="name"),
            output_column="name",
        ),
    )
    res = compiler.compile_plan([step], input_columns=["id", "name"])

    # Harness script generation with mandatory session settings and isolated temp table
    harness = res.generate_certification_harness(
        table_name="#dhara_input",
        column_types={"id": "INT", "name": "NVARCHAR(100)"},
        rows=[{"id": 1, "name": "  Alice  "}, {"id": 2, "name": "Bob"}],
    )

    assert "SET NOCOUNT ON;" in harness
    assert "SET XACT_ABORT ON;" in harness
    assert "SET ANSI_NULLS ON;" in harness
    assert "SET ANSI_WARNINGS ON;" in harness
    assert "SET ARITHABORT ON;" in harness
    assert "CREATE TABLE #dhara_input" in harness
    assert "INSERT INTO #dhara_input" in harness
    assert "DROP TABLE IF EXISTS #dhara_input;" in harness
    assert res.query_sql in harness


# ============================================================================
# 11. Default-Denial Governance on Uncertified Capabilities
# ============================================================================

def test_default_denial_blocks_uncertified_capabilities():
    # Fresh compiler with non-verified / default registry
    empty_reg = EngineCapabilityRegistry()
    prof = AzureSqlDatabaseRuntimeProfile()
    strict_compiler = AzureSqlTsqlIRCompiler(runtime_profile=prof, capability_registry=empty_reg)

    step_unregistered = _make_step(
        step_id="step_unreg",
        family=ActionFamily.MAP_VALUE,
        expr=ScalarMapExpression(
            operation=ScalarMapOperation.TRIM,
            source_column=ColumnRef(column_name="name"),
            output_column="name",
        ),
    )

    with pytest.raises(AzureSqlUnsupportedCapabilityError) as exc:
        strict_compiler.compile_plan([step_unregistered], input_columns=["name"])
    assert exc.value.reason_code == "UNSUPPORTED_AZURE_SQL_CAPABILITY"
    assert exc.value.category == FailureCategory.UNSUPPORTED_CAPABILITY
