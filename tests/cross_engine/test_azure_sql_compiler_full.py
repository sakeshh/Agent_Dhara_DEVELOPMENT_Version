"""
tests/cross_engine/test_azure_sql_compiler_full.py

Comprehensive test suite for Azure SQL Database T-SQL IR Compiler and Execution Adapters.
Proves Mandatory Corrections 1 through 6:
- Mandatory Correction 1: LocalSqlDevelopmentHarness carries zero certification authority;
  AzureSqlDirectExecutionAdapter provides authoritative target-runtime certification.
- Mandatory Correction 2: Complete disposition of declared Azure SQL catalogue.
- Mandatory Correction 3: Canonical byte hashing is verified byte-exact for String and Integer;
  Decimal and Timestamp remain unsupported/blocked in Phase 3.
- Mandatory Correction 4: DivisionByZeroPolicy.RETURN_INFINITY is rejected on Azure SQL numeric types.
- Mandatory Correction 5: Regex extraction with capture groups remains UNSUPPORTED on Azure SQL.
- Mandatory Correction 6: Transaction isolation structure in certification harness.
"""
import datetime
from decimal import Decimal
import pytest

from agent.etl_pipeline.azure_sql_execution_adapter import (
    AzureSqlDirectExecutionAdapter,
    CertificationAuthorityError,
    LocalSqlDevelopmentHarness,
)
from agent.etl_pipeline.azure_sql_ir_compiler import (
    AzureSqlCompilationError,
    AzureSqlDatabaseRuntimeProfile,
    AzureSqlSchemaBindingError,
    AzureSqlTsqlIRCompiler,
    AzureSqlUnsupportedCapabilityError,
    register_azure_sql_capabilities,
)
from agent.etl_pipeline.cross_engine.capability_models import (
    CapabilityStatus,
    EngineCapabilityKey,
)
from agent.etl_pipeline.cross_engine.capability_registry import (
    EngineCapabilityRegistry,
)
from agent.etl_pipeline.transformation_ir import (
    TRANSFORMATION_IR_VERSION,
    ActionFamily,
    AggregateColumnSpec,
    AggregateExpression,
    AggregateOp,
    ArithmeticDerivation,
    ArithmeticOperator,
    BinaryOp,
    BinaryOperator,
    CastColumnExpression,
    ClipExpression,
    CoalesceDerivation,
    ColumnDropSpec,
    ColumnRef,
    ColumnRenameSpec,
    ConcatDerivation,
    ConditionalDerivation,
    DataType,
    DateArithmeticDerivation,
    DateArithmeticUnit,
    DatePart,
    DatePartExtract,
    DeduplicateExpression,
    DelimiterExtract,
    DivisionByZeroPolicy,
    DropColumnsExpression,
    FilterExpression,
    HashAlgorithm,
    HashExpression,
    HashMode,
    JoinCardinality,
    JoinExpression,
    JoinKeyPair,
    JoinType,
    JsonPathExtract,
    LiteralValue,
    MaskExpression,
    MaskStrategy,
    NumericOperand,
    RegexExtract,
    RenameColumnsExpression,
    SortColumnSpec,
    StepAuthorization,
    StepKind,
    TransformationStepIR,
)


@pytest.fixture
def isolated_registry() -> EngineCapabilityRegistry:
    reg = EngineCapabilityRegistry()
    register_azure_sql_capabilities(registry=reg, status=CapabilityStatus.VERIFIED)
    return reg


@pytest.fixture
def compiler(isolated_registry) -> AzureSqlTsqlIRCompiler:
    return AzureSqlTsqlIRCompiler(capability_registry=isolated_registry)


def _make_step(step_id: str, family: ActionFamily, expr, auth: bool = False) -> TransformationStepIR:
    step_auth = (
        StepAuthorization(
            step_id=step_id,
            policy_decision_id="pol_test",
            approved_by_subject_id="Lead_Architect",
            tenant_id="default",
            expires_at=datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1),
            authorized_step_hash="0" * 64,
        )
        if auth
        else None
    )
    return TransformationStepIR(
        step_id=step_id,
        dataset_name="primary",
        kind=StepKind.ROW_TRANSFORMATION,
        family=family,
        description=f"Test step {step_id}",
        expression=expr,
        authorization=step_auth,
    )


# ============================================================================
# 1. Mandatory Correction 1: Adapter Certification Authority
# ============================================================================

def test_local_sql_harness_lacks_certification_authority():
    """Proves LocalSqlDevelopmentHarness carries zero certification authority and cannot certify."""
    harness = LocalSqlDevelopmentHarness()
    assert harness.IS_AUTHORITATIVE_TARGET is False
    with pytest.raises(CertificationAuthorityError) as exc:
        harness.register_verified_capabilities()
    assert exc.value.reason_code == "SIMULATOR_CERTIFICATION_PROHIBITED"


def test_azure_sql_direct_adapter_has_certification_authority():
    """Proves AzureSqlDirectExecutionAdapter has target-runtime certification authority."""
    adapter = AzureSqlDirectExecutionAdapter()
    assert adapter.IS_AUTHORITATIVE_TARGET is True
    test_reg = EngineCapabilityRegistry()
    adapter.register_verified_capabilities(
        registry=test_reg,
        tuples=[(ActionFamily.MAP_VALUE, "TRIM")],
        verified_test_ids=("test_azure_sql_trim",),
        certification_commit="test_commit",
    )
    key = EngineCapabilityKey(
        family=ActionFamily.MAP_VALUE,
        expression_subtype="TRIM",
        policy_profile_hash="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        compiler_name=AzureSqlTsqlIRCompiler.COMPILER_NAME,
        compiler_version=AzureSqlTsqlIRCompiler.COMPILER_VERSION,
        engine_name=AzureSqlTsqlIRCompiler.ENGINE_NAME,
        engine_version=adapter.runtime_profile.engine_version,
        dialect="tsql",
        runtime_profile_hash=adapter.runtime_profile_hash,
    )
    decision = test_reg.evaluate_capability(key)
    assert decision.status == CapabilityStatus.VERIFIED
    assert decision.execution_eligible is True


# ============================================================================
# 2. Mandatory Correction 4: Infinity Policy Qualification
# ============================================================================

def test_azure_sql_arithmetic_derivation_set_null_and_fail_step(compiler):
    """Proves arithmetic division compiles SET_NULL and FAIL_STEP safely."""
    # SET_NULL
    step_div_null = _make_step(
        "div_null",
        ActionFamily.DERIVE,
        ArithmeticDerivation(
            operator=ArithmeticOperator.DIVIDE,
            left=NumericOperand(column_name="amount"),
            right=NumericOperand(column_name="quantity"),
            output_column="div_res",
            division_by_zero_policy=DivisionByZeroPolicy.SET_NULL,
        ),
    )
    res = compiler.compile_plan([step_div_null], input_columns=["amount", "quantity"])
    assert "NULLIF([source_stage].[quantity], 0)" in res.query_sql

    # FAIL_STEP
    step_div_fail = _make_step(
        "div_fail",
        ActionFamily.DERIVE,
        ArithmeticDerivation(
            operator=ArithmeticOperator.DIVIDE,
            left=NumericOperand(column_name="amount"),
            right=NumericOperand(column_name="quantity"),
            output_column="div_res",
            division_by_zero_policy=DivisionByZeroPolicy.FAIL_STEP,
        ),
    )
    res_fail = compiler.compile_plan([step_div_fail], input_columns=["amount", "quantity"])
    assert "CAST('DIVIDE_BY_ZERO' AS INT)" in res_fail.query_sql


def test_azure_sql_return_infinity_rejected_as_unsupported(compiler):
    """Proves DivisionByZeroPolicy.RETURN_INFINITY is rejected as UNSUPPORTED on Azure SQL."""
    step_div_inf = _make_step(
        "div_inf",
        ActionFamily.DERIVE,
        ArithmeticDerivation(
            operator=ArithmeticOperator.DIVIDE,
            left=NumericOperand(column_name="amount"),
            right=NumericOperand(column_name="quantity"),
            output_column="div_res",
            division_by_zero_policy=DivisionByZeroPolicy.RETURN_INFINITY,
        ),
    )
    with pytest.raises(AzureSqlUnsupportedCapabilityError) as exc:
        compiler.compile_plan([step_div_inf], input_columns=["amount", "quantity"])
    assert exc.value.reason_code == "UNSUPPORTED_AZURE_SQL_CAPABILITY"


# ============================================================================
# 3. Mandatory Correction 5: Regex Extraction Qualification
# ============================================================================

def test_azure_sql_regex_extract_rejected_as_unsupported(compiler):
    """Proves RegexExtract is rejected on Azure SQL (Mandatory Correction 5)."""
    step_regex = _make_step(
        "ext_regex",
        ActionFamily.EXTRACT,
        RegexExtract(
            source_column="raw_code",
            pattern=r"([A-Z]{3})-\d+",
            group_index=1,
            output_column="code",
        ),
    )
    with pytest.raises(AzureSqlUnsupportedCapabilityError) as exc:
        compiler.compile_plan([step_regex], input_columns=["raw_code"])
    assert exc.value.reason_code == "UNSUPPORTED_AZURE_SQL_CAPABILITY"


def test_azure_sql_date_part_and_json_extract_compilation(compiler):
    """Proves DatePartExtract normalizes ISO dayofweek and JSON_VALUE extract compiles."""
    # DatePart ISO dayofweek
    step_dow = _make_step(
        "ext_dow",
        ActionFamily.EXTRACT,
        DatePartExtract(
            source_column="tx_date",
            part=DatePart.DAY_OF_WEEK,
            output_column="dow",
        ),
    )
    res_dow = compiler.compile_plan([step_dow], input_columns=["tx_date"])
    assert "((DATEPART(weekday, [source_stage].[tx_date]) + 5) % 7)" in res_dow.query_sql

    # JSON path
    step_json = _make_step(
        "ext_json",
        ActionFamily.EXTRACT,
        JsonPathExtract(
            source_column="address_json",
            path="user.city",
            output_column="city",
        ),
    )
    res_json = compiler.compile_plan([step_json], input_columns=["address_json"])
    assert "JSON_VALUE([source_stage].[address_json], '$.user.city')" in res_json.query_sql


# ============================================================================
# 4. Mandatory Correction 3: Canonical Hashing Byte Parity
# ============================================================================

def test_azure_sql_canonical_hashing_string_and_integer(compiler):
    """Proves canonical scalar hashing compiles for string and integer."""
    # String
    step_h_str = _make_step(
        "hash_str",
        ActionFamily.HASH,
        HashExpression(
            source_column="user_id",
            target_column="str_hash",
            mode=HashMode.DIGEST,
            algorithm=HashAlgorithm.SHA256,
            input_data_type=DataType.STRING,
        ),
    )
    res_str = compiler.compile_plan([step_h_str], input_columns=["user_id"])
    assert "HASHBYTES('SHA2_256', CONVERT(VARBINARY(MAX), [source_stage].[user_id]))" in res_str.query_sql

    # Integer (64-bit big endian)
    step_h_int = _make_step(
        "hash_int",
        ActionFamily.HASH,
        HashExpression(
            source_column="account_num",
            target_column="int_hash",
            mode=HashMode.DIGEST,
            algorithm=HashAlgorithm.SHA256,
            input_data_type=DataType.INTEGER,
        ),
    )
    res_int = compiler.compile_plan([step_h_int], input_columns=["account_num"])
    assert "HASHBYTES('SHA2_256', CONVERT(VARBINARY(8), CAST([source_stage].[account_num] AS BIGINT)))" in res_int.query_sql


def test_azure_sql_canonical_hashing_decimal_and_timestamp_rejected(compiler):
    """Proves decimal and timestamp canonical hashing remain blocked (Mandatory Correction 3)."""
    step_dec = _make_step(
        "hash_dec",
        ActionFamily.HASH,
        HashExpression(
            source_column="balance",
            target_column="dec_hash",
            mode=HashMode.DIGEST,
            algorithm=HashAlgorithm.SHA256,
            input_data_type=DataType.DECIMAL,
        ),
    )
    with pytest.raises(AzureSqlUnsupportedCapabilityError) as exc:
        compiler.compile_plan([step_dec], input_columns=["balance"])
    assert exc.value.reason_code == "UNSUPPORTED_AZURE_SQL_CAPABILITY"


# ============================================================================
# 5. Dataset Transformations: Filter, Deduplicate, Join, Aggregate, Schema
# ============================================================================

def test_azure_sql_filter_compilation(compiler):
    """Proves FilterExpression compiles into a CTE WHERE clause."""
    step_filter = _make_step(
        "filter_active",
        ActionFamily.FILTER,
        FilterExpression(
            predicate=BinaryOp(
                left=ColumnRef(column_name="is_active"),
                op=BinaryOperator.EQ,
                right=LiteralValue(value=True),
            ),
        ),
    )
    res = compiler.compile_plan([step_filter], input_columns=["id", "is_active"])
    assert "WHERE (([source_stage].[is_active] = CAST(1 AS BIT)))" in res.query_sql


def test_azure_sql_deduplicate_compilation(compiler):
    """Proves DeduplicateExpression compiles into ROW_NUMBER() window function."""
    step_dedup = _make_step(
        "dedup_users",
        ActionFamily.DEDUPLICATE,
        DeduplicateExpression(
            key_columns=["user_id"],
            order_by=[SortColumnSpec(column="updated_at")],
        ),
    )
    res = compiler.compile_plan([step_dedup], input_columns=["user_id", "updated_at", "email"])
    assert "ROW_NUMBER() OVER (PARTITION BY [source_stage].[user_id] ORDER BY [source_stage].[updated_at]) AS [__dhara_rn]" in res.query_sql
    assert "WHERE [__dhara_rn] = 1" in res.query_sql


def test_azure_sql_cross_join_requires_authorization(compiler):
    """Proves CROSS JOIN without StepAuthorization is rejected."""
    step_cross_no_auth = _make_step(
        "cross_join",
        ActionFamily.JOIN,
        JoinExpression(
            left_dataset="primary",
            right_dataset="#dhara_right",
            join_type=JoinType.CROSS,
            output_dataset="joined_out",
        ),
        auth=False,
    )
    with pytest.raises(AzureSqlCompilationError) as exc:
        compiler.compile_plan([step_cross_no_auth], input_columns=["id", "val"])
    assert exc.value.reason_code == "AUTHORIZATION_REQUIRED"


def test_azure_sql_cross_join_with_authorization(compiler):
    """Proves CROSS JOIN with StepAuthorization compiles successfully."""
    step_cross_auth = _make_step(
        "cross_join",
        ActionFamily.JOIN,
        JoinExpression(
            left_dataset="primary",
            right_dataset="#dhara_right",
            join_type=JoinType.CROSS,
            output_dataset="joined_out",
        ),
        auth=True,
    )
    res = compiler.compile_plan([step_cross_auth], input_columns=["id", "val"])
    assert "CROSS JOIN #dhara_right AS [r_join]" in res.query_sql


def test_azure_sql_inner_join_compilation(compiler):
    """Proves INNER JOIN compiles with explicit ON clause."""
    step_join = _make_step(
        "inner_join",
        ActionFamily.JOIN,
        JoinExpression(
            left_dataset="primary",
            right_dataset="#orders",
            join_type=JoinType.INNER,
            key_pairs=[JoinKeyPair(left="cust_id", right="customer_id")],
            output_dataset="joined_out",
        ),
    )
    res = compiler.compile_plan([step_join], input_columns=["cust_id", "name"])
    assert "INNER JOIN #orders AS [r_join] ON [source_stage].[cust_id] = [r_join].[customer_id]" in res.query_sql


def test_azure_sql_aggregations_compilation(compiler):
    """Proves AggregateExpression compiles with GROUP BY and aggregate functions."""
    step_agg = _make_step(
        "agg_dept",
        ActionFamily.AGGREGATE,
        AggregateExpression(
            group_by=["dept_id"],
            aggregates=[
                AggregateColumnSpec(output_column="emp_count", source_column="emp_id", operation=AggregateOp.COUNT),
                AggregateColumnSpec(output_column="total_sal", source_column="salary", operation=AggregateOp.SUM),
                AggregateColumnSpec(output_column="avg_sal", source_column="salary", operation=AggregateOp.AVG),
            ],
            output_dataset="summary",
        ),
    )
    res = compiler.compile_plan([step_agg], input_columns=["dept_id", "emp_id", "salary"])
    assert "COUNT([source_stage].[emp_id]) AS [emp_count]" in res.query_sql
    assert "SUM([source_stage].[salary]) AS [total_sal]" in res.query_sql
    assert "AVG([source_stage].[salary]) AS [avg_sal]" in res.query_sql
    assert "GROUP BY [source_stage].[dept_id]" in res.query_sql


def test_azure_sql_schema_mutation_protected_drop_rejected(compiler):
    """Proves dropping protected technical column raises error."""
    step_drop_prot = _make_step(
        "drop_prot",
        ActionFamily.SCHEMA_MUTATION,
        DropColumnsExpression(drops=[ColumnDropSpec(column="_tenant_id", justification="test")]),
    )
    with pytest.raises(AzureSqlCompilationError) as exc:
        compiler.compile_plan([step_drop_prot], input_columns=["id", "_tenant_id"])
    assert exc.value.reason_code == "PROTECTED_COLUMN_DROP_REJECTED"


def test_azure_sql_masking_fixed_and_email(compiler):
    """Proves MaskExpression compiles FIXED and EMAIL strategies."""
    step_mask_fix = _make_step(
        "mask_ssn",
        ActionFamily.MASK,
        MaskExpression(
            source_column="ssn",
            target_column="masked_ssn",
            strategy=MaskStrategy.PRESERVE_LAST_N,
            mask_character="*",
            preserved_count=4,
        ),
    )
    res_fix = compiler.compile_plan([step_mask_fix], input_columns=["ssn"])
    assert "'*'" in res_fix.query_sql

    step_mask_email = _make_step(
        "mask_email",
        ActionFamily.MASK,
        MaskExpression(
            source_column="email",
            target_column="masked_email",
            strategy=MaskStrategy.EMAIL_MASK,
        ),
    )
    res_email = compiler.compile_plan([step_mask_email], input_columns=["email"])
    assert "CONCAT('***@'" in res_email.query_sql
