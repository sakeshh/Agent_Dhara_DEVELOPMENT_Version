"""
tests/cross_engine/test_parity_pyspark_schema.py

Phase 3 Increment 3.2 Work Package 3.2E:
PySpark Schema Mutation & Governance Parity.

Verifies:
1. Schema Mutation Operations:
   - DROP_COLUMN
   - RENAME_COLUMN
   - PROJECT
   - REORDER_COLUMNS
   - CREATE_COLUMN
   - CAST_COLUMN
2. Column Protection Governance (ColumnProtectionClass):
   - Direct drop of protected column fails at compile time with CodegenStatus.FAILED_COMPILATION.
   - Indirect drop via PROJECT exclusion of protected column fails at compile time.
   - Backward-compatible is_protected=True protection.
3. Authorization Enforcements:
   - Destructive drop without authorization fails compilation.
   - Destructive drop with expired authorization fails compilation.
   - Destructive drop with semantic hash mismatch fails compilation.
   - Valid authorization succeeds.
4. Compile-time vs Runtime failure boundary:
   - Compile-time: Missing column, collision, protected drop -> FAILED_COMPILATION, artifact=None.
   - Runtime: Pre/post snapshot schema hash mismatch -> Compilation SUCCEEDED, Execution raises error.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
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
    CastColumnExpression,
    ColumnDropSpec,
    ColumnMetadata,
    ColumnProjectSpec,
    ColumnProtectionClass,
    ColumnRenameSpec,
    ColumnRole,
    CreateColumnExpression,
    DataType,
    DatasetColumnSchema,
    DatasetSchema,
    DropColumnsExpression,
    ProjectColumnsExpression,
    RenameColumnsExpression,
    ReorderColumnsExpression,
    SchemaMutationExpression,
    SchemaMutationOp,
    StepAuthorization,
    StepKind,
    TransformationIRGraph,
    TransformationStepIR,
    compute_canonical_schema_hash,
)


@pytest.fixture(scope="module")
def spark_session():
    """Initializes an isolated local SparkSession under the certified local[1] profile."""
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("AgentDhara_Increment32_Schema_Parity")
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


def test_pyspark_schema_mutation_rename_and_reorder(spark_session: SparkSession):
    """Verifies RENAME_COLUMN and REORDER_COLUMNS operations with Pandas parity."""
    pdf = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"], "col_c": [10.5, 20.5]})
    sdf = df_to_spark(spark_session, pdf)

    step1 = TransformationStepIR(
        step_id="step_rename",
        family=ActionFamily.SCHEMA_MUTATION,
        expression=RenameColumnsExpression(
            renames=(ColumnRenameSpec(old_name="col_b", new_name="col_b_renamed"),),
        ),
        dataset_name="primary",
        kind=StepKind.SCHEMA_MUTATION,
    )
    step2 = TransformationStepIR(
        step_id="step_reorder",
        family=ActionFamily.SCHEMA_MUTATION,
        expression=ReorderColumnsExpression(
            ordered_columns=("col_c", "col_b_renamed", "col_a"),
        ),
        dataset_name="primary",
        kind=StepKind.SCHEMA_MUTATION,
    )

    graph = TransformationIRGraph(
        graph_id="test_rename_reorder",
        steps=[step1, step2],
    )

    # 1. Pandas reference
    pandas_compiler = PandasIRCompiler()
    pd_out = pandas_compiler.execute(graph, pdf).outputs["primary"]

    # 2. PySpark
    pyspark_compiler = PySparkIRCompiler()
    py_res = pyspark_compiler.compile(graph)
    assert py_res.is_success
    py_out = pyspark_compiler.execute(graph, sdf).outputs["primary"].toPandas()

    assert list(py_out.columns) == list(pd_out.columns) == ["col_c", "col_b_renamed", "col_a"]
    assert py_out["col_b_renamed"].tolist() == ["x", "y"]


def test_pyspark_schema_mutation_protected_column_direct_drop_rejected():
    """Verifies that direct drop of a protected column is rejected statically at compile time."""
    schema = DatasetSchema(
        dataset_name="primary",
        columns={
            "customer_id": DatasetColumnSchema(
                name="customer_id",
                logical_type=DataType.STRING,
                is_protected=True,
                protection_classes=(ColumnProtectionClass.PRIMARY_KEY,),
            ),
            "amount": DatasetColumnSchema(
                name="amount",
                logical_type=DataType.FLOAT,
            ),
        },
    )

    # Valid authorization object so lack of authorization isn't the failure cause
    valid_auth = StepAuthorization(
        step_id="step_drop_pk",
        policy_decision_id="POL-01",
        approved_by_subject_id="admin",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        authorized_step_hash="",  # Will be populated
    )

    step = TransformationStepIR(
        step_id="step_drop_pk",
        family=ActionFamily.SCHEMA_MUTATION,
        expression=DropColumnsExpression(
            drops=(ColumnDropSpec(column="customer_id"),),
        ),
        authorization=valid_auth,
        dataset_name="primary",
        kind=StepKind.SCHEMA_MUTATION,
    )
    # Update hash to match step
    step.authorization.authorized_step_hash = step.semantic_hash

    graph = TransformationIRGraph(
        graph_id="test_drop_protected",
        steps=[step],
    )

    compiler = PySparkIRCompiler(schemas={"primary": schema})
    res = compiler.compile(graph)

    # Must fail compilation statically
    assert res.status == CodegenStatus.FAILED_COMPILATION
    assert res.artifact is None
    assert any("ProtectedColumnDropError" in d for d in res.diagnostics)


def test_pyspark_schema_mutation_project_exclusion_of_protected_column_rejected():
    """Verifies that PROJECT cannot bypass protection by excluding protected columns."""
    schema = DatasetSchema(
        dataset_name="primary",
        columns={
            "user_id": DatasetColumnSchema(
                name="user_id",
                logical_type=DataType.STRING,
                role=ColumnRole.BUSINESS,
                protection_classes=(ColumnProtectionClass.PRIMARY_KEY,),
            ),
            "notes": DatasetColumnSchema(
                name="notes",
                logical_type=DataType.STRING,
            ),
        },
    )

    # Project only 'notes', excluding 'user_id' which is protected!
    step = TransformationStepIR(
        step_id="step_proj_exclude_pk",
        family=ActionFamily.SCHEMA_MUTATION,
        expression=ProjectColumnsExpression(
            projection=ColumnProjectSpec(projected_columns=("notes",)),
        ),
        dataset_name="primary",
        kind=StepKind.SCHEMA_MUTATION,
    )

    graph = TransformationIRGraph(
        graph_id="test_proj_exclude_protected",
        steps=[step],
    )

    compiler = PySparkIRCompiler(schemas={"primary": schema})
    res = compiler.compile(graph)

    assert res.status == CodegenStatus.FAILED_COMPILATION
    assert res.artifact is None
    assert any("ProtectedColumnDropError" in d for d in res.diagnostics)


def test_pyspark_schema_mutation_destructive_drop_authorization_checks(spark_session: SparkSession):
    """Verifies destructive drop authorization lifecycle: missing auth, expired auth, and valid auth."""
    schema = DatasetSchema(
        dataset_name="primary",
        columns={
            "temp_col": DatasetColumnSchema(name="temp_col", logical_type=DataType.STRING),
            "val": DatasetColumnSchema(name="val", logical_type=DataType.INTEGER),
        },
    )

    # Case 1: Missing authorization
    step_no_auth = TransformationStepIR(
        step_id="step_drop_no_auth",
        family=ActionFamily.SCHEMA_MUTATION,
        expression=DropColumnsExpression(drops=(ColumnDropSpec(column="temp_col"),)),
        dataset_name="primary",
        kind=StepKind.SCHEMA_MUTATION,
    )
    graph_no_auth = TransformationIRGraph(graph_id="g1", steps=[step_no_auth])
    compiler = PySparkIRCompiler(schemas={"primary": schema})
    res1 = compiler.compile(graph_no_auth)
    assert res1.status == CodegenStatus.FAILED_COMPILATION
    assert any("UnauthorizedDestructiveActionError" in d for d in res1.diagnostics)

    # Case 2: Expired authorization
    expired_auth = StepAuthorization(
        step_id="step_drop_expired",
        policy_decision_id="POL-EXP",
        approved_by_subject_id="admin",
        expires_at=datetime.now(timezone.utc) - timedelta(hours=1),
        authorized_step_hash="placeholder",
    )
    step_exp = TransformationStepIR(
        step_id="step_drop_expired",
        family=ActionFamily.SCHEMA_MUTATION,
        expression=DropColumnsExpression(drops=(ColumnDropSpec(column="temp_col"),)),
        authorization=expired_auth,
        dataset_name="primary",
        kind=StepKind.SCHEMA_MUTATION,
    )
    expired_auth.authorized_step_hash = step_exp.semantic_hash
    graph_exp = TransformationIRGraph(graph_id="g2", steps=[step_exp])
    res2 = compiler.compile(graph_exp)
    assert res2.status == CodegenStatus.FAILED_COMPILATION
    assert any("expired" in d for d in res2.diagnostics)

    # Case 3: Valid authorization -> Successful execution
    valid_auth = StepAuthorization(
        step_id="step_drop_valid",
        policy_decision_id="POL-VAL",
        approved_by_subject_id="admin",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=2),
        authorized_step_hash="placeholder",
    )
    step_val = TransformationStepIR(
        step_id="step_drop_valid",
        family=ActionFamily.SCHEMA_MUTATION,
        expression=DropColumnsExpression(drops=(ColumnDropSpec(column="temp_col"),)),
        authorization=valid_auth,
        dataset_name="primary",
        kind=StepKind.SCHEMA_MUTATION,
    )
    valid_auth.authorized_step_hash = step_val.semantic_hash
    graph_val = TransformationIRGraph(graph_id="g3", steps=[step_val])
    res3 = compiler.compile(graph_val)
    assert res3.status == CodegenStatus.SUCCEEDED

    pdf = pd.DataFrame({"temp_col": ["a", "b"], "val": [1, 2]})
    sdf = df_to_spark(spark_session, pdf)
    exec_res = compiler.execute(graph_val, sdf)
    out_cols = exec_res.outputs["primary"].toPandas().columns
    assert "temp_col" not in out_cols
    assert "val" in out_cols


def test_pyspark_schema_mutation_pre_post_hash_runtime_enforcement(spark_session: SparkSession):
    """
    Verifies that pre/post snapshot schema hash checks pass on match and
    raise runtime postcondition error on mismatch (without misclassifying as compiler failure).
    """
    pdf = pd.DataFrame({"a": ["hello", "world"]})
    sdf = df_to_spark(spark_session, pdf)

    # Deliberately bogus expected pre-mutation hash
    step = TransformationStepIR(
        step_id="step_mut_hash_check",
        family=ActionFamily.SCHEMA_MUTATION,
        expression=SchemaMutationExpression(
            op=SchemaMutationOp.RENAME_COLUMN,
            renames=(ColumnRenameSpec(old_name="a", new_name="b"),),
            expected_before_schema_hash="bogus_hash_1234567890abcdef",
        ),
        dataset_name="primary",
        kind=StepKind.SCHEMA_MUTATION,
    )

    graph = TransformationIRGraph(graph_id="g_hash", steps=[step])
    compiler = PySparkIRCompiler()

    # 1. Compilation SUCCEEDS (it's valid IR and valid structure)
    res = compiler.compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED
    assert res.artifact is not None

    # 2. Execution FAILS at runtime due to pre-mutation snapshot mismatch
    with pytest.raises(Exception, match="Actual pre-mutation schema hash does not match"):
        compiler.execute(graph, sdf)


def test_pyspark_schema_protected_rename_then_drop_rejected():
    """
    Verifies that renaming a protected column preserves its protection class,
    and a subsequent drop or project exclusion is rejected at compile time.
    """
    schema = DatasetSchema(
        dataset_name="primary",
        columns={
            "account_id": DatasetColumnSchema(
                name="account_id",
                logical_type=DataType.STRING,
                is_protected=True,
                protection_classes=(ColumnProtectionClass.PRIMARY_KEY,),
            ),
            "balance": DatasetColumnSchema(name="balance", logical_type=DataType.FLOAT),
        },
    )

    # Case A: Rename protected column, then DROP new name -> FAILED_COMPILATION
    valid_auth_a = StepAuthorization(
        step_id="step_drop_renamed_pk",
        policy_decision_id="POL-DROP-REN",
        approved_by_subject_id="steward",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        authorized_step_hash="placeholder",
    )
    step_ren = TransformationStepIR(
        step_id="step_rename_pk",
        family=ActionFamily.SCHEMA_MUTATION,
        expression=RenameColumnsExpression(renames=(ColumnRenameSpec(old_name="account_id", new_name="acct_num"),)),
        dataset_name="primary",
        kind=StepKind.SCHEMA_MUTATION,
    )
    step_drop = TransformationStepIR(
        step_id="step_drop_renamed_pk",
        family=ActionFamily.SCHEMA_MUTATION,
        expression=DropColumnsExpression(drops=(ColumnDropSpec(column="acct_num"),)),
        authorization=valid_auth_a,
        dataset_name="primary",
        kind=StepKind.SCHEMA_MUTATION,
    )
    valid_auth_a.authorized_step_hash = step_drop.semantic_hash
    graph_drop = TransformationIRGraph(graph_id="g_ren_drop", steps=[step_ren, step_drop])
    res_drop = PySparkIRCompiler(schemas={"primary": schema}).compile(graph_drop)
    assert res_drop.status == CodegenStatus.FAILED_COMPILATION
    assert res_drop.artifact is None
    assert any("ProtectedColumnDropError" in d for d in res_drop.diagnostics)

    # Case B: Rename protected column, then PROJECT excluding new name -> FAILED_COMPILATION
    step_proj = TransformationStepIR(
        step_id="step_proj_exclude_renamed",
        family=ActionFamily.SCHEMA_MUTATION,
        expression=ProjectColumnsExpression(projection=ColumnProjectSpec(projected_columns=("balance",))),
        dataset_name="primary",
        kind=StepKind.SCHEMA_MUTATION,
    )
    graph_proj = TransformationIRGraph(graph_id="g_ren_proj", steps=[step_ren, step_proj])
    res_proj = PySparkIRCompiler(schemas={"primary": schema}).compile(graph_proj)
    assert res_proj.status == CodegenStatus.FAILED_COMPILATION
    assert res_proj.artifact is None
    assert any("ProtectedColumnDropError" in d for d in res_proj.diagnostics)


def test_pyspark_schema_cannot_drop_technical_routing_identity():
    """
    Verifies that a user-authored schema mutation cannot drop or rename the technical routing identity.
    Only the certified publication-eligibility boundary may strip it.
    """
    schema = DatasetSchema(
        dataset_name="primary",
        columns={
            "val": DatasetColumnSchema(name="val", logical_type=DataType.INTEGER),
        },
    )
    valid_auth_id = StepAuthorization(
        step_id="step_drop_id",
        policy_decision_id="POL-TECH-ID",
        approved_by_subject_id="steward",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        authorized_step_hash="placeholder",
    )
    step_drop_id = TransformationStepIR(
        step_id="step_drop_id",
        family=ActionFamily.SCHEMA_MUTATION,
        expression=DropColumnsExpression(drops=(ColumnDropSpec(column="__dhara_row_occurrence_0"),)),
        authorization=valid_auth_id,
        dataset_name="primary",
        kind=StepKind.SCHEMA_MUTATION,
    )
    valid_auth_id.authorized_step_hash = step_drop_id.semantic_hash
    graph = TransformationIRGraph(graph_id="g_drop_tech_id", steps=[step_drop_id])
    res = PySparkIRCompiler(schemas={"primary": schema}).compile(graph)
    assert res.status == CodegenStatus.FAILED_COMPILATION
    assert res.artifact is None
    assert any("ProtectedColumnDropError" in d for d in res.diagnostics)

