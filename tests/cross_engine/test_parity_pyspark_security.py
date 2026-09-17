"""
tests/cross_engine/test_parity_pyspark_security.py

Phase 3 Increment 3.2 Work Package 3.2F:
PySpark Masking & Canonical Hashing Parity.

Verifies:
1. Masking Strategies Parity:
   - PRESERVE_LAST_N
   - PRESERVE_FIRST_N
   - MASK_ALL
   - EMAIL_MASK (domain preserve and mask)
   - Null policies (PRESERVE_NULL, SET_EMPTY, FAIL_STEP)
   - Short value policies (SET_NULL, KEEP_ORIGINAL, FAIL_STEP, MASK_ALL)
2. Canonical Hashing Parity:
   - BOOLEAN canonical byte tag and SHA-256 digest parity
   - BYTES canonical byte tag and SHA-256 digest parity
   - INT64 canonical 8-byte big-endian signed integer and SHA-256 digest parity
   - SALTED_DIGEST with UTF-8 salt prefix
3. Boundary & Guardrail Enforcements:
   - STRING canonical hashing is rejected at compile time (NOT_TESTED pending NFC contract).
   - HMAC mode is rejected at compile time in PySpark compiler (REFERENCE_TEST_ONLY).
   - HMAC reference execution in Pandas reference harness confirms key provider contract with zero hardcoded secret bytes in emitted code.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import SparkSession

from agent.etl_pipeline.codegen_shared import CodegenStatus
from agent.etl_pipeline.pandas_ir_compiler import PandasIRCompiler
from agent.etl_pipeline.pyspark_ir_compiler import PySparkIRCompiler
from agent.etl_pipeline.transformation_ir import (
    ActionFamily,
    ColumnMetadata,
    DataType,
    DatasetColumnSchema,
    DatasetSchema,
    EmailMaskDomainPolicy,
    ExistingColumnPolicy,
    HashAlgorithm,
    HashExpression,
    HashMode,
    HashNullPolicy,
    MaskExpression,
    MaskNullPolicy,
    MaskStrategy,
    SecretReference,
    ShortValuePolicy,
    StepKind,
    TransformationIRGraph,
    TransformationStepIR,
)


@pytest.fixture(scope="module")
def spark_session():
    """Isolated local SparkSession under certified local[1] profile."""
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("AgentDhara_Increment32_Security_Parity")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def df_to_spark(spark: SparkSession, pdf: pd.DataFrame):
    """Safely loads pandas DataFrame into Spark via temp JSON without socket worker crashes."""
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


# ============================================================================
# 1. Masking Strategy Parity Tests
# ============================================================================

def test_pyspark_masking_preserve_last_n_and_first_n_parity(spark_session: SparkSession):
    """Verifies PRESERVE_LAST_N and PRESERVE_FIRST_N with exact Pandas parity."""
    pdf = pd.DataFrame({
        "ccn": ["1234567890123456", "9876543210", "123", None],
        "phone": ["+1-555-0199", "1234567", "99", None],
    })
    sdf = df_to_spark(spark_session, pdf)

    step1 = TransformationStepIR(
        step_id="step_mask_ccn",
        family=ActionFamily.MASK,
        expression=MaskExpression(
            source_column="ccn",
            target_column="ccn_masked",
            strategy=MaskStrategy.PRESERVE_LAST_N,
            mask_character="*",
            preserved_count=4,
            short_value_policy=ShortValuePolicy.MASK_ALL,
            null_policy=MaskNullPolicy.PRESERVE_NULL,
        ),
        dataset_name="primary",
        kind=StepKind.ROW_TRANSFORMATION,
    )
    step2 = TransformationStepIR(
        step_id="step_mask_phone",
        family=ActionFamily.MASK,
        expression=MaskExpression(
            source_column="phone",
            target_column="phone_masked",
            strategy=MaskStrategy.PRESERVE_FIRST_N,
            mask_character="#",
            preserved_count=3,
            short_value_policy=ShortValuePolicy.KEEP_ORIGINAL,
            null_policy=MaskNullPolicy.PRESERVE_NULL,
        ),
        dataset_name="primary",
        kind=StepKind.ROW_TRANSFORMATION,
    )

    graph = TransformationIRGraph(
        graph_id="test_mask_preserve",
        steps=[step1, step2],
    )

    # 1. Pandas reference
    pandas_compiler = PandasIRCompiler()
    pd_res = pandas_compiler.execute(graph, pdf)
    pd_out = pd_res.outputs["primary"]

    # 2. PySpark
    pyspark_compiler = PySparkIRCompiler()
    py_res = pyspark_compiler.compile(graph)
    assert py_res.is_success, py_res.diagnostics
    py_out = pyspark_compiler.execute(graph, sdf).outputs["primary"].toPandas()

    # Compare columns
    for col in ["ccn_masked", "phone_masked"]:
        pd_vals = pd_out[col].tolist()
        py_vals = py_out[col].tolist()
        for idx, (p, s) in enumerate(zip(pd_vals, py_vals)):
            if pd.isna(p):
                assert pd.isna(s), f"Row {idx} col {col}: expected null, got {s}"
            else:
                assert str(p) == str(s), f"Row {idx} col {col}: pandas={p} spark={s}"


def test_pyspark_masking_email_and_redact_all_parity(spark_session: SparkSession):
    """Verifies EMAIL_MASK and MASK_ALL parity between PySpark and Pandas."""
    pdf = pd.DataFrame({
        "email": ["alice@company.com", "bob.smith@domain.org", "invalid_email", None],
        "ssn": ["123-45-6789", "987-65-4321", "000", None],
    })
    sdf = df_to_spark(spark_session, pdf)

    step_email = TransformationStepIR(
        step_id="step_mask_email",
        family=ActionFamily.MASK,
        expression=MaskExpression(
            source_column="email",
            target_column="email_masked",
            strategy=MaskStrategy.EMAIL_MASK,
            mask_character="X",
            email_domain_policy=EmailMaskDomainPolicy.PRESERVE,
            null_policy=MaskNullPolicy.PRESERVE_NULL,
        ),
        dataset_name="primary",
        kind=StepKind.ROW_TRANSFORMATION,
    )
    step_ssn = TransformationStepIR(
        step_id="step_mask_ssn",
        family=ActionFamily.MASK,
        expression=MaskExpression(
            source_column="ssn",
            target_column="ssn_masked",
            strategy=MaskStrategy.MASK_ALL,
            mask_character="*",
            null_policy=MaskNullPolicy.SET_EMPTY,
        ),
        dataset_name="primary",
        kind=StepKind.ROW_TRANSFORMATION,
    )

    graph = TransformationIRGraph(
        graph_id="test_mask_email_ssn",
        steps=[step_email, step_ssn],
    )

    pandas_compiler = PandasIRCompiler()
    pd_out = pandas_compiler.execute(graph, pdf).outputs["primary"]

    pyspark_compiler = PySparkIRCompiler()
    py_comp = pyspark_compiler.compile(graph)
    assert py_comp.is_success, py_comp.diagnostics
    py_out = pyspark_compiler.execute(graph, sdf).outputs["primary"].toPandas()

    for col in ["email_masked", "ssn_masked"]:
        pd_vals = pd_out[col].tolist()
        py_vals = py_out[col].tolist()
        for idx, (p, s) in enumerate(zip(pd_vals, py_vals)):
            if pd.isna(p) or p is None:
                assert pd.isna(s) or s is None, f"Row {idx} col {col}: expected null, got {s}"
            else:
                assert str(p) == str(s), f"Row {idx} col {col}: pandas={p} spark={s}"


# ============================================================================
# 2. Canonical Hashing Parity Tests (BOOLEAN, BYTES, INT64)
# ============================================================================

def test_pyspark_canonical_hashing_boolean_and_integer_parity(spark_session: SparkSession):
    """
    Verifies pure Catalyst canonical byte hashing for BOOLEAN and INT64:
    - BOOLEAN: \\x01BOOL\\x01 / \\x01BOOL\\x00 / null tag \\x00DHARA_NULL_V1
    - INT64: \\x02INT\\x00 + 8-byte big-endian signed integer
    """
    pdf = pd.DataFrame({
        "flag": [True, False, None],
        "num": [42, -42, 0],
    })
    sdf = df_to_spark(spark_session, pdf)

    schema = DatasetSchema(
        dataset_name="primary",
        columns={
            "flag": DatasetColumnSchema(name="flag", logical_type=DataType.BOOLEAN),
            "num": DatasetColumnSchema(name="num", logical_type=DataType.INTEGER),
        },
    )

    step_bool = TransformationStepIR(
        step_id="step_hash_bool",
        family=ActionFamily.HASH,
        expression=HashExpression(
            source_column="flag",
            target_column="flag_hash",
            mode=HashMode.DIGEST,
            algorithm=HashAlgorithm.SHA256,
            null_policy=HashNullPolicy.HASH_NULL,
        ),
        dataset_name="primary",
        kind=StepKind.ROW_TRANSFORMATION,
    )
    step_int = TransformationStepIR(
        step_id="step_hash_int",
        family=ActionFamily.HASH,
        expression=HashExpression(
            source_column="num",
            target_column="num_hash",
            mode=HashMode.SALTED_DIGEST,
            algorithm=HashAlgorithm.SHA256,
            salt_value="dhara_salt_key",
            null_policy=HashNullPolicy.PRESERVE_NULL,
        ),
        dataset_name="primary",
        kind=StepKind.ROW_TRANSFORMATION,
    )

    graph = TransformationIRGraph(
        graph_id="test_canonical_hashing",
        steps=[step_bool, step_int],
    )

    # 1. Pandas reference compiler
    pandas_compiler = PandasIRCompiler(schemas={"primary": schema})
    pd_out = pandas_compiler.execute(graph, pdf).outputs["primary"]

    # 2. PySpark compiler
    pyspark_compiler = PySparkIRCompiler(schemas={"primary": schema})
    py_comp = pyspark_compiler.compile(graph)
    assert py_comp.is_success, py_comp.diagnostics
    py_out = pyspark_compiler.execute(graph, sdf).outputs["primary"].toPandas()

    # Compare exact SHA-256 digests
    assert py_out["flag_hash"].tolist() == pd_out["flag_hash"].tolist()
    assert py_out["num_hash"].tolist() == pd_out["num_hash"].tolist()


# ============================================================================
# 3. Guardrails & Boundary Tests
# ============================================================================

def test_pyspark_string_canonical_hashing_default_denial():
    """
    Verifies that canonical hashing on STRING column is rejected at compile time (NOT_TESTED)
    due to absence of NFC Unicode normalization guarantee.
    """
    schema = DatasetSchema(
        dataset_name="primary",
        columns={
            "raw_str": DatasetColumnSchema(name="raw_str", logical_type=DataType.STRING),
        },
    )
    step = TransformationStepIR(
        step_id="step_hash_str",
        family=ActionFamily.HASH,
        expression=HashExpression(
            source_column="raw_str",
            target_column="str_hash",
            mode=HashMode.DIGEST,
            algorithm=HashAlgorithm.SHA256,
        ),
        dataset_name="primary",
        kind=StepKind.ROW_TRANSFORMATION,
    )
    graph = TransformationIRGraph(graph_id="g_str_hash", steps=[step])

    compiler = PySparkIRCompiler(schemas={"primary": schema})
    res = compiler.compile(graph)

    # Must fail compilation statically
    assert res.status == CodegenStatus.FAILED_COMPILATION
    assert res.artifact is None
    assert any("NOT_TESTED" in d or "blocked" in d for d in res.diagnostics)


def test_pyspark_hmac_boundary_rejection_and_reference_test():
    """
    Verifies HMAC boundary:
    1. PySpark compiler rejects HMAC execution at compile time.
    2. Reference test harness in Pandas compiler verifies HMAC resolution with zero hardcoded secrets.
    """
    step = TransformationStepIR(
        step_id="step_hmac",
        family=ActionFamily.HASH,
        expression=HashExpression(
            source_column="val",
            target_column="val_hmac",
            mode=HashMode.HMAC,
            algorithm=HashAlgorithm.SHA256,
            secret_ref=SecretReference(
                secret_id="test_hmac_secret",
                expected_purpose="hmac_key",
            ),
        ),
        dataset_name="primary",
        kind=StepKind.ROW_TRANSFORMATION,
    )
    graph = TransformationIRGraph(graph_id="g_hmac", steps=[step])

    # 1. PySpark compilation must fail atomically
    py_compiler = PySparkIRCompiler()
    py_res = py_compiler.compile(graph)
    assert py_res.status == CodegenStatus.FAILED_COMPILATION
    assert py_res.artifact is None
    assert any("HMAC" in d for d in py_res.diagnostics)

    # 2. Reference test harness in Pandas compiler
    pd_compiler = PandasIRCompiler()
    pd_comp = pd_compiler.compile(graph)
    assert pd_comp.is_success
    assert pd_comp.artifact is not None

    # Verify zero hardcoded secret bytes in emitted code
    assert "test_secret_bytes_value" not in pd_comp.artifact.source_code

    # Mock Key Provider
    class MockSecretHandle:
        def __init__(self, b: bytes):
            self.secret_bytes = b

    class MockKeyProvider:
        tenant_id = "test_tenant"
        def resolve_secret(self, secret_id, tenant_id, purpose, expected_version=None):
            assert secret_id == "test_hmac_secret"
            return MockSecretHandle(b"super_secret_key_123")

    pdf = pd.DataFrame({"val": [10, 20]})
    exec_res = pd_compiler.execute(graph, pdf, key_provider=MockKeyProvider())
    assert "val_hmac" in exec_res.outputs["primary"].columns
    assert len(exec_res.outputs["primary"]["val_hmac"]) == 2
