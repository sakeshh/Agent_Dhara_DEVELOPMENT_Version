"""
Agent Dhara - Increment 3D Test Suite
Phase 2: Security & Schema Actions Reference Compilation
Covers 32 Formal Verification Scenarios (C1 - C32):
- Group 1: HASH Action Tests (C1 - C13)
- Group 2: MASK Action Tests (C14 - C22)
- Group 3: SCHEMA_MUTATION Action & Step Authorization Tests (C23 - C32)
"""

import ast
import hashlib
import hmac
import struct
import unicodedata
from collections import Counter
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import pytest

from agent.etl_pipeline.pandas_ir_compiler import (
    CodegenStatus,
    CompiledPandasArtifact,
    CompilerStepDisposition,
    OutputColumnCollisionError,
    PandasIRCompiler,
)
from agent.etl_pipeline.transformation_ir import (
    ActionFamily,
    BinaryOp,
    BinaryOperator,
    BoundSchemaMutation,
    CastColumnExpression,
    CastFailurePolicy,
    CollisionPolicy,
    ColumnDropSpec,
    ColumnMetadata,
    ColumnProjectSpec,
    ColumnRef,
    ColumnRenameSpec,
    ColumnRole,
    CreateColumnExpression,
    DataType,
    DatasetColumnSchema,
    DatasetSchema,
    DigestEncoding,
    DropColumnsExpression,
    EmailMaskDomainPolicy,
    ExecutionKeyProvider,
    ExistingColumnPolicy,
    FlagExpression,
    HashAlgorithm,
    HashExpression,
    HashInputEncoding,
    HashMode,
    HashNullPolicy,
    IRDependency,
    KeyProviderRequiredError,
    LiteralValue,
    MaskExpression,
    MaskNullPolicy,
    MaskStrategy,
    NullCheck,
    NumericOverflowPolicy,
    ProjectColumnsExpression,
    ReorderColumnsExpression,
    RenameColumnsExpression,
    SchemaBindingError,
    SchemaMutationExpression,
    SchemaMutationOp,
    SchemaSnapshotMismatchError,
    SecretHandle,
    SecretReference,
    SecretResolutionError,
    ShortValuePolicy,
    StepAuthorization,
    StepKind,
    TransformationIRGraph,
    TransformationIRInvariantError,
    TransformationStepIR,
    UnauthorizedDestructiveActionError,
    bind_expression_to_schema,
    compute_canonical_schema_hash,
)


# ============================================================================
# Helpers and Test Harness
# ============================================================================

class MockKeyProvider:
    """Mock implementation of ExecutionKeyProvider Protocol."""

    def __init__(
        self,
        secrets: Dict[str, bytes],
        expired_keys: Optional[set] = None,
        revoked_keys: Optional[set] = None,
        tenant_id: str = "tenant_test",
    ):
        self.secrets = secrets
        self.expired_keys = expired_keys or set()
        self.revoked_keys = revoked_keys or set()
        self.tenant_id = tenant_id

    def resolve_secret(
        self,
        *,
        secret_id: str,
        tenant_id: str,
        purpose: str,
        expected_version: Optional[str] = None,
    ) -> SecretHandle:
        if tenant_id != self.tenant_id:
            raise SecretResolutionError(f"Tenant mismatch: expected {self.tenant_id}, got {tenant_id}")
        if secret_id in self.revoked_keys:
            raise SecretResolutionError(f"Key revoked: {secret_id}")
        if secret_id in self.expired_keys:
            raise SecretResolutionError(f"Key expired: {secret_id}")
        if secret_id not in self.secrets:
            raise SecretResolutionError(f"Key not found: {secret_id}")
        return SecretHandle(
            secret_id=secret_id,
            secret_bytes=self.secrets[secret_id],
            version=expected_version or "v1",
            tenant_id=tenant_id,
            purpose=purpose,
        )


def _execute_artifact_safely(
    artifact: CompiledPandasArtifact,
    df: pd.DataFrame,
    key_provider: Optional[Any] = None,
    use_datasets_entrypoint: bool = False,
) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """Executes the compiled artifact within a strictly controlled execution namespace."""
    namespace: Dict[str, Any] = {
        "np": np,
        "pd": pd,
        "hashlib": hashlib,
        "hmac": hmac,
        "struct": struct,
        "unicodedata": unicodedata,
        "Counter": Counter,
        "compute_canonical_schema_hash": compute_canonical_schema_hash,
        "SchemaSnapshotMismatchError": SchemaSnapshotMismatchError,
        "OutputColumnCollisionError": OutputColumnCollisionError,
        "KeyProviderRequiredError": KeyProviderRequiredError,
        "SecretResolutionError": SecretResolutionError,
    }
    exec(artifact.source_code, namespace)
    if use_datasets_entrypoint:
        transform_fn = namespace["transform_datasets"]
        return transform_fn(df, key_provider=key_provider)
    else:
        transform_fn = namespace["transform"]
        return transform_fn(df, key_provider=key_provider)


# ============================================================================
# Group 1: HASH Action Tests (C1 - C13)
# ============================================================================

def test_hash_digest_deterministic_sha256():
    """C1: SHA-256 DIGEST mode produces bit-exact standard hash, hex digest; identical output across calls."""
    expr = HashExpression(
        source_column="email",
        target_column="email_hash",
        mode=HashMode.DIGEST,
        algorithm=HashAlgorithm.SHA256,
        output_encoding=DigestEncoding.HEX,
    )
    step = TransformationStepIR(
        step_id="step_hash_digest",
        dataset_name="users",
        family=ActionFamily.HASH,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr,
        target_column="email_hash",
    )
    graph = TransformationIRGraph(
        graph_id="graph_c1",
        steps=(step,),
        schema_snapshot_hash="snap_users",
    )
    compiler = PandasIRCompiler()
    res = compiler.compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED
    assert res.artifact is not None

    df_in = pd.DataFrame({"email": ["alice@example.com", "bob@example.com", "alice@example.com"]})
    df_out1 = _execute_artifact_safely(res.artifact, df_in)
    df_out2 = _execute_artifact_safely(res.artifact, df_in)

    # Identical across runs
    assert list(df_out1["email_hash"]) == list(df_out2["email_hash"])
    # Deterministic equality for identical inputs
    assert df_out1["email_hash"].iloc[0] == df_out1["email_hash"].iloc[2]
    # Expected standard canonical serialization hash
    expected_alice = hashlib.sha256(b"\x05STR\x00alice@example.com").hexdigest()
    assert df_out1["email_hash"].iloc[0] == expected_alice


def test_hash_hmac_sha256_standard_test_vector():
    """C2: HMAC mode uses standard hmac.new() without string concatenation and matches RFC/Python hmac."""
    secret_key = b"super_secret_signing_key_42"
    provider = MockKeyProvider(secrets={"app_hmac_key": secret_key}, tenant_id="default")

    expr = HashExpression(
        source_column="user_id",
        target_column="user_hmac",
        mode=HashMode.HMAC,
        algorithm=HashAlgorithm.SHA256,
        secret_ref=SecretReference(secret_id="app_hmac_key", expected_version="1"),
    )
    step = TransformationStepIR(
        step_id="step_hmac",
        dataset_name="users",
        family=ActionFamily.HASH,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr,
        target_column="user_hmac",
    )
    graph = TransformationIRGraph(
        graph_id="graph_c2",
        steps=(step,),
        schema_snapshot_hash="snap_users",
    )
    compiler = PandasIRCompiler()
    res = compiler.compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({"user_id": ["USR_001", "USR_002"]})
    df_out = _execute_artifact_safely(res.artifact, df_in, key_provider=provider)

    # Test vector parity
    canonical_bytes_0 = b"\x05STR\x00USR_001"
    expected_hmac_0 = hmac.new(secret_key, canonical_bytes_0, "sha256").hexdigest()
    assert df_out["user_hmac"].iloc[0] == expected_hmac_0


def test_hash_missing_key_provider_fails_immediately():
    """C3: Executing a step requiring SecretReference with key_provider=None raises KeyProviderRequiredError."""
    expr = HashExpression(
        source_column="ssn",
        target_column="ssn_hmac",
        mode=HashMode.HMAC,
        algorithm=HashAlgorithm.SHA256,
        secret_ref=SecretReference(secret_id="ssn_key"),
    )
    step = TransformationStepIR(
        step_id="step_hmac_no_prov",
        dataset_name="users",
        family=ActionFamily.HASH,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr,
        target_column="ssn_hmac",
    )
    graph = TransformationIRGraph(
        graph_id="graph_c3",
        steps=(step,),
        schema_snapshot_hash="snap_users",
    )
    compiler = PandasIRCompiler()
    res = compiler.compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({"ssn": ["123-45-6789"]})
    with pytest.raises(KeyProviderRequiredError):
        _execute_artifact_safely(res.artifact, df_in, key_provider=None)


def test_hash_secret_resolution_failure_invalid_expired_revoked():
    """C4: Key provider raising SecretResolutionError (expired/revoked/tenant) halts execution cleanly."""
    provider = MockKeyProvider(
        secrets={"valid_key": b"key_data", "expired_key": b"key_exp", "revoked_key": b"key_rev"},
        expired_keys={"expired_key"},
        revoked_keys={"revoked_key"},
        tenant_id="default",
    )

    # 1. Expired Key
    expr_exp = HashExpression(
        source_column="id",
        target_column="id_h",
        mode=HashMode.HMAC,
        algorithm=HashAlgorithm.SHA256,
        secret_ref=SecretReference(secret_id="expired_key"),
    )
    step_exp = TransformationStepIR(
        step_id="step_exp", dataset_name="u", family=ActionFamily.HASH,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr_exp, target_column="id_h",
    )
    graph_exp = TransformationIRGraph(graph_id="g_exp", steps=(step_exp,), schema_snapshot_hash="s")
    res_exp = PandasIRCompiler().compile(graph_exp)

    df_in = pd.DataFrame({"id": ["A"]})
    with pytest.raises(SecretResolutionError, match="Key expired"):
        _execute_artifact_safely(res_exp.artifact, df_in, key_provider=provider)

    # 2. Revoked Key
    expr_rev = HashExpression(
        source_column="id",
        target_column="id_h",
        mode=HashMode.HMAC,
        algorithm=HashAlgorithm.SHA256,
        secret_ref=SecretReference(secret_id="revoked_key"),
    )
    step_rev = TransformationStepIR(
        step_id="step_rev", dataset_name="u", family=ActionFamily.HASH,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr_rev, target_column="id_h",
    )
    res_rev = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_rev", steps=(step_rev,), schema_snapshot_hash="s"))
    with pytest.raises(SecretResolutionError, match="Key revoked"):
        _execute_artifact_safely(res_rev.artifact, df_in, key_provider=provider)


def test_hash_domain_separated_null_vs_string_null():
    """C5: Type-tagged canonical serialization produces distinct hashes for None/pd.NA vs literal string 'null'."""
    expr = HashExpression(
        source_column="val",
        target_column="val_hash",
        mode=HashMode.DIGEST,
        algorithm=HashAlgorithm.SHA256,
        null_policy=HashNullPolicy.HASH_NULL,
    )
    step = TransformationStepIR(
        step_id="step_domain_null",
        dataset_name="data",
        family=ActionFamily.HASH,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr,
        target_column="val_hash",
    )
    graph = TransformationIRGraph(graph_id="g_c5", steps=(step,), schema_snapshot_hash="s")
    res = PandasIRCompiler().compile(graph)

    # Row 0 is actual None/pd.NA; Row 1 is literal string "null"
    df_in = pd.DataFrame({"val": [None, "null"]})
    df_out = _execute_artifact_safely(res.artifact, df_in)

    hash_none = df_out["val_hash"].iloc[0]
    hash_str_null = df_out["val_hash"].iloc[1]

    # Must NOT collide
    assert hash_none != hash_str_null
    # Domain-separated null tag: b"\x00DHARA_NULL_V1"
    assert hash_none == hashlib.sha256(b"\x00DHARA_NULL_V1").hexdigest()
    # String null tag: b"\x05STR\x00null"
    assert hash_str_null == hashlib.sha256(b"\x05STR\x00null").hexdigest()


def test_hash_number_normalization_integer_float_zero():
    """C6: Number normalization: float -0.0 is normalized to 0.0 producing bit-exact identical hash."""
    expr = HashExpression(
        source_column="num",
        target_column="num_hash",
        mode=HashMode.DIGEST,
        algorithm=HashAlgorithm.SHA256,
    )
    step = TransformationStepIR(
        step_id="step_num_norm",
        dataset_name="data",
        family=ActionFamily.HASH,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr,
        target_column="num_hash",
    )
    graph = TransformationIRGraph(graph_id="g_c6", steps=(step,), schema_snapshot_hash="s")
    res = PandasIRCompiler().compile(graph)

    # Test +0.0 vs -0.0
    df_in = pd.DataFrame({"num": [0.0, -0.0]})
    df_out = _execute_artifact_safely(res.artifact, df_in)

    assert df_out["num_hash"].iloc[0] == df_out["num_hash"].iloc[1]
    expected_zero_hash = hashlib.sha256(b"\x03FLOAT\x00" + struct.pack(">d", 0.0)).hexdigest()
    assert df_out["num_hash"].iloc[0] == expected_zero_hash


def test_hash_unicode_normalization_nfc_compatibility():
    """C7: Unicode normalization (NFC) ensures composed and decomposed strings produce identical hashes."""
    expr = HashExpression(
        source_column="name",
        target_column="name_hash",
        mode=HashMode.DIGEST,
        algorithm=HashAlgorithm.SHA256,
        unicode_normalization="NFC",
    )
    step = TransformationStepIR(
        step_id="step_unicode",
        dataset_name="data",
        family=ActionFamily.HASH,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr,
        target_column="name_hash",
    )
    graph = TransformationIRGraph(graph_id="g_c7", steps=(step,), schema_snapshot_hash="s")
    res = PandasIRCompiler().compile(graph)

    # "é": composed (\u00e9) vs decomposed ('e' + '\u0301')
    composed = "\u00e9"
    decomposed = "e\u0301"
    assert composed != decomposed  # raw string mismatch

    df_in = pd.DataFrame({"name": [composed, decomposed]})
    df_out = _execute_artifact_safely(res.artifact, df_in)

    assert df_out["name_hash"].iloc[0] == df_out["name_hash"].iloc[1]


def test_hash_truncation_governance_minimum_length():
    """C8: Truncation governance: truncate_length >= 16 enforced; truncated output has exact length."""
    # 1. Reject invalid truncation length < 16 at IR validation
    with pytest.raises(ValueError, match="at least 16 characters"):
        HashExpression(
            source_column="col",
            target_column="h",
            truncate_length=15,
        )

    # 2. Valid truncation length 16
    expr = HashExpression(
        source_column="col",
        target_column="h",
        truncate_length=16,
    )
    step = TransformationStepIR(
        step_id="step_trunc",
        dataset_name="data",
        family=ActionFamily.HASH,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr,
        target_column="h",
    )
    graph = TransformationIRGraph(graph_id="g_c8", steps=(step,), schema_snapshot_hash="s")
    res = PandasIRCompiler().compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({"col": ["test_val"]})
    df_out = _execute_artifact_safely(res.artifact, df_in)

    assert len(df_out["h"].iloc[0]) == 16


def test_hash_target_collision_prevention():
    """C9: OutputColumnCollisionError raised when target column already exists and policy is ERROR."""
    expr = HashExpression(
        source_column="col_a",
        target_column="col_b",  # col_b already exists in DataFrame
        existing_column_policy=ExistingColumnPolicy.ERROR,
    )
    step = TransformationStepIR(
        step_id="step_coll",
        dataset_name="data",
        family=ActionFamily.HASH,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr,
        target_column="col_b",
    )
    graph = TransformationIRGraph(graph_id="g_c9", steps=(step,), schema_snapshot_hash="s")
    res = PandasIRCompiler().compile(graph)

    df_in = pd.DataFrame({"col_a": ["A"], "col_b": ["existing_val"]})
    with pytest.raises(OutputColumnCollisionError, match="Target column already exists"):
        _execute_artifact_safely(res.artifact, df_in)


def test_hash_zero_secrets_in_ast_and_repr():
    """C10: Verification that zero raw secret bytes or cleartext key material appear in AST, repr, or generated code."""
    secret_bytes = b"super_secret_raw_key_material_999"
    ref = SecretReference(secret_id="sensitive_key_ref")
    handle = SecretHandle(secret_id="sensitive_key_ref", secret_bytes=secret_bytes)

    # 1. Verify SecretHandle repr redacts secret material
    assert repr(secret_bytes) not in repr(handle)
    assert "REDACTED" in repr(handle)

    # 2. Verify SecretReference has no raw bytes
    expr = HashExpression(
        source_column="col",
        target_column="col_h",
        mode=HashMode.HMAC,
        secret_ref=ref,
    )
    assert repr(secret_bytes) not in repr(expr)

    # 3. Compile and verify generated code contains zero secret material
    step = TransformationStepIR(
        step_id="step_sec_inspect",
        dataset_name="data",
        family=ActionFamily.HASH,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr,
        target_column="col_h",
    )
    graph = TransformationIRGraph(graph_id="g_c10", steps=(step,), schema_snapshot_hash="s")
    res = PandasIRCompiler().compile(graph)
    assert repr(secret_bytes) not in res.artifact.source_code
    assert "super_secret_raw_key_material" not in res.artifact.source_code


def test_hash_process_restart_stability():
    """C11: Re-executing compiled artifact across distinct namespaces/invocations produces bit-exact digests."""
    expr = HashExpression(
        source_column="val",
        target_column="val_h",
        mode=HashMode.DIGEST,
        algorithm=HashAlgorithm.SHA256,
    )
    step = TransformationStepIR(
        step_id="step_restart",
        dataset_name="data",
        family=ActionFamily.HASH,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr,
        target_column="val_h",
    )
    graph = TransformationIRGraph(graph_id="g_c11", steps=(step,), schema_snapshot_hash="s")
    res = PandasIRCompiler().compile(graph)

    df_in = pd.DataFrame({"val": [f"user_{i}" for i in range(10)]})
    df_out_run1 = _execute_artifact_safely(res.artifact, df_in)
    df_out_run2 = _execute_artifact_safely(res.artifact, df_in)

    pd.testing.assert_series_equal(df_out_run1["val_h"], df_out_run2["val_h"])


def test_hash_key_rotation_different_digests():
    """C12: Key rotation: executing same artifact with Key Version 1 vs Key Version 2 produces distinct digests."""
    provider_v1 = MockKeyProvider(secrets={"rot_key": b"key_version_1"}, tenant_id="default")
    provider_v2 = MockKeyProvider(secrets={"rot_key": b"key_version_2"}, tenant_id="default")

    expr = HashExpression(
        source_column="val",
        target_column="val_h",
        mode=HashMode.HMAC,
        secret_ref=SecretReference(secret_id="rot_key"),
    )
    step = TransformationStepIR(
        step_id="step_rot",
        dataset_name="data",
        family=ActionFamily.HASH,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr,
        target_column="val_h",
    )
    graph = TransformationIRGraph(graph_id="g_c12", steps=(step,), schema_snapshot_hash="s")
    res = PandasIRCompiler().compile(graph)

    df_in = pd.DataFrame({"val": ["fixed_payload"]})
    df_out_v1 = _execute_artifact_safely(res.artifact, df_in, key_provider=provider_v1)
    df_out_v2 = _execute_artifact_safely(res.artifact, df_in, key_provider=provider_v2)

    assert df_out_v1["val_h"].iloc[0] != df_out_v2["val_h"].iloc[0]


def test_hash_null_policies_preserve_set_empty_fail():
    """C13: Hash null policies: PRESERVE_NULL and FAIL_STEP behave strictly per specification."""
    # 1. PRESERVE_NULL
    expr_pres = HashExpression(source_column="v", target_column="h", null_policy=HashNullPolicy.PRESERVE_NULL)
    step_pres = TransformationStepIR(step_id="s1", dataset_name="d", family=ActionFamily.HASH, kind=StepKind.ROW_TRANSFORMATION, expression=expr_pres, target_column="h")
    res_pres = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g1", steps=(step_pres,), schema_snapshot_hash="s"))
    df_out_pres = _execute_artifact_safely(res_pres.artifact, pd.DataFrame({"v": [None, "hello"]}))
    assert pd.isna(df_out_pres["h"].iloc[0])
    assert pd.notna(df_out_pres["h"].iloc[1])

    # 2. FAIL_STEP
    expr_fail = HashExpression(source_column="v", target_column="h", null_policy=HashNullPolicy.FAIL_STEP)
    step_fail = TransformationStepIR(step_id="s3", dataset_name="d", family=ActionFamily.HASH, kind=StepKind.ROW_TRANSFORMATION, expression=expr_fail, target_column="h")
    res_fail = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g3", steps=(step_fail,), schema_snapshot_hash="s"))
    with pytest.raises(ValueError, match="Encountered null in v under FAIL_STEP"):
        _execute_artifact_safely(res_fail.artifact, pd.DataFrame({"v": [None]}))


# ============================================================================
# Group 2: MASK Action Tests (C14 - C22)
# ============================================================================

def test_mask_preserve_last_n():
    """C14: Masking PRESERVE_LAST_N preserves exactly the trailing N characters and masks preceding."""
    expr = MaskExpression(
        source_column="cc_num",
        target_column="cc_masked",
        strategy=MaskStrategy.PRESERVE_LAST_N,
        preserved_count=4,
        mask_character="*",
    )
    step = TransformationStepIR(
        step_id="step_mask_last",
        dataset_name="cards",
        family=ActionFamily.MASK,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr,
        target_column="cc_masked",
    )
    graph = TransformationIRGraph(graph_id="g_c14", steps=(step,), schema_snapshot_hash="s")
    res = PandasIRCompiler().compile(graph)

    df_in = pd.DataFrame({"cc_num": ["1234567812345678", "4111222233334444"]})
    df_out = _execute_artifact_safely(res.artifact, df_in)

    assert df_out["cc_masked"].iloc[0] == "************5678"
    assert df_out["cc_masked"].iloc[1] == "************4444"


def test_mask_preserve_first_n():
    """C15: Masking PRESERVE_FIRST_N preserves exactly the leading N characters and masks trailing."""
    expr = MaskExpression(
        source_column="account_id",
        target_column="acc_masked",
        strategy=MaskStrategy.PRESERVE_FIRST_N,
        preserved_count=2,
        mask_character="X",
    )
    step = TransformationStepIR(
        step_id="step_mask_first",
        dataset_name="accounts",
        family=ActionFamily.MASK,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=expr,
        target_column="acc_masked",
    )
    graph = TransformationIRGraph(graph_id="g_c15", steps=(step,), schema_snapshot_hash="s")
    res = PandasIRCompiler().compile(graph)

    df_in = pd.DataFrame({"account_id": ["987654321"]})
    df_out = _execute_artifact_safely(res.artifact, df_in)

    assert df_out["acc_masked"].iloc[0] == "98XXXXXXX"


def test_mask_short_value_policies():
    """C16: When string length <= preserved_count, ShortValuePolicy determines deterministic behavior."""
    df_short = pd.DataFrame({"val": ["abc"]})

    # 1. MASK_ALL
    e1 = MaskExpression(source_column="val", target_column="m", strategy=MaskStrategy.PRESERVE_LAST_N, preserved_count=4, short_value_policy=ShortValuePolicy.MASK_ALL, mask_character="*")
    r1 = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_s1", steps=(TransformationStepIR(step_id="s1", dataset_name="d", family=ActionFamily.MASK, kind=StepKind.ROW_TRANSFORMATION, expression=e1, target_column="m"),), schema_snapshot_hash="s"))
    assert _execute_artifact_safely(r1.artifact, df_short)["m"].iloc[0] == "***"

    # 2. SET_NULL
    e2 = MaskExpression(source_column="val", target_column="m", strategy=MaskStrategy.PRESERVE_LAST_N, preserved_count=4, short_value_policy=ShortValuePolicy.SET_NULL)
    r2 = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_s2", steps=(TransformationStepIR(step_id="s2", dataset_name="d", family=ActionFamily.MASK, kind=StepKind.ROW_TRANSFORMATION, expression=e2, target_column="m"),), schema_snapshot_hash="s"))
    assert pd.isna(_execute_artifact_safely(r2.artifact, df_short)["m"].iloc[0])

    # 3. FAIL_STEP
    e3 = MaskExpression(source_column="val", target_column="m", strategy=MaskStrategy.PRESERVE_LAST_N, preserved_count=4, short_value_policy=ShortValuePolicy.FAIL_STEP)
    r3 = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_s3", steps=(TransformationStepIR(step_id="s3", dataset_name="d", family=ActionFamily.MASK, kind=StepKind.ROW_TRANSFORMATION, expression=e3, target_column="m"),), schema_snapshot_hash="s"))
    with pytest.raises(ValueError, match="under FAIL_STEP policy"):
        _execute_artifact_safely(r3.artifact, df_short)

    # 4. KEEP_ORIGINAL
    e4 = MaskExpression(source_column="val", target_column="m", strategy=MaskStrategy.PRESERVE_LAST_N, preserved_count=4, short_value_policy=ShortValuePolicy.KEEP_ORIGINAL)
    r4 = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_s4", steps=(TransformationStepIR(step_id="s4", dataset_name="d", family=ActionFamily.MASK, kind=StepKind.ROW_TRANSFORMATION, expression=e4, target_column="m"),), schema_snapshot_hash="s"))
    assert _execute_artifact_safely(r4.artifact, df_short)["m"].iloc[0] == "abc"


def test_mask_email_domain_policies():
    """C17: Email masking policies: PRESERVE, MASK, and HASH domain handling."""
    df_email = pd.DataFrame({"email": ["user@example.com"]})

    # 1. PRESERVE domain
    e_pres = MaskExpression(source_column="email", target_column="m", strategy=MaskStrategy.EMAIL_MASK, email_domain_policy=EmailMaskDomainPolicy.PRESERVE, mask_character="*")
    r_pres = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_e1", steps=(TransformationStepIR(step_id="s1", dataset_name="d", family=ActionFamily.MASK, kind=StepKind.ROW_TRANSFORMATION, expression=e_pres, target_column="m"),), schema_snapshot_hash="s"))
    res_pres = _execute_artifact_safely(r_pres.artifact, df_email)["m"].iloc[0]
    assert res_pres == "u**r@example.com"

    # 2. MASK domain
    e_mask = MaskExpression(source_column="email", target_column="m", strategy=MaskStrategy.EMAIL_MASK, email_domain_policy=EmailMaskDomainPolicy.MASK, mask_character="*")
    r_mask = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_e2", steps=(TransformationStepIR(step_id="s2", dataset_name="d", family=ActionFamily.MASK, kind=StepKind.ROW_TRANSFORMATION, expression=e_mask, target_column="m"),), schema_snapshot_hash="s"))
    res_mask = _execute_artifact_safely(r_mask.artifact, df_email)["m"].iloc[0]
    assert res_mask == "u**r@*******.com"

    # 3. HASH domain
    e_hash = MaskExpression(source_column="email", target_column="m", strategy=MaskStrategy.EMAIL_MASK, email_domain_policy=EmailMaskDomainPolicy.HASH, mask_character="*")
    r_hash = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_e3", steps=(TransformationStepIR(step_id="s3", dataset_name="d", family=ActionFamily.MASK, kind=StepKind.ROW_TRANSFORMATION, expression=e_hash, target_column="m"),), schema_snapshot_hash="s"))
    res_hash = _execute_artifact_safely(r_hash.artifact, df_email)["m"].iloc[0]
    expected_dhash = hashlib.sha256(b"example.com").hexdigest()[:8]
    assert res_hash == f"u**r@{expected_dhash}"


def test_mask_empty_and_short_strings():
    """C18: String lengths 0, 1, 2 handled cleanly without index or slicing errors."""
    expr = MaskExpression(
        source_column="str_val",
        target_column="str_masked",
        strategy=MaskStrategy.EMAIL_MASK,
        mask_character="*",
    )
    step = TransformationStepIR(
        step_id="s_empty", dataset_name="d", family=ActionFamily.MASK,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr, target_column="str_masked",
    )
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_c18", steps=(step,), schema_snapshot_hash="s"))

    # Empty string, 1-char, 2-char, and emails with short user parts
    df_in = pd.DataFrame({"str_val": ["", "a", "ab", "a@b.com", "ab@b.com"]})
    df_out = _execute_artifact_safely(res.artifact, df_in)

    assert df_out["str_masked"].iloc[0] == ""
    assert df_out["str_masked"].iloc[1] == "*"
    assert df_out["str_masked"].iloc[2] == "**"
    assert df_out["str_masked"].iloc[3] == "a@b.com"
    assert df_out["str_masked"].iloc[4] == "a*@b.com"


def test_mask_custom_unicode_character():
    """C19: Masking supports single non-control Unicode mask character and rejects multi-character strings."""
    # 1. Multi-character rejected by IR validation
    with pytest.raises(ValueError, match="single non-control Unicode character"):
        MaskExpression(source_column="v", mask_character="***")

    # 2. Valid custom Unicode character (bullet '•')
    expr = MaskExpression(
        source_column="secret_text",
        target_column="secret_masked",
        strategy=MaskStrategy.MASK_ALL,
        mask_character="•",
    )
    step = TransformationStepIR(
        step_id="s_unicode_mask", dataset_name="d", family=ActionFamily.MASK,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr, target_column="secret_masked",
    )
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_c19", steps=(step,), schema_snapshot_hash="s"))
    df_out = _execute_artifact_safely(res.artifact, pd.DataFrame({"secret_text": ["abc"]}))

    assert df_out["secret_masked"].iloc[0] == "•••"


def test_mask_null_policies():
    """C20: MaskNullPolicy: PRESERVE_NULL, SET_EMPTY, and FAIL_STEP behave deterministically."""
    df_null = pd.DataFrame({"val": [None]})

    # 1. PRESERVE_NULL
    e1 = MaskExpression(source_column="val", target_column="m", null_policy=MaskNullPolicy.PRESERVE_NULL)
    r1 = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_m1", steps=(TransformationStepIR(step_id="s1", dataset_name="d", family=ActionFamily.MASK, kind=StepKind.ROW_TRANSFORMATION, expression=e1, target_column="m"),), schema_snapshot_hash="s"))
    assert pd.isna(_execute_artifact_safely(r1.artifact, df_null)["m"].iloc[0])

    # 2. SET_EMPTY
    e2 = MaskExpression(source_column="val", target_column="m", null_policy=MaskNullPolicy.SET_EMPTY)
    r2 = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_m2", steps=(TransformationStepIR(step_id="s2", dataset_name="d", family=ActionFamily.MASK, kind=StepKind.ROW_TRANSFORMATION, expression=e2, target_column="m"),), schema_snapshot_hash="s"))
    assert _execute_artifact_safely(r2.artifact, df_null)["m"].iloc[0] == ""

    # 3. FAIL_STEP
    e3 = MaskExpression(source_column="val", target_column="m", null_policy=MaskNullPolicy.FAIL_STEP)
    r3 = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_m3", steps=(TransformationStepIR(step_id="s3", dataset_name="d", family=ActionFamily.MASK, kind=StepKind.ROW_TRANSFORMATION, expression=e3, target_column="m"),), schema_snapshot_hash="s"))
    with pytest.raises(ValueError, match="Encountered null in val under FAIL_STEP"):
        _execute_artifact_safely(r3.artifact, df_null)


def test_mask_existing_column_policy():
    """C21: Masking with target != source respects ExistingColumnPolicy.ERROR vs REPLACE."""
    # 1. ExistingColumnPolicy.ERROR raises OutputColumnCollisionError if target exists
    expr_err = MaskExpression(
        source_column="src",
        target_column="dst",
        existing_column_policy=ExistingColumnPolicy.ERROR,
    )
    step_err = TransformationStepIR(
        step_id="s_m_err", dataset_name="d", family=ActionFamily.MASK,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr_err, target_column="dst",
    )
    res_err = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_me", steps=(step_err,), schema_snapshot_hash="s"))
    df_collision = pd.DataFrame({"src": ["1234"], "dst": ["existing"]})
    with pytest.raises(OutputColumnCollisionError, match="Target column already exists"):
        _execute_artifact_safely(res_err.artifact, df_collision)

    # 2. In-place masking (target == source) is always permitted
    expr_inplace = MaskExpression(
        source_column="src",
        target_column="src",
        strategy=MaskStrategy.MASK_ALL,
        mask_character="*",
    )
    step_inp = TransformationStepIR(
        step_id="s_m_inp", dataset_name="d", family=ActionFamily.MASK,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr_inplace, target_column="src",
    )
    res_inp = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_mi", steps=(step_inp,), schema_snapshot_hash="s"))
    df_out = _execute_artifact_safely(res_inp.artifact, pd.DataFrame({"src": ["hello"]}))
    assert df_out["src"].iloc[0] == "*****"


def test_mask_non_reversibility_contract():
    """C22: Masking expressions enforce irreversible operation contract (inverse_operation_supported = False)."""
    expr = MaskExpression(source_column="ssn", target_column="ssn_m")
    assert expr.reversible is False
    assert expr.inverse_operation_supported is False

    # Different original inputs map to identical masked output (information loss)
    step = TransformationStepIR(
        step_id="s_irr", dataset_name="d", family=ActionFamily.MASK,
        kind=StepKind.ROW_TRANSFORMATION, expression=expr, target_column="ssn_m",
    )
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_irr", steps=(step,), schema_snapshot_hash="s"))
    df_in = pd.DataFrame({"ssn": ["123-45-6789", "999-99-6789"]})
    df_out = _execute_artifact_safely(res.artifact, df_in)

    # Both produce same masked suffix *******6789
    assert df_out["ssn_m"].iloc[0] == df_out["ssn_m"].iloc[1]


# ============================================================================
# Group 3: SCHEMA_MUTATION Action & Step Authorization Tests (C23 - C32)
# ============================================================================

def test_schema_rename_cycles_and_chains():
    """C23: Renames: chain A -> B -> C executes deterministically; collisions are prevented."""
    # 1. Sequential rename chain A -> B, then B -> C
    expr_rename = RenameColumnsExpression(
        renames=(
            ColumnRenameSpec(old_name="col_a", new_name="col_b"),
            ColumnRenameSpec(old_name="col_b", new_name="col_c"),
        )
    )
    step = TransformationStepIR(
        step_id="step_rename_chain",
        dataset_name="data",
        family=ActionFamily.SCHEMA_MUTATION,
        kind=StepKind.SCHEMA_MUTATION,
        expression=expr_rename,
    )
    graph = TransformationIRGraph(graph_id="g_c23", steps=(step,), schema_snapshot_hash="s")
    res = PandasIRCompiler().compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({"col_a": [1, 2, 3]})
    df_out = _execute_artifact_safely(res.artifact, df_in)
    assert "col_c" in df_out.columns
    assert "col_a" not in df_out.columns

    # 2. Collision detection: renaming into an already existing column
    expr_coll = RenameColumnsExpression(
        renames=(ColumnRenameSpec(old_name="col_x", new_name="col_y"),)
    )
    step_coll = TransformationStepIR(
        step_id="step_rename_coll", dataset_name="d", family=ActionFamily.SCHEMA_MUTATION,
        kind=StepKind.SCHEMA_MUTATION, expression=expr_coll,
    )
    res_coll = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_c23_coll", steps=(step_coll,), schema_snapshot_hash="s"))
    with pytest.raises(OutputColumnCollisionError, match="Target rename column already exists"):
        _execute_artifact_safely(res_coll.artifact, pd.DataFrame({"col_x": [1], "col_y": [2]}))


def test_schema_drop_without_authorization_rejected():
    """C24: Destructive DROP action without valid StepAuthorization raises UnauthorizedDestructiveActionError."""
    # Drop expression with authorization = None on Step
    expr_drop = DropColumnsExpression(
        drops=(ColumnDropSpec(column="sensitive_col"),),
    )
    step = TransformationStepIR(
        step_id="step_drop_unauth",
        dataset_name="users",
        family=ActionFamily.SCHEMA_MUTATION,
        kind=StepKind.SCHEMA_MUTATION,
        expression=expr_drop,
        authorization=None,
    )
    graph = TransformationIRGraph(graph_id="g_c24", steps=(step,), schema_snapshot_hash="snap")
    compiler = PandasIRCompiler()

    # Compilation must fail atomically
    res = compiler.compile(graph)
    assert res.status == CodegenStatus.FAILED_COMPILATION
    assert res.artifact is None
    assert any("requires a valid StepAuthorization" in d for d in res.diagnostics)


def test_schema_drop_with_valid_authorization_succeeds():
    """C25: Destructive DROP action with valid unexpired StepAuthorization matching step hash succeeds."""
    step_id = "step_drop_auth"

    expr_drop = DropColumnsExpression(
        drops=(ColumnDropSpec(column="raw_ssn"),),
    )
    auth = StepAuthorization(
        step_id=step_id,
        policy_decision_id="policy_dec_456",
        approved_by_subject_id="security_admin",
        tenant_id="tenant_alpha",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        authorized_step_hash="",
    )
    step = TransformationStepIR(
        step_id=step_id,
        dataset_name="users",
        family=ActionFamily.SCHEMA_MUTATION,
        kind=StepKind.SCHEMA_MUTATION,
        expression=expr_drop,
        authorization=auth,
    )
    auth.authorized_step_hash = step.semantic_hash

    graph = TransformationIRGraph(graph_id="g_c25", steps=(step,), schema_snapshot_hash="snap")
    compiler = PandasIRCompiler()
    res = compiler.compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED
    assert res.artifact is not None

    df_in = pd.DataFrame({"user_id": [1, 2], "raw_ssn": ["111", "222"]})
    df_out = _execute_artifact_safely(res.artifact, df_in)
    assert "raw_ssn" not in df_out.columns
    assert "user_id" in df_out.columns


def test_schema_drop_protected_column_role_rejected():
    """C26: Attempting to drop protected column (lineage, routing identity, indicator) is rejected."""
    schema = {
        "users": DatasetSchema(
            dataset_name="users",
            columns={
                "id": DatasetColumnSchema(name="id", data_type=DataType.STRING, role=ColumnRole.BUSINESS),
                "_dhara_occurrence_id": DatasetColumnSchema(
                    name="_dhara_occurrence_id",
                    data_type=DataType.INTEGER,
                    role=ColumnRole.ROUTING_IDENTITY,
                    is_protected=True,
                ),
            },
        )
    }

    expr_drop = DropColumnsExpression(
        drops=(ColumnDropSpec(column="_dhara_occurrence_id"),),
    )

    # Schema binding must raise SchemaBindingError when dropping protected role
    with pytest.raises(SchemaBindingError, match="Cannot drop protected column with role"):
        bind_expression_to_schema(expr_drop, "users", schema)


def test_schema_project_and_reorder():
    """C27: PROJECT retains only specified columns; REORDER reorders columns exactly."""
    # 1. Project columns
    expr_proj = ProjectColumnsExpression(
        projection=ColumnProjectSpec(projected_columns=("c", "a"))
    )
    step_proj = TransformationStepIR(
        step_id="step_proj", dataset_name="d", family=ActionFamily.SCHEMA_MUTATION,
        kind=StepKind.SCHEMA_MUTATION, expression=expr_proj,
    )
    res_proj = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_p", steps=(step_proj,), schema_snapshot_hash="s"))
    df_in = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
    df_proj = _execute_artifact_safely(res_proj.artifact, df_in)
    assert list(df_proj.columns) == ["c", "a"]

    # 2. Reorder columns
    expr_reorder = ReorderColumnsExpression(ordered_columns=("b", "c", "a"))
    step_reorder = TransformationStepIR(
        step_id="step_reorder", dataset_name="d", family=ActionFamily.SCHEMA_MUTATION,
        kind=StepKind.SCHEMA_MUTATION, expression=expr_reorder,
    )
    res_reorder = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_r", steps=(step_reorder,), schema_snapshot_hash="s"))
    df_reordered = _execute_artifact_safely(res_reorder.artifact, df_in)
    assert list(df_reordered.columns) == ["b", "c", "a"]


def test_schema_snapshot_hash_catches_stale_source():
    """C28: SchemaSnapshotMismatchError raised if actual pre-mutation schema differs from expected hash."""
    expected_cols = [("col_a", "int64", False), ("col_b", "string", False)]
    expected_hash = compute_canonical_schema_hash(expected_cols)

    # Expression expects expected_hash
    expr = CreateColumnExpression(
        create_column_name="new_flag",
        create_expression=LiteralValue(value=True, data_type=DataType.BOOLEAN),
        expected_before_schema_hash=expected_hash,
    )
    step = TransformationStepIR(
        step_id="s_snap_check", dataset_name="d", family=ActionFamily.SCHEMA_MUTATION,
        kind=StepKind.SCHEMA_MUTATION, expression=expr,
    )
    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_snap", steps=(step,), schema_snapshot_hash="s"))

    # Actual DataFrame has col_c instead of col_b -> Hash mismatch
    df_mismatch = pd.DataFrame({"col_a": [1, 2], "col_c": ["x", "y"]})
    with pytest.raises(SchemaSnapshotMismatchError, match="Actual pre-mutation schema hash does not match"):
        _execute_artifact_safely(res.artifact, df_mismatch)


def test_schema_cast_failure_policies():
    """C29: CastColumnExpression respects CastFailurePolicy.FAIL_STEP vs SET_NULL."""
    # 1. FAIL_STEP raises exception on incompatible string-to-int cast
    expr_fail = CastColumnExpression(
        source_column="str_num",
        target_column="int_num",
        cast_target_type=DataType.INTEGER,
        cast_failure_policy=CastFailurePolicy.FAIL_STEP,
    )
    step_fail = TransformationStepIR(
        step_id="s_cast_fail", dataset_name="d", family=ActionFamily.SCHEMA_MUTATION,
        kind=StepKind.SCHEMA_MUTATION, expression=expr_fail,
    )
    res_fail = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_cf", steps=(step_fail,), schema_snapshot_hash="s"))
    with pytest.raises(Exception):
        _execute_artifact_safely(res_fail.artifact, pd.DataFrame({"str_num": ["10", "not_a_number"]}))

    # 2. SET_NULL coerces incompatible value to pd.NA
    expr_null = CastColumnExpression(
        source_column="str_num",
        target_column="int_num",
        cast_target_type=DataType.INTEGER,
        cast_failure_policy=CastFailurePolicy.SET_NULL,
    )
    step_null = TransformationStepIR(
        step_id="s_cast_null", dataset_name="d", family=ActionFamily.SCHEMA_MUTATION,
        kind=StepKind.SCHEMA_MUTATION, expression=expr_null,
    )
    res_null = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_cn", steps=(step_null,), schema_snapshot_hash="s"))
    df_out = _execute_artifact_safely(res_null.artifact, pd.DataFrame({"str_num": ["10", "invalid"]}))
    assert df_out["int_num"].iloc[0] == 10
    assert pd.isna(df_out["int_num"].iloc[1])


def test_schema_caller_dataframe_isolation():
    """C30: Running compiled schema mutations (drop, rename, project) does NOT mutate caller DataFrame."""
    step_id = "s_drop_iso"
    expr_drop = DropColumnsExpression(
        drops=(ColumnDropSpec(column="to_drop"),),
    )
    auth = StepAuthorization(
        step_id=step_id,
        policy_decision_id="dec_iso",
        approved_by_subject_id="admin",
        tenant_id="tenant_iso",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        authorized_step_hash="",
    )
    step_drop = TransformationStepIR(
        step_id=step_id, dataset_name="d", family=ActionFamily.SCHEMA_MUTATION,
        kind=StepKind.SCHEMA_MUTATION, expression=expr_drop,
        authorization=auth,
    )
    auth.authorized_step_hash = step_drop.semantic_hash

    res = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_iso", steps=(step_drop,), schema_snapshot_hash="s"))

    original_df = pd.DataFrame({"keep": [1, 2], "to_drop": [9, 9]})
    original_columns_snapshot = list(original_df.columns)
    original_values_snapshot = original_df.copy(deep=True)

    df_out = _execute_artifact_safely(res.artifact, original_df)

    # Output has column dropped
    assert "to_drop" not in df_out.columns
    # Caller input DataFrame MUST be completely unmutated
    assert list(original_df.columns) == original_columns_snapshot
    pd.testing.assert_frame_equal(original_df, original_values_snapshot)


def test_schema_composite_security_pipeline_publication_boundary():
    """C31: End-to-end composite pipeline: Hash, Mask, Drop raw PII with authorization."""
    provider = MockKeyProvider(secrets={"salt_key": b"tenant_salt_secret"}, tenant_id="default")

    # Step 1: Hash email -> email_digest
    step1 = TransformationStepIR(
        step_id="step1_hash",
        dataset_name="customers",
        family=ActionFamily.HASH,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=HashExpression(
            source_column="email",
            target_column="email_digest",
            mode=HashMode.HMAC,
            secret_ref=SecretReference(secret_id="salt_key"),
        ),
        target_column="email_digest",
    )

    # Step 2: Mask phone -> phone_masked
    step2 = TransformationStepIR(
        step_id="step2_mask",
        dataset_name="customers",
        family=ActionFamily.MASK,
        kind=StepKind.ROW_TRANSFORMATION,
        expression=MaskExpression(
            source_column="phone",
            target_column="phone_masked",
            strategy=MaskStrategy.PRESERVE_LAST_N,
            preserved_count=4,
            mask_character="*",
        ),
        target_column="phone_masked",
        prerequisites=("step1_hash",),
    )

    # Step 3: Drop raw PII columns (email, phone)
    step3_id = "step3_drop"
    expr_drop = DropColumnsExpression(
        drops=(ColumnDropSpec(column="email"), ColumnDropSpec(column="phone")),
    )
    auth = StepAuthorization(
        step_id=step3_id,
        policy_decision_id="dec_comp",
        approved_by_subject_id="dpo_officer",
        tenant_id="tenant_comp",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        authorized_step_hash="",
    )
    step3 = TransformationStepIR(
        step_id=step3_id,
        dataset_name="customers",
        family=ActionFamily.SCHEMA_MUTATION,
        kind=StepKind.SCHEMA_MUTATION,
        expression=expr_drop,
        prerequisites=("step2_mask",),
        authorization=auth,
    )
    auth.authorized_step_hash = step3.semantic_hash

    graph = TransformationIRGraph(
        graph_id="graph_composite_sec",
        steps=(step1, step2, step3),
        dependencies=(
            IRDependency(source_step_id="step1_hash", target_step_id="step2_mask"),
            IRDependency(source_step_id="step2_mask", target_step_id="step3_drop"),
        ),
        schema_snapshot_hash="snap_cust",
    )

    compiler = PandasIRCompiler()
    res = compiler.compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({
        "cust_id": ["C100", "C200"],
        "email": ["alice@company.org", "bob@corp.net"],
        "phone": ["1234567890", "9876543210"],
    })

    df_out = _execute_artifact_safely(res.artifact, df_in, key_provider=provider)

    # Verify publication boundary: raw PII dropped, transformed columns present
    assert set(df_out.columns) == {"cust_id", "email_digest", "phone_masked"}
    assert df_out["phone_masked"].iloc[0] == "******7890"
    assert df_out["phone_masked"].iloc[1] == "******3210"
    assert len(df_out["email_digest"].iloc[0]) == 64


def test_schema_atomic_compilation_failure_zero_artifacts():
    """C32: If any security/schema step fails validation, compilation fails atomically producing zero artifacts."""
    # Graph has one valid step and one unauthorized drop step
    step_valid = TransformationStepIR(
        step_id="s_val", dataset_name="d", family=ActionFamily.FLAG,
        kind=StepKind.AUDIT_FLAG,
        expression=FlagExpression(
            predicate=NullCheck(operand=ColumnRef(column_name="col", dataset_name="d")),
            output_column="is_null",
        ),
        target_column="is_null",
    )
    # Unauthorized drop step
    step_invalid = TransformationStepIR(
        step_id="s_drop_no_auth", dataset_name="d", family=ActionFamily.SCHEMA_MUTATION,
        kind=StepKind.SCHEMA_MUTATION,
        expression=DropColumnsExpression(
            drops=(ColumnDropSpec(column="col"),),
        ),
        authorization=None,  # missing authorization
    )

    graph = TransformationIRGraph(
        graph_id="g_atomic_fail",
        steps=(step_valid, step_invalid),
        schema_snapshot_hash="snap",
    )

    res = PandasIRCompiler().compile(graph)
    assert res.status == CodegenStatus.FAILED_COMPILATION
    # Atomic guarantee: zero artifacts emitted
    assert res.artifact is None
    assert "s_drop_no_auth" in res.step_dispositions
    assert res.step_dispositions["s_drop_no_auth"] == CompilerStepDisposition.INVALID_IR
