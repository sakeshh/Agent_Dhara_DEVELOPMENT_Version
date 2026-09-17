"""
tests/cross_engine/test_phase4c_controlled_dispatch.py

Phase 4C Test Suite: Controlled Engine Dispatch, Runtime Limits, and Governance (PROD-5).
Verifies:
1. Preflight gate rejection produces dispatch_allowed=False, execution_started=False, publication_started=False.
2. Result invariant validation (invariant violations raise ValueError).
3. Execution timeout fencing and deterministic cleanup.
4. Cooperative cancellation (pre-execution and during execution).
5. Resource limits (max input rows, max output rows, join expansion ratio, aggregation cardinality cap).
6. Multi-engine dispatch (Pandas, Azure SQL target evidence, ADF delegation).
7. Deterministic cleanup guarantees in finally blocks across all failure and success modes.
"""
from __future__ import annotations

import datetime
import hashlib
import json
import threading
import time
from typing import Any, Dict, List, Tuple

import pandas as pd
import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519

from agent.etl_pipeline.adf_orchestration_adapter import (
    AdfExecutionEnvelope,
    AdfOrchestrationAdapter,
)
from agent.etl_pipeline.azure_sql_execution_adapter import (
    AzureSqlDatabaseRuntimeProfile,
    AzureSqlDirectExecutionAdapter,
)
from agent.etl_pipeline.controlled_dispatcher import (
    ControlledDispatcher,
    DispatchExecutionResult,
    ExecutionCancelledError,
    ExecutionGateDisallowedError,
    ExecutionTimeoutError,
    ResourceLimitExceededError,
    RuntimeLimits,
)
from agent.etl_pipeline.execution_gate import (
    ExecutionGateDecision,
    ExecutionPreflightGate,
)
from agent.etl_pipeline.secure_manifest import (
    ArtifactSigner,
    SignedArtifactManifest,
)


def _make_valid_manifest_and_code() -> Tuple[SignedArtifactManifest, str, ed25519.Ed25519PrivateKey, str]:
    signer = ArtifactSigner()
    private_key, public_key_hex = signer.generate_keypair()
    code = "df['processed'] = True\n"
    code_digest = hashlib.sha256(code.encode("utf-8")).hexdigest()

    manifest = ArtifactSigner.create_signed_manifest(
        tenant_id="tenant-prod-alpha",
        plan_id="plan-4c-001",
        plan_digest="a" * 64,
        artifact_id="art-4c-001",
        artifact_content=code,
        engine_name="pandas",
        engine_version="2.2.0",
        compiler_name="pandas_ir_compiler",
        compiler_version="1.0.0",
        catalogue_version="v1.0",
        runtime_profile_hash="b" * 64,
        capability_decision_ids=("CAP-001", "CAP-002"),
        parameter_manifest_hash="c" * 64,
        signer_key_id="trusted_prod",
        private_key=private_key,
        publication_scope="certified_production",
    )
    return manifest, code, private_key, public_key_hex


def test_dispatcher_enforces_gate_rejection_invariants():
    """Gate failure must produce dispatch_allowed=False, execution_started=False, publication_started=False."""
    manifest, code, private_key, pubkey_hex = _make_valid_manifest_and_code()
    # Provide mismatched code to trigger gate failure
    bad_code = "df['tampered'] = True\n"
    dispatcher = ControlledDispatcher()

    cleanup_log = []
    def cleanup_fn():
        cleanup_log.append("cleaned_up")

    result = dispatcher.dispatch(
        manifest=manifest,
        code_artifact=bad_code,
        target_engine="pandas",
        trusted_keys={"trusted_prod": pubkey_hex},
        cleanup_callbacks=[("test_cleanup", cleanup_fn)],
    )

    assert result.dispatch_allowed is False
    assert result.execution_started is False
    assert result.publication_started is False
    assert result.status == "BLOCKED"
    assert result.cleanup_guaranteed is True
    assert "test_cleanup" in result.cleanup_actions
    assert cleanup_log == ["cleaned_up"]


def test_dispatch_result_invariant_violations_raise():
    """Validates that DispatchExecutionResult rejects invalid combinations of execution states."""
    # Cannot have execution_started = True if dispatch_allowed = False
    with pytest.raises(ValueError, match="Invariant violation"):
        DispatchExecutionResult(
            dispatch_allowed=False,
            execution_started=True,
            publication_started=False,
            status="BLOCKED",
            engine_name="pandas",
        )

    # Cannot have publication_started = True during execution dispatch
    with pytest.raises(ValueError, match="Invariant violation"):
        DispatchExecutionResult(
            dispatch_allowed=True,
            execution_started=True,
            publication_started=True,
            status="SUCCESS",
            engine_name="pandas",
        )


def test_dispatcher_timeout_fencing():
    """Tasks exceeding timeout must transition to TIMED_OUT and guarantee cleanup."""
    manifest, code, private_key, pubkey_hex = _make_valid_manifest_and_code()
    dispatcher = ControlledDispatcher()

    def slow_task():
        time.sleep(1.0)
        return "finished"

    cleanup_log = []
    def cleanup_fn():
        cleanup_log.append("temp_tables_dropped")

    result = dispatcher.dispatch(
        manifest=manifest,
        code_artifact=code,
        target_engine="pandas",
        runtime_limits=RuntimeLimits(timeout_seconds=0.1),
        execution_callable=slow_task,
        trusted_keys={"trusted_prod": pubkey_hex},
        cleanup_callbacks=[("drop_temp", cleanup_fn)],
    )

    assert result.dispatch_allowed is True
    assert result.execution_started is True
    assert result.publication_started is False
    assert result.status == "TIMED_OUT"
    assert "EXECUTION_TIMEOUT" in result.error_message
    assert result.cleanup_guaranteed is True
    assert "drop_temp" in result.cleanup_actions
    assert cleanup_log == ["temp_tables_dropped"]


def test_dispatcher_cancellation_before_start():
    """Cancellation prior to start must not set execution_started=True."""
    manifest, code, private_key, pubkey_hex = _make_valid_manifest_and_code()
    dispatcher = ControlledDispatcher()
    cancel_token = threading.Event()
    cancel_token.set()  # Pre-cancelled

    cleanup_called = False
    def cleanup():
        nonlocal cleanup_called
        cleanup_called = True

    result = dispatcher.dispatch(
        manifest=manifest,
        code_artifact=code,
        target_engine="pandas",
        cancellation_token=cancel_token,
        trusted_keys={"trusted_prod": pubkey_hex},
        cleanup_callbacks=[("cleanup", cleanup)],
    )

    assert result.dispatch_allowed is True
    assert result.execution_started is False
    assert result.publication_started is False
    assert result.status == "CANCELLED"
    assert result.cleanup_guaranteed is True
    assert cleanup_called is True


def test_dispatcher_cancellation_during_execution():
    """Cancellation during execution transitions to CANCELLED and guarantees cleanup."""
    manifest, code, private_key, pubkey_hex = _make_valid_manifest_and_code()
    dispatcher = ControlledDispatcher()
    cancel_token = threading.Event()

    def mid_task():
        time.sleep(0.05)
        cancel_token.set()
        time.sleep(0.2)
        return "done"

    cleanup_log = []
    result = dispatcher.dispatch(
        manifest=manifest,
        code_artifact=code,
        target_engine="pandas",
        runtime_limits=RuntimeLimits(timeout_seconds=2.0),
        cancellation_token=cancel_token,
        execution_callable=mid_task,
        trusted_keys={"trusted_prod": pubkey_hex},
        cleanup_callbacks=[("close_socket", lambda: cleanup_log.append("socket_closed"))],
    )

    assert result.dispatch_allowed is True
    assert result.execution_started is True
    assert result.publication_started is False
    assert result.status == "CANCELLED"
    assert result.cleanup_guaranteed is True
    assert cleanup_log == ["socket_closed"]


def test_dispatcher_input_row_limit_fencing():
    """Input row limit exceeded before execution begins prevents execution_started=True."""
    manifest, code, private_key, pubkey_hex = _make_valid_manifest_and_code()
    dispatcher = ControlledDispatcher()
    input_df = pd.DataFrame({"col": range(100)})

    result = dispatcher.dispatch(
        manifest=manifest,
        code_artifact=code,
        target_engine="pandas",
        runtime_limits=RuntimeLimits(max_input_rows=50),
        input_data=input_df,
        trusted_keys={"trusted_prod": pubkey_hex},
    )

    assert result.dispatch_allowed is True
    assert result.execution_started is False
    assert result.publication_started is False
    assert result.status == "LIMIT_EXCEEDED"
    assert "INPUT_ROW_LIMIT_EXCEEDED" in result.error_message


def test_dispatcher_output_row_limit_fencing():
    """Output row limit exceeded marks LIMIT_EXCEEDED and guarantees cleanup."""
    manifest, code, private_key, pubkey_hex = _make_valid_manifest_and_code()
    dispatcher = ControlledDispatcher()

    # Returns 150 rows when limit is 100
    def gen_output():
        return pd.DataFrame({"out": range(150)})

    result = dispatcher.dispatch(
        manifest=manifest,
        code_artifact=code,
        target_engine="pandas",
        runtime_limits=RuntimeLimits(max_output_rows=100),
        execution_callable=gen_output,
        trusted_keys={"trusted_prod": pubkey_hex},
    )

    assert result.dispatch_allowed is True
    assert result.execution_started is True
    assert result.publication_started is False
    assert result.status == "LIMIT_EXCEEDED"
    assert "OUTPUT_ROW_LIMIT_EXCEEDED" in result.error_message
    assert result.output_artifact is None  # Suppressed due to limit violation


def test_dispatcher_join_expansion_limit():
    """Join-expansion ratio limit exceeded aborts and reports violation."""
    manifest, code, private_key, pubkey_hex = _make_valid_manifest_and_code()
    dispatcher = ControlledDispatcher()
    input_df = pd.DataFrame({"id": [1, 2, 3, 4, 5]})  # 5 rows

    # Returns 50 rows (expansion ratio = 10.0x, configured max = 3.0x)
    def joined_output():
        return pd.DataFrame({"id": range(50)})

    result = dispatcher.dispatch(
        manifest=manifest,
        code_artifact=code,
        target_engine="pandas",
        runtime_limits=RuntimeLimits(max_row_expansion_ratio=3.0),
        input_data=input_df,
        execution_callable=joined_output,
        trusted_keys={"trusted_prod": pubkey_hex},
    )

    assert result.dispatch_allowed is True
    assert result.execution_started is True
    assert result.status == "LIMIT_EXCEEDED"
    assert "JOIN_ROW_EXPANSION_LIMIT_EXCEEDED" in result.error_message
    assert result.runtime_metrics["row_expansion_ratio"] == 10.0


def test_dispatcher_aggregation_cardinality_cap():
    """Aggregation cardinality cap exceeded aborts and reports violation."""
    manifest, code, private_key, pubkey_hex = _make_valid_manifest_and_code()
    dispatcher = ControlledDispatcher()

    def agg_output():
        return pd.DataFrame({"group": range(500), "count": [1] * 500})

    result = dispatcher.dispatch(
        manifest=manifest,
        code_artifact=code,
        target_engine="pandas",
        runtime_limits=RuntimeLimits(max_aggregation_cardinality=200),
        execution_callable=agg_output,
        trusted_keys={"trusted_prod": pubkey_hex},
    )

    assert result.dispatch_allowed is True
    assert result.execution_started is True
    assert result.status == "LIMIT_EXCEEDED"
    assert "AGGREGATION_CARDINALITY_LIMIT_EXCEEDED" in result.error_message


def test_dispatcher_pandas_successful_dispatch():
    """Successful execution of Python/Pandas code with input data."""
    manifest, code, private_key, pubkey_hex = _make_valid_manifest_and_code()
    dispatcher = ControlledDispatcher()
    df_in = pd.DataFrame({"a": [1, 2, 3]})

    result = dispatcher.dispatch(
        manifest=manifest,
        code_artifact=code,
        target_engine="pandas",
        input_data=df_in,
        trusted_keys={"trusted_prod": pubkey_hex},
    )

    assert result.dispatch_allowed is True
    assert result.execution_started is True
    assert result.publication_started is False
    assert result.status == "SUCCESS"
    assert isinstance(result.output_artifact, pd.DataFrame)
    assert "processed" in result.output_artifact.columns
    assert result.output_artifact["processed"].tolist() == [True, True, True]


def test_dispatcher_azure_sql_target_evidence_dispatch():
    """Authoritative Azure SQL target evidence dispatch via AzureSqlDirectExecutionAdapter."""
    signer = ArtifactSigner()
    privkey, pubkey_hex = signer.generate_keypair()
    sql_code = "SELECT 1 AS val;"
    sql_digest = hashlib.sha256(sql_code.encode("utf-8")).hexdigest()

    manifest = ArtifactSigner.create_signed_manifest(
        tenant_id="tenant-prod-sql",
        plan_id="plan-sql-001",
        plan_digest="a" * 64,
        artifact_id="art-sql-001",
        artifact_content=sql_code,
        engine_name="azure_sql",
        engine_version="160",
        compiler_name="azure_sql_tsql_ir_compiler",
        compiler_version="1.0.0",
        catalogue_version="v1.0",
        runtime_profile_hash="b" * 64,
        capability_decision_ids=("CAP-SQL-01",),
        parameter_manifest_hash="c" * 64,
        signer_key_id="trusted_prod",
        private_key=privkey,
        publication_scope="certified_production",
    )

    adapter = AzureSqlDirectExecutionAdapter(
        runtime_profile=AzureSqlDatabaseRuntimeProfile(compatibility_level="160")
    )

    # Valid evidence bundle
    res_rows = [{"val": 1}]
    evidence = {
        "runtime_profile": {"engine_name": "azure_sql_database", "compatibility_level": "160"},
        "generated_tsql_artifact": sql_code,
        "tsql_artifact_sha256": sql_digest,
        "azure_sql_result": res_rows,
        "azure_sql_result_hash": hashlib.sha256(json.dumps(res_rows, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest(),
    }

    dispatcher = ControlledDispatcher()
    result = dispatcher.dispatch(
        manifest=manifest,
        code_artifact=sql_code,
        target_engine="azure_sql",
        azure_sql_adapter=adapter,
        azure_sql_evidence=evidence,
        trusted_keys={"trusted_prod": pubkey_hex},
    )

    assert result.dispatch_allowed is True
    assert result.execution_started is True
    assert result.status == "SUCCESS"
    assert result.output_artifact == res_rows


def test_dispatcher_adf_delegation_dispatch():
    """ADF orchestration and delegation envelope dispatch."""
    signer = ArtifactSigner()
    privkey, pubkey_hex = signer.generate_keypair()
    py_code = "# PySpark script to be delegated via ADF"
    py_digest = hashlib.sha256(py_code.encode("utf-8")).hexdigest()

    manifest = ArtifactSigner.create_signed_manifest(
        tenant_id="tenant-adf-prod",
        plan_id="plan-adf-001",
        plan_digest="a" * 64,
        artifact_id="art-adf-001",
        artifact_content=py_code,
        engine_name="azure_data_factory",
        engine_version="v2",
        compiler_name="adf_orchestration_adapter",
        compiler_version="1.0.0",
        catalogue_version="v1.0",
        runtime_profile_hash="b" * 64,
        capability_decision_ids=("CAP-ADF-01",),
        parameter_manifest_hash="c" * 64,
        signer_key_id="trusted_prod",
        private_key=privkey,
        publication_scope="certified_production",
    )

    adf_adapter = AdfOrchestrationAdapter()
    envelope = AdfExecutionEnvelope(
        tenant_id="tenant-adf-prod",
        plan_id="plan-adf-001",
        plan_digest=hashlib.sha256(b"plan_adf").hexdigest(),
        artifact_id="art-adf-001",
        artifact_sha256=py_digest,
        compiler_name="pyspark_ir_compiler",
        compiler_version="v1.0",
        engine_name="pyspark",
        engine_version="3.5.1",
        runtime_profile_hash="a" * 64,
        capability_decision_ids=("DEC-01",),
        parameter_manifest_hash="b" * 64,
        publication_scope="certified_production",
    )

    dispatcher = ControlledDispatcher()
    result = dispatcher.dispatch(
        manifest=manifest,
        code_artifact=py_code,
        target_engine="azure_data_factory",
        adf_adapter=adf_adapter,
        adf_envelope=envelope,
        trusted_keys={"trusted_prod": pubkey_hex},
    )

    assert result.dispatch_allowed is True
    assert result.execution_started is True
    assert result.status == "SUCCESS"
    assert result.output_artifact.activity_name == "dispatch_plan-adf-001"


def test_cleanup_guarantee_runs_all_callbacks_even_with_exceptions():
    """Verifies that all cleanup callbacks are run even if one throws an exception."""
    manifest, code, private_key, pubkey_hex = _make_valid_manifest_and_code()
    dispatcher = ControlledDispatcher()

    cleanup_log = []
    def faulty_cleanup():
        cleanup_log.append("faulty_called")
        raise RuntimeError("Disk unmount failed")

    def normal_cleanup():
        cleanup_log.append("normal_called")

    result = dispatcher.dispatch(
        manifest=manifest,
        code_artifact=code,
        target_engine="pandas",
        execution_callable=lambda: "done",
        trusted_keys={"trusted_prod": pubkey_hex},
        cleanup_callbacks=[
            ("faulty", faulty_cleanup),
            ("normal", normal_cleanup),
        ],
    )

    assert result.cleanup_guaranteed is True
    assert "faulty_FAILED" in result.cleanup_actions
    assert "normal" in result.cleanup_actions
    assert cleanup_log == ["faulty_called", "normal_called"]
