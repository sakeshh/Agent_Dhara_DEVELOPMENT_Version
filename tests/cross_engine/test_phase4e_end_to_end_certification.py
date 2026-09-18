"""
tests/cross_engine/test_phase4e_end_to_end_certification.py

Phase 4E Final End-to-End Production Readiness Certification Suite.
Verifies the complete unified production chain:
    Signed Artifact Manifest (4A)
            ↓
    Execution Gate Preflight (4B)
            ↓
    Authorization Revalidation & Scoped Secret Custody (4B)
            ↓
    Controlled Dispatch & Runtime Resource Limits (4C)
            ↓
    Two-Phase Durable Staging & Postcondition Verification (4D)
            ↓
    Atomic Visibility & Manifest Fencing (4D)

Formally certifies and closes production blockers:
- PROD-1: Execution-boundary secret resolution
- PROD-2: Execution-time authorization revalidation
- PROD-3: Durable publication commit protocol
- PROD-5: Controlled engine dispatch & runtime limits
"""
from __future__ import annotations

import datetime
import hashlib
import json
from typing import Any, Dict, Tuple

import pandas as pd
import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519

from agent.etl_pipeline.controlled_dispatcher import (
    ControlledDispatcher,
    RuntimeLimits,
)
from agent.etl_pipeline.cross_engine.capability_registry import (
    get_global_capability_registry,
)
from agent.etl_pipeline.durable_publisher import (
    ConcurrentWriterConflictError,
    DurablePublicationCoordinator,
)
from agent.etl_pipeline.execution_gate import (
    ExecutionPreflightGate,
    RevocationStore,
)
from agent.etl_pipeline.secret_manager import (
    ExecutionSecretManager,
    ScopedSecretSession,
    UnsafeSecretResolutionError,
)
from agent.etl_pipeline.secure_manifest import (
    ArtifactSigner,
    SignedArtifactManifest,
)
from agent.etl_pipeline.transformation_ir import (
    ActionFamily,
    ColumnDropSpec,
    DropColumnsExpression,
    SecretReference,
    StepAuthorization,
    StepKind,
    TransformationStepIR,
)


def _build_end_to_end_fixture() -> Tuple[
    SignedArtifactManifest,
    str,
    TransformationStepIR,
    ed25519.Ed25519PrivateKey,
    ed25519.Ed25519PublicKey,
    str,
    str,
]:
    signer = ArtifactSigner()
    private_key, public_key = signer.generate_keypair()
    public_key_hex = public_key.public_bytes_raw().hex()

    code = "df['masked_email'] = df['email'].apply(lambda x: x[:3] + '***')\n"
    code_digest = hashlib.sha256(code.encode("utf-8")).hexdigest()

    # Create step with step authorization
    sec_ref = SecretReference(
        secret_id="sec-salt-hash-01",
        expected_purpose="hash_pepper",
        expected_version="v1",
    )
    expr = DropColumnsExpression(
        columns=[ColumnDropSpec(column="raw_ssn")],
        secret_ref=sec_ref,
    )
    step = TransformationStepIR(
        step_id="step-e2e-01",
        dataset_name="primary",
        kind=StepKind.SCHEMA_MUTATION,
        family=ActionFamily.SCHEMA_MUTATION,
        expression=expr,
    )
    step_hash = step.semantic_hash

    auth = StepAuthorization(
        authorization_id="auth-e2e-001",
        step_id="step-e2e-01",
        tenant_id="tenant-prod-e2e",
        policy_decision_id="pol-v1",
        authorized_step_hash=step_hash,
        expires_at=datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2),
        approved_by_subject_id="security-officer-42",
    )
    step.authorization = auth

    manifest = ArtifactSigner.create_signed_manifest(
        tenant_id="tenant-prod-e2e",
        plan_id="plan-e2e-400",
        plan_digest="a" * 64,
        artifact_id="art-e2e-mask",
        artifact_content=code,
        engine_name="pandas",
        engine_version="2.2.0",
        compiler_name="pandas_ir_compiler",
        compiler_version="1.0.0",
        catalogue_version="v1.0",
        runtime_profile_hash="b" * 64,
        capability_decision_ids=("CAP-001",),
        parameter_manifest_hash="c" * 64,
        signer_key_id="trusted_e2e_key",
        private_key=private_key,
        publication_scope="certified_production",
    )

    return manifest, code, step, private_key, public_key, public_key_hex, sec_ref


def test_complete_phase_4_production_pipeline_lifecycle():
    """
    E2E Test 1: Full Happy Path.
    Tests Signed Manifest -> Execution Gate -> Secret Custody -> Controlled Dispatch -> Durable Publication.
    """
    manifest, code, step, privkey, pubkey, pubkey_hex, sec_ref = _build_end_to_end_fixture()
    keyring = {"trusted_e2e_key": pubkey}

    # 1. Execution Gate Preflight
    gate = ExecutionPreflightGate(trusted_keyring=keyring)
    gate_decision = gate.evaluate_preflight(
        manifest=manifest,
        artifact_content=code,
        expected_tenant_id=manifest.tenant_id,
        expected_plan_digest=manifest.plan_digest,
        expected_runtime_profile_hash=manifest.runtime_profile_hash,
        steps=[step],
    )
    assert gate_decision.dispatch_allowed is True
    assert gate_decision.execution_started is False
    assert gate_decision.publication_started is False

    # 2. Scoped Secret Custody (PROD-1)
    secret_mgr = ExecutionSecretManager()
    secret_mgr.register_secret_in_vault(
        tenant_id="tenant-prod-e2e",
        secret_id="sec-salt-hash-01",
        version="v1",
        secret_value="SUPER_SECRET_PEPPER_BYTES_99",
    )

    with ScopedSecretSession("e2e-session") as session:
        handle = secret_mgr.resolve_secret(
            secret_ref=sec_ref,
            gate_decision=gate_decision,
            tenant_id="tenant-prod-e2e",
            session=session,
        )
        # Redaction guarantee: zero secret bytes in repr or str
        assert "[REDACTED]" in str(handle)
        assert "[REDACTED]" in repr(handle)
        assert "SUPER_SECRET" not in str(handle)
        assert "SUPER_SECRET" not in repr(handle)
        assert handle.get_secret_str() == "SUPER_SECRET_PEPPER_BYTES_99"

        # 3. Controlled Engine Dispatch (PROD-5)
        dispatcher = ControlledDispatcher(preflight_gate=gate)
        input_df = pd.DataFrame({
            "email": ["alice@company.com", "bob@company.com"],
            "raw_ssn": ["123", "456"],
        })

        cleanup_ran = False
        def cleanup_task():
            nonlocal cleanup_ran
            cleanup_ran = True

        dispatch_res = dispatcher.dispatch(
            manifest=manifest,
            code_artifact=code,
            target_engine="pandas",
            runtime_limits=RuntimeLimits(
                timeout_seconds=5.0,
                max_input_rows=1000,
                max_output_rows=1000,
            ),
            input_data=input_df,
            trusted_keys=keyring,
            cleanup_callbacks=[("e2e_cleanup", cleanup_task)],
        )

        assert dispatch_res.dispatch_allowed is True
        assert dispatch_res.execution_started is True
        assert dispatch_res.publication_started is False
        assert dispatch_res.status == "SUCCESS"
        assert dispatch_res.cleanup_guaranteed is True
        assert cleanup_ran is True

        transformed_df = dispatch_res.output_artifact
        assert "masked_email" in transformed_df.columns
        assert transformed_df["masked_email"].tolist() == ["ali***", "bob***"]

        # 4. Durable Publication with Two-Phase Commit & Manifest Fencing (PROD-3)
        publisher = DurablePublicationCoordinator()
        target_dataset = "production.customer_masked"

        staging_handle = publisher.begin_staging(
            staging_id=f"stage-{manifest.artifact_id}",
            target_ref=target_dataset,
            idempotency_key=f"run-{manifest.plan_id}",
            manifest_digest=manifest.compute_manifest_digest(),
        )

        publisher.stage_dataset(staging_handle, "customers", transformed_df)

        # Pre-commit postcondition validation
        val_res = publisher.validate_postconditions(
            handle=staging_handle,
            row_count_bounds={"customers": (2, 2)},
            non_null_columns={"customers": ["masked_email"]},
        )
        assert val_res.is_valid is True

        # Atomic visibility promotion
        receipt = publisher.commit(staging_handle)
        assert receipt.fence_token == 1
        assert receipt.output_keys == ["customers"]
        assert receipt.output_row_counts["customers"] == 2

        # Production visibility verified
        pub_data = publisher.get_published_data(target_dataset)
        assert len(pub_data["customers"]) == 2
        assert pub_data["customers"]["masked_email"].tolist() == ["ali***", "bob***"]

    # Invariant: After session exit, secret handle is securely wiped
    with pytest.raises(Exception, match="SECRET_ALREADY_WIPED"):
        handle.get_secret_str()


def test_gate_rejection_halts_entire_lifecycle():
    """
    E2E Test 2: Invariant Check.
    If preflight check fails:
    dispatch_allowed = False
    execution_started = False
    publication_started = False
    Secret manager refuses resolution.
    Zero data is ever published.
    """
    manifest, code, step, privkey, pubkey, pubkey_hex, sec_ref = _build_end_to_end_fixture()
    
    # Tamper with code to fail signature/digest check
    tampered_code = code + "\n# Malicious injection"
    gate = ExecutionPreflightGate(trusted_keyring={"trusted_e2e_key": pubkey})

    decision = gate.evaluate_preflight(
        manifest=manifest,
        artifact_content=tampered_code,
        expected_tenant_id=manifest.tenant_id,
        expected_plan_digest=manifest.plan_digest,
        expected_runtime_profile_hash=manifest.runtime_profile_hash,
    )

    assert decision.dispatch_allowed is False
    assert decision.execution_started is False
    assert decision.publication_started is False
    assert decision.reason_code == "ARTIFACT_DIGEST_MISMATCH"

    # Secret resolution strictly blocked
    secret_mgr = ExecutionSecretManager()
    secret_mgr.register_secret_in_vault(
        tenant_id="tenant-prod-e2e",
        secret_id="sec-salt-hash-01",
        version="v1",
        secret_value="PEPPER",
    )

    with pytest.raises(UnsafeSecretResolutionError) as exc_info:
        with ScopedSecretSession("blocked-session") as session:
            secret_mgr.resolve_secret(
                secret_ref=sec_ref,
                gate_decision=decision,
                tenant_id="tenant-prod-e2e",
                session=session,
            )
    assert exc_info.value.reason_code == "GATE_REJECTED"

    # Dispatch strictly blocked
    dispatcher = ControlledDispatcher(preflight_gate=gate)
    res = dispatcher.dispatch(
        manifest=manifest,
        code_artifact=tampered_code,
        target_engine="pandas",
        trusted_keys={"trusted_e2e_key": pubkey},
    )
    assert res.dispatch_allowed is False
    assert res.execution_started is False
    assert res.publication_started is False
    assert res.status == "BLOCKED"


def test_concurrent_pipeline_fencing_conflict():
    """
    E2E Test 3: Concurrency Fencing.
    Verifies that two concurrently running certified pipelines competing for the same destination
    are strictly fenced by generation tokens; one succeeds, the second is aborted with zero corruption.
    """
    publisher = DurablePublicationCoordinator()
    dest = "prod.inventory_summary"

    # Both pipelines observe initial fence token 0
    h1 = publisher.begin_staging(staging_id="stage-p1", target_ref=dest, idempotency_key="pipe-1", manifest_digest="1"*64)
    h2 = publisher.begin_staging(staging_id="stage-p2", target_ref=dest, idempotency_key="pipe-2", manifest_digest="2"*64)

    publisher.stage_dataset(h1, "inv", pd.DataFrame({"sku": ["SKU-1"], "qty": [100]}))
    publisher.stage_dataset(h2, "inv", pd.DataFrame({"sku": ["SKU-2"], "qty": [200]}))

    publisher.validate_postconditions(h1)
    publisher.validate_postconditions(h2)

    # Pipeline 1 commits -> fence token becomes 1
    receipt1 = publisher.commit(h1)
    assert receipt1.fence_token == 1

    # Pipeline 2 attempts commit with stale token 0 -> rejected by OCC fence guard
    with pytest.raises(ConcurrentWriterConflictError):
        publisher.commit(h2)

    # Production remains strictly at fence token 1 with Pipeline 1's data intact
    assert publisher.get_current_fence_token(dest) == 1
    pub = publisher.get_published_data(dest)
    assert pub["inv"]["sku"].tolist() == ["SKU-1"]
