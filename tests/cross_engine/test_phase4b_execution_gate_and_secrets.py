"""
tests/cross_engine/test_phase4b_execution_gate_and_secrets.py

Unit and security tests for Phase 4B:
- Centralized Execution Preflight Gate (10 mandatory checks)
- Authorization Freshness, Semantic Hash & Revocation Revalidation (PROD-2)
- Execution-Boundary Secret Resolution & Zero-Leakage Custody (PROD-1)
"""
import datetime
import pytest

from agent.etl_pipeline.cross_engine.capability_registry import (
    EngineCapabilityRegistry,
    get_global_capability_registry,
)
from agent.etl_pipeline.execution_gate import (
    ExecutionGateDecision,
    ExecutionPreflightGate,
    RevocationStore,
)
from agent.etl_pipeline.secret_manager import (
    ExecutionSecretManager,
    RedactedSecretHandle,
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
    JoinCardinality,
    JoinExpression,
    JoinType,
    SecretReference,
    StepAuthorization,
    StepKind,
    TransformationStepIR,
)


@pytest.fixture
def keypair():
    return ArtifactSigner.generate_keypair()


@pytest.fixture
def sample_artifact():
    return "# PySpark Clean Code\nimport pyspark.sql.functions as F\n"


@pytest.fixture
def valid_manifest(keypair, sample_artifact):
    priv, _ = keypair
    return ArtifactSigner.create_signed_manifest(
        tenant_id="tenant-corp-01",
        plan_id="plan-p4b-001",
        plan_digest="a" * 64,
        artifact_id="art-pyspark-clean",
        artifact_content=sample_artifact,
        compiler_name="pyspark_ir_compiler",
        compiler_version="v1.0",
        engine_name="pyspark",
        engine_version="3.5.1",
        runtime_profile_hash="b" * 64,
        capability_decision_ids=("cap-001",),
        parameter_manifest_hash="c" * 64,
        publication_scope="certified_production",
        signer_key_id="key-trusted-01",
        private_key=priv,
    )


@pytest.fixture
def gate(keypair):
    _, pub = keypair
    keyring = {"key-trusted-01": pub}
    reg = get_global_capability_registry()
    rev_store = RevocationStore()
    return ExecutionPreflightGate(
        trusted_keyring=keyring,
        capability_registry=reg,
        revocation_store=rev_store,
    )


# ============================================================================
# 1. Execution Gate Preflight Verification (10 Checks)
# ============================================================================

def test_execution_gate_all_10_checks_pass(gate, valid_manifest, sample_artifact):
    """Proves that a compliant artifact passes all 10 preflight checks."""
    decision = gate.evaluate_preflight(
        manifest=valid_manifest,
        artifact_content=sample_artifact,
        expected_tenant_id="tenant-corp-01",
        expected_plan_digest="a" * 64,
        expected_runtime_profile_hash="b" * 64,
    )
    assert decision.dispatch_allowed is True
    assert decision.execution_started is False
    assert decision.publication_started is False
    assert decision.reason_code == "PREFLIGHT_GATE_PASSED"


def test_execution_gate_enforces_invariant_failure():
    """Proves invariant: execution or publication cannot be True if dispatch_allowed is False."""
    with pytest.raises(ValueError):
        ExecutionGateDecision(
            dispatch_allowed=False,
            execution_started=True,  # Invariant violation!
            publication_started=False,
            reason_code="INVALID_STATE",
        )

    with pytest.raises(ValueError):
        ExecutionGateDecision(
            dispatch_allowed=False,
            execution_started=False,
            publication_started=True,  # Invariant violation!
            reason_code="INVALID_STATE",
        )


def test_execution_gate_rejects_untrusted_key(gate, sample_artifact):
    """Proves manifest signed with unknown key ID is rejected."""
    other_priv, _ = ArtifactSigner.generate_keypair()
    untrusted_manifest = ArtifactSigner.create_signed_manifest(
        tenant_id="tenant-corp-01",
        plan_id="plan-p4b-001",
        plan_digest="a" * 64,
        artifact_id="art-pyspark-clean",
        artifact_content=sample_artifact,
        compiler_name="pyspark_ir_compiler",
        compiler_version="v1.0",
        engine_name="pyspark",
        engine_version="3.5.1",
        runtime_profile_hash="b" * 64,
        capability_decision_ids=("cap-001",),
        parameter_manifest_hash="c" * 64,
        publication_scope="certified_production",
        signer_key_id="key-rogue-unknown",
        private_key=other_priv,
    )
    dec = gate.evaluate_preflight(
        manifest=untrusted_manifest,
        artifact_content=sample_artifact,
        expected_tenant_id="tenant-corp-01",
        expected_plan_digest="a" * 64,
        expected_runtime_profile_hash="b" * 64,
    )
    assert dec.dispatch_allowed is False
    assert dec.reason_code == "UNTRUSTED_SIGNER_KEY"


def test_execution_gate_rejects_revoked_key(gate, valid_manifest, sample_artifact):
    """Proves manifest signed with a revoked key is rejected."""
    gate.revocation_store.revoke_signing_key("key-trusted-01")
    dec = gate.evaluate_preflight(
        manifest=valid_manifest,
        artifact_content=sample_artifact,
        expected_tenant_id="tenant-corp-01",
        expected_plan_digest="a" * 64,
        expected_runtime_profile_hash="b" * 64,
    )
    assert dec.dispatch_allowed is False
    assert dec.reason_code == "REVOKED_SIGNER_KEY"


def test_execution_gate_rejects_tampered_artifact(gate, valid_manifest, sample_artifact):
    """Proves modified artifact content fails digest check."""
    dec = gate.evaluate_preflight(
        manifest=valid_manifest,
        artifact_content=sample_artifact + "# tampered code",
        expected_tenant_id="tenant-corp-01",
        expected_plan_digest="a" * 64,
        expected_runtime_profile_hash="b" * 64,
    )
    assert dec.dispatch_allowed is False
    assert dec.reason_code == "ARTIFACT_DIGEST_MISMATCH"


def test_execution_gate_rejects_tenant_and_plan_mismatches(gate, valid_manifest, sample_artifact):
    """Proves tenant and plan boundary mismatches are rejected."""
    # Tenant mismatch
    dec_t = gate.evaluate_preflight(
        manifest=valid_manifest,
        artifact_content=sample_artifact,
        expected_tenant_id="tenant-other-corp",
        expected_plan_digest="a" * 64,
        expected_runtime_profile_hash="b" * 64,
    )
    assert dec_t.dispatch_allowed is False
    assert dec_t.reason_code == "TENANT_MISMATCH"

    # Plan digest mismatch
    dec_p = gate.evaluate_preflight(
        manifest=valid_manifest,
        artifact_content=sample_artifact,
        expected_tenant_id="tenant-corp-01",
        expected_plan_digest="f" * 64,
        expected_runtime_profile_hash="b" * 64,
    )
    assert dec_p.dispatch_allowed is False
    assert dec_p.reason_code == "PLAN_DIGEST_MISMATCH"


# ============================================================================
# 2. Authorization Freshness & Revocation Revalidation (PROD-2)
# ============================================================================

def test_execution_gate_authorization_freshness_revalidation(gate, valid_manifest, sample_artifact):
    """Proves PROD-2 authorization revalidation: expiry, hash mismatch, and revocation."""
    now = datetime.datetime.now(datetime.timezone.utc)
    expr_cj = JoinExpression(
        left_dataset="primary",
        right_dataset="secondary",
        join_type=JoinType.CROSS,
        cardinality=JoinCardinality.MANY_TO_MANY,
        output_dataset="joined",
    )
    step_probe = TransformationStepIR(
        step_id="step_cross",
        dataset_name="primary",
        kind=StepKind.MULTI_DATASET_JOIN,
        family=ActionFamily.JOIN,
        expression=expr_cj,
    )

    # 1. Valid Fresh Authorization -> Passes
    valid_auth = StepAuthorization(
        step_id="step_cross",
        policy_decision_id="pol-001",
        approved_by_subject_id="Architect",
        tenant_id="tenant-corp-01",
        expires_at=now + datetime.timedelta(hours=1),
        authorized_step_hash=step_probe.semantic_hash,
    )
    step_valid = TransformationStepIR(
        step_id="step_cross",
        dataset_name="primary",
        kind=StepKind.MULTI_DATASET_JOIN,
        family=ActionFamily.JOIN,
        expression=expr_cj,
        authorization=valid_auth,
    )
    dec_ok = gate.evaluate_preflight(
        manifest=valid_manifest,
        artifact_content=sample_artifact,
        expected_tenant_id="tenant-corp-01",
        expected_plan_digest="a" * 64,
        expected_runtime_profile_hash="b" * 64,
        steps=[step_valid],
        now_utc=now,
    )
    assert dec_ok.dispatch_allowed is True

    # 2. Expired Authorization -> Fails
    expired_auth = StepAuthorization(
        step_id="step_cross",
        policy_decision_id="pol-001",
        approved_by_subject_id="Architect",
        tenant_id="tenant-corp-01",
        expires_at=now - datetime.timedelta(minutes=1),  # Expired!
        authorized_step_hash=step_probe.semantic_hash,
    )
    step_exp = TransformationStepIR(
        step_id="step_cross",
        dataset_name="primary",
        kind=StepKind.MULTI_DATASET_JOIN,
        family=ActionFamily.JOIN,
        expression=expr_cj,
        authorization=expired_auth,
    )
    dec_exp = gate.evaluate_preflight(
        manifest=valid_manifest,
        artifact_content=sample_artifact,
        expected_tenant_id="tenant-corp-01",
        expected_plan_digest="a" * 64,
        expected_runtime_profile_hash="b" * 64,
        steps=[step_exp],
        now_utc=now,
    )
    assert dec_exp.dispatch_allowed is False
    assert dec_exp.reason_code == "AUTHORIZATION_EXPIRED"

    # 3. Step Hash Tampered -> Fails
    tampered_auth = StepAuthorization(
        step_id="step_cross",
        policy_decision_id="pol-001",
        approved_by_subject_id="Architect",
        tenant_id="tenant-corp-01",
        expires_at=now + datetime.timedelta(hours=1),
        authorized_step_hash="0" * 64,  # Mismatched step hash!
    )
    step_tamp = TransformationStepIR(
        step_id="step_cross",
        dataset_name="primary",
        kind=StepKind.MULTI_DATASET_JOIN,
        family=ActionFamily.JOIN,
        expression=expr_cj,
        authorization=tampered_auth,
    )
    dec_tamp = gate.evaluate_preflight(
        manifest=valid_manifest,
        artifact_content=sample_artifact,
        expected_tenant_id="tenant-corp-01",
        expected_plan_digest="a" * 64,
        expected_runtime_profile_hash="b" * 64,
        steps=[step_tamp],
        now_utc=now,
    )
    assert dec_tamp.dispatch_allowed is False
    assert dec_tamp.reason_code == "STEP_HASH_TAMPERED"

    # 4. Revoked Authorization -> Fails
    gate.revocation_store.revoke_authorization("pol-001")
    dec_rev = gate.evaluate_preflight(
        manifest=valid_manifest,
        artifact_content=sample_artifact,
        expected_tenant_id="tenant-corp-01",
        expected_plan_digest="a" * 64,
        expected_runtime_profile_hash="b" * 64,
        steps=[step_valid],
        now_utc=now,
    )
    assert dec_rev.dispatch_allowed is False
    assert dec_rev.reason_code == "AUTHORIZATION_REVOKED"


# ============================================================================
# 3. Secret Resolution & Key Custody (PROD-1)
# ============================================================================

def test_secret_resolution_when_gate_approved_and_finally_disposal():
    """Proves PROD-1 secret custody: gated resolution, zero leakage, and finally wipe."""
    mgr = ExecutionSecretManager()
    mgr.register_secret_in_vault(
        tenant_id="tenant-corp-01",
        secret_id="hmac_key_customer",
        version="v1",
        secret_value="super_secret_pepper_value_999",
    )

    approved_decision = ExecutionGateDecision(
        dispatch_allowed=True,
        reason_code="PREFLIGHT_GATE_PASSED",
        tenant_id="tenant-corp-01",
    )

    s_ref = SecretReference(secret_id="hmac_key_customer", expected_version="v1", expected_purpose="hmac_key")

    # 1. Resolve within ScopedSecretSession
    with ScopedSecretSession("test_run_01") as session:
        handle = mgr.resolve_secret(
            secret_ref=s_ref,
            gate_decision=approved_decision,
            tenant_id="tenant-corp-01",
            session=session,
        )
        assert handle.secret_id == "hmac_key_customer"
        assert handle.get_secret_str() == "super_secret_pepper_value_999"

        # Zero leakage in string representations
        assert "super_secret" not in str(handle)
        assert "super_secret" not in repr(handle)
        assert "[REDACTED]" in str(handle)
        assert "[REDACTED]" in repr(handle)

    # 2. After session exit (finally clause), secret bytes MUST be wiped
    with pytest.raises(UnsafeSecretResolutionError) as exc:
        handle.get_secret_str()
    assert exc.value.reason_code == "SECRET_ALREADY_WIPED"


def test_secret_resolution_rejected_when_gate_disallowed():
    """Proves secrets CANNOT be resolved when the execution gate rejects dispatch."""
    mgr = ExecutionSecretManager()
    mgr.register_secret_in_vault(
        tenant_id="tenant-corp-01",
        secret_id="hmac_key_customer",
        version="v1",
        secret_value="secret_val",
    )

    rejected_decision = ExecutionGateDecision(
        dispatch_allowed=False,
        reason_code="SIGNATURE_VERIFICATION_FAILED",
        tenant_id="tenant-corp-01",
    )

    s_ref = SecretReference(secret_id="hmac_key_customer", expected_version="v1")

    with ScopedSecretSession("test_rejected") as session:
        with pytest.raises(UnsafeSecretResolutionError) as exc:
            mgr.resolve_secret(
                secret_ref=s_ref,
                gate_decision=rejected_decision,
                tenant_id="tenant-corp-01",
                session=session,
            )
        assert exc.value.reason_code == "GATE_REJECTED"
