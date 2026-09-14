"""
Tests for agent.rules.authorization_gate: Ed25519 asymmetric signatures,
manifest tampering rejection, plan authorization gates, and audit linkage.
"""
import base64
import hashlib
from datetime import datetime, timedelta, timezone
import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519

from agent.rules.authorization_gate import (
    AuditEvent,
    AuthorizedPlan,
    ExecutionGateError,
    GeneratedArtifact,
    GeneratedArtifactManifest,
    PlanAuthorizationRecord,
    PlanAuthorizationStatus,
    VerificationKeyRecord,
    validate_authorized_plan,
    verify_and_execute_artifact,
    verify_manifest_signature,
)
from agent.rules.plan_models import (
    PlanStep,
    PlanStepDependency,
    ProposedPlan,
    ProposedPlanStatus,
    StepAuthorizationStatus,
)
from agent.rules.rule_models import CanonicalRuleRegistry


class MockPolicyStore:
    def is_valid_for_step(self, decision_id: str, step_hash: str, plan_id: str) -> bool:
        return decision_id == "pol-dec-1"


class MockCatalogue:
    def validate_action(self, name: str, version: str, parameters: dict, target_engine: str) -> None:
        if name != "impute_mean":
            raise ValueError(f"Unknown action {name}")


class MockRevocationStore:
    def is_revoked(self, plan_id: str) -> bool:
        return False


class MockAuditStore:
    def __init__(self, event: AuditEvent):
        self.event = event

    def get(self, audit_event_id: str):
        if audit_event_id == self.event.event_id:
            return self.event
        return None


@pytest.fixture
def keypair():
    private_key = ed25519.Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    return private_key, public_key


@pytest.fixture
def plan_fixture():
    step = PlanStep(
        step_id="step-1",
        action="impute_mean",
        action_version="1.0",
        authorization_status=StepAuthorizationStatus.AUTHORIZED,
        policy_decision_id="pol-dec-1",
    )
    prop_plan = ProposedPlan(
        plan_id="plan-123",
        status=ProposedPlanStatus.READY_FOR_AUTHORIZATION,
        steps=(step,),
        dependencies=(),
    )
    registry = CanonicalRuleRegistry()
    auth_record = PlanAuthorizationRecord(
        status=PlanAuthorizationStatus.AUTHORIZED,
        authorized_by="lead_engineer",
        authorized_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=2),
        policy_decision_id="pol-dec-1",
    )
    auth_plan = AuthorizedPlan(
        plan_id="plan-123",
        plan_hash=prop_plan.compute_hash(),
        canonical_registry_hash=registry.compute_hash(),
        policy_version="1.0",
        catalogue_version="1.0",
        authorized_target_engines=frozenset({"pyspark", "duckdb"}),
        proposed_plan=prop_plan,
        authorization=auth_record,
    )
    return auth_plan, registry


def test_ed25519_manifest_signing_and_execution_success(keypair, plan_fixture):
    private_key, public_key = keypair
    plan, registry = plan_fixture

    raw_code = b"print('Transforming customer data')"
    raw_hash = hashlib.sha256(raw_code).hexdigest()

    manifest = GeneratedArtifactManifest(
        artifact_id="art-100",
        tenant_id="tenant-alpha",
        artifact_hash=raw_hash,
        source_plan_id=plan.plan_id,
        source_plan_hash=plan.plan_hash,
        source_registry_hash=plan.canonical_registry_hash,
        policy_decision_id="pol-dec-1",
        policy_version="1.0",
        catalogue_version="1.0",
        target_engine="duckdb",
        execution_environment="production",
        codegen_version="1.0",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        code_generated_audit_event_id="aud-gen-1",
        signing_algorithm="Ed25519",
        signing_key_id="key-1",
        manifest_signature="",
    )

    # Sign manifest payload
    payload_bytes = manifest.compute_signing_payload()
    sig = private_key.sign(payload_bytes)
    manifest.manifest_signature = base64.b64encode(sig).decode("utf-8")

    artifact = GeneratedArtifact(manifest=manifest, artifact_bytes=raw_code)

    key_record = VerificationKeyRecord(
        key_id="key-1",
        tenant_id="tenant-alpha",
        algorithm="Ed25519",
        valid_from=datetime.now(timezone.utc) - timedelta(days=1),
        valid_to=datetime.now(timezone.utc) + timedelta(days=30),
        public_key=public_key,
    )

    audit_event = AuditEvent(
        event_id="aud-gen-1",
        tenant_id="tenant-alpha",
        plan_id=plan.plan_id,
        artifact_id="art-100",
        event_type="CODE_GENERATED",
        created_at=datetime.now(timezone.utc),
    )
    audit_store = MockAuditStore(audit_event)

    validator_kwargs = {
        "current_policy_version": "1.0",
        "current_catalogue_version": "1.0",
        "policy_store": MockPolicyStore(),
        "catalogue": MockCatalogue(),
        "revocation_store": MockRevocationStore(),
    }

    res = verify_and_execute_artifact(
        artifact=artifact,
        plan=plan,
        registry=registry,
        current_tenant_id="tenant-alpha",
        target_engine="duckdb",
        key_record=key_record,
        audit_store=audit_store,
        validator_kwargs=validator_kwargs,
    )
    assert res["status"] == "SUCCESS"


def test_tampered_code_bytes_rejected(keypair, plan_fixture):
    private_key, public_key = keypair
    plan, registry = plan_fixture

    raw_code = b"original_code()"
    manifest = GeneratedArtifactManifest(
        artifact_id="art-101",
        tenant_id="tenant-alpha",
        artifact_hash=hashlib.sha256(raw_code).hexdigest(),
        source_plan_id=plan.plan_id,
        source_plan_hash=plan.plan_hash,
        source_registry_hash=plan.canonical_registry_hash,
        policy_decision_id="pol-dec-1",
        policy_version="1.0",
        catalogue_version="1.0",
        target_engine="duckdb",
        execution_environment="production",
        codegen_version="1.0",
        created_at=datetime.now(timezone.utc),
        code_generated_audit_event_id="aud-1",
        signing_algorithm="Ed25519",
        signing_key_id="key-1",
        manifest_signature=base64.b64encode(private_key.sign(b"dummy")).decode("utf-8"),
    )
    # Tamper with code bytes: different from manifest.artifact_hash
    tampered_artifact = GeneratedArtifact(manifest=manifest, artifact_bytes=b"tampered_code()")

    key_record = VerificationKeyRecord(
        key_id="key-1",
        tenant_id="tenant-alpha",
        algorithm="Ed25519",
        valid_from=datetime.now(timezone.utc) - timedelta(days=1),
        public_key=public_key,
    )
    with pytest.raises(ExecutionGateError, match="Artifact bytes hash does not match"):
        verify_and_execute_artifact(
            artifact=tampered_artifact,
            plan=plan,
            registry=registry,
            current_tenant_id="tenant-alpha",
            target_engine="duckdb",
            key_record=key_record,
            audit_store=MockAuditStore(AuditEvent(event_id="aud-1", tenant_id="tenant-alpha", plan_id=plan.plan_id, artifact_id="art-101", event_type="X", created_at=datetime.now(timezone.utc))),
            validator_kwargs={},
        )


def test_unauthorized_engine_rejected(keypair, plan_fixture):
    private_key, public_key = keypair
    plan, registry = plan_fixture

    # Request execution on snowflake (not in {"pyspark", "duckdb"})
    raw_code = b"code"
    manifest = GeneratedArtifactManifest(
        artifact_id="art-102",
        tenant_id="tenant-alpha",
        artifact_hash=hashlib.sha256(raw_code).hexdigest(),
        source_plan_id=plan.plan_id,
        source_plan_hash=plan.plan_hash,
        source_registry_hash=plan.canonical_registry_hash,
        policy_decision_id="pol-dec-1",
        policy_version="1.0",
        catalogue_version="1.0",
        target_engine="snowflake",
        execution_environment="production",
        codegen_version="1.0",
        created_at=datetime.now(timezone.utc),
        code_generated_audit_event_id="aud-1",
        signing_algorithm="Ed25519",
        signing_key_id="key-1",
        manifest_signature="fake",
    )
    artifact = GeneratedArtifact(manifest=manifest, artifact_bytes=raw_code)
    key_record = VerificationKeyRecord(
        key_id="key-1",
        tenant_id="tenant-alpha",
        algorithm="Ed25519",
        valid_from=datetime.now(timezone.utc) - timedelta(days=1),
        public_key=public_key,
    )

    with pytest.raises(ExecutionGateError, match="is not authorized"):
        verify_and_execute_artifact(
            artifact=artifact,
            plan=plan,
            registry=registry,
            current_tenant_id="tenant-alpha",
            target_engine="snowflake",
            key_record=key_record,
            audit_store=None,
            validator_kwargs={},
        )
