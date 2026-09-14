"""
Cryptographic Authorization Gate, Asymmetric Manifest Signing, and Execution Boundary.

Implements the frozen v9.3 specification with Errata E3:
- Full 9-step plan authorization validator shared across codegen and execution boundaries.
- Asymmetric manifest signing and verification supporting Ed25519, ES256 (P-256), and PS256 (>=2048 bits).
- VerificationKeyRecord with tenant scoping, validity windows, and revocation tracking.
- Strict binding of artifact manifest to authorized plan, active registry, policy decision, and audit events.
- Raw artifact bytes SHA-256 hashing.
"""
from __future__ import annotations

import base64
import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, FrozenSet, List, Literal, Mapping, Optional, Tuple

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, padding, rsa
from pydantic import BaseModel, Field, field_validator, model_validator

from agent.rules.canonical import (
    CANONICALIZATION_SCHEME,
    CANONICALIZATION_VERSION,
    canonical_json,
    compute_canonical_hash,
)
from agent.rules.conflict_resolver import build_dependency_graph, topological_sort_steps
from agent.rules.plan_models import (
    PlanStep,
    PlanStepDependency,
    PlannerInvariantError,
    ProposedPlan,
    ProposedPlanStatus,
    StepAuthorizationStatus,
)
from agent.rules.rule_models import CanonicalRuleRegistry


class ExecutionGateError(RuntimeError):
    """Raised when an authorization, integrity, or cryptographic gate fails."""
    pass


# ==============================================================================
# 1. Authorization Models
# ==============================================================================

class PlanAuthorizationStatus(str, Enum):
    AUTHORIZED = "authorized"
    PENDING = "pending"
    EXPIRED = "expired"
    REVOKED = "revoked"
    REJECTED = "rejected"


class PlanAuthorizationRecord(BaseModel):
    """Governance authorization record granting execution permission for a plan."""
    status: PlanAuthorizationStatus
    authorized_by: str
    authorized_at: datetime
    expires_at: datetime
    policy_decision_id: str

    @field_validator("authorized_at", "expires_at")
    @classmethod
    def require_tz(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("Authorization timestamps must be timezone-aware")
        return v.astimezone(timezone.utc)

    def is_expired(self, current_time: Optional[datetime] = None) -> bool:
        now = current_time or datetime.now(timezone.utc)
        return now > self.expires_at


class AuthorizedPlan(BaseModel):
    """Immutable plan container binding a ProposedPlan to formal authorization."""
    plan_id: str
    plan_hash: str
    canonical_registry_hash: str
    policy_version: str
    catalogue_version: str
    authorized_target_engines: FrozenSet[str]
    proposed_plan: ProposedPlan
    authorization: PlanAuthorizationRecord

    @field_validator("authorized_target_engines", mode="before")
    @classmethod
    def ensure_frozenset(cls, v: Any) -> FrozenSet[str]:
        if isinstance(v, (list, set, tuple)):
            return frozenset(str(x) for x in v)
        if isinstance(v, frozenset):
            return v
        return frozenset()


# ==============================================================================
# 2. Key Management and Manifest Models
# ==============================================================================

class VerificationKeyRecord(BaseModel):
    """Cryptographic verification key record scoped to tenant, algorithm, and validity window."""
    key_id: str
    tenant_id: str
    algorithm: Literal["Ed25519", "ES256", "PS256"]
    valid_from: datetime
    valid_to: Optional[datetime] = None
    revoked_at: Optional[datetime] = None
    usage: Literal["artifact_verification"] = "artifact_verification"
    public_key: Any

    @field_validator("valid_from", "valid_to", "revoked_at")
    @classmethod
    def require_tz(cls, v: Optional[datetime]) -> Optional[datetime]:
        if v is None:
            return None
        if v.tzinfo is None:
            raise ValueError("Key record timestamps must be timezone-aware")
        return v.astimezone(timezone.utc)

    def is_active_at(self, dt: datetime) -> bool:
        if self.revoked_at is not None and dt >= self.revoked_at:
            return False
        if dt < self.valid_from:
            return False
        if self.valid_to is not None and dt > self.valid_to:
            return False
        return True


class GeneratedArtifactManifest(BaseModel):
    """Cryptographically signed metadata manifest accompanying an executable artifact."""
    manifest_version: str = "1.0"
    artifact_id: str
    tenant_id: str

    artifact_hash: str                 # SHA-256 hex digest of raw stored artifact bytes
    source_plan_id: str
    source_plan_hash: str
    source_registry_hash: str

    policy_decision_id: str
    policy_version: str
    catalogue_version: str

    target_engine: str
    execution_environment: str
    codegen_version: str

    created_at: datetime
    expires_at: Optional[datetime] = None
    code_generated_audit_event_id: str

    signing_algorithm: Literal["Ed25519", "ES256", "PS256"]
    signing_key_id: str
    manifest_signature: str

    @field_validator("created_at", "expires_at")
    @classmethod
    def require_tz(cls, v: Optional[datetime]) -> Optional[datetime]:
        if v is None:
            return None
        if v.tzinfo is None:
            raise ValueError("Manifest timestamps must be timezone-aware")
        return v.astimezone(timezone.utc)

    def compute_signing_payload(self) -> bytes:
        """Excludes signature field; signs canonical JSON structure (RFC-clean)."""
        payload_dict = self.model_dump(mode="json", exclude={"manifest_signature"})
        return canonical_json(payload_dict).encode("utf-8")


class GeneratedArtifact(BaseModel):
    """Complete executable artifact bundle containing signed manifest and raw bytes."""
    manifest: GeneratedArtifactManifest
    artifact_bytes: bytes


class AuditEvent(BaseModel):
    """Audit log entry for verifying plan and artifact linkage."""
    event_id: str
    tenant_id: str
    plan_id: str
    artifact_id: str
    event_type: str
    created_at: datetime


# ==============================================================================
# 3. Validation and Execution Gates
# ==============================================================================

def validate_authorized_plan(
    plan: AuthorizedPlan,
    registry: CanonicalRuleRegistry,
    current_policy_version: str,
    current_catalogue_version: str,
    target_engine: str,
    policy_store: Any,  # PolicyAuthorizationStore
    catalogue: Any,     # ActionCatalogue
    revocation_store: Any,  # AuthorizationRevocationStore
) -> None:
    """Executes all 9 critical authorization and dependency integrity validations."""
    # 1. Proposed Plan Status Gate
    if plan.proposed_plan.status != ProposedPlanStatus.READY_FOR_AUTHORIZATION:
        raise ExecutionGateError(
            f"AuthorizedPlan contains invalid proposed plan status: '{plan.proposed_plan.status.value}'. "
            f"Expected '{ProposedPlanStatus.READY_FOR_AUTHORIZATION.value}'"
        )

    # 2. Engine Binding Gate
    if target_engine not in plan.authorized_target_engines:
        raise ExecutionGateError(
            f"Target engine '{target_engine}' is not permitted by authorization. "
            f"Allowed engines: {sorted(plan.authorized_target_engines)}"
        )

    # 3. Plan-Level Authorization & Revocation
    if plan.authorization.status != PlanAuthorizationStatus.AUTHORIZED:
        raise ExecutionGateError(f"Plan is not authorized: {plan.authorization.status.value}")
    if plan.authorization.is_expired():
        raise ExecutionGateError(f"Plan authorization expired at {plan.authorization.expires_at}")
    if revocation_store.is_revoked(plan.plan_id):
        raise ExecutionGateError(f"Plan authorization {plan.plan_id} has been revoked")

    # 4. Version Locks
    if plan.policy_version != current_policy_version:
        raise ExecutionGateError(f"Policy version mismatch: plan={plan.policy_version}, current={current_policy_version}")
    if plan.catalogue_version != current_catalogue_version:
        raise ExecutionGateError(f"Catalogue version mismatch: plan={plan.catalogue_version}, current={current_catalogue_version}")

    # 5. Registry & Plan Hash Verification
    if plan.canonical_registry_hash != registry.compute_hash():
        raise ExecutionGateError("Canonical registry hash mismatch against active registry")
    if plan.proposed_plan.compute_hash() != plan.plan_hash:
        raise ExecutionGateError("Proposed plan content hash does not match authorized plan_hash")

    # 6. Step Existence and Dependency Graph Integrity
    step_ids = {step.step_id for step in plan.proposed_plan.steps}
    for dep in plan.proposed_plan.dependencies:
        if dep.before_step_id not in step_ids:
            raise ExecutionGateError(f"Dependency references unknown before_step_id: {dep.before_step_id}")
        if dep.after_step_id not in step_ids:
            raise ExecutionGateError(f"Dependency references unknown after_step_id: {dep.after_step_id}")
        if dep.before_step_id == dep.after_step_id:
            raise ExecutionGateError(f"Self-dependency detected on step: {dep.before_step_id}")

    # 7. Step-Level Authorization and Policy Validation
    for step in plan.proposed_plan.steps:
        if step.authorization_status != StepAuthorizationStatus.AUTHORIZED:
            raise ExecutionGateError(f"Step {step.step_id} is not independently authorized: {step.authorization_status.value}")
        if not step.policy_decision_id:
            raise ExecutionGateError(f"Step {step.step_id} missing policy_decision_id")
        if not policy_store.is_valid_for_step(
            decision_id=step.policy_decision_id,
            step_hash=step.compute_hash(),
            plan_id=plan.plan_id,
        ):
            raise ExecutionGateError(f"Invalid or revoked policy decision for step {step.step_id}")

        catalogue.validate_action(
            name=step.action,
            version=step.action_version,
            parameters=step.parameters,
            target_engine=target_engine,
        )

    # 8. Global DAG Acyclicity
    topological_sort_steps(plan.proposed_plan.steps, plan.proposed_plan.dependencies)


def verify_manifest_signature(
    manifest: GeneratedArtifactManifest,
    key_record: VerificationKeyRecord,
    payload_bytes: bytes,
) -> None:
    """Dispatches algorithm-specific cryptographic signature verification with parameter validation."""
    # 1. Tenant match
    if key_record.tenant_id != manifest.tenant_id:
        raise ExecutionGateError(f"Verification key tenant mismatch: {key_record.tenant_id} vs {manifest.tenant_id}")

    # 2. Algorithm match
    if key_record.algorithm != manifest.signing_algorithm:
        raise ExecutionGateError(f"Key algorithm mismatch: key={key_record.algorithm}, manifest={manifest.signing_algorithm}")

    # 3. Validity window check
    now = datetime.now(timezone.utc)
    if not key_record.is_active_at(now):
        raise ExecutionGateError(f"Verification key {key_record.key_id} is expired or revoked")

    # 4. Strict Base64 signature decode
    try:
        sig_bytes = base64.b64decode(manifest.manifest_signature, validate=True)
    except ValueError as exc:
        raise ExecutionGateError("Manifest signature is not valid Base64") from exc

    # 5. Algorithm parameter validation and verification
    pub_key = key_record.public_key
    try:
        if manifest.signing_algorithm == "Ed25519":
            if not isinstance(pub_key, ed25519.Ed25519PublicKey):
                raise ExecutionGateError("Key type mismatch: expected Ed25519PublicKey")
            pub_key.verify(sig_bytes, payload_bytes)

        elif manifest.signing_algorithm == "ES256":
            if not isinstance(pub_key, ec.EllipticCurvePublicKey):
                raise ExecutionGateError("Key type mismatch: expected EllipticCurvePublicKey")
            if not isinstance(pub_key.curve, ec.SECP256R1):
                raise ExecutionGateError(f"ES256 requires SECP256R1 (P-256) curve, got {pub_key.curve.name}")
            pub_key.verify(sig_bytes, payload_bytes, ec.ECDSA(hashes.SHA256()))

        elif manifest.signing_algorithm == "PS256":
            if not isinstance(pub_key, rsa.RSAPublicKey):
                raise ExecutionGateError("Key type mismatch: expected RSAPublicKey")
            if pub_key.key_size < 2048:
                raise ExecutionGateError(f"PS256 requires RSA key >= 2048 bits, got {pub_key.key_size}")
            pub_key.verify(
                sig_bytes,
                payload_bytes,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=hashes.SHA256().digest_size,
                ),
                hashes.SHA256(),
            )
    except InvalidSignature as exc:
        raise ExecutionGateError("Manifest cryptographic signature verification failed") from exc


def verify_and_execute_artifact(
    artifact: GeneratedArtifact,
    plan: AuthorizedPlan,
    registry: CanonicalRuleRegistry,
    current_tenant_id: str,
    target_engine: str,
    key_record: VerificationKeyRecord,
    audit_store: Any,
    validator_kwargs: Dict[str, Any],
) -> Any:
    """
    Complete 10-step verification boundary before artifact execution:
    1. Tenant match
    2. Expiration check
    3. Target engine match
    4. Plan and registry linkage
    5. Policy decision linkage
    6. Audit event existence and linkage
    7. Raw artifact bytes hash verification
    8. Asymmetric signature verification
    9. AuthorizedPlan re-validation
    10. Execution
    """
    manifest = artifact.manifest

    # 1. Tenant binding
    if manifest.tenant_id != current_tenant_id:
        raise ExecutionGateError(f"Artifact tenant mismatch: {manifest.tenant_id} vs {current_tenant_id}")

    # 2. Expiration check
    if manifest.expires_at and datetime.now(timezone.utc) > manifest.expires_at:
        raise ExecutionGateError(f"Artifact expired at {manifest.expires_at}")

    # 3. Engine binding
    if manifest.target_engine != target_engine:
        raise ExecutionGateError(f"Artifact compiled for engine '{manifest.target_engine}', requested '{target_engine}'")
    if manifest.target_engine not in plan.authorized_target_engines:
        raise ExecutionGateError(f"Artifact target engine '{manifest.target_engine}' is not authorized")

    # 4. Plan & Registry Linkage
    if manifest.source_plan_id != plan.plan_id:
        raise ExecutionGateError(f"Artifact belongs to plan {manifest.source_plan_id}, executing {plan.plan_id}")
    if manifest.source_plan_hash != plan.plan_hash:
        raise ExecutionGateError("Artifact source_plan_hash mismatch against authorized plan")
    if manifest.source_registry_hash != plan.canonical_registry_hash:
        raise ExecutionGateError("Artifact source_registry_hash mismatch against authorized plan")
    if manifest.source_registry_hash != registry.compute_hash():
        raise ExecutionGateError("Artifact registry hash mismatch against active canonical registry")
    if manifest.policy_version != plan.policy_version:
        raise ExecutionGateError("Artifact policy version mismatch against authorized plan")
    if manifest.catalogue_version != plan.catalogue_version:
        raise ExecutionGateError("Artifact catalogue version mismatch against authorized plan")

    # 5. Policy Decision Linkage
    if manifest.policy_decision_id != plan.authorization.policy_decision_id:
        raise ExecutionGateError("Artifact policy_decision_id does not match plan authorization")

    # 6. Audit Event Verification
    audit_event = audit_store.get(manifest.code_generated_audit_event_id)
    if audit_event is None:
        raise ExecutionGateError("Code-generation audit event does not exist in audit store")
    if audit_event.plan_id != manifest.source_plan_id:
        raise ExecutionGateError("Code-generation audit event references a different plan")
    if audit_event.artifact_id != manifest.artifact_id:
        raise ExecutionGateError("Code-generation audit event references a different artifact")
    if audit_event.tenant_id != manifest.tenant_id:
        raise ExecutionGateError("Code-generation audit event tenant mismatch")

    # 7. Raw Artifact Bytes Hash Verification
    actual_code_hash = hashlib.sha256(artifact.artifact_bytes).hexdigest()
    if actual_code_hash != manifest.artifact_hash:
        raise ExecutionGateError("Artifact bytes hash does not match manifest artifact_hash")

    # 8. Asymmetric Cryptographic Signature Verification
    payload_bytes = manifest.compute_signing_payload()
    verify_manifest_signature(
        manifest=manifest,
        key_record=key_record,
        payload_bytes=payload_bytes,
    )

    # 9. Re-run Authorized Plan Validation
    validate_authorized_plan(
        plan=plan,
        registry=registry,
        target_engine=target_engine,
        **validator_kwargs,
    )

    # 10. Execution callback (return success for testing or engine invoke)
    return {"status": "SUCCESS", "executed_artifact_id": manifest.artifact_id, "engine": target_engine}
