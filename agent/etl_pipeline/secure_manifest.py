"""
agent/etl_pipeline/secure_manifest.py

Phase 4A: Canonical Execution Manifest, SHA-256 Artifact Integrity & Ed25519 Cryptographic Signing.
Binds compiled artifacts, transformation IR plans, tenant identity, runtime profiles, and capability
decisions into an immutable, cryptographically verifiable envelope.
"""
from __future__ import annotations

import base64
import datetime
import hashlib
import json
import re
from typing import Any, Dict, Optional, Tuple, Union

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric import ed25519
from pydantic import BaseModel, Field, field_validator


class ManifestSecurityError(Exception):
    """Raised when an artifact manifest fails integrity, parsing, or signature verification."""

    def __init__(self, message: str, *, reason_code: str = "MANIFEST_INTEGRITY_VIOLATION"):
        super().__init__(f"[{reason_code}] {message}")
        self.reason_code = reason_code


class SignedArtifactManifest(BaseModel):
    """
    Immutable, cryptographically signed execution manifest for Agent Dhara Phase 4.
    """
    tenant_id: str
    plan_id: str
    plan_digest: str
    artifact_id: str
    artifact_sha256: str
    compiler_name: str
    compiler_version: str
    engine_name: str
    engine_version: str
    catalogue_version: str = "v1.0"
    runtime_profile_hash: str
    capability_decision_ids: Tuple[str, ...]
    parameter_manifest_hash: str
    publication_scope: str
    created_at: str
    signer_key_id: str
    signature: str = ""

    @field_validator("publication_scope")
    @classmethod
    def validate_scope(cls, v: str) -> str:
        allowed = {"internal_staged", "certified_production"}
        if v not in allowed:
            raise ValueError(f"publication_scope must be one of {allowed}, got {v!r}")
        return v

    @field_validator("plan_digest", "artifact_sha256", "runtime_profile_hash", "parameter_manifest_hash")
    @classmethod
    def validate_sha256(cls, v: str) -> str:
        if not re.fullmatch(r"[0-9a-fA-F]{64}", v):
            raise ValueError(f"Value must be a 64-character SHA256 hex string, got {v!r}")
        return v.lower()

    def canonical_payload_dict(self) -> Dict[str, Any]:
        """Extracts the exact 16 data fields used for signing (excludes signature)."""
        return {
            "artifact_id": self.artifact_id,
            "artifact_sha256": self.artifact_sha256,
            "capability_decision_ids": list(self.capability_decision_ids),
            "catalogue_version": self.catalogue_version,
            "compiler_name": self.compiler_name,
            "compiler_version": self.compiler_version,
            "created_at": self.created_at,
            "engine_name": self.engine_name,
            "engine_version": self.engine_version,
            "parameter_manifest_hash": self.parameter_manifest_hash,
            "plan_digest": self.plan_digest,
            "plan_id": self.plan_id,
            "publication_scope": self.publication_scope,
            "runtime_profile_hash": self.runtime_profile_hash,
            "signer_key_id": self.signer_key_id,
            "tenant_id": self.tenant_id,
        }

    def canonical_payload_bytes(self) -> bytes:
        """Deterministic byte representation of the canonical manifest payload."""
        data = self.canonical_payload_dict()
        raw = json.dumps(data, sort_keys=True, separators=(",", ":"))
        return raw.encode("utf-8")

    def compute_manifest_digest(self) -> str:
        """SHA-256 digest of the canonical manifest payload."""
        return hashlib.sha256(self.canonical_payload_bytes()).hexdigest()


class ArtifactSigner:
    """
    Cryptographic signing and verification utility using Ed25519.
    """

    @staticmethod
    def generate_keypair() -> Tuple[ed25519.Ed25519PrivateKey, ed25519.Ed25519PublicKey]:
        """Generates a fresh Ed25519 private/public keypair."""
        priv = ed25519.Ed25519PrivateKey.generate()
        return priv, priv.public_key()

    @staticmethod
    def compute_artifact_sha256(content: Union[str, bytes]) -> str:
        """Computes deterministic SHA-256 hex string of code artifact content."""
        b = content.encode("utf-8") if isinstance(content, str) else content
        return hashlib.sha256(b).hexdigest()

    @classmethod
    def create_signed_manifest(
        cls,
        *,
        tenant_id: str,
        plan_id: str,
        plan_digest: str,
        artifact_id: str,
        artifact_content: Union[str, bytes],
        compiler_name: str,
        compiler_version: str,
        engine_name: str,
        engine_version: str,
        runtime_profile_hash: str,
        capability_decision_ids: Tuple[str, ...],
        parameter_manifest_hash: str,
        publication_scope: str,
        signer_key_id: str,
        private_key: ed25519.Ed25519PrivateKey,
        catalogue_version: str = "v1.0",
        created_at: Optional[str] = None,
    ) -> SignedArtifactManifest:
        """
        Creates and signs a new SignedArtifactManifest using an Ed25519 private key.
        """
        artifact_sha256 = cls.compute_artifact_sha256(artifact_content)
        ts = created_at or datetime.datetime.now(datetime.timezone.utc).isoformat()

        unsigned = SignedArtifactManifest(
            tenant_id=tenant_id,
            plan_id=plan_id,
            plan_digest=plan_digest,
            artifact_id=artifact_id,
            artifact_sha256=artifact_sha256,
            compiler_name=compiler_name,
            compiler_version=compiler_version,
            engine_name=engine_name,
            engine_version=engine_version,
            catalogue_version=catalogue_version,
            runtime_profile_hash=runtime_profile_hash,
            capability_decision_ids=capability_decision_ids,
            parameter_manifest_hash=parameter_manifest_hash,
            publication_scope=publication_scope,
            created_at=ts,
            signer_key_id=signer_key_id,
            signature="",
        )

        sig_bytes = private_key.sign(unsigned.canonical_payload_bytes())
        sig_hex = sig_bytes.hex()

        return SignedArtifactManifest(
            tenant_id=unsigned.tenant_id,
            plan_id=unsigned.plan_id,
            plan_digest=unsigned.plan_digest,
            artifact_id=unsigned.artifact_id,
            artifact_sha256=unsigned.artifact_sha256,
            compiler_name=unsigned.compiler_name,
            compiler_version=unsigned.compiler_version,
            engine_name=unsigned.engine_name,
            engine_version=unsigned.engine_version,
            catalogue_version=unsigned.catalogue_version,
            runtime_profile_hash=unsigned.runtime_profile_hash,
            capability_decision_ids=unsigned.capability_decision_ids,
            parameter_manifest_hash=unsigned.parameter_manifest_hash,
            publication_scope=unsigned.publication_scope,
            created_at=unsigned.created_at,
            signer_key_id=unsigned.signer_key_id,
            signature=sig_hex,
        )

    @classmethod
    def verify_manifest(
        cls,
        manifest: SignedArtifactManifest,
        public_key: ed25519.Ed25519PublicKey,
    ) -> bool:
        """
        Verifies the Ed25519 signature on the canonical payload of the manifest.
        Raises ManifestSecurityError if invalid.
        """
        if not manifest.signature:
            raise ManifestSecurityError("Manifest missing signature", reason_code="MISSING_SIGNATURE")

        try:
            sig_bytes = bytes.fromhex(manifest.signature)
        except ValueError as exc:
            raise ManifestSecurityError(f"Invalid signature encoding: {exc}", reason_code="INVALID_SIGNATURE_HEX")

        payload_bytes = manifest.canonical_payload_bytes()
        try:
            public_key.verify(sig_bytes, payload_bytes)
            return True
        except InvalidSignature as exc:
            raise ManifestSecurityError(
                f"Ed25519 signature verification failed for key '{manifest.signer_key_id}'",
                reason_code="SIGNATURE_VERIFICATION_FAILED",
            ) from exc

    @classmethod
    def verify_manifest_and_artifact(
        cls,
        manifest: SignedArtifactManifest,
        artifact_content: Union[str, bytes],
        public_key: ed25519.Ed25519PublicKey,
    ) -> bool:
        """
        Verifies both the cryptographic signature on the manifest AND the SHA-256 digest
        of the supplied executable artifact content.
        """
        cls.verify_manifest(manifest, public_key)

        expected_hash = cls.compute_artifact_sha256(artifact_content)
        if manifest.artifact_sha256 != expected_hash:
            raise ManifestSecurityError(
                f"Artifact digest mismatch: manifest expected {manifest.artifact_sha256}, actual content produced {expected_hash}",
                reason_code="ARTIFACT_DIGEST_MISMATCH",
            )
        return True
