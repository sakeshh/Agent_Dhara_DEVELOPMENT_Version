"""
tests/cross_engine/test_phase4a_manifest_signing.py

Unit and negative security tests for Phase 4A:
- Canonical Execution Manifest
- SHA-256 Artifact Integrity
- Ed25519 Cryptographic Signing & Tamper Detection
"""
import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519

from agent.etl_pipeline.secure_manifest import (
    ArtifactSigner,
    ManifestSecurityError,
    SignedArtifactManifest,
)


@pytest.fixture
def keypair():
    return ArtifactSigner.generate_keypair()


@pytest.fixture
def sample_artifact():
    return (
        "# Dhara Compiled PySpark Artifact\n"
        "import pyspark.sql.functions as F\n"
        "df = df.withColumn('email_clean', F.trim(F.lower(F.col('email'))))\n"
    )


@pytest.fixture
def signed_manifest(keypair, sample_artifact):
    priv, _ = keypair
    return ArtifactSigner.create_signed_manifest(
        tenant_id="tenant-acme-corp",
        plan_id="plan-etl-daily-001",
        plan_digest="a" * 64,
        artifact_id="art-pyspark-daily-001",
        artifact_content=sample_artifact,
        compiler_name="pyspark_ir_compiler",
        compiler_version="v1.0",
        engine_name="pyspark",
        engine_version="3.5.1",
        runtime_profile_hash="b" * 64,
        capability_decision_ids=("cap-001", "cap-002"),
        parameter_manifest_hash="c" * 64,
        publication_scope="certified_production",
        signer_key_id="key-prod-ed25519-v1",
        private_key=priv,
    )


def test_manifest_creation_and_signing(signed_manifest, keypair):
    """Proves manifest is signed with valid Ed25519 signature and produces deterministic digest."""
    _, pub = keypair
    assert signed_manifest.tenant_id == "tenant-acme-corp"
    assert len(signed_manifest.signature) == 128  # 64 bytes in hex
    assert len(signed_manifest.compute_manifest_digest()) == 64

    # Verification passes
    assert ArtifactSigner.verify_manifest(signed_manifest, pub) is True


def test_verify_manifest_and_artifact_content_match(signed_manifest, sample_artifact, keypair):
    """Proves manifest verification succeeds when artifact content matches exact sha256."""
    _, pub = keypair
    assert ArtifactSigner.verify_manifest_and_artifact(signed_manifest, sample_artifact, pub) is True


def test_tampered_artifact_content_detected(signed_manifest, sample_artifact, keypair):
    """Proves tampering with artifact code content is detected via SHA-256 digest check."""
    _, pub = keypair
    tampered_code = sample_artifact + "\n# malicious injection"
    with pytest.raises(ManifestSecurityError) as exc:
        ArtifactSigner.verify_manifest_and_artifact(signed_manifest, tampered_code, pub)
    assert exc.value.reason_code == "ARTIFACT_DIGEST_MISMATCH"


def test_tampered_manifest_field_detected(signed_manifest, keypair):
    """Proves tampering with any manifest metadata field breaks the cryptographic signature."""
    _, pub = keypair
    # Tamper with tenant_id
    tampered = SignedArtifactManifest(
        **{**signed_manifest.model_dump(), "tenant_id": "tenant-attacker"}
    )
    with pytest.raises(ManifestSecurityError) as exc:
        ArtifactSigner.verify_manifest(tampered, pub)
    assert exc.value.reason_code == "SIGNATURE_VERIFICATION_FAILED"

    # Tamper with engine_version
    tampered_engine = SignedArtifactManifest(
        **{**signed_manifest.model_dump(), "engine_version": "9.9.9"}
    )
    with pytest.raises(ManifestSecurityError) as exc2:
        ArtifactSigner.verify_manifest(tampered_engine, pub)
    assert exc2.value.reason_code == "SIGNATURE_VERIFICATION_FAILED"


def test_wrong_public_key_rejected(signed_manifest):
    """Proves verification fails when attempted with an untrusted / different public key."""
    _, other_pub = ArtifactSigner.generate_keypair()
    with pytest.raises(ManifestSecurityError) as exc:
        ArtifactSigner.verify_manifest(signed_manifest, other_pub)
    assert exc.value.reason_code == "SIGNATURE_VERIFICATION_FAILED"


def test_invalid_signature_hex_or_empty_rejected(signed_manifest, keypair):
    """Proves empty or corrupted signature hex raises security error."""
    _, pub = keypair
    empty_sig = SignedArtifactManifest(
        **{**signed_manifest.model_dump(), "signature": ""}
    )
    with pytest.raises(ManifestSecurityError) as exc:
        ArtifactSigner.verify_manifest(empty_sig, pub)
    assert exc.value.reason_code == "MISSING_SIGNATURE"

    corrupted_sig = SignedArtifactManifest(
        **{**signed_manifest.model_dump(), "signature": "not_a_hex_string"}
    )
    with pytest.raises(ManifestSecurityError) as exc2:
        ArtifactSigner.verify_manifest(corrupted_sig, pub)
    assert exc2.value.reason_code == "INVALID_SIGNATURE_HEX"


def test_unauthorized_scope_rejected_at_validation(keypair, sample_artifact):
    """Proves publication scope validation strictly enforces allowed scopes."""
    priv, _ = keypair
    with pytest.raises(ValueError):
        ArtifactSigner.create_signed_manifest(
            tenant_id="tenant-1",
            plan_id="plan-1",
            plan_digest="a" * 64,
            artifact_id="art-1",
            artifact_content=sample_artifact,
            compiler_name="pyspark_ir_compiler",
            compiler_version="v1.0",
            engine_name="pyspark",
            engine_version="3.5.1",
            runtime_profile_hash="b" * 64,
            capability_decision_ids=("cap-1",),
            parameter_manifest_hash="c" * 64,
            publication_scope="public_unregulated",  # Disallowed
            signer_key_id="key-1",
            private_key=priv,
        )
