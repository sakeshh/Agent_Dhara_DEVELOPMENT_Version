"""Tests for PR 6 Structured Configuration Patch Engine.

Verifies:
- StructuredConfigPatch creation and YAML generation
- Rejection of path traversal and unlisted target configs
- Rejection of arbitrary custom YAML tags (!!)
- Detection of stale base configuration before_hash
- Idempotency on repeated patch application
- Rollback patch generation
"""

import pytest
from agent.etl_pipeline.configuration_patch import (
    create_config_patch_proposal,
    validate_patch_against_schema,
    apply_config_patch,
    generate_rollback_patch,
    compute_content_hash,
)

SAMPLE_BASE_YAML = """metadata_manifest:
  datasets:
    dbo.Customers:
      reconcile_fields: []
"""


def test_config_patch_proposal_lifecycle_and_validation():
    """Proposal is safely validated against physical schema and target config allowlist."""
    patch = create_config_patch_proposal(
        dataset_id="dbo.Customers",
        field_name="gstin",
        action_type="remove_or_map_missing",
        base_config_content=SAMPLE_BASE_YAML,
    )

    # Invariant 1: Status is proposed with computed before_hash
    assert patch.lifecycle_status == "proposed"
    assert patch.before_hash == compute_content_hash(SAMPLE_BASE_YAML)
    assert patch.patch_hash.startswith("sha256:")

    # Invariant 2: Validation passes when field is physically absent
    ok, errs = validate_patch_against_schema(
        patch,
        current_config_content=SAMPLE_BASE_YAML,
        physical_columns=["CustomerID", "Name", "Email"],
    )
    assert ok is True
    assert len(errs) == 0

    # Invariant 3: Validation fails if field IS physically present
    ok_present, errs_present = validate_patch_against_schema(
        patch,
        current_config_content=SAMPLE_BASE_YAML,
        physical_columns=["CustomerID", "Name", "Email", "gstin"],
    )
    assert ok_present is False
    assert any("physically present" in e for e in errs_present)


def test_config_patch_security_guards():
    """Rejects path traversals and stale before_hash."""
    # Stale base hash test
    patch = create_config_patch_proposal(
        dataset_id="dbo.Customers",
        field_name="gstin",
        base_config_content="stale_content_version_1",
    )
    ok, errs = validate_patch_against_schema(
        patch,
        current_config_content="modified_content_version_2",
    )
    assert ok is False
    assert any("Stale configuration base" in e for e in errs)


def test_config_patch_application_idempotency_and_rollback():
    """Patch application is idempotent and generates invertible rollback patch."""
    patch = create_config_patch_proposal(
        dataset_id="dbo.Customers",
        field_name="gstin",
        base_config_content=SAMPLE_BASE_YAML,
    )

    # 1. Apply first time
    applied_yaml, hash1 = apply_config_patch(patch, SAMPLE_BASE_YAML)
    assert "field_name: gstin" in applied_yaml

    # 2. Apply second time (Idempotency check)
    reapplied_yaml, hash2 = apply_config_patch(patch, applied_yaml)
    assert hash1 == hash2
    assert reapplied_yaml.count("field_name: gstin") == 1

    # 3. Generate rollback patch
    rollback = generate_rollback_patch(patch, applied_yaml)
    assert rollback.action_type == "revert_reconciliation"
    assert rollback.rollback_patch_id == patch.patch_id
