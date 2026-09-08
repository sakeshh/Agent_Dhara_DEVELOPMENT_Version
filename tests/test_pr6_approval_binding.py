"""Tests for PR 6 Multi-Dimensional Canonical Approval Binding and Invalidation.

Verifies:
- Canonical payload sorting produces deterministic binding hashes
- Multi-dimensional invalidation tracks exact changed dimensions
- Invalidation on schema hash, rules hash, finding signature, or source snapshot
"""

import pytest
from agent.etl_pipeline.remediation_models import ApprovalBinding


def test_canonical_binding_hash_determinism():
    """Canonical binding hash is stable and deterministic regardless of field initialization order."""
    b1 = ApprovalBinding(
        remediation_unit_id="ru-dbo.Customers::field::Email",
        option_id="opt-nullify",
        action_id="nullify_clean_value",
        plan_hash="sha256:plan111",
        schema_hash="sha256:schema222",
        rules_hash="sha256:rules333",
        finding_signature="sha256:find444",
        generator_version="2.4.1",
        threshold_policy_version="2026.08.2",
        source_snapshot_id="snap-555",
        reviewer_id="reviewer_01",
    )

    b2 = ApprovalBinding(
        source_snapshot_id="snap-555",
        remediation_unit_id="ru-dbo.Customers::field::Email",
        action_id="nullify_clean_value",
        option_id="opt-nullify",
        finding_signature="sha256:find444",
        threshold_policy_version="2026.08.2",
        generator_version="2.4.1",
        rules_hash="sha256:rules333",
        schema_hash="sha256:schema222",
        plan_hash="sha256:plan111",
        reviewer_id="reviewer_01",
    )

    assert b1.canonical_binding_hash() == b2.canonical_binding_hash()
    assert b1.canonical_binding_hash().startswith("sha256:")


def test_multi_dimensional_invalidation():
    """Identifies exact dimensions that changed upon re-evaluation."""
    binding = ApprovalBinding(
        remediation_unit_id="ru-dbo.Customers::field::Email",
        option_id="opt-flag",
        action_id="flag_only",
        plan_hash="sha256:p1",
        schema_hash="sha256:s1",
        rules_hash="sha256:r1",
        finding_signature="sha256:f1",
        source_snapshot_id="snap-01",
    )

    # 1. Matching state -> Valid
    valid_state = {
        "option_id": "opt-flag",
        "schema_hash": "sha256:s1",
        "rules_hash": "sha256:r1",
        "finding_signature": "sha256:f1",
        "source_snapshot_id": "snap-01",
    }
    eval_res = binding.evaluate_invalidation(valid_state)
    assert eval_res["valid"] is True
    assert len(eval_res["invalidated_dimensions"]) == 0

    # 2. Source snapshot changed (new data batch arrived)
    changed_snap = dict(valid_state, source_snapshot_id="snap-02-new")
    eval_snap = binding.evaluate_invalidation(changed_snap)
    assert eval_snap["valid"] is False
    assert "source_snapshot_id" in eval_snap["invalidated_dimensions"]
    assert "APPROVAL_SOURCE_SNAPSHOT_CHANGED" in eval_snap["reason_codes"]

    # 3. Rules hash changed (threshold policy altered)
    changed_rules = dict(valid_state, rules_hash="sha256:r2-modified")
    eval_rules = binding.evaluate_invalidation(changed_rules)
    assert eval_rules["valid"] is False
    assert "rules_hash" in eval_rules["invalidated_dimensions"]
    assert "APPROVAL_RULES_CHANGED" in eval_rules["reason_codes"]

    # 4. Finding signature changed (data degraded further)
    changed_find = dict(valid_state, finding_signature="sha256:f2-degraded")
    eval_find = binding.evaluate_invalidation(changed_find)
    assert eval_find["valid"] is False
    assert "finding_signature" in eval_find["invalidated_dimensions"]
    assert "APPROVAL_FINDINGS_CHANGED" in eval_find["reason_codes"]
