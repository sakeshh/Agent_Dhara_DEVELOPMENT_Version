"""Unit tests for PR 4.6b Governance, Issue Registry, and Immutable Provenance.

Verifies:
- Canonical issue registry consumed directly in validate_plan
- Distinguishes executable, manual-only, configuration, and unknown issues
- Human-approved actions retain original recommendation source, recorded in approval event
- Execution bucket is approved_manual, never automatic_standard
- Projected readiness is estimated pending dry-run validation, runtime auto-fix authorized is False
- Approval binding invalidation when option ID or schema hash changes
"""

import pytest
from agent.etl_pipeline.validate_plan import validate_issue_registration
from agent.etl_pipeline.validate_plan import validate_issue_registration
from agent.etl_pipeline.manual_review_promote import apply_manual_resolutions
from agent.etl_pipeline.readiness_engine import recompute_readiness_scorecard
from agent.etl_pipeline.remediation_models import ApprovalBinding


def test_issue_registration_validation():
    """Validates registration, execution scope, and generator requirements from capability registry."""
    # 1. Standard executable issue
    res_exec = validate_issue_registration("missing_values")
    assert res_exec["registration_status"] == "registered"
    assert res_exec["execution_scope"] == "data"
    assert res_exec["generator_required"] is True

    # 2. Manual review issue
    res_manual = validate_issue_registration("impossible_date")
    assert res_manual["registration_status"] == "registered"
    assert res_manual["requires_manual_review"] is True

    # 3. Configuration change action
    res_cfg = validate_issue_registration("metadata_manifest_mismatch")
    assert res_cfg["registration_status"] == "registered"
    assert res_cfg["execution_scope"] == "configuration"
    assert res_cfg["generator_required"] is False

    # 4. Unknown fail-safe issue
    res_unknown = validate_issue_registration("non_existent_issue_type_xyz")
    assert res_unknown["registration_status"] == "unknown"
    assert res_unknown["execution_scope"] == "validation_only"
    assert res_unknown["generator_required"] is False
    assert res_unknown["requires_manual_review"] is True


def test_manual_review_promotion_immutable_provenance():
    """Human approval appends event, preserves recommendation source, and sets approved_manual bucket."""
    plan = {
        "manual_review": [
            {
                "id": "mr-courses-price",
                "dataset": "dbo.Courses",
                "column": "Price",
                "issue_type": "negative_values",
                "authority_source": "CATALOG_FALLBACK",
                "recommendation_source": "CATALOG_FALLBACK",
                "severity": "high",
                "status": "pending",
                "supported_resolutions": [
                    {"id": "flag_only", "action": "flag_only"}
                ]
            }
        ]
    }
    updated_plan, errs = apply_manual_resolutions(
        plan,
        [{"item_id": "mr-courses-price", "resolution_id": "flag_only"}],
    )
    assert not errs
    promoted = updated_plan["datasets"]["dbo.Courses"]["steps"][0]

    # Invariant: Bucket is approved_manual, never automatic_standard
    assert promoted["execution_bucket"] == "approved_manual"
    assert promoted["bucket"] == "approved_manual"
    assert promoted["approval_status"] == "approved"
    assert promoted["approval_authority"] == "HUMAN_OPERATOR"

    # Invariant: Original recommendation source is preserved
    assert promoted["recommendation_source"] == "CATALOG_FALLBACK"

    # Invariant: Approval event appended
    events = promoted.get("events", [])
    assert len(events) >= 1
    assert any(e["type"] == "approval_granted" and e["authority"] == "HUMAN_OPERATOR" for e in events)


def test_readiness_scorecard_estimated_projection():
    """Readiness scorecard clearly marks post-remediation projections as estimated pending validation."""
    scorecard = recompute_readiness_scorecard(
        plan={"plan_id": "p1"},
        assessment={"datasets": {"dbo.Courses": {"row_count": 100}}},
        rules={},
    )

    overall = scorecard.get("Overall", {})
    # Invariant: Clearly estimated pending validation
    assert overall.get("projection_status") == "estimated_pending_validation"
    assert overall.get("runtime_auto_fix_authorized") is False
    assert overall.get("observed_post_etl_score") is None


def test_approval_binding_invalidation():
    """Approval binding is invalid if option_id or schema_hash changes."""
    binding = ApprovalBinding(
        binding_id="bind-001",
        remediation_unit_id="ru-dbo.Customers_Raw::field::Email",
        selected_option_id="opt-nullify",
        action_id="nullify_clean_value",
        plan_hash="sha256:plan111",
        schema_hash="sha256:schema222",
        reviewer_id="operator_jane",
    )

    # Valid check with matching hashes
    assert binding.validate(current_option_id="opt-nullify", current_schema_hash="sha256:schema222") is True

    # Invalid check when option changes
    assert binding.validate(current_option_id="opt-different", current_schema_hash="sha256:schema222") is False

    # Invalid check when schema changes
    assert binding.validate(current_option_id="opt-nullify", current_schema_hash="sha256:schema_tampered") is False
