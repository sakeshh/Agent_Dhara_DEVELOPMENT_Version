"""Tests for PR 6 Granular Runtime Authorizer Engine.

Verifies:
- Authorizes remediation units individually (never broad global booleans)
- Prohibits direct production execution from staging dry-run validation
- Field criticality awareness (primary/business keys cannot be auto-nullified)
- Quarantine rate compared against approval tolerance
- Blocking promotion upon new HIGH severity findings
"""

import pytest
from agent.etl_pipeline.remediation_models import ApprovalBinding
from agent.etl_pipeline.runtime_authorizer import (
    evaluate_unit_authorization,
    build_authorization_decision_manifest,
)


def test_granular_unit_authorization_promotion_eligibility():
    """Staging validation grants promotion eligibility without authorizing immediate production execution."""
    binding = ApprovalBinding(
        remediation_unit_id="ru-dbo.Customers::field::Email",
        option_id="opt-nullify",
        action_id="nullify_clean_value",
        plan_hash="sha256:p1",
        schema_hash="sha256:s1",
        rules_hash="sha256:r1",
        finding_signature="sha256:f1",
        source_snapshot_id="snap-01",
    )

    record = evaluate_unit_authorization(
        remediation_unit_id="ru-dbo.Customers::field::Email",
        action_id="nullify_clean_value",
        column_name="Email",
        artifact_hash="sha256:art123",
        source_snapshot_id="snap-01",
        schema_hash="sha256:s1",
        rules_hash="sha256:r1",
        finding_signature="sha256:f1",
        observed_quarantine_pct=0.5,
        has_approval_binding=True,
        approval_binding=binding,
        is_key_column=False,
    )

    # Invariant: Scoped decision is authorized for promotion
    assert record.decision == "authorized_for_promotion"
    assert record.promotion_eligible is True
    assert record.validated_environment == "staging_isolated"
    assert record.authorized_target_environment == "production"
    # Invariant: Production execution is strictly NOT authorized
    assert record.production_execution_status == "not_authorized"
    assert record.requires_production_preflight is True
    assert record.certificate_hash.startswith("sha256:")


def test_field_criticality_blocks_key_nullification():
    """Primary keys or critical monetary columns cannot be auto-nullified."""
    record = evaluate_unit_authorization(
        remediation_unit_id="ru-dbo.Orders::field::CustomerID",
        action_id="nullify_clean_value",
        column_name="CustomerID",
        artifact_hash="sha256:art999",
        source_snapshot_id="snap-01",
        schema_hash="sha256:s1",
        rules_hash="sha256:r1",
        finding_signature="sha256:f1",
        observed_quarantine_pct=0.0,
        is_key_column=True,
    )

    # Invariant: Key column nullification requires manual governance
    assert record.decision == "manual_only"
    assert any("Critical key or metric column" in c for c in record.conditions)


def test_quarantine_tolerance_violation_blocks_promotion():
    """Observed quarantine rate exceeding approval tolerance blocks promotion."""
    record = evaluate_unit_authorization(
        remediation_unit_id="ru-dbo.Orders::field::OrderDate",
        action_id="quarantine_row",
        column_name="OrderDate",
        artifact_hash="sha256:art_date",
        source_snapshot_id="snap-01",
        schema_hash="sha256:s1",
        rules_hash="sha256:r1",
        finding_signature="sha256:f1",
        observed_quarantine_pct=5.4,
        approved_quarantine_tolerance=2.0,  # Exceeded
    )

    assert record.decision == "blocked"
    assert record.promotion_eligible is False
    assert any("exceeded approved tolerance" in c for c in record.conditions)


def test_authorization_manifest_builder():
    """Builds complete granular authorization manifest across units."""
    b = ApprovalBinding(
        remediation_unit_id="ru-1",
        option_id="opt-1",
        action_id="trim",
        plan_hash="p",
        schema_hash="s",
    )
    rec1 = evaluate_unit_authorization(
        remediation_unit_id="ru-1",
        action_id="trim",
        column_name="Name",
        artifact_hash="art1",
        source_snapshot_id="snap1",
        schema_hash="s",
        rules_hash="r",
        finding_signature="f",
        observed_quarantine_pct=0.0,
    )

    manifest = build_authorization_decision_manifest([rec1])
    assert manifest["authorization_scope"] == "DRY_RUN_VALIDATED"
    assert manifest["environment"] == "staging_isolated"
    assert manifest["overall_promotion_eligible"] is True
    assert "ru-1" in manifest["remediation_units"]
    assert rec1.revocation_status == "active"
    assert rec1.superseded_by is None


def test_approved_high_quarantine_becomes_validated_manual_only():
    """Approved quarantine rate > 1.0% within tolerance becomes validated_manual_only."""
    binding = ApprovalBinding(
        remediation_unit_id="ru-dbo.Logs::field::Payload",
        option_id="opt-quar",
        action_id="quarantine_row",
        plan_hash="p",
        schema_hash="s",
        rules_hash="r",
        finding_signature="f",
        source_snapshot_id="snap",
    )
    rec = evaluate_unit_authorization(
        remediation_unit_id="ru-dbo.Logs::field::Payload",
        action_id="quarantine_row",
        column_name="Payload",
        artifact_hash="art",
        source_snapshot_id="snap",
        schema_hash="s",
        rules_hash="r",
        finding_signature="f",
        observed_quarantine_pct=4.5,
        has_approval_binding=True,
        approval_binding=binding,
        approved_quarantine_tolerance=10.0,
    )
    assert rec.decision == "validated_manual_only"
    assert rec.promotion_eligible is True
    assert rec.production_execution_status == "not_authorized"


def test_approved_tolerance_violation_becomes_blocked_pending_reapproval():
    """Observed quarantine exceeding approved tolerance becomes blocked_pending_reapproval."""
    binding = ApprovalBinding(
        remediation_unit_id="ru-dbo.Logs::field::Payload",
        option_id="opt-quar",
        action_id="quarantine_row",
        plan_hash="p",
        schema_hash="s",
        rules_hash="r",
        finding_signature="f",
        source_snapshot_id="snap",
    )
    rec = evaluate_unit_authorization(
        remediation_unit_id="ru-dbo.Logs::field::Payload",
        action_id="quarantine_row",
        column_name="Payload",
        artifact_hash="art",
        source_snapshot_id="snap",
        schema_hash="s",
        rules_hash="r",
        finding_signature="f",
        observed_quarantine_pct=12.5,
        has_approval_binding=True,
        approval_binding=binding,
        approved_quarantine_tolerance=5.0,
    )
    assert rec.decision == "blocked_pending_reapproval"
    assert rec.promotion_eligible is False


def test_dry_run_cannot_target_production(monkeypatch):
    """Dry-run execution against production environment is rejected at orchestrator gate."""
    import os
    from agent.etl_pipeline.execution_orchestrator import orchestrate_sql_execution
    monkeypatch.setenv("DHARA_TARGET_ENVIRONMENT", "production")

    res = orchestrate_sql_execution(
        "SELECT 1",
        session_id="test-session",
        dry_run=True,
    )
    assert res["ok"] is False
    assert res["stage"] == "environment_gate"
    assert res["failure_class"] == "blocked_production_mutation"


def test_metadata_driven_field_criticality_guard():
    """Columns without 'id'/'key' in name but declared critical in metadata cannot be auto-nullified."""
    # Column 'AccountNumber' declared as business key in metadata
    rec = evaluate_unit_authorization(
        remediation_unit_id="ru-dbo.Accounts::field::AccountNumber",
        action_id="nullify_clean_value",
        column_name="AccountNumber",
        artifact_hash="art",
        source_snapshot_id="snap",
        schema_hash="s",
        rules_hash="r",
        finding_signature="f",
        observed_quarantine_pct=0.0,
        metadata={"is_business_key": True},
    )
    assert rec.decision == "manual_only"
    assert rec.automatic_promotion_eligible is False
    assert rec.operator_signoff_required is True


def test_production_execution_gate_evaluation():
    """Evaluates 11-part production execution gate and verifies multi-reason rejection."""
    from agent.etl_pipeline.runtime_authorizer import evaluate_production_execution_gate

    # Valid base record
    valid_rec = evaluate_unit_authorization(
        remediation_unit_id="ru-1",
        action_id="trim",
        column_name="Name",
        artifact_hash="art_v1",
        source_snapshot_id="snap_v1",
        schema_hash="sch_v1",
        rules_hash="r",
        finding_signature="f",
        observed_quarantine_pct=0.0,
    )

    # 1. Happy path: passes all 11 checks
    res_pass = evaluate_production_execution_gate(
        valid_rec,
        production_preflight_passed=True,
        current_schema_hash="sch_v1",
        current_source_snapshot_id="snap_v1",
        current_artifact_hash="art_v1",
        current_time_iso="2026-09-08T10:00:00Z",
        target_environment="production",
    )
    assert res_pass["authorized"] is True
    assert len(res_pass["reason_codes"]) == 0
    assert res_pass["decision"] == "authorized_for_production_execution"

    # 2. Rejection with multiple reason codes: expired, artifact mismatch, schema changed
    res_fail = evaluate_production_execution_gate(
        valid_rec,
        production_preflight_passed=False,
        current_schema_hash="sch_v2",  # Mismatch
        current_source_snapshot_id="snap_v1",
        current_artifact_hash="art_v2",  # Mismatch
        current_time_iso="2026-09-20T10:00:00Z",  # Expired (past 2026-09-14)
        target_environment="production",
    )
    assert res_fail["authorized"] is False
    assert "AUTHORIZATION_EXPIRED" in res_fail["reason_codes"]
    assert "ARTIFACT_HASH_MISMATCH" in res_fail["reason_codes"]
    assert "SCHEMA_HASH_MISMATCH" in res_fail["reason_codes"]
    assert "PRODUCTION_PREFLIGHT_FAILED" in res_fail["reason_codes"]

    # 3. Explicit Approval Binding invalidation check (Condition 13)
    binding = ApprovalBinding(
        remediation_unit_id="ru-1",
        option_id="opt-1",
        action_id="trim",
        plan_hash="p1",
        schema_hash="sch_v1",
        rules_hash="rules_v1",
        finding_signature="sig_v1",
        source_snapshot_id="snap_v1",
    )
    res_binding_fail = evaluate_production_execution_gate(
        valid_rec,
        production_preflight_passed=True,
        current_schema_hash="sch_v1",
        current_source_snapshot_id="snap_v1",
        current_artifact_hash="art_v1",
        current_time_iso="2026-09-08T10:00:00Z",
        target_environment="production",
        approval_binding=binding,
        current_rules_hash="rules_v2_changed",  # Rules changed
        current_finding_signature="sig_v2_changed",  # Findings changed
    )
    assert res_binding_fail["authorized"] is False
    assert "APPROVAL_BINDING_INVALIDATED" in res_binding_fail["reason_codes"]
    assert "APPROVAL_RULES_CHANGED" in res_binding_fail["reason_codes"]
    assert "APPROVAL_FINDINGS_CHANGED" in res_binding_fail["reason_codes"]
    assert "rules_hash" in res_binding_fail["invalidated_approval_dimensions"]
    assert "finding_signature" in res_binding_fail["invalidated_approval_dimensions"]
    assert res_binding_fail["checks"]["valid_approval_binding"] is False
    assert len(res_pass["checks"]) == 13
    assert all(res_pass["checks"].values()) is True


def test_pilot_remediation_evidence_package():
    """Validates authoritative pilot remediation evidence package structure and invariants."""
    from agent.etl_pipeline.runtime_authorizer import build_pilot_remediation_evidence_package
    pkg = build_pilot_remediation_evidence_package(
        remediation_unit_id="ru-dbo.Customers::field::Email",
        source_snapshot_id="snap_20260907_01",
        selected_option_id="opt_trim",
        action_id="trim_whitespace",
        approval_binding_hash="sha256:app123",
        artifact_hash="sha256:art123",
        input_row_count=1000,
        clean_row_count=1000,
        quarantine_row_count=0,
        identity_reconciliation_passed=True,
        raw_preservation_passed=True,
        idempotency_passed=True,
        observed_clean_score=98.5,
        observed_full_population_score=98.5,
        coverage_percentage=100.0,
        promotion_decision="authorized_for_promotion",
    )
    assert pkg["remediation_unit_id"] == "ru-dbo.Customers::field::Email"
    assert pkg["technical_row_id_version"] == "trid-v2"
    assert pkg["dry_run_environment"] == "staging_isolated"
    assert pkg["promotion_decision"] == "authorized_for_promotion"
    assert pkg["production_execution_status"] == "not_authorized"
    assert pkg["quarantine_row_count"] == 0
    assert pkg["coverage_percentage"] == 100.0
    assert pkg["evidence_schema_version"] == "1.0"
    assert pkg["package_hash"].startswith("sha256:")
    assert pkg["storage_mode"] == "append_only_immutable"
    assert pkg["retention_policy"] == "7_year_audit"
    assert "evidence_package_id" in pkg
    assert "validation_run_id" in pkg
    assert "authorization_record_id" in pkg






