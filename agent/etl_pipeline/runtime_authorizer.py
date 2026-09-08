"""Granular Runtime Authorizer for Agent Dhara (PR 6).

Evaluates empirical dry-run evidence per remediation unit and issues
scoped authorization decisions. Enforces:
- Unit-level granularity (never generic action names)
- Field criticality rules (primary/business keys cannot be auto-nullified)
- Approval tolerance vs observed quarantine percentage
- Staging promotion eligibility vs immediate production execution prohibition
- Integrity-bound authorization record issuance
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional

from agent.etl_pipeline.remediation_models import (
    ApprovalBinding,
    IntegrityBoundAuthorizationRecord,
)

logger = logging.getLogger("agent.runtime_authorizer")

# Critical field indicators that forbid automatic nullification
CRITICAL_FIELD_SUBSTRINGS = {"id", "key", "price", "amount", "revenue", "salary", "ssn", "gstin"}


def evaluate_unit_authorization(
    remediation_unit_id: str,
    action_id: str,
    column_name: str,
    artifact_hash: str,
    source_snapshot_id: str,
    schema_hash: str,
    rules_hash: str,
    finding_signature: str,
    observed_quarantine_pct: float,
    has_approval_binding: bool = False,
    approval_binding: Optional[ApprovalBinding] = None,
    is_key_column: bool = False,
    is_nullable: bool = True,
    metadata: Optional[Dict[str, Any]] = None,
    approved_quarantine_tolerance: float = 1.0,
    has_new_high_defects: bool = False,
    environment: str = "staging_isolated",
) -> IntegrityBoundAuthorizationRecord:
    """Evaluates all safety policies for a single remediation unit and returns an authorization record."""
    conditions: List[str] = [
        "same artifact hash",
        "same schema hash",
        "same rules hash",
        "production preflight required",
    ]

    # Metadata-driven criticality check with substring heuristic fallback
    meta = metadata or {}
    is_declared_critical = bool(
        meta.get("is_primary_key")
        or meta.get("is_business_key")
        or meta.get("is_pii")
        or meta.get("is_financial")
        or str(meta.get("semantic_type") or "").lower() in ("key", "identifier", "financial", "currency", "ssn", "gstin")
    )
    is_critical_field = is_key_column or is_declared_critical or any(k in column_name.lower() for k in CRITICAL_FIELD_SUBSTRINGS)

    # 1. Check for blocking defects
    if has_new_high_defects:
        decision = "blocked"
        conditions.append("Blocked due to new HIGH severity defects introduced")
        promotion_eligible = False

    # 2. Check field criticality: primary/business keys or critical amounts cannot be auto-nullified
    elif action_id == "nullify_clean_value" and is_critical_field:
        decision = "manual_only"
        conditions.append("Critical key or metric column requires explicit human governance")
        promotion_eligible = True if has_approval_binding else False

    # 3. Check observed quarantine vs approved tolerance
    elif observed_quarantine_pct > approved_quarantine_tolerance:
        decision = "blocked_pending_reapproval" if has_approval_binding else "blocked"
        conditions.append(
            f"Observed quarantine ({observed_quarantine_pct}%) exceeded approved tolerance ({approved_quarantine_tolerance}%)"
        )
        promotion_eligible = False

    # 4. Check approval binding validity if required
    elif has_approval_binding and approval_binding is not None:
        eval_res = approval_binding.evaluate_invalidation({
            "schema_hash": schema_hash,
            "rules_hash": rules_hash,
            "finding_signature": finding_signature,
            "source_snapshot_id": source_snapshot_id,
        })
        if not eval_res["valid"]:
            decision = "invalidated"
            conditions.extend([f"Invalidated: {dim}" for dim in eval_res["invalidated_dimensions"]])
            promotion_eligible = False
        elif observed_quarantine_pct > 1.0:
            decision = "validated_manual_only"
            conditions.append("High quarantine rate authorized for promotion under manual review only")
            promotion_eligible = True
        else:
            decision = "authorized_for_promotion"
            promotion_eligible = True

    # 5. Low-risk standard formatting actions
    elif action_id in ("trim", "lowercase", "uppercase", "flag_only") and observed_quarantine_pct <= 1.0:
        decision = "authorized_for_promotion"
        promotion_eligible = True

    # 6. Default closed: manual review required
    else:
        decision = "manual_only"
        conditions.append("Requires human sign-off prior to production promotion")
        promotion_eligible = False

    # Granular promotion eligibility flags
    if decision == "authorized_for_promotion":
        auto_promo = True
        manual_promo = True
        op_signoff = False
    elif decision in ("manual_only", "validated_manual_only"):
        auto_promo = False
        manual_promo = promotion_eligible
        op_signoff = True
    else:
        auto_promo = False
        manual_promo = False
        op_signoff = True

    # Compute certificate integrity hash over canonical payload
    cert_payload = {
        "action_id": action_id,
        "artifact_hash": artifact_hash,
        "decision": decision,
        "environment": environment,
        "finding_signature": finding_signature,
        "remediation_unit_id": remediation_unit_id,
        "rules_hash": rules_hash,
        "schema_hash": schema_hash,
        "source_snapshot_id": source_snapshot_id,
    }
    cert_bytes = json.dumps(cert_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    cert_hash = "sha256:" + hashlib.sha256(cert_bytes).hexdigest()

    return IntegrityBoundAuthorizationRecord(
        remediation_unit_id=remediation_unit_id,
        action_id=action_id,
        artifact_hash=artifact_hash,
        source_snapshot_id=source_snapshot_id,
        schema_hash=schema_hash,
        rules_hash=rules_hash,
        finding_signature=finding_signature,
        decision=decision,
        validated_environment=environment,
        authorized_target_environment="production",
        intended_target_environment="production",
        production_execution_status="not_authorized",
        promotion_eligible=promotion_eligible,
        automatic_promotion_eligible=auto_promo,
        manual_promotion_eligible=manual_promo,
        operator_signoff_required=op_signoff,
        requires_production_preflight=True,
        conditions=conditions,
        certificate_hash=cert_hash,
        signature_status="not_signed",
        revocation_status="active",
        superseded_by=None,
    )


def build_authorization_decision_manifest(
    records: List[IntegrityBoundAuthorizationRecord],
    environment: str = "staging_isolated",
) -> Dict[str, Any]:
    """Builds the complete granular authorization manifest for an assessment run."""
    units_dict = {}
    any_blocked = False
    all_promo_eligible = True

    for rec in records:
        units_dict[rec.remediation_unit_id] = rec.to_dict()
        if rec.decision in ("blocked", "blocked_pending_reapproval", "invalidated"):
            any_blocked = True
        if not rec.promotion_eligible:
            all_promo_eligible = False

    return {
        "authorization_scope": "DRY_RUN_VALIDATED",
        "environment": environment,
        "overall_promotion_eligible": all_promo_eligible and not any_blocked,
        "production_execution_status": "not_authorized",
        "requires_production_preflight": True,
        "total_units_evaluated": len(records),
        "remediation_units": units_dict,
    }


def evaluate_production_execution_gate(
    record: IntegrityBoundAuthorizationRecord,
    *,
    production_preflight_passed: bool,
    current_schema_hash: str,
    current_source_snapshot_id: str,
    current_artifact_hash: str,
    current_time_iso: Optional[str] = None,
    operator_approved: bool = False,
    target_environment: str = "production",
    execution_role: Optional[str] = None,
    approved_roles: Optional[Set[str]] = None,
    approval_binding: Optional[ApprovalBinding] = None,
    current_rules_hash: Optional[str] = None,
    current_finding_signature: Optional[str] = None,
    current_generator_version: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Authoritative 13-part production execution gate.
    Evaluates all 13 execution prerequisites at production execution time and returns
    all violated reason codes rather than failing at the first mismatch:
    1. active_certificate
    2. not_revoked
    3. not_superseded
    4. not_expired (now >= expires_at)
    5. valid_promotion_decision
    6. valid_approval_binding (rules, findings, generator version, option, schema, snapshot)
    7. production_preflight_passed
    8. exact_artifact_hash_match
    9. schema_fresh
    10. source_fresh
    11. operator_approved (when required)
    12. target_environment_verified
    13. execution_identity_authorized
    """
    from datetime import datetime, timezone
    now_iso = current_time_iso or datetime.now(timezone.utc).isoformat()
    roles = approved_roles or {"data_governance_lead", "production_deployer", "release_manager"}

    reason_codes: List[str] = []
    invalidated_approval_dimensions: List[str] = []

    # 1. Certificate active and present
    if not record.certificate_hash:
        reason_codes.append("CERTIFICATE_MISSING")

    # 2. Revocation status
    if record.revocation_status != "active":
        reason_codes.append(f"CERTIFICATE_{record.revocation_status.upper()}")

    # 3. Superseded check
    if record.superseded_by is not None:
        reason_codes.append("CERTIFICATE_SUPERSEDED")

    # 4. Expiry evaluation: now >= expires_at
    if now_iso >= record.expires_at:
        reason_codes.append("AUTHORIZATION_EXPIRED")

    # 5. Valid promotion decision
    if record.decision not in ("authorized_for_promotion", "validated_manual_only"):
        reason_codes.append(f"DECISION_{record.decision.upper()}_PROHIBITS_EXECUTION")

    # 6. Valid approval binding
    binding_is_valid = True
    if approval_binding is not None:
        eval_binding = approval_binding.evaluate_invalidation({
            "schema_hash": current_schema_hash,
            "rules_hash": current_rules_hash or record.rules_hash,
            "finding_signature": current_finding_signature or record.finding_signature,
            "source_snapshot_id": current_source_snapshot_id,
            "generator_version": current_generator_version,
        })
        if not eval_binding["valid"]:
            binding_is_valid = False
            reason_codes.append("APPROVAL_BINDING_INVALIDATED")
            for rc in eval_binding.get("reason_codes", []):
                reason_codes.append(rc)
            invalidated_approval_dimensions.extend(eval_binding.get("invalidated_dimensions", []))
    elif record.operator_signoff_required or record.decision == "validated_manual_only":
        binding_is_valid = False
        reason_codes.append("APPROVAL_BINDING_MISSING")

    # 7. Production preflight check
    if not production_preflight_passed:
        reason_codes.append("PRODUCTION_PREFLIGHT_FAILED")

    # 8. Exact artifact hash match
    if record.artifact_hash != current_artifact_hash:
        reason_codes.append("ARTIFACT_HASH_MISMATCH")

    # 9. Schema freshness
    if record.schema_hash != current_schema_hash:
        reason_codes.append("SCHEMA_HASH_MISMATCH")

    # 10. Source freshness
    if record.source_snapshot_id != current_source_snapshot_id:
        reason_codes.append("SOURCE_SNAPSHOT_CHANGED")

    # 11. Operator sign-off when required
    if record.operator_signoff_required and not operator_approved:
        reason_codes.append("OPERATOR_APPROVAL_REQUIRED")

    # 12. Target environment verified
    if target_environment != record.intended_target_environment:
        reason_codes.append("TARGET_ENVIRONMENT_MISMATCH")

    # 13. Execution identity authorized
    if execution_role is not None and execution_role not in roles:
        reason_codes.append("EXECUTION_IDENTITY_UNAUTHORIZED")

    checks = {
        "active_certificate": bool(record.certificate_hash),
        "not_revoked": record.revocation_status == "active",
        "not_superseded": record.superseded_by is None,
        "not_expired": now_iso < record.expires_at,
        "valid_promotion_decision": record.decision in ("authorized_for_promotion", "validated_manual_only"),
        "valid_approval_binding": binding_is_valid,
        "production_preflight_passed": bool(production_preflight_passed),
        "exact_artifact_hash_match": record.artifact_hash == current_artifact_hash,
        "schema_fresh": record.schema_hash == current_schema_hash,
        "source_fresh": record.source_snapshot_id == current_source_snapshot_id,
        "operator_approved": (not record.operator_signoff_required) or bool(operator_approved),
        "target_environment_verified": target_environment == record.intended_target_environment,
        "execution_identity_authorized": (execution_role is None) or (execution_role in roles),
    }

    is_authorized = len(reason_codes) == 0

    return {
        "authorized": is_authorized,
        "remediation_unit_id": record.remediation_unit_id,
        "decision": "authorized_for_production_execution" if is_authorized else "execution_denied",
        "checks": checks,
        "reason_codes": reason_codes,
        "failed_reason_codes": reason_codes,
        "invalidated_approval_dimensions": invalidated_approval_dimensions,
        "evaluated_at_utc": now_iso,
        "operator_approved": operator_approved,
        "target_environment": target_environment,
    }


def build_pilot_remediation_evidence_package(
    *,
    remediation_unit_id: str,
    source_snapshot_id: str,
    selected_option_id: str,
    action_id: str,
    approval_binding_hash: str,
    artifact_hash: str,
    input_row_count: int,
    clean_row_count: int,
    quarantine_row_count: int,
    identity_reconciliation_passed: bool,
    raw_preservation_passed: bool,
    idempotency_passed: bool,
    observed_clean_score: float,
    observed_full_population_score: float,
    coverage_percentage: float,
    promotion_decision: str = "authorized_for_promotion",
    dry_run_environment: str = "staging_isolated",
    technical_row_id_version: str = "trid-v2",
    evidence_package_id: Optional[str] = None,
    validation_run_id: Optional[str] = None,
    authorization_record_id: Optional[str] = None,
    generator_version: str = "v2.4.0",
    created_at_iso: Optional[str] = None,
) -> Dict[str, Any]:
    """Generates the authoritative immutable pilot remediation evidence package.
    Computes a canonical SHA-256 package hash over all attributes and guarantees
    that production_execution_status is strictly 'not_authorized'.
    """
    from datetime import datetime, timezone
    pkg_id = evidence_package_id or f"evidence-{hashlib.sha256(f'{remediation_unit_id}:{source_snapshot_id}:{artifact_hash}'.encode('utf-8')).hexdigest()[:16]}"
    run_id = validation_run_id or f"dryrun-{hashlib.sha256(f'{source_snapshot_id}:{artifact_hash}'.encode('utf-8')).hexdigest()[:12]}"
    auth_id = authorization_record_id or f"auth-{hashlib.sha256(approval_binding_hash.encode('utf-8')).hexdigest()[:12]}"
    now_utc = created_at_iso or datetime.now(timezone.utc).isoformat()

    pkg_core: Dict[str, Any] = {
        "evidence_package_id": pkg_id,
        "evidence_schema_version": "1.0",
        "created_at": now_utc,
        "generator_version": generator_version,
        "validation_run_id": run_id,
        "authorization_record_id": auth_id,
        "remediation_unit_id": remediation_unit_id,
        "source_snapshot_id": source_snapshot_id,
        "technical_row_id_version": technical_row_id_version,
        "selected_option_id": selected_option_id,
        "action_id": action_id,
        "approval_binding_hash": approval_binding_hash,
        "artifact_hash": artifact_hash,
        "dry_run_environment": dry_run_environment,
        "input_row_count": input_row_count,
        "clean_row_count": clean_row_count,
        "quarantine_row_count": quarantine_row_count,
        "identity_reconciliation_passed": identity_reconciliation_passed,
        "raw_preservation_passed": raw_preservation_passed,
        "idempotency_passed": idempotency_passed,
        "observed_clean_score": observed_clean_score,
        "observed_full_population_score": observed_full_population_score,
        "coverage_percentage": coverage_percentage,
        "promotion_decision": promotion_decision,
        "production_execution_status": "not_authorized",
        "storage_mode": "append_only_immutable",
        "retention_policy": "7_year_audit",
    }

    # Canonical serialization and hash
    serialized_bytes = json.dumps(pkg_core, sort_keys=True, separators=(",", ":")).encode("utf-8")
    pkg_hash = "sha256:" + hashlib.sha256(serialized_bytes).hexdigest()

    pkg_core["package_hash"] = pkg_hash
    return pkg_core


