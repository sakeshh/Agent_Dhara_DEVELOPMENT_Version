"""
Remediation data models for Agent Dhara.
Defines risk levels, certainty classifications, capability entries, impact policies, and routing contracts.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Tuple

Certainty = Literal["high", "medium", "low", "unknown"]
RiskLevel = Literal["low", "medium", "high", "critical"]
ApprovalPolicy = Literal["never_auto", "always_approval", "policy_dependent", "low_risk_auto"]
RecommendationCategory = Literal["standard_remediation", "decision_required", "manual_investigation"]

ExecutionScope = Literal[
    "field_transform",
    "row_partition",
    "dataset_transform",
    "configuration_change",
    "validation_only",
    "no_op",
]

ExecutionBucket = Literal[
    "automatic_standard",
    "approved_manual",
    "validation_only",
    "configuration_change",
    "no_op",
    "blocked",
]

RemediationUnitScope = Literal["field", "row", "configuration", "relationship", "dataset"]



@dataclass(frozen=True)
class ImpactPolicy:
    """Action-level or capability-level impact boundary for automatic execution."""
    max_affected_percentage: float = 5.0
    max_affected_rows: int = 10000
    max_quarantine_percentage: float = 1.0
    allow_row_drop: bool = False
    requires_explicit_reapproval: bool = True


@dataclass(frozen=True)
class CapabilityEntry:
    """Registered capability and governance contract for a specific issue type."""
    canonical_issue_type: str
    detection_certainty: Certainty
    remediation_certainty: Certainty
    risk_level: RiskLevel
    default_action: str
    supported_actions: List[str]
    generator_ids: Dict[str, str] = field(default_factory=dict)
    approval_policy: ApprovalPolicy = "policy_dependent"
    impact_policy: ImpactPolicy = field(default_factory=ImpactPolicy)
    cluster_group: str = "general_quality"
    context_tags: List[str] = field(default_factory=list)
    reversible: bool = True
    row_destructive: bool = False
    schema_mutating: bool = False
    auto_fix_eligible: bool = False
    execution_scope: ExecutionScope = "field_transform"
    validator_ids: List[str] = field(default_factory=lambda: ["codegen_validator"])

    @property
    def approval_required(self) -> bool:
        return self.approval_policy in ("always_approval", "never_auto", "policy_dependent") or not self.auto_fix_eligible


@dataclass
class RoutingResult:
    """Auditable routing outcome for an evaluated data quality finding."""
    issue_id: str
    raw_issue_type: str
    canonical_issue_type: str
    dataset: str
    column: Optional[str]
    recommendation_category: RecommendationCategory
    recommendation_status: str
    approval_status: str
    codegen_status: str
    validation_status: str
    execution_status: str
    auto_fix_eligible: bool
    reason_codes: List[str]
    capability: CapabilityEntry
    cluster_key: Tuple[str, str]
    primary_reason_code: Optional[str] = None
    registry_version: str = "v1.0"
    affected_rows: Optional[int] = None
    affected_percentage: Optional[float] = None
    applied_impact_limit_pct: float = 5.0
    applied_impact_limit_rows: int = 10000

    @property
    def suggested_action(self) -> str:
        return self.capability.default_action if self.capability else "keep_as_is"

    def __post_init__(self):
        # Structural invariant: Unknown capability is strictly review-only
        if self.canonical_issue_type == "unknown_profiler_issue" or getattr(self.capability, "canonical_issue_type", None) == "unknown_profiler_issue":
            if self.auto_fix_eligible:
                raise ValueError("Structural invariant violation: unknown capability cannot have auto_fix_eligible=True")
            if self.approval_status != "pending":
                raise ValueError("Structural invariant violation: unknown capability must have approval_status='pending'")
            if self.recommendation_category == "standard_remediation":
                raise ValueError("Structural invariant violation: unknown capability cannot be standard_remediation")

    @property
    def lifecycle(self) -> Dict[str, str]:
        return {
            "recommendation_status": self.recommendation_status,
            "approval_status": self.approval_status,
            "codegen_status": self.codegen_status,
            "validation_status": self.validation_status,
            "execution_status": self.execution_status,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "issue_id": self.issue_id,
            "raw_issue_type": self.raw_issue_type,
            "canonical_issue_type": self.canonical_issue_type,
            "dataset": self.dataset,
            "column": self.column,
            "recommendation_category": self.recommendation_category,
            "lifecycle": self.lifecycle,
            "auto_fix_eligible": self.auto_fix_eligible,
            "primary_reason_code": self.primary_reason_code or (self.reason_codes[0] if self.reason_codes else "UNKNOWN"),
            "reason_codes": self.reason_codes,
            "registry_version": self.registry_version,
            "cluster_key": list(self.cluster_key),
            "evaluated_policy": {
                "risk": self.capability.risk_level,
                "risk_level": self.capability.risk_level,
                "percentage_limit": self.applied_impact_limit_pct,
                "row_limit": self.applied_impact_limit_rows,
                "approval_policy": self.capability.approval_policy,
            },
            "observed": {
                "affected_rows": self.affected_rows,
                "affected_percentage": self.affected_percentage,
            },
            "metrics": {
                "affected_rows": self.affected_rows,
                "affected_percentage": self.affected_percentage,
                "applied_impact_limit_pct": self.applied_impact_limit_pct,
                "applied_impact_limit_rows": self.applied_impact_limit_rows,
            },
            "capability": {
                "detection_certainty": self.capability.detection_certainty,
                "remediation_certainty": self.capability.remediation_certainty,
                "risk_level": self.capability.risk_level,
                "default_action": self.capability.default_action,
                "supported_actions": self.capability.supported_actions,
                "reversible": self.capability.reversible,
                "row_destructive": self.capability.row_destructive,
            },
        }


# ---------------------------------------------------------------------------
# PR 4.5: Action-Code Contract Alignment Models & Exceptions
# ---------------------------------------------------------------------------

class UnsupportedRemediationAction(ValueError):
    """Raised when an action is not supported by the capability entry."""
    pass


class RemediationApprovalRequired(PermissionError):
    """Raised when an unapproved action requires human sign-off before code generation."""
    pass


class NoOpActionHasGenerator(ValueError):
    """Raised when a no-op action (keep_as_is, skip_etl) is configured with an active code generator."""
    pass


class ActionSemanticMismatch(ValueError):
    """Raised when resolved action flags contradict action semantics (e.g. flag_only quarantining rows)."""
    pass


@dataclass(frozen=True)
class ResolvedRemediationAction:
    issue_id: str
    remediation_unit_id: str
    option_id: str
    action_id: str
    dataset_id: str
    column_name: Optional[str]
    authority_source: str
    approval_status: str

    preserves_raw: bool
    retains_row: bool
    nullifies_clean_value: bool
    quarantines_row: bool
    drops_row: bool

    generator_id: Optional[str]


def validate_resolved_action(
    action: ResolvedRemediationAction,
    capability: CapabilityEntry,
) -> None:
    """Validate action-code alignment before code generation.
    Enforces that supported actions, approval status, and execution semantics
    strictly adhere to registry invariants.
    """
    if action.action_id not in capability.supported_actions:
        raise UnsupportedRemediationAction(
            f"Action '{action.action_id}' is not supported by capability '{capability.canonical_issue_type}' (supported: {capability.supported_actions})"
        )

    if action.approval_status != "approved" and capability.approval_required:
        raise RemediationApprovalRequired(
            f"Remediation action '{action.action_id}' for issue '{action.issue_id}' requires explicit human approval before code generation."
        )

    if action.action_id in {"keep_as_is", "skip_etl"}:
        if action.generator_id is not None:
            raise NoOpActionHasGenerator(
                f"No-op action '{action.action_id}' cannot have an active generator (found: {action.generator_id})"
            )

    if action.action_id == "flag_only" and (action.quarantines_row or action.drops_row):
        raise ActionSemanticMismatch("flag_only cannot quarantine or drop rows")

    if action.action_id == "nullify_clean_value" and not action.retains_row:
        raise ActionSemanticMismatch("nullify_clean_value must retain the source row")


@dataclass(frozen=True)
class GeneratedRemediationArtifact:
    action_id: str
    dialect: str
    code: Optional[str]
    outcome_metadata: Dict[str, Any]
    validation_status: str
    is_provisional: bool
    provisional_label: Optional[str] = None
    artifact_id: str = "art-default"
    recommendation_id: Optional[str] = None
    selected_option_id: Optional[str] = None
    generator_id: Optional[str] = None
    generator_version: str = "2.4.1"
    approval_record_id: Optional[str] = None
    plan_hash: Optional[str] = None
    schema_hash: Optional[str] = None
    registry_version: str = "2026.09"
    generated_at: str = "2026-09-07T13:00:00Z"
    code_hash: Optional[str] = None
    artifact_status: str = "generated"  # provisional | approved | generated | validated | superseded
    output_columns: List[str] = field(default_factory=list)
    output_types: Dict[str, str] = field(default_factory=dict)
    validation_columns: List[str] = field(default_factory=list)
    row_disposition: str = "retain_all"


@dataclass(frozen=True)
class RemediationUnitKey:
    """General remediation-unit key separating report and plan grouping from simple field grouping."""
    target_container: str  # dataset_id or relationship_id
    scope: RemediationUnitScope  # "field" | "row" | "configuration" | "relationship" | "dataset"
    target_identifier: str  # column_name, finding_family, manifest_field, rule_id

    def to_string(self) -> str:
        return f"{self.target_container}::{self.scope}::{self.target_identifier}"


@dataclass
class RemediationUnit:
    """Canonical remediation unit grouping findings sharing the same target unit."""
    remediation_unit_id: str
    scope: ExecutionScope
    dataset_id: str
    target_identifier: str
    source_finding_ids: List[str] = field(default_factory=list)
    canonical_issue_types: List[str] = field(default_factory=list)
    column_name: Optional[str] = None
    recommendation_source: str = "CATALOG_FALLBACK"  # DETERMINISTIC_REGISTRY | LLM_ADVISORY | CATALOG_FALLBACK
    selection_source: Optional[str] = None  # HUMAN_OPERATOR | SYSTEM_DEFAULT
    approval_authority: Optional[str] = None  # HUMAN_OPERATOR | SYSTEM_POLICY
    ai_assistance_status: str = "not_applicable"  # available | unavailable | not_applicable
    invocation_record: Dict[str, Any] = field(default_factory=dict)
    supported_options: List[Dict[str, Any]] = field(default_factory=list)
    selected_option_id: Optional[str] = None
    approval_status: str = "pending"  # pending | approved | rejected
    artifact_status: str = "not_generated"  # not_generated | provisional | generated | approved | validated | superseded
    execution_bucket: ExecutionBucket = "automatic_standard"
    operations: List[str] = field(default_factory=list)
    row_disposition: str = "retain_all"  # retain_all | clean_xor_quarantine | quarantine_all | drop_rows
    expected_outcome: Dict[str, Any] = field(default_factory=dict)
    reason_codes: List[str] = field(default_factory=list)
    events: List[Dict[str, Any]] = field(default_factory=list)
    generated_artifact: Optional[GeneratedRemediationArtifact] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "remediation_unit_id": self.remediation_unit_id,
            "scope": self.scope,
            "dataset_id": self.dataset_id,
            "column_name": self.column_name,
            "target_identifier": self.target_identifier,
            "source_finding_ids": self.source_finding_ids,
            "canonical_issue_types": self.canonical_issue_types,
            "recommendation_source": self.recommendation_source,
            "selection_source": self.selection_source,
            "approval_authority": self.approval_authority,
            "ai_assistance_status": self.ai_assistance_status,
            "invocation_record": self.invocation_record,
            "supported_options": self.supported_options,
            "selected_option_id": self.selected_option_id,
            "approval_status": self.approval_status,
            "artifact_status": self.artifact_status,
            "execution_bucket": self.execution_bucket,
            "operations": self.operations,
            "row_disposition": self.row_disposition,
            "expected_outcome": self.expected_outcome,
            "reason_codes": self.reason_codes,
            "events": self.events,
            "details": self.details,
        }


@dataclass(frozen=True)
class ApprovalBinding:
    """Explicit cryptographic/governance binding for manual human approvals."""
    remediation_unit_id: str
    option_id: str = ""
    action_id: str = ""
    plan_hash: str = ""
    schema_hash: str = ""
    rules_hash: str = ""
    finding_signature: str = ""
    generator_version: str = "2.4.1"
    threshold_policy_version: str = "2026.08.2"
    source_snapshot_id: str = ""
    registry_version: str = "2026.09"
    business_rule_version: str = "2026.08.2"
    estimated_impact: Dict[str, Any] = field(default_factory=dict)
    reviewer_identity: str = "reviewer_01"
    justification: str = "Approved by data steward"
    approval_timestamp: str = "2026-09-07T13:00:00Z"
    binding_id: Optional[str] = None
    reviewer_id: Optional[str] = None
    selected_option_id: Optional[str] = None

    def __post_init__(self):
        if self.selected_option_id and not self.option_id:
            object.__setattr__(self, "option_id", self.selected_option_id)
        if self.reviewer_id and self.reviewer_identity == "reviewer_01":
            object.__setattr__(self, "reviewer_identity", self.reviewer_id)

    def canonical_binding_payload(self) -> Dict[str, Any]:
        """Returns deterministic dictionary representation omitting timestamps and non-governance metadata."""
        return {
            "action_id": self.action_id,
            "finding_signature": self.finding_signature,
            "generator_version": self.generator_version,
            "option_id": self.option_id,
            "plan_hash": self.plan_hash,
            "registry_version": self.registry_version,
            "remediation_unit_id": self.remediation_unit_id,
            "rules_hash": self.rules_hash,
            "schema_hash": self.schema_hash,
            "source_snapshot_id": self.source_snapshot_id,
            "threshold_policy_version": self.threshold_policy_version,
        }

    def canonical_binding_hash(self) -> str:
        """Computes SHA-256 over canonicalized, sorted JSON payload."""
        import json as _json
        import hashlib as _hl
        payload_bytes = _json.dumps(self.canonical_binding_payload(), sort_keys=True, separators=(",", ":")).encode("utf-8")
        return "sha256:" + _hl.sha256(payload_bytes).hexdigest()

    def evaluate_invalidation(self, current_state: Dict[str, Any]) -> Dict[str, Any]:
        """Compares current state against bound dimensions and returns detailed diagnostic report."""
        invalidated_dims: List[str] = []
        reason_codes: List[str] = []

        cur_opt = current_state.get("option_id") or current_state.get("selected_option_id")
        if cur_opt and cur_opt != self.option_id:
            invalidated_dims.append("option_id")
            reason_codes.append("APPROVAL_OPTION_CHANGED")

        cur_schema = current_state.get("schema_hash")
        if cur_schema and cur_schema != self.schema_hash:
            invalidated_dims.append("schema_hash")
            reason_codes.append("APPROVAL_SCHEMA_CHANGED")

        cur_rules = current_state.get("rules_hash")
        if cur_rules and self.rules_hash and cur_rules != self.rules_hash:
            invalidated_dims.append("rules_hash")
            reason_codes.append("APPROVAL_RULES_CHANGED")

        cur_find = current_state.get("finding_signature")
        if cur_find and self.finding_signature and cur_find != self.finding_signature:
            invalidated_dims.append("finding_signature")
            reason_codes.append("APPROVAL_FINDINGS_CHANGED")

        cur_gen = current_state.get("generator_version")
        if cur_gen and self.generator_version and cur_gen != self.generator_version:
            invalidated_dims.append("generator_version")
            reason_codes.append("APPROVAL_GENERATOR_VERSION_CHANGED")

        cur_snap = current_state.get("source_snapshot_id")
        if cur_snap and self.source_snapshot_id and cur_snap != self.source_snapshot_id:
            invalidated_dims.append("source_snapshot_id")
            reason_codes.append("APPROVAL_SOURCE_SNAPSHOT_CHANGED")

        return {
            "valid": len(invalidated_dims) == 0,
            "invalidated_dimensions": invalidated_dims,
            "reason_codes": reason_codes,
            "binding_hash": self.canonical_binding_hash(),
        }

    def is_valid(self, current_plan_hash: str, current_schema_hash: str, current_option_id: str) -> bool:
        return (
            self.plan_hash == current_plan_hash
            and self.schema_hash == current_schema_hash
            and self.option_id == current_option_id
        )

    def validate(
        self,
        current_option_id: str,
        current_schema_hash: str,
        current_plan_hash: Optional[str] = None,
        current_rules_hash: Optional[str] = None,
        current_finding_signature: Optional[str] = None,
        current_source_snapshot_id: Optional[str] = None,
    ) -> bool:
        check_dict: Dict[str, Any] = {
            "option_id": current_option_id,
            "schema_hash": current_schema_hash,
        }
        if current_plan_hash is not None:
            check_dict["plan_hash"] = current_plan_hash
        if current_rules_hash is not None:
            check_dict["rules_hash"] = current_rules_hash
        if current_finding_signature is not None:
            check_dict["finding_signature"] = current_finding_signature
        if current_source_snapshot_id is not None:
            check_dict["source_snapshot_id"] = current_source_snapshot_id

        eval_res = self.evaluate_invalidation(check_dict)
        return eval_res["valid"]


@dataclass(frozen=True)
class IntegrityBoundAuthorizationRecord:
    """PR 6 granular authorization record bound to a specific remediation unit and exact artifact."""
    remediation_unit_id: str
    action_id: str
    artifact_hash: str
    source_snapshot_id: str
    schema_hash: str
    rules_hash: str
    finding_signature: str
    decision: str = "authorized_for_promotion"  # authorized_for_promotion | manual_only | blocked | expired | invalidated | not_evaluated
    validated_environment: str = "staging_isolated"
    authorized_target_environment: str = "production"
    intended_target_environment: str = "production"
    production_execution_status: str = "not_authorized"
    promotion_eligible: bool = True
    automatic_promotion_eligible: bool = False
    manual_promotion_eligible: bool = True
    operator_signoff_required: bool = True
    requires_production_preflight: bool = True
    conditions: List[str] = field(default_factory=lambda: [
        "same artifact hash",
        "same schema hash",
        "same rules hash",
        "production preflight required",
    ])
    record_id: str = "auth_rec_01"
    issuer: str = "agent-dhara-runtime-authorizer"
    authorization_policy_version: str = "1.0.0"
    certificate_hash: str = ""
    signature_status: str = "not_signed"
    issued_at: str = "2026-09-07T13:00:00Z"
    expires_at: str = "2026-09-14T13:00:00Z"
    revocation_status: str = "active"  # active | revoked | expired
    revoked_at: Optional[str] = None
    revocation_reason: Optional[str] = None
    superseded_by: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "record_id": self.record_id,
            "issuer": self.issuer,
            "authorization_policy_version": self.authorization_policy_version,
            "remediation_unit_id": self.remediation_unit_id,
            "action_id": self.action_id,
            "artifact_hash": self.artifact_hash,
            "source_snapshot_id": self.source_snapshot_id,
            "schema_hash": self.schema_hash,
            "rules_hash": self.rules_hash,
            "finding_signature": self.finding_signature,
            "decision": self.decision,
            "validated_environment": self.validated_environment,
            "authorized_target_environment": self.authorized_target_environment,
            "intended_target_environment": self.intended_target_environment,
            "production_execution_status": self.production_execution_status,
            "promotion_eligible": self.promotion_eligible,
            "automatic_promotion_eligible": self.automatic_promotion_eligible,
            "manual_promotion_eligible": self.manual_promotion_eligible,
            "operator_signoff_required": self.operator_signoff_required,
            "requires_production_preflight": self.requires_production_preflight,
            "conditions": self.conditions,
            "certificate_hash": self.certificate_hash,
            "signature_status": self.signature_status,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "revocation_status": self.revocation_status,
            "revoked_at": self.revoked_at,
            "revocation_reason": self.revocation_reason,
            "superseded_by": self.superseded_by,
        }


class ActionCodeMismatchError(ValueError):
    """Raised when action IDs mismatch along the selection -> compilation -> artifact chain."""
    pass


def resolve_remediation_action(
    *,
    finding: Dict[str, Any],
    selected_option: Optional[Dict[str, Any]] = None,
    approval_status: Optional[str] = None,
    authority_source: Optional[str] = None,
) -> ResolvedRemediationAction:
    """Resolve an approved action specification from a finding and selected option."""
    issue_id = str(finding.get("id") or finding.get("issue_id") or f"{finding.get('dataset', 'global')}.{finding.get('column', '*')}.{finding.get('issue_type', 'unknown')}")
    dataset_id = str(finding.get("dataset") or finding.get("dataset_id") or "global")
    col_name = finding.get("column") or finding.get("column_name")
    rem_unit_id = finding.get("remediation_unit_id") or (f"{dataset_id}.{col_name}" if col_name else dataset_id)

    # If selected_option not provided, search in finding.supported_options
    if not selected_option and finding.get("supported_options"):
        target_opt_id = finding.get("selected_option_id")
        for opt in finding.get("supported_options", []):
            if opt.get("option_id") == target_opt_id or opt.get("id") == target_opt_id:
                selected_option = opt
                break
        if not selected_option and finding.get("supported_options"):
            selected_option = finding["supported_options"][0]

    opt_id = (
        (selected_option.get("option_id") or selected_option.get("id")) if selected_option else None
    ) or finding.get("selected_option_id") or "default"

    action_id = (
        (selected_option.get("action_id") or selected_option.get("action")) if selected_option else None
    ) or finding.get("mapped_action") or finding.get("action_id") or finding.get("action") or finding.get("suggested_action") or "keep_as_is"

    auth_src = authority_source or finding.get("authority_source") or "DETERMINISTIC_REGISTRY"
    appr_st = approval_status or finding.get("approval_status") or finding.get("lifecycle", {}).get("approval_status") or "pending"

    # Action semantics flags
    if action_id == "flag_only":
        preserves_raw = True
        retains_row = True
        nullifies_clean_value = False
        quarantines_row = False
        drops_row = False
        generator_id = "flag_only_generator"
    elif action_id == "nullify_clean_value":
        preserves_raw = True
        retains_row = True
        nullifies_clean_value = True
        quarantines_row = False
        drops_row = False
        generator_id = "nullify_clean_value_generator"
    elif action_id == "quarantine_row":
        preserves_raw = True
        retains_row = False
        nullifies_clean_value = False
        quarantines_row = True
        drops_row = False
        generator_id = "quarantine_row_generator"
    elif action_id in ("keep_as_is", "skip_etl", "noop"):
        preserves_raw = True
        retains_row = True
        nullifies_clean_value = False
        quarantines_row = False
        drops_row = False
        generator_id = None
    elif action_id in ("drop_row", "drop_rows"):
        preserves_raw = True
        retains_row = False
        nullifies_clean_value = False
        quarantines_row = False
        drops_row = True
        generator_id = "drop_rows_generator"
    else:
        preserves_raw = True
        retains_row = True
        nullifies_clean_value = False
        quarantines_row = False
        drops_row = False
        generator_id = f"{action_id}_generator"

    return ResolvedRemediationAction(
        issue_id=issue_id,
        remediation_unit_id=rem_unit_id,
        option_id=opt_id,
        action_id=action_id,
        dataset_id=dataset_id,
        column_name=col_name,
        authority_source=auth_src,
        approval_status=appr_st,
        preserves_raw=preserves_raw,
        retains_row=retains_row,
        nullifies_clean_value=nullifies_clean_value,
        quarantines_row=quarantines_row,
        drops_row=drops_row,
        generator_id=generator_id,
    )


def generate_aligned_remediation(
    resolved_action: ResolvedRemediationAction,
    capability: CapabilityEntry,
    dialect: str = "pyspark",
) -> GeneratedRemediationArtifact:
    """Generate deterministic remediation code adhering strictly to action-code contract invariants.
    Invariant 1: selected_option.action_id == compiled_step.action_id
    Invariant 2: compiled_step.action_id == generated_artifact.action_id
    If either fails, code generation stops immediately.
    """
    from agent.dq_recommendations_agent import get_pandas_code, get_pyspark_code, ACTION_OUTCOME_METADATA

    # Validate action against capability and policy
    validate_resolved_action(resolved_action, capability)

    is_provisional = False
    provisional_label = None
    if resolved_action.approval_status != "approved":
        if resolved_action.approval_status == "pending":
            is_provisional = True
            provisional_label = "Provisional code preview for the selected option. This transformation is not approved or authorized for execution."
        else:
            raise RemediationApprovalRequired(f"Cannot generate code: approval status is '{resolved_action.approval_status}'")

    action_id = resolved_action.action_id
    if action_id in ("keep_as_is", "skip_etl", "noop"):
        code = None
    elif dialect == "pyspark":
        code = get_pyspark_code(
            resolved_action.column_name,
            capability.canonical_issue_type,
            action_id=action_id,
        )
    elif dialect == "pandas":
        code = get_pandas_code(
            resolved_action.column_name,
            capability.canonical_issue_type,
            action_id=action_id,
        )
    else:
        code = None

    outcome_meta = ACTION_OUTCOME_METADATA.get(action_id, ACTION_OUTCOME_METADATA.get("keep_as_is", {}))

    import hashlib
    c_hash = hashlib.sha256(code.encode("utf-8")).hexdigest() if code else None
    art_status = "provisional" if is_provisional else ("approved" if resolved_action.approval_status == "approved" else "generated")
    art_id = f"art-{resolved_action.dataset_id}-{resolved_action.column_name or 'row'}-{resolved_action.action_id}".lower()

    artifact = GeneratedRemediationArtifact(
        action_id=action_id,
        dialect=dialect,
        code=code,
        outcome_metadata=outcome_meta,
        validation_status="not_started",
        is_provisional=is_provisional,
        provisional_label=provisional_label,
        artifact_id=art_id,
        recommendation_id=resolved_action.issue_id,
        selected_option_id=resolved_action.option_id,
        generator_id=resolved_action.generator_id,
        generator_version="2.4.1",
        approval_record_id=f"appr-{resolved_action.issue_id}" if resolved_action.approval_status == "approved" else None,
        code_hash=c_hash,
        artifact_status=art_status,
    )

    # Invariant enforcement: compiled action ID == generated artifact action ID
    if artifact.action_id != resolved_action.action_id:
        raise ActionCodeMismatchError(
            f"Artifact action ID '{artifact.action_id}' does not match compiled action ID '{resolved_action.action_id}'"
        )

    return artifact


# ---------------------------------------------------------------------------
# PR 4.5 Step 3: Composite Field Work Packages & Conflict Detection
# ---------------------------------------------------------------------------

class ConflictingRemediationActions(ValueError):
    """Raised when mutually incompatible remediation actions are combined for a single field."""
    pass


class TypeCompatibilityError(TypeError):
    """Raised when generator output type is incompatible with semantic column type."""
    pass


@dataclass(frozen=True)
class FieldRemediationWorkPackage:
    """Consolidated field-level work package grouping all findings on a single column."""
    work_package_id: str
    dataset_id: str
    column_name: str
    semantic_type: Optional[str]
    findings: List[Dict[str, Any]]
    resolved_actions: List[ResolvedRemediationAction]
    ordered_transforms: List[str]
    final_disposition_policy: str = "clean_xor_quarantine"
    preserves_raw: bool = True
    has_quarantine: bool = False
    has_nullification: bool = False
    is_provisional: bool = False
    code_hash: Optional[str] = None


def detect_field_action_conflicts(actions: List[ResolvedRemediationAction]) -> None:
    """Validate that a set of actions for the same field do not contradict each other."""
    if not actions:
        return
    action_ids = {a.action_id for a in actions}
    if "flag_only" in action_ids and "quarantine_row" in action_ids:
        raise ConflictingRemediationActions(
            "Conflict: 'flag_only' (retain row) and 'quarantine_row' (route to quarantine) cannot both apply to the same field."
        )
    if ("keep_as_is" in action_ids or "skip_etl" in action_ids) and len(action_ids - {"keep_as_is", "skip_etl", "noop"}) > 0:
        raise ConflictingRemediationActions(
            f"Conflict: no-op action ('keep_as_is'/'skip_etl') cannot be combined with active transformations ({action_ids - {'keep_as_is', 'skip_etl', 'noop'}})."
        )
    if any(a.drops_row for a in actions) and any(a.retains_row for a in actions):
        raise ConflictingRemediationActions(
            "Conflict: actions cannot simultaneously drop and retain rows."
        )


def validate_type_compatibility(
    semantic_type: Optional[str],
    action_id: str,
    output_dtype: Optional[str],
) -> None:
    """Enforce that the generator output type aligns with the column's semantic type."""
    if not semantic_type or not output_dtype:
        return
    sem = semantic_type.lower()
    out = output_dtype.lower()
    if any(k in sem for k in ["date", "timestamp", "time"]) and out in ("long", "int", "integer", "bigint", "float", "double"):
        raise TypeCompatibilityError(
            f"Type incompatibility: Date field with semantic type '{semantic_type}' cannot be cast to numeric '{output_dtype}'."
        )
    if any(k in sem for k in ["phone", "email", "identifier", "key", "code"]) and out in ("float", "double", "int", "long"):
        raise TypeCompatibilityError(
            f"Type incompatibility: Text/identifier field with semantic type '{semantic_type}' cannot be cast to numeric '{output_dtype}'."
        )
