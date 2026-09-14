"""
Planning, Codegen Authorization, and Execution Bridge.

Implements Integration 3 of the Business Rules Pipeline:
- Converts actionable rules from CanonicalRuleRegistry + RuleAssessmentEvidence into a ProposedPlan.
- Invariant: Condition-only rules (RuleAction.ASSERT) NEVER create transformation steps!
- Aggregates multi-effect interactions and derives directional PlanStepDependency edges.
- Authorizes proposed plans into an immutable AuthorizedPlan with engine binding.
- Generates cryptographically signed GeneratedArtifact bundles (Ed25519) and registers audit events.
- Enforces strict 10-step verification gate before executing any generated artifact.
"""
from __future__ import annotations

import base64
import hashlib
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Sequence, Set, Tuple

from cryptography.hazmat.primitives.asymmetric import ed25519
from pydantic import BaseModel, Field

from agent.rules.assessment_models import RuleAssessmentEvidence
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
)
from agent.rules.canonical import canonical_json, compute_canonical_hash
from agent.rules.conflict_resolver import (
    aggregate_compatibility_results,
    build_dependency_graph,
    resolve_semantic_effect_compatibility,
    topological_sort_steps,
)
from agent.rules.plan_models import (
    CompatibilityOutcome,
    DataEffect,
    DataEffectAccess,
    OrderingDirection,
    PlanStep,
    PlanStepDependency,
    PlannerInvariantError,
    ProposedPlan,
    ProposedPlanStatus,
    StepAuthorizationStatus,
)
from agent.rules.rule_models import (
    CanonicalRule,
    CanonicalRuleRegistry,
    RuleAction,
    RuleType,
)

logger = logging.getLogger("agent.rules.planning_bridge")


class InMemoryAuditEventStore:
    """In-memory store for tracking and verifying code-generation audit events."""
    def __init__(self) -> None:
        self._events: Dict[str, AuditEvent] = {}

    def record(self, event: AuditEvent) -> None:
        self._events[event.event_id] = event

    def get(self, audit_event_id: str) -> Optional[AuditEvent]:
        return self._events.get(audit_event_id)


class InMemoryPolicyStore:
    """Policy authorization store verifying step decisions."""
    def __init__(self) -> None:
        self._valid_decisions: Set[str] = set()

    def register_decision(self, decision_id: str) -> None:
        self._valid_decisions.add(decision_id)

    def is_valid_for_step(self, decision_id: str, step_hash: str, plan_id: str) -> bool:
        return decision_id in self._valid_decisions


class SimpleActionCatalogue:
    """Catalogue validating supported actions per target engine."""
    def __init__(self, supported_actions: Mapping[str, Set[str]]) -> None:
        # engine -> set of action names
        self.supported = supported_actions

    def validate_action(self, name: str, version: str, parameters: dict, target_engine: str) -> None:
        allowed = self.supported.get(target_engine, set())
        if name not in allowed:
            raise ExecutionGateError(f"Action '{name}' is not supported by target engine '{target_engine}'")


class SimpleRevocationStore:
    """Tracks revoked plan authorizations."""
    def __init__(self) -> None:
        self._revoked: Set[str] = set()

    def revoke(self, plan_id: str) -> None:
        self._revoked.add(plan_id)

    def is_revoked(self, plan_id: str) -> bool:
        return plan_id in self._revoked


def build_proposed_plan(
    registry: CanonicalRuleRegistry,
    assessment_evidence: Sequence[RuleAssessmentEvidence] = (),
    plan_id: Optional[str] = None,
) -> ProposedPlan:
    """
    Constructs a ProposedPlan from actionable canonical rules.
    CRITICAL INVARIANT: Condition-only rules (RuleAction.ASSERT) NEVER produce transformation steps!
    """
    active_plan_id = plan_id or f"plan-{uuid.uuid4().hex[:8]}"
    steps: List[PlanStep] = []
    step_effects: Dict[str, List[DataEffect]] = {}

    for rule_id, rule in registry.rules.items():
        mat = rule.material

        # INVARIANT: Condition-only assertions are skipped for transformation planning
        if mat.action == RuleAction.ASSERT:
            continue

        step_id = f"step-{len(steps) + 1}-{mat.action.value}"
        target_col = mat.scope.columns[0] if mat.scope.columns else "all"

        # Construct concrete data effect
        effect = DataEffect(
            access=DataEffectAccess.WRITE,
            resource_ref=mat.scope.dataset,
            column_scope=frozenset(mat.scope.columns),
            action=mat.action.value,
            effect_semantic_hash=compute_canonical_hash(mat.normalized_parameters),
            normalized_parameters_hash=compute_canonical_hash(mat.normalized_parameters),
            idempotent=(mat.action in (RuleAction.REPLACE, RuleAction.STANDARDIZE, RuleAction.IMPUTE)),
        )

        step = PlanStep(
            step_id=step_id,
            action=mat.action.value,
            action_version="1.0",
            parameters=dict(mat.normalized_parameters),
            source_rule_ids=(rule_id,),
            effects=(effect,),
            authorization_status=StepAuthorizationStatus.PENDING,
            policy_decision_id=f"pol-dec-{step_id}",
        )
        steps.append(step)
        step_effects[step_id] = [effect]

    # Resolve pairwise compatibility and generate ordering dependencies
    dependencies: List[PlanStepDependency] = []
    for i in range(len(steps)):
        for j in range(i + 1, len(steps)):
            s_a, s_b = steps[i], steps[j]
            e_a_list, e_b_list = step_effects[s_a.step_id], step_effects[s_b.step_id]

            # Pairwise aggregation across effects
            pair_results = [
                resolve_semantic_effect_compatibility(ea, eb)
                for ea in e_a_list
                for eb in e_b_list
            ]
            agg_result = aggregate_compatibility_results(pair_results)

            if agg_result.outcome == CompatibilityOutcome.MUTUALLY_EXCLUSIVE:
                raise PlannerInvariantError(
                    f"Mutually exclusive conflict between steps {s_a.step_id} and {s_b.step_id}: {agg_result.reason_code}"
                )
            elif agg_result.outcome == CompatibilityOutcome.UNRESOLVED:
                raise PlannerInvariantError(
                    f"Unresolved conflict between steps {s_a.step_id} and {s_b.step_id}: {agg_result.reason_code}"
                )
            elif agg_result.outcome == CompatibilityOutcome.ORDERED_COMPOSITION:
                if agg_result.ordering == OrderingDirection.A_BEFORE_B:
                    dependencies.append(PlanStepDependency(before_step_id=s_a.step_id, after_step_id=s_b.step_id))
                elif agg_result.ordering == OrderingDirection.B_BEFORE_A:
                    dependencies.append(PlanStepDependency(before_step_id=s_b.step_id, after_step_id=s_a.step_id))

    # Verify DAG integrity and absence of cycles
    build_dependency_graph(tuple(steps), tuple(dependencies))
    topological_sort_steps(tuple(steps), tuple(dependencies))

    return ProposedPlan(
        plan_id=active_plan_id,
        status=ProposedPlanStatus.READY_FOR_AUTHORIZATION,
        steps=tuple(steps),
        dependencies=tuple(dependencies),
    )


def authorize_proposed_plan(
    proposed_plan: ProposedPlan,
    registry: CanonicalRuleRegistry,
    authorized_by: str = "security_lead",
    authorized_target_engines: FrozenSet[str] = frozenset({"duckdb", "pyspark"}),
    validity_duration: timedelta = timedelta(hours=2),
    policy_version: str = "1.0",
    catalogue_version: str = "1.0",
    policy_store: Optional[InMemoryPolicyStore] = None,
) -> AuthorizedPlan:
    """
    Validates ProposedPlan readiness and promotes it into an immutable AuthorizedPlan.
    """
    if proposed_plan.status != ProposedPlanStatus.READY_FOR_AUTHORIZATION:
        raise ExecutionGateError(f"Cannot authorize plan with status '{proposed_plan.status.value}'")

    now = datetime.now(timezone.utc)
    plan_policy_decision_id = f"pol-{proposed_plan.plan_id}"

    # Promote step authorization statuses and register policy decisions
    authorized_steps: List[PlanStep] = []
    for step in proposed_plan.steps:
        auth_step = step.model_copy(update={"authorization_status": StepAuthorizationStatus.AUTHORIZED})
        authorized_steps.append(auth_step)
        if policy_store and auth_step.policy_decision_id:
            policy_store.register_decision(auth_step.policy_decision_id)

    auth_proposed_plan = proposed_plan.model_copy(update={"steps": tuple(authorized_steps)})

    auth_record = PlanAuthorizationRecord(
        status=PlanAuthorizationStatus.AUTHORIZED,
        authorized_by=authorized_by,
        authorized_at=now,
        expires_at=now + validity_duration,
        policy_decision_id=plan_policy_decision_id,
    )

    return AuthorizedPlan(
        plan_id=proposed_plan.plan_id,
        plan_hash=auth_proposed_plan.compute_hash(),
        canonical_registry_hash=registry.compute_hash(),
        policy_version=policy_version,
        catalogue_version=catalogue_version,
        authorized_target_engines=authorized_target_engines,
        proposed_plan=auth_proposed_plan,
        authorization=auth_record,
    )


def generate_authorized_artifact(
    plan: AuthorizedPlan,
    registry: CanonicalRuleRegistry,
    target_engine: str,
    tenant_id: str,
    signing_private_key: ed25519.Ed25519PrivateKey,
    signing_key_id: str,
    audit_store: InMemoryAuditEventStore,
    codegen_version: str = "1.0",
    execution_environment: str = "production",
    code_content_override: Optional[bytes] = None,
) -> GeneratedArtifact:
    """
    Compiles code for an AuthorizedPlan and cryptographically signs a GeneratedArtifactManifest.
    """
    # 1. Generate executable code bytes
    raw_code_bytes = code_content_override or (
        f"# Generated Agent Dhara ETL Script for {target_engine}\n"
        f"# Plan ID: {plan.plan_id}\n"
        f"def execute_pipeline():\n"
        f"    print('Executing authorized ETL steps for plan {plan.plan_id}')\n"
    ).encode("utf-8")

    code_hash = hashlib.sha256(raw_code_bytes).hexdigest()
    artifact_id = f"art-{uuid.uuid4().hex[:12]}"
    audit_event_id = f"aud-{uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc)

    # 2. Construct unsigned manifest
    manifest = GeneratedArtifactManifest(
        manifest_version="1.0",
        artifact_id=artifact_id,
        tenant_id=tenant_id,
        artifact_hash=code_hash,
        source_plan_id=plan.plan_id,
        source_plan_hash=plan.plan_hash,
        source_registry_hash=plan.canonical_registry_hash,
        policy_decision_id=plan.authorization.policy_decision_id,
        policy_version=plan.policy_version,
        catalogue_version=plan.catalogue_version,
        target_engine=target_engine,
        execution_environment=execution_environment,
        codegen_version=codegen_version,
        created_at=now,
        expires_at=plan.authorization.expires_at,
        code_generated_audit_event_id=audit_event_id,
        signing_algorithm="Ed25519",
        signing_key_id=signing_key_id,
        manifest_signature="",
    )

    # 3. Cryptographically sign canonical JSON payload
    payload_bytes = manifest.compute_signing_payload()
    sig_bytes = signing_private_key.sign(payload_bytes)
    manifest.manifest_signature = base64.b64encode(sig_bytes).decode("utf-8")

    # 4. Record audit event in audit store
    audit_event = AuditEvent(
        event_id=audit_event_id,
        tenant_id=tenant_id,
        plan_id=plan.plan_id,
        artifact_id=artifact_id,
        event_type="CODE_GENERATION_SUCCESS",
        created_at=now,
    )
    audit_store.record(audit_event)

    return GeneratedArtifact(manifest=manifest, artifact_bytes=raw_code_bytes)


def execute_authorized_artifact(
    artifact: GeneratedArtifact,
    plan: AuthorizedPlan,
    registry: CanonicalRuleRegistry,
    current_tenant_id: str,
    target_engine: str,
    key_record: VerificationKeyRecord,
    audit_store: InMemoryAuditEventStore,
    policy_store: InMemoryPolicyStore,
    catalogue: SimpleActionCatalogue,
    revocation_store: SimpleRevocationStore,
) -> Dict[str, Any]:
    """
    Executes an artifact through the complete 10-step cryptographic and authorization gate.
    """
    validator_kwargs = {
        "current_policy_version": plan.policy_version,
        "current_catalogue_version": plan.catalogue_version,
        "policy_store": policy_store,
        "catalogue": catalogue,
        "revocation_store": revocation_store,
    }

    return verify_and_execute_artifact(
        artifact=artifact,
        plan=plan,
        registry=registry,
        current_tenant_id=current_tenant_id,
        target_engine=target_engine,
        key_record=key_record,
        audit_store=audit_store,
        validator_kwargs=validator_kwargs,
    )
