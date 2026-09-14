"""
Tests for agent.rules.planning_bridge:
Building ProposedPlan from actionable rules, condition-only rule exclusion,
plan authorization promotion, Ed25519 artifact signing, and end-to-end execution gating.
"""
from datetime import datetime, timedelta, timezone
import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519

from agent.rules.authorization_gate import ExecutionGateError, VerificationKeyRecord
from agent.rules.canonical import canonical_json, compute_canonical_hash
from agent.rules.plan_models import ProposedPlanStatus
from agent.rules.planning_bridge import (
    InMemoryAuditEventStore,
    InMemoryPolicyStore,
    SimpleActionCatalogue,
    SimpleRevocationStore,
    authorize_proposed_plan,
    build_proposed_plan,
    execute_authorized_artifact,
    generate_authorized_artifact,
)
from agent.rules.rule_models import (
    CanonicalExpression,
    CanonicalRule,
    CanonicalRuleMaterial,
    CanonicalRuleRegistry,
    ColumnOperand,
    LiteralOperand,
    RuleAction,
    RuleScope,
    RuleType,
    build_rule_id,
)


@pytest.fixture
def keypair():
    priv = ed25519.Ed25519PrivateKey.generate()
    pub = priv.public_key()
    return priv, pub


@pytest.fixture
def mixed_registry():
    # 1 condition-only rule (ASSERT)
    mat_assert = CanonicalRuleMaterial(
        rule_type=RuleType.VALIDATION_ASSERTION,
        expression=CanonicalExpression(
            operator=">=",
            operands=(ColumnOperand(column="Age"), LiteralOperand(value=18, data_type="int")),
        ),
        scope=RuleScope(dataset="customers", columns=("Age",)),
        action=RuleAction.ASSERT,
    )
    r_id_assert = build_rule_id(mat_assert)
    rule_assert = CanonicalRule(
        rule_id=r_id_assert,
        logical_rule_id="age_assert",
        rule_version=1,
        material=mat_assert,
    )

    # 1 actionable rule (REPLACE)
    mat_replace = CanonicalRuleMaterial(
        rule_type=RuleType.TRANSFORMATION,
        expression=CanonicalExpression(
            operator="==",
            operands=(ColumnOperand(column="Status"), LiteralOperand(value="A", data_type="string")),
        ),
        scope=RuleScope(dataset="customers", columns=("Status",)),
        action=RuleAction.REPLACE,
        normalized_parameters={"target_value": "Active"},
    )
    r_id_replace = build_rule_id(mat_replace)
    rule_replace = CanonicalRule(
        rule_id=r_id_replace,
        logical_rule_id="status_replace",
        rule_version=1,
        material=mat_replace,
    )

    # 1 actionable rule (IMPUTE)
    mat_impute = CanonicalRuleMaterial(
        rule_type=RuleType.TRANSFORMATION,
        expression=CanonicalExpression(
            operator="IS_NULL",
            operands=(ColumnOperand(column="Income"),),
        ),
        scope=RuleScope(dataset="customers", columns=("Income",)),
        action=RuleAction.IMPUTE,
        normalized_parameters={"strategy": "median"},
    )
    r_id_impute = build_rule_id(mat_impute)
    rule_impute = CanonicalRule(
        rule_id=r_id_impute,
        logical_rule_id="income_impute",
        rule_version=1,
        material=mat_impute,
    )

    reg = CanonicalRuleRegistry(
        rules={
            r_id_assert: rule_assert,
            r_id_replace: rule_replace,
            r_id_impute: rule_impute,
        }
    )
    return reg


def test_build_proposed_plan_excludes_condition_only_rules(mixed_registry):
    # Total rules in registry = 3 (1 ASSERT, 1 REPLACE, 1 IMPUTE)
    plan = build_proposed_plan(mixed_registry)
    # INVARIANT: ASSERT rule must NOT create a transformation step!
    assert len(plan.steps) == 2
    step_actions = [s.action for s in plan.steps]
    assert "assert" not in step_actions
    assert "replace" in step_actions
    assert "impute" in step_actions
    assert plan.status == ProposedPlanStatus.READY_FOR_AUTHORIZATION


def test_end_to_end_planning_signing_and_execution_flow(mixed_registry, keypair):
    priv_key, pub_key = keypair
    tenant_id = "tenant-gamma"
    signing_key_id = "key-gamma-1"

    audit_store = InMemoryAuditEventStore()
    policy_store = InMemoryPolicyStore()
    revocation_store = SimpleRevocationStore()
    catalogue = SimpleActionCatalogue({
        "duckdb": {"replace", "impute", "quarantine", "drop"},
        "pyspark": {"replace", "impute", "quarantine", "drop"},
    })

    key_record = VerificationKeyRecord(
        key_id=signing_key_id,
        tenant_id=tenant_id,
        algorithm="Ed25519",
        valid_from=datetime.now(timezone.utc) - timedelta(days=1),
        valid_to=datetime.now(timezone.utc) + timedelta(days=30),
        public_key=pub_key,
    )

    # 1. Build Proposed Plan
    prop_plan = build_proposed_plan(mixed_registry, plan_id="plan-test-1")
    assert prop_plan.status == ProposedPlanStatus.READY_FOR_AUTHORIZATION

    # 2. Authorize Plan
    auth_plan = authorize_proposed_plan(
        proposed_plan=prop_plan,
        registry=mixed_registry,
        authorized_by="security_admin",
        authorized_target_engines=frozenset({"duckdb", "pyspark"}),
        policy_store=policy_store,
    )
    assert auth_plan.plan_id == prop_plan.plan_id

    # 3. Generate Artifact & Sign Manifest
    artifact = generate_authorized_artifact(
        plan=auth_plan,
        registry=mixed_registry,
        target_engine="duckdb",
        tenant_id=tenant_id,
        signing_private_key=priv_key,
        signing_key_id=signing_key_id,
        audit_store=audit_store,
    )
    assert artifact.manifest.signing_algorithm == "Ed25519"
    assert artifact.manifest.target_engine == "duckdb"

    # 4. Execute Artifact through 10-step Execution Gate
    exec_res = execute_authorized_artifact(
        artifact=artifact,
        plan=auth_plan,
        registry=mixed_registry,
        current_tenant_id=tenant_id,
        target_engine="duckdb",
        key_record=key_record,
        audit_store=audit_store,
        policy_store=policy_store,
        catalogue=catalogue,
        revocation_store=revocation_store,
    )
    assert exec_res["status"] == "SUCCESS"
    assert exec_res["engine"] == "duckdb"


def test_revoked_plan_blocked_at_execution(mixed_registry, keypair):
    priv_key, pub_key = keypair
    tenant_id = "tenant-gamma"
    signing_key_id = "key-gamma-1"

    audit_store = InMemoryAuditEventStore()
    policy_store = InMemoryPolicyStore()
    revocation_store = SimpleRevocationStore()
    catalogue = SimpleActionCatalogue({"duckdb": {"replace", "impute"}})

    key_record = VerificationKeyRecord(
        key_id=signing_key_id,
        tenant_id=tenant_id,
        algorithm="Ed25519",
        valid_from=datetime.now(timezone.utc) - timedelta(days=1),
        public_key=pub_key,
    )

    prop_plan = build_proposed_plan(mixed_registry)
    auth_plan = authorize_proposed_plan(prop_plan, mixed_registry, policy_store=policy_store)
    artifact = generate_authorized_artifact(auth_plan, mixed_registry, "duckdb", tenant_id, priv_key, signing_key_id, audit_store)

    # Revoke plan authorization
    revocation_store.revoke(auth_plan.plan_id)

    with pytest.raises(ExecutionGateError, match="has been revoked"):
        execute_authorized_artifact(
            artifact=artifact,
            plan=auth_plan,
            registry=mixed_registry,
            current_tenant_id=tenant_id,
            target_engine="duckdb",
            key_record=key_record,
            audit_store=audit_store,
            policy_store=policy_store,
            catalogue=catalogue,
            revocation_store=revocation_store,
        )
