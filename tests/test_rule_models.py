"""
Tests for agent.rules.rule_models: intake invariants, parallel lifecycle dimensions,
transition verification, AST normalization, and semantic rule identity.
"""
from datetime import datetime, timezone
import pytest

from agent.rules.rule_models import (
    IntakeDisposition,
    RuleIntakeRecord,
    RuleProcessingStage,
    AssessmentLifecycleStatus,
    DataRuleOutcome,
    PlanningLifecycleStatus,
    ExecutionLifecycleStatus,
    LifecycleEntityType,
    LifecycleTransition,
    validate_transition_chain,
    LifecycleTransitionInvariantError,
    RuleDispositionInvariantError,
    ExpressionArityError,
    ColumnOperand,
    LiteralOperand,
    CanonicalExpression,
    RuleType,
    RuleAction,
    NullPolicy,
    RuleScope,
    CanonicalRuleMaterial,
    CanonicalRule,
    RuleOccurrence,
    CanonicalRuleRegistry,
    build_rule_id,
    validate_intake_records,
)


def test_intake_validation_success():
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.VALIDATION_ASSERTION,
        expression=CanonicalExpression(
            operator=">",
            operands=(
                ColumnOperand(column="age", data_type="int"),
                LiteralOperand(value=18, data_type="int"),
            ),
        ),
        scope=RuleScope(dataset="customers", columns=("age",)),
        action=RuleAction.ASSERT,
    )
    rule_id = build_rule_id(mat)
    can_rule = CanonicalRule(
        rule_id=rule_id,
        logical_rule_id="cust-age-check",
        rule_version=1,
        material=mat,
    )

    registry = CanonicalRuleRegistry(
        rules={rule_id: can_rule},
        intake_records={
            "inp-1": RuleIntakeRecord(
                input_rule_id="inp-1",
                disposition=IntakeDisposition.INTERPRETED,
                reason_code="mapped_success",
                canonical_rule_id=rule_id,
            ),
            "inp-2": RuleIntakeRecord(
                input_rule_id="inp-2",
                disposition=IntakeDisposition.PRESERVED_UNMAPPED,
                reason_code="free_text_notes",
                canonical_rule_id=None,
            ),
        },
    )

    # Bi-directional match: exact input rules match records
    validate_intake_records({"inp-1", "inp-2"}, registry.intake_records, registry)


def test_intake_validation_missing_and_unexpected():
    registry = CanonicalRuleRegistry()
    # Missing record for inp-2
    with pytest.raises(RuleDispositionInvariantError, match="missing intake records"):
        validate_intake_records(
            {"inp-1", "inp-2"},
            {"inp-1": RuleIntakeRecord(input_rule_id="inp-1", disposition=IntakeDisposition.INVALID, reason_code="x")},
            registry,
        )

    # Unexpected record for inp-3
    with pytest.raises(RuleDispositionInvariantError, match="unknown input rules"):
        validate_intake_records(
            {"inp-1"},
            {
                "inp-1": RuleIntakeRecord(input_rule_id="inp-1", disposition=IntakeDisposition.INVALID, reason_code="x"),
                "inp-3": RuleIntakeRecord(input_rule_id="inp-3", disposition=IntakeDisposition.INVALID, reason_code="x"),
            },
            registry,
        )


def test_intake_interpreted_canonical_id_enforcement():
    registry = CanonicalRuleRegistry()
    # INTERPRETED missing canonical_rule_id
    with pytest.raises(RuleDispositionInvariantError, match="missing canonical_rule_id"):
        validate_intake_records(
            {"inp-1"},
            {"inp-1": RuleIntakeRecord(input_rule_id="inp-1", disposition=IntakeDisposition.INTERPRETED, reason_code="x")},
            registry,
        )

    # Non-interpreted with canonical_rule_id
    with pytest.raises(RuleDispositionInvariantError, match="must have null canonical_rule_id"):
        validate_intake_records(
            {"inp-1"},
            {"inp-1": RuleIntakeRecord(input_rule_id="inp-1", disposition=IntakeDisposition.REJECTED, reason_code="x", canonical_rule_id="some-id")},
            registry,
        )


def test_ast_normalization_commutative_sorting():
    # Commutative: "==" sorts operands deterministically
    expr1 = CanonicalExpression(
        operator="==",
        operands=(
            LiteralOperand(value="ACTIVE", data_type="string"),
            ColumnOperand(column="status", data_type="string"),
        ),
    )
    expr2 = CanonicalExpression(
        operator="EQ",  # alias for "=="
        operands=(
            ColumnOperand(column="status", data_type="string"),
            LiteralOperand(value="ACTIVE", data_type="string"),
        ),
    )

    norm1 = expr1.normalize()
    norm2 = expr2.normalize()
    assert norm1.operator == "=="
    assert norm2.operator == "=="
    # Both normalize to identical operand sequence
    assert norm1.model_dump() == norm2.model_dump()


def test_ast_arity_validation():
    # ">" requires exactly 2 operands
    bad_expr = CanonicalExpression(
        operator=">",
        operands=(ColumnOperand(column="age", data_type="int"),),
    )
    with pytest.raises(ExpressionArityError, match="requires at least 2"):
        bad_expr.normalize()


def test_semantic_rule_id_excludes_governance():
    mat1 = CanonicalRuleMaterial(
        rule_type=RuleType.VALIDATION_ASSERTION,
        expression=CanonicalExpression(
            operator=">=",
            operands=(
                ColumnOperand(column="salary", data_type="decimal"),
                LiteralOperand(value=50000, data_type="decimal"),
            ),
        ),
        scope=RuleScope(dataset="employees", columns=("salary",)),
        action=RuleAction.ASSERT,
    )

    # Same semantic material produces same rule_id regardless of logical_rule_id or version
    id1 = build_rule_id(mat1)
    id2 = build_rule_id(mat1)
    assert id1 == id2

    rule_v1 = CanonicalRule(rule_id=id1, logical_rule_id="sal-check", rule_version=1, material=mat1)
    rule_v2 = CanonicalRule(rule_id=id1, logical_rule_id="corp-sal-policy", rule_version=2, material=mat1)
    assert rule_v1.rule_id == rule_v2.rule_id


def test_lifecycle_transition_chain_validation():
    t1 = LifecycleTransition(
        transition_id="tr-1",
        correlation_id="corr-1",
        entity_type=LifecycleEntityType.RULE,
        entity_id="rule-100",
        dimension="assessment",
        sequence_number=1,
        from_state="pending",
        to_state="evaluated",
        transitioned_at=datetime(2026, 9, 14, 10, 0, 0, tzinfo=timezone.utc),
        actor_type="system",
        reason_code="assessment_completed",
        audit_event_id="aud-1",
    )
    t2 = LifecycleTransition(
        transition_id="tr-2",
        correlation_id="corr-1",
        entity_type=LifecycleEntityType.RULE,
        entity_id="rule-100",
        dimension="assessment",
        sequence_number=2,
        from_state="evaluated",
        to_state="post_validated",
        transitioned_at=datetime(2026, 9, 14, 10, 5, 0, tzinfo=timezone.utc),
        actor_type="system",
        reason_code="post_val_completed",
        audit_event_id="aud-2",
    )
    # Valid continuous chain
    validate_transition_chain((t1, t2))

    # Test discontinuous state: t2 expects from_state="pending"
    bad_t2 = t2.model_copy(update={"from_state": "pending"})
    with pytest.raises(LifecycleTransitionInvariantError, match="Discontinuous state transition"):
        validate_transition_chain((t1, bad_t2))

    # Test non-contiguous sequence: sequence 3 instead of 2
    bad_seq_t2 = t2.model_copy(update={"sequence_number": 3})
    with pytest.raises(LifecycleTransitionInvariantError, match="Non-contiguous sequence"):
        validate_transition_chain((t1, bad_seq_t2))


def test_independent_entities_do_not_collide():
    # Rule A and Rule B both having sequence 1 and 2 under same correlation
    t_a1 = LifecycleTransition(
        transition_id="tr-a1",
        correlation_id="corr-1",
        entity_type=LifecycleEntityType.RULE,
        entity_id="rule-A",
        dimension="assessment",
        sequence_number=1,
        from_state="pending",
        to_state="evaluated",
        transitioned_at=datetime(2026, 9, 14, 10, 0, 0, tzinfo=timezone.utc),
        actor_type="system",
        reason_code="a1",
        audit_event_id="aud-a1",
    )
    t_b1 = LifecycleTransition(
        transition_id="tr-b1",
        correlation_id="corr-1",
        entity_type=LifecycleEntityType.RULE,
        entity_id="rule-B",
        dimension="assessment",
        sequence_number=1,
        from_state="pending",
        to_state="evaluated",
        transitioned_at=datetime(2026, 9, 14, 10, 0, 0, tzinfo=timezone.utc),
        actor_type="system",
        reason_code="b1",
        audit_event_id="aud-b1",
    )
    # Both entities at sequence 1 should validate cleanly because of entity-scoping errata E2
    validate_transition_chain((t_a1, t_b1))
