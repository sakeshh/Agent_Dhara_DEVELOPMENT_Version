"""
Tests for agent.rules.assessment_bridge:
Evaluating CanonicalRuleRegistry against DataFrames, strict 5-part accounting,
dual completeness, notes_assessment_mode handling, and legacy DQ issue projection.
"""
from datetime import datetime, timezone
import pandas as pd
import pytest

from agent.rules.assessment_bridge import evaluate_canonical_registry
from agent.rules.assessment_models import EvaluationCompleteness
from agent.rules.ingestion_bridge import ingest_business_rules
from agent.rules.rule_models import (
    AssessmentLifecycleStatus,
    CanonicalExpression,
    CanonicalRule,
    CanonicalRuleMaterial,
    CanonicalRuleRegistry,
    ColumnOperand,
    DataRuleOutcome,
    LiteralOperand,
    NullPolicy,
    RuleAction,
    RuleOccurrence,
    RuleScope,
    RuleType,
    build_rule_id,
)


@pytest.fixture
def sample_dataframe():
    data = {
        "CustomerID": ["C001", "C002", "C003", None, "C005"],
        "Age": [25, 17, 40, 15, None],
        "Status": ["Active", "Inactive", "Pending", "Active", "Active"],
    }
    return pd.DataFrame(data)


def test_evaluate_canonical_rules_against_dataframe(sample_dataframe):
    raw_rules = {
        "required_columns": ["CustomerID"],
        "valid_values": {"Status": ["Active", "Inactive"]},
        "custom_assertions": [
            {"assertion": "Age >= 18"},
        ],
    }
    ingest_res = ingest_business_rules(raw_rules, dataset_name="customers")
    reg = ingest_res.registry

    eval_res = evaluate_canonical_registry(
        registry=reg,
        datasets={"customers": sample_dataframe},
        notes_assessment_mode="pre_assessment",
    )

    assert len(eval_res.evidence_records) == 3
    # Age >= 18:
    # 5 rows total: 25(pass), 17(viol), 40(pass), 15(viol), None(null_unknown under SEPARATE_VIOLATION)
    # pass=2, viol=2, null_unk=1, total=5
    age_evidence = next(e for e in eval_res.evidence_records if "Age" in str(reg.rules[e.rule_id].material.scope.columns))
    assert age_evidence.counts.total_source_rows == 5
    assert age_evidence.counts.pass_rows == 2
    assert age_evidence.counts.violation_rows == 2
    assert age_evidence.counts.null_unknown_rows == 1
    assert age_evidence.data_rule_outcome == DataRuleOutcome.FAIL

    # Legacy projection produced for failed rules
    assert "customers" in eval_res.legacy_dq_issues
    issues = eval_res.legacy_dq_issues["customers"]
    assert any(i["column"] == "Age" and i["count"] == 2 for i in issues)


def test_notes_assessment_mode_planning_only(sample_dataframe):
    # Create a rule whose occurrence source_type is "notes"
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.VALIDATION_ASSERTION,
        expression=CanonicalExpression(
            operator=">=",
            operands=(ColumnOperand(column="Age"), LiteralOperand(value=21, data_type="int")),
        ),
        scope=RuleScope(dataset="customers", columns=("Age",)),
        action=RuleAction.ASSERT,
    )
    r_id = build_rule_id(mat)
    occ = RuleOccurrence(
        occurrence_id="occ-note-1",
        canonical_rule_id=r_id,
        source_type="notes",
        source_reference="note_line_1",
        authority_rank=10,
        is_explicit=True,
        observed_at=datetime.now(timezone.utc),
    )
    rule = CanonicalRule(rule_id=r_id, logical_rule_id="note_age_rule", rule_version=1, material=mat, source_occurrences=("occ-note-1",))
    reg = CanonicalRuleRegistry(rules={r_id: rule}, occurrences={"occ-note-1": occ})

    # When notes_assessment_mode is "planning_only", rule must NOT be evaluated and violation_count must be None
    eval_res = evaluate_canonical_registry(
        registry=reg,
        datasets={"customers": sample_dataframe},
        notes_assessment_mode="planning_only",
    )
    assert len(eval_res.evidence_records) == 0
    assert len(eval_res.not_evaluated_rules) == 1
    not_eval = eval_res.not_evaluated_rules[0]
    assert not_eval["assessment_status"] == AssessmentLifecycleStatus.NOT_EVALUATED.value
    assert not_eval["violation_count"] is None  # CRITICAL: NEVER 0!
    assert "planning_only mode" in not_eval["reason"]


def test_notes_assessment_mode_pre_assessment(sample_dataframe):
    # Under "pre_assessment", the same note rule IS evaluated against data
    mat = CanonicalRuleMaterial(
        rule_type=RuleType.VALIDATION_ASSERTION,
        expression=CanonicalExpression(
            operator=">=",
            operands=(ColumnOperand(column="Age"), LiteralOperand(value=21, data_type="int")),
        ),
        scope=RuleScope(dataset="customers", columns=("Age",)),
        action=RuleAction.ASSERT,
    )
    r_id = build_rule_id(mat)
    occ = RuleOccurrence(
        occurrence_id="occ-note-1",
        canonical_rule_id=r_id,
        source_type="notes",
        source_reference="note_line_1",
        authority_rank=10,
        is_explicit=True,
        observed_at=datetime.now(timezone.utc),
    )
    rule = CanonicalRule(rule_id=r_id, logical_rule_id="note_age_rule", rule_version=1, material=mat, source_occurrences=("occ-note-1",))
    reg = CanonicalRuleRegistry(rules={r_id: rule}, occurrences={"occ-note-1": occ})

    eval_res = evaluate_canonical_registry(
        registry=reg,
        datasets={"customers": sample_dataframe},
        notes_assessment_mode="pre_assessment",
    )
    assert len(eval_res.evidence_records) == 1
    ev = eval_res.evidence_records[0]
    assert ev.counts.total_source_rows == 5
    assert ev.data_rule_outcome == DataRuleOutcome.FAIL


def test_load_and_profile_with_canonical_business_rules(sample_dataframe):
    from agent.profiling.assessment_orchestrator import load_and_profile

    raw_rules = {
        "required_columns": ["CustomerID"],
        "valid_values": {"Status": ["Active", "Inactive"]},
    }
    source_cfg = {"locations": []}
    out = load_and_profile(
        source_cfg=source_cfg,
        additional_data={"customers": sample_dataframe},
        business_rules=raw_rules,
    )

    # Verify canonical_assessment bundle attached
    assert "canonical_assessment" in out
    canonical_bundle = out["canonical_assessment"]
    assert canonical_bundle is not None
    assert len(canonical_bundle["evidence_records"]) == 2
    assert canonical_bundle["summary"]["total_rules_evaluated"] == 2
