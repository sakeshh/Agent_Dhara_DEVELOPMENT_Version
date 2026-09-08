"""
Engine 3.0 Production Runtime Fixes Test Suite.
Validates relationship gating invariants, zero-evaluated dimension null scoring,
score coverage thresholds, context-aware placeholders, universal semantic guards,
and reproducibility metadata.
"""

import pytest
import pandas as pd
from typing import Dict, List, Any

from agent.profiling.rule_status import RuleStatus, RuleEvaluationResult
from agent.profiling.dimension_scorer import (
    calculate_dimension_quality_scores,
    DimensionScoreBreakdown,
    OverallQualityScore,
)
from agent.profiling.relationship_engine import (
    RelationshipResult,
    RelationshipEvaluationState,
    RELATIONSHIP_GATING_MAP,
)
from agent.profiling.placeholder_detector import (
    classify_placeholder_value,
    classify_character_unicode,
)
from agent.profiling.rule_engine import QualityRule, QualityRuleContext


# ── 1. Relationship Gating Invariants & Scope Filtering ────────────────────────

def test_relationship_gating_map_invariants():
    """Verify NOT_EVALUATED, CONFIGURATION_ERROR, EXECUTION_ERROR produce UNKNOWN gating."""
    res_not_eval = RelationshipResult(
        parent_dataset="customers",
        parent_column="id",
        child_dataset="orders",
        child_column="customer_id",
        status=RelationshipEvaluationState.NOT_EVALUATED,
    )
    assert res_not_eval.gating_result == "UNKNOWN"

    res_cfg_err = RelationshipResult(
        parent_dataset="customers",
        parent_column="id",
        child_dataset="orders",
        child_column="customer_id",
        status=RelationshipEvaluationState.CONFIGURATION_ERROR,
    )
    assert res_cfg_err.gating_result == "UNKNOWN"

    res_exec_err = RelationshipResult(
        parent_dataset="customers",
        parent_column="id",
        child_dataset="orders",
        child_column="customer_id",
        status=RelationshipEvaluationState.EXECUTION_ERROR,
    )
    assert res_exec_err.gating_result == "UNKNOWN"


def test_partial_pass_relationship_gating():
    """PARTIAL_PASS yields PARTIAL_PASS gating result."""
    res_partial = RelationshipResult(
        parent_dataset="customers",
        parent_column="id",
        child_dataset="orders",
        child_column="customer_id",
        status=RelationshipEvaluationState.PARTIAL_PASS,
        normalized_match_rate=0.92,
    )
    assert res_partial.gating_result == "PARTIAL_PASS"


# ── 2. Zero-Evaluated Dimension Scoring & Coverage ─────────────────────────────

def test_zero_evaluated_dimension_null_score():
    """Dimensions with 0 evaluated rules yield score=None and effective_weight=0.0."""
    rule1 = RuleEvaluationResult(
        rule_id="validity_check",
        rule_version="3.0.0",
        status=RuleStatus.PASS,
        dataset="test_ds",
        column="col1",
        dimension="validity",
        evaluated_count=100,
        failed_count=0,
    )

    score_result = calculate_dimension_quality_scores([rule1])
    assert score_result.dimension_scores["validity"].score == 100.0
    assert score_result.dimension_scores["validity"].status == "EVALUATED"

    # Completeness had 0 evaluated rules
    assert score_result.dimension_scores["completeness"].score is None
    assert score_result.dimension_scores["completeness"].status == "NOT_EVALUATED"
    assert score_result.dimension_scores["completeness"].effective_weight == 0.0


def test_unevaluated_dimension_excluded_from_denominator():
    """Unevaluated dimensions do not skew the overall quality score denominator."""
    rule_validity = RuleEvaluationResult(
        rule_id="validity_check",
        rule_version="3.0.0",
        status=RuleStatus.FAIL,
        dataset="test_ds",
        column="col1",
        dimension="validity",
        evaluated_count=100,
        failed_count=50,  # 50% score
    )

    # Pass enough rules to hit high score coverage (>0.80)
    rules = [rule_validity]
    for dim in ["completeness", "uniqueness", "consistency", "conformity", "referential_integrity", "accuracy"]:
        rules.append(RuleEvaluationResult(
            rule_id=f"{dim}_check",
            rule_version="3.0.0",
            status=RuleStatus.PASS,
            dataset="test_ds",
            column="col1",
            dimension=dim,
            evaluated_count=100,
            failed_count=0,
        ))

    score_result = calculate_dimension_quality_scores(rules)
    assert score_result.dimension_scores["validity"].score == 50.0
    assert score_result.dimension_scores["timeliness"].score is None  # 0.05 weight excluded


def test_insufficient_score_coverage_gating():
    """Score coverage below 0.80 triggers readiness UNKNOWN."""
    only_validity_rule = RuleEvaluationResult(
        rule_id="validity_check",
        rule_version="3.0.0",
        status=RuleStatus.PASS,
        dataset="test_ds",
        column="col1",
        dimension="validity",  # weight 0.20
        evaluated_count=100,
        failed_count=0,
    )

    score_result = calculate_dimension_quality_scores([only_validity_rule], minimum_score_coverage=0.80)
    assert score_result.score_coverage == pytest.approx(0.20, abs=0.05)
    assert score_result.readiness == "UNKNOWN"
    assert any("INSUFFICIENT_SCORE_COVERAGE" in b for b in score_result.gating_blockers)


# ── 3. Context-Aware Placeholder & Unicode Classification ─────────────────────

@pytest.mark.parametrize(
    "value",
    ["c00001", "o000001", "p000001", "CUS-0001", "AB_123", "000123"],
)
def test_lowercase_and_formatted_identifiers_not_placeholders(value: str):
    """Structured identifier patterns are not misclassified as generic placeholders."""
    res = classify_placeholder_value(value, semantic_type="identifier")
    assert not res.is_placeholder
    assert res.specific_diagnosis in ("valid_identifier", "valid")


@pytest.mark.parametrize(
    "value",
    ["$1,000", "₹1,000.00", "EUR 1.000,50", "USD 100", "(1,200.25)"],
)
def test_monetary_currency_values_not_placeholders(value: str):
    """Formatted monetary values are not misclassified as placeholders."""
    res = classify_placeholder_value(value, semantic_type="currency_amount")
    assert not res.is_placeholder
    assert res.specific_diagnosis in ("formatted_currency_value", "valid")


def test_emoji_classified_as_symbol():
    """Emoji characters like 💳 are classified as SYMBOL, not PUNCTUATION."""
    cat = classify_character_unicode("💳")
    assert cat == "SYMBOL"


# ── 4. Universal Rule-Planner Semantic Guards ─────────────────────────────────

def test_country_field_blocks_numeric_rules():
    """Country and text columns block numeric rules via universal rule guards."""
    rule = QualityRule()
    rule.rule_id = "invalid_numeric"
    rule.rule_family = "invalid_numeric"

    ctx_country = QualityRuleContext(
        dataset_name="clients",
        df=pd.DataFrame({"country": ["India", "USA"]}),
        column_name="country",
        semantic_type="country",
    )

    assert not rule.is_applicable(ctx_country)


def test_near_duplicate_configurable_domains():
    """Ignored email domains can be configured to avoid artificial matching across corporate providers."""
    from agent.specialists.gx_validation_specialist import run_gx_validation

    df = pd.DataFrame({
        "customer_name": ["Robert Taylor", "Robert Taylor", "Sarah Jenkins"],
        "city": ["Chicago", "Chicago", "Dallas"],
        "email": ["rtaylor@acme-corp.com", "rtaylor@partner-corp.com", "sjenkins@partner-corp.com"],
    })
    
    thresholds = {
        "near_duplicate": {
            "enabled": True,
            "threshold": 0.90,
            "ignored_email_domains": ["acme-corp", "partner-corp"],
        }
    }
    res = run_gx_validation({"clients": df}, {}, thresholds=thresholds)
    proc = res.get("clients", {}).get("results_processed", [])
    nd_results = [r for r in proc if r.get("expectation") == "near_duplicate_rows"]
    assert len(nd_results) == 1
    # Robert Taylor rows match, while Sarah Jenkins is distinct (no single-linkage chaining)
    metrics = nd_results[0].get("metrics", {})
    assert metrics.get("duplicate_clusters_count") == 1
    assert nd_results[0].get("unexpected_count") == 1


def test_impossible_calendar_dates_detected():
    """Structurally valid strings like 2022-02-30 and 2023-13-01 fail calendar validation."""
    from agent.specialists.gx_validation_specialist import run_gx_validation

    df = pd.DataFrame({
        "birth_date": ["2022-02-30", "2023-13-01", "2021-05-15", "2020-01-01"],
    })
    res = run_gx_validation({"dates": df}, {})
    proc = res.get("dates", {}).get("results_processed", [])
    date_issues = [r for r in proc if r.get("expectation") in ("invalid_date_format", "invalid_calendar_date", "impossible_date")]
    assert len(date_issues) >= 1
    bad_dates_issue = [r for r in date_issues if "calendar" in r.get("details", "").lower() or r.get("expectation") in ("invalid_calendar_date", "impossible_date")]
    assert len(bad_dates_issue) >= 1
    assert bad_dates_issue[0]["unexpected_count"] == 2
