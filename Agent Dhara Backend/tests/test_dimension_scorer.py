"""
Test suite for Sprint 8: Dimension-Based Quality Scoring & Readiness Gating Overrides.
"""

import pytest
from agent.profiling.rule_status import RuleStatus, RuleEvaluationResult
from agent.profiling.dimension_scorer import calculate_dimension_quality_scores


def test_dimension_scoring_calculation():
    r1 = RuleEvaluationResult(
        rule_id="null_check",
        status=RuleStatus.PASS,
        dataset="users",
        column="id",
        dimension="completeness",
        evaluated_count=100,
        failed_count=0
    )
    r2 = RuleEvaluationResult(
        rule_id="email_format",
        status=RuleStatus.FAIL,
        dataset="users",
        column="email",
        dimension="validity",
        evaluated_count=100,
        failed_count=20
    )

    res = calculate_dimension_quality_scores([r1, r2], minimum_score_coverage=0.0)

    assert res.dimension_scores["completeness"].score == 100.0
    assert res.dimension_scores["validity"].score == 80.0
    assert res.quality_score > 0.0
    assert res.readiness == "READY"


def test_gating_override_blocked():
    r1 = RuleEvaluationResult(
        rule_id="null_check",
        status=RuleStatus.PASS,
        dataset="users",
        column="id",
        dimension="completeness",
        evaluated_count=100,
        failed_count=0
    )

    res = calculate_dimension_quality_scores([r1], has_missing_primary_key=True, minimum_score_coverage=0.0)

    assert res.readiness == "BLOCKED"
    assert any("Missing primary key" in b for b in res.gating_blockers)
