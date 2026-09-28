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
        severity="LOW",
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


def test_translated_gx_results_missing_element_count_do_not_skew_dimension_scorer():
    """Verify that GX/custom results without element_count default to n_rows / unexp_count and do not produce negative scores."""
    from agent.profiling.gx_result_translator import translate_gx_result_list

    raw_custom_result = {
        "expectation": "whitespace",
        "column": "Status",
        "success": False,
        "unexpected_count": 15,
        # Notice element_count is deliberately omitted here, mimicking custom GX checks
    }

    translated = translate_gx_result_list([raw_custom_result], dataset_id="dbo.test", n_rows=24)
    assert len(translated) == 1
    assert translated[0].evaluated_count == 24
    assert translated[0].failed_count == 15

    # Run through dimension scorer
    score = calculate_dimension_quality_scores(translated, minimum_score_coverage=0.0)
    assert score is not None
    # 100 * (1 - 15/24) = 37.5
    assert score.dimension_scores["validity"].score == 37.5
    assert len(score.score_calculation_warnings) == 0

