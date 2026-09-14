"""
Tests for agent.rules.assessment_models: population partitions, zero-row edge cases,
dual operator/population completeness, and evidence generation.
"""
from datetime import datetime, timezone
import pytest

from agent.rules.assessment_models import (
    AssessmentAccountingError,
    EvaluationCompleteness,
    EvaluationPopulationCounts,
    calculate_completeness_metrics,
    RuleAssessmentEvidence,
)
from agent.rules.rule_models import DataRuleOutcome


def test_valid_population_partition():
    # 1000 total = 900 pass + 50 viol + 20 null + 10 op_unknown + 10 tech_excl + 10 out_of_scope
    counts = EvaluationPopulationCounts(
        total_source_rows=1000,
        pass_rows=900,
        violation_rows=50,
        null_unknown_rows=20,
        operator_unknown_rows=10,
        technical_excluded_rows=10,
        out_of_scope_rows=10,
    )
    assert counts.evaluated_rows == 950

    op_comp, pop_comp, op_pct, pop_pct = calculate_completeness_metrics(counts)
    # operator_candidates = 1000 - 10 - 10 - 20 = 960
    # operator_evaluated = 960 - 10 = 950
    # op_pct = (950 / 960) * 100 = 98.9583...
    assert op_comp == EvaluationCompleteness.PARTIAL
    assert abs(op_pct - (950 / 960 * 100)) < 1e-4

    # in_scope_population = 1000 - 10 = 990
    # evaluated_rows = 950
    # pop_pct = (950 / 990) * 100 = 95.9595...
    assert pop_comp == EvaluationCompleteness.PARTIAL
    assert abs(pop_pct - (950 / 990 * 100)) < 1e-4


from pydantic import ValidationError


def test_invalid_partition_sum_raises():
    with pytest.raises((AssessmentAccountingError, ValidationError), match="does not equal total_source_rows"):
        EvaluationPopulationCounts(
            total_source_rows=100,
            pass_rows=80,
            violation_rows=10,
            null_unknown_rows=0,
            operator_unknown_rows=0,
            technical_excluded_rows=0,
            out_of_scope_rows=0,  # sum is 90 != 100
        )


def test_complete_evaluation_metrics():
    counts = EvaluationPopulationCounts(
        total_source_rows=500,
        pass_rows=480,
        violation_rows=20,
        null_unknown_rows=0,
        operator_unknown_rows=0,
        technical_excluded_rows=0,
        out_of_scope_rows=0,
    )
    op_comp, pop_comp, op_pct, pop_pct = calculate_completeness_metrics(counts)
    assert op_comp == EvaluationCompleteness.COMPLETE
    assert pop_comp == EvaluationCompleteness.COMPLETE
    assert op_pct == 100.0
    assert pop_pct == 100.0


def test_zero_row_empty_dataset_handling():
    # Empty dataset should NOT divide by zero or raise
    counts = EvaluationPopulationCounts(
        total_source_rows=0,
        pass_rows=0,
        violation_rows=0,
        null_unknown_rows=0,
        operator_unknown_rows=0,
        technical_excluded_rows=0,
        out_of_scope_rows=0,
    )
    op_comp, pop_comp, op_pct, pop_pct = calculate_completeness_metrics(counts)
    assert op_comp == EvaluationCompleteness.NOT_EVALUATED
    assert pop_comp == EvaluationCompleteness.NOT_EVALUATED
    assert op_pct is None
    assert pop_pct is None


def test_evidence_creation_and_hash():
    counts = EvaluationPopulationCounts(
        total_source_rows=100,
        pass_rows=90,
        violation_rows=10,
        null_unknown_rows=0,
        operator_unknown_rows=0,
        technical_excluded_rows=0,
        out_of_scope_rows=0,
    )
    ev = RuleAssessmentEvidence.create(
        evidence_id="ev-123",
        rule_id="rule-abc",
        counts=counts,
        evaluated_at=datetime(2026, 9, 14, 12, 0, 0, tzinfo=timezone.utc),
        execution_duration_ms=12.5,
    )
    assert ev.data_rule_outcome == DataRuleOutcome.FAIL  # violation_rows > 0
    h1 = ev.compute_evidence_hash()
    assert isinstance(h1, str)
    assert len(h1) == 64  # SHA-256 hex
