"""
Test suite for Sprint 1: Explicit 7-State Rule Execution and Coverage Tracking.
Verifies zero silent skips, explicit state assignment, and output model compliance.
"""

import pytest
import pandas as pd

from agent.profiling.rule_status import RuleStatus, RuleEvaluationResult, RuleCoverageSummary
from agent.profiling.rule_engine import QualityRule, QualityRuleContext, RulePlanner, RuleExecutor, RuleRegistry
from agent.models.assessment_result import AssessmentResultModel, DQIssue, DataQualitySummary


class DummyNullCheckRule(QualityRule):
    rule_id = "null_check"
    version = "3.0.0"
    dimension = "completeness"
    default_severity = "HIGH"

    def evaluate(self, context: QualityRuleContext) -> RuleEvaluationResult:
        if context.column_name is None:
            return RuleEvaluationResult(
                rule_id=self.rule_id,
                status=RuleStatus.NOT_APPLICABLE,
                dataset=context.dataset_name,
                dimension=self.dimension
            )

        col_data = context.df[context.column_name]
        null_mask = col_data.isna()
        failed_count = int(null_mask.sum())
        total_count = len(col_data)
        
        status = RuleStatus.FAIL if failed_count > 0 else RuleStatus.PASS
        affected_rows = col_data[null_mask].index.tolist()

        return RuleEvaluationResult(
            rule_id=self.rule_id,
            status=status,
            dataset=context.dataset_name,
            column=context.column_name,
            dimension=self.dimension,
            evaluated_count=total_count,
            failed_count=failed_count,
            failure_rate=failed_count / total_count if total_count > 0 else 0.0,
            affected_rows=affected_rows[:10]
        )


class DummyExceptionRule(QualityRule):
    rule_id = "faulty_rule"
    version = "3.0.0"
    dimension = "validity"

    def evaluate(self, context: QualityRuleContext) -> RuleEvaluationResult:
        raise ValueError("Simulated execution failure inside rule")


def test_rule_execution_states():
    df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "email": ["a@b.com", None, "c@d.com", "invalid"]
    })

    registry = RuleRegistry()
    registry.register(DummyNullCheckRule())
    registry.register(DummyExceptionRule())

    planner = RulePlanner(registry=registry)
    planned = planner.plan_rules_for_dataset("test_ds", df)
    
    executor = RuleExecutor()
    results, summary = executor.execute_plan(planned)

    assert summary.total_configured_rules == len(planned)
    assert summary.silent_failure_count == 0
    assert summary.coverage_percentage > 0.0

    statuses = {r.status for r in results}
    assert RuleStatus.PASS in statuses or RuleStatus.FAIL in statuses
    assert RuleStatus.EXECUTION_ERROR in statuses


def test_assessment_result_model_7state_fields():
    model = AssessmentResultModel()
    model.coverage = RuleCoverageSummary(
        total_configured_rules=10,
        evaluated_count=8,
        passed_count=6,
        failed_count=2,
        not_applicable_count=2,
        not_evaluated_count=0,
        coverage_percentage=100.0
    )

    issue = DQIssue(
        column="email",
        type="missing_values",
        severity="HIGH",
        rule_id="null_check",
        rule_status=RuleStatus.FAIL.value,
        dimension="completeness",
        count=1,
        message="1 null value found in email"
    )

    assert issue.rule_status == "FAIL"
    assert issue.dimension == "completeness"
    assert model.coverage.total_configured_rules == 10
    assert model.coverage.not_evaluated_count == 0
