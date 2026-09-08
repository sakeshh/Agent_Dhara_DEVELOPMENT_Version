"""
Unit tests for PR 4: Planning Safety Gates and Impact Threshold Enforcement.
Validates:
1. Low-risk standard findings under threshold compile to auto-fixed steps in datasets_steps.
2. Standard findings exceeding impact thresholds (>5% or >10k rows) are diverted to manual review.
3. Contextual and unknown findings cannot be auto-fixed and require human review.
4. LLM advisory suggestions never authorize automatic remediation without human approval.
"""
import pytest
from agent.etl_pipeline.issue_to_step_compiler import compile_issues_to_steps
from agent.etl_pipeline.remediation_router import (
    REASON_IMPACT_PERCENTAGE_EXCEEDED,
    REASON_IMPACT_ROWS_EXCEEDED,
    REASON_UNKNOWN_ISSUE_TYPE,
)


def test_standard_under_threshold_becomes_auto_step():
    """Standard whitespace on 200 rows (2%) becomes an auto-fixed step."""
    suggestions = [
        {
            "dataset": "Customers",
            "column": "name",
            "issue_type": "whitespace",
            "suggested_action": "trim",
            "severity": "low",
            "count": 200,
            "row_count_affected": 200,
            "affected_percentage": 2.0,
            "auto_fixable": True,
        }
    ]
    rules = {}
    sem_schema = {}

    steps, manual_review = compile_issues_to_steps(suggestions, rules, sem_schema)
    assert "Customers" in steps
    assert len(steps["Customers"]) == 1
    step = steps["Customers"][0]
    assert step["action"] == "trim"
    assert step["auto_fixable"] is True
    assert step["lifecycle"]["approval_status"] == "not_required"
    assert len(manual_review) == 0


def test_standard_exceeding_threshold_diverts_to_manual_review():
    """Standard whitespace on 60,000 rows (>50k limit) diverts to manual review."""
    suggestions = [
        {
            "dataset": "Transactions",
            "column": "description",
            "issue_type": "whitespace",
            "suggested_action": "trim",
            "severity": "low",
            "count": 60000,
            "row_count_affected": 60000,
            "affected_percentage": 12.0,
            "auto_fixable": True,  # Claimed auto_fixable, but gates must override!
        }
    ]
    rules = {}
    sem_schema = {}

    steps, manual_review = compile_issues_to_steps(suggestions, rules, sem_schema)
    # Must NOT be in auto steps
    assert len(steps.get("Transactions", [])) == 0
    # Must be in manual review
    assert len(manual_review) == 1
    mr = manual_review[0]
    assert mr["column"] == "description"
    assert mr["auto_fixable"] is False
    assert mr["lifecycle"]["approval_status"] == "pending"
    assert any("EXCEEDS_SAFETY_LIMIT" in code for code in mr["reason_codes"])


def test_unknown_issue_diverts_to_manual_review():
    """An unclassified issue type must never auto-fix."""
    suggestions = [
        {
            "dataset": "Telemetry",
            "column": "sensor_reading",
            "issue_type": "unregistered_frequency_jitter",
            "suggested_action": "trim",
            "severity": "medium",
            "count": 5,
            "affected_percentage": 0.1,
            "auto_fixable": True,  # Attempting auto-fix on unknown issue
        }
    ]
    rules = {}
    sem_schema = {}

    steps, manual_review = compile_issues_to_steps(suggestions, rules, sem_schema)
    assert len(steps.get("Telemetry", [])) == 0
    assert len(manual_review) == 1
    mr = manual_review[0]
    assert mr["auto_fixable"] is False
    assert mr["lifecycle"]["approval_status"] == "pending"
    assert REASON_UNKNOWN_ISSUE_TYPE in mr["reason_codes"]


def test_contextual_issue_diverts_to_manual_review():
    """Negative values and duplicates must divert to manual review with pending approval."""
    suggestions = [
        {
            "dataset": "Ledger",
            "column": "amount",
            "issue_type": "negative_values",
            "suggested_action": "flag_outliers",
            "severity": "high",
            "count": 15,
            "affected_percentage": 0.1,
            "auto_fixable": True,
        }
    ]
    rules = {}
    sem_schema = {}

    steps, manual_review = compile_issues_to_steps(suggestions, rules, sem_schema)
    assert len(steps.get("Ledger", [])) == 0
    assert len(manual_review) == 1
    mr = manual_review[0]
    assert mr["auto_fixable"] is False
    assert mr["lifecycle"]["approval_status"] == "pending"


def test_llm_advisory_suggestion_requires_approval():
    """LLM advisory suggestion never bypasses human review even with high confidence."""
    suggestions = [
        {
            "dataset": "Customers",
            "column": "email",
            "issue_type": "invalid_email",
            "suggested_action": "sanitize_email",
            "severity": "medium",
            "llm_recommendation": {
                "suggested_fix": "sanitize email",
                "mapped_action": "sanitize_email",
                "why_it_matters": "Clean emails",
            },
            "llm_confidence": 0.99,  # High LLM confidence!
            "auto_fixable": True,
        }
    ]
    rules = {}
    sem_schema = {}

    steps, manual_review = compile_issues_to_steps(suggestions, rules, sem_schema)
    assert len(steps.get("Customers", [])) == 0
    assert len(manual_review) == 1
    mr = manual_review[0]
    assert mr["auto_fixable"] is False
    assert mr["lifecycle"]["approval_status"] == "pending"
