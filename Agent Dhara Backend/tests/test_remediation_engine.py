"""
Test suite for Sprint 9: Remediation Engine and Safety Constraints.
"""

import pytest
from agent.models.assessment_result import DQIssue
from agent.remediation.remediation_engine import (
    generate_safe_remediations,
    RecommendationCategory
)


def test_safe_auto_fix_whitespace():
    issue = DQIssue(
        column="user_name",
        type="whitespace_inconsistency",
        severity="LOW",
        message="Whitespace found"
    )

    recs = generate_safe_remediations([issue])

    assert len(recs) == 1
    assert recs[0].category == RecommendationCategory.SAFE_AUTO_FIX
    assert recs[0].risk_level == "LOW"
    assert "strip" in recs[0].transformation


def test_record_quarantine_for_invalid_email():
    issue = DQIssue(
        column="user_email",
        type="invalid_email",
        severity="HIGH",
        message="Invalid email syntax"
    )

    recs = generate_safe_remediations([issue])

    assert len(recs) == 1
    assert recs[0].category == RecommendationCategory.RECORD_QUARANTINE
    assert recs[0].risk_level == "MEDIUM"
