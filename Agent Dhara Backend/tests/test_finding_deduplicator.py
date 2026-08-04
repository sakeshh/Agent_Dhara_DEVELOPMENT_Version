"""
Test suite for Sprint 6: Finding Identity & Deduplication Engine.
"""

import pytest
from agent.models.assessment_result import DQIssue
from agent.profiling.finding_deduplicator import (
    generate_finding_key,
    deduplicate_and_link_findings
)


def test_deterministic_finding_key():
    k1 = generate_finding_key("users", "email", "invalid_email")
    k2 = generate_finding_key("users", "email", "invalid_email")
    k3 = generate_finding_key("users", "phone", "invalid_email")

    assert k1 == k2
    assert k1 != k3


def test_finding_deduplication():
    i1 = DQIssue(
        column="email",
        type="invalid_email",
        severity="HIGH",
        count=5,
        row_indexes=[1, 2],
        message="Invalid email format"
    )
    i2 = DQIssue(
        column="email",
        type="invalid_email",
        severity="HIGH",
        count=3,
        row_indexes=[3, 4],
        message="Invalid email format"
    )

    deduped, diags = deduplicate_and_link_findings([i1, i2])

    assert len(deduped) == 1
    assert deduped[0].count == 8
    assert len(deduped[0].row_indexes) == 4
    assert len(diags) == 1
