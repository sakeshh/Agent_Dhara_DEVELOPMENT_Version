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


def test_api_etl_approve_remediation_standalone():
    from agent.mcp_server import api_etl_approve_remediation, RemediationApprovalPayload

    payload = RemediationApprovalPayload(
        session_id="test_nonexistent_session",
        recommendation_id="rec_standalone_001",
        selected_option_id="flag_negative_returns",
        recommendation_version="v1.0",
        registry_version="v1.0",
    )
    res = api_etl_approve_remediation(payload)
    assert res["ok"] is True
    assert res["approval_status"] == "approved"
    assert res["recommendation_id"] == "rec_standalone_001"


def test_api_etl_approve_remediation_stale_concurrency_rejection():
    from agent.mcp_server import api_etl_approve_remediation, RemediationApprovalPayload
    from agent.session_store import save_session, get_session_lock
    from fastapi import HTTPException
    import pytest

    sid = "test_stale_concurrency_sid"
    with get_session_lock(sid):
        save_session({
            "session_id": sid,
            "context": {
                "etl_flow": {
                    "plan": {"version": "v1", "manual_review": []}
                }
            }
        })

    payload = RemediationApprovalPayload(
        session_id=sid,
        recommendation_id="rec_001",
        selected_option_id="opt_001",
        plan_hash="sha256:stale_fake_hash_value",
    )
    with pytest.raises(HTTPException) as exc_info:
        api_etl_approve_remediation(payload)
    assert exc_info.value.status_code == 409
    assert "changed after it was loaded" in exc_info.value.detail


def test_api_etl_approve_remediation_justification_enforcement():
    from agent.mcp_server import api_etl_approve_remediation, RemediationApprovalPayload
    from agent.session_store import save_session, get_session_lock
    from fastapi import HTTPException
    import pytest

    sid = "test_justification_sid"
    with get_session_lock(sid):
        save_session({
            "session_id": sid,
            "context": {
                "etl_flow": {
                    "plan": {
                        "manual_review": [
                            {
                                "id": "mr_destructive",
                                "risk_tier": "complex",
                                "resolution_options": [
                                    {"id": "drop_invalid_rows", "action": "drop_rows"}
                                ]
                            }
                        ]
                    }
                }
            }
        })

    # Missing justification on destructive action -> 422
    payload_no_just = RemediationApprovalPayload(
        session_id=sid,
        recommendation_id="mr_destructive",
        selected_option_id="drop_invalid_rows",
    )
    with pytest.raises(HTTPException) as exc_info:
        api_etl_approve_remediation(payload_no_just)
    assert exc_info.value.status_code == 422
    assert "justification is required" in exc_info.value.detail

    # With justification -> 200 OK
    payload_with_just = RemediationApprovalPayload(
        session_id=sid,
        recommendation_id="mr_destructive",
        selected_option_id="drop_invalid_rows",
        reviewer_justification="Verified invalid rows represent test accounts created prior to launch."
    )
    res = api_etl_approve_remediation(payload_with_just)
    assert res["ok"] is True
    assert res["approval_status"] == "approved"
