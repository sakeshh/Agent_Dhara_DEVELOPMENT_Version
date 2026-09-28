"""
Adversarial End-to-End Hardening Matrix for Agent Dhara.
Tests all 7 critical failure and recovery workflows:
1. Manual-review recovery (multi-turn surgical corrections)
2. User changes their mind (re-opening approved plan for edits)
3. Browser refresh & session rehydration (no lost decisions)
4. Stale-plan protection (concurrency guard with STALE_PLAN_VERSION)
5. Network failure & idempotency (replay protection)
6. Schema changes after planning (schema drift guard)
7. Runtime conversion failure (circuit breaker governance)
"""

import pytest
from agent.etl_handlers import (
    etl_apply_manual_resolutions,
    etl_confirm_plan,
)
from agent.session_store import load_session, save_session


@pytest.fixture
def mock_assessment_20_items():
    """Builds a rich mock assessment with customer and order tables."""
    return {
        "datasets": {
            "customers": {
                "row_count": 1000,
                "columns": {
                    "customer_id": {"null_percentage": 0.0, "semantic_role": "PRIMARY_KEY", "type": "int"},
                    "email": {"null_percentage": 15.0, "semantic_role": "ATTRIBUTE", "type": "string"},
                    "phone": {"null_percentage": 12.0, "semantic_role": "ATTRIBUTE", "type": "string"},
                    "signup_date": {"null_percentage": 5.0, "semantic_role": "ATTRIBUTE", "type": "string"},
                    "country_code": {"null_percentage": 8.0, "semantic_role": "ATTRIBUTE", "type": "string"},
                },
            },
            "orders": {
                "row_count": 5000,
                "columns": {
                    "order_id": {"null_percentage": 0.0, "semantic_role": "PRIMARY_KEY", "type": "int"},
                    "customer_id": {"null_percentage": 2.0, "semantic_role": "FOREIGN_KEY", "type": "int"},
                    "amount": {"null_percentage": 0.0, "semantic_role": "MEASURE", "type": "float"},
                    "order_date": {"null_percentage": 0.0, "semantic_role": "ATTRIBUTE", "type": "string"},
                },
            }
        },
        "quality_report": {
            "issues": [
                {"id": f"issue_{i}", "dataset": "customers", "column": "email", "type": "high_null_percentage"}
                for i in range(1, 21)
            ]
        }
    }


def test_scenario_1_manual_review_surgical_recovery(mock_assessment_20_items):
    """
    Scenario 1:
    Assessment -> Planning -> 20 manual review items ->
    User makes 18 legal choices + 2 illegal choices ->
    Validation fails with 2 conflicts -> User fixes first -> 1 remains ->
    User fixes second -> Plan v2 atomically committed.
    """
    sid = "sess_scen1_hardening"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": mock_assessment_20_items,
        "etl_flow": {
            "plan_version": 1,
            "business_rules": {"never_drop_rows": True},
            "plan": {
                "plan_version": 1,
                "datasets": {
                    "customers": {"steps": []},
                    "orders": {"steps": []}
                },
                "manual_review": [
                    {
                        "id": "item_1",
                        "dataset": "customers",
                        "column": "customer_id",
                        "issue_type": "high_null_percentage",
                        "status": "pending",
                        "resolution_options": [
                            {"id": "opt_1_impute", "action": "impute_numeric", "parameters": {"strategy": "mean"}},
                            {"id": "opt_1_flag", "action": "flag_only", "recommended": True},
                        ]
                    },
                    {
                        "id": "item_2",
                        "dataset": "orders",
                        "column": "order_id",
                        "issue_type": "high_null_percentage",
                        "status": "pending",
                        "resolution_options": [
                            {"id": "opt_2_impute", "action": "impute_numeric", "parameters": {"strategy": "mean"}},
                            {"id": "opt_2_flag", "action": "flag_only", "recommended": True},
                        ]
                    }
                ] + [
                    {
                        "id": f"item_{i}",
                        "dataset": "customers",
                        "column": "email",
                        "issue_type": "high_null_percentage",
                        "status": "pending",
                        "resolution_options": [
                            {"id": f"opt_{i}_flag", "action": "flag_only", "recommended": True},
                        ]
                    }
                    for i in range(3, 21)
                ]
            }
        }
    }
    save_session(sess)

    # 18 legal choices (flag_only on email) + 2 illegal choices (imputing customer_id on customers and order_id on orders)
    resolutions = [
        {"item_id": "item_1", "resolution_id": "opt_1_impute", "parameters": {"strategy": "mean"}},
        {"item_id": "item_2", "resolution_id": "opt_2_impute", "parameters": {"strategy": "mean"}},
    ] + [
        {"item_id": f"item_{i}", "resolution_id": f"opt_{i}_flag"}
        for i in range(3, 21)
    ]

    # First attempt: 2 illegal choices
    resp1 = etl_apply_manual_resolutions(sid, resolutions)
    assert resp1["status"] == "REQUIRES_CORRECTION"
    assert resp1["ok"] is False
    assert resp1["unresolved_issues_count"] == 2
    assert resp1["accepted_decisions_count"] == 18
    # Approved Plan v1 must remain untouched
    reloaded_sess = load_session(sid)
    assert reloaded_sess["context"]["etl_flow"]["plan"]["plan_version"] == 1

    # Fix item 1 with legal flag_only (1 illegal remains)
    resolutions[0] = {"item_id": "item_1", "resolution_id": "opt_1_flag"}
    resp2 = etl_apply_manual_resolutions(sid, resolutions)
    assert resp2["status"] == "REQUIRES_CORRECTION"
    assert resp2["unresolved_issues_count"] == 1
    assert resp2["accepted_decisions_count"] == 19

    # Fix item 2 with legal flag_only (all 20 now valid)
    resolutions[1] = {"item_id": "item_2", "resolution_id": "opt_2_flag"}
    resp3 = etl_apply_manual_resolutions(sid, resolutions)
    assert resp3["status"] == "VALID"
    assert resp3["ok"] is True
    assert resp3["plan_version"] == 2

    # Approved plan v2 committed atomically
    final_sess = load_session(sid)
    assert final_sess["context"]["etl_flow"]["plan_version"] == 2
    assert final_sess["context"]["etl_flow"]["candidate_plan"] is None


def test_scenario_2_user_changes_their_mind_on_approved_plan(mock_assessment_20_items):
    """
    Scenario 2: User changes their mind:
    Valid Plan v1 -> User edits a previously accepted choice ->
    Candidate plan evaluated -> Valid -> Promoted to Plan v2.
    If user makes invalid change, Plan v1 remains intact.
    """
    sid = "sess_scen2_mind_change"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": mock_assessment_20_items,
        "etl_flow": {
            "plan_version": 1,
            "lifecycle_state": "READY_TO_GENERATE",
            "business_rules": {"never_drop_rows": True},
            "plan": {
                "plan_version": 1,
                "datasets": {
                    "customers": {"steps": [{"step_id": "s1", "action": "flag_only", "target_column": "email", "item_id": "rev_1"}]},
                    "orders": {"steps": []}
                },
                "manual_review": [
                    {
                        "id": "rev_1",
                        "dataset": "customers",
                        "column": "email",
                        "issue_type": "high_null_percentage",
                        "status": "resolved",
                        "resolution_options": [
                            {"id": "opt_drop", "action": "drop_rows"},
                            {"id": "opt_nullify", "action": "nullify_clean_value"},
                        ]
                    }
                ]
            }
        }
    }
    save_session(sess)

    # User changes mind to an illegal choice (drop_rows under never_drop_rows)
    bad_res = [{"item_id": "rev_1", "resolution_id": "opt_drop"}]
    resp_bad = etl_apply_manual_resolutions(sid, bad_res)
    assert resp_bad["status"] == "REQUIRES_CORRECTION"
    assert resp_bad["plan_version"] == 1

    # Verify approved Plan v1 is NOT corrupted
    s_chk = load_session(sid)
    assert s_chk["context"]["etl_flow"]["plan"]["plan_version"] == 1

    # User changes mind to a legal choice (nullify_clean_value)
    good_res = [{"item_id": "rev_1", "resolution_id": "opt_nullify"}]
    resp_good = etl_apply_manual_resolutions(sid, good_res)
    assert resp_good["ok"] is True
    assert resp_good["plan_version"] == 2

    s_final = load_session(sid)
    assert s_final["context"]["etl_flow"]["plan_version"] == 2


def test_scenario_3_browser_refresh_session_rehydration(mock_assessment_20_items):
    """
    Scenario 3: Browser refresh:
    Manual review halfway completed -> Browser refresh ->
    Session reloaded from DB -> Decisions and candidate state intact.
    """
    sid = "sess_scen3_refresh"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": mock_assessment_20_items,
        "etl_flow": {
            "plan_version": 1,
            "plan": {
                "plan_version": 1,
                "datasets": {"customers": {"steps": []}, "orders": {"steps": []}},
                "manual_review": [
                    {
                        "id": f"item_{i}",
                        "dataset": "customers",
                        "column": "customer_id" if i == 5 else "email",
                        "issue_type": "high_null_percentage",
                        "status": "pending",
                        "resolution_options": [
                            {"id": f"opt_{i}_impute", "action": "impute_numeric", "parameters": {"strategy": "mean"}},
                            {"id": f"opt_{i}_flag", "action": "flag_only"},
                        ]
                    }
                    for i in range(1, 11)
                ]
            }
        }
    }
    save_session(sess)

    # User resolves 5 items, 1 has invalid key imputation
    partial_resolutions = [
        {"item_id": f"item_{i}", "resolution_id": f"opt_{i}_flag"}
        for i in range(1, 5)
    ] + [
        {"item_id": "item_5", "resolution_id": "opt_5_impute", "parameters": {"strategy": "mean"}}
    ]

    resp = etl_apply_manual_resolutions(sid, partial_resolutions)
    assert resp["status"] == "REQUIRES_CORRECTION"
    assert resp["accepted_decisions_count"] == 4

    # Simulate browser refresh by loading a brand new session object from SQLite
    refreshed_sess = load_session(sid)
    ctx = refreshed_sess["context"]
    flow = ctx["etl_flow"]

    # Invariant: 4 accepted decisions must be persisted in candidate_plan and session
    candidate = flow["candidate_plan"]
    assert candidate is not None
    assert len(candidate["accepted_decisions"]) == 4
    assert candidate["unresolved_items"] == ["item_5"]
    assert len(ctx["manual_review_resolutions"]) == 5


def test_scenario_4_stale_plan_protection(mock_assessment_20_items):
    """
    Scenario 4: Stale-plan protection:
    Browser A has Plan v4.
    Browser B modifies -> Plan becomes v5.
    Browser A submits changes with expected_plan_version=4 ->
    Rejected with STALE_PLAN_VERSION, never overwrites v5.
    """
    sid = "sess_scen4_stale_guard"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": mock_assessment_20_items,
        "etl_flow": {
            "plan_version": 5,  # Browser B already bumped it to 5
            "plan": {
                "plan_version": 5,
                "datasets": {"customers": {"steps": [{"step_id": "b_step", "action": "flag_only"}]}},
            }
        }
    }
    save_session(sess)

    # Browser A submits against stale v4
    stale_resolutions = [{"item_id": "stale_1", "action": "flag_only", "column": "email", "dataset": "customers"}]
    resp = etl_apply_manual_resolutions(sid, stale_resolutions, expected_plan_version=4)

    assert resp["ok"] is False
    assert resp["status"] == "STALE_PLAN_VERSION"
    assert resp["http_status"] == 409
    assert resp["lifecycle_state"] == "REBASE_REQUIRED"
    assert "version 5" in resp["what_happened"]
    assert resp["available_transitions"][0]["transition_id"] == "REBASE"

    # Invariant: Database remains at v5 untouched
    chk = load_session(sid)
    assert chk["context"]["etl_flow"]["plan_version"] == 5
    assert chk["context"]["etl_flow"]["plan"]["datasets"]["customers"]["steps"][0]["step_id"] == "b_step"


def test_scenario_5_network_failure_idempotency(mock_assessment_20_items):
    """
    Scenario 5: Network failure:
    User clicks Apply -> Backend commits -> Network response lost ->
    Frontend retries with same idempotency key ->
    Idempotency detects replay -> No duplicated steps or extra version bumps.
    """
    sid = "sess_scen5_idempotency"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": mock_assessment_20_items,
        "etl_flow": {
            "plan_version": 1,
            "plan": {
                "plan_version": 1,
                "datasets": {"customers": {"steps": []}},
                "manual_review": [
                    {
                        "id": "r1",
                        "dataset": "customers",
                        "column": "email",
                        "issue_type": "high_null_percentage",
                        "status": "pending",
                        "resolution_options": [
                            {"id": "opt_r1", "action": "flag_only"}
                        ]
                    }
                ]
            }
        }
    }
    save_session(sess)

    resolutions = [{"item_id": "r1", "resolution_id": "opt_r1"}]
    idem_key = "idemp-req-778899"

    # First call succeeds
    resp1 = etl_apply_manual_resolutions(sid, resolutions, idempotency_key=idem_key)
    assert resp1["ok"] is True
    assert resp1["plan_version"] == 2

    # Second call (network retry) with same idempotency key
    resp2 = etl_apply_manual_resolutions(sid, resolutions, idempotency_key=idem_key)
    assert resp2["ok"] is True
    assert resp2.get("idempotent_replay") is True
    assert resp2["plan_version"] == 2  # Did not bump to 3!

    # Verify steps were NOT duplicated in the stored plan
    s_verify = load_session(sid)
    steps = s_verify["context"]["etl_flow"]["plan"]["datasets"]["customers"]["steps"]
    assert len(steps) == 1


def test_scenario_6_schema_changes_after_planning(mock_assessment_20_items):
    """
    Scenario 6: Schema changes after planning:
    Assessment -> Plan -> Database schema changes ->
    Attempting confirmation or code generation detects fingerprint mismatch ->
    SCHEMA_DRIFT returned with recovery transitions, not code against old schema.
    """
    sid = "sess_scen6_schema_drift"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": mock_assessment_20_items,
        "etl_flow": {
            "plan_version": 2,
            "assessment_schema_signature": "sha256:original_schema_hash_123",
            "phase": "preview_ready",
            "plan": {
                "plan_version": 2,
                "datasets": {"customers": {"steps": []}},
            }
        }
    }
    save_session(sess)

    # Attempt confirm with drifted schema (assessment has different columns/signature)
    resp = etl_confirm_plan(sid)
    assert resp["ok"] is False
    assert resp["status"] == "SCHEMA_DRIFT"
    assert resp["http_status"] == 409
    assert resp["lifecycle_state"] == "REVALIDATION_REQUIRED"
    assert resp["available_transitions"][0]["transition_id"] == "REVALIDATE"
    assert "schema fingerprint changed" in resp["what_happened"].lower()


def test_scenario_7_runtime_safety_circuit_breaker():
    """
    Scenario 7: Runtime conversion failure / circuit breaker:
    Dry-run simulator or execution trips error budget ->
    Surfaces CIRCUIT_BREAKER_TRIGGERED with explicit recovery options:
    quarantine / block / accept warning.
    """
    from agent.etl_pipeline.recovery_contract import build_circuit_breaker_recovery_envelope

    envelope = build_circuit_breaker_recovery_envelope(
        error_rate=0.25,
        threshold=0.05,
        dataset="customers",
        column="signup_date",
        failure_sample=["invalid_date_abc", "99/99/9999"],
    )

    resp = envelope.to_dict()
    assert resp["status"] == "CIRCUIT_BREAKER_TRIGGERED"
    assert resp["recoverable"] is True
    assert resp["lifecycle_state"] == "SAFE_CHECKPOINT_RECOVERY"
    
    # Check 5 questions
    assert "25.0%" in resp["what_happened"]
    assert "circuit breaker halted execution" in resp["what_protected"].lower()
    assert "customers.signup_date" in resp["what_needs_attention"]
    assert len(resp["available_transitions"]) == 3
    
    trans_ids = [t["transition_id"] for t in resp["available_transitions"]]
    assert "QUARANTINE_AFFECTED_ROWS" in trans_ids
    assert "BLOCK_PIPELINE" in trans_ids
    assert "ALLOW_WITH_WARNING" in trans_ids
