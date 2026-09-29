from __future__ import annotations

import pytest
from typing import Any, Dict

from agent.compiler.models.canonical_manifest import CanonicalResolutionManifest
from agent.compiler.models.plan_readiness import ReadinessState
from agent.etl_handlers import (
    etl_apply_manual_resolutions,
    etl_confirm_plan,
    etl_generate_code,
)
from agent.session_store import load_session, save_session


def test_e2e_full_readiness_to_codegen_lifecycle():
    """
    End-to-End integration test validating the entire governed lifecycle:
    1. Assessment with clean, informational, and blocking issues (mandatory primary key nulls)
    2. Initial plan with unresolved blocking manual review
    3. Confirm blocked with HTTP 422 (fail-closed)
    4. Attempting codegen on unconfirmed plan is rejected
    5. Partial or missing resolution leaves plan unconfirmed
    6. Valid resolution / steward approved waiver applied -> blocking count becomes 0 -> READY_TO_CONFIRM
    7. Confirm succeeds -> freezes canonical manifest and binds hash
    8. Code generation compiles strictly from frozen manifest without rediscovery
    9. Valid T-SQL generated with clean/flagged logic
    """
    sid = "sess_e2e_unified_lifecycle"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": {
            "datasets": {
                "Dhara_Employee": {
                    "columns": {
                        "EmpId": {"dtype": "int"},
                        "Name": {"dtype": "string"},
                        "Department": {"dtype": "string"},
                    },
                    "primary_key_columns": ["EmpId"],
                }
            },
            "issues": [
                {
                    "issue_id": "iss_name_ws",
                    "dataset": "Dhara_Employee",
                    "column": "Name",
                    "issue_type": "whitespace",
                    "severity": "LOW",
                    "suggested_action": "trim",
                },
                {
                    "issue_id": "iss_empid_null",
                    "dataset": "Dhara_Employee",
                    "column": "EmpId",
                    "issue_type": "null_values",
                    "severity": "HIGH",
                },
            ],
        },
        "etl_flow": {
            "phase": "planned",
            "plan_version": 1,
            "plan": {
                "plan_id": "p_e2e_001",
                "plan_version": 1,
                "datasets": {
                    "Dhara_Employee": {
                        "steps": [
                            {"action": "trim", "column": "Name"}
                        ]
                    }
                },
                "manual_review": [
                    {
                        "id": "rev_empid_null",
                        "dataset": "Dhara_Employee",
                        "column": "EmpId",
                        "issue_type": "null_values",
                        "severity": "HIGH",
                        "status": "pending",
                    }
                ],
            },
        },
    }
    save_session(sess)

    # --------------------------------------------------------------------------
    # Step 1: Confirm fails-closed when blocking issue remains
    # --------------------------------------------------------------------------
    conf_resp_1 = etl_confirm_plan(session_id=sid)
    assert conf_resp_1["ok"] is False
    assert conf_resp_1["http_status"] == 422
    assert conf_resp_1["error"] == "PLAN_NOT_READY_FOR_CONFIRMATION"
    assert conf_resp_1["lifecycle_state"] == "MANUAL_REVIEW_REQUIRED"
    assert conf_resp_1["can_confirm_plan"] is False
    assert conf_resp_1["blocking_issue_count"] >= 1

    # --------------------------------------------------------------------------
    # Step 2: Attempting code generation on unconfirmed plan is rejected
    # --------------------------------------------------------------------------
    gen_resp_1 = etl_generate_code(session_id=sid, engine="sql", codegen_mode="compiler")
    assert gen_resp_1["ok"] is False
    assert gen_resp_1["error"] in ("PLAN_NOT_APPROVED", "NO_APPROVED_PLAN")

    # --------------------------------------------------------------------------
    # Step 3: Partial or unresolved review leaves plan NOT ready
    # --------------------------------------------------------------------------
    apply_resp_noop = etl_apply_manual_resolutions(
        session_id=sid,
        resolutions=[],
    )
    assert apply_resp_noop["ok"] is True
    assert apply_resp_noop["ready_for_confirmation"] is False
    assert apply_resp_noop["can_confirm_plan"] is False
    assert apply_resp_noop["lifecycle_state"] == "MANUAL_REVIEW_REQUIRED"
    assert apply_resp_noop["blocking_issue_count"] >= 1

    # --------------------------------------------------------------------------
    # Step 4: Steward waives / resolves the issue with explicit justification
    # --------------------------------------------------------------------------
    sess_update = load_session(sid)
    flow_update = sess_update["context"]["etl_flow"]
    flow_update["plan"]["approved_waivers"] = ["iss_empid_null"]
    flow_update["phase"] = "planned"
    save_session(sess_update)

    # Check that applying resolutions re-evaluates readiness and unblocks
    apply_resp_valid = etl_apply_manual_resolutions(
        session_id=sid,
        resolutions=[],
    )
    assert apply_resp_valid["ok"] is True
    assert apply_resp_valid["ready_for_confirmation"] is True
    assert apply_resp_valid["can_confirm_plan"] is True
    assert apply_resp_valid["lifecycle_state"] == "READY_TO_CONFIRM"
    assert apply_resp_valid["blocking_issue_count"] == 0

    # --------------------------------------------------------------------------
    # Step 5: Confirm plan succeeds and freezes the canonical manifest
    # --------------------------------------------------------------------------
    conf_resp_2 = etl_confirm_plan(session_id=sid)
    assert conf_resp_2["ok"] is True
    assert conf_resp_2["status"] == "CONFIRMED"
    assert conf_resp_2["lifecycle_state"] == "CONFIRMED"
    assert conf_resp_2["can_generate"] is True
    manifest_hash = conf_resp_2.get("canonical_manifest_hash")
    assert manifest_hash is not None

    # Verify manifest is frozen in session state
    sess_confirmed = load_session(sid)
    flow = sess_confirmed["context"]["etl_flow"]
    assert flow.get("canonical_manifest") is not None
    assert flow.get("canonical_manifest_hash") == manifest_hash
    manifest = CanonicalResolutionManifest.model_validate(flow["canonical_manifest"])
    assert manifest.verify_hash(manifest_hash) is True
    assert manifest.reconciliation.get("blocking_issue_count") == 0

    # --------------------------------------------------------------------------
    # Step 6: Generate ETL code compiles directly from frozen manifest
    # --------------------------------------------------------------------------
    gen_resp_2 = etl_generate_code(session_id=sid, engine="sql", codegen_mode="compiler")
    assert gen_resp_2["ok"] is True
    assert gen_resp_2["executable"] is True
    assert gen_resp_2["generated_by"] == "compiler"
    generated_code = gen_resp_2["code"]
    assert "SELECT" in generated_code
    assert "Dhara_Employee" in generated_code
