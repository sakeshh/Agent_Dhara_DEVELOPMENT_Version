import json
import logging
import os
import pytest
from unittest.mock import patch

from agent.feature_flags import get_flags, reset_flags
from agent.etl_handlers import (
    etl_plan_start,
    etl_confirm_plan,
    etl_generate_code,
    _run_compiler_codegen,
    save_plan_snapshot,
    load_plan_snapshot,
)
from agent.session_store import load_session, save_session


@pytest.fixture(autouse=True)
def setup_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DHARA_LLM_IR_PLANNER", "1")
    monkeypatch.setenv("DHARA_DATA_DIR", str(tmp_path))
    reset_flags()
    yield
    reset_flags()


def _sample_assessment():
    return {
        "datasets": {
            "Employees": {
                "columns": {
                    "EmployeeID": {"dtype": "int"},
                    "FullName": {"dtype": "string"},
                    "Department": {"dtype": "string"},
                    "Score": {"dtype": "int"},
                },
                "row_count": 500,
            }
        },
        "issues": [
            {
                "dataset": "Employees",
                "column": "EmployeeID",
                "issue_type": "business_key_duplicate",
                "severity": "HIGH",
                "message": "Duplicate EmployeeID found",
            }
        ],
    }


def test_prompt_lifecycle_tracing_and_persistence(caplog):
    """
    Sprint 1: Trace prompt lifecycle, persist prompt in session,
    persist route in session, persist IR in session.
    """
    caplog.set_level(logging.INFO)
    sid = "test_lifecycle_sess"
    assess = _sample_assessment()
    user_prompt = "Clean duplicate records and filter score > 80"

    # Step 1: PLAN stage
    plan_res = etl_plan_start(
        session_id=sid,
        business_rules={"user_prompt": user_prompt, "notes": user_prompt},
        assessment_result=assess,
        engine="sql",
    )
    assert plan_res["ok"] is True
    plan = plan_res["plan"]

    # Verify Prompt Persistence in Plan, Session, Flow
    assert plan.get("user_prompt") == user_prompt
    sess = load_session(sid)
    assert sess.get("user_prompt") == user_prompt
    assert sess["context"]["user_prompt"] == user_prompt
    assert sess["context"]["etl_flow"]["user_prompt"] == user_prompt
    assert sess.get("had_user_prompt") is True

    # Verify IR & Route Persistence in Session
    assert "ir_plan" in plan
    assert "planner_route" in plan
    assert sess.get("ir_plan") is not None
    assert sess.get("planner_route") is not None
    assert sess["context"]["etl_flow"].get("ir_plan") is not None
    assert sess["context"]["etl_flow"].get("planner_route") is not None

    # Verify Logging Diagnostics for PLAN & IntentRouter
    assert f"[PLAN] user_prompt={repr(user_prompt)}" in caplog.text
    assert "[IntentRouter] route=" in caplog.text
    assert "[SESSION] Persisted user_prompt" in caplog.text

    # Step 2: CONFIRM stage
    # UI sends plan_override without user_prompt (simulating frontend stripping fields)
    plan_override = {
        "datasets": plan["datasets"],
        "plan_id": plan["plan_id"],
        "validation_certificate": plan.get("validation_certificate"),
    }
    confirm_res = etl_confirm_plan(session_id=sid, plan_override=plan_override)
    assert confirm_res["ok"] is True
    approved_plan = confirm_res["approved_plan"]

    # Verify Prompt is recovered and preserved during CONFIRM
    assert approved_plan.get("user_prompt") == user_prompt
    assert approved_plan.get("ir_plan") is not None
    assert approved_plan.get("planner_route") is not None

    sess = load_session(sid)
    assert sess.get("user_prompt") == user_prompt
    assert sess["context"]["etl_flow"]["approved_plan"].get("user_prompt") == user_prompt

    # Verify CONFIRM logging
    assert f"[CONFIRM] user_prompt={repr(user_prompt)}" in caplog.text

    # Step 3: GENERATE stage
    gen_res = etl_generate_code(session_id=sid, engine="sql", codegen_mode="compiler")
    assert gen_res["ok"] is True
    assert gen_res["generated_by"] == "compiler"
    assert "compiler" in gen_res["generation_path"]

    # Verify GENERATE diagnostic logging across all 4 sources
    assert f"plan.user_prompt={user_prompt}" in caplog.text
    assert f"flow.user_prompt={user_prompt}" in caplog.text
    assert f"session.user_prompt={user_prompt}" in caplog.text
    assert f"ctx.user_prompt={user_prompt}" in caplog.text
    assert "[CompilerPipeline] using persisted IR" in caplog.text


def test_hard_fail_on_missing_prompt(caplog):
    """
    Sprint 2: Hard fail instead of silent fallback if prompt is lost.
    Returns MISSING_PROMPT error instead of silently falling back to issues-based compiler.
    """
    caplog.set_level(logging.ERROR)
    sid = "test_missing_prompt_sess"
    sess = load_session(sid)
    sess["had_user_prompt"] = True
    sess["user_prompt"] = None
    ctx = sess.setdefault("context", {})
    ctx["had_user_prompt"] = True
    ctx["user_prompt"] = None
    flow = ctx.setdefault("etl_flow", {})
    flow["had_user_prompt"] = True
    flow["user_prompt"] = None
    flow["planner_route"] = "llm_ir_planner"
    flow["phase"] = "approved"

    plan = {
        "plan_id": "test_plan_no_prompt",
        "datasets": {"Employees": {"columns": ["EmployeeID", "FullName"]}},
        "user_prompt": None,
        "had_user_prompt": True,
        "validation_certificate": {
            "validated_plan_hash": "dummy"
        },
    }
    # Create valid dummy hash for bypass
    import hashlib
    core_plan = {
        "datasets": plan.get("datasets") or {},
        "business_rules": {},
        "relationships": {},
        "global_steps": [],
        "manual_review": [],
    }
    blob = json.dumps(core_plan, sort_keys=True, default=str)
    plan["validation_certificate"]["validated_plan_hash"] = "sha256:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()

    flow["approved_plan"] = plan
    flow["plan"] = plan
    save_session(sess)

    # Call etl_generate_code with missing prompt
    res = etl_generate_code(session_id=sid, engine="sql", codegen_mode="compiler")

    assert res["ok"] is False
    assert res["error"] == "MISSING_PROMPT"
    assert res["message"] == "Prompt lost between planning and generation"
    assert "[LLM-IR] Missing prompt" in caplog.text


def test_generate_from_persisted_ir_without_prompt():
    """
    Sprint 2: Generate from stored IR with zero dependency on prompt during generate.
    """
    sid = "test_ir_generate_sess"
    assess = _sample_assessment()
    user_prompt = "Find employees with score > 85"

    plan_res = etl_plan_start(
        session_id=sid,
        business_rules={"user_prompt": user_prompt},
        assessment_result=assess,
        engine="sql",
    )
    assert plan_res["ok"] is True
    stored_ir = plan_res["plan"]["ir_plan"]
    assert stored_ir is not None

    # Simulate wipe of prompt during generate, keeping IR plan
    plan = {
        "datasets": plan_res["plan"]["datasets"],
        "ir_plan": stored_ir,
        "planner_route": "llm_ir_planner",
        "user_prompt": None,
    }
    ctx = {
        "etl_flow": {
            "ir_plan": stored_ir,
            "planner_route": "llm_ir_planner",
            "user_prompt": None,
        }
    }

    code, success, issues, mode = _run_compiler_codegen(
        session_id=sid,
        engine="sql",
        plan=plan,
        assess=assess,
        ctx=ctx,
        user_prompt=None,
    )
    assert success is True
    assert mode == "compiler"
    assert "Score" in code or "Employees" in code


def test_snapshot_persistence_and_route_replay():
    """
    Sprint 3: Verify plan_snapshot.json persistence, route replay, and telemetry verification.
    """
    sid = "test_snapshot_sess"
    snapshot_data = {
        "user_prompt": "Find certified employees",
        "planner_route": "llm_ir_planner",
        "ir_plan": {"sources": [], "logical_plan": []},
    }
    snap_path = save_plan_snapshot(sid, snapshot_data)
    assert os.path.exists(snap_path)
    assert snap_path.endswith("plan_snapshot.json")

    loaded = load_plan_snapshot(sid)
    assert loaded is not None
    assert loaded["user_prompt"] == "Find certified employees"
    assert loaded["planner_route"] == "llm_ir_planner"


def test_telemetry_startup_log(caplog):
    """
    Sprint 3: Telemetry verification: FeatureFlags startup logs LLM_IR_PLANNER=True.
    """
    caplog.set_level(logging.INFO)
    reset_flags()
    flags = get_flags()
    assert flags.llm_ir_planner_enabled is True
    assert "LLM_IR_PLANNER=True" in caplog.text
