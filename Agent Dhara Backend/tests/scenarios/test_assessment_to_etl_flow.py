from __future__ import annotations
import pytest
from unittest.mock import patch
from agent.etl_handlers import (
    etl_plan_start,
    etl_confirm_plan,
    etl_generate_code,
)
from agent.session_store import load_session, save_session
from tests.fixtures.blob_pair_assessment import make_blob_pair_assessment


def test_e2e_assessment_to_etl_flow():
    """
    Scenario Test: Load assessment result -> Plan start -> Confirm plan -> Generate code.
    This exercises the full backend handlers integration with session persistence.
    """
    session_id = "e2e-test-session-456"
    assess = make_blob_pair_assessment()

    # Step 1: Start the ETL plan
    res_plan = etl_plan_start(
        session_id=session_id,
        business_rules={"never_drop_rows": True, "auto_resolve_safe_defaults": True},
        assessment_result=assess,
        engine="python",
        sql_dialect="tsql",
        target_destination="dataframe_only",
    )
    
    assert "plan" in res_plan
    
    # Check that session was correctly saved
    sess = load_session(session_id)
    assert sess["session_state"] == "planned"
    assert sess["context"]["etl_flow"]["phase"] in ["planned", "preview_ready"]
    
    # Step 2: Confirm the plan to advance phase to preview_ready / approved
    res_confirm = etl_confirm_plan(session_id=session_id, plan_override=res_plan["plan"])
    print("\n--- res_confirm output ---")
    import pprint
    pprint.pprint(res_confirm)
    assert res_confirm["ok"] is True
    
    # Check session approved phase
    sess_approved = load_session(session_id)
    assert sess_approved["context"]["etl_flow"]["phase"] == "approved"
    
    # Step 3: Generate the ETL code
    res_gen = etl_generate_code(
        session_id=session_id,
        engine="python",
        sql_dialect="tsql",
    )
    
    assert res_gen["ok"] is True
    assert "code" in res_gen
    assert len(res_gen["code"]) > 0
    
    # Check the final session state
    sess_final = load_session(session_id)
    assert sess_final["context"]["etl_flow"]["phase"] in ["validated", "code_ready"]
    assert sess_final["context"]["etl_flow"]["validation_ok"] is True
