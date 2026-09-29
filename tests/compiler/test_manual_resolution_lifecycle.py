import pytest
from agent.compiler.readiness.plan_readiness_service import PlanReadinessService
from agent.compiler.models.plan_readiness import ReadinessState
from agent.etl_handlers import etl_apply_manual_resolutions, etl_confirm_plan
from agent.session_store import load_session, save_session

def test_manual_resolutions_on_authoritative_key():
    sid = "sess_repro_authoritative_key"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": {
            "datasets": {
                "dbo.Dhara_Employee": {
                    "columns": {
                        "EmployeeID": {"dtype": "int"},
                        "Name": {"dtype": "string"},
                        "ManagerID": {"dtype": "int"},
                    },
                    "primary_key_columns": ["EmployeeID"],
                }
            },
            "issues": [
                {
                    "issue_id": "iss_emp_null",
                    "dataset": "dbo.Dhara_Employee",
                    "column": "EmployeeID",
                    "issue_type": "null_values",
                    "severity": "HIGH",
                },
                {
                    "issue_id": "iss_mgr_ri",
                    "dataset": "dbo.Dhara_Employee",
                    "column": "ManagerID",
                    "issue_type": "referential_integrity",
                    "severity": "HIGH",
                }
            ],
        },
        "etl_flow": {
            "phase": "planned",
            "plan_version": 1,
            "plan": {
                "plan_id": "p_repro",
                "plan_version": 1,
                "datasets": {
                    "dbo.Dhara_Employee": {
                        "steps": []
                    }
                },
                "manual_review": [
                    {
                        "id": "rev_emp_null",
                        "dataset": "dbo.Dhara_Employee",
                        "column": "EmployeeID",
                        "issue_type": "null_values",
                        "severity": "HIGH",
                        "status": "pending",
                        "resolution_options": [
                            {"id": "flag_missing", "action": "flag_only", "label": "Flag only"}
                        ]
                    },
                    {
                        "id": "rev_mgr_ri",
                        "dataset": "dbo.Dhara_Employee",
                        "column": "ManagerID",
                        "issue_type": "referential_integrity",
                        "severity": "HIGH",
                        "status": "pending",
                        "resolution_options": [
                            {"id": "flag_orphan", "action": "flag_only", "label": "Flag orphan"}
                        ]
                    }
                ],
            },
        },
    }
    save_session(sess)

    # User resolves all manual reviews
    res = etl_apply_manual_resolutions(
        session_id=sid,
        resolutions=[
            {"item_id": "rev_emp_null", "resolution_id": "flag_missing"},
            {"item_id": "rev_mgr_ri", "resolution_id": "flag_orphan"},
        ]
    )
    print("\nApplied manual resolutions result:")
    print("ok:", res.get("ok"))
    print("can_confirm_plan:", res.get("can_confirm_plan"))
    print("ready_for_confirmation:", res.get("ready_for_confirmation"))
    print("lifecycle_state:", res.get("lifecycle_state"))
    print("blocking_issues:", res.get("blocking_issues"))
    print("blocking_issue_count:", res.get("blocking_issue_count"))
    
    assert res["can_confirm_plan"] is True
    assert res["ready_for_confirmation"] is True
    assert res["lifecycle_state"] == "READY_TO_CONFIRM"

    # User clicks "Save plan & go to ETL code" -> etl_confirm_plan
    conf_res = etl_confirm_plan(session_id=sid)
    print("\nConfirm plan result:")
    print("ok:", conf_res.get("ok"))
    print("status:", conf_res.get("status"))
    print("lifecycle_state:", conf_res.get("lifecycle_state"))
    assert conf_res["ok"] is True
    assert conf_res["lifecycle_state"] == "CONFIRMED"
    assert conf_res.get("can_generate") is True

    # User generates code -> etl_generate_code
    from agent.etl_handlers import etl_generate_code
    gen_res = etl_generate_code(session_id=sid, engine="sql", codegen_mode="compiler")
    print("\nGenerate code result:")
    print("ok:", gen_res.get("ok"))
    print("generated_by:", gen_res.get("generated_by"))
    assert gen_res["ok"] is True
    assert gen_res["generated_by"] == "compiler"
    assert "EmployeeID" in gen_res["code"]
