import sys
sys.path.append(".")
from agent.etl_handlers import etl_plan_start
from agent.session_store import reset_session, load_session
from agent.etl_handlers import _ctx

sid = "debug_res_session"
reset_session(sid)
sess = load_session(sid)
ctx = _ctx(sess)

assess = {
    "datasets": {
        "ds1": {
            "columns": {
                "critical_col": {"dtype": "int"}
            }
        }
    },
    "data_quality_issues": {
        "datasets": {
            "ds1": {
                "issues": [
                    {"column": "critical_col", "type": "nulls", "severity": "medium", "message": "nulls"}
                ]
            }
        }
    }
}
ctx["assessment"] = assess
ctx["etl_flow"] = {
    "target_engine": "python",
    "sql_dialect": "tsql",
    "etl_intent": {
        "target_destination": "dataframe_only",
        "generation_mode": "full"
    }
}
import agent.session_store
agent.session_store.save_session(sess)

rules = {"auto_resolve_safe_defaults": False}
source_context = {
    "suggestions": [
        {
            "dataset": "ds1",
            "column": "critical_col",
            "issue_type": "nulls",
            "suggested_action": "fill_or_drop",
            "auto_fixable": False,
            "severity": "medium",
            "message": "nulls requiring review"
        }
    ]
}

res = etl_plan_start(sid, business_rules=rules, assessment_result=assess, source_context=source_context)
print("ok:", res.get("ok"))
print("validation_errors:", res.get("plan_validation_errors"))
if "plan" in res:
    import pprint
    pprint.pprint(res["plan"].get("datasets"))
