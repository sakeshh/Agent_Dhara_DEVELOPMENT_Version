"""
Test step ID collision prevention across datasets and within datasets.
Ensures that identically named columns across different tables (e.g. EffectiveFrom)
never produce duplicate step IDs, and validate_promoted_plan self-heals any unscoped IDs.
"""

from agent.etl_pipeline.issue_identity import make_step_id, step_identity, merge_steps
from agent.etl_pipeline.planner import PlanBuilder
from agent.etl_pipeline.validate_plan import validate_promoted_plan


def test_make_step_id_dataset_differentiation():
    """Different datasets with the same column and action must produce distinct step IDs."""
    id1 = make_step_id("dbo.Dhara_EmployeeStatusHistory", "EffectiveFrom", "parse_dates")
    id2 = make_step_id("dbo.Dhara_OrgReference", "EffectiveFrom", "parse_dates")
    id_global = make_step_id("_global", "EffectiveFrom", "parse_dates")

    assert id1 != id2, "Dataset-scoped step IDs must differ across tables"
    assert id1 != id_global, "Dataset-scoped step ID must differ from _global"
    assert id2 != id_global, "Dataset-scoped step ID must differ from _global"
    assert id1.startswith("STEP-")
    assert id2.startswith("STEP-")


def test_merge_steps_preserves_dataset():
    """merge_steps must retain dataset name from winner or loser and not default to _global."""
    step1 = {
        "dataset": "dbo.Dhara_OrgReference",
        "column": "EffectiveFrom",
        "action": "parse_dates",
        "params": {}
    }
    step2 = {
        "column": "EffectiveFrom",
        "action": "parse_dates",
        "params": {"filter_impossible": True}
    }

    merged = merge_steps(step1, step2)
    assert merged.get("dataset") == "dbo.Dhara_OrgReference"
    assert merged.get("step_id") == make_step_id("dbo.Dhara_OrgReference", "EffectiveFrom", "parse_dates")
    assert "d45fb414" not in merged.get("step_id")


def test_validate_promoted_plan_self_heals_unscoped_duplicate():
    """
    If two datasets have steps that collided on an unscoped legacy step ID (e.g. STEP-d45fb414),
    validate_promoted_plan must self-heal the duplicate by scoping it with ds_name.
    """
    plan = {
        "datasets": {
            "dbo.Dhara_EmployeeStatusHistory": {
                "steps": [
                    {
                        "step_id": "STEP-d45fb414",
                        "column": "EffectiveFrom",
                        "action": "parse_dates",
                        "params": {}
                    }
                ]
            },
            "dbo.Dhara_OrgReference": {
                "steps": [
                    {
                        "step_id": "STEP-d45fb414",
                        "column": "EffectiveFrom",
                        "action": "parse_dates",
                        "params": {}
                    }
                ]
            }
        }
    }

    valid, issues = validate_promoted_plan(plan, {}, {})
    assert valid is True, f"Plan should validate after self-healing duplicate step IDs: {issues}"
    assert len(issues) == 0

    s1 = plan["datasets"]["dbo.Dhara_EmployeeStatusHistory"]["steps"][0]["step_id"]
    s2 = plan["datasets"]["dbo.Dhara_OrgReference"]["steps"][0]["step_id"]
    assert s1 != s2, "Self-healed step IDs must be distinct"
    assert s2 == make_step_id("dbo.Dhara_OrgReference", "EffectiveFrom", "parse_dates")
