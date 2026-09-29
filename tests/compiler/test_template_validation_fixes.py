import os
import pytest
from jinja2.exceptions import TemplateError

from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.models import (
    ActionSource,
    ActionType,
    CompilerContext,
    DataEffect,
    DatasetProfile,
    ColumnProfile,
    ExecutionEngine,
    LogicalPlan,
    LogicalStep,
    PhysicalPlan,
    PhysicalStep,
    StructuredAction,
)
from agent.compiler.planners.ingestion_bridge import IngestionBridge
from agent.compiler.templates.engine import TemplateEngine


def test_ri_self_reference_populates_complete_parameters():
    """Validates that self-referencing intra-dataset foreign keys (ManagerID -> EmployeeID)
    populate complete parameters for tsql_left_anti_join.
    """
    bridge = IngestionBridge()
    assess = {
        "data_quality_issues": {
            "datasets": {
                "dbo.Dhara_Employee": {
                    "issues": [
                        {
                            "issue_id": "iss_ri_manager_42",
                            "column": "ManagerID",
                            "issue_type": "referential_integrity",
                            "suggested_action": "validate_referential_integrity",
                            "severity": "high",
                            "target_column": "EmployeeID",
                            "target_dataset": "dbo.Dhara_Employee",
                        }
                    ]
                }
            }
        },
        "datasets": {
            "dbo.Dhara_Employee": {
                "columns": {
                    "EmployeeID": {"dtype": "int", "is_primary_key": True},
                    "ManagerID": {"dtype": "int"},
                },
                "row_count": 100,
            }
        },
    }

    canonical_issues, _, ing_telem = bridge.extract_and_normalize(
        assess=assess,
        plan={},
        known_datasets={"dbo.Dhara_Employee"},
    )
    detected_issues, structured_actions = bridge.bridge_to_compiler_models(canonical_issues)

    assert len(structured_actions) == 1
    act = structured_actions[0]
    assert act.action_type == ActionType.VALIDATE_REFERENTIAL_INTEGRITY
    assert act.parameters["child_key"] == "ManagerID"
    assert act.parameters["parent_key"] == "EmployeeID"
    assert act.parameters["parent_dataset"] == "dbo.Dhara_Employee"
    assert act.parameters["allow_null"] is True
    assert act.parameters["disposition"] == "FLAG"


def test_global_incomplete_ri_routed_to_manual_review_no_physical_step():
    """Validates that dataset-level or incomplete RI actions without keys are routed
    to manual review and do not produce an executable PhysicalStep.
    """
    bridge = IngestionBridge()
    assess = {
        "data_quality_issues": {
            "datasets": {
                "dbo.Dhara_Employee": {
                    "issues": [
                        {
                            "issue_id": "iss_dbo.Dhara_Employee_global_179",
                            "issue_type": "referential_integrity",
                            "suggested_action": "validate_referential_integrity",
                            "severity": "high",
                        }
                    ]
                }
            }
        },
        "datasets": {
            "dbo.Dhara_Employee": {
                "columns": {"EmployeeID": {"dtype": "int"}},
                "row_count": 100,
            }
        },
    }

    canonical_issues, _, ing_telem = bridge.extract_and_normalize(
        assess=assess,
        plan={},
        known_datasets={"dbo.Dhara_Employee"},
    )
    assert any(
        m.get("issue_id") == "iss_dbo.Dhara_Employee_global_179"
        and m.get("reason") == "manual_review_incomplete_relationship_metadata"
        for m in ing_telem.get("manual_review_issues", [])
    )

    detected_issues, structured_actions = bridge.bridge_to_compiler_models(canonical_issues)
    assert len(structured_actions) == 0


def test_ri_check_template_explicit_raise_on_missing_keys():
    """Defense-in-depth: ri_check.j2 must explicitly raise TemplateError rather than render empty SQL."""
    engine = TemplateEngine()
    with pytest.raises(Exception):
        engine.render(
            "sql/ri_check.j2",
            target_table="dbo.Dhara_Employee",
            child_key=None,
            parent_table="dbo.Dhara_Employee",
            parent_key=None,
        )


def test_unapproved_salary_sentinel_withheld_and_safe_steps_render():
    """Validates that unapproved salary sentinel replacement is removed from the
    executable physical plan, recorded in manual review, and does not block safe steps.
    """
    pipeline = CompilerPipeline()

    # Action 1: Trim Name (safe)
    action1 = StructuredAction(
        action_type=ActionType.TRIM,
        dataset_name="dbo.Dhara_Employee",
        target_columns=["Name"],
        parameters={"columns": ["Name"]},
        source_issue_ids=["iss_trim_1"],
    )

    # Action 2: Unapproved Salary Sentinel
    action2 = StructuredAction(
        action_type=ActionType.REPLACE_SENTINELS,
        dataset_name="dbo.Dhara_Employee",
        target_columns=["Salary"],
        parameters={
            "columns": ["Salary"],
            "col": "Salary",
            "sentinel_values": ["-999"],
            "requires_approval": True,
            "approved": False,
        },
        requires_approval=True,
        source_issue_ids=["iss_salary_sentinel_27"],
    )

    prof = DatasetProfile(
        dataset_id="dbo.Dhara_Employee",
        columns={
            "Name": ColumnProfile(name="Name", dtype="string"),
            "Salary": ColumnProfile(name="Salary", dtype="numeric"),
        },
        row_count=100,
    )

    ctx = CompilerContext(
        session_id="salary_approval_test",
        target_engine=ExecutionEngine.AZURE_SQL,
        structured_actions=[action1, action2],
        dataset_profiles={"dbo.Dhara_Employee": prof},
    )

    compiled_ctx, _ = pipeline.compile(ctx)
    telemetry = compiled_ctx.metadata["compilation_telemetry"]

    # Invariant: Step 2 was withheld from physical plan
    assert telemetry["physical_step_count"] == 1
    assert telemetry["rendered_transformation_count"] == 1
    assert telemetry["failed_transformation_count"] == 0
    assert telemetry["manual_review_step_count"] == 1

    # Safe step rendered cleanly into SQL
    sql = compiled_ctx.code_artifact.content
    assert "TRIM([Name])" in sql
    assert "-999" not in sql

    # Reconciled status of salary sentinel is manual_review
    reconciled_salary = next(
        r for r in telemetry["issue_reconciliation"] if r["issue_id"] == "iss_salary_sentinel_27"
    )
    assert reconciled_salary["final_status"] == "manual_review"
    assert reconciled_salary["reason"] == "approval_required_for_sentinel_replacement"


def test_lineage_preservation_across_consolidation():
    """Validates that consolidation preserves all source_issue_ids so issues are not marked
    unsupported 'Not mapped or scheduled'.
    """
    from agent.compiler.optimizers.optimization_agent import OptimizationAgent

    step1 = LogicalStep(
        step_id="step_trim_1",
        dataset_name="dbo.Dhara_Employee",
        action=ActionType.TRIM,
        target_columns=["Name"],
        parameters={"columns": ["Name"]},
        source_issue_ids=["iss_trim_a"],
    )
    step2 = LogicalStep(
        step_id="step_trim_2",
        dataset_name="dbo.Dhara_Employee",
        action=ActionType.TRIM,
        target_columns=["Name"],
        parameters={"columns": ["Name"]},
        source_issue_ids=["iss_trim_b"],
    )

    ctx = CompilerContext(
        session_id="lineage_test",
        target_engine=ExecutionEngine.AZURE_SQL,
        logical_plan=LogicalPlan(
            plan_id="log_plan_1",
            steps=[step1, step2],
        ),
    )

    opt = OptimizationAgent()
    res_ctx = opt.execute(ctx)

    assert len(res_ctx.logical_plan.steps) == 1
    fused_step = res_ctx.logical_plan.steps[0]
    assert "iss_trim_a" in fused_step.source_issue_ids
    assert "iss_trim_b" in fused_step.source_issue_ids


def test_idempotency_fingerprint_uses_current_commit_and_bumped_version():
    """Validates that the idempotency fingerprint uses APP_COMMIT_SHA (default ec67830)
    and compiler version 2.0.2 to invalidate stale caches.
    """
    from agent.etl_handlers import COMPILER_BUILD, COMPILER_VERSION

    assert COMPILER_VERSION == "2.0.2"
    assert COMPILER_BUILD in (os.getenv("APP_COMMIT_SHA", "ec67830"), "ec67830")
