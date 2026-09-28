import pytest

from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.models.actions import StructuredAction
from agent.compiler.models.compilation_result import CodeGenerationError
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.issues import Issue, IssueCategory, IssueSeverity
from agent.compiler.models.logical import ActionType
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import ColumnProfile, DatasetProfile


def _build_context(engine: ExecutionEngine = ExecutionEngine.AZURE_SQL, session_id: str = "sess_gate") -> CompilerContext:
    cols = {
        "id": ColumnProfile(name="id", dtype="int"),
        "name": ColumnProfile(name="name", dtype="string"),
        "email": ColumnProfile(name="email", dtype="string"),
        "salary": ColumnProfile(name="salary", dtype="float"),
    }
    profile = DatasetProfile(dataset_id="Employee", row_count=100, columns=cols)
    return CompilerContext(
        session_id=session_id,
        target_engine=engine,
        dataset_profiles={"Employee": profile},
    )


def test_scenario_a_all_issues_supported():
    """Scenario A: All issues supported -> ok=True, executable=True."""
    pipeline = CompilerPipeline()
    ctx = _build_context()

    # 2 supported actions (trim, lower)
    ctx.structured_actions = [
        StructuredAction(action_type=ActionType.TRIM, dataset_name="Employee", target_columns=["name"]),
        StructuredAction(action_type=ActionType.LOWERCASE, dataset_name="Employee", target_columns=["email"]),
    ]
    ctx.metadata["ingestion_telemetry"] = {
        "assessment_discovered_count": 2,
        "mapped_action_count": 2,
        "unsupported_issue_count": 0,
    }

    compiled_ctx, _ = pipeline.compile(ctx)
    telemetry = compiled_ctx.metadata.get("compilation_telemetry", {})

    assert telemetry["ok"] is True
    assert telemetry["executable"] is True
    assert telemetry["rendered_transformation_count"] == 2
    assert telemetry["rendered_step_count"] == 2
    assert telemetry["physical_step_count"] == 2
    assert telemetry["logical_step_count"] == 2
    assert telemetry["structured_action_count"] == 2
    assert telemetry["unsupported_issue_count"] == 0
    assert telemetry["fallback_projection_used"] is False
    assert telemetry["infrastructure_statement_count"] >= 3  # Proc DDL + Staging DDL
    assert telemetry["projection_statement_count"] == 1

    # Lineage invariants
    assert all(s.source_action_id is not None for s in compiled_ctx.logical_plan.steps)
    assert all(s.source_logical_step_id is not None for s in compiled_ctx.physical_plan.steps)
    assert all(s.source_action_id is not None for s in compiled_ctx.physical_plan.steps)

    # Compilation accounting invariant
    assert (
        telemetry["rendered_transformation_count"]
        + telemetry["template_missing_count"]
        + telemetry["parameter_missing_count"]
        + telemetry["render_failed_count"]
        + telemetry["skipped_step_count"]
        + telemetry["unsupported_step_count"]
        == telemetry["physical_step_count"]
    )


def test_scenario_b_issues_detected_none_mapped():
    """Scenario B: Issues detected, none mapped -> raises CodeGenerationError."""
    pipeline = CompilerPipeline()
    ctx = _build_context()

    ctx.metadata["ingestion_telemetry"] = {
        "assessment_discovered_count": 5,
        "mapped_action_count": 0,
        "mapped_issue_count": 0,
        "unsupported_issue_count": 5,
    }

    with pytest.raises(CodeGenerationError) as exc_info:
        pipeline.compile(ctx)

    assert "none could be mapped" in str(exc_info.value)
    assert exc_info.value.failure_stage == "capability_validation"


def test_scenario_c_action_mapped_template_missing(monkeypatch):
    """Scenario C: Action mapped, but template is missing -> raises CodeGenerationError."""
    pipeline = CompilerPipeline()
    ctx = _build_context()

    # Intentionally point an action to a missing template
    monkeypatch.setitem(
        pipeline.physical_planner.SQL_STRATEGIES,
        ActionType.DROP_COLUMN,
        ("tsql_drop", "missing_non_existent.j2"),
    )
    ctx.structured_actions = [
        StructuredAction(action_type=ActionType.DROP_COLUMN, dataset_name="Employee", target_columns=["email"]),
    ]
    ctx.metadata["ingestion_telemetry"] = {
        "assessment_discovered_count": 1,
        "mapped_action_count": 1,
        "mapped_issue_count": 1,
        "unsupported_issue_count": 0,
    }

    with pytest.raises(CodeGenerationError) as exc_info:
        pipeline.compile(ctx)

    assert "could not be rendered" in str(exc_info.value)
    assert exc_info.value.failure_stage == "template_validation"


def test_scenario_d_strict_vs_preview_mode():
    """Scenario D: 20 detected, 12 mapped, 8 unsupported.
    Strict mode fails; preview mode allows partial non-executable generation.
    """
    pipeline = CompilerPipeline()

    # 1. Strict mode (default) -> must raise CodeGenerationError
    ctx_strict = _build_context()
    ctx_strict.structured_actions = [
        StructuredAction(action_type=ActionType.TRIM, dataset_name="Employee", target_columns=["name"]),
    ]
    ctx_strict.metadata["ingestion_telemetry"] = {
        "assessment_discovered_count": 20,
        "mapped_action_count": 12,
        "mapped_issue_count": 12,
        "unsupported_issue_count": 8,
    }

    with pytest.raises(CodeGenerationError) as exc_info:
        pipeline.compile(ctx_strict)

    assert "8 assessment issue(s) have no supported remediation" in str(exc_info.value)
    assert exc_info.value.failure_stage == "capability_validation"

    # 2. Preview mode -> generates partial artifact with executable=False
    ctx_preview = _build_context()
    ctx_preview.options["preview_mode"] = True
    ctx_preview.structured_actions = [
        StructuredAction(action_type=ActionType.TRIM, dataset_name="Employee", target_columns=["name"]),
    ]
    ctx_preview.metadata["ingestion_telemetry"] = {
        "assessment_discovered_count": 20,
        "mapped_action_count": 12,
        "mapped_issue_count": 12,
        "unsupported_issue_count": 8,
    }

    compiled_ctx, _ = pipeline.compile(ctx_preview)
    telemetry = compiled_ctx.metadata.get("compilation_telemetry", {})

    assert telemetry["ok"] is True
    assert telemetry["executable"] is False
    assert telemetry["partial"] is True
    assert telemetry["unsupported_issue_count"] == 8
    assert telemetry["rendered_transformation_count"] == 1
    assert len(telemetry["warnings"]) > 0


def test_scenario_e_zero_issues_pass_through():
    """Scenario E: No issues detected -> clean pass-through clearly labeled."""
    pipeline = CompilerPipeline()
    ctx = _build_context()

    ctx.metadata["ingestion_telemetry"] = {
        "assessment_discovered_count": 0,
        "mapped_action_count": 0,
        "mapped_issue_count": 0,
        "unsupported_issue_count": 0,
    }

    compiled_ctx, _ = pipeline.compile(ctx)
    telemetry = compiled_ctx.metadata.get("compilation_telemetry", {})

    assert telemetry["ok"] is True
    assert telemetry["executable"] is True
    assert telemetry["pipeline_type"] == "pass_through"
    assert "No cleaning transformations were required" in telemetry["message"]


def test_end_to_end_lineage_and_accounting_invariants():
    """Validates end-to-end lineage IDs and strict accounting across all layers."""
    from agent.compiler.planners.ingestion_bridge import IngestionBridge

    assessment = {
        "datasets": {
            "Employee": {
                "columns": {
                    "Email": {"dtype": "varchar(100)"},
                    "Phone": {"dtype": "varchar(20)"},
                },
                "quality": {
                    "issues": [
                        {"column": "Email", "issue_type": "invalid_email", "severity": "medium"},
                        {"column": "Phone", "issue_type": "invalid_phone", "severity": "medium"},
                    ]
                }
            }
        }
    }

    bridge = IngestionBridge(default_schema="dbo")
    cols = {
        "Email": ColumnProfile(name="Email", dtype="string"),
        "Phone": ColumnProfile(name="Phone", dtype="string"),
    }
    profiles = {"Employee": DatasetProfile(dataset_id="Employee", columns=cols)}

    deduped, rules, telemetry = bridge.extract_and_normalize(assess=assessment, plan={}, known_datasets={"Employee"})
    detected_issues, structured_actions = bridge.bridge_to_compiler_models(deduped)

    # Ingestion accounting invariant
    assert (
        telemetry["mapped_issue_count"]
        + telemetry["unsupported_issue_count"]
        + telemetry["manual_review_issue_count"]
        == telemetry["assessment_discovered_count"]
    )

    ctx = CompilerContext(
        session_id="lineage_test",
        target_engine=ExecutionEngine.AZURE_SQL,
        dataset_profiles=profiles,
        detected_issues=detected_issues,
        structured_actions=structured_actions,
    )
    ctx.metadata["ingestion_telemetry"] = telemetry

    pipeline = CompilerPipeline()
    compiled_ctx, artifact = pipeline.compile(ctx)
    comp_telem = compiled_ctx.metadata.get("compilation_telemetry", {})

    # Planner Lineage invariant
    assert len(compiled_ctx.logical_plan.steps) == len(structured_actions)
    assert len(compiled_ctx.physical_plan.steps) == len(structured_actions)
    for l_step, p_step in zip(compiled_ctx.logical_plan.steps, compiled_ctx.physical_plan.steps):
        assert l_step.source_action_id is not None
        assert p_step.source_logical_step_id == l_step.step_id
        assert p_step.source_action_id == l_step.source_action_id
        assert len(p_step.source_issue_ids) > 0

    # Compilation accounting invariant
    assert (
        comp_telem["rendered_transformation_count"]
        + comp_telem["template_missing_count"]
        + comp_telem["parameter_missing_count"]
        + comp_telem["render_failed_count"]
        + comp_telem["skipped_step_count"]
        + comp_telem["unsupported_step_count"]
        == comp_telem["physical_step_count"]
    )
    assert comp_telem["rendered_transformation_count"] == 2
    assert comp_telem["infrastructure_statement_count"] >= 3

