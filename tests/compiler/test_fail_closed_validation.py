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
    assert telemetry["rendered_step_count"] == 2
    assert telemetry["unsupported_issue_count"] == 0
    assert telemetry["fallback_projection_used"] is False


def test_scenario_b_issues_detected_none_mapped():
    """Scenario B: Issues detected, none mapped -> raises CodeGenerationError."""
    pipeline = CompilerPipeline()
    ctx = _build_context()

    ctx.metadata["ingestion_telemetry"] = {
        "assessment_discovered_count": 5,
        "mapped_action_count": 0,
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
        "unsupported_issue_count": 8,
    }

    compiled_ctx, _ = pipeline.compile(ctx_preview)
    telemetry = compiled_ctx.metadata.get("compilation_telemetry", {})

    assert telemetry["ok"] is True
    assert telemetry["executable"] is False
    assert telemetry["partial"] is True
    assert telemetry["unsupported_issue_count"] == 8
    assert len(telemetry["warnings"]) > 0


def test_scenario_e_zero_issues_pass_through():
    """Scenario E: No issues detected -> clean pass-through clearly labeled."""
    pipeline = CompilerPipeline()
    ctx = _build_context()

    ctx.metadata["ingestion_telemetry"] = {
        "assessment_discovered_count": 0,
        "mapped_action_count": 0,
        "unsupported_issue_count": 0,
    }

    compiled_ctx, _ = pipeline.compile(ctx)
    telemetry = compiled_ctx.metadata.get("compilation_telemetry", {})

    assert telemetry["ok"] is True
    assert telemetry["executable"] is True
    assert telemetry["pipeline_type"] == "pass_through"
    assert "No cleaning transformations were required" in telemetry["message"]
