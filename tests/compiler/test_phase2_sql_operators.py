from __future__ import annotations

import pytest

from agent.compiler.router.language_router import LanguageRouter
from agent.compiler.models.actions import (
    ActionSource,
    ConditionalUpdateParameters,
    PadParameters,
    PatternReplaceParameters,
    PrefixAddParameters,
    StructuredAction,
    SuffixAddParameters,
    ValueMapParameters,
)
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType, LogicalStep
from agent.compiler.models.physical import ExecutionEngine, PhysicalPlan, PhysicalStep
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.physical.physical_planner import PhysicalPlanner
from agent.compiler.planners.logical_planner import LogicalPlanner
from agent.compiler.templates.engine import TemplateEngine
from agent.compiler.validators.sql_validator import SQLValidator
from agent.errors import UnsupportedCapabilityError


@pytest.fixture
def sample_dataset_profiles() -> dict[str, DatasetProfile]:
    return {
        "Employees": DatasetProfile(
            dataset_name="Employees",
            columns={
                "EmployeeCode": ColumnProfile(name="EmployeeCode", dtype="nvarchar(50)"),
                "EmployeeName": ColumnProfile(name="EmployeeName", dtype="nvarchar(100)"),
                "Department": ColumnProfile(name="Department", dtype="nvarchar(50)"),
                "Gender": ColumnProfile(name="Gender", dtype="nvarchar(10)"),
                "Status": ColumnProfile(name="Status", dtype="nvarchar(20)"),
                "Notes": ColumnProfile(name="Notes", dtype="nvarchar(255)"),
            },
        )
    }


class TestPhase2SQLOperators:
    """Test suite validating Phase 2 Azure SQL physical implementations and templates."""

    def test_layer_a_physical_translation_all_six(self):
        """Layer A: Each of the 6 logical operators translates to a valid PhysicalStep."""
        planner = PhysicalPlanner()

        steps = [
            LogicalStep(
                step_id="s1",
                dataset_name="Employees",
                action=ActionType.PREFIX_ADD,
                target_columns=["EmployeeCode"],
                parameters={"prefix": "EMP-", "only_if_missing": True},
            ),
            LogicalStep(
                step_id="s2",
                dataset_name="Employees",
                action=ActionType.SUFFIX_ADD,
                target_columns=["EmployeeCode"],
                parameters={"suffix": "_HQ", "only_if_missing": True},
            ),
            LogicalStep(
                step_id="s3",
                dataset_name="Employees",
                action=ActionType.PATTERN_REPLACE,
                target_columns=["Notes"],
                parameters={"pattern": "old_dept", "replacement": "new_dept", "regex": False},
            ),
            LogicalStep(
                step_id="s4",
                dataset_name="Employees",
                action=ActionType.VALUE_MAP,
                target_columns=["Gender"],
                parameters={"mappings": {"M": "Male", "F": "Female"}, "preserve_unmapped": True},
            ),
            LogicalStep(
                step_id="s5",
                dataset_name="Employees",
                action=ActionType.PAD,
                target_columns=["EmployeeCode"],
                parameters={"length": 8, "fill_character": "0", "side": "left"},
            ),
            LogicalStep(
                step_id="s6",
                dataset_name="Employees",
                action=ActionType.CONDITIONAL_UPDATE,
                target_columns=["Status"],
                parameters={"condition": {"column": "Status", "operator": "IS_NULL", "value": None}, "new_value": "ACTIVE"},
            ),
        ]

        expected_strategies = [
            ("tsql_prefix_add", "prefix_add.j2"),
            ("tsql_suffix_add", "suffix_add.j2"),
            ("tsql_pattern_replace", "pattern_replace.j2"),
            ("tsql_value_map", "value_map.j2"),
            ("tsql_pad", "pad.j2"),
            ("tsql_conditional_update", "conditional_update.j2"),
        ]

        for step, (exp_strat, exp_tmpl) in zip(steps, expected_strategies):
            phys_step = planner._map_step(step, ExecutionEngine.AZURE_SQL, 0)
            assert phys_step.strategy_name == exp_strat
            assert phys_step.template_name == exp_tmpl
            assert phys_step.dataset_name == "Employees"
            assert phys_step.target_columns == step.target_columns

    def test_layer_b_prefix_add_rendering_and_semantics(self):
        """Layer B & C: PREFIX_ADD renders correctly, handles apostrophes, NULLs, and idempotency."""
        engine = TemplateEngine.get_instance()

        rendered = engine.render(
            "sql/prefix_add.j2",
            step_id="step_prefix_1",
            description="Add EMP prefix",
            dataset_name="Employees",
            columns=["EmployeeCode"],
            prefix="O'Brien-",  # Apostrophe test
            only_if_missing=True,
        )

        assert "UPDATE [dbo].[Employees_Staging]" in rendered
        assert "WHEN [EmployeeCode] IS NULL THEN NULL" in rendered
        assert "WHEN [EmployeeCode] LIKE N'O''Brien-%' THEN [EmployeeCode]" in rendered
        assert "ELSE CONCAT(N'O''Brien-', [EmployeeCode])" in rendered

    def test_layer_b_suffix_add_rendering_and_semantics(self):
        """Layer B & C: SUFFIX_ADD renders correctly with NULL safety and idempotency."""
        engine = TemplateEngine.get_instance()

        rendered = engine.render(
            "sql/suffix_add.j2",
            step_id="step_suffix_1",
            description="Add HQ suffix",
            dataset_name="Employees",
            columns=["EmployeeCode"],
            suffix="_HQ",
            only_if_missing=True,
        )

        assert "UPDATE [dbo].[Employees_Staging]" in rendered
        assert "WHEN [EmployeeCode] IS NULL THEN NULL" in rendered
        assert "WHEN [EmployeeCode] LIKE N'%_HQ' THEN [EmployeeCode]" in rendered
        assert "ELSE CONCAT([EmployeeCode], N'_HQ')" in rendered

    def test_layer_b_pad_no_unexpected_truncation(self):
        """Layer B & C: PAD preserves values longer than requested length without truncation."""
        engine = TemplateEngine.get_instance()

        rendered = engine.render(
            "sql/pad.j2",
            step_id="step_pad_1",
            description="Pad EmployeeCode to 8 digits",
            dataset_name="Employees",
            columns=["EmployeeCode"],
            length=8,
            fill_character="0",
            side="left",
        )

        assert "UPDATE [dbo].[Employees_Staging]" in rendered
        assert "WHEN [EmployeeCode] IS NULL THEN NULL" in rendered
        assert "WHEN LEN([EmployeeCode]) >= 8 THEN [EmployeeCode]" in rendered
        assert "ELSE CONCAT(REPLICATE(N'0', 8 - LEN([EmployeeCode])), [EmployeeCode])" in rendered

        # Right-pad test
        rendered_r = engine.render(
            "sql/pad.j2",
            step_id="step_pad_2",
            description="Right pad",
            dataset_name="Employees",
            columns=["EmployeeCode"],
            length=10,
            fill_character="X",
            side="right",
        )
        assert "ELSE CONCAT([EmployeeCode], REPLICATE(N'X', 10 - LEN([EmployeeCode])))" in rendered_r

    def test_layer_b_value_map_unmapped_treatment(self):
        """Layer B & C: VALUE_MAP respects preserve_unmapped flag and maintains NULL safety."""
        engine = TemplateEngine.get_instance()

        # preserve_unmapped = True -> ELSE [col]
        rendered_preserve = engine.render(
            "sql/value_map.j2",
            step_id="step_vmap_1",
            description="Map Gender",
            dataset_name="Employees",
            columns=["Gender"],
            mappings={"M": "Male", "F": "Female"},
            preserve_unmapped=True,
        )
        assert "WHEN [Gender] IS NULL THEN NULL" in rendered_preserve
        assert "WHEN [Gender] = N'M' THEN N'Male'" in rendered_preserve
        assert "WHEN [Gender] = N'F' THEN N'Female'" in rendered_preserve
        assert "ELSE [Gender]" in rendered_preserve

        # preserve_unmapped = False -> ELSE NULL
        rendered_discard = engine.render(
            "sql/value_map.j2",
            step_id="step_vmap_2",
            description="Map Gender Discard Unknown",
            dataset_name="Employees",
            columns=["Gender"],
            mappings={"M": "Male", "F": "Female"},
            preserve_unmapped=False,
        )
        assert "ELSE NULL" in rendered_discard

    def test_layer_b_pattern_replace_literal_vs_unsupported_regex(self):
        """Layer B & C: PATTERN_REPLACE allows literal replacement but rejects unsupported POSIX regex."""
        planner = PhysicalPlanner()
        engine = TemplateEngine.get_instance()

        # 1. Literal replacement works cleanly
        step_literal = LogicalStep(
            step_id="step_pat_lit",
            dataset_name="Employees",
            action=ActionType.PATTERN_REPLACE,
            target_columns=["Notes"],
            parameters={"pattern": "old_dept", "replacement": "new_dept", "regex": False},
        )
        phys_step = planner._map_step(step_literal, ExecutionEngine.AZURE_SQL, 0)
        assert phys_step.strategy_name == "tsql_pattern_replace"

        rendered = engine.render(
            f"sql/{phys_step.template_name}",
            **phys_step.template_parameters,
            step_id=phys_step.step_id,
            description="Replace department",
        )
        assert "ELSE REPLACE([Notes], N'old_dept', N'new_dept')" in rendered

        # 2. True regex replacement is rejected with UnsupportedCapabilityError (does not fake regex!)
        step_regex = LogicalStep(
            step_id="step_pat_regex",
            dataset_name="Employees",
            action=ActionType.PATTERN_REPLACE,
            target_columns=["Notes"],
            parameters={"pattern": r"\d{3}-\d{2}-\d{4}", "replacement": "***", "regex": True},
        )
        with pytest.raises(UnsupportedCapabilityError, match="Azure SQL does not support POSIX regular expression"):
            planner._map_step(step_regex, ExecutionEngine.AZURE_SQL, 0)

    def test_layer_b_conditional_update_structured_vs_arbitrary_rejection(self):
        """Layer B & C: CONDITIONAL_UPDATE translates structured conditions and rejects arbitrary expressions."""
        planner = PhysicalPlanner()
        engine = TemplateEngine.get_instance()

        # 1. Valid structured condition
        step_struct = LogicalStep(
            step_id="step_cond_1",
            dataset_name="Employees",
            action=ActionType.CONDITIONAL_UPDATE,
            target_columns=["Status"],
            parameters={
                "condition": {"column": "Department", "operator": "EQ", "value": "Contractor"},
                "new_value": "EXTERNAL",
                "else_value": "INTERNAL",
            },
        )
        phys_step = planner._map_step(step_struct, ExecutionEngine.AZURE_SQL, 0)
        assert phys_step.template_parameters["sql_condition"] == "[Department] = N'Contractor'"

        rendered = engine.render(
            f"sql/{phys_step.template_name}",
            **phys_step.template_parameters,
            step_id=phys_step.step_id,
            description="Update contractor status",
        )
        assert "WHEN [Department] = N'Contractor' THEN N'EXTERNAL'" in rendered
        assert "ELSE N'INTERNAL'" in rendered

        # 2. Arbitrary SQL injection attempt is rejected
        step_unsafe = LogicalStep(
            step_id="step_cond_unsafe",
            dataset_name="Employees",
            action=ActionType.CONDITIONAL_UPDATE,
            target_columns=["Status"],
            parameters={
                "condition": "1=1; DROP TABLE Employees; --",
                "new_value": "HACKED",
            },
        )
        with pytest.raises(UnsupportedCapabilityError, match="rejects arbitrary unvalidated expression"):
            planner._map_step(step_unsafe, ExecutionEngine.AZURE_SQL, 0)

    def test_layer_d_end_to_end_pipeline_and_sql_validator(self, sample_dataset_profiles):
        """Layer D: End-to-end execution through SQLCodegenAgent and SQLValidator produces valid stored procedure."""
        planner = LogicalPlanner()
        phys_planner = PhysicalPlanner()
        codegen = LanguageRouter.create_default().get_agent(ExecutionEngine.AZURE_SQL)
        validator = SQLValidator()

        actions = [
            StructuredAction(
                action_type=ActionType.PREFIX_ADD,
                dataset_name="Employees",
                target_columns=["EmployeeCode"],
                parameters={"prefix": "EMP-", "only_if_missing": True},
                source=ActionSource.BUSINESS_RULE,
            ),
            StructuredAction(
                action_type=ActionType.PAD,
                dataset_name="Employees",
                target_columns=["EmployeeCode"],
                parameters={"length": 8, "fill_character": "0", "side": "left"},
                source=ActionSource.BUSINESS_RULE,
            ),
            StructuredAction(
                action_type=ActionType.VALUE_MAP,
                dataset_name="Employees",
                target_columns=["Gender"],
                parameters={"mappings": {"M": "Male", "F": "Female"}, "preserve_unmapped": True},
                source=ActionSource.BUSINESS_RULE,
            ),
        ]

        context = CompilerContext(
            session_id="phase2_e2e_session",
            dataset_profiles=sample_dataset_profiles,
            target_engine=ExecutionEngine.AZURE_SQL,
            structured_actions=actions,
        )

        # 1. Logical planning
        context = planner.execute(context)
        assert context.logical_plan is not None

        # 2. Physical planning
        context = phys_planner.execute(context)
        assert context.physical_plan is not None
        assert len(context.physical_plan.steps) == 3

        # 3. SQL Code generation
        artifact = codegen.generate(context.physical_plan, context)
        context.code_artifact = artifact
        assert artifact is not None
        assert "CREATE OR ALTER PROCEDURE [dbo].[sp_etl_" in artifact.content

        # 4. SQL Validation
        val_result = validator.validate(artifact, context)
        assert val_result.is_valid is True
        assert val_result.error_count == 0
        assert not val_result.has_blocking_errors
