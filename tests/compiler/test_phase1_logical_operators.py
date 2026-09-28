from __future__ import annotations

import pytest

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
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType, DataEffect, LogicalStep
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.planners.logical_planner import LogicalPlanner


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
                "Salary": ColumnProfile(name="Salary", dtype="decimal(18,2)", inferred_type="numeric"),
            },
        )
    }


class TestPhase1LogicalOperators:
    """Test suite validating Phase 1 Composable Logical Operators acceptance criteria."""

    def test_a_all_six_operators_produce_correct_logical_steps(self, sample_dataset_profiles):
        """Test A: Each of the 6 composable operators adapts into a typed LogicalStep."""
        planner = LogicalPlanner()

        actions = [
            StructuredAction(
                action_type=ActionType.PREFIX_ADD,
                dataset_name="Employees",
                target_columns=["EmployeeCode"],
                parameters={"prefix": "EMP-", "only_if_missing": True},
                source=ActionSource.BUSINESS_RULE,
            ),
            StructuredAction(
                action_type=ActionType.SUFFIX_ADD,
                dataset_name="Employees",
                target_columns=["EmployeeCode"],
                parameters={"suffix": "_HQ", "only_if_missing": True},
                source=ActionSource.BUSINESS_RULE,
            ),
            StructuredAction(
                action_type=ActionType.PATTERN_REPLACE,
                dataset_name="Employees",
                target_columns=["EmployeeName"],
                parameters={"pattern": r"\s+", "replacement": " ", "regex": True},
                source=ActionSource.BUSINESS_RULE,
            ),
            StructuredAction(
                action_type=ActionType.VALUE_MAP,
                dataset_name="Employees",
                target_columns=["Gender"],
                parameters={"mappings": {"M": "Male", "F": "Female"}, "preserve_unmapped": True},
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
                action_type=ActionType.CONDITIONAL_UPDATE,
                dataset_name="Employees",
                target_columns=["Status"],
                parameters={"condition": "Status IS NULL", "new_value": "ACTIVE", "else_value": None},
                source=ActionSource.BUSINESS_RULE,
            ),
        ]

        steps = [planner.action_to_logical_step(act, idx, sample_dataset_profiles) for idx, act in enumerate(actions)]

        assert len(steps) == 6
        expected_actions = [
            ActionType.PREFIX_ADD,
            ActionType.SUFFIX_ADD,
            ActionType.PATTERN_REPLACE,
            ActionType.VALUE_MAP,
            ActionType.PAD,
            ActionType.CONDITIONAL_UPDATE,
        ]
        for step, exp_act in zip(steps, expected_actions):
            assert isinstance(step, LogicalStep)
            assert step.action == exp_act
            assert step.dataset_name == "Employees"
            assert step.effect == DataEffect.TRANSFORM_IN_PLACE

    def test_b_parameter_preservation(self, sample_dataset_profiles):
        """Test B: All operator parameters are preserved without loss during adaptation."""
        planner = LogicalPlanner()

        pad_action = StructuredAction(
            action_type=ActionType.PAD,
            dataset_name="Employees",
            target_columns=["EmployeeCode"],
            parameters={"length": 8, "fill_character": "0", "side": "left"},
            source=ActionSource.BUSINESS_RULE,
        )
        pad_step = planner.action_to_logical_step(pad_action, 0, sample_dataset_profiles)
        assert pad_step.parameters["length"] == 8
        assert pad_step.parameters["fill_character"] == "0"
        assert pad_step.parameters["side"] == "left"

        vmap_action = StructuredAction(
            action_type=ActionType.VALUE_MAP,
            dataset_name="Employees",
            target_columns=["Gender"],
            parameters={"mappings": {"M": "Male", "F": "Female"}, "preserve_unmapped": False},
            source=ActionSource.BUSINESS_RULE,
        )
        vmap_step = planner.action_to_logical_step(vmap_action, 1, sample_dataset_profiles)
        assert vmap_step.parameters["mappings"] == {"M": "Male", "F": "Female"}
        assert vmap_step.parameters["preserve_unmapped"] is False

    def test_c_provenance_preservation(self, sample_dataset_profiles):
        """Test C: Source, source_reference, and extraction_method are preserved on LogicalStep."""
        planner = LogicalPlanner()

        action = StructuredAction(
            action_type=ActionType.PREFIX_ADD,
            dataset_name="Employees",
            target_columns=["EmployeeCode"],
            parameters={"prefix": "EMP-"},
            source=ActionSource.BUSINESS_RULE,
            source_reference="RULE-BR-2026",
            extraction_method="llm",
            confidence=0.95,
            requires_approval=True,
        )
        step = planner.action_to_logical_step(action, 0, sample_dataset_profiles)

        assert step.source == "business_rule"
        assert step.source_reference == "RULE-BR-2026"
        assert step.extraction_method == "llm"
        assert step.confidence == 0.95
        assert step.requires_user_approval is True

    def test_d_semantic_ordering_and_explicit_dependencies(self, sample_dataset_profiles):
        """
        Test D: Logical order honors semantic groups (TRIM -> PAD -> PREFIX_ADD)
        and strict explicit dependency edges.
        """
        planner = LogicalPlanner()

        # Step 1: TRIM (group 1, priority 5)
        # Step 2: PAD (group 3, priority 54)
        # Step 3: PREFIX_ADD (group 3, priority 56)
        actions = [
            StructuredAction(
                action_type=ActionType.PREFIX_ADD,
                dataset_name="Employees",
                target_columns=["EmployeeCode"],
                parameters={"prefix": "EMP-"},
                source=ActionSource.BUSINESS_RULE,
            ),
            StructuredAction(
                action_type=ActionType.TRIM,
                dataset_name="Employees",
                target_columns=["EmployeeCode"],
                parameters={"side": "both"},
                source=ActionSource.BUSINESS_RULE,
            ),
            StructuredAction(
                action_type=ActionType.PAD,
                dataset_name="Employees",
                target_columns=["EmployeeCode"],
                parameters={"length": 6, "fill_character": "0", "side": "left"},
                source=ActionSource.BUSINESS_RULE,
            ),
        ]

        context = CompilerContext(
            session_id="test_sess_semantic_order",
            dataset_profiles=sample_dataset_profiles,
            structured_actions=actions,
        )
        context = planner.execute(context)
        plan = context.logical_plan
        assert plan is not None

        # Verify semantic order: TRIM (priority 5) -> PAD (priority 54) -> PREFIX_ADD (priority 56)
        plan_actions = [s.action for s in plan.steps]
        assert plan_actions == [ActionType.TRIM, ActionType.PAD, ActionType.PREFIX_ADD]

    def test_d2_explicit_dependency_overrides_priority(self, sample_dataset_profiles):
        """Explicit dependency edges override default priority weights."""
        planner = LogicalPlanner()

        # Even though PREFIX_ADD has default priority 56 and TRIM has priority 5,
        # if TRIM explicitly depends on PREFIX_ADD, PREFIX_ADD must run first!
        step_prefix = LogicalStep(
            step_id="step_prefix_1",
            dataset_name="Employees",
            action=ActionType.PREFIX_ADD,
            target_columns=["EmployeeCode"],
            parameters={"prefix": "EMP-"},
            priority=56,
        )
        step_trim = LogicalStep(
            step_id="step_trim_2",
            dataset_name="Employees",
            action=ActionType.TRIM,
            target_columns=["EmployeeCode"],
            parameters={"side": "both"},
            priority=5,
            depends_on_step_ids=["step_prefix_1"],  # Explicit dependency
        )

        sorted_steps = planner._topological_sort([step_trim, step_prefix])
        assert [s.step_id for s in sorted_steps] == ["step_prefix_1", "step_trim_2"]

    def test_e_same_priority_deterministic_tie_breaker(self):
        """Test E: Steps with equivalent priority produce stable, deterministic ordering."""
        planner = LogicalPlanner()

        # LOWERCASE and UPPERCASE both have priority 8
        step_a = LogicalStep(step_id="step_a", dataset_name="Employees", action=ActionType.LOWERCASE, priority=8)
        step_b = LogicalStep(step_id="step_b", dataset_name="Employees", action=ActionType.UPPERCASE, priority=8)

        # Run multiple times to verify strict idempotency
        for _ in range(5):
            res1 = planner._topological_sort([step_a, step_b])
            res2 = planner._topological_sort([step_a, step_b])
            assert [s.step_id for s in res1] == [s.step_id for s in res2] == ["step_a", "step_b"]

    def test_f_invalid_action_rejected_before_planning(self, sample_dataset_profiles):
        """Test F: Invalid StructuredAction (e.g. pad length <= 0) cannot become a LogicalStep."""
        planner = LogicalPlanner()

        invalid_pad_action = StructuredAction(
            action_type=ActionType.PAD,
            dataset_name="Employees",
            target_columns=["EmployeeCode"],
            parameters={"length": -5, "fill_character": "0", "side": "left"},
            source=ActionSource.BUSINESS_RULE,
        )

        with pytest.raises(ValueError, match="Invalid StructuredAction cannot enter LogicalPlanner"):
            planner.action_to_logical_step(invalid_pad_action, 0, sample_dataset_profiles)

    def test_g_cycle_detection(self):
        """Test G: Cyclic dependencies between steps are detected and raise ValueError."""
        planner = LogicalPlanner()

        step_1 = LogicalStep(
            step_id="step_1",
            dataset_name="Employees",
            action=ActionType.TRIM,
            depends_on_step_ids=["step_2"],  # Step 1 depends on Step 2
        )
        step_2 = LogicalStep(
            step_id="step_2",
            dataset_name="Employees",
            action=ActionType.PREFIX_ADD,
            depends_on_step_ids=["step_1"],  # Step 2 depends on Step 1 -> CYCLE!
        )

        with pytest.raises(ValueError, match="Dependency cycle detected"):
            planner._topological_sort([step_1, step_2])

    def test_h_level1_regression_intact(self, sample_dataset_profiles):
        """Test H: Existing detected_issues compile and sort deterministically as before."""
        from agent.compiler.models.issues import Issue, IssueCategory, IssueSeverity

        planner = LogicalPlanner()
        issues = [
            Issue(
                issue_id="iss_1",
                dataset_name="Employees",
                column_name="Salary",
                category=IssueCategory.MISSING_VALUE,
                severity=IssueSeverity.HIGH,
                message="Null salary",
            ),
            Issue(
                issue_id="iss_2",
                dataset_name="Employees",
                column_name="EmployeeName",
                category=IssueCategory.UNTRIMMED_WHITESPACE,
                severity=IssueSeverity.LOW,
                message="Untrimmed whitespace",
            ),
        ]

        context = CompilerContext(
            session_id="test_sess_level1",
            dataset_profiles=sample_dataset_profiles,
            detected_issues=issues,
        )
        context = planner.execute(context)
        plan = context.logical_plan

        assert plan is not None
        assert len(plan.steps) == 2
        # TRIM (priority 5) must precede FILL_NULLS (priority 80)
        assert plan.steps[0].action == ActionType.TRIM
        assert plan.steps[1].action == ActionType.FILL_NULLS

    def test_i_no_rendering_dependency_in_logical_layer(self, sample_dataset_profiles):
        """Test I: Logical planning executes completely engine-neutrally without Jinja2 or Codegen."""
        planner = LogicalPlanner()
        action = StructuredAction(
            action_type=ActionType.PREFIX_ADD,
            dataset_name="Employees",
            target_columns=["EmployeeCode"],
            parameters={"prefix": "EMP-"},
            source=ActionSource.BUSINESS_RULE,
        )
        context = CompilerContext(
            session_id="test_neutral_logical",
            dataset_profiles=sample_dataset_profiles,
            structured_actions=[action],
        )

        # Execute logical planning without any rendering or codegen agents
        context = planner.execute(context)
        assert context.logical_plan is not None
        assert len(context.logical_plan.steps) == 1
        assert context.code_artifact is None  # Ensures no premature code generation!
