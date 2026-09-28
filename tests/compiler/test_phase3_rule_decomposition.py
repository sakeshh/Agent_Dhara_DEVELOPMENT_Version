from __future__ import annotations

import json
from unittest.mock import MagicMock
import pytest

from agent.compiler.models.actions import (
    ActionSource,
    ComparisonOperator,
    StructuredAction,
)
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.physical.physical_planner import PhysicalPlanner
from agent.compiler.planners.logical_planner import LogicalPlanner
from agent.compiler.router.language_router import LanguageRouter
from agent.compiler.rules.decomposer import RuleDecomposer
from agent.compiler.rules.models import DecompositionStatus
from agent.compiler.validators.sql_validator import SQLValidator


@pytest.fixture
def sample_dataset_profiles() -> dict[str, DatasetProfile]:
    return {
        "Employees": DatasetProfile(
            dataset_name="Employees",
            columns={
                "EmployeeCode": ColumnProfile(name="EmployeeCode", dtype="nvarchar(50)"),
                "EmployeeName": ColumnProfile(name="EmployeeName", dtype="nvarchar(100)"),
                "AccountCode": ColumnProfile(name="AccountCode", dtype="nvarchar(50)"),
                "Department": ColumnProfile(name="Department", dtype="nvarchar(50)"),
                "EmployeeType": ColumnProfile(name="EmployeeType", dtype="nvarchar(50)"),
                "Status": ColumnProfile(name="Status", dtype="nvarchar(20)"),
            },
        )
    }


class TestPhase3RuleDecomposition:
    """Test suite validating Phase 3 Business-Rule Decomposition criteria (Tests 1 through 15)."""

    def test_1_deterministic_prefix(self, sample_dataset_profiles):
        """Test 1: 'EmployeeCode must start with 'EMP-'' decomposes to PREFIX_ADD."""
        decomposer = RuleDecomposer()
        res = decomposer.decompose(
            "EmployeeCode must start with 'EMP-'",
            "Employees",
            sample_dataset_profiles,
        )
        assert res.status == DecompositionStatus.RESOLVED
        assert len(res.actions) == 1
        act = res.actions[0]
        assert act.action_type == ActionType.PREFIX_ADD
        assert act.target_columns == ["EmployeeCode"]
        assert act.parameters["prefix"] == "EMP-"
        assert act.parameters["only_if_missing"] is True
        assert act.source == ActionSource.BUSINESS_RULE
        assert res.extraction_method == "deterministic"

    def test_2_deterministic_suffix(self, sample_dataset_profiles):
        """Test 2: 'EmployeeCode must end with '-IN'' decomposes to SUFFIX_ADD."""
        decomposer = RuleDecomposer()
        res = decomposer.decompose(
            "EmployeeCode must end with '-IN'",
            "Employees",
            sample_dataset_profiles,
        )
        assert res.status == DecompositionStatus.RESOLVED
        assert len(res.actions) == 1
        act = res.actions[0]
        assert act.action_type == ActionType.SUFFIX_ADD
        assert act.target_columns == ["EmployeeCode"]
        assert act.parameters["suffix"] == "-IN"
        assert res.extraction_method == "deterministic"

    def test_3_deterministic_padding(self, sample_dataset_profiles):
        """Test 3: 'Left pad EmployeeCode with zeros to length 8' decomposes to PAD."""
        decomposer = RuleDecomposer()
        res = decomposer.decompose(
            "Left pad EmployeeCode with zeros to length 8",
            "Employees",
            sample_dataset_profiles,
        )
        assert res.status == DecompositionStatus.RESOLVED
        assert len(res.actions) == 1
        act = res.actions[0]
        assert act.action_type == ActionType.PAD
        assert act.target_columns == ["EmployeeCode"]
        assert act.parameters["length"] == 8
        assert act.parameters["fill_character"] == "0"
        assert act.parameters["side"] == "left"

    def test_4_deterministic_replacement(self, sample_dataset_profiles):
        """Test 4: 'Replace '-' with '/' in AccountCode' decomposes to PATTERN_REPLACE."""
        decomposer = RuleDecomposer()
        res = decomposer.decompose(
            "Replace '-' with '/' in AccountCode",
            "Employees",
            sample_dataset_profiles,
        )
        assert res.status == DecompositionStatus.RESOLVED
        assert len(res.actions) == 1
        act = res.actions[0]
        assert act.action_type == ActionType.PATTERN_REPLACE
        assert act.target_columns == ["AccountCode"]
        assert act.parameters["pattern"] == "-"
        assert act.parameters["replacement"] == "/"
        assert act.parameters["regex"] is False

    def test_5_deterministic_value_mapping(self, sample_dataset_profiles):
        """Test 5: 'Map A to Active and I to Inactive in Status' decomposes to VALUE_MAP."""
        decomposer = RuleDecomposer()
        res = decomposer.decompose(
            "Map A to Active and I to Inactive in Status",
            "Employees",
            sample_dataset_profiles,
        )
        assert res.status == DecompositionStatus.RESOLVED
        assert len(res.actions) == 1
        act = res.actions[0]
        assert act.action_type == ActionType.VALUE_MAP
        assert act.target_columns == ["Status"]
        assert act.parameters["mappings"] == {"A": "Active", "I": "Inactive"}
        assert act.parameters["preserve_unmapped"] is True

    def test_6_deterministic_conditional_rule(self, sample_dataset_profiles):
        """Test 6: 'If Department equals Contractor, set EmployeeType to External' decomposes to structured CONDITIONAL_UPDATE."""
        decomposer = RuleDecomposer()
        res = decomposer.decompose(
            "If Department equals Contractor, set EmployeeType to External",
            "Employees",
            sample_dataset_profiles,
        )
        assert res.status == DecompositionStatus.RESOLVED
        assert len(res.actions) == 1
        act = res.actions[0]
        assert act.action_type == ActionType.CONDITIONAL_UPDATE
        assert act.target_columns == ["EmployeeType"]
        cond = act.parameters["condition"]
        assert cond["column"] == "Department"
        assert cond["operator"] == "EQ"
        assert cond["value"] == "Contractor"
        assert act.parameters["new_value"] == "External"

    def test_7_unknown_column_fails_validation(self, sample_dataset_profiles):
        """Test 7: 'Prefix FakeColumn with EMP-' fails column resolution and returns INVALID."""
        decomposer = RuleDecomposer()
        res = decomposer.decompose(
            "Prefix FakeColumn with EMP-",
            "Employees",
            sample_dataset_profiles,
        )
        assert res.status == DecompositionStatus.INVALID
        assert any("Unknown target column" in d for d in res.diagnostics)

    def test_8_missing_parameter_is_ambiguous(self, sample_dataset_profiles):
        """Test 8: 'EmployeeCode should have a prefix' is detected as AMBIGUOUS, not guessed."""
        decomposer = RuleDecomposer()
        res = decomposer.decompose(
            "EmployeeCode should have a prefix.",
            "Employees",
            sample_dataset_profiles,
        )
        assert res.status == DecompositionStatus.AMBIGUOUS
        assert len(res.missing_information) > 0

    def test_9_unsupported_transformation(self, sample_dataset_profiles):
        """Test 9: 'Apply our proprietary regional normalization algorithm' is flagged as UNSUPPORTED."""
        decomposer = RuleDecomposer()
        res = decomposer.decompose(
            "Apply our proprietary regional normalization algorithm.",
            "Employees",
            sample_dataset_profiles,
        )
        assert res.status == DecompositionStatus.UNSUPPORTED
        assert any("proprietary" in d for d in res.diagnostics)

    def test_10_multi_action_rule_with_dependencies(self, sample_dataset_profiles):
        """Test 10: 'Trim EmployeeCode, pad with zeros to 8, then prefix EMP-' produces 3 actions with dependency chain."""
        decomposer = RuleDecomposer()
        res = decomposer.decompose(
            "Trim EmployeeCode, pad with zeros to 8, then prefix EMP-",
            "Employees",
            sample_dataset_profiles,
        )
        assert res.status == DecompositionStatus.RESOLVED
        assert len(res.actions) == 3

        act1, act2, act3 = res.actions
        assert act1.action_type == ActionType.TRIM
        assert act2.action_type == ActionType.PAD
        assert act3.action_type == ActionType.PREFIX_ADD

        # Dependency chain verification
        step1_id = act1.metadata["step_id"]
        step2_id = act2.metadata["step_id"]
        assert act2.metadata["depends_on_step_ids"] == [step1_id]
        assert act3.metadata["depends_on_step_ids"] == [step2_id]

    def test_11_deterministic_path_does_not_invoke_llm(self, sample_dataset_profiles):
        """Test 11: Obvious rules match Tier 1 and NEVER call the LLM (call count = 0)."""
        mock_llm = MagicMock(return_value="{}")
        decomposer = RuleDecomposer(llm_callable=mock_llm)

        res = decomposer.decompose(
            "EmployeeCode must start with 'EMP-'",
            "Employees",
            sample_dataset_profiles,
        )
        assert res.status == DecompositionStatus.RESOLVED
        assert res.extraction_method == "deterministic"
        mock_llm.assert_not_called()

    def test_12_llm_schema_enforcement_rejects_unrecognized_action(self, sample_dataset_profiles):
        """Test 12: If LLM returns an unrecognized action_type (MAGICAL_CLEANING), it is rejected as UNSUPPORTED."""
        llm_resp = json.dumps({
            "action_type": "MAGICAL_CLEANING",
            "target_columns": ["EmployeeCode"],
            "parameters": {},
        })
        mock_llm = MagicMock(return_value=llm_resp)
        decomposer = RuleDecomposer(llm_callable=mock_llm)

        # Rule that bypasses Tier 1 regex
        res = decomposer.decompose(
            "Perform advanced format transformation on EmployeeCode",
            "Employees",
            sample_dataset_profiles,
        )
        assert res.status == DecompositionStatus.UNSUPPORTED
        assert any("unrecognized action_type" in d for d in res.diagnostics)

    def test_13_llm_hallucinated_column_rejected(self, sample_dataset_profiles):
        """Test 13: If LLM references a non-existent column (EmployeeIdentifier), it is rejected as INVALID."""
        llm_resp = json.dumps({
            "action_type": "prefix_add",
            "target_columns": ["EmployeeIdentifier"],  # Not in schema!
            "parameters": {"prefix": "EMP-"},
        })
        mock_llm = MagicMock(return_value=llm_resp)
        decomposer = RuleDecomposer(llm_callable=mock_llm)

        res = decomposer.decompose(
            "Format the identifier to start with EMP-",
            "Employees",
            sample_dataset_profiles,
        )
        assert res.status == DecompositionStatus.INVALID
        assert any("hallucinated or referenced non-existent column" in d for d in res.diagnostics)

    def test_14_llm_produced_code_prohibited(self, sample_dataset_profiles):
        """Test 14: If LLM attempts to output executable SQL (DROP TABLE, UPDATE), it is blocked immediately."""
        llm_resp = "DROP TABLE Employees; -- {action_type: 'prefix_add'}"
        mock_llm = MagicMock(return_value=llm_resp)
        decomposer = RuleDecomposer(llm_callable=mock_llm)

        res = decomposer.decompose(
            "Execute custom cleanup on EmployeeCode",
            "Employees",
            sample_dataset_profiles,
        )
        assert res.status == DecompositionStatus.INVALID
        assert any("Prohibited executable SQL" in d for d in res.diagnostics)

    def test_15_end_to_end_from_business_rule_to_tsql(self, sample_dataset_profiles):
        """
        Test 15: Full end-to-end pipeline:
        Business Rule -> RuleDecomposer -> StructuredAction -> LogicalPlanner -> PhysicalPlanner -> Jinja2 -> SQLValidator
        """
        decomposer = RuleDecomposer()
        planner = LogicalPlanner()
        phys_planner = PhysicalPlanner()
        codegen = LanguageRouter.create_default().get_agent(ExecutionEngine.AZURE_SQL)
        validator = SQLValidator()

        # 1. Rule Decomposition
        decomp_res = decomposer.decompose(
            "EmployeeCode must start with 'EMP-'",
            "Employees",
            sample_dataset_profiles,
        )
        assert decomp_res.status == DecompositionStatus.RESOLVED
        assert len(decomp_res.actions) == 1

        # 2. Context creation
        context = CompilerContext(
            session_id="phase3_e2e_session",
            dataset_profiles=sample_dataset_profiles,
            target_engine=ExecutionEngine.AZURE_SQL,
            structured_actions=decomp_res.actions,
        )

        # 3. Logical planning
        context = planner.execute(context)
        assert context.logical_plan is not None
        assert context.logical_plan.steps[0].action == ActionType.PREFIX_ADD

        # 4. Physical planning
        context = phys_planner.execute(context)
        assert context.physical_plan is not None
        assert context.physical_plan.steps[0].strategy_name == "tsql_prefix_add"

        # 5. SQL generation
        artifact = codegen.generate(context.physical_plan, context)
        context.code_artifact = artifact
        assert artifact is not None
        assert "WHEN [EmployeeCode] LIKE N'EMP-%' THEN [EmployeeCode]" in artifact.content
        assert "ELSE CONCAT(N'EMP-', [EmployeeCode])" in artifact.content

        # 6. SQL validation
        val_result = validator.validate(artifact, context)
        assert val_result.is_valid is True
        assert val_result.error_count == 0
        assert not val_result.has_blocking_errors
