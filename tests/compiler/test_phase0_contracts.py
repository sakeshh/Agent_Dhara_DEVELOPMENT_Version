from __future__ import annotations

import json
import pytest

from agent.compiler.models.actions import (
    ActionSource,
    PrefixAddParameters,
    PadParameters,
    StructuredAction,
)
from agent.compiler.models.execution import ExecutionMode, FailureBehavior, TransformationPolicy
from agent.compiler.models.logical import ActionType, DataEffect, LogicalStep
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.policies.row_disposition import RiskLevel, RowDisposition
from agent.compiler.validation.action_validator import ActionValidator


@pytest.fixture
def sample_dataset_profiles() -> dict[str, DatasetProfile]:
    return {
        "Employees": DatasetProfile(
            dataset_name="Employees",
            columns={
                "EmployeeCode": ColumnProfile(name="EmployeeCode", dtype="nvarchar(50)"),
                "EmployeeName": ColumnProfile(name="EmployeeName", dtype="nvarchar(100)"),
                "Salary": ColumnProfile(name="Salary", dtype="decimal(18,2)", inferred_type="numeric"),
                "IsActive": ColumnProfile(name="IsActive", dtype="boolean"),
                "BinaryPhoto": ColumnProfile(name="BinaryPhoto", dtype="varbinary(max)"),
            },
        )
    }


class TestPhase0Contracts:
    """Test suite validating Phase 0 Contract Foundation acceptance criteria."""

    def test_criterion_1_valid_known_action(self, sample_dataset_profiles):
        """Test 1: Valid known action (PREFIX_ADD, EmployeeCode, 'EMP-') should pass."""
        action = StructuredAction(
            action_type=ActionType.PREFIX_ADD,
            dataset_name="Employees",
            target_columns=["EmployeeCode"],
            parameters={"prefix": "EMP-", "only_if_missing": True},
            source=ActionSource.BUSINESS_RULE,
            extraction_method="deterministic",
        )
        validator = ActionValidator()
        result = validator.validate(
            action=action,
            dataset_profiles=sample_dataset_profiles,
            target_engine=ExecutionEngine.AZURE_SQL,
        )

        assert result.is_valid is True
        assert result.error_count == 0
        typed_params = action.get_typed_parameters()
        assert isinstance(typed_params, PrefixAddParameters)
        assert typed_params.prefix == "EMP-"
        assert typed_params.only_if_missing is True

    def test_criterion_2_unknown_column_fails_schema(self, sample_dataset_profiles):
        """Test 2: Unknown column (PREFIX_ADD, FakeColumn, 'EMP-') must fail schema validation."""
        action = StructuredAction(
            action_type=ActionType.PREFIX_ADD,
            dataset_name="Employees",
            target_columns=["FakeColumn"],
            parameters={"prefix": "EMP-"},
            source=ActionSource.BUSINESS_RULE,
        )
        validator = ActionValidator()
        result = validator.validate(
            action=action,
            dataset_profiles=sample_dataset_profiles,
            target_engine=ExecutionEngine.AZURE_SQL,
        )

        assert result.is_valid is False
        assert any(i.issue_code == "ACTION_UNKNOWN_COLUMN" for i in result.issues)

    def test_criterion_3_missing_parameter_fails_structural(self, sample_dataset_profiles):
        """Test 3: Missing parameter (PREFIX_ADD, EmployeeCode, prefix missing) must fail structural validation."""
        action = StructuredAction(
            action_type=ActionType.PREFIX_ADD,
            dataset_name="Employees",
            target_columns=["EmployeeCode"],
            parameters={},  # Missing required 'prefix'
            source=ActionSource.BUSINESS_RULE,
        )
        validator = ActionValidator()
        result = validator.validate(
            action=action,
            dataset_profiles=sample_dataset_profiles,
            target_engine=ExecutionEngine.AZURE_SQL,
        )

        assert result.is_valid is False
        assert any(i.issue_code == "ACTION_PARAMETER_INVALID" for i in result.issues)

    def test_criterion_4_invalid_parameter_fails_semantic_and_structural(self, sample_dataset_profiles):
        """Test 4: Invalid parameter (PAD, EmployeeCode, length = -10) must fail."""
        action = StructuredAction(
            action_type=ActionType.PAD,
            dataset_name="Employees",
            target_columns=["EmployeeCode"],
            parameters={"length": -10, "fill_character": "0", "side": "left"},
            source=ActionSource.BUSINESS_RULE,
        )
        validator = ActionValidator()
        result = validator.validate(
            action=action,
            dataset_profiles=sample_dataset_profiles,
            target_engine=ExecutionEngine.AZURE_SQL,
        )

        assert result.is_valid is False
        assert any("length" in i.message.lower() for i in result.issues)

    def test_criterion_5_wrong_datatype_fails_compatibility(self, sample_dataset_profiles):
        """Test 5: String-specific transformations against incompatible column (e.g. boolean, binary) must fail."""
        action_bool = StructuredAction(
            action_type=ActionType.PREFIX_ADD,
            dataset_name="Employees",
            target_columns=["IsActive"],  # Boolean column
            parameters={"prefix": "FLAG_"},
            source=ActionSource.BUSINESS_RULE,
        )
        validator = ActionValidator()
        res_bool = validator.validate(
            action=action_bool,
            dataset_profiles=sample_dataset_profiles,
            target_engine=ExecutionEngine.AZURE_SQL,
        )
        assert res_bool.is_valid is False
        assert any(i.issue_code == "ACTION_TYPE_INCOMPATIBLE" for i in res_bool.issues)

        action_bin = StructuredAction(
            action_type=ActionType.PATTERN_REPLACE,
            dataset_name="Employees",
            target_columns=["BinaryPhoto"],  # Varbinary column
            parameters={"pattern": "abc", "replacement": "xyz"},
            source=ActionSource.BUSINESS_RULE,
        )
        res_bin = validator.validate(
            action=action_bin,
            dataset_profiles=sample_dataset_profiles,
            target_engine=ExecutionEngine.AZURE_SQL,
        )
        assert res_bin.is_valid is False
        assert any(i.issue_code == "ACTION_TYPE_INCOMPATIBLE" for i in res_bin.issues)

    def test_criterion_6_existing_level1_behavior_unbroken(self, sample_dataset_profiles):
        """Test 6: Existing TRIM, CAST_TYPE, FILL_NULLS, DEDUPLICATE must produce exactly the same logical behavior."""
        from agent.compiler.planners.issue_compiler import IssueCompiler
        from agent.compiler.planners.logical_planner import LogicalPlanner
        from agent.compiler.models.issues import Issue, IssueCategory, IssueSeverity

        compiler = IssueCompiler()
        raw_issue = {
            "issue_type": "null_values",
            "column": "EmployeeName",
            "severity": "medium",
            "message": "Null values found in EmployeeName",
        }
        issue = compiler.compile_raw_issue(raw_issue, "Employees", 0)
        assert issue.category == IssueCategory.MISSING_VALUE

        step = compiler.issue_to_logical_step(issue, 0)
        assert step.action == ActionType.FILL_NULLS
        assert step.target_columns == ["EmployeeName"]
        assert step.effect == DataEffect.TRANSFORM_IN_PLACE

        # StructuredAction bridge for Level-1
        level1_action = StructuredAction(
            action_type=ActionType.FILL_NULLS,
            dataset_name="Employees",
            target_columns=["EmployeeName"],
            parameters={"strategy": "constant", "fill_value": "UNKNOWN"},
            source=ActionSource.ASSESSMENT,
        )
        validator = ActionValidator()
        res = validator.validate(
            action=level1_action,
            dataset_profiles=sample_dataset_profiles,
            target_engine=ExecutionEngine.AZURE_SQL,
        )
        assert res.is_valid is True

    def test_criterion_7_quarantine_separation(self):
        """Test 7: There must be no ActionType.QUARANTINE. Quarantine remains policy/disposition."""
        # 1. ActionType must NOT have QUARANTINE
        action_names = [a.name for a in ActionType]
        action_values = [a.value for a in ActionType]
        assert "QUARANTINE" not in action_names
        assert "quarantine" not in action_values
        assert "QUARANTINE_ROWS" not in action_names

        # 2. RowDisposition has QUARANTINE
        assert hasattr(RowDisposition, "QUARANTINE")
        assert RowDisposition.QUARANTINE.value == "quarantine"

        # 3. DataEffect has QUARANTINE_ROWS
        assert hasattr(DataEffect, "QUARANTINE_ROWS")
        assert DataEffect.QUARANTINE_ROWS.value == "quarantine_rows"

        # 4. FailureBehavior has QUARANTINE_ROW
        assert hasattr(FailureBehavior, "QUARANTINE_ROW")
        assert FailureBehavior.QUARANTINE_ROW.value == "quarantine_row"

        # 5. TransformationPolicy ties them together cleanly
        policy = TransformationPolicy(
            action_type=ActionType.PREFIX_ADD,
            row_disposition=RowDisposition.TRANSFORM,
            risk_level=RiskLevel.LOW,
            execution_mode=ExecutionMode.INTERACTIVE,
            on_failure=FailureBehavior.QUARANTINE_ROW,
            max_failure_percentage=0.0,
        )
        assert policy.on_failure == FailureBehavior.QUARANTINE_ROW

    def test_criterion_8_serialization_round_trip(self):
        """Test 8: StructuredAction should safely serialize and deserialize to/from JSON."""
        action = StructuredAction(
            action_type=ActionType.PREFIX_ADD,
            dataset_name="Employees",
            target_columns=["EmployeeCode"],
            parameters={"prefix": "EMP-", "only_if_missing": True},
            source=ActionSource.BUSINESS_RULE,
            source_reference="BR-001",
            extraction_method="llm",
            confidence=0.98,
            requires_approval=True,
            metadata={"domain": "HR", "author": "DataGovTeam"},
        )

        # 1. Serialize to JSON
        json_str = action.model_dump_json()
        raw_dict = json.loads(json_str)

        # 2. Verify JSON fields
        assert raw_dict["action_type"] == "prefix_add"
        assert raw_dict["dataset_name"] == "Employees"
        assert raw_dict["target_columns"] == ["EmployeeCode"]
        assert raw_dict["parameters"]["prefix"] == "EMP-"
        assert raw_dict["source"] == "business_rule"
        assert raw_dict["confidence"] == 0.98
        assert raw_dict["requires_approval"] is True

        # 3. Deserialize back to model
        reconstructed = StructuredAction.model_validate_json(json_str)
        assert reconstructed.action_type == ActionType.PREFIX_ADD
        assert reconstructed.dataset_name == "Employees"
        assert reconstructed.confidence == 0.98
        assert reconstructed.requires_approval is True

        # 4. Verify typed parameter reconstruction
        typed = reconstructed.get_typed_parameters()
        assert isinstance(typed, PrefixAddParameters)
        assert typed.prefix == "EMP-"
        assert typed.only_if_missing is True
