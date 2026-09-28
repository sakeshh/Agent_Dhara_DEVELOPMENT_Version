from __future__ import annotations

import pytest

from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.models.tier3_resolution import Tier3ResolutionProposal
from agent.compiler.tier3 import (
    Tier3TypeValidator,
    ValidationCode,
)


def _build_test_profile() -> dict[str, DatasetProfile]:
    emp_cols = {
        "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
        "EmployeeName": ColumnProfile(name="EmployeeName", dtype="nvarchar(100)"),
        "Salary": ColumnProfile(name="Salary", dtype="decimal(18,2)"),
        "BirthDate": ColumnProfile(name="BirthDate", dtype="date"),
        "HireDateTime": ColumnProfile(name="HireDateTime", dtype="datetime2"),
    }
    return {
        "dbo.Dhara_Employee": DatasetProfile(dataset_name="dbo.Dhara_Employee", dataset_id="dbo.Dhara_Employee", columns=emp_cols),
    }


def _build_proposal(
    action_type: str = "TRIM",
    target_columns: list[str] | None = None,
    condition_ast: dict | None = None,
    action_parameters: dict | None = None,
    approval_required: bool = False,
) -> Tier3ResolutionProposal:
    return Tier3ResolutionProposal(
        proposal_id="prop_type_test",
        issue_id="iss_type_test",
        dataset_name="dbo.Dhara_Employee",
        proposed_action_type=action_type,
        target_columns=target_columns or ["EmployeeName"],
        disposition="CORRECT",
        condition_ast=condition_ast,
        action_parameters=action_parameters or {},
        confidence=0.9,
        risk_level="LOW",
        approval_required=approval_required,
        evidence_authorities=["APPROVED_CATALOG"],
    )


def test_type_validator_accepts_valid_string_trim():
    validator = Tier3TypeValidator()
    profiles = _build_test_profile()
    proposal = _build_proposal(action_type="TRIM", target_columns=["EmployeeName"])

    res = validator.validate(proposal, profiles)
    assert res.valid is True
    assert len(res.findings) == 0


def test_type_validator_rejects_trim_on_numeric_column():
    validator = Tier3TypeValidator()
    profiles = _build_test_profile()
    proposal = _build_proposal(action_type="TRIM", target_columns=["Salary"])

    res = validator.validate(proposal, profiles)
    assert res.valid is False
    codes = [f.code for f in res.findings]
    assert ValidationCode.ACTION_TYPE_INCOMPATIBLE in codes


def test_type_validator_accepts_numeric_ordering_condition():
    validator = Tier3TypeValidator()
    profiles = _build_test_profile()
    ast = {
        "node_type": "COMPARISON",
        "operator": "LT",
        "left": {"node_type": "COLUMN", "dataset": "dbo.Dhara_Employee", "column": "Salary"},
        "right": {"node_type": "LITERAL", "data_type": "DECIMAL", "value": 50000},
    }
    proposal = _build_proposal(action_type="VALIDATE_RANGE", target_columns=["Salary"], condition_ast=ast)

    res = validator.validate(proposal, profiles)
    assert res.valid is True


def test_type_validator_rejects_ordering_on_string_column():
    validator = Tier3TypeValidator()
    profiles = _build_test_profile()
    ast = {
        "node_type": "COMPARISON",
        "operator": "LT",
        "left": {"node_type": "COLUMN", "dataset": "dbo.Dhara_Employee", "column": "EmployeeName"},
        "right": {"node_type": "LITERAL", "data_type": "STRING", "value": "Smith"},
    }
    proposal = _build_proposal(action_type="VALIDATE_RANGE", target_columns=["Salary"], condition_ast=ast)

    res = validator.validate(proposal, profiles)
    assert res.valid is False
    assert ValidationCode.INVALID_OPERATOR_FOR_TYPE in [f.code for f in res.findings]


def test_type_validator_rejects_like_on_numeric_column():
    validator = Tier3TypeValidator()
    profiles = _build_test_profile()
    ast = {
        "node_type": "LIKE",
        "column": {"node_type": "COLUMN", "dataset": "dbo.Dhara_Employee", "column": "Salary"},
        "pattern": "%100%",
    }
    proposal = _build_proposal(action_type="VALIDATE_RANGE", target_columns=["Salary"], condition_ast=ast)

    res = validator.validate(proposal, profiles)
    assert res.valid is False
    assert ValidationCode.INVALID_OPERATOR_FOR_TYPE in [f.code for f in res.findings]


def test_type_validator_rejects_between_with_invalid_bounds():
    validator = Tier3TypeValidator()
    profiles = _build_test_profile()
    ast = {
        "node_type": "BETWEEN",
        "column": {"node_type": "COLUMN", "dataset": "dbo.Dhara_Employee", "column": "Salary"},
        "lower": {"node_type": "LITERAL", "data_type": "DECIMAL", "value": 1000},
        "upper": {"node_type": "LITERAL", "data_type": "DECIMAL", "value": 500},  # Lower > Upper
    }
    proposal = _build_proposal(action_type="VALIDATE_RANGE", target_columns=["Salary"], condition_ast=ast)

    res = validator.validate(proposal, profiles)
    assert res.valid is False
    assert ValidationCode.INVALID_RANGE_BOUNDS in [f.code for f in res.findings]


def test_type_validator_rejects_empty_in_list():
    validator = Tier3TypeValidator()
    profiles = _build_test_profile()
    ast = {
        "node_type": "IN_LIST",
        "column": {"node_type": "COLUMN", "dataset": "dbo.Dhara_Employee", "column": "EmployeeID"},
        "values": [],
    }
    proposal = _build_proposal(action_type="FLAG_ROW", target_columns=["EmployeeID"], condition_ast=ast)

    res = validator.validate(proposal, profiles)
    assert res.valid is False
    assert ValidationCode.EMPTY_VALUE_LIST in [f.code for f in res.findings]


def test_type_validator_rejects_unapproved_lossy_cast():
    validator = Tier3TypeValidator()
    profiles = _build_test_profile()
    proposal = _build_proposal(
        action_type="CAST_TYPE",
        target_columns=["Salary"],
        action_parameters={"target_type": "int"},
        approval_required=False,  # Lossy cast from DECIMAL to INT must require approval
    )

    res = validator.validate(proposal, profiles)
    assert res.valid is False
    assert ValidationCode.LOSSY_CONVERSION_UNAPPROVED in [f.code for f in res.findings]
