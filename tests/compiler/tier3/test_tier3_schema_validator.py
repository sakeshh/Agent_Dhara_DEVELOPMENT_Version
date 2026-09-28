from __future__ import annotations

import pytest

from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.models.tier3_resolution import Tier3ResolutionProposal
from agent.compiler.tier3 import (
    Tier3SchemaValidator,
    ValidationCode,
    ValidationSeverity,
)


def _build_test_profiles() -> dict[str, DatasetProfile]:
    emp_cols = {
        "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
        "EmployeeName": ColumnProfile(name="EmployeeName", dtype="nvarchar(100)"),
        "EmploymentType": ColumnProfile(name="EmploymentType", dtype="varchar(50)"),
        "Salary": ColumnProfile(name="Salary", dtype="decimal(18,2)"),
    }
    org_cols = {
        "OrgID": ColumnProfile(name="OrgID", dtype="int"),
        "OrgName": ColumnProfile(name="OrgName", dtype="nvarchar(100)"),
    }
    return {
        "dbo.Dhara_Employee": DatasetProfile(dataset_name="dbo.Dhara_Employee", dataset_id="dbo.Dhara_Employee", columns=emp_cols),
        "dbo.Dhara_OrgReference": DatasetProfile(dataset_name="dbo.Dhara_OrgReference", dataset_id="dbo.Dhara_OrgReference", columns=org_cols),
    }


def _build_proposal(
    dataset_name: str = "dbo.Dhara_Employee",
    target_columns: list[str] | None = None,
    condition_ast: dict | None = None,
) -> Tier3ResolutionProposal:
    return Tier3ResolutionProposal(
        proposal_id="prop_schema_test",
        issue_id="iss_schema_test",
        dataset_name=dataset_name,
        proposed_action_type="TRIM",
        target_columns=target_columns or ["EmployeeName"],
        disposition="CORRECT",
        condition_ast=condition_ast,
        confidence=0.9,
        risk_level="LOW",
        approval_required=False,
        evidence_authorities=["APPROVED_CATALOG"],
    )


def test_schema_validator_accepts_valid_target_columns():
    validator = Tier3SchemaValidator()
    profiles = _build_test_profiles()
    proposal = _build_proposal(target_columns=["EmployeeName", "EmploymentType"])

    res = validator.validate(proposal, profiles)
    assert res.valid is True
    assert len(res.findings) == 0
    assert res.normalized_proposal is not None
    assert res.normalized_proposal.target_columns == ["EmployeeName", "EmploymentType"]


def test_schema_validator_rejects_unknown_dataset():
    validator = Tier3SchemaValidator()
    profiles = _build_test_profiles()
    proposal = _build_proposal(dataset_name="dbo.Unknown_Table")

    res = validator.validate(proposal, profiles)
    assert res.valid is False
    codes = [f.code for f in res.findings]
    assert ValidationCode.UNKNOWN_DATASET in codes


def test_schema_validator_rejects_unknown_target_column():
    validator = Tier3SchemaValidator()
    profiles = _build_test_profiles()
    proposal = _build_proposal(target_columns=["NonExistentColumn"])

    res = validator.validate(proposal, profiles)
    assert res.valid is False
    codes = [f.code for f in res.findings]
    assert ValidationCode.UNKNOWN_COLUMN in codes


def test_schema_validator_does_not_fuzzy_substitute_columns():
    validator = Tier3SchemaValidator()
    profiles = _build_test_profiles()
    # EmployeeCategory is similar to EmploymentType, but must NOT be silently substituted
    proposal = _build_proposal(target_columns=["EmployeeCategory"])

    res = validator.validate(proposal, profiles)
    assert res.valid is False
    assert ValidationCode.UNKNOWN_COLUMN in [f.code for f in res.findings]


def test_schema_validator_rejects_internal_protected_column():
    validator = Tier3SchemaValidator()
    profiles = _build_test_profiles()
    proposal = _build_proposal(target_columns=["_DharaStagingRowId"])

    res = validator.validate(proposal, profiles)
    assert res.valid is False
    assert ValidationCode.INTERNAL_COLUMN_PROTECTED in [f.code for f in res.findings]


def test_schema_validator_normalizes_bracketed_identifiers():
    validator = Tier3SchemaValidator()
    profiles = _build_test_profiles()
    # Unquoted or bracketed forms resolve to canonical key
    proposal = _build_proposal(dataset_name="[dbo].[Dhara_Employee]")

    res = validator.validate(proposal, profiles)
    assert res.valid is True
    assert res.normalized_proposal.dataset_name == "dbo.Dhara_Employee"


def test_schema_validator_rejects_ambiguous_unqualified_dataset():
    validator = Tier3SchemaValidator()
    # Create two datasets with the same base table name in different schemas
    profiles = {
        "dbo.Employee": DatasetProfile(dataset_name="dbo.Employee", dataset_id="dbo.Employee", columns={}),
        "hr.Employee": DatasetProfile(dataset_name="hr.Employee", dataset_id="hr.Employee", columns={}),
    }
    proposal = _build_proposal(dataset_name="Employee")

    res = validator.validate(proposal, profiles)
    assert res.valid is False
    assert ValidationCode.AMBIGUOUS_DATASET in [f.code for f in res.findings]


def test_schema_validator_rejects_unknown_column_in_condition_ast():
    validator = Tier3SchemaValidator()
    profiles = _build_test_profiles()
    ast = {
        "node_type": "COMPARISON",
        "operator": "EQ",
        "left": {
            "node_type": "COLUMN",
            "dataset": "dbo.Dhara_Employee",
            "column": "GhostColumn",
        },
        "right": {
            "node_type": "LITERAL",
            "data_type": "STRING",
            "value": "Test",
        },
    }
    proposal = _build_proposal(condition_ast=ast)

    res = validator.validate(proposal, profiles)
    assert res.valid is False
    finding = next(f for f in res.findings if f.code == ValidationCode.UNKNOWN_COLUMN)
    assert finding.column == "GhostColumn"
    assert "condition_ast.left.column" in finding.ast_path
