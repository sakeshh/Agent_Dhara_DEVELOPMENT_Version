from __future__ import annotations

import pytest

from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.models.tier3_resolution import Tier3ResolutionProposal
from agent.compiler.tier3 import (
    Tier3ValidationOrchestrator,
    ValidationCode,
)


def _build_test_profile() -> dict[str, DatasetProfile]:
    emp_cols = {
        "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
        "EmployeeName": ColumnProfile(name="EmployeeName", dtype="nvarchar(100)"),
        "Salary": ColumnProfile(name="Salary", dtype="decimal(18,2)"),
    }
    return {
        "dbo.Dhara_Employee": DatasetProfile(dataset_name="dbo.Dhara_Employee", dataset_id="dbo.Dhara_Employee", columns=emp_cols),
    }


def test_orchestrator_accepts_clean_valid_proposal():
    orchestrator = Tier3ValidationOrchestrator()
    profiles = _build_test_profile()
    proposal = Tier3ResolutionProposal(
        proposal_id="prop_clean",
        issue_id="iss_clean",
        dataset_name="dbo.Dhara_Employee",
        proposed_action_type="TRIM",
        target_columns=["EmployeeName"],
        disposition="CORRECT",
        confidence=0.95,
        risk_level="LOW",
        approval_required=False,
        evidence_authorities=["APPROVED_CATALOG"],
    )

    res = orchestrator.validate_proposal(proposal, profiles)
    assert res.valid is True
    assert len(res.findings) == 0
    assert res.normalized_proposal is not None


def test_orchestrator_halts_on_contract_raw_sql_failure():
    orchestrator = Tier3ValidationOrchestrator()
    profiles = _build_test_profile()
    bad_payload = {
        "proposal_id": "prop_bad_sql",
        "issue_id": "iss_bad",
        "dataset_name": "dbo.Dhara_Employee",
        "proposed_action_type": "TRIM",
        "target_columns": ["EmployeeName"],
        "disposition": "CORRECT",
        "raw_sql": "DELETE FROM dbo.Dhara_Employee",  # Raw SQL forbidden
        "confidence": 0.9,
        "risk_level": "LOW",
        "evidence_authorities": ["APPROVED_CATALOG"],
    }

    res = orchestrator.validate_proposal(bad_payload, profiles)
    assert res.valid is False
    assert any("raw SQL" in f.message for f in res.findings)


def test_orchestrator_does_not_mutate_original_proposal():
    orchestrator = Tier3ValidationOrchestrator()
    profiles = _build_test_profile()
    proposal = Tier3ResolutionProposal(
        proposal_id="prop_immutable",
        issue_id="iss_imm",
        dataset_name="[dbo].[Dhara_Employee]",  # Bracketed
        proposed_action_type="TRIM",
        target_columns=["[EmployeeName]"],
        disposition="CORRECT",
        confidence=0.9,
        risk_level="LOW",
        approval_required=False,
        evidence_authorities=["APPROVED_CATALOG"],
    )

    orig_dataset = proposal.dataset_name
    orig_cols = list(proposal.target_columns)

    res = orchestrator.validate_proposal(proposal, profiles)
    assert res.valid is True
    # Original proposal must remain unmodified
    assert proposal.dataset_name == orig_dataset
    assert proposal.target_columns == orig_cols
    # Normalized copy has canonical unbracketed identifiers
    assert res.normalized_proposal.dataset_name == "dbo.Dhara_Employee"
    assert res.normalized_proposal.target_columns == ["EmployeeName"]
