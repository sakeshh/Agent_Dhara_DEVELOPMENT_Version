from __future__ import annotations

import pytest

from agent.compiler.models.tier3_resolution import Tier3ResolutionProposal
from agent.compiler.tier3 import (
    Tier3ConflictDetector,
    ValidationCode,
)


def _build_p(
    proposal_id: str,
    target_columns: list[str] | None = None,
    action_type: str = "CONDITIONAL_UPDATE",
    condition_ast: dict | None = None,
    action_parameters: dict | None = None,
    disposition: str = "CORRECT",
) -> Tier3ResolutionProposal:
    return Tier3ResolutionProposal(
        proposal_id=proposal_id,
        issue_id=f"iss_{proposal_id}",
        dataset_name="dbo.Dhara_Employee",
        proposed_action_type=action_type,
        target_columns=target_columns or ["Status"],
        disposition=disposition,
        condition_ast=condition_ast,
        action_parameters=action_parameters or {},
        confidence=0.9,
        risk_level="HIGH" if disposition in ("REJECT", "QUARANTINE") else "LOW",
        approval_required=True if disposition in ("REJECT", "QUARANTINE") else False,
        evidence_authorities=["APPROVED_BUSINESS_RULE"],
    )


def test_conflict_detector_detects_exact_duplicates():
    detector = Tier3ConflictDetector()
    p1 = _build_p("p1", target_columns=["Status"], action_parameters={"new_value": "Active"})
    p2 = _build_p("p2", target_columns=["Status"], action_parameters={"new_value": "Active"})

    res = detector.validate_proposals([p1, p2])
    # Exact duplicates produce warning, not hard error
    assert any(f.code == ValidationCode.DUPLICATE_ACTION for f in res.findings)


def test_conflict_detector_blocks_unconditional_conflicting_writes():
    detector = Tier3ConflictDetector()
    p1 = _build_p("p1", target_columns=["Status"], action_parameters={"new_value": "Active"})
    p2 = _build_p("p2", target_columns=["Status"], action_parameters={"new_value": "Inactive"})

    res = detector.validate_proposals([p1, p2])
    assert res.valid is False
    assert any(f.code == ValidationCode.CONFLICTING_WRITE for f in res.findings)


def test_conflict_detector_blocks_identical_condition_conflicting_writes():
    detector = Tier3ConflictDetector()
    ast = {
        "node_type": "COMPARISON",
        "operator": "EQ",
        "left": {"node_type": "COLUMN", "dataset": "dbo.Dhara_Employee", "column": "Department"},
        "right": {"node_type": "LITERAL", "data_type": "STRING", "value": "Contractor"},
    }
    p1 = _build_p("p1", target_columns=["Status"], condition_ast=ast, action_parameters={"new_value": "Active"})
    p2 = _build_p("p2", target_columns=["Status"], condition_ast=ast, action_parameters={"new_value": "Inactive"})

    res = detector.validate_proposals([p1, p2])
    assert res.valid is False
    assert any(f.code == ValidationCode.CONFLICTING_WRITE for f in res.findings)


def test_conflict_detector_detects_destructive_overlap():
    detector = Tier3ConflictDetector()
    ast = {
        "node_type": "COMPARISON",
        "operator": "LT",
        "left": {"node_type": "COLUMN", "dataset": "dbo.Dhara_Employee", "column": "Salary"},
        "right": {"node_type": "LITERAL", "data_type": "DECIMAL", "value": 0},
    }
    # One rejects rows where Salary < 0, another corrects Status on Salary < 0
    p1 = _build_p("p1", target_columns=["Salary"], condition_ast=ast, disposition="REJECT")
    p2 = _build_p("p2", target_columns=["Status"], condition_ast=ast, disposition="CORRECT")

    res = detector.validate_proposals([p1, p2])
    assert any(f.code == ValidationCode.OVERLAPPING_DESTRUCTIVE_ACTION for f in res.findings)


def test_conflict_detector_blocks_dependency_cycle():
    detector = Tier3ConflictDetector()
    p1 = _build_p("p1", action_parameters={"depends_on": ["p2"]})
    p2 = _build_p("p2", action_parameters={"depends_on": ["p3"]})
    p3 = _build_p("p3", action_parameters={"depends_on": ["p1"]})

    res = detector.validate_proposals([p1, p2, p3])
    assert res.valid is False
    assert any(f.code == ValidationCode.DEPENDENCY_CYCLE for f in res.findings)
