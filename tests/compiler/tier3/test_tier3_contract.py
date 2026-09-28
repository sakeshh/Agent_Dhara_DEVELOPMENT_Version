from __future__ import annotations

import pytest

from agent.compiler.models.tier3_resolution import (
    AstNodeType,
    ComparisonOperator,
    EvidenceAuthority,
    ResolutionDisposition,
    RiskLevel,
    Tier3ResolutionProposal,
)
from agent.compiler.tier3 import (
    Tier3ContractValidator,
    compute_proposal_fingerprint,
)


def _build_valid_payload() -> dict:
    return {
        "contract_version": "1.0",
        "proposal_id": "prop_emp_salary_1",
        "issue_id": "iss_emp_salary_neg",
        "dataset_name": "dbo.Dhara_Employee",
        "proposed_action_type": "VALIDATE_RANGE",
        "target_columns": ["Salary"],
        "disposition": "FLAG",
        "condition_ast": {
            "node_type": "COMPARISON",
            "operator": "LT",
            "left": {
                "node_type": "COLUMN",
                "dataset": "dbo.Dhara_Employee",
                "column": "Salary",
            },
            "right": {
                "node_type": "LITERAL",
                "data_type": "DECIMAL",
                "value": 0,
            },
        },
        "action_parameters": {
            "min_value": 0.0,
        },
        "evidence_authorities": ["APPROVED_BUSINESS_RULE"],
        "evidence_references": ["BR-SALARY-NON-NEG"],
        "confidence": 0.95,
        "risk_level": "LOW",
        "approval_required": False,
        "rationale": "Approved rule requires non-negative salary values to be flagged.",
        "assumptions": [],
    }


# ==============================================================================
# 1. Contract Acceptance Test
# ==============================================================================

def test_contract_accepts_valid_structured_proposal():
    validator = Tier3ContractValidator()
    payload = _build_valid_payload()

    res = validator.validate(payload)
    assert res.is_valid is True
    assert len(res.issues) == 0
    assert res.proposal is not None
    assert res.proposal.proposed_action_type == "VALIDATE_RANGE"
    assert res.proposal.disposition == ResolutionDisposition.FLAG


# ==============================================================================
# 2. Raw SQL Field Rejection
# ==============================================================================

@pytest.mark.parametrize("forbidden_key", ["raw_sql", "where_clause", "sql", "statement", "ddl"])
def test_contract_rejects_forbidden_sql_keys(forbidden_key: str):
    validator = Tier3ContractValidator()
    payload = _build_valid_payload()
    payload[forbidden_key] = "DELETE FROM dbo.Dhara_Employee WHERE Salary < 0"

    res = validator.validate(payload)
    assert res.is_valid is False
    codes = [i.issue_code for i in res.issues]
    assert "TIER3_RAW_SQL_KEY_FORBIDDEN" in codes


# ==============================================================================
# 3. Raw SQL Token Rejection in Structural Fields
# ==============================================================================

def test_contract_rejects_raw_sql_in_structural_fields():
    validator = Tier3ContractValidator()
    payload = _build_valid_payload()
    payload["action_parameters"]["extra_clause"] = "DROP TABLE dbo.Dhara_Employee; --"

    res = validator.validate(payload)
    assert res.is_valid is False
    codes = [i.issue_code for i in res.issues]
    assert "TIER3_RAW_SQL_TOKEN_FORBIDDEN" in codes


# ==============================================================================
# 4. Unknown Action Rejection
# ==============================================================================

def test_contract_rejects_unknown_action_type():
    validator = Tier3ContractValidator()
    payload = _build_valid_payload()
    payload["proposed_action_type"] = "INTELLIGENT_CLEAN_MAGIC"

    res = validator.validate(payload)
    assert res.is_valid is False
    codes = [i.issue_code for i in res.issues]
    assert "TIER3_CONTRACT_FIELD_INVALID" in codes


# ==============================================================================
# 5. Unknown AST Node Rejection
# ==============================================================================

def test_contract_rejects_unknown_ast_node_type():
    validator = Tier3ContractValidator()
    payload = _build_valid_payload()
    payload["condition_ast"] = {
        "node_type": "ARBITRARY_PYTHON_EXPRESSION",
        "code": "lambda x: x < 0",
    }

    res = validator.validate(payload)
    assert res.is_valid is False
    codes = [i.issue_code for i in res.issues]
    assert "TIER3_AST_UNKNOWN_NODE_TYPE" in codes


# ==============================================================================
# 6. Invalid Operator Rejection
# ==============================================================================

def test_contract_rejects_invalid_ast_operator():
    validator = Tier3ContractValidator()
    payload = _build_valid_payload()
    payload["condition_ast"]["operator"] = "EXEC"

    res = validator.validate(payload)
    assert res.is_valid is False
    codes = [i.issue_code for i in res.issues]
    assert "TIER3_AST_VALIDATION_ERROR" in codes


# ==============================================================================
# 7. Missing Provenance / Evidence Authority Rejection
# ==============================================================================

def test_contract_rejects_missing_evidence_authority():
    validator = Tier3ContractValidator()
    payload = _build_valid_payload()
    payload["evidence_authorities"] = []

    res = validator.validate(payload)
    assert res.is_valid is False
    codes = [i.issue_code for i in res.issues]
    assert "TIER3_CONTRACT_FIELD_INVALID" in codes


# ==============================================================================
# 8. Confidence Boundary Validation
# ==============================================================================

@pytest.mark.parametrize("bad_conf", [-0.1, 1.05, 2.0])
def test_contract_rejects_out_of_bounds_confidence(bad_conf: float):
    validator = Tier3ContractValidator()
    payload = _build_valid_payload()
    payload["confidence"] = bad_conf

    res = validator.validate(payload)
    assert res.is_valid is False
    codes = [i.issue_code for i in res.issues]
    assert "TIER3_CONTRACT_FIELD_INVALID" in codes


# ==============================================================================
# 9. Contradictory Policy Rejection (Destructive without Authority)
# ==============================================================================

def test_contract_rejects_destructive_disposition_without_approval_on_model_inference():
    validator = Tier3ContractValidator()
    payload = _build_valid_payload()
    payload["disposition"] = "REJECT"
    payload["evidence_authorities"] = ["MODEL_INFERENCE"]
    payload["risk_level"] = "LOW"
    payload["approval_required"] = False  # Contradiction: Destructive model inference cannot be auto-approved or LOW risk

    res = validator.validate(payload)
    assert res.is_valid is False
    codes = [i.issue_code for i in res.issues]
    assert "TIER3_CONTRACT_FIELD_INVALID" in codes


# ==============================================================================
# 10. Canonical Fingerprint Stability
# ==============================================================================

def test_fingerprint_is_stable_across_key_order_and_ephemeral_fields():
    payload_a = _build_valid_payload()
    proposal_a = Tier3ResolutionProposal.model_validate(payload_a)

    # Payload B has same semantics but different proposal_id, different confidence, different rationale
    payload_b = dict(payload_a)
    payload_b["proposal_id"] = "prop_different_id_999"
    payload_b["confidence"] = 0.50
    payload_b["rationale"] = "Completely different wording of rationale."
    proposal_b = Tier3ResolutionProposal.model_validate(payload_b)

    fp_a = compute_proposal_fingerprint(proposal_a)
    fp_b = compute_proposal_fingerprint(proposal_b)

    assert fp_a == fp_b, "Fingerprint must ignore ephemeral proposal_id, confidence, and free-form rationale"


# ==============================================================================
# 11. Fingerprint Invalidation on Semantic Change
# ==============================================================================

def test_fingerprint_changes_on_semantic_variation():
    base_payload = _build_valid_payload()
    base_proposal = Tier3ResolutionProposal.model_validate(base_payload)
    base_fp = compute_proposal_fingerprint(base_proposal)

    # 1. Changing disposition
    p_disp = dict(base_payload)
    p_disp["disposition"] = "CORRECT"
    fp_disp = compute_proposal_fingerprint(Tier3ResolutionProposal.model_validate(p_disp))
    assert fp_disp != base_fp

    # 2. Changing target column
    p_col = dict(base_payload)
    p_col["target_columns"] = ["BaseSalary"]
    fp_col = compute_proposal_fingerprint(Tier3ResolutionProposal.model_validate(p_col))
    assert fp_col != base_fp

    # 3. Changing condition literal value
    p_lit = _build_valid_payload()
    p_lit["condition_ast"]["right"]["value"] = 100
    fp_lit = compute_proposal_fingerprint(Tier3ResolutionProposal.model_validate(p_lit))
    assert fp_lit != base_fp

    # 4. Changing schema fingerprint
    fp_schema = compute_proposal_fingerprint(base_proposal, schema_fingerprint="sha256_schema_v2")
    assert fp_schema != base_fp
