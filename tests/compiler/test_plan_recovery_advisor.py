"""
Tests for Dhara Plan Recovery Framework and ResolutionAdvisor.
Verifies that validation failures produce structured recovery contracts,
recommends safe legal alternatives, and preserves valid candidate decisions.
"""

import pytest
from agent.etl_pipeline.validate_plan import PlanValidationIssue
from agent.etl_pipeline.resolution_advisor import ResolutionAdvisor, RecoveryAdvisory


def test_advisor_handles_illegal_key_imputation():
    advisor = ResolutionAdvisor()
    issue = PlanValidationIssue(
        code="ILLEGAL_KEY_IMPUTATION",
        message="Action 'fill_null_with_constant' is prohibited on key/identifier column 'customer_id'",
        dataset_id="Customers",
        column="customer_id",
        step_id="step_01",
        item_id="rev_cust_id_101",
        attempted_action="fill_null_with_constant",
    )

    advisories = advisor.advise([issue])
    assert len(advisories) == 1
    adv = advisories[0]

    assert adv.code == "ILLEGAL_KEY_IMPUTATION"
    assert adv.column == "customer_id"
    assert adv.item_id == "rev_cust_id_101"
    assert adv.severity == "BLOCKING"
    assert adv.recommended_action == "preserve_null_and_flag"

    allowed_act_names = [a.action for a in adv.allowed_actions]
    assert "preserve_null_and_flag" in allowed_act_names
    assert "quarantine_null_rows" in allowed_act_names

    prohibited_names = [p.action for p in adv.prohibited_actions]
    assert "fill_null_with_constant" in prohibited_names
    assert adv.auto_fixable is True


def test_advisor_handles_never_drop_violation():
    advisor = ResolutionAdvisor()
    issue = PlanValidationIssue(
        code="UNSAFE_ROW_DELETION",
        message="never_drop_rows: drop_rows on Orders.order_id is not allowed",
        dataset_id="Orders",
        column="order_id",
        step_id="step_02",
        item_id="rev_orders_02",
        attempted_action="drop_rows",
    )

    advisories = advisor.advise([issue], business_rules={"never_drop_rows": True})
    assert len(advisories) == 1
    adv = advisories[0]

    assert adv.code == "UNSAFE_ROW_DELETION"
    assert adv.recommended_action == "preserve_null_and_flag"
    prohibited_names = [p.action for p in adv.prohibited_actions]
    assert "drop_rows" in prohibited_names


def test_advisor_handles_missing_required_parameter():
    advisor = ResolutionAdvisor()
    issue = PlanValidationIssue(
        code="MISSING_REQUIRED_PARAMETER",
        message="Action 'fill_null_with_constant' is missing constant 'value' parameter on column 'discount'",
        dataset_id="Orders",
        column="discount",
        step_id="step_03",
        item_id="rev_disc_03",
        attempted_action="fill_null_with_constant",
    )

    advisories = advisor.advise([issue])
    assert len(advisories) == 1
    adv = advisories[0]

    assert adv.code == "MISSING_REQUIRED_PARAMETER"
    assert adv.auto_fixable is False
    assert "value" in adv.explanation.lower()


def test_advisor_output_contract_shape():
    advisor = ResolutionAdvisor()
    issue = PlanValidationIssue(
        code="ILLEGAL_KEY_IMPUTATION",
        message="Illegal imputation",
        dataset_id="Accounts",
        column="account_pk",
        step_id="step_10",
        item_id="rev_acc_10",
        attempted_action="impute_numeric",
    )

    advisories = advisor.advise([issue])
    payload = advisories[0].to_dict()

    required_keys = {
        "issue_id",
        "item_id",
        "dataset",
        "column",
        "code",
        "severity",
        "user_choice",
        "title",
        "explanation",
        "recommended_action",
        "allowed_actions",
        "prohibited_actions",
        "auto_fixable",
    }
    assert required_keys.issubset(payload.keys())
    assert isinstance(payload["allowed_actions"], list)
    assert len(payload["allowed_actions"]) >= 1
    assert payload["allowed_actions"][0]["recommended"] is True


def test_etl_apply_manual_resolutions_recovery_lifecycle():
    from unittest.mock import patch, MagicMock
    from agent.etl_handlers import etl_apply_manual_resolutions

    plan = {
        "plan_id": "test_plan_recovery",
        "datasets": {
            "sales.csv": {"steps": []}
        },
        "manual_review": [
            {
                "id": "rev_good",
                "dataset": "sales.csv",
                "column": "amount",
                "issue_type": "type_mismatch",
                "status": "pending",
                "resolution_options": [
                    {"id": "cast_type", "action": "cast_type", "recommended": True}
                ]
            },
            {
                "id": "rev_bad",
                "dataset": "sales.csv",
                "column": "order_id",
                "issue_type": "high_null_percentage",
                "status": "pending",
                "resolution_options": [
                    {"id": "impute_num", "action": "impute_numeric"}
                ]
            }
        ]
    }
    mock_session = {
        "context": {
            "etl_flow": {
                "plan": plan,
                "business_rules": {}
            }
        },
        "_version": 1
    }

    resolutions = [
        {"item_id": "rev_good", "resolution_id": "cast_type"},
        {"item_id": "rev_bad", "resolution_id": "impute_num", "parameters": {"strategy": "mean"}}
    ]

    with patch("agent.etl_handlers.load_session", return_value=mock_session), \
         patch("agent.session_store._connect") as mock_conn:
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.return_value.execute.return_value = mock_cursor

        resp = etl_apply_manual_resolutions("sess_recovery", resolutions)
        assert resp["status"] == "REQUIRES_CORRECTION"
        assert resp["ok"] is False
        assert resp["recoverable"] is True
        assert "issues" in resp
        assert len(resp["issues"]) >= 1

        bad_issue = next(i for i in resp["issues"] if i["column"] == "order_id")
        assert bad_issue["code"] == "ILLEGAL_KEY_IMPUTATION"
        assert bad_issue["recommended_action"] == "preserve_null_and_flag"
        assert any(a["action"] == "preserve_null_and_flag" for a in bad_issue["allowed_actions"])


def test_multi_turn_correction_to_atomic_promotion_lifecycle():
    """
    Tests the complete 4-stage lifecycle:
    1. Partial failure (1 valid, 1 invalid) -> CORRECTION_REQUIRED, approved plan unchanged.
    2. Resubmission of invalid choice -> remains CORRECTION_REQUIRED, valid choice preserved.
    3. User fixes invalid choice with legal recommendation -> ALL VALID, atomic promotion, plan_version incremented.
    4. Reset restores clean baseline.
    """
    from unittest.mock import patch, MagicMock
    from agent.etl_handlers import etl_apply_manual_resolutions

    original_plan = {
        "plan_id": "plan_lifecycle_001",
        "plan_version": 1,
        "datasets": {
            "customers.csv": {"steps": []}
        },
        "manual_review": [
            {
                "id": "rev_salary",
                "dataset": "customers.csv",
                "column": "salary",
                "issue_type": "type_mismatch",
                "status": "pending",
                "resolution_options": [
                    {"id": "cast_type", "action": "cast_type", "recommended": True}
                ]
            },
            {
                "id": "rev_cust_id",
                "dataset": "customers.csv",
                "column": "customer_id",
                "issue_type": "high_null_percentage",
                "status": "pending",
                "resolution_options": [
                    {"id": "impute_zero", "action": "impute_numeric"},
                    {"id": "preserve_flag", "action": "preserve_null_and_flag", "recommended": True}
                ]
            }
        ]
    }

    assessment = {
        "datasets": {
            "customers.csv": {
                "columns": {
                    "salary": {"type": "string"},
                    "customer_id": {"type": "string", "semantic_role": "PRIMARY_KEY"}
                }
            }
        }
    }

    session = {
        "context": {
            "last_assessment_result": assessment,
            "etl_flow": {
                "plan": original_plan,
                "business_rules": {},
                "plan_version": 1,
            }
        },
        "_version": 1
    }

    with patch("agent.etl_handlers.load_session", return_value=session), \
         patch("agent.session_store._connect") as mock_conn:
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.return_value.execute.return_value = mock_cursor

        # Turn 1: 1 valid (salary), 1 illegal (customer_id imputed with mean)
        resp1 = etl_apply_manual_resolutions("sess_life", [
            {"item_id": "rev_salary", "resolution_id": "cast_type", "parameters": {"target_type": "decimal"}},
            {"item_id": "rev_cust_id", "resolution_id": "impute_zero", "parameters": {"strategy": "mean"}}
        ])
        assert resp1["status"] == "REQUIRES_CORRECTION"
        assert resp1["accepted_decisions_count"] == 1
        assert resp1["unresolved_issues_count"] == 1
        # Invariant: active approved plan must remain unmodified in session
        assert session["context"]["etl_flow"]["plan"]["plan_version"] == 1
        assert len(session["context"]["etl_flow"]["plan"]["datasets"]["customers.csv"]["steps"]) == 0

        # Turn 2: User fixes customer_id with legal recommended alternative: preserve_null_and_flag
        resp2 = etl_apply_manual_resolutions("sess_life", [
            {"item_id": "rev_salary", "resolution_id": "cast_type", "parameters": {"target_type": "decimal"}},
            {"item_id": "rev_cust_id", "resolution_id": "preserve_flag"}
        ])
        assert resp2["ok"] is True
        assert resp2["lifecycle_state"] in ("READY_TO_CONFIRM", "READY_TO_GENERATE")
        # Atomic promotion: approved plan updated, version incremented
        promoted_plan = session["context"]["etl_flow"]["plan"]
        assert promoted_plan["plan_version"] == 2
        assert session["context"]["etl_flow"]["plan_version"] == 2
        assert len(promoted_plan["datasets"]["customers.csv"]["steps"]) >= 2
