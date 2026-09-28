"""
Regression tests for resolution promotion, parameter completeness, and step identity.
"""
from __future__ import annotations
import unittest
from unittest.mock import patch, MagicMock

from agent.etl_pipeline.manual_review_promote import (
    apply_manual_resolutions,
    make_step_id,
)
from agent.etl_pipeline.validate_plan import (
    validate_promoted_plan,
    is_key_or_identifier_column,
)
from agent.etl_handlers import etl_apply_manual_resolutions

class TestManualReviewPromotion(unittest.TestCase):
    def test_step_id_derivation_is_deterministic_and_parameter_sensitive(self) -> None:
        step_base = {
            "dataset": "sales",
            "column": "amount",
            "action": "fill_null_with_constant",
            "params": {"value": 10},
            "source_issue_ids": ["ISS-1"],
        }
        id1, f1 = make_step_id(step_base)
        
        # Changing parameter must change fingerprint and step ID
        step_changed_param = {**step_base, "params": {"value": 20}}
        id2, f2 = make_step_id(step_changed_param)
        self.assertNotEqual(id1, id2)
        self.assertNotEqual(f1, f2)

        # Ordering of keys inside params should produce same ID
        step_param_order = {
            **step_base,
            "params": {"preserve_raw": True, "value": 10}
        }
        step_param_order_alt = {
            **step_base,
            "params": {"value": 10, "preserve_raw": True}
        }
        id_a, _ = make_step_id(step_param_order)
        id_b, _ = make_step_id(step_param_order_alt)
        self.assertEqual(id_a, id_b)

    def test_key_column_rejects_impute_numeric(self) -> None:
        plan = {
            "generation_mode": "cleanse",
            "relationships": {
                "joins": [
                    {
                        "left_dataset": "sales.csv",
                        "left_key": "customer_id",
                        "right_dataset": "customers.csv",
                        "right_key": "id"
                    }
                ]
            },
            "datasets": {
                "sales.csv": {
                    "steps": [
                        {
                            "step_id": "STEP-CUST-1",
                            "column": "customer_id",
                            "action": "impute_numeric",
                            "params": {
                                "strategy": "median",
                                "preserve_raw": True,
                                "output_column": "customer_id_clean",
                                "policy_id": "POL-1"
                            },
                            "manual_resolution": {
                                "selected_option_id": "impute_numeric",
                                "selected_by": "user"
                            }
                        }
                    ]
                }
            }
        }
        valid, issues = validate_promoted_plan(plan)
        self.assertFalse(valid)
        codes = [i.code for i in issues]
        self.assertIn("ILLEGAL_KEY_IMPUTATION", codes)

    def test_failed_promotion_leaves_original_plan_unchanged(self) -> None:
        # If validate_promoted_plan fails, etl_apply_manual_resolutions should return rejection
        # and not modify the session payload.
        plan = {
            "plan_id": "test_plan",
            "datasets": {},
            "manual_review": [
                {
                    "id": "item1",
                    "dataset": "sales.csv",
                    "column": "amount",
                    "issue_type": "null_values",
                    "status": "pending",
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
        
        # Simulate applying a resolution mapping to an invalid legacy action or failing parameter validation
        # (e.g. fill_constant option but value not passed)
        resolutions = [
            {"item_id": "item1", "resolution_id": "fill_constant"}  # Missing 'value' parameter
        ]
        
        with patch("agent.etl_handlers.load_session", return_value=mock_session), \
             patch("agent.etl_handlers.save_session") as mock_save, \
             patch("agent.session_store._connect") as mock_conn:
            
            # Setup DB mock to avoid concurrency check mismatch
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (1,)
            mock_conn.return_value.execute.return_value = mock_cursor
            
            resp = etl_apply_manual_resolutions("sess_1", resolutions)
            self.assertIn(resp["status"], ("PROMOTION_REJECTED", "REQUIRES_CORRECTION"))
            # Invariant: active approved plan must never be corrupted or modified
            if mock_save.called:
                saved_plan = mock_save.call_args[0][0]["context"]["etl_flow"]["plan"]
                self.assertEqual(saved_plan["datasets"], {})
                self.assertEqual(saved_plan["plan_id"], "test_plan")

    def test_plan_revision_conflict_rejects_promotion(self) -> None:
        plan = {
            "plan_id": "test_plan",
            "datasets": {},
            "manual_review": []
        }
        mock_session = {
            "context": {
                "etl_flow": {
                    "plan": plan,
                    "business_rules": {}
                }
            },
            "_version": 1  # Loaded version is 1
        }
        
        with patch("agent.etl_handlers.load_session", return_value=mock_session), \
             patch("agent.session_store._connect") as mock_conn:
            
            # DB has version 2 (concurrency mismatch)
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (2,)
            mock_conn.return_value.execute.return_value = mock_cursor
            
            resp = etl_apply_manual_resolutions("sess_1", [])
            self.assertEqual(resp["status"], "PLAN_REVISION_CONFLICT")
            self.assertFalse(resp["ok"])

    def test_promoted_action_matches_selected_option_action(self) -> None:
        # A promoted step with a mapping mismatch (e.g. selected fill_constant but action is impute_numeric)
        # must fail validation.
        plan = {
            "generation_mode": "cleanse",
            "datasets": {
                "sales.csv": {
                    "steps": [
                        {
                            "step_id": "STEP-CUST-1",
                            "column": "customer_id",
                            "action": "impute_numeric",
                            "params": {
                                "strategy": "median",
                                "preserve_raw": True,
                                "output_column": "customer_id_clean",
                                "policy_id": "POL-1"
                            },
                            "manual_resolution": {
                                "selected_option_id": "fill_constant",  # Mismatch! fill_constant maps to fill_null_with_constant
                                "selected_by": "user"
                            }
                        }
                    ]
                }
            }
        }
        valid, issues = validate_promoted_plan(plan)
        self.assertFalse(valid)
        codes = [i.code for i in issues]
        self.assertIn("PROMOTION_ACTION_MISMATCH", codes)

    def test_unreviewed_draft_steps_do_not_falsely_fail_with_missing_provenance(self) -> None:
        """Draft review steps pending user decision must not falsely raise MISSING_PROVENANCE."""
        plan = {
            "generation_mode": "cleanse",
            "datasets": {
                "dbo.Courses": {
                    "steps": [
                        {
                            "step_id": "step_d962e415de",
                            "column": "Price",
                            "action": "range_clip",
                            "classification": "review",
                            "requires_user_choice": True,
                            "params": {"lower_bound": 0.0, "output_column": "Price_clean"},
                        }
                    ]
                },
                "dbo.Customers": {
                    "steps": [
                        {
                            "step_id": "step_ffbce23954",
                            "column": "Age",
                            "action": "range_clip",
                            "classification": "review",
                            "requires_user_choice": True,
                            "params": {"lower_bound": 0.0, "upper_bound": 120, "output_column": "Age_clean"},
                        }
                    ]
                },
            }
        }
        valid, issues = validate_promoted_plan(plan)
        self.assertTrue(valid)
        self.assertEqual(len(issues), 0)

    def test_nullify_date_action_validates_and_promotes_cleanly(self) -> None:
        """Resolving impossible date with nullify_date succeeds and supersedes draft parse_dates."""
        plan = {
            "generation_mode": "cleanse",
            "datasets": {
                "dbo.Customers_Raw": {
                    "steps": [
                        {
                            "step_id": "step_ae4691d8a5",
                            "column": "CreatedDate",
                            "action": "parse_dates",
                            "classification": "review",
                            "requires_user_choice": True,
                            "params": {"output_column": "CreatedDate_clean"},
                        }
                    ]
                }
            },
            "manual_review": [
                {
                    "id": "mr-createddate-imp",
                    "dataset": "dbo.Customers_Raw",
                    "column": "CreatedDate",
                    "issue_type": "impossible_date",
                    "status": "pending",
                    "resolution_options": [
                        {"id": "nullify_date", "action": "nullify_date", "label": "Nullify impossible dates"}
                    ]
                }
            ]
        }
        updated, errs = apply_manual_resolutions(
            plan,
            [{"item_id": "mr-createddate-imp", "resolution_id": "nullify_date"}]
        )
        self.assertEqual(errs, [])
        valid, issues = validate_promoted_plan(updated)
        self.assertTrue(valid, f"Validation failed with: {[i.message for i in issues]}")
        # The old draft step should be superseded
        steps = updated["datasets"]["dbo.Customers_Raw"]["steps"]
        self.assertEqual(len(steps), 1)
        self.assertEqual(steps[0]["action"], "nullify_date")
        self.assertEqual(steps[0]["manual_resolution"]["selected_option_id"], "nullify_date")


