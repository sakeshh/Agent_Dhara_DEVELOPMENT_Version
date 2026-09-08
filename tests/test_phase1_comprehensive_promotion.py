"""
Tests for comprehensive Phase 1 single-dataset issue promotion and AI recommendation resolution.
"""
from __future__ import annotations

import unittest
from typing import Any, Dict, List

from agent.etl_pipeline.action_registry import is_phase1_action, is_phase2_action
from agent.etl_pipeline.manual_review_catalog import action_for_resolution
from agent.etl_pipeline.manual_review_promote import apply_manual_resolutions


class TestPhase1ComprehensivePromotion(unittest.TestCase):

    def test_single_dataset_vs_multi_dataset_action_taxonomy(self):
        """Single-dataset column cleansers must be Phase 1; relational operations Phase 2."""
        single_dataset_actions = [
            "trim", "trim_preserve_case", "lowercase", "uppercase",
            "range_clip", "clip_outliers", "cap_outliers", "flag_range_violation",
            "replace_sentinel_values", "nullify_future_dates", "nullify_dummy_dates",
            "parse_dates", "parse_dates_safe", "coerce_numeric", "cast_type",
            "normalize_phone", "hash_phone", "sanitize_email", "standardize_boolean",
            "preserve_null_and_flag", "fill_null_with_constant", "quarantine_null_rows",
            "impute_numeric", "deduplicate"
        ]
        for act in single_dataset_actions:
            self.assertTrue(is_phase1_action(act), f"Action '{act}' must be Phase 1")
            self.assertFalse(is_phase2_action(act), f"Action '{act}' cannot be Phase 2")

        multi_dataset_actions = [
            "validate_and_partition_relationship",
            "quarantine_orphan_fk",
            "join_datasets",
        ]
        for act in multi_dataset_actions:
            self.assertTrue(is_phase2_action(act), f"Relational action '{act}' must be Phase 2")
            self.assertFalse(is_phase1_action(act), f"Relational action '{act}' cannot be Phase 1")

    def test_action_for_resolution_aliases(self):
        """action_for_resolution should support recommended, direct action names, and standard options."""
        # Direct action name
        self.assertEqual(action_for_resolution("negative_values", "range_clip"), "range_clip")
        self.assertEqual(action_for_resolution("negative_values", "clip_outliers"), "clip_outliers")
        self.assertEqual(action_for_resolution("negative_values", "flag_outliers"), "flag_outliers")

        # Recommended alias
        opts = [
            {"id": "opt1", "action": "range_clip", "recommended": True},
            {"id": "opt2", "action": "noop", "recommended": False},
        ]
        self.assertEqual(action_for_resolution("negative_values", "recommended", opts), "range_clip")

    def test_llm_suggested_promotes_negative_values(self):
        """Negative price with llm_suggested promotes cleanly to Phase 1 steps."""
        plan = {
            "plan_id": "test_neg_price",
            "generation_mode": "cleanse",
            "datasets": {"dbo.Courses": {"steps": []}},
            "manual_review": [
                {
                    "id": "dbo.Courses|Price|negative_values_in_non_negative_measure",
                    "dataset": "dbo.Courses",
                    "column": "Price",
                    "issue_type": "negative_values_in_non_negative_measure",
                    "suggested_action": "flag_range_violation",
                    "status": "pending",
                    "llm_recommendation": {
                        "action": "range_clip",
                        "suggested_fix": "Clip negative price values to 0.00"
                    }
                }
            ]
        }
        resolutions = [
            {
                "item_id": "dbo.Courses|Price|negative_values_in_non_negative_measure",
                "resolution_id": "llm_suggested"
            }
        ]

        new_plan, errs = apply_manual_resolutions(plan, resolutions)
        self.assertEqual(len(errs), 0, f"Expected no errors, got: {errs}")

        steps = new_plan["datasets"]["dbo.Courses"]["steps"]
        self.assertEqual(len(steps), 1)
        self.assertEqual(steps[0]["column"], "Price")
        self.assertEqual(steps[0]["action"], "range_clip")
        self.assertEqual(steps[0]["phase"], "cleanse")
        self.assertIn("min_value", steps[0]["params"])
        self.assertEqual(steps[0]["params"]["status_column"], "Price_validation_status")

        resolved = new_plan["resolved_manual_review"]
        self.assertEqual(len(resolved), 1)
        self.assertEqual(resolved[0]["status"], "resolved")
        self.assertTrue(resolved[0]["promoted"])

    def test_llm_suggested_promotes_implausible_age(self):
        """Implausible age with llm_suggested promotes cleanly to Phase 1 steps."""
        plan = {
            "plan_id": "test_age",
            "generation_mode": "cleanse",
            "datasets": {"dbo.Customers": {"steps": []}},
            "manual_review": [
                {
                    "id": "dbo.Customers|Age|implausible_age",
                    "dataset": "dbo.Customers",
                    "column": "Age",
                    "issue_type": "implausible_age",
                    "suggested_action": "flag_range_violation",
                    "status": "pending",
                    "llm_recommendation": {
                        "action": "range_clip",
                        "suggested_fix": "Validate age between 0 and 150"
                    }
                }
            ]
        }
        resolutions = [
            {
                "item_id": "dbo.Customers|Age|implausible_age",
                "resolution_id": "llm_suggested"
            }
        ]

        new_plan, errs = apply_manual_resolutions(plan, resolutions)
        self.assertEqual(len(errs), 0, f"Expected no errors, got: {errs}")

        steps = new_plan["datasets"]["dbo.Customers"]["steps"]
        self.assertEqual(len(steps), 1)
        self.assertEqual(steps[0]["column"], "Age")
        self.assertEqual(steps[0]["action"], "range_clip")
        self.assertEqual(steps[0]["phase"], "cleanse")

    def test_llm_suggested_promotes_sentinels_and_phones(self):
        """Sentinel values and phone sanitization with llm_suggested promote cleanly."""
        plan = {
            "plan_id": "test_sentinel_phone",
            "generation_mode": "cleanse",
            "datasets": {"dbo.Customers_Raw": {"steps": []}},
            "manual_review": [
                {
                    "id": "dbo.Customers_Raw|InstructorID|sentinel_numeric_value",
                    "dataset": "dbo.Customers_Raw",
                    "column": "InstructorID",
                    "issue_type": "sentinel_numeric_value",
                    "suggested_action": "replace_sentinel_values",
                    "status": "pending",
                    "llm_recommendation": {
                        "action": "replace_sentinel_values",
                        "suggested_fix": "Replace 999 with null"
                    }
                },
                {
                    "id": "dbo.Customers_Raw|Phone|invalid_phone",
                    "dataset": "dbo.Customers_Raw",
                    "column": "Phone",
                    "issue_type": "invalid_phone",
                    "suggested_action": "normalize_phone",
                    "status": "pending",
                    "llm_recommendation": {
                        "action": "normalize_phone",
                        "suggested_fix": "Strip non-numeric characters"
                    }
                }
            ]
        }
        resolutions = [
            {"item_id": "dbo.Customers_Raw|InstructorID|sentinel_numeric_value", "resolution_id": "llm_suggested"},
            {"item_id": "dbo.Customers_Raw|Phone|invalid_phone", "resolution_id": "llm_suggested"},
        ]

        new_plan, errs = apply_manual_resolutions(plan, resolutions)
        self.assertEqual(len(errs), 0, f"Expected no errors, got: {errs}")

        steps = new_plan["datasets"]["dbo.Customers_Raw"]["steps"]
        self.assertEqual(len(steps), 2)
        actions = [s["action"] for s in steps]
        self.assertIn("replace_sentinel_values", actions)
        self.assertIn("normalize_phone", actions)
        for s in steps:
            self.assertEqual(s["phase"], "cleanse")

    def test_batch_manual_review_promotion_simulating_session(self):
        """Batch promotion of diverse items promotes all single-table items without phase2_blocked skip."""
        plan = {
            "plan_id": "test_batch",
            "generation_mode": "cleanse",
            "datasets": {
                "dbo.Courses": {"steps": []},
                "dbo.Customers": {"steps": []},
                "dbo.Customers_Raw": {"steps": []},
            },
            "manual_review": [
                {"id": "dbo.Courses|Price|negative_values", "dataset": "dbo.Courses", "column": "Price", "issue_type": "negative_values", "suggested_action": "flag_range_violation", "status": "pending", "llm_recommendation": {"action": "range_clip"}},
                {"id": "dbo.Customers|Age|negative_values", "dataset": "dbo.Customers", "column": "Age", "issue_type": "negative_values", "suggested_action": "flag_range_violation", "status": "pending", "llm_recommendation": {"action": "clip_outliers"}},
                {"id": "dbo.Customers|Email|invalid_email", "dataset": "dbo.Customers", "column": "Email", "issue_type": "invalid_email", "suggested_action": "sanitize_email", "status": "pending", "llm_recommendation": {"action": "sanitize_email"}},
                {"id": "dbo.Customers_Raw|CreatedDate|future_dates", "dataset": "dbo.Customers_Raw", "column": "CreatedDate", "issue_type": "future_dates", "suggested_action": "nullify_future_dates", "status": "pending", "llm_recommendation": {"action": "nullify_future_dates"}},
            ]
        }
        resolutions = [
            {"item_id": "dbo.Courses|Price|negative_values", "resolution_id": "llm_suggested"},
            {"item_id": "dbo.Customers|Age|negative_values", "resolution_id": "llm_suggested"},
            {"item_id": "dbo.Customers|Email|invalid_email", "resolution_id": "llm_suggested"},
            {"item_id": "dbo.Customers_Raw|CreatedDate|future_dates", "resolution_id": "llm_suggested"},
        ]

        new_plan, errs = apply_manual_resolutions(plan, resolutions)
        self.assertEqual(len(errs), 0)

        # None of the items should be skipped with phase2_blocked
        resolved = new_plan["resolved_manual_review"]
        for r in resolved:
            self.assertEqual(r["status"], "resolved")
            self.assertNotEqual(r.get("skip_reason"), "phase2_blocked")
            self.assertTrue(r["promoted"])


if __name__ == "__main__":
    unittest.main()
