import unittest
import pytest
from agent.etl_pipeline.action_registry import (
    ACTION_REGISTRY,
    is_phase1_action,
    is_phase2_action,
    is_invalid_action,
)
from agent.etl_pipeline.phase_classifier import (
    classify_action_phase,
    split_plan_phases,
    PlanPhaseError,
)
from agent.etl_pipeline.classify_steps import classify_step_bucket

class TestPhaseTaxonomy(unittest.TestCase):

    def test_deduplicate_is_phase_1(self):
        self.assertTrue(is_phase1_action("deduplicate"))
        self.assertFalse(is_phase2_action("deduplicate"))
        self.assertEqual(classify_action_phase("deduplicate"), "cleanse")

    def test_drop_column_is_phase_1(self):
        self.assertTrue(is_phase1_action("drop_column"))
        self.assertFalse(is_phase2_action("drop_column"))
        self.assertEqual(classify_action_phase("drop_column"), "cleanse")

    def test_drop_rows_is_phase_1(self):
        self.assertTrue(is_phase1_action("drop_rows"))
        self.assertFalse(is_phase2_action("drop_rows"))
        self.assertEqual(classify_action_phase("drop_rows"), "cleanse")

    def test_range_clip_is_phase_1(self):
        self.assertTrue(is_phase1_action("range_clip"))
        self.assertFalse(is_phase2_action("range_clip"))
        self.assertEqual(classify_action_phase("range_clip"), "cleanse")

    def test_clip_outliers_is_phase_1(self):
        self.assertTrue(is_phase1_action("clip_outliers"))
        self.assertFalse(is_phase2_action("clip_outliers"))
        self.assertEqual(classify_action_phase("clip_outliers"), "cleanse")

    def test_validate_relationship_is_phase_2(self):
        self.assertFalse(is_phase1_action("validate_and_partition_relationship"))
        self.assertTrue(is_phase2_action("validate_and_partition_relationship"))
        self.assertEqual(classify_action_phase("validate_and_partition_relationship"), "transform")

    def test_quarantine_orphan_fk_is_phase_2(self):
        self.assertFalse(is_phase1_action("quarantine_orphan_fk"))
        self.assertTrue(is_phase2_action("quarantine_orphan_fk"))
        self.assertEqual(classify_action_phase("quarantine_orphan_fk"), "transform")

    def test_fill_or_drop_is_invalid(self):
        self.assertTrue(is_invalid_action("fill_or_drop"))
        self.assertFalse(is_phase1_action("fill_or_drop"))
        self.assertFalse(is_phase2_action("fill_or_drop"))
        self.assertEqual(classify_action_phase("fill_or_drop"), "invalid")

    def test_split_plan_phases_raises_on_invalid_action(self):
        plan = {
            "plan_id": "invalid_test",
            "datasets": {
                "dbo.Customers_Raw": {
                    "steps": [
                        {"order": 1, "action": "trim", "column": "CustomerName"},
                        {"order": 2, "action": "fill_or_drop", "column": "Phone"},
                    ]
                }
            }
        }
        with self.assertRaises(PlanPhaseError) as ctx:
            split_plan_phases(plan)
        
        self.assertEqual(len(ctx.exception.invalid_steps), 1)
        self.assertEqual(ctx.exception.invalid_steps[0]["action"], "fill_or_drop")

    @pytest.mark.xfail(strict=True, reason="Legacy resolution actions mapping checked pre-PR 4.5 action registry names - tracked by ADR-006")
    def test_all_catalog_actions_exist_in_action_registry(self):
        # Read manual_review_catalog and check all resolution actions
        from agent.etl_pipeline.manual_review_catalog import RESOLUTION_TO_ACTION_MAP
        for resolution_id, action in RESOLUTION_TO_ACTION_MAP.items():
            self.assertIn(action, ACTION_REGISTRY, f"Action '{action}' from manual_review_catalog is not registered!")

    def test_phase2_action_is_never_auto_in_phase1(self):
        # Make sure no Phase 2 action has phase=1 or is classified as auto/cleanse
        for action, meta in ACTION_REGISTRY.items():
            bucket_res = classify_step_bucket(action)
            if meta.get("phase") == 2:
                self.assertEqual(bucket_res["phase"], "transform")
                self.assertNotEqual(bucket_res["bucket"], "auto")

if __name__ == "__main__":
    unittest.main()
