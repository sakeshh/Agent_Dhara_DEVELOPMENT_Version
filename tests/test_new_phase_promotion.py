"""
Regression tests for new Phase 1 dataset preparation vs Phase 2 cross-dataset promotion taxonomy.
"""
from __future__ import annotations

import unittest
import pytest
from typing import Any, Dict

from agent.etl_pipeline.action_registry import ACTION_REGISTRY, is_phase1_action, is_phase2_action
from agent.etl_pipeline.manual_review_catalog import RESOLUTION_TO_ACTION_MAP, action_for_resolution, LEGACY_AMBIGUOUS_RESOLUTIONS
from agent.etl_pipeline.manual_review_promote import apply_manual_resolutions
from agent.etl_pipeline.issue_registry import canonicalize_issue_type, ISSUE_REGISTRY


class TestNewPhasePromotion(unittest.TestCase):
    def test_invalid_numeric_resolves_to_coerce_numeric(self) -> None:
        # Check alias canonicalization
        self.assertEqual(canonicalize_issue_type("invalid_numeric_values"), "invalid_numeric")
        self.assertEqual(canonicalize_issue_type("invalid_number"), "invalid_numeric")
        self.assertEqual(canonicalize_issue_type("non_numeric_value"), "invalid_numeric")
        
        # Check first-class registry entry
        self.assertIn("invalid_numeric", ISSUE_REGISTRY)
        self.assertEqual(ISSUE_REGISTRY["invalid_numeric"]["phase1_action"], "coerce_numeric")
        
        # Check action registry property
        self.assertTrue(is_phase1_action("coerce_numeric"))

    @pytest.mark.xfail(strict=True, reason="Legacy resolution IDs superseded by PR 4.6 canonical remediation units - tracked by ADR-006")
    def test_legacy_resolution_ids_map_to_phase1_actions(self) -> None:
        # Deduplication options
        self.assertEqual(RESOLUTION_TO_ACTION_MAP["deduplicate_last"], "deduplicate")
        self.assertEqual(RESOLUTION_TO_ACTION_MAP["deduplicate_first"], "deduplicate")
        self.assertEqual(RESOLUTION_TO_ACTION_MAP["deduplicate_near_duplicates"], "flag_near_duplicate_cluster")
        self.assertEqual(RESOLUTION_TO_ACTION_MAP["deduplicate_business_key"], "flag_business_key_duplicate")
        self.assertEqual(RESOLUTION_TO_ACTION_MAP["deduplicate_insensitive_values"], "create_normalized_match_key")
        
        # Null options
        self.assertEqual(RESOLUTION_TO_ACTION_MAP["preserve_and_flag"], "preserve_null_and_flag")
        self.assertEqual(RESOLUTION_TO_ACTION_MAP["fill_constant"], "fill_null_with_constant")
        
        # Outliers
        self.assertEqual(RESOLUTION_TO_ACTION_MAP["flag_outliers"], "flag_outliers")
        
        # Verify legacy ambiguous set
        self.assertIn("fill_nulls", LEGACY_AMBIGUOUS_RESOLUTIONS)
        self.assertIn("fill_nulls_simple", LEGACY_AMBIGUOUS_RESOLUTIONS)
        self.assertIn("clip_or_flag", LEGACY_AMBIGUOUS_RESOLUTIONS)
        
        # All actions in the map should exist in the ACTION_REGISTRY
        for res_id, act in RESOLUTION_TO_ACTION_MAP.items():
            self.assertIn(act, ACTION_REGISTRY, f"Action '{act}' (from resolution '{res_id}') not registered.")

    def test_promotion_rejects_global_scope(self) -> None:
        plan = {
            "plan_id": "test_global",
            "generation_mode": "cleanse",
            "datasets": {
                "marketing.csv": {"steps": []}
            },
            "manual_review": [
                {
                    "id": "item1",
                    "dataset": "global", # global scope
                    "column": "campaign_id",
                    "issue_type": "nulls",
                    "message": "Null values issue",
                    "status": "pending",
                }
            ]
        }
        resolutions = [
            {"item_id": "item1", "resolution_id": "preserve_and_flag"}
        ]
        
        updated, errors = apply_manual_resolutions(plan, resolutions)
        self.assertTrue(any("global" in e for e in errors), f"Should raise a global scope error. Got: {errors}")
        
        # Assert item status is skipped or stays pending, and not promoted to steps
        self.assertEqual(updated["manual_review"], [])
        self.assertEqual(updated["resolved_manual_review"][0]["status"], "skipped")
        self.assertFalse(updated["resolved_manual_review"][0]["promoted"])

    def test_promotion_migrates_phase2_actions_in_phase1_plan(self) -> None:
        plan = {
            "plan_id": "test_phase2_migration",
            "generation_mode": "cleanse", # Phase 1 mode
            "datasets": {
                "sales.xml": {"steps": []}
            },
            "manual_review": [
                {
                    "id": "item1",
                    "dataset": "sales.xml",
                    "column": "amount",
                    "issue_type": "outlier_detected",
                    "message": "Outliers detected",
                    "status": "pending",
                    "resolution_options": [
                        {"id": "clip_vals", "action": "range_clip"} # range_clip is Phase 1 now under the new definition, let's use a Phase 2 action
                    ]
                }
            ]
        }
        
        # Let's verify validate_and_partition_relationship is Phase 2
        self.assertTrue(is_phase2_action("validate_and_partition_relationship"))
        
        # Let's try to promote validate_and_partition_relationship on a single dataset
        plan["manual_review"][0]["resolution_options"][0]["action"] = "validate_and_partition_relationship"
        resolutions = [
            {"item_id": "item1", "resolution_id": "clip_vals"}
        ]
        
        updated, errors = apply_manual_resolutions(plan, resolutions)
        
        # Validate_and_partition_relationship has no direct Phase 1 safe equivalent in the migrations list, so it will be skipped
        self.assertTrue(any("validate_and_partition_relationship" in e for e in errors))
        self.assertEqual(updated["datasets"]["sales.xml"]["steps"], [])


if __name__ == "__main__":
    unittest.main()
