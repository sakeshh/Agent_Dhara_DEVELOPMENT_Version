"""
Tests for legacy plan action migration.
"""
from __future__ import annotations
import unittest

from agent.etl_pipeline.plan_migration import migrate_legacy_plan_actions

class TestPlanMigration(unittest.TestCase):
    def test_legacy_fill_constant_migrates_automatically(self) -> None:
        plan = {
            "datasets": {
                "sales.csv": {
                    "steps": [
                        {
                            "column": "region",
                            "action": "fill_nulls_simple",
                            "params": {
                                "fill_strategy": "constant",
                                "fill_value": "UNKNOWN",
                            }
                        }
                    ]
                }
            }
        }
        migrated, applied, review = migrate_legacy_plan_actions(plan)
        self.assertEqual(len(review), 0)
        self.assertEqual(len(applied), 1)
        step = migrated["datasets"]["sales.csv"]["steps"][0]
        self.assertEqual(step["action"], "fill_null_with_constant")
        self.assertEqual(step["params"]["value"], "UNKNOWN")
        self.assertTrue(step["params"]["preserve_raw"])

    def test_legacy_fill_impute_migrates_automatically(self) -> None:
        plan = {
            "datasets": {
                "sales.csv": {
                    "steps": [
                        {
                            "column": "amount",
                            "action": "fill_nulls_simple",
                            "params": {
                                "strategy": "median",
                            }
                        }
                    ]
                }
            }
        }
        migrated, applied, review = migrate_legacy_plan_actions(plan)
        self.assertEqual(len(review), 0)
        self.assertEqual(len(applied), 1)
        step = migrated["datasets"]["sales.csv"]["steps"][0]
        self.assertEqual(step["action"], "impute_numeric")
        self.assertEqual(step["params"]["strategy"], "median")

    def test_ambiguous_legacy_fill_requires_review(self) -> None:
        plan = {
            "datasets": {
                "sales.csv": {
                    "steps": [
                        {
                            "column": "region",
                            "action": "fill_nulls_simple",
                            "params": {}
                        }
                    ]
                }
            }
        }
        migrated, applied, review = migrate_legacy_plan_actions(plan)
        self.assertEqual(len(applied), 0)
        self.assertEqual(len(review), 1)
        step = migrated["datasets"]["sales.csv"]["steps"][0]
        self.assertIsNone(step["action"])
        self.assertEqual(step["migration_status"], "REVIEW_REQUIRED")

    def test_legacy_clip_or_flag_migrates(self) -> None:
        plan = {
            "datasets": {
                "sales.csv": {
                    "steps": [
                        {
                            "column": "amount",
                            "action": "clip_or_flag",
                            "params": {
                                "outlier_method": "flag"
                            }
                        }
                    ]
                }
            }
        }
        migrated, applied, review = migrate_legacy_plan_actions(plan)
        self.assertEqual(len(review), 0)
        self.assertEqual(len(applied), 1)
        step = migrated["datasets"]["sales.csv"]["steps"][0]
        self.assertEqual(step["action"], "flag_outliers")
