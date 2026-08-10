"""
Tests for action registry invariants and new Phase 1 actions.
"""
from __future__ import annotations
import unittest

from agent.etl_pipeline.action_registry import (
    is_valid_action,
    is_invalid_action,
    is_legacy_action,
    get_semantic_prohibitions,
    requires_impact_preview,
)

class TestActionRegistry(unittest.TestCase):
    def test_legacy_actions_are_recognized_but_invalid(self) -> None:
        legacy_actions = ["fill_nulls_simple", "fill_or_drop", "clip_or_flag"]
        for act in legacy_actions:
            self.assertFalse(is_valid_action(act), f"{act} should not be valid")
            self.assertTrue(is_invalid_action(act), f"{act} should be invalid")
            self.assertTrue(is_legacy_action(act), f"{act} should be marked as legacy")

    def test_new_phase1_explicit_actions_are_valid(self) -> None:
        new_actions = [
            "preserve_null_and_flag",
            "fill_null_with_constant",
            "quarantine_null_rows",
            "impute_numeric",
            "accept_null_as_optional",
        ]
        for act in new_actions:
            self.assertTrue(is_valid_action(act), f"{act} should be valid")
            self.assertFalse(is_invalid_action(act), f"{act} should not be invalid")
            self.assertFalse(is_legacy_action(act), f"{act} is not a legacy action")

    def test_key_imputation_prohibitions(self) -> None:
        self.assertIn("primary_key", get_semantic_prohibitions("fill_null_with_constant"))
        self.assertIn("primary_key", get_semantic_prohibitions("impute_numeric"))
        self.assertNotIn("primary_key", get_semantic_prohibitions("preserve_null_and_flag"))

    def test_requires_impact_preview_setting(self) -> None:
        self.assertTrue(requires_impact_preview("impute_numeric"))
        self.assertFalse(requires_impact_preview("preserve_null_and_flag"))
