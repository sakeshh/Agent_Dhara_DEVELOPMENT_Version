import unittest
import pandas as pd
import json
from agent.service_layer import classify_readiness, aggregate_readiness, build_dq_scorecard
from agent.profiling.relationship_registry import (
    _normalize_key_series,
    _build_composite_key_series,
    compute_directional_orphans,
    validate_relationship_config,
    RelationshipConfigError
)
from agent.profiling.dq_checks import analyze_dataset_quality

class TestR1ProductionReadiness(unittest.TestCase):

    def test_readiness_precedence_ac01_to_ac06(self):
        # AC-01: classify(score=20, gating_highs=1) == "BLOCKED" — score < 35 wins over HIGH check
        state, qualifier = classify_readiness(
            assessment_status="COMPLETE",
            score=20.0,
            gating_highs=1,
            gating_mediums=0,
            advisory_highs=0,
            medium=0
        )
        self.assertEqual(state, "BLOCKED")
        self.assertEqual(qualifier, "score_below_blocked_threshold")

        # AC-02: classify(score=75, gating_highs=1) == "NEEDS_WORK"
        state, qualifier = classify_readiness(
            assessment_status="COMPLETE",
            score=75.0,
            gating_highs=1,
            gating_mediums=0,
            advisory_highs=0,
            medium=0
        )
        self.assertEqual(state, "NEEDS_WORK")
        self.assertEqual(qualifier, "gating_high_issues_present")

        # AC-03: classify(score=75, gating_highs=0, medium=1) == "READY_WITH_WARNINGS"
        state, qualifier = classify_readiness(
            assessment_status="COMPLETE",
            score=75.0,
            gating_highs=0,
            gating_mediums=0,
            advisory_highs=0,
            medium=1
        )
        self.assertEqual(state, "READY_WITH_WARNINGS")
        self.assertEqual(qualifier, "non_blocking_issues_present")

        # AC-04: classify(score=75, gating_highs=0, medium=0) == "READY"
        state, qualifier = classify_readiness(
            assessment_status="COMPLETE",
            score=75.0,
            gating_highs=0,
            gating_mediums=0,
            advisory_highs=0,
            medium=0
        )
        self.assertEqual(state, "READY")
        self.assertEqual(qualifier, "all_gates_passed")

        # AC-05: classify(score=None) == "UNKNOWN"
        state, qualifier = classify_readiness(
            assessment_status="COMPLETE",
            score=None,
            gating_highs=0,
            gating_mediums=0,
            advisory_highs=0,
            medium=0
        )
        self.assertEqual(state, "UNKNOWN")
        self.assertEqual(qualifier, "score_unavailable")

        # AC-06: classify(assessment_status="PARTIAL") == "UNKNOWN"
        state, qualifier = classify_readiness(
            assessment_status="PARTIAL",
            score=90.0,
            gating_highs=0,
            gating_mediums=0,
            advisory_highs=0,
            medium=0
        )
        self.assertEqual(state, "UNKNOWN")
        self.assertEqual(qualifier, "assessment_incomplete")

    def test_readiness_aggregation_ac07_to_ac08(self):
        # AC-07: aggregate(["READY", "UNKNOWN"]) == "UNKNOWN"
        self.assertEqual(aggregate_readiness(["READY", "UNKNOWN"]), "UNKNOWN")
        self.assertEqual(aggregate_readiness(["READY_WITH_WARNINGS", "UNKNOWN"]), "UNKNOWN")

        # AC-08: aggregate(["BLOCKED", "UNKNOWN"]) == "BLOCKED"
        self.assertEqual(aggregate_readiness(["BLOCKED", "UNKNOWN"]), "BLOCKED")
        self.assertEqual(aggregate_readiness(["BLOCKED", "NEEDS_WORK"]), "BLOCKED")

    def test_normalization_and_composite_keys(self):
        # Test basic normalization (trim, casefold, token exclusion)
        s = pd.Series(["  abc  ", "XYZ", "UNKNOWN", "N/A", "null", ""], dtype=object)
        norm = _normalize_key_series(s, {
            "trim": True,
            "casefold": True,
            "null_tokens": ["unknown", "n/a", "null", ""]
        })
        # Check that excluded are masked to NA
        self.assertEqual(norm["values"].dropna().tolist(), ["abc", "xyz"])
        self.assertTrue(norm["placeholder_mask"].iloc[2]) # "UNKNOWN"
        self.assertTrue(norm["placeholder_mask"].iloc[3]) # "N/A"
        self.assertTrue(norm["placeholder_mask"].iloc[4]) # "null"
        self.assertTrue(norm["null_mask"].iloc[5])        # ""

        # Test composite key building
        df = pd.DataFrame({
            "tenant_id": ["T1", "T1", "  T2  ", "T2"],
            "prod_id": ["P1", "P2", "P3", "UNKNOWN"]
        })
        keys, excl = _build_composite_key_series(df, ["tenant_id", "prod_id"], {
            "trim": True,
            "casefold": False,
            "null_tokens": ["UNKNOWN"]
        })
        self.assertEqual(keys.iloc[0], ("T1", "P1"))
        self.assertEqual(keys.iloc[2], ("T2", "P3"))
        self.assertTrue(pd.isna(keys.iloc[3])) # Component "UNKNOWN" is excluded, so whole composite is NA
        self.assertTrue(excl.iloc[3])

    def test_directional_orphans_composite(self):
        # Test parent-child directional mismatch on composite keys
        parent_df = pd.DataFrame({
            "t_id": ["T1", "T1", "T2"],
            "p_id": ["P1", "P2", "P3"]
        })
        child_df = pd.DataFrame({
            "t_id": ["T1", "T1", "T2", "T3", "T1"],
            "p_id": ["P1", "P3", "P3", "P4", None]
        })
        
        metrics = compute_directional_orphans(
            parent_df=parent_df,
            parent_columns=["t_id", "p_id"],
            child_df=child_df,
            child_columns=["t_id", "p_id"],
            normalization={"trim": True, "null_tokens": ["UNKNOWN"]}
        )

        # Expected:
        # parent unique keys: (T1, P1), (T1, P2), (T2, P3)
        # child unique keys: (T1, P1), (T1, P3), (T2, P3), (T3, P4). Row index 4 is null-excluded.
        # shared keys: (T1, P1), (T2, P3) -> 2 keys
        # child orphans: (T1, P3), (T3, P4) -> 2 keys (row indices: 1 and 3)
        # unreferenced parent: (T1, P2) -> 1 key (row index: 1)
        self.assertEqual(metrics["shared_distinct_keys"], 2)
        self.assertEqual(metrics["child_orphan_distinct_keys"], 2)
        self.assertEqual(metrics["child_orphan_row_count"], 2)
        self.assertEqual(metrics["unreferenced_parent_distinct_keys"], 1)
        self.assertEqual(metrics["unreferenced_parent_row_count"], 1)
        self.assertEqual(metrics["child_orphan_indexes"], [1, 3])
        self.assertEqual(metrics["unreferenced_parent_indexes"], [1])

    def test_config_validation(self):
        # Count mismatch
        rel_bad = {
            "name": "bad",
            "parent": {"dataset": "A", "columns": ["id"]},
            "child": {"dataset": "B", "columns": ["id", "code"]},
            "severity": "high"
        }
        with self.assertRaises(RelationshipConfigError):
            validate_relationship_config(rel_bad)

        # Inferred gating
        rel_inferred_gate = {
            "name": "bad",
            "source": "inferred",
            "gating": True,
            "parent": {"dataset": "A", "columns": ["id"]},
            "child": {"dataset": "B", "columns": ["id"]},
            "severity": "high"
        }
        with self.assertRaises(RelationshipConfigError):
            validate_relationship_config(rel_inferred_gate)
            
        # Valid config
        rel_good = {
            "name": "good",
            "source": "declared",
            "gating": True,
            "parent": {"dataset": "A", "columns": ["id"]},
            "child": {"dataset": "B", "columns": ["id"]},
            "severity": "high"
        }
        validate_relationship_config(rel_good)
        
    def test_duplicate_group_decomposition(self):
        # Exact duplicates on 3 rows
        df = pd.DataFrame({
            "id": ["1", "2", "3", "3", "3", "4"],
            "name": ["A", "B", "C", "C", "C", "D"]
        })
        
        # Test full row duplicate metrics calculation by running analyze_dataset_quality
        # candidate PK would be on "id" but since there's duplicates it failed.
        # Let's mock a profile with "id" having candidate_primary_key=True to trigger uniqueness validation.
        profile = {
            "columns": {
                "id": {
                    "nullable": "YES",
                    "candidate_primary_key": True,
                    "semantic_type": "identifier"
                }
            }
        }
        thresholds = {
            "relationships": {"readiness": {"ready_threshold": 70, "blocked_threshold": 35}},
            "uniqueness": {}
        }
        
        res = analyze_dataset_quality(
            name="test_dataset",
            df=df,
            profile=profile,
            thresholds=thresholds,
            business_rules={}
        )
        
        issues = res["issues"]
        # Find candidate_key_violation issue
        key_issue = next(i for i in issues if i["type"] == "candidate_key_violation")
        self.assertIsNotNone(key_issue.get("duplicate_group_metrics"))
        
        # In id, "3" repeats 3 times.
        # group_count = 1 (only group "3" has count > 1)
        # rows_in_groups = 3 (the three rows of "3")
        # redundant_rows = 3 - 1 = 2
        # largest_group_size = 3
        metrics = key_issue["duplicate_group_metrics"]
        self.assertEqual(metrics["group_count"], 1)
        self.assertEqual(metrics["rows_in_groups"], 3)
        self.assertEqual(metrics["redundant_rows"], 2)
        self.assertEqual(metrics["largest_group_size"], 3)
