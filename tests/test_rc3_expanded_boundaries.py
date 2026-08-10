"""
Expanded Boundary and Adversarial Tests (RC-3)
==============================================
Provides robust, edge-case coverage of Agent Dhara's scoring ledger,
waiver evaluation, remediation safety, and security filters.
"""
from __future__ import annotations

import unittest
from datetime import datetime, timezone, timedelta
import pandas as pd
from typing import Any, Dict, List

# Target system imports
from agent.service_layer import build_dq_scorecard
from agent.governance.waiver_registry import WaiverRegistry
from agent.governance.waiver_evaluator import apply_waivers_to_assessment
from agent.remediation.action_contract import RemediationAction
from agent.remediation.planner import plan_remediations
from agent.remediation.executor import execute_remediations
from agent.remediation.rollback import rollback_remediations
from agent.remediation.post_validation import run_post_remediation_validation
from agent.security.output_sanitizer import escape_html_value, redact_paths, sanitize_csv_cell


class TestScoreLedgerBoundaries(unittest.TestCase):
    """Verifies score ledger mathematics, caps, and advisory exclusions."""

    def test_score_non_negative_cap(self):
        """Verify that extreme DQ issue counts never result in a negative score (capped at 0.0)."""
        # Create an assessment result with 10 gating HIGH issues that each affect all rows
        result = {
            "datasets": {
                "heavy_failure.csv": {
                    "row_count": 1000,
                    "columns": {"id": {"dtype": "int"}},
                    "assessment_status": "COMPLETE"
                }
            },
            "data_quality_issues": {
                "datasets": {
                    "heavy_failure.csv": {
                        "summary": {
                            "issue_count": 50,
                            "high_severity": 10,
                            "medium_severity": 20,
                            "low_severity": 20,
                            "dq_score_0_100": 0.0  # Force 0 score at engine level
                        },
                        "issues": [
                            {
                                "column": "id",
                                "type": f"high_issue_{idx}",
                                "severity": "high",
                                "count": 1000,
                                "gating": True
                            }
                            for idx in range(10)
                        ]
                    }
                }
            }
        }
        scorecard = build_dq_scorecard(result)
        self.assertEqual(scorecard["overall_dq_score"], 0.0)
        self.assertEqual(scorecard["verdict"], "BLOCKED")

    def test_advisory_issues_ignored_in_readiness(self):
        """Verify that issues marked gating=False do not trigger NEEDS_WORK or BLOCKED unless thresholds crossed."""
        result = {
            "datasets": {
                "advisory_only.csv": {
                    "row_count": 100,
                    "columns": {"id": {"dtype": "int"}},
                    "assessment_status": "COMPLETE"
                }
            },
            "data_quality_issues": {
                "datasets": {
                    "advisory_only.csv": {
                        "summary": {
                            "issue_count": 5,
                            "high_severity": 5,
                            "medium_severity": 0,
                            "low_severity": 0,
                            # Supplying a high score because the engine ignored them
                            "dq_score_0_100": 95.0
                        },
                        "issues": [
                            {
                                "column": "id",
                                "type": "advisory_high",
                                "severity": "high",
                                "count": 5,
                                "gating": False
                            }
                        ]
                    }
                }
            }
        }
        scorecard = build_dq_scorecard(result)
        # Verdict should be READY_WITH_WARNINGS instead of NEEDS_WORK because high issues are advisory
        self.assertEqual(scorecard["verdict"], "READY_WITH_WARNINGS")


class TestWaiverBoundaryConditions(unittest.TestCase):
    """Verifies waiver lifecycle boundaries: expired, pre-active, and target mismatches."""

    def setUp(self):
        self.now = datetime(2026, 8, 3, 12, 0, 0, tzinfo=timezone.utc)
        self.dq_data = {
            "datasets": {
                "orders.csv": {
                    "issues": [
                        {
                            "type": "nulls",
                            "column": "customer_id",
                            "severity": "high",
                            "gating": True
                        }
                    ]
                }
            }
        }

    def test_active_waiver_applied(self):
        """Waiver is active and matches dataset/rule ID -> issue is waived."""
        waivers = [
            {
                "waiver_id": "WVR-001",
                "rule_id": "nulls",
                "dataset": "orders.csv",
                "effective_from": (self.now - timedelta(days=1)).isoformat(),
                "expires_at": (self.now + timedelta(days=1)).isoformat(),
                "effect": {"suppress_gating": True, "suppress_score_impact": True}
            }
        ]
        reg = WaiverRegistry(waivers)
        res = apply_waivers_to_assessment(self.dq_data, reg, current_time=self.now)
        issue = res["datasets"]["orders.csv"]["issues"][0]
        self.assertTrue(issue.get("waived"))
        self.assertFalse(issue.get("gating"))

    def test_expired_waiver_ignored(self):
        """Waiver is expired -> issue remains gating."""
        waivers = [
            {
                "waiver_id": "WVR-002",
                "rule_id": "nulls",
                "dataset": "orders.csv",
                "effective_from": (self.now - timedelta(days=5)).isoformat(),
                "expires_at": (self.now - timedelta(days=1)).isoformat(),
                "effect": {"suppress_gating": True, "suppress_score_impact": True}
            }
        ]
        reg = WaiverRegistry(waivers)
        res = apply_waivers_to_assessment(self.dq_data, reg, current_time=self.now)
        issue = res["datasets"]["orders.csv"]["issues"][0]
        self.assertFalse(issue.get("waived", False))
        self.assertTrue(issue.get("gating"))

    def test_pre_active_waiver_ignored(self):
        """Waiver is not yet effective -> issue remains gating."""
        waivers = [
            {
                "waiver_id": "WVR-003",
                "rule_id": "nulls",
                "dataset": "orders.csv",
                "effective_from": (self.now + timedelta(days=1)).isoformat(),
                "expires_at": (self.now + timedelta(days=5)).isoformat(),
                "effect": {"suppress_gating": True, "suppress_score_impact": True}
            }
        ]
        reg = WaiverRegistry(waivers)
        res = apply_waivers_to_assessment(self.dq_data, reg, current_time=self.now)
        issue = res["datasets"]["orders.csv"]["issues"][0]
        self.assertFalse(issue.get("waived", False))
        self.assertTrue(issue.get("gating"))

    def test_rule_mismatch_not_applied(self):
        """Waiver targets 'whitespace' but issue is 'nulls' -> issue remains gating."""
        waivers = [
            {
                "waiver_id": "WVR-004",
                "rule_id": "whitespace",
                "dataset": "orders.csv",
                "effective_from": (self.now - timedelta(days=1)).isoformat(),
                "expires_at": (self.now + timedelta(days=1)).isoformat(),
                "effect": {"suppress_gating": True, "suppress_score_impact": True}
            }
        ]
        reg = WaiverRegistry(waivers)
        res = apply_waivers_to_assessment(self.dq_data, reg, current_time=self.now)
        issue = res["datasets"]["orders.csv"]["issues"][0]
        self.assertFalse(issue.get("waived", False))
        self.assertTrue(issue.get("gating"))


class TestRemediationSafety(unittest.TestCase):
    """Verifies dry-run isolation, apply execution, and exact rollback behaviour."""

    def setUp(self):
        self.plans = plan_remediations({
            "datasets": {
                "data.csv": {
                    "issues": [
                        {
                            "type": "whitespace",
                            "column": "name",
                            "fixability_details": {"fixability": "AUTO_FIXABLE"}
                        }
                    ]
                }
            }
        }, mode="APPLY_APPROVED_FIXES")
        self.df = pd.DataFrame({"name": ["  John  ", "Bob "]})

    def test_dry_run_idempotency(self):
        """Verify that dry-run mode does not apply modifications to the original input DataFrame."""
        plans_dry = plan_remediations({
            "datasets": {
                "data.csv": {
                    "issues": [
                        {
                            "type": "whitespace",
                            "column": "name",
                            "fixability_details": {"fixability": "AUTO_FIXABLE"}
                        }
                    ]
                }
            }
        }, mode="DRY_RUN")
        
        dfs = {"data.csv": self.df.copy()}
        updated_dfs, executed = execute_remediations(dfs, plans_dry)
        
        # Dry run should NOT perform modifications in-place on data.csv, but records planned action
        self.assertEqual(len(executed), 1)
        self.assertEqual(executed[0].mode, "DRY_RUN")
        self.assertEqual(updated_dfs["data.csv"]["name"].iloc[0], "  John  ")
        self.assertNotIn("name_raw", updated_dfs["data.csv"].columns)


    def test_apply_and_rollback_flow(self):
        """Verify full lifecycle: original -> apply (preserves raw) -> rollback (exact restoration)."""
        dfs = {"data.csv": self.df.copy()}
        
        # 1. Apply
        updated_dfs, executed = execute_remediations(dfs, self.plans)
        updated_df = updated_dfs["data.csv"]
        self.assertIn("name_raw", updated_df.columns)
        self.assertEqual(updated_df["name"].iloc[0], "John")
        
        # 2. Rollback
        restored_dfs = rollback_remediations(updated_dfs, executed)
        restored_df = restored_dfs["data.csv"]
        self.assertNotIn("name_raw", restored_df.columns)
        self.assertEqual(restored_df["name"].iloc[0], "  John  ")


class TestSecurityAdversarial(unittest.TestCase):
    """Verifies string sanitization boundaries against injection vectors."""

    def test_html_injection_redacted(self):
        """HTML markup and scripts must be fully escaped."""
        payloads = [
            ("<script>alert(1)</script>", "&lt;script&gt;alert(1)&lt;/script&gt;"),
            ("<img src=x onerror=alert(2)>", "&lt;img src=x onerror=alert(2)&gt;"),
            ("hello <b>world</b>", "hello &lt;b&gt;world&lt;/b&gt;")
        ]
        for val, expected in payloads:
            self.assertEqual(escape_html_value(val), expected)

    def test_csv_formula_injection(self):
        """CSV cells starting with formula trigger chars (=, +, -, @) must be escaped."""
        payloads = [
            ("=SUM(A1:A10)", "'=SUM(A1:A10)"),
            ("+100", "'+100"),
            ("-50", "'-50"),
            ("@username", "'@username"),
            ("normal_text", "normal_text")
        ]
        for val, expected in payloads:
            self.assertEqual(sanitize_csv_cell(val), expected)

    def test_path_redaction(self):
        """Symmetric path and credential redactor must match OS patterns."""
        self.assertIn("[REDACTED_USER]", redact_paths("C:\\Users\\admin\\Desktop"))
        self.assertIn("[REDACTED_USER]", redact_paths("/home/ubuntu/data"))
        self.assertIn("[REDACTED_CREDENTIALS]", redact_paths("my-connection-string?sas_token=abcdefg"))
