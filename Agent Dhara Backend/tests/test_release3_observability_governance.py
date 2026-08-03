import unittest
import pandas as pd
from datetime import datetime, timezone
from agent.observability.metrics import metrics_registry
from agent.observability.structured_logging import log_event
from agent.observability.tracing import ExecutionTracker
from agent.security.sample_protection import protect_samples, hash_value, mask_value
from agent.security.output_sanitizer import escape_html_value, sanitize_csv_cell, redact_paths
from agent.security.authorization import is_authorized_for_cleartext
from agent.governance.waiver_registry import WaiverRegistry
from agent.governance.waiver_evaluator import apply_waivers_to_assessment
from agent.governance.approval_policy import validate_approval_policy
from agent.remediation.action_contract import RemediationAction
from agent.remediation.planner import plan_remediations
from agent.remediation.executor import execute_remediations
from agent.remediation.rollback import rollback_remediations
from agent.remediation.post_validation import run_post_remediation_validation

class TestRelease3ObservabilityGovernance(unittest.TestCase):

    def test_observability_telemetry(self):
        # 1. Test metrics increment
        metrics_registry.inc_counter("dhara_assessment_runs_total", 2)
        metrics_registry.observe_histogram("dhara_assessment_duration_seconds", 1.25)
        
        snap = metrics_registry.get_snapshot()
        self.assertEqual(snap["counters"]["dhara_assessment_runs_total"], 2)
        self.assertEqual(snap["histograms"]["dhara_assessment_duration_seconds"]["count"], 1)

        # 2. Test Execution Tracing Context
        tracker = ExecutionTracker("RUN-TEST-001")
        with tracker.trace_stage("referential_integrity"):
            # Dummy work
            pass

    def test_privacy_and_security(self):
        # 1. Sample masking modes
        raw = ["secret@domain.com", "+1-555-0199"]
        masked = protect_samples(raw, mode="masked")
        self.assertEqual(masked[0], "s*****@domain.com")
        self.assertTrue(masked[1].startswith("+1-"))

        hashed = protect_samples(raw, mode="hashed", salt="salt123")
        self.assertTrue(hashed[0].startswith("sha256:"))

        redacted = protect_samples(raw, mode="redacted")
        self.assertEqual(redacted[0], "[REDACTED]")

        # 2. CSV / HTML sanitizer
        self.assertEqual(escape_html_value("<script>"), "&lt;script&gt;")
        self.assertEqual(sanitize_csv_cell("=SUM(1)"), "'=SUM(1)")

        # 3. Path redactions
        self.assertIn("[REDACTED_USER]", redact_paths("C:\\Users\\john\\Desktop\\data"))
        self.assertIn("[REDACTED_CREDENTIALS]", redact_paths("https://blob.core.windows.net/data?sas_token=abc"))

        # 4. Authorization checks
        self.assertTrue(is_authorized_for_cleartext(["data_steward"]))
        self.assertFalse(is_authorized_for_cleartext(["developer"]))

    def test_governance_waivers(self):
        # 1. Waiver Registry
        waiver_list = [
            {
                "waiver_id": "WVR-2026-001",
                "rule_id": "nulls",
                "dataset": "marketing.csv",
                "effective_from": "2026-08-01T00:00:00Z",
                "expires_at": "2026-08-15T00:00:00Z",
                "effect": {"suppress_gating": True, "suppress_score_impact": True}
            }
        ]
        reg = WaiverRegistry(waiver_list)
        active = reg.get_active_waivers(datetime(2026, 8, 3, tzinfo=timezone.utc))
        self.assertEqual(len(active), 1)

        # 2. Waiver Evaluator application
        mock_dq = {
            "datasets": {
                "marketing.csv": {
                    "issues": [
                        {
                            "type": "nulls",
                            "column": "email",
                            "severity": "high",
                            "gating": True
                        }
                    ]
                }
            }
        }
        res = apply_waivers_to_assessment(mock_dq, reg, current_time=datetime(2026, 8, 3, tzinfo=timezone.utc))
        issue = res["datasets"]["marketing.csv"]["issues"][0]
        self.assertTrue(issue["waived"])
        self.assertFalse(issue["gating"])
        self.assertFalse(issue["score_included"])

    def test_remediation_actions(self):
        # 1. Planner
        mock_dq = {
            "datasets": {
                "sales.csv": {
                    "issues": [
                        {
                            "type": "whitespace",
                            "column": "name",
                            "fixability_details": {"fixability": "AUTO_FIXABLE"}
                        }
                    ]
                }
            }
        }
        plans = plan_remediations(mock_dq, mode="APPLY_APPROVED_FIXES")
        self.assertEqual(len(plans), 1)
        self.assertEqual(plans[0].issue_type, "whitespace")

        # 2. Executor raw preservation & Apply
        df = pd.DataFrame({"name": [" Alice  ", "Bob  "]})
        dfs = {"sales.csv": df}
        
        updated_dfs, executed = execute_remediations(dfs, plans)
        updated_df = updated_dfs["sales.csv"]
        
        self.assertIn("name_raw", updated_df.columns)
        self.assertEqual(updated_df["name"].iloc[0], "Alice")

        # 3. Rollback
        restored = rollback_remediations(updated_dfs, executed)
        restored_df = restored["sales.csv"]
        self.assertNotIn("name_raw", restored_df.columns)
        self.assertEqual(restored_df["name"].iloc[0], " Alice  ")

        # 4. Post Validation Comparison
        original_issues = [{"type": "whitespace", "column": "name"}]
        remediated_issues = []
        ok, stats = run_post_remediation_validation(original_issues, remediated_issues, executed)
        self.assertTrue(ok)
        self.assertIn("whitespace", stats["resolved_issue_types"])
