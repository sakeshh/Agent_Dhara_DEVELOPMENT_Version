import unittest
import pandas as pd
import sqlite3
import os
import json
from agent.auto_cross_field_rules import generate_auto_cross_field_rules
from agent.profiling.assessment_orchestrator import _same_dataset_representation
from agent.session_store import _connect, save_pipeline_run, get_pipeline_runs_for_datasets

class TestAuditFixes(unittest.TestCase):
    def test_same_dataset_representation_optimized(self):
        # Create identical dataframes to test same representation
        df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        df2 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        
        # Should return True since schemas match, high overlap, and values match
        self.assertTrue(_same_dataset_representation(df1, df2))

        # Different schemas or id missing
        df_no_id = pd.DataFrame({"key": [1, 2, 3]})
        self.assertFalse(_same_dataset_representation(df1, df_no_id))

        # Low overlap
        df_low_overlap = pd.DataFrame({"id": [10, 20, 30], "value": ["a", "b", "c"]})
        self.assertFalse(_same_dataset_representation(df1, df_low_overlap))

    def test_auto_cross_field_rules_no_duplicates(self):
        # Assessment containing two date columns
        assessment = {
            "datasets": {
                "sales": {
                    "columns": {
                        "start_date": {"semantic_type": "date"},
                        "end_date": {"semantic_type": "date"}
                    }
                }
            }
        }
        rules = generate_auto_cross_field_rules(assessment)
        
        # Verify it generates a single rule (c1, c2) or (c2, c1), but not duplicate pairs
        date_rules = [r for r in rules if r["type"] == "date_order"]
        self.assertEqual(len(date_rules), 1)

    def test_session_store_thread_local_and_sql_filter(self):
        # Verify ConnectionProxy behaves correctly
        conn = _connect()
        self.assertTrue(hasattr(conn, "cursor"))
        
        # Run a test session store pipeline run write & retrieve
        session_id = "test_audit_session_unique_123"
        run_id = save_pipeline_run(
            session_id=session_id,
            dataset_names=["orders_dataset", "customers_dataset"],
            schema_hash="abc",
            dq_score=95,
            dq_issue_count=0
        )
        self.assertTrue(run_id > 0)
        
        # Retrieve using SQL-side filter
        runs = get_pipeline_runs_for_datasets(["orders_dataset"], limit=5)
        self.assertTrue(len(runs) >= 1)
        # Find our specific run
        matched_run = next((r for r in runs if r["session_id"] == session_id), None)
        self.assertIsNotNone(matched_run)
        self.assertIn("orders_dataset", matched_run["dataset_names"])

        # No match should return empty list
        no_runs = get_pipeline_runs_for_datasets(["non_existent_dataset"])
        # Our specific run shouldn't be matched
        matched_bad = next((r for r in no_runs if r["session_id"] == session_id), None)
        self.assertIsNone(matched_bad)

    def test_is_sensitive_column_false_positives(self):
        from agent.pii_masking import is_sensitive_column
        # Name matches heuristic (card), but sample values do not match credit card format (16 digits)
        self.assertFalse(is_sensitive_column("card_type", ["Visa", "MasterCard", "Amex"]))
        
        # Name matches heuristic (pan), but sample values do not match Indian PAN format
        self.assertFalse(is_sensitive_column("span", ["some_value", "another_value"]))
        
        # Real sensitive column should return True
        self.assertTrue(is_sensitive_column("credit_card", ["1234-5678-1234-5678", "4321 8765 4321 8765"]))
        
        # When sample values is None, fallback to name check but exclude obvious name false positives
        self.assertFalse(is_sensitive_column("card_type", None))
        self.assertFalse(is_sensitive_column("span", None))

    def test_plan_coverage_report_generation(self):
        from agent.etl_pipeline.plan_coverage_report import build_coverage_report
        assessment = {
            "datasets": {
                "customers": {
                    "quality": {
                        "issues": [
                            {"column": "email", "type": "invalid_email", "severity": "high", "message": "Bad email"},
                            {"column": "phone", "type": "invalid_phone", "severity": "low", "message": "Bad phone"}
                        ]
                    }
                }
            }
        }
        plan = {
            "datasets": {
                "customers": {
                    "steps": [
                        {"column": "email", "action": "sanitize_email"}
                    ]
                }
            }
        }
        report = build_coverage_report(assessment, plan)
        self.assertEqual(report["coverage_pct"], 50.0)
        self.assertEqual(len(report["covered"]), 1)
        self.assertEqual(report["covered"][0]["column"], "email")
        self.assertEqual(len(report["uncovered"]), 1)
        self.assertEqual(report["uncovered"][0]["column"], "phone")

    def test_claim_next_job_concurrency(self):
        import threading
        import time
        from agent.jobs_store import create_job, claim_next_job
        
        # Create a job to claim
        job_id = create_job(kind="etl_execute", input={"session_id": "test_concurrency"})
        
        results = []
        def worker():
            job = claim_next_job(kinds=["etl_execute"])
            if job:
                results.append(job["job_id"])
                
        # Start multiple threads trying to claim the same job
        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
            
        # Only one thread should have successfully claimed the job
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], job_id)

    def test_save_session_payload_size_guard(self):
        from agent.session_store import save_session, load_session
        session_id = "test_payload_size_guard_session"
        # Create a large dummy dict > 5MB
        large_data = {"data": "x" * (6 * 1024 * 1024), "datasets": {"orders": {}}}
        payload = {
            "session_id": session_id,
            "context": {"last_assessment_result": large_data}
        }
        save_session(session_id, payload)
        loaded = load_session(session_id)
        ctx = loaded.get("context", {})
        assess = ctx.get("last_assessment_result", {})
        self.assertTrue(assess.get("_trimmed"))
        self.assertIn("orders", assess.get("datasets", []))

    def test_etl_execute_rate_limiter_wired(self):
        from agent.api_routes import api_etl_execute, EtlExecutePayload, HTTPException
        from unittest.mock import MagicMock
        req = MagicMock()
        req.client.host = "192.168.1.99"
        req.headers = {}
        payload = EtlExecutePayload(session_id="test_rate_limiter")
        
        # 5 calls should succeed
        for _ in range(5):
            res = api_etl_execute(payload=payload, request=req, _auth=None)
            self.assertTrue(res.get("ok"))
            
        # 6th call should raise HTTPException 429
        with self.assertRaises(HTTPException) as cm:
            api_etl_execute(payload=payload, request=req, _auth=None)
        self.assertEqual(cm.exception.status_code, 429)
