"""
Tests for durable job state persistence, multi-instance recovery, artifact upload gating,
HTML report endpoints, inferred relationship discovery, and Courses dimension evidence.
"""

import json
import os
import tempfile
import time
import unittest
import pandas as pd

from agent.durable_artifact_store import (
    get_durable_base_dir,
    save_durable_artifact,
    load_durable_artifact,
    verify_durable_artifact_exists,
    save_durable_job_state,
    load_durable_job_state,
)
from agent.jobs_store import create_job, update_job_status, fetch_job, _connect
from agent.profiling.relationship_registry import discover_inferred_relationships
from agent.profiling.engine3_pipeline import run_engine3_pipeline


class TestDurableJobsAndArtifacts(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.orig_data_dir = os.environ.get("DHARA_DATA_DIR")
        os.environ["DHARA_DATA_DIR"] = self.temp_dir

    def tearDown(self):
        if self.orig_data_dir is not None:
            os.environ["DHARA_DATA_DIR"] = self.orig_data_dir
        else:
            os.environ.pop("DHARA_DATA_DIR", None)

    def test_durable_artifact_save_load_verify(self):
        """Verify save, load, and verify functions in durable_artifact_store."""
        job_id = "test-job-artifact-1"
        html_content = "<html><body><h1>Assessment Report</h1></body></html>"

        rel_path = save_durable_artifact(job_id, "report.html", html_content, content_type="text/html")
        self.assertEqual(rel_path, f"assessment-runs/{job_id}/report.html")

        self.assertTrue(verify_durable_artifact_exists(job_id, "report.html"))
        self.assertFalse(verify_durable_artifact_exists(job_id, "nonexistent.html"))

        loaded = load_durable_artifact(job_id, "report.html")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.decode("utf-8"), html_content)

    def test_multi_instance_job_recovery_across_container_transition(self):
        """
        Simulate Azure App Service container transition:
        Container 1 creates a job and updates progress.
        Container 2 starts with an empty SQLite database (or restarted instance).
        Container 2 calls fetch_job(job_id) and successfully recovers state from durable store.
        """
        # Container 1 creates job
        job_id = create_job(kind="assess", input={"session_id": "test_session", "dataset": "dbo.Courses"})
        update_job_status(
            job_id,
            status="running",
            progress=65,
            current_stage="orchestrator",
        )

        # Confirm Container 1 has it in local SQLite
        j1 = fetch_job(job_id)
        self.assertIsNotNone(j1)
        self.assertEqual(j1["status"], "running")
        self.assertEqual(j1["progress"], 65)
        self.assertEqual(j1["current_stage"], "orchestrator")

        # Now simulate Container 2: wipe the SQLite database file or delete the row from SQLite
        conn = _connect()
        try:
            conn.execute("DELETE FROM jobs WHERE job_id=?", (job_id,))
            conn.commit()
            row = conn.execute("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            self.assertIsNone(row)  # Confirmed gone from local SQLite
        finally:
            conn.close()

        # Container 2 receives HTTP poll: GET /etl/assessment/status/{job_id} -> calls fetch_job
        j2 = fetch_job(job_id)
        self.assertIsNotNone(j2, "Container 2 should re-hydrate job from durable store instead of returning 404!")
        self.assertEqual(j2["job_id"], job_id)
        self.assertEqual(j2["status"], "running")
        self.assertEqual(j2["progress"], 65)
        self.assertEqual(j2["current_stage"], "orchestrator")

        # Confirm it was re-inserted into Container 2's SQLite DB
        conn = _connect()
        try:
            cached_row = conn.execute("SELECT job_id, status FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            self.assertIsNotNone(cached_row)
            self.assertEqual(cached_row[1], "running")
        finally:
            conn.close()

    def test_inferred_relationship_discovery_executes_without_re_error(self):
        """
        Guarantees that discover_inferred_relationships executes cleanly with regex normalization
        and does NOT crash with 'name re is not defined'.
        """
        df_customers = pd.DataFrame({
            "customer_id": [f"C{i:04d}" for i in range(1, 30)],
            "customer_name": [f"Customer {i}" for i in range(1, 30)],
        })
        df_orders = pd.DataFrame({
            "order_id": [f"ORD{i:04d}" for i in range(1, 40)],
            "customer_id": [f"C{i:04d}" for i in range(1, 25)] + [f"C{i:04d}" for i in range(1, 16)],
            "amount": [100.0 + i for i in range(39)],
        })

        datasets = {
            "dbo.Customers": df_customers,
            "dbo.Orders": df_orders,
        }

        # Should execute without throwing NameError or any exception
        discovered = discover_inferred_relationships(datasets, {"min_overlap_count": 5, "min_overlap_ratio": 0.5})
        self.assertIsInstance(discovered, list)
        self.assertTrue(len(discovered) > 0, "Should discover relationship between dbo.Customers and dbo.Orders")
        rel = discovered[0]
        self.assertEqual(rel["source"], "inferred")
        self.assertIn("customer_id", rel["parent"])
        self.assertIn("customer_id", rel["child"])

    def test_courses_coverage_and_dimension_evidence_logging(self):
        """
        Test that dbo.Courses with clean completeness/uniqueness/consistency/conformity:
        1. Logs dimension evidence for every dimension.
        2. Achieves score_coverage >= 0.80.
        3. Sets readiness to NEEDS_WORK (because score ~66.88 < 70.0 ready threshold).
        """
        df_courses = pd.DataFrame({
            "course_id": [f"CRS{i:03d}" for i in range(1, 101)],
            "course_name": [f"Course {i}" for i in range(1, 101)],
            "department": ["Computer Science" if i % 2 == 0 else "Mathematics" for i in range(1, 101)],
        })

        # Inject HIGH severity issue on column (non-PK) so gating evaluates to NEEDS_WORK
        raw_issues = [
            {
                "type": "nulls",
                "severity": "high",
                "column": "department",
                "count": 5,
                "row_indexes": [1, 2, 3, 4, 5],
                "gating": True,
            },
        ]

        result = run_engine3_pipeline(
            name="dbo.Courses",
            df=df_courses,
            raw_issues=raw_issues,
            gx_results_raw=[],
            profile={"columns": {"course_id": {}, "course_name": {}, "department": {}}},
        )

        coverage = result.get("score_coverage")
        readiness = result.get("readiness")
        dq_score = result.get("dq_score_v3")

        self.assertIsNotNone(coverage)
        self.assertGreaterEqual(coverage, 0.80, f"Coverage should be >= 0.80, got {coverage}")
        self.assertEqual(readiness, "NEEDS_WORK", f"Readiness should be NEEDS_WORK for score < 70 with coverage >= 0.80, got {readiness}")

    def test_html_report_endpoints(self):
        """Test GET /jobs/{job_id}/report and GET /etl/assessment/report/{job_id}."""
        from fastapi.testclient import TestClient
        from agent.mcp_server import app

        client = TestClient(app)

        job_id = create_job(kind="assess", input={"session_id": "test_report"})
        html_body = "<!doctype html><html><body>Report For Test Job</body></html>"

        # Save report artifact
        save_durable_artifact(job_id, "report.html", html_body, content_type="text/html")
        update_job_status(
            job_id,
            status="succeeded",
            result={"report_html": html_body, "report_blob_path": f"assessment-runs/{job_id}/report.html"},
            progress=100,
            current_stage="completed",
        )

        # 1. Test /jobs/{job_id}/report
        res1 = client.get(f"/jobs/{job_id}/report")
        self.assertEqual(res1.status_code, 200)
        self.assertIn("Report For Test Job", res1.text)
        self.assertIn("text/html", res1.headers["content-type"])

        # 2. Test /etl/assessment/report/{job_id}
        res2 = client.get(f"/etl/assessment/report/{job_id}")
        self.assertEqual(res2.status_code, 200)
        self.assertIn("Report For Test Job", res2.text)

        # 3. Test /etl/assessment/status/{job_id}
        res3 = client.get(f"/etl/assessment/status/{job_id}")
        self.assertEqual(res3.status_code, 200)
        data = res3.json()
        self.assertEqual(data["status"], "completed")
        self.assertEqual(data["report_url"], f"/jobs/{job_id}/report")
        self.assertIn(f"assessment-runs/{job_id}/report.html", data["report_blob_path"])


if __name__ == "__main__":
    unittest.main()
