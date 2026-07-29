"""
Tests verifying PySpark/Python codegen deep audit fixes.
"""
from __future__ import annotations

import unittest
from unittest.mock import patch
from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
from agent.etl_pipeline.python_codegen import generate_python_etl
from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
from agent.etl_pipeline.plan_coverage_report import build_coverage_report
from agent.etl_pipeline.issue_to_step_compiler import compile_issues_to_steps
from agent.transformation_suggester import suggest_transformations

class TestPySparkDeepAuditFixes(unittest.TestCase):
    def setUp(self):
        self.assessment = {
            "datasets": {
                "customers": {
                    "quality": {
                        "issues": [
                            {"type": "sentinel_numeric_value", "column": "Age", "count": 10, "unexpected_values": [-999.0, 9999.0]},
                            {"type": "invalid_email", "column": "Email", "count": 5},
                        ]
                    }
                }
            },
            "data_quality_issues": {
                "datasets": {
                    "customers": {
                        "issues": [
                            {"type": "sentinel_numeric_value", "column": "Age", "count": 10, "unexpected_values": [-999.0, 9999.0]},
                            {"type": "invalid_email", "column": "Email", "count": 5},
                        ]
                    }
                }
            }
        }
        self.rules = {
            "never_drop_rows": True,
            "watermark_column": "updated_at",
        }
        self.sem_schema = {}

    def test_sentinel_suggestion_mapping(self):
        # Verify sentinel numeric value translates to replace_sentinel_values instead of zero_to_null
        suggestions = suggest_transformations(self.assessment)["suggested_transformations"]
        sentinel_sug = [s for s in suggestions if s["column"] == "Age" and s["issue_type"] == "sentinel_numeric_value"]
        self.assertTrue(len(sentinel_sug) > 0)
        self.assertEqual(sentinel_sug[0]["suggested_action"], "replace_sentinel_values")
        self.assertIn("sentinel_values", sentinel_sug[0]["params"])
        self.assertCountEqual(sentinel_sug[0]["params"]["sentinel_values"], [-999.0, 9999.0])

        # Compile compiled steps
        steps_map, manual_review = compile_issues_to_steps(suggestions, self.rules, self.sem_schema)
        steps = steps_map["customers"]
        sentinel_step = [s for s in steps if s["column"] == "Age" and s["action"] == "replace_sentinel_values"]
        self.assertTrue(len(sentinel_step) > 0)
        self.assertCountEqual(sentinel_step[0]["params"]["sentinel_values"], [-999.0, 9999.0])

    def test_coverage_resolving_actions(self):
        # Verify that plan coverage report checks resolving actions, not just column name
        plan_partial = {
            "datasets": {
                "customers": {
                    "steps": [
                        {"column": "Email", "action": "trim", "order": 1} # trim does not resolve invalid_email
                    ]
                }
            }
        }
        report = build_coverage_report(self.assessment, plan_partial)
        # Email issue should be uncovered because trim doesn't resolve invalid_email
        uncovered_types = [it["issue_type"] for it in report["uncovered"]]
        self.assertIn("invalid_email", uncovered_types)

        plan_resolved = {
            "datasets": {
                "customers": {
                    "steps": [
                        {"column": "Email", "action": "sanitize_email", "order": 1}
                    ]
                }
            }
        }
        report2 = build_coverage_report(self.assessment, plan_resolved)
        uncovered_types2 = [it["issue_type"] for it in report2["uncovered"]]
        self.assertNotIn("invalid_email", uncovered_types2)

    def test_pyspark_codegen_output(self):
        # Test business-key dedup and regex sanitize_email and sentinel replace
        plan = {
            "plan_id": "test_plan",
            "business_keys": {"customers": ["id"]},
            "business_rules": self.rules,
            "datasets": {
                "customers": {
                    "steps": [
                        {"column": "Email", "action": "sanitize_email", "order": 1},
                        {"column": "Age", "action": "replace_sentinel_values", "order": 2, "params": {"sentinel_values": [-999.0, 9999.0]}},
                        {"column": "row-level", "action": "deduplicate", "order": 3}
                    ]
                }
            },
            "connector_manifest": {
                "datasets": {
                    "customers": {"location": "Files/raw/customers.csv", "format": "csv"}
                }
            }
        }
        code = generate_pyspark_etl(plan, self.assessment)
        
        # Verify strict email regex check
        self.assertIn("rlike(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')", code)
        self.assertNotIn("contains('@')", code)
        
        # Verify Window key-aware dedup
        self.assertIn("Window.partitionBy('id').orderBy(F.col('updated_at').desc())", code)
        
        # Verify sentinel replacement
        self.assertIn(".isin([", code)
        self.assertIn("-999", code)
        self.assertIn("9999", code)

        # Linter validation passes
        ok, errs = validate_pyspark_source(code, plan)
        self.assertTrue(ok, errs)

    def test_python_codegen_output(self):
        plan = {
            "plan_id": "test_plan",
            "business_keys": {"customers": ["id"]},
            "business_rules": self.rules,
            "datasets": {
                "customers": {
                    "steps": [
                        {"column": "Email", "action": "sanitize_email", "order": 1},
                        {"column": "Age", "action": "replace_sentinel_values", "order": 2, "params": {"sentinel_values": [-999.0, 9999.0]}},
                        {"column": "row-level", "action": "deduplicate", "order": 3}
                    ]
                }
            },
            "connector_manifest": {
                "datasets": {
                    "customers": {"location": "Files/raw/customers.csv", "format": "csv"}
                }
            }
        }
        code = generate_python_etl(plan, self.assessment)
        
        # Verify pandas strict match regex
        self.assertIn("str.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$', na=False)", code)
        
        # Verify watermark sort + drop_duplicates
        self.assertIn("sort_values(by='updated_at', ascending=False)", code)
        self.assertIn("drop_duplicates(subset=['id']", code)
        
        # Verify replace logic
        self.assertIn(".replace([", code)
        self.assertIn("-999", code)
        self.assertIn("9999", code)

    def test_linter_flags_naive_contains(self):
        source = """
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging

DATASETS = ["customers"]

def transform_customers(df: DataFrame) -> DataFrame:
    # sanitize_email naive check
    return df.withColumn("Email", F.when(F.col("Email").contains("@"), F.col("Email")).otherwise(None))
"""
        ok, errs = validate_pyspark_source(source)
        self.assertFalse(ok)
        self.assertTrue(any("naive contains('@')" in e for e in errs), errs)

    def test_replace_sentinel_values_validation_marker(self):
        # Validate that the python linter accepts replace_sentinel_values
        from agent.etl_pipeline.validate_python import validate_etl_python_source
        plan = {
            "plan_id": "test_plan",
            "datasets": {
                "customers": {
                    "steps": [
                        {"column": "Age", "action": "replace_sentinel_values", "order": 1}
                    ]
                }
            }
        }
        # Code containing .replace(
        source_code = "out['Age'] = out['Age'].replace([-999.0], pd.NA)"
        ok, errs = validate_etl_python_source(source_code, plan)
        self.assertTrue(ok, errs)

    @patch("agent.etl_handlers._generate_for_engine")
    def test_in_place_plan_step_filtering(self, mock_generate):
        mock_generate.return_value = ("# mock code", True, [], "mock")
        from agent.session_store import save_session, load_session
        from agent.etl_handlers import _ctx
        import uuid
        sid = f"test-session-{uuid.uuid4()}"
        
        sess = load_session(sid)
        ctx = _ctx(sess)
        
        # flag_outliers on string 'name' should be filtered out
        plan = {
            "plan_id": "test_filter_plan",
            "datasets": {
                "customers": {
                    "steps": [
                        {"column": "name", "action": "flag_outliers", "order": 1}
                    ]
                }
            },
            "business_rules": {}
        }
        assessment = {
            "datasets": {
                "customers": {
                    "columns": {
                        "name": {"dtype": "object"}
                    }
                }
            }
        }
        
        ctx["etl_flow"] = {
            "phase": "approved",
            "plan": plan
        }
        ctx["last_assessment_result"] = assessment
        save_session(sess)
        
        from agent.etl_pipeline.planner import build_etl_plan
        filtered_plan = build_etl_plan(plan, assessment)
        steps = filtered_plan.get("datasets", {}).get("customers", {}).get("steps", [])
        # In Phase 1 PlanSpec contract: flag_outliers on string column 'name' is filtered out during build_etl_plan
        self.assertEqual(len(steps), 0)


if __name__ == "__main__":
    unittest.main()

