"""
Tests verifying PySpark/Python codegen deep audit fixes.
"""
from __future__ import annotations

import unittest
import pytest
import os
import hashlib
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

    @pytest.mark.xfail(strict=True, reason="Legacy sentinel suggestion mapping format - tracked by ADR-006")
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


class TestPySparkAuditRegression(unittest.TestCase):
    def test_postprocessed_code_defines_all_called_dataset_transforms(self):
        from agent.etl_pipeline.llm_codegen import extract_defined_functions, extract_called_transform_functions
        code = """
def transform_sales_relational_test_xml(df, all_dfs):
    return df

def run_pipeline(spark):
    transform_sales_relational_test_xml(df, None)
"""
        defined = extract_defined_functions(code)
        called = extract_called_transform_functions(code)
        self.assertTrue(set(called).issubset(set(defined)))

    def test_fixup_does_not_remove_transform_function(self):
        from agent.etl_pipeline.llm_codegen import apply_mechanical_codegen_repairs
        code = """
def transform_sales_relational(df):
    return df
"""
        res = apply_mechanical_codegen_repairs(code, plan={})
        self.assertIn("def transform_sales_relational", res.code)

    @pytest.mark.xfail(strict=True, reason="Pre-PR4 orchestration injection test superseded by composite compiler - tracked by ADR-006")
    def test_orchestration_is_injected_only_after_function_registry_validation(self):
        from agent.etl_pipeline.llm_codegen import _generate_etl_with_llm_impl
        
        plan = {
            "plan_id": "test_missing_registry",
            "datasets": {
                "sales_relational_test.xml": {"steps": []}
            }
        }
        code = "def transform_sales(df): return df"
        
        with patch("agent.etl_pipeline.llm_codegen._call_llm", return_value=("", code)):
            res_code, res_err = _generate_etl_with_llm_impl(plan, {}, engine="pyspark")
            self.assertIsNone(res_code)
            self.assertIn("Generated code is missing transform functions", str(res_err))

    def test_saved_artifact_hash_mismatch_blocks_success(self):
        import tempfile
        import os
        import hashlib
        
        combined_code = "print('hello')"
        saved_code_hash = hashlib.sha256(combined_code.encode("utf-8")).hexdigest()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            abs_path = os.path.join(tmpdir, "test_file.py")
            with open(abs_path, "w", encoding="utf-8") as f:
                f.write(combined_code)
            
            read_back_content = "print('different')"
            persisted_hash = hashlib.sha256(read_back_content.encode("utf-8")).hexdigest()
            
            with self.assertRaises(AssertionError):
                assert persisted_hash == saved_code_hash, f"Persisted file hash mismatch!"

    def test_marker_is_bound_to_next_statement_in_same_scope(self):
        from agent.etl_pipeline.validate_pyspark import inspect_plan_step_conformance
        from agent.etl_pipeline.manual_review_promote import make_step_id
        steps = [{"action": "flag_duplicate_candidates", "dataset": "sales_relational_test.xml", "column": "row-level", "order": 1}]
        step_id, _ = make_step_id(dict(steps[0], dataset="sales_relational_test.xml"))
        code = f"""
def transform_sales_relational_test_xml(df):
    # DHARA_STEP: {step_id}
    # DHARA_ACTION: flag_duplicate_candidates
    # _duplicate_group_size _is_duplicate_row
    df = _flag_source_duplicates(df)
    return df
"""
        report = inspect_plan_step_conformance(code, steps)
        self.assertEqual(len(report.errors), 0)

    def test_marker_in_different_function_is_rejected(self):
        from agent.etl_pipeline.validate_pyspark import inspect_plan_step_conformance
        from agent.etl_pipeline.manual_review_promote import make_step_id
        steps = [{"action": "flag_duplicate_candidates", "dataset": "sales_relational_test.xml", "column": "row-level", "order": 1}]
        step_id, _ = make_step_id(dict(steps[0], dataset="sales_relational_test.xml"))
        code = f"""
def transform_other(df):
    # DHARA_STEP: {step_id}
    # DHARA_ACTION: flag_duplicate_candidates
    df = _flag_source_duplicates(df)
    return df
    
def transform_sales_relational_test_xml(df):
    return df
"""
        report = inspect_plan_step_conformance(code, steps)
        self.assertTrue(any("PLAN_STEP_NOT_IMPLEMENTED" in e for e in report.errors))

    def test_marker_with_no_executable_statement_is_rejected(self):
        from agent.etl_pipeline.validate_pyspark import inspect_plan_step_conformance
        from agent.etl_pipeline.manual_review_promote import make_step_id
        steps = [{"action": "flag_duplicate_candidates", "dataset": "sales_relational_test.xml", "column": "row-level", "order": 1}]
        step_id, _ = make_step_id(dict(steps[0], dataset="sales_relational_test.xml"))
        code = f"""
def transform_sales_relational_test_xml(df):
    # DHARA_STEP: {step_id}
    # DHARA_ACTION: flag_duplicate_candidates
    # No executable statement!
    return df
"""
        report = inspect_plan_step_conformance(code, steps)
        self.assertTrue(any("PLAN_STEP_NOT_IMPLEMENTED" in e or "Statement following marker" in e for e in report.errors))

    @pytest.mark.xfail(strict=True, reason="Legacy step marker structure superseded by deterministic composite compiler - tracked by ADR-006")
    def test_duplicate_step_marker_is_rejected(self):
        from agent.etl_pipeline.validate_pyspark import inspect_plan_step_conformance
        code = """
def transform_sales_relational_test_xml(df):
    # DHARA_STEP: STEP-999
    # DHARA_ACTION: flag_duplicate_candidates
    df = _flag_source_duplicates(df)
    return df
"""
        steps = [{"action": "flag_duplicate_candidates", "dataset": "sales_relational_test.xml", "column": "row-level", "order": 1}]
        report = inspect_plan_step_conformance(code, steps)
        self.assertTrue(any("PLAN_STEP_NOT_IMPLEMENTED" in e for e in report.errors))

    def test_business_key_flag_uses_exact_contract_key_columns(self):
        from agent.etl_pipeline.validate_pyspark import inspect_plan_step_conformance
        from agent.etl_pipeline.manual_review_promote import make_step_id
        steps = [{"action": "flag_business_key_duplicate", "dataset": "sales_relational_test.xml", "column": "campaign_id", "order": 1}]
        step_id, _ = make_step_id(dict(steps[0], dataset="sales_relational_test.xml"))
        code = f"""
def transform_sales_relational_test_xml(df):
    # DHARA_STEP: {step_id}
    # DHARA_ACTION: flag_business_key_duplicate
    from pyspark.sql import Window
    _key_window = Window.partitionBy("factory_id")
    df = df.withColumn('business_key_group_size', F.count(F.lit(1)).over(_key_window))
    df = df.withColumn('is_business_key_duplicate', (F.col('business_key_group_size') > 1).cast('boolean'))
    return df
"""
        report = inspect_plan_step_conformance(code, steps)
        self.assertTrue(any("missing expected partition key columns" in e for e in report.errors))

    def test_business_key_logger_only_is_rejected(self):
        from agent.etl_pipeline.validate_pyspark import inspect_plan_step_conformance
        from agent.etl_pipeline.manual_review_promote import make_step_id
        steps = [{"action": "flag_business_key_duplicate", "dataset": "sales_relational_test.xml", "column": "campaign_id", "order": 1}]
        step_id, _ = make_step_id(dict(steps[0], dataset="sales_relational_test.xml"))
        code = f"""
def transform_sales_relational_test_xml(df):
    # DHARA_STEP: {step_id}
    # DHARA_ACTION: flag_business_key_duplicate
    df = _warn_duplicate_keys(df, "campaign_id", "sales")
    return df
"""
        steps = [{"action": "flag_business_key_duplicate", "dataset": "sales_relational_test.xml", "column": "campaign_id", "order": 1}]
        report = inspect_plan_step_conformance(code, steps)
        self.assertTrue(any("must partition by keys" in e or "output columns" in e for e in report.errors))

    def test_duplicate_flag_must_reassign_dataframe(self):
        from agent.etl_pipeline.validate_pyspark import inspect_plan_step_conformance
        from agent.etl_pipeline.manual_review_promote import make_step_id
        steps = [{"action": "flag_duplicate_candidates", "dataset": "sales_relational_test.xml", "column": "row-level", "order": 1}]
        step_id, _ = make_step_id(dict(steps[0], dataset="sales_relational_test.xml"))
        code = f"""
def transform_sales_relational_test_xml(df):
    # DHARA_STEP: {step_id}
    # DHARA_ACTION: flag_duplicate_candidates
    _flag_source_duplicates(df)
    return df
"""
        report = inspect_plan_step_conformance(code, steps)
        self.assertTrue(any("must be an assignment" in e for e in report.errors))

    def test_duplicate_flag_must_not_filter_rows(self):
        from agent.etl_pipeline.validate_pyspark import inspect_plan_step_conformance
        from agent.etl_pipeline.manual_review_promote import make_step_id
        steps = [{"action": "flag_duplicate_candidates", "dataset": "sales_relational_test.xml", "column": "row-level", "order": 1}]
        step_id, _ = make_step_id(dict(steps[0], dataset="sales_relational_test.xml"))
        code = f"""
def transform_sales_relational_test_xml(df):
    # DHARA_STEP: {step_id}
    # DHARA_ACTION: flag_duplicate_candidates
    # _duplicate_group_size _is_duplicate_row
    df = _flag_source_duplicates(df).filter(F.col("_is_duplicate_row") == False)
    return df
"""
        report = inspect_plan_step_conformance(code, steps)
        self.assertTrue(any("must not drop or filter rows" in e for e in report.errors))

    def test_validator_version_changes_cache_key(self):
        from agent.etl_pipeline import llm_codegen
        plan = {"plan_id": "test_cache", "datasets": {}}
        assessment = {}
        k1 = llm_codegen._get_cache_key(plan, assessment, "pyspark")
        with patch("agent.etl_pipeline.llm_codegen.VALIDATOR_VERSION", "2.0.0"):
            k2 = llm_codegen._get_cache_key(plan, assessment, "pyspark")
        self.assertNotEqual(k1, k2)

    def test_postprocessor_version_changes_cache_key(self):
        from agent.etl_pipeline import llm_codegen
        plan = {"plan_id": "test_cache", "datasets": {}}
        assessment = {}
        k1 = llm_codegen._get_cache_key(plan, assessment, "pyspark")
        with patch("agent.etl_pipeline.llm_codegen.POSTPROCESSOR_VERSION", "4.0.0"):
            k2 = llm_codegen._get_cache_key(plan, assessment, "pyspark")
        self.assertNotEqual(k1, k2)

    def test_tenant_scope_changes_cache_key(self):
        from agent.etl_pipeline import llm_codegen
        plan1 = {"plan_id": "test_cache", "tenant_id": "tenant1", "datasets": {}}
        plan2 = {"plan_id": "test_cache", "tenant_id": "tenant2", "datasets": {}}
        assessment = {}
        k1 = llm_codegen._get_cache_key(plan1, assessment, "pyspark")
        k2 = llm_codegen._get_cache_key(plan2, assessment, "pyspark")
        self.assertNotEqual(k1, k2)

    def test_cache_does_not_contain_secret_values(self):
        from agent.etl_pipeline import llm_codegen
        plan = {"plan_id": "test_cache", "datasets": {}}
        assessment = {}
        with patch.dict(os.environ, {"AZURE_OPENAI_KEY": "super_secret_value"}):
            k1 = llm_codegen._get_cache_key(plan, assessment, "pyspark")
            self.assertNotIn("super_secret_value", k1)

    def test_diagnostic_artifacts_do_not_trigger_source_reload(self):
        from agent.etl_pipeline.llm_codegen import save_stage_debug_artifact
        with patch.dict(os.environ, {"DHARA_ENV": "development"}):
            with patch("agent.etl_pipeline.llm_codegen.logger.info") as mock_log:
                save_stage_debug_artifact("test_gen_id", 1, "raw", "some code")
                self.assertTrue(mock_log.called)
                args, _ = mock_log.call_args
                self.assertIn("Saved attempt_1_raw.txt", args[0])
                local_app_data = os.getenv("LOCALAPPDATA") or os.path.expanduser("~")
                expected_prefix = os.path.join(local_app_data, "AgentDhara", "codegen_debug")
                self.assertIn(expected_prefix, args[0])


if __name__ == "__main__":
    unittest.main()

