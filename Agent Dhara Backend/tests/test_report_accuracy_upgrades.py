import unittest
import pandas as pd
import json
import tempfile
import os
from unittest.mock import MagicMock, patch

from agent.intelligent_data_assessment import (
    profile_database_table_full,
    merge_in_db_profile,
    load_csv_sampled,
    _load_json_to_df,
    analyze_dataset_quality,
    check_custom_assertions
)
from agent.etl_pipeline.manual_review_catalog import (
    get_dynamic_resolution_options,
    action_for_resolution
)
from agent.model_config import LLMConfig

class TestReportAccuracyUpgrades(unittest.TestCase):
    
    def test_profile_database_table_full_success(self):
        # Create a mock connector
        mock_connector = MagicMock()
        mock_connector.get_table_schema.return_value = [
            {"name": "id", "type": "int", "nullable": "NO"},
            {"name": "name", "type": "varchar", "nullable": "YES"},
            {"name": "is_active", "type": "bit", "nullable": "NO"},
            {"name": "ignored_blob", "type": "image", "nullable": "YES"} # should be excluded
        ]
        mock_connector._quote_two_part_name.return_value = "[dbo].[test_table]"
        
        # The execute_select method will return a single-row DataFrame
        df_result = pd.DataFrame([{
            "__total_rows__": 100,
            "id__null_cnt": 0,
            "id__distinct_cnt": 100,
            "id__min_val": 1,
            "id__max_val": 100,
            "name__null_cnt": 10,
            "name__distinct_cnt": 90,
            "name__min_val": "Alice",
            "name__max_val": "Zach",
            "is_active__null_cnt": 0,
            "is_active__distinct_cnt": 2,
            "is_active__min_val": 0,
            "is_active__max_val": 1
        }])
        mock_connector.execute_select.return_value = df_result
        
        df_sample = pd.DataFrame(columns=["id", "name", "is_active", "ignored_blob"])
        db_prof = profile_database_table_full(mock_connector, "dbo.test_table", df_sample)
        
        self.assertEqual(db_prof["row_count"], 100)
        self.assertIn("id", db_prof["columns"])
        self.assertIn("name", db_prof["columns"])
        self.assertIn("is_active", db_prof["columns"])
        self.assertNotIn("ignored_blob", db_prof["columns"]) # Excluded unsafe type
        
        # Check assertions
        self.assertEqual(db_prof["columns"]["id"]["null_count"], 0)
        self.assertEqual(db_prof["columns"]["id"]["unique_count"], 100)
        self.assertEqual(db_prof["columns"]["id"]["min"], 1)
        self.assertEqual(db_prof["columns"]["id"]["max"], 100)
        self.assertTrue(db_prof["columns"]["id"]["candidate_primary_key"])
        
        self.assertEqual(db_prof["columns"]["name"]["null_count"], 10)
        self.assertEqual(db_prof["columns"]["name"]["null_percentage"], 0.10)
        self.assertEqual(db_prof["columns"]["name"]["unique_count"], 90)
        self.assertFalse(db_prof["columns"]["name"]["candidate_primary_key"])

    def test_merge_in_db_profile(self):
        sample_profile = {
            "row_count": 10,
            "columns": {
                "id": {
                    "null_count": 0,
                    "null_percentage": 0.0,
                    "unique_count": 10,
                    "min": 1,
                    "max": 10,
                    "candidate_primary_key": True
                },
                "name": {
                    "null_count": 0,
                    "null_percentage": 0.0,
                    "unique_count": 10,
                    "min": "A",
                    "max": "J",
                    "candidate_primary_key": True
                }
            }
        }
        
        db_profile = {
            "row_count": 1000,
            "columns": {
                "id": {
                    "null_count": 0,
                    "null_percentage": 0.0,
                    "unique_count": 1000,
                    "min": 1,
                    "max": 1000,
                    "candidate_primary_key": True
                },
                "name": {
                    "null_count": 100,
                    "null_percentage": 0.1,
                    "unique_count": 900,
                    "min": "A",
                    "max": "Z",
                    "candidate_primary_key": False
                }
            }
        }
        
        merged = merge_in_db_profile(sample_profile, db_profile)
        self.assertEqual(merged["row_count"], 1000)
        self.assertIn("Full dataset has 1,000 rows.", merged["sampling_info"])
        self.assertEqual(merged["columns"]["name"]["null_count"], 100)
        self.assertEqual(merged["columns"]["name"]["null_percentage"], 0.1)
        self.assertEqual(merged["columns"]["name"]["unique_count"], 900)
        self.assertEqual(merged["columns"]["name"]["min"], "A")
        self.assertEqual(merged["columns"]["name"]["max"], "Z")
        self.assertFalse(merged["columns"]["name"]["candidate_primary_key"])

    def test_load_csv_sampled(self):
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", newline="") as tmp:
            tmp.write("id,val\n")
            for i in range(100):
                tmp.write(f"{i},value_{i}\n")
            tmp_path = tmp.name
            
        try:
            # Under max_rows, returns full
            df_full = load_csv_sampled(tmp_path, max_rows=150)
            self.assertEqual(len(df_full), 100)
            
            # Over max_rows, returns sampled
            df_sampled = load_csv_sampled(tmp_path, max_rows=20)
            self.assertEqual(len(df_sampled), 20)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_load_jsonl_sampled(self):
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".jsonl", newline="") as tmp:
            for i in range(100):
                tmp.write(json.dumps({"id": i, "val": f"value_{i}"}) + "\n")
            tmp_path = tmp.name
            
        try:
            # Under max_rows or max_rows is None
            df_full = _load_json_to_df(tmp_path, max_rows=None)
            self.assertEqual(len(df_full), 100)
            
            # Sampled using Reservoir Sampling
            df_sampled = _load_json_to_df(tmp_path, max_rows=20)
            self.assertEqual(len(df_sampled), 20)
            # Ensure it is a DataFrame with correct structure
            self.assertIn("id", df_sampled.columns)
            self.assertIn("val", df_sampled.columns)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_valid_values_lookup_check(self):
        df = pd.DataFrame({
            "dept": ["IT", "HR", "CS", "invalid_dept", "IT", "HR"]
        })
        profile = {
            "columns": {
                "dept": {"semantic_type": "categorical"}
            }
        }
        business_rules = {
            "valid_values": {
                "dbo.employees.dept": ["IT", "HR", "CS"]
            }
        }
        
        # Test validation triggers invalid_lookup_value
        res = analyze_dataset_quality(
            "dbo.employees",
            df,
            profile,
            business_rules=business_rules
        )
        
        issues = [it for it in res.get("issues", []) if it.get("type") == "invalid_lookup_value"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["column"], "dept")
        self.assertEqual(issues[0]["count"], 1)
        self.assertEqual(issues[0]["sample_values"], ["invalid_dept"])

    def test_custom_cross_column_assertions(self):
        df = pd.DataFrame({
            "age": [25, 17, 30, 16],
            "is_adult": [True, False, False, False] # row 3 is an adult (30) but is_adult is False!
        })
        profile = {
            "columns": {
                "age": {"semantic_type": "numeric"},
                "is_adult": {"semantic_type": "boolean"}
            }
        }
        business_rules = {
            "custom_assertions": [
                {
                    "assertion": "age < 18 or is_adult == True",
                    "severity": "high",
                    "message": "is_adult must be True if age >= 18"
                }
            ]
        }
        
        res = analyze_dataset_quality(
            "dbo.members",
            df,
            profile,
            business_rules=business_rules
        )
        
        issues = [it for it in res.get("issues", []) if it.get("type") == "custom_rule_violation"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["count"], 1)
        self.assertIn("age", issues[0]["column"])
        self.assertIn("is_adult", issues[0]["column"])

    @patch("agent.model_config.load_llm_config")
    @patch("openai.OpenAI")
    def test_dynamic_resolution_options_llm(self, mock_openai_cls, mock_load_config):
        # Configure LLM config mock
        mock_load_config.return_value = LLMConfig(
            provider="openai",
            api_key="mock-key",
            model="gpt-4o-mini"
        )
        
        # Configure OpenAI Client Mock
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client
        
        mock_choice = MagicMock()
        mock_choice.message.content = json.dumps({
            "options": [
                {
                    "id": "opt_lowercase",
                    "label": "Convert to lowercase text",
                    "action": "lowercase",
                    "recommended": True,
                    "description": "Standardize string case to lowercase."
                }
            ]
        })
        mock_client.chat.completions.create.return_value.choices = [mock_choice]
        
        item = {
            "dataset": "dbo.courses",
            "column": "course_name",
            "issue_type": "weird_casing_issue",
            "message": "Casing is weird"
        }
        
        opts = get_dynamic_resolution_options("weird_casing_issue", item)
        
        # There should be keep_as_is + the one returned by LLM
        self.assertEqual(len(opts), 2)
        
        keep_as_is_opt = [o for o in opts if o.get("id") == "keep_as_is"]
        self.assertEqual(len(keep_as_is_opt), 1)
        
        custom_opt = [o for o in opts if o.get("id") == "opt_lowercase"]
        self.assertEqual(len(custom_opt), 1)
        self.assertEqual(custom_opt[0]["action"], "lowercase")
        self.assertTrue(custom_opt[0]["recommended"])
        
        # Verify action_for_resolution maps correctly
        action = action_for_resolution("weird_casing_issue", "opt_lowercase", opts)
        self.assertEqual(action, "lowercase")

    def test_html_report_rendering_enhancements(self):
        from main import build_html_report
        
        sample_result = {
            "datasets": {
                "test_dataset.csv": {
                    "row_count": 100,
                    "column_count": 2,
                    "source_root": "azure_blob:",
                    "columns": {
                        "id": {"dtype": "int", "null_percentage": 0.0, "unique_count": 100},
                        "name": {"dtype": "str", "null_percentage": 0.05, "unique_count": 95}
                    }
                }
            },
            "relationships": [],
            "data_quality_issues": {
                "datasets": {
                    "test_dataset.csv": {
                        "summary": {
                            "issue_count": 1,
                            "high_severity": 0,
                            "medium_severity": 0,
                            "low_severity": 1
                        },
                        "issues": [
                            {
                                "severity": "low",
                                "type": "whitespace",
                                "column": "name",
                                "count": 5,
                                "message": "5 leading/trailing spaces"
                            }
                        ]
                    }
                },
                "global_issues": {
                    "cross_dataset_consistency": [
                        {
                            "severity": "high",
                            "type": "id_type_drift_across_datasets",
                            "message": "ID column uses inconsistent scalar types across datasets (serialization/type drift).",
                            "recommendation": "Align ID types to match across all sources."
                        }
                    ]
                }
            }
        }
        
        html_report = build_html_report(sample_result)
        
        # Verify that since there is a low severity issue (n_low = 1, n_high = 0),
        # dq_details_open_attr is " open", so "<details class=\"dq-issue-details\" open>" is present
        self.assertIn("class=\"dq-issue-details\" open", html_report)
        
        # Verify the expanded javascript details logic is present
        self.assertIn("details.dq-issue-details", html_report)

        # Verify that cross-dataset consistency is rendered in the HTML report
        self.assertIn("Cross-dataset consistency insights", html_report)
        self.assertIn("id_type_drift_across_datasets", html_report)
        self.assertIn("Align ID types to match across all sources.", html_report)

if __name__ == "__main__":
    unittest.main()

