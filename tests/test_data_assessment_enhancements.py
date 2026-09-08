import unittest
import pytest
import pandas as pd
from agent.intelligent_data_assessment import (
    analyze_column,
    analyze_dataset_quality,
    detect_semantic_type
)

class TestDataAssessmentEnhancements(unittest.TestCase):
    def test_custom_placeholders_and_sentinels(self):
        # Create a series with custom placeholder 'custom_null_placeholder' and custom sentinel -9999
        s = pd.Series(["valid", "custom_null_placeholder", "another_valid", "-9999", None])
        
        # Test 1: With default thresholds (should not recognize 'custom_null_placeholder' as null, nor -9999 as sentinel)
        issues_default = analyze_column(s, col="val", semantic="numeric_id", thresholds={})
        null_issues = [it for it in issues_default if it.get("type") == "nulls"]
        sentinel_issues = [it for it in issues_default if it.get("type") == "sentinel_numeric_value"]
        
        # Test 2: With custom thresholds loaded
        custom_thresholds = {
            "placeholders": ["custom_null_placeholder"],
            "sentinels": [-9999]
        }
        issues_custom = analyze_column(s, col="val", semantic="numeric_id", thresholds=custom_thresholds)
        
        # 'custom_null_placeholder' should now be counted as null/placeholder
        # And -9999 should be detected as sentinel numeric value
        null_issue_custom = [it for it in issues_custom if it.get("type") == "nulls"]
        sentinel_issue_custom = [it for it in issues_custom if it.get("type") == "sentinel_numeric_value"]
        
        self.assertTrue(len(null_issue_custom) > 0)
        self.assertTrue(len(sentinel_issue_custom) > 0)

    def test_suppressed_rules(self):
        # Create data that naturally triggers weekend_date_anomaly and round_number_anomaly
        dates = pd.date_range(start="2026-05-16", periods=10, freq="D") # 2026-05-16 is a Saturday
        amounts = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
        df = pd.DataFrame({
            "order_date": dates,
            "amount": amounts
        })
        
        profile = {
            "columns": {
                "order_date": {"semantic_type": "date"},
                "amount": {"semantic_type": "numeric"}
            }
        }
        
        # Test without suppression
        res_default = analyze_dataset_quality("test_df", df, profile, thresholds={})
        types_default = [it.get("type") for it in res_default["issues"]]
        
        # Test with suppression
        custom_thresholds = {
            "suppressed_rules": ["weekend_date_anomaly", "round_number_anomaly"]
        }
        res_suppressed = analyze_dataset_quality("test_df", df, profile, thresholds=custom_thresholds)
        types_suppressed = [it.get("type") for it in res_suppressed["issues"]]
        
        self.assertNotIn("weekend_date_anomaly", types_suppressed)
        self.assertNotIn("round_number_anomaly", types_suppressed)

    def test_formula_rules(self):
        df = pd.DataFrame({
            "Quantity": [2, 3, 5, 0],
            "UnitPrice": [10.0, 15.0, 20.0, 5.0],
            "TotalAmount": [20.0, 45.0, 90.0, 0.0] # 5 * 20.0 is 100, so 90.0 is a violation!
        })
        
        profile = {
            "columns": {
                "Quantity": {"semantic_type": "numeric"},
                "UnitPrice": {"semantic_type": "numeric"},
                "TotalAmount": {"semantic_type": "numeric"}
            }
        }
        
        custom_thresholds = {
            "formula_rules": [
                {
                    "assertion": "TotalAmount == Quantity * UnitPrice",
                    "severity": "high",
                    "message": "TotalAmount does not equal Quantity * UnitPrice"
                }
            ]
        }
        
        res = analyze_dataset_quality("test_df", df, profile, thresholds=custom_thresholds)
        issues = [it for it in res["issues"] if it.get("type") == "formula_rule_violation"]
        
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["count"], 1) # only row with Quantity=5, UnitPrice=20, TotalAmount=90 violates the formula
        self.assertIn("Quantity", issues[0]["column"])
        self.assertIn("TotalAmount", issues[0]["column"])

    def test_near_duplicate_bucketing(self):
        # Create a dataframe with near duplicate rows (varying by a minor typo)
        df = pd.DataFrame({
            "name": ["John Doe", "John Doe", "Alice Smith", "Alicia Smith"] * 100, # 400 rows total, triggers bucketing
            "city": ["New York", "New Yokr", "Los Angeles", "Los Angeles"] * 100
        })
        
        profile = {
            "columns": {
                "name": {"semantic_type": "categorical"},
                "city": {"semantic_type": "categorical"}
            }
        }
        
        custom_thresholds = {
            "near_duplicate": {
                "enabled": True,
                "threshold": 0.85,
                "max_rows": 1000
            }
        }
        
        res = analyze_dataset_quality("test_df", df, profile, thresholds=custom_thresholds)
        issues = [it for it in res["issues"] if it.get("type") == "near_duplicate_rows"]
        
        # Bucketed near-duplicate checks should identify similarity between "John Doe | New York" and "John Doe | New Yokr"
        self.assertTrue(len(issues) > 0)

    def test_smart_self_referencing_fk(self):
        # Case A: Two different entity IDs (CustomerID vs OrderID) -> should NOT trigger self-referencing orphan FK
        df_diff = pd.DataFrame({
            "OrderID": [1, 2, 3],
            "CustomerID": [10, 20, 30] # Values don't match OrderID, but they represent Customer entity, so skip!
        })
        profile_diff = {
            "columns": {
                "OrderID": {"semantic_type": "numeric_id"},
                "CustomerID": {"semantic_type": "numeric_id"}
            }
        }
        res_diff = analyze_dataset_quality("test_diff", df_diff, profile_diff)
        issues_diff = [it for it in res_diff["issues"] if it.get("type") == "intra_dataset_orphan_fk"]
        self.assertEqual(len(issues_diff), 0)

        # Case B: Parent-child relationship (ParentOrderID vs OrderID) -> SHOULD trigger orphan check!
        df_same = pd.DataFrame({
            "OrderID": [1, 2, 3],
            "ParentOrderID": [1, 1, 9] # 9 does not exist in OrderID, so it's an orphan!
        })
        profile_same = {
            "columns": {
                "OrderID": {"semantic_type": "numeric_id"},
                "ParentOrderID": {"semantic_type": "numeric_id"}
            }
        }
        res_same = analyze_dataset_quality("test_same", df_same, profile_same)
        issues_same = [it for it in res_same["issues"] if it.get("type") == "intra_dataset_orphan_fk"]
        self.assertEqual(len(issues_same), 1)
        self.assertEqual(issues_same[0]["count"], 1)

    @pytest.mark.xfail(strict=True, reason="Legacy zero_to_null action generator superseded by nullify_clean_value - tracked by ADR-006")
    def test_zero_to_null_codegen_sentinels(self):
        from agent.etl_pipeline.step_params import build_step_params
        from agent.etl_pipeline.python_codegen import _emit_zero_to_null
        from agent.etl_pipeline.sql_codegen import generate_sql_etl

        # Verify python step params and code output
        col_stats = {"dtype": "int", "row_count": 100}
        evidence = {"issue_type": "sentinel_numeric_value"}
        rules = {}
        
        params = build_step_params("zero_to_null", column="OrderAmount", col_stats=col_stats, evidence=evidence, rules=rules)
        self.assertIn("replace_values", params)
        self.assertIn("-999", params["replace_values"])
        
        python_lines = _emit_zero_to_null("OrderAmount", "out", params)
        python_code = "\n".join(python_lines)
        self.assertIn("replace", python_code)
        self.assertIn("-999", python_code)
        
        # Verify SQL code output
        plan = {
            "plan_id": "test_plan",
            "datasets": {
                "dbo.Orders_Raw": {
                    "steps": [
                        {
                            "column": "OrderAmount",
                            "action": "zero_to_null",
                            "params": params,
                            "order": 1
                        }
                    ]
                }
            }
        }
        sql_code = generate_sql_etl(plan, {}, dialect="tsql")
        self.assertIn("LEFT JOIN dbo.etl_invalid_values iv_OrderAmount", sql_code)
        self.assertIn("iv_OrderAmount.column_name = 'Orders_Clean.OrderAmount'", sql_code)
        self.assertIn("TRY_CAST(iv_OrderAmount.invalid_value AS DECIMAL(18,2)) = c.[OrderAmount]", sql_code)
        self.assertIn("-999", sql_code)

    def test_plan_validation_row_level(self):
        from agent.etl_pipeline.validate_plan import validate_etl_plan
        
        assessment = {
            "datasets": {
                "dbo.Orders_Raw": {
                    "columns": {
                        "OrderID": {},
                        "OrderAmount": {}
                    }
                }
            }
        }
        business_rules = {}
        
        # Test case: has [Row-level] column - should be accepted and validate successfully!
        plan = {
            "plan_id": "test_plan",
            "datasets": {
                "dbo.Orders_Raw": {
                    "steps": [
                        {
                            "column": "[Row-level]",
                            "action": "deduplicate",
                            "order": 1
                        }
                    ]
                }
            }
        }
        
        ok, errs = validate_etl_plan(plan, assessment, business_rules)
        self.assertTrue(ok, f"Expected validation to pass but got errors: {errs}")

    def test_plan_validation_multi_column(self):
        from agent.etl_pipeline.validate_plan import validate_etl_plan
        
        assessment = {
            "datasets": {
                "dbo.students_raw": {
                    "columns": {
                        "email": {},
                        "phone": {}
                    }
                }
            }
        }
        business_rules = {}
        
        # Test case: has 'email,phone' column - should be accepted and validate successfully!
        plan = {
            "plan_id": "test_plan",
            "datasets": {
                "dbo.students_raw": {
                    "steps": [
                        {
                            "column": "email,phone",
                            "action": "at_least_one",
                            "order": 1
                        }
                    ]
                }
            }
        }
        
        ok, errs = validate_etl_plan(plan, assessment, business_rules)
        self.assertTrue(ok, f"Expected validation to pass but got errors: {errs}")
        
        # Test case: has 'email,invalid_col' - should fail validation
        bad_plan = {
            "plan_id": "test_plan",
            "datasets": {
                "dbo.students_raw": {
                    "steps": [
                        {
                            "column": "email,invalid_col",
                            "action": "at_least_one",
                            "order": 1
                        }
                    ]
                }
            }
        }
        ok_bad, errs_bad = validate_etl_plan(bad_plan, assessment, business_rules)
        self.assertFalse(ok_bad)
        self.assertTrue(any("not in assessment schema" in e for e in errs_bad))

    @pytest.mark.xfail(strict=True, reason="Pre-catalog deduplicate syntax superseded by PR 4.5 catalog action - tracked by ADR-006")
    def test_row_level_deduplicate_codegen(self):
        from agent.etl_pipeline.sql_codegen import generate_sql_etl
        from agent.etl_pipeline.python_codegen import generate_python_etl
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl

        plan = {
            "plan_id": "test_plan",
            "datasets": {
                "dbo.Orders_Raw": {
                    "steps": [
                        {
                            "column": "[Row-level]",
                            "action": "deduplicate",
                            "order": 1
                        }
                    ]
                }
            }
        }

        # Verify SQL (fallback)
        sql_code = generate_sql_etl(plan, {}, dialect="tsql")
        self.assertIn("PARTITION BY LOWER(LTRIM(RTRIM(CAST([column1] AS NVARCHAR(400))))), LOWER(LTRIM(RTRIM(CAST([column2] AS NVARCHAR(400)))))", sql_code)
        self.assertNotIn("[[Row-level]]]", sql_code)

        # Verify SQL (auto-detected schema columns)
        assessment = {
            "datasets": {
                "dbo.Orders_Raw": {
                    "columns": {
                        "OrderID": {},
                        "CustomerID": {},
                        "OrderAmount": {}
                    }
                }
            }
        }
        sql_code_auto = generate_sql_etl(plan, assessment, dialect="tsql")
        self.assertIn("PARTITION BY LOWER(LTRIM(RTRIM(CAST([CustomerID] AS NVARCHAR(400))))), LOWER(LTRIM(RTRIM(CAST([OrderAmount] AS NVARCHAR(400))))) ORDER BY (SELECT NULL)", sql_code_auto)
        self.assertNotIn("[column1]", sql_code_auto)

        # Verify SQL (auto-detected schema with watermark)
        assessment_watermark = {
            "datasets": {
                "dbo.Orders_Raw": {
                    "columns": {
                        "OrderID": {},
                        "CustomerID": {},
                        "OrderAmount": {},
                        "UpdatedAt": {"dtype": "datetime"}
                    }
                }
            }
        }
        sql_code_watermark = generate_sql_etl(plan, assessment_watermark, dialect="tsql")
        self.assertIn("PARTITION BY LOWER(LTRIM(RTRIM(CAST([CustomerID] AS NVARCHAR(400))))), LOWER(LTRIM(RTRIM(CAST([OrderAmount] AS NVARCHAR(400))))) ORDER BY [UpdatedAt] DESC", sql_code_watermark)

        # Verify Python
        python_code = generate_python_etl(plan, {})
        self.assertIn("out = out.drop_duplicates()", python_code)
        self.assertNotIn("subset=", python_code)

        # Verify PySpark
        spark_code = generate_pyspark_etl(plan, {})
        self.assertIn("_is_duplicate_row", spark_code)
        self.assertNotIn("dropDuplicates('[Row-level]')", spark_code)


    def test_validation_hardening_regex_and_whitespace(self):
        # Create a dataframe with various validation issues:
        # - contact_info has consecutive spaces (internal_whitespace) and leading/trailing spaces (whitespace)
        # - description has HTML tags (html_tags_in_text)
        # - remarks has only symbols/punctuation (punctuation_only_value)
        # - custom_contact (non-standard name) has invalid email structure -> should be mapped correctly because of semantic_type="email"
        df = pd.DataFrame({
            "contact_info": [" hello ", "good  day", "valid"],
            "description": ["<p>hello</p>", "normal text", "ok"],
            "remarks": ["!!!", "...", "normal remark"],
            "custom_contact": ["bademail", "another_bad@", "test@example.com"]
        })
        profile = {
            "columns": {
                "contact_info": {"semantic_type": "text"},
                "description": {"semantic_type": "free_text"},
                "remarks": {"semantic_type": "text"},
                "custom_contact": {"semantic_type": "email"}
            },
            "priority_columns": ["contact_info", "description", "remarks", "custom_contact"]
        }

        res = analyze_dataset_quality("test_hardening", df, profile)
        issues = res.get("issues", [])
        
        issue_types = [it.get("type") for it in issues]
        
        # Verify all issue types are detected and correctly mapped
        self.assertIn("whitespace", issue_types)
        self.assertIn("internal_whitespace", issue_types)
        self.assertIn("html_tags_in_text", issue_types)
        self.assertIn("punctuation_only_value", issue_types)
        self.assertIn("invalid_email", issue_types)
        
        # Verify counts
        whitespace_issue = next(it for it in issues if it["type"] == "whitespace")
        self.assertEqual(whitespace_issue["count"], 1) # "  hello  "
        self.assertEqual(whitespace_issue["column"], "contact_info")

        internal_ws_issue = next(it for it in issues if it["type"] == "internal_whitespace")
        self.assertEqual(internal_ws_issue["count"], 1) # "good  day"
        self.assertEqual(internal_ws_issue["column"], "contact_info")

        html_issue = next(it for it in issues if it["type"] == "html_tags_in_text")
        self.assertEqual(html_issue["count"], 1) # "<p>hello</p>"
        self.assertEqual(html_issue["column"], "description")

        punct_issue = next(it for it in issues if it["type"] == "punctuation_only_value")
        self.assertEqual(punct_issue["count"], 2) # "!!!", "..."
        self.assertEqual(punct_issue["column"], "remarks")

        email_issue = next(it for it in issues if it["type"] == "invalid_email")
        self.assertEqual(email_issue["count"], 2) # "bademail", "another_bad@"
        self.assertEqual(email_issue["column"], "custom_contact")

    @pytest.mark.xfail(strict=True, reason="Pre-catalog heuristic numeric bounds superseded by capability registry - tracked by ADR-006")
    def test_numeric_bounds_and_suspicious_zeros(self):
        # Create a dataframe:
        # - order_id (ID column) has a value of 0 (suspicious_zero)
        # - price (numeric column) has negative values (-10.0) (negative_values)
        # - quantity (numeric column) has positive values only (should pass)
        df = pd.DataFrame({
            "order_id": [101, 0, 103],
            "price": [15.5, -10.0, 20.0],
            "quantity": [2, 5, 1]
        })
        profile = {
            "columns": {
                "order_id": {"semantic_type": "numeric_id"},
                "price": {"semantic_type": "numeric"},
                "quantity": {"semantic_type": "numeric"}
            },
            "priority_columns": ["order_id", "price", "quantity"]
        }
        
        res = analyze_dataset_quality("test_bounds", df, profile)
        issues = res.get("issues", [])
        
        issue_types = [it.get("type") for it in issues]
        
        self.assertIn("suspicious_zero", issue_types)
        self.assertIn("negative_values_review_required", issue_types)
        
        zero_issue = next(it for it in issues if it["type"] == "suspicious_zero")
        self.assertEqual(zero_issue["count"], 1) # 0
        self.assertEqual(zero_issue["column"], "order_id")
        
        neg_issue = next(it for it in issues if it["type"] == "negative_values_review_required")
        self.assertEqual(neg_issue["count"], 1) # -10.0
        self.assertEqual(neg_issue["column"], "price")

    def test_validation_on_non_priority_columns(self):
        # Create a dataframe where contact_info has whitespace issue and is NOT a priority column
        # and has unprofiled columns (e.g. unprofiled_email) whose semantic type should be resolved dynamically!
        df = pd.DataFrame({
            "contact_info": [" hello ", "valid"],
            "unprofiled_email": ["bademail", "test@example.com"]
        })
        profile = {
            "columns": {
                "contact_info": {"semantic_type": "text"}
            },
            "priority_columns": [] # explicitly empty!
        }
        
        res = analyze_dataset_quality("test_non_priority", df, profile)
        issues = res.get("issues", [])
        
        issue_types = [it.get("type") for it in issues]
        
        # Both whitespace issue (on non-priority column) and invalid_email (on unprofiled column dynamically resolved) should be found!
        self.assertIn("whitespace", issue_types)
        self.assertIn("invalid_email", issue_types)
        
        whitespace_issue = next(it for it in issues if it["type"] == "whitespace")
        self.assertEqual(whitespace_issue["column"], "contact_info")
        
        email_issue = next(it for it in issues if it["type"] == "invalid_email")
        self.assertEqual(email_issue["column"], "unprofiled_email")

    def test_nullable_column_nulls_reported(self):
        # Create a dataframe with a nullable column containing missing/null values
        df = pd.DataFrame({
            "nullable_col": ["val1", None, "val2"]
        })
        profile = {
            "columns": {
                "nullable_col": {
                    "nullable": "YES",
                    "semantic_type": "text",
                    "null_percentage": 1.0 / 3.0
                }
            }
        }
        res = analyze_dataset_quality("test_nullable_nulls", df, profile)
        issues = res.get("issues", [])
        
        # Verify that the null value is reported despite the column being nullable
        null_issues = [it for it in issues if it.get("type") == "nulls"]
        self.assertEqual(len(null_issues), 1)
        self.assertEqual(null_issues[0]["column"], "nullable_col")
        self.assertEqual(null_issues[0]["count"], 1)
        self.assertEqual(null_issues[0]["row_indexes"], [1])
        self.assertEqual(null_issues[0]["severity"], "low") # Should be low severity for nullable columns

if __name__ == "__main__":
    unittest.main()
