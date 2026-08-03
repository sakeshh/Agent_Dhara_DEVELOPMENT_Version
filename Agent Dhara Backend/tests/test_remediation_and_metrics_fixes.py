import unittest
import pandas as pd
from agent.profiling.dq_checks import analyze_dataset_quality
from agent.profiling.sql_dialect_adapter import get_remediation_sql
from agent.dq_recommendations_agent import _fallback

class TestRemediationAndMetricsFixes(unittest.TestCase):
    def test_declared_key_null_violation_split(self):
        # Case A: null ratio < 5% (should trigger declared_key_null_violation)
        df_low = pd.DataFrame({
            "id": ["1", None] + [str(i) for i in range(2, 50)]  # 1 null out of 50 rows = 2%
        })
        profile = {
            "columns": {
                "id": {
                    "nullable": "YES",
                    "candidate_primary_key": True,
                    "semantic_type": "identifier"
                }
            }
        }
        # In DHARA, declared keys are inferred from declared relationships in thresholds
        thresholds_low = {
            "relationships": {
                "declared": [
                    {
                        "parent": {"dataset": "test_dataset_low", "columns": ["id"]},
                        "child": {"dataset": "other", "columns": ["other_id"]}
                    }
                ],
                "readiness": {}
            }
        }
        res_low = analyze_dataset_quality(
            name="test_dataset_low",
            df=df_low,
            profile=profile,
            thresholds=thresholds_low,
            business_rules={}
        )
        issues_low = res_low["issues"]
        key_issue_low = next(i for i in issues_low if "id" in str(i.get("column")))
        self.assertEqual(key_issue_low["type"], "declared_key_null_violation")
        self.assertIn("null violation", key_issue_low["message"])

        # Case B: null ratio >= 5% (should trigger declared_key_high_null_ratio)
        df_high = pd.DataFrame({
            "id": ["1", None, None] + [str(i) for i in range(2, 10)] # 2 nulls out of 11 rows = 18.1%
        })
        thresholds_high = {
            "relationships": {
                "declared": [
                    {
                        "parent": {"dataset": "test_dataset_high", "columns": ["id"]},
                        "child": {"dataset": "other", "columns": ["other_id"]}
                    }
                ],
                "readiness": {}
            }
        }
        res_high = analyze_dataset_quality(
            name="test_dataset_high",
            df=df_high,
            profile=profile,
            thresholds=thresholds_high,
            business_rules={}
        )
        issues_high = res_high["issues"]
        key_issue_high = next(i for i in issues_high if "id" in str(i.get("column")))
        self.assertEqual(key_issue_high["type"], "declared_key_high_null_ratio")
        self.assertIn("high null ratio", key_issue_high["message"])

    def test_sql_dialect_adapter_predicates(self):
        # Row-level duplicate strategy check
        dup_sql = get_remediation_sql("fabric_spark_sql", "factory_test", "[Row-level]", "exact_duplicate_rows")
        self.assertIn("duplicate_group_size", dup_sql)
        self.assertIn("PARTITION BY", dup_sql)
        self.assertNotIn("ADD [Row-level]_raw", dup_sql)

        # Placeholder detected check
        ph_sql = get_remediation_sql("fabric_spark_sql", "factory_test", "machine_name", "placeholder_detected")
        self.assertIn("LOWER(TRIM(CAST(machine_name AS STRING))) IN", ph_sql)

        # Invalid date format check
        date_sql = get_remediation_sql("fabric_spark_sql", "factory_test", "inspection_date", "invalid_date_format")
        self.assertIn("TRY_CAST", date_sql)
        self.assertIn("AS DATE) IS NULL", date_sql)

        # Punctuation/symbol-only check
        punc_sql = get_remediation_sql("fabric_spark_sql", "factory_test", "machine_name", "punctuation_only")
        self.assertIn("RLIKE '^[^A-Za-z0-9]+$'", punc_sql)

        # Unsupported issue type check
        unsupported_sql = get_remediation_sql("fabric_spark_sql", "factory_test", "some_col", "unknown_future_issue")
        self.assertIn("Remediation automatically unsupported", unsupported_sql)

    def test_fallback_recs_priority_and_pandas_code(self):
        dq = {
            "datasets": {
                "factory": {
                    "issues": [
                        {
                            "type": "orphan_foreign_key_rows",
                            "column": "product_id",
                            "severity": "high",
                            "gating": True,
                            "count": 167,
                            "message": "Orphan foreign key violation"
                        },
                        {
                            "type": "declared_key_null_violation",
                            "column": "factory_id",
                            "severity": "high",
                            "gating": True,
                            "count": 2,
                            "message": "Null values in declared key"
                        },
                        {
                            "type": "whitespace",
                            "column": "machine_name",
                            "severity": "medium",
                            "gating": False,
                            "count": 10,
                            "message": "Whitespace detected"
                        },
                        {
                            "type": "negative_values",
                            "column": "production_qty",
                            "severity": "high",
                            "gating": True,
                            "count": 5,
                            "message": "Negative quantity"
                        }
                    ]
                }
            }
        }
        recs = _fallback(dq).recommendations
        
        # Check P0 sorting priority: relationship orphan is P0
        self.assertEqual(recs[0]["priority"], 0)
        self.assertEqual(recs[0]["column"], "product_id")
        
        # Check P1 sorting priority: declared key violation is P1
        self.assertEqual(recs[1]["priority"], 1)
        self.assertEqual(recs[1]["column"], "factory_id")
        # Key nulls must use quarantine instead of ffill
        self.assertIn("invalid_key_rows = df[df['factory_id'].isna()]", recs[1]["example_pandas"])
        self.assertIn("quarantine_df = df.filter(df['factory_id'].isNull())", recs[1]["example_pyspark"])

        # Check negative quantity handling: must not clip to zero, sets to NaN/validation status
        neg_rec = next(r for r in recs if r["column"] == "production_qty")
        self.assertIn("qty.where(qty >= 0)", neg_rec["example_pandas"])
        self.assertNotIn("clip(lower=0)", neg_rec["example_pandas"])
        self.assertIn("F.when(df['production_qty'] >= 0, 'valid')", neg_rec["example_pyspark"])

        # Check trim string conversion preserves nulls
        ws_rec = next(r for r in recs if r["column"] == "machine_name")
        self.assertIn("astype('string').str.strip()", ws_rec["example_pandas"])
        self.assertIn("F.trim(df['machine_name'])", ws_rec["example_pyspark"])

        # Check that auto_fixable is disabled on fallback exceptions
        self.assertFalse(recs[0]["auto_fixable"])
        self.assertTrue(ws_rec["auto_fixable"]) # whitespace remains True

