import unittest
import pandas as pd
import numpy as np
import logging
import re
from agent.etl_pipeline.python_codegen import generate_python_etl
from agent.etl_pipeline.validate_python import validate_etl_python_source
from agent.etl_pipeline.plan_coverage_report import build_coverage_report

class TestTraceabilityFirst(unittest.TestCase):
    def setUp(self):
        # Golden Dataset 1: Email edge cases
        self.email_df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "email": ["@example.com", "abc @example.com", "abc@@example.com", "Test@example.com", "valid.email@domain.co.uk"]
        })
        self.email_assessment = {
            "datasets": {
                "customers": {
                    "columns": {
                        "id": {"dtype": "int"},
                        "email": {"dtype": "object"}
                    }
                }
            },
            "data_quality_issues": {
                "datasets": {
                    "customers": {
                        "issues": [
                            {"column": "email", "type": "invalid_email", "severity": "medium", "message": "invalid email format"}
                        ]
                    }
                }
            }
        }

        # Golden Dataset 2: Mixed ID cases
        self.id_df = pd.DataFrame({
            "id": ["210", "ID_210", "", "  ", "-999", "9999", "123"],
            "value": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0]
        })
        self.id_assessment = {
            "datasets": {
                "customers": {
                    "columns": {
                        "id": {"dtype": "object"},
                        "value": {"dtype": "float"}
                    }
                }
            },
            "data_quality_issues": {
                "datasets": {
                    "customers": {
                        "issues": [
                            {"column": "id", "type": "invalid_numeric", "severity": "medium", "message": "non-numeric IDs"},
                            {"column": "id", "type": "sentinel_numeric_value", "severity": "medium", "unexpected_values": [-999.0, 9999.0]}
                        ]
                    }
                }
            }
        }

        # Golden Dataset 3: Duplicate cases
        self.duplicate_df = pd.DataFrame({
            "id": [1, 2, 2, 3, 3, 4],
            "name": ["Alice", "Bob", "Bob", "Charlie", "Charlie Edit", "David"],
            "updated_at": ["2023-01-01", "2023-01-02", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"]
        })
        self.duplicate_assessment = {
            "datasets": {
                "customers": {
                    "columns": {
                        "id": {"dtype": "int"},
                        "name": {"dtype": "object"},
                        "updated_at": {"dtype": "object"}
                    }
                }
            },
            "data_quality_issues": {
                "datasets": {
                    "customers": {
                        "issues": [
                            {"column": "id", "type": "near_duplicates", "severity": "high", "message": "duplicate keys"}
                        ]
                    }
                }
            }
        }

        # Golden Dataset 4: High-risk row-loss case
        rows = []
        for i in range(1000):
            email = "valid@example.com"
            if i % 10 == 0:
                email = "@invalid.com"
            age = 30
            if i % 20 == 0:
                age = -999
            id_val = i
            if i % 50 == 0:
                id_val = f"ID_{i}"
            rows.append({"id": id_val, "email": email, "age": age, "updated_at": "2023-01-01"})
        
        # Add duplicate rows
        for i in range(50):
            rows.append({"id": i * 10, "email": "valid@example.com", "age": 30, "updated_at": "2023-01-02"})
        self.high_risk_df = pd.DataFrame(rows)

    def test_layer_1_issue_coverage(self):
        # Assert that reported issues map to expected ETL transform steps
        plan = {
            "plan_id": "cov_plan",
            "datasets": {
                "customers": {
                    "steps": [
                        {"column": "email", "action": "sanitize_email", "order": 1},
                        {"column": "id", "action": "coerce_numeric", "order": 2},
                        {"column": "id", "action": "replace_sentinel_values", "order": 3, "params": {"sentinel_values": [-999.0, 9999.0]}},
                        {"column": "id", "action": "deduplicate", "order": 4}
                    ]
                }
            }
        }
        cov_report = build_coverage_report(self.id_assessment, plan)
        self.assertEqual(len(cov_report.get("uncovered", [])), 0)

    def test_layer_2_static_code_checks(self):
        # 1. Naive email contains('@') check should fail
        plan_email = {
            "plan_id": "test_plan",
            "datasets": {
                "customers": {
                    "steps": [
                        {"column": "email", "action": "sanitize_email", "order": 1}
                    ]
                }
            }
        }
        naive_code = """
import pandas as pd
def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    # Row count logging
    print("Pre count:", len(df))
    df['email'] = df['email'].apply(lambda x: x if '@' in str(x) else None)
    print("Post count:", len(df))
    return df
"""
        ok, errs = validate_etl_python_source(naive_code, plan_email)
        self.assertFalse(ok)
        self.assertTrue(any("naive contains('@')" in e for e in errs), errs)

        # 2. Bare drop_duplicates() when business keys exist should fail
        plan_dedup = {
            "plan_id": "test_plan",
            "business_keys": {"customers": ["id"]},
            "datasets": {
                "customers": {
                    "steps": [
                        {"column": "id", "action": "deduplicate", "order": 1}
                    ]
                }
            }
        }
        bare_dedup_code = """
import pandas as pd
def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    print("Pre count:", len(df))
    df = df.drop_duplicates()
    print("Post count:", len(df))
    return df
"""
        ok, errs = validate_etl_python_source(bare_dedup_code, plan_dedup)
        self.assertFalse(ok)
        self.assertTrue(any("bare drop_duplicates" in e for e in errs), errs)

        # 3. Unconditional dropna() when never_drop_rows is set should fail
        plan_dropna = {
            "plan_id": "test_plan",
            "business_rules": {"never_drop_rows": True},
            "datasets": {
                "customers": {
                    "steps": [
                        {"column": "id", "action": "noop", "order": 1}
                    ]
                }
            }
        }
        dropna_code = """
import pandas as pd
def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    print("Pre count:", len(df))
    df = df.dropna()
    print("Post count:", len(df))
    return df
"""
        ok, errs = validate_etl_python_source(dropna_code, plan_dropna)
        self.assertFalse(ok)
        self.assertTrue(any("never_drop_rows: do not use dropna()" in e for e in errs), errs)

    def test_layer_3_and_4_execution_and_reconciliation(self):
        # Generate and execute ETL on Golden duplicate dataset to verify near duplicates resolution
        plan = {
            "plan_id": "test_plan",
            "business_keys": {"customers": ["id"]},
            "business_rules": {"watermark_column": "updated_at"},
            "datasets": {
                "customers": {
                    "steps": [
                        {"column": "id", "action": "deduplicate", "order": 1}
                    ]
                }
            },
            "connector_manifest": {
                "datasets": {
                    "customers": {"location": "Files/raw/customers.csv", "format": "csv"}
                }
            }
        }
        code = generate_python_etl(plan, self.duplicate_assessment)
        
        # Load and execute code dynamically
        local_vars = {"pd": pd, "np": np, "logging": logging}
        exec(code, globals(), local_vars)
        transform_func = local_vars["transform_customers"]
        
        cleaned = transform_func(self.duplicate_df)
        
        # Reconciliation: input had 6 rows, output should have 4 (resolved exact and near duplicates)
        self.assertEqual(len(cleaned), 4)
        # Verify we kept the latest updated_at for id 3 (Charlie Edit)
        charlie_row = cleaned[cleaned["id"] == 3]
        self.assertEqual(charlie_row.iloc[0]["name"], "Charlie Edit")

    def test_layer_5_pipeline_observability(self):
        # Enforce that if output row count drops by more than 20% unexplained, verification fails
        input_rows = 1000
        output_rows = 750 # drops by 25%
        
        # Approved drops: 10 rows deleted by explicit key dedup, 5 quarantine. Total explained: 15 rows.
        explained_drops = 15
        unexplained_loss_pct = (input_rows - output_rows - explained_drops) / input_rows
        
        # Limit is 20% (0.20)
        self.assertTrue(unexplained_loss_pct > 0.20)
        
        # Verify the deployment gate fails under this unexplained drop
        def evaluate_observability_gate(in_rows, out_rows, explained):
            loss = (in_rows - out_rows - explained) / in_rows
            if loss > 0.20:
                return False, f"Observability failure: unexplained row loss of {loss:.1%} exceeds threshold"
            return True, "Passed"
            
        gate_ok, msg = evaluate_observability_gate(input_rows, output_rows, explained_drops)
        self.assertFalse(gate_ok)
        self.assertIn("exceeds threshold", msg)

if __name__ == "__main__":
    unittest.main()
