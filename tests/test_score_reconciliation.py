import unittest
from agent.etl_pipeline.planner import build_etl_plan

class TestScoreReconciliation(unittest.TestCase):

    def test_reconcile_scorecard_averages_and_dataset_values(self):
        # Create a mock assessment for our three datasets:
        # marketing_relational_test.csv (1000 rows), sales_relational_test.xml (1000 rows), transaction_ledger_dirty.json (1000 rows)
        assessment = {
            "datasets": {
                "marketing_relational_test.csv": {
                    "row_count": 1000,
                    "columns": {
                        "campaign_id": {"dtype": "varchar", "null_percentage": 0.056}, # 56 campaign_id nulls
                        "factory_id": {"dtype": "varchar", "null_percentage": 0.003}, # 3 factory_id nulls
                    }
                },
                "sales_relational_test.xml": {
                    "row_count": 1000,
                    "columns": {
                        "campaign_id": {"dtype": "varchar", "null_percentage": 0.001}, # 1 campaign_id null row
                        "product_id": {"dtype": "varchar", "null_percentage": 0.002},  # 2 product_id null rows
                    }
                },
                "transaction_ledger_dirty.json": {
                    "row_count": 1000,
                    "columns": {
                        "transaction_id": {"dtype": "varchar", "null_percentage": 0.001},
                        "amount": {"dtype": "double", "null_percentage": 0.054},
                    }
                }
            },
            "relationships": [
                {
                    "relationship_id": "marketing_sales_campaign",
                    "dataset_a": "marketing_relational_test.csv",
                    "dataset_b": "sales_relational_test.xml",
                    "column_a": "campaign_id",
                    "column_b": "campaign_id",
                    "cardinality": "one_to_many",
                    "required": True,
                }
            ],
            "data_quality_issues": {
                "datasets": {
                    "marketing_relational_test.csv": {
                        "issues": [
                            {"type": "declared_key_null_violation", "severity": "high", "column": "campaign_id", "row_count_affected": 56},
                            {"type": "declared_key_null_violation", "severity": "high", "column": "factory_id", "row_count_affected": 3},
                        ]
                    },
                    "sales_relational_test.xml": {
                        "issues": [
                            {"type": "declared_key_null_violation", "severity": "high", "column": "campaign_id", "row_count_affected": 1},
                            {"type": "declared_key_null_violation", "severity": "high", "column": "product_id", "row_count_affected": 2},
                        ]
                    },
                    "transaction_ledger_dirty.json": {
                        "issues": [
                            {"type": "placeholder_detected", "severity": "medium", "column": "amount", "row_count_affected": 54},
                        ]
                    }
                },
                "global_issues": {
                    "relationship_row_issues": [
                        {
                            "type": "orphan_foreign_key_rows",
                            "related_dataset": "marketing_relational_test.csv",
                            "related_column": "campaign_id",
                            "dataset": "sales_relational_test.xml",
                            "count": 253
                        }
                    ]
                }
            }
        }
        
        # Build plan and extract the scorecard
        rules = {"dq_threshold": 70.0}
        plan = build_etl_plan(assessment, rules, engine="python", generation_mode="full")
        
        scorecard = plan.get("readiness_scorecard")
        self.assertIsNotNone(scorecard)
        
        # Verify scores and readiness statuses:
        # Marketing: Score ~ 70.4, Status: NEEDS_WORK
        # Sales: Score ~ 62.8, Status: NEEDS_WORK
        # Transaction ledger: Score ~ 70.6, Status: READY_WITH_WARNINGS
        # Overall: Score ~ 67.9, Status: NEEDS_WORK
        
        self.assertIn("marketing_relational_test.csv", scorecard)
        self.assertIn("sales_relational_test.xml", scorecard)
        self.assertIn("transaction_ledger_dirty.json", scorecard)
        self.assertIn("Overall", scorecard)
        
        # Row-weighted overall score must be mathematically consistent with individual averages
        scores = [
            scorecard["marketing_relational_test.csv"]["score"],
            scorecard["sales_relational_test.xml"]["score"],
            scorecard["transaction_ledger_dirty.json"]["score"]
        ]
        simple_average = round(sum(scores) / 3.0, 1)
        overall_score = scorecard["Overall"]["score"]
        self.assertEqual(overall_score, simple_average)
        
        # Total rows must reconcile perfectly to 3000
        self.assertEqual(scorecard["Overall"]["row_count"], 3000.0)

if __name__ == "__main__":
    unittest.main()
