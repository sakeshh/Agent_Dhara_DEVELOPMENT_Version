import unittest
from agent.etl_pipeline.relationship_planner import build_relationship_plan

class TestRelationshipSafety(unittest.TestCase):

    def test_orphan_lookup_miss_is_evidence_not_found_not_zero(self):
        # Setup assessment where relationship exists but parent and child are not fully assessed (evidence missing)
        assessment = {
            "datasets": {
                "marketing_relational_test.csv": {"row_count": 1000},
                # sales_relational_test.xml is missing, so evidence is not found
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
            "data_quality_issues": {}
        }
        res = build_relationship_plan(assessment)
        out_of_scope = res["out_of_scope_relationships"]
        self.assertEqual(len(out_of_scope), 1)
        self.assertEqual(out_of_scope[0]["status"], "SCOPE_INCOMPLETE")
        self.assertIn("sales_relational_test.xml", out_of_scope[0]["missing_datasets"])

    def test_missing_optional_parent_marks_not_applicable(self):
        assessment = {
            "datasets": {
                "marketing_relational_test.csv": {"row_count": 1000},
            },
            "relationships": [
                {
                    "relationship_id": "marketing_sales_campaign",
                    "dataset_a": "marketing_relational_test.csv",
                    "dataset_b": "sales_relational_test.xml",
                    "column_a": "campaign_id",
                    "column_b": "campaign_id",
                    "cardinality": "one_to_many",
                    "required": False, # Optional
                }
            ],
            "data_quality_issues": {}
        }
        res = build_relationship_plan(assessment)
        out_of_scope = res["out_of_scope_relationships"]
        self.assertEqual(len(out_of_scope), 1)
        self.assertEqual(out_of_scope[0]["status"], "NOT_APPLICABLE")

    def test_three_state_orphan_count_evaluated(self):
        assessment = {
            "datasets": {
                "marketing_relational_test.csv": {"row_count": 1000},
                "sales_relational_test.xml": {"row_count": 1000},
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
        res = build_relationship_plan(assessment)
        joins = res["joins"]
        self.assertEqual(len(joins), 1)
        self.assertEqual(joins[0]["orphan_row_count"], 253)
        self.assertEqual(joins[0]["assessment_status"], "EVALUATED")

    def test_three_state_orphan_count_zero(self):
        assessment = {
            "datasets": {
                "marketing_relational_test.csv": {"row_count": 1000},
                "sales_relational_test.xml": {"row_count": 1000},
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
            "data_quality_issues": {} # Empty - means 0 orphans found
        }
        res = build_relationship_plan(assessment)
        joins = res["joins"]
        self.assertEqual(len(joins), 1)
        self.assertEqual(joins[0]["orphan_row_count"], 0)
        self.assertEqual(joins[0]["assessment_status"], "EVALUATED")

if __name__ == "__main__":
    unittest.main()
