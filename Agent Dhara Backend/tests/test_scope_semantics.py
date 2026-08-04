import unittest
from agent.etl_pipeline.relationship_planner import build_relationship_plan

class TestScopeSemantics(unittest.TestCase):

    def test_factory_missing_required_produces_scope_incomplete(self):
        assessment = {
            "datasets": {
                "marketing_relational_test.csv": {"row_count": 1000},
            },
            "relationships": [
                {
                    "relationship_id": "marketing_factory_rel",
                    "dataset_a": "marketing_relational_test.csv",
                    "dataset_b": "factory_relational_test.json",
                    "column_a": "factory_id",
                    "column_b": "factory_id",
                    "required": True,
                }
            ],
            "data_quality_issues": {}
        }
        res = build_relationship_plan(assessment)
        out_of_scope = res["out_of_scope_relationships"]
        self.assertEqual(len(out_of_scope), 1)
        self.assertEqual(out_of_scope[0]["status"], "SCOPE_INCOMPLETE")

    def test_factory_missing_optional_produces_not_applicable(self):
        assessment = {
            "datasets": {
                "marketing_relational_test.csv": {"row_count": 1000},
            },
            "relationships": [
                {
                    "relationship_id": "marketing_factory_rel",
                    "dataset_a": "marketing_relational_test.csv",
                    "dataset_b": "factory_relational_test.json",
                    "column_a": "factory_id",
                    "column_b": "factory_id",
                    "required": False,
                }
            ],
            "data_quality_issues": {}
        }
        res = build_relationship_plan(assessment)
        out_of_scope = res["out_of_scope_relationships"]
        self.assertEqual(len(out_of_scope), 1)
        self.assertEqual(out_of_scope[0]["status"], "NOT_APPLICABLE")

if __name__ == "__main__":
    unittest.main()
