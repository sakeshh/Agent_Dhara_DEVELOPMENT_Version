import unittest
from agent.etl_pipeline.issue_to_step_compiler import compile_issues_to_steps

class TestPlanTruthfulness(unittest.TestCase):

    def test_near_dup_emits_flag_not_deduplicate(self):
        suggestions = [
            {
                "dataset": "sales_relational_test.xml",
                "column": "[Row-level]",
                "issue_type": "near_duplicate_rows",
                "suggested_action": "deduplicate",
                "auto_fixable": True,
            }
        ]
        rules = {}
        sem_schema = {}
        steps, manual = compile_issues_to_steps(suggestions, rules, sem_schema)
        # Near duplicate is a flag-only type, so auto_fixable should become False
        # and it should route to manual review since it is not auto-dedupable in Phase 1
        self.assertIn("sales_relational_test.xml", steps)
        steps_list = steps["sales_relational_test.xml"]
        self.assertEqual(len(steps_list), 1)
        self.assertEqual(steps_list[0]["action"], "flag_near_duplicate_cluster")

    def test_exact_dup_emits_flag_not_deduplicate(self):
        suggestions = [
            {
                "dataset": "transaction_ledger_dirty.json",
                "column": "[Row-level]",
                "issue_type": "exact_duplicate_rows",
                "suggested_action": "deduplicate",
                "auto_fixable": True,
            }
        ]
        rules = {}
        sem_schema = {}
        steps, manual = compile_issues_to_steps(suggestions, rules, sem_schema)
        self.assertIn("transaction_ledger_dirty.json", steps)
        steps_list = steps["transaction_ledger_dirty.json"]
        self.assertEqual(len(steps_list), 1)
        self.assertEqual(steps_list[0]["action"], "flag_duplicate_candidates")

    def test_key_null_emits_flag_not_fill(self):
        suggestions = [
            {
                "dataset": "marketing_relational_test.csv",
                "column": "campaign_id",
                "issue_type": "declared_key_null_violation",
                "suggested_action": "fill_nulls_simple",
            }
        ]
        rules = {}
        sem_schema = {
            "marketing_relational_test.csv.campaign_id": {
                "semantic_type": "identifier",
                "sub_type": "key"
            }
        }
        steps, manual = compile_issues_to_steps(suggestions, rules, sem_schema)
        self.assertIn("marketing_relational_test.csv", steps)
        steps_list = steps["marketing_relational_test.csv"]
        self.assertEqual(len(steps_list), 1)
        self.assertEqual(steps_list[0]["action"], "flag_missing_declared_key")

    def test_name_column_emits_trim_not_lowercase(self):
        suggestions = [
            {
                "dataset": "marketing_relational_test.csv",
                "column": "customer_name",
                "issue_type": "case_inconsistency",
                "suggested_action": "lowercase",
            }
        ]
        rules = {}
        sem_schema = {}
        steps, manual = compile_issues_to_steps(suggestions, rules, sem_schema)
        self.assertIn("marketing_relational_test.csv", steps)
        steps_list = steps["marketing_relational_test.csv"]
        self.assertEqual(len(steps_list), 1)
        self.assertEqual(steps_list[0]["action"], "trim_preserve_case")

    def test_llm_confidence_any_value_cannot_set_auto_fixable(self):
        suggestions = [
            {
                "dataset": "sales_relational_test.xml",
                "column": "campaign_id",
                "issue_type": "placeholder_detected",
                "suggested_action": "nullify_punctuation",
                "llm_recommendation": "nullify_punctuation",
                "llm_confidence": 0.99,
                "auto_fixable": True,
            }
        ]
        rules = {}
        sem_schema = {}
        steps, manual = compile_issues_to_steps(suggestions, rules, sem_schema)
        # Any LLM suggestion must be non-auto-fixable and require review
        # So it shouldn't appear in the auto steps!
        self.assertEqual(len(steps), 0)

if __name__ == "__main__":
    unittest.main()
