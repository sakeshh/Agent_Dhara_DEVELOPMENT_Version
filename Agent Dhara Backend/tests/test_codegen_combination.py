import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Mock deltalake and other heavy libraries to speed up import/test execution
sys.modules['deltalake'] = MagicMock()

from agent.etl_handlers import etl_generate_code

class TestCodegenCombination(unittest.TestCase):
    @patch("agent.etl_handlers.load_session")
    @patch("agent.etl_handlers.save_session")
    @patch("agent.etl_handlers._get_assessment")
    @patch("agent.etl_handlers._template_fallback")
    @patch("agent.etl_handlers._generate_for_engine")
    def test_codegen_combines_cleanse_and_transform(
        self,
        mock_generate,
        mock_template_fallback,
        mock_get_assess,
        mock_save_session,
        mock_load_session
    ):
        # Setup session context mock
        flow = {
            "phase": "approved",
            "codegen_engine": "sql",
            "target_engine": "sql",
            "approved_plan": {
                "plan_id": "test_plan",
                "datasets": {
                    "dbo.Accounts": {}
                }
            }
        }
        sess = {
            "context": {
                "etl_flow": flow
            }
        }
        mock_load_session.return_value = sess
        mock_get_assess.return_value = {}

        # 1. Generate cleanse_only code
        mock_generate.return_value = ("-- CLEANSE CODE", True, [], "template")
        res1 = etl_generate_code(
            "test-session",
            engine="sql",
            sql_dialect="tsql",
            codegen_mode="llm",
            generation_mode="cleanse_only"
        )
        self.assertTrue(res1["ok"])
        self.assertEqual(flow.get("code_cleanse"), "-- CLEANSE CODE")
        self.assertEqual(flow.get("code"), "-- CLEANSE CODE")

        # 2. Generate transform_only code
        mock_generate.return_value = ("-- TRANSFORM CODE", True, [], "template")
        res2 = etl_generate_code(
            "test-session",
            engine="sql",
            sql_dialect="tsql",
            codegen_mode="llm",
            generation_mode="transform_only"
        )
        self.assertTrue(res2["ok"])
        self.assertEqual(flow.get("code_transform"), "-- TRANSFORM CODE")
        
        # Verify they are combined with \nGO\n\n batch separator
        self.assertEqual(flow.get("code"), "-- CLEANSE CODE\nGO\n\n-- TRANSFORM CODE")

if __name__ == "__main__":
    unittest.main()
