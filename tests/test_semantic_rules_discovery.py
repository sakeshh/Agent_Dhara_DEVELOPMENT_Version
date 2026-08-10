import unittest
import pandas as pd
import json
from unittest.mock import MagicMock, patch

from agent.specialists.semantic_rules_generator import generate_semantic_rules_from_metadata
from agent.etl_pipeline.business_rules import normalize_business_rules
from agent.model_config import LLMConfig
from agent.chat_graph import _node_discover_semantic_rules, ChatState


class TestSemanticRulesDiscovery(unittest.TestCase):

    @patch("agent.specialists.semantic_rules_generator.load_llm_config")
    @patch("openai.OpenAI")
    def test_generate_semantic_rules_success(self, mock_openai_cls, mock_load_config):
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
            "required_columns": ["student_id", "dept_id"],
            "non_nullable": ["student_id"],
            "valid_values": {
                "student.status": ["Active", "Inactive"]
            },
            "custom_assertions": [
                {
                    "assertion": "course_fees >= 0",
                    "severity": "high",
                    "message": "Fees cannot be negative"
                }
            ]
        })
        mock_client.chat.completions.create.return_value.choices = [mock_choice]

        df = pd.DataFrame({
            "student_id": [1, 2, 3],
            "dept_id": [10, 20, 10],
            "status": ["Active", "Active", "Inactive"],
            "course_fees": [1000, 1500, 2000]
        })

        datasets = {"student": df}
        rules = generate_semantic_rules_from_metadata(datasets)

        self.assertEqual(rules["required_columns"], ["student_id", "dept_id"])
        self.assertEqual(rules["non_nullable"], ["student_id"])
        self.assertIn("student.status", rules["valid_values"])
        self.assertEqual(rules["valid_values"]["student.status"], ["Active", "Inactive"])
        self.assertEqual(len(rules["custom_assertions"]), 1)
        self.assertEqual(rules["custom_assertions"][0]["assertion"], "course_fees >= 0")

    def test_normalize_business_rules_with_custom_assertions(self):
        raw_rules = {
            "custom_assertions": [
                {
                    "assertion": "age > 18",
                    "severity": "high",
                    "message": "Must be an adult"
                },
                "status == 'Active'"
            ]
        }
        normalized = normalize_business_rules(raw_rules)
        self.assertIn("custom_assertions", normalized)
        assertions = normalized["custom_assertions"]
        self.assertEqual(len(assertions), 2)
        
        self.assertEqual(assertions[0]["assertion"], "age > 18")
        self.assertEqual(assertions[0]["severity"], "high")
        self.assertEqual(assertions[0]["message"], "Must be an adult")
        
        self.assertEqual(assertions[1]["assertion"], "status == 'Active'")
        self.assertEqual(assertions[1]["severity"], "medium")
        self.assertEqual(assertions[1]["message"], "")

    @patch("agent.session_store.load_session")
    @patch("agent.session_store.save_session")
    @patch("agent.chat_graph._load_sample_dfs_for_discovery")
    @patch("agent.specialists.semantic_rules_generator.generate_semantic_rules_from_metadata")
    def test_discover_semantic_rules_node_success(self, mock_generate, mock_load_dfs, mock_save, mock_load):
        # Setup session context
        session_id = "test-session-123"
        session_data = {
            "session_id": session_id,
            "context": {
                "selected_tables": ["student"],
                "sources_path": "config/sources.yaml"
            }
        }
        mock_load.return_value = session_data

        # Setup mock loaded dataframes
        df_mock = pd.DataFrame({"student_id": [1, 2]})
        mock_load_dfs.return_value = {"student": df_mock}

        # Setup mock generated rules
        discovered = {
            "required_columns": ["student_id"],
            "non_nullable": ["student_id"],
            "valid_values": {},
            "custom_assertions": []
        }
        mock_generate.return_value = discovered

        # Run node
        state = {"session_id": session_id}
        result = _node_discover_semantic_rules(state)

        # Assert result and side effects
        self.assertIn("reply", result)
        self.assertIn("Discovered Semantic Rules", result["reply"])
        self.assertEqual(result["payload"]["intent"], "discover_semantic_rules")
        
        # Verify save_session was called and pending rules stored
        mock_save.assert_called_once()
        stored_rules = session_data["context"]["pending_business_rules"]
        self.assertEqual(stored_rules["required_columns"], ["student_id"])


if __name__ == "__main__":
    unittest.main()
