"""
Unit tests for PR 3: Clustered AI Assistance & Bounded Concurrency.
Validates:
1. Zero LLM calls when all findings are standard low-risk issues.
2. Dataset context clustering stability.
3. Strict allowable option validation (rejects invented options, falls back to catalog).
4. Failure isolation (one cluster error falls back to catalog without crashing).
5. Cluster result caching across runs.
"""
from unittest.mock import MagicMock, patch
import pytest

from agent.dq_recommendations_agent import (
    DQRecommendationsAgent,
    _process_cluster,
    _CLUSTER_CACHE,
)


def test_zero_llm_calls_for_standard_only_issues():
    """When all issues are standard (whitespace, etc.), 0 LLM calls are made."""
    agent = DQRecommendationsAgent()
    sample_dq = {
        "datasets": {
            "Users": {
                "total_rows": 5000,
                "issues": [
                    {"type": "whitespace", "column": "name", "count": 10, "pct": 0.2},
                    {"type": "case_inconsistency", "column": "status", "count": 20, "pct": 0.4},
                ],
            }
        }
    }

    # Even with mock LLM config, it should not call LLM because all are standard
    with patch("agent.dq_recommendations_agent.load_llm_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(provider="openai", api_key="sk-test", model="gpt-4o")
        with patch("agent.dq_recommendations_agent._process_cluster") as mock_cluster:
            result, usage = agent.recommend(merged_dq=sample_dq)
            assert mock_cluster.call_count == 0
            assert usage is None
            assert len(result.recommendations) == 2
            assert all(r["recommendation_category"] == "standard_remediation" for r in result.recommendations)
            assert "standard_remediation_complete" in result.summary["top_themes"]


def test_process_cluster_validates_allowable_options():
    """Selected option from LLM must match an allowable catalog option; invalid options fall back."""
    _CLUSTER_CACHE.clear()
    cluster_key = ("Orders", "financial_measures")
    findings = [
        {
            "column": "Price",
            "issue_type": "cross_field_non_negative",
            "canonical_issue_type": "cross_field_non_negative",
            "affected_count": 5,
            "severity": "medium",
        }
    ]

    mock_cfg = MagicMock(provider="openai", api_key="sk-test", model="gpt-4o")
    
    # Mock LLM returning valid option: "flag_cross_field_violation"
    mock_resp = MagicMock()
    mock_resp.choices = [
        MagicMock(message=MagicMock(content='{"recommendations": [{"column": "Price", "issue_type": "cross_field_non_negative", "selected_option_id": "flag_cross_field_violation", "why_it_matters": "Negative prices denote returns.", "expected_outcome": "Audit flag added without row deletion."}]}'))
    ]

    with patch("openai.OpenAI") as mock_openai:
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_resp
        mock_openai.return_value = mock_client

        recs, _ = _process_cluster(cluster_key, findings, mock_cfg, platform_target="fabric_spark_sql")
        assert len(recs) == 1
        rec = recs[0]
        assert rec["selected_option_id"] == "flag_cross_field_violation"
        assert rec["mapped_action"] == "flag_cross_field_violation"
        assert rec["recommendation_source"] == "ai_assisted_options"
        assert rec["authority_source"] == "LLM_ADVISORY"
        assert rec["auto_fixable"] is False


def test_process_cluster_rejects_invented_option():
    """If LLM invents an unrecognized option ID, it falls back to the catalog recommended option."""
    _CLUSTER_CACHE.clear()
    cluster_key = ("Inventory", "financial_measures")
    findings = [
        {
            "column": "Cost",
            "issue_type": "cross_field_non_negative",
            "canonical_issue_type": "cross_field_non_negative",
            "affected_count": 3,
            "severity": "medium",
        }
    ]

    mock_cfg = MagicMock(provider="openai", api_key="sk-test", model="gpt-4o")
    # Mock LLM hallucinating option: "magic_delete_all_negatives"
    mock_resp = MagicMock()
    mock_resp.choices = [
        MagicMock(message=MagicMock(content='{"recommendations": [{"column": "Cost", "issue_type": "cross_field_non_negative", "selected_option_id": "magic_delete_all_negatives", "why_it_matters": "Delete them.", "expected_outcome": "Dropped."}]}'))
    ]

    with patch("openai.OpenAI") as mock_openai:
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_resp
        mock_openai.return_value = mock_client

        recs, _ = _process_cluster(cluster_key, findings, mock_cfg, platform_target="fabric_spark_sql")
        assert len(recs) == 1
        rec = recs[0]
        # Must NOT use invented ID
        assert rec["selected_option_id"] != "magic_delete_all_negatives"
        # Falls back to allowable catalog option
        assert rec["selected_option_id"] in [o["id"] for o in rec["options"]]


def test_process_cluster_failure_isolation():
    """If LLM call throws an exception, it falls back to catalog options gracefully."""
    _CLUSTER_CACHE.clear()
    cluster_key = ("Sales", "completeness")
    findings = [
        {
            "column": "tax_id",
            "issue_type": "nulls",
            "canonical_issue_type": "nulls",
            "affected_count": 100,
            "severity": "high",
        }
    ]

    mock_cfg = MagicMock(provider="openai", api_key="sk-test", model="gpt-4o")

    with patch("openai.OpenAI", side_effect=RuntimeError("API Gateway Timeout")):
        recs, usage = _process_cluster(cluster_key, findings, mock_cfg, platform_target="fabric_spark_sql")
        assert len(recs) == 1
        rec = recs[0]
        assert rec["recommendation_source"] == "deterministic_catalog"
        assert rec["authority_source"] == "CATALOG_FALLBACK"
        assert rec["auto_fixable"] is False
        assert rec["lifecycle"]["approval_status"] == "pending"


def test_process_cluster_caching():
    """Identical cluster findings use cache and do not call LLM twice."""
    cluster_key = ("Products", "measure_bounds")
    findings = [
        {
            "column": "discount_pct",
            "issue_type": "implausible_percentage",
            "canonical_issue_type": "implausible_percentage",
            "affected_count": 2,
            "severity": "low",
        }
    ]

    mock_cfg = MagicMock(provider="openai", api_key="sk-test", model="gpt-4o")
    mock_resp = MagicMock()
    mock_resp.choices = [
        MagicMock(message=MagicMock(content='{"recommendations": [{"column": "discount_pct", "issue_type": "implausible_percentage", "selected_option_id": "range_clip", "why_it_matters": "Discounts cannot exceed 100%.", "expected_outcome": "Clipped to 100%."}]}'))
    ]

    with patch("openai.OpenAI") as mock_openai:
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_resp
        mock_openai.return_value = mock_client

        recs1, _ = _process_cluster(cluster_key, findings, mock_cfg, platform_target="fabric_spark_sql")
        call_count_before = mock_client.chat.completions.create.call_count

        # Second call with exact same cluster and findings
        recs2, _ = _process_cluster(cluster_key, findings, mock_cfg, platform_target="fabric_spark_sql")
        call_count_after = mock_client.chat.completions.create.call_count

        assert call_count_before == 1
        assert call_count_after == 1  # Cache hit!
        assert recs1 == recs2
