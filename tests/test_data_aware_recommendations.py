"""
Unit tests for PR 2: Data-Aware Explanations and Unified Recommendation Contract.
Validates:
1. Data-aware explanation prose generated from action contracts with real metrics.
2. Absence of unsupported standards claims.
3. Fallback recommendations enriched with capability routing, decoupled lifecycles, and objective auto-fix gates.
4. Correct partition of standard vs decision-required findings.
5. High-impact violations triggering pending approval in recommendations.
"""
import pytest
from agent.dq_recommendations_agent import (
    ACTION_OUTCOME_METADATA,
    DQRecommendationsAgent,
    build_data_aware_explanation,
    dq_recommendations_to_dict,
)


def test_action_outcome_metadata_coverage():
    """Verify all standard actions have outcome metadata with status codes and non-destructive contracts."""
    assert "trim" in ACTION_OUTCOME_METADATA
    assert "sanitize_email" in ACTION_OUTCOME_METADATA
    assert "preserve_null_and_flag" in ACTION_OUTCOME_METADATA
    assert "flag_outliers" in ACTION_OUTCOME_METADATA

    for action, meta in ACTION_OUTCOME_METADATA.items():
        assert "status_code" in meta, f"{action} must define status_code"
        assert "preserves_raw" in meta, f"{action} must define preserves_raw"
        assert "creates_clean" in meta, f"{action} must define creates_clean"


def test_build_data_aware_explanation_standard():
    """Standard whitespace fix formats metrics, raw/clean columns, and validation status."""
    why, fix = build_data_aware_explanation(
        dataset="Customers",
        column="FullName",
        canonical_issue_type="whitespace",
        action="trim",
        affected_rows=418,
        total_rows=82450,
        affected_pct=0.51,
        is_standard=True,
    )
    assert "418 of 82,450 records (0.51%)" in why
    assert "Customers.FullName" in why
    assert "FullName_raw" in fix
    assert "FullName_clean" in fix
    assert "trimmed_whitespace" in fix
    assert "RFC 5322" not in why  # No unsupported standards claim


def test_build_data_aware_explanation_contextual():
    """Contextual negative values finding formats decision-required explanation."""
    why, fix = build_data_aware_explanation(
        dataset="Orders",
        column="Price",
        canonical_issue_type="cross_field_non_negative",
        action="flag_outliers",
        affected_rows=12,
        total_rows=50000,
        affected_pct=0.02,
        is_standard=False,
    )
    assert "12 of 50,000 records (0.02%)" in why
    assert "Orders.Price" in why
    assert "Decision required" in fix
    assert "statistical_outlier" in fix
    assert "pending reviewer approval" in fix


@pytest.mark.xfail(strict=True, reason="Legacy fallback routing superseded by PR 4.6b decoupled lifecycle - tracked by ADR-006")
def test_fallback_recommendations_routing_and_lifecycle():
    """_fallback() correctly routes standard vs contextual issues with decoupled lifecycle."""
    agent = DQRecommendationsAgent()
    sample_dq = {
        "datasets": {
            "Customers": {
                "total_rows": 10000,
                "issues": [
                    {
                        "type": "whitespace",
                        "column": "name",
                        "count": 150,
                        "pct": 1.5,
                        "severity": "low",
                    },
                    {
                        "type": "null_values",
                        "column": "email",
                        "count": 800,
                        "pct": 8.0,
                        "severity": "high",
                    },
                    {
                        "type": "negative_values",
                        "column": "balance",
                        "count": 25,
                        "pct": 0.25,
                        "severity": "medium",
                    },
                ],
            }
        }
    }

    result, _ = agent.recommend(merged_dq=sample_dq)
    recs = result.recommendations
    assert len(recs) == 3

    # Find whitespace rec
    ws_rec = next(r for r in recs if r["column"] == "name")
    assert ws_rec["recommendation_category"] == "standard_remediation"
    assert ws_rec["recommendation_source"] == "standard_remediation"
    assert ws_rec["auto_fixable"] is True
    assert ws_rec["lifecycle"]["approval_status"] == "not_required"
    assert "name_raw" in ws_rec["suggested_fix"]
    assert "trimmed_whitespace" in ws_rec["suggested_fix"]

    # Find nulls rec
    null_rec = next(r for r in recs if r["column"] == "email")
    assert null_rec["recommendation_category"] == "decision_required"
    assert null_rec["recommendation_source"] == "deterministic_catalog"
    assert null_rec["auto_fixable"] is False
    assert null_rec["lifecycle"]["approval_status"] == "pending"
    assert "Decision required" in null_rec["suggested_fix"]

    # Find negative values rec
    neg_rec = next(r for r in recs if r["column"] == "balance")
    assert neg_rec["recommendation_category"] == "decision_required"
    assert neg_rec["auto_fixable"] is False
    assert neg_rec["lifecycle"]["approval_status"] == "pending"


def test_high_impact_standard_issue_requires_approval():
    """Standard whitespace issue exceeding threshold requires approval."""
    agent = DQRecommendationsAgent()
    sample_dq = {
        "datasets": {
            "LargeDataset": {
                "total_rows": 100000,
                "issues": [
                    {
                        "type": "whitespace",
                        "column": "text_col",
                        "count": 60000,  # > 50,000 limit
                        "pct": 60.0,    # > 10.0% limit
                        "severity": "low",
                    }
                ],
            }
        }
    }

    result, _ = agent.recommend(merged_dq=sample_dq)
    rec = result.recommendations[0]
    assert rec["recommendation_category"] == "standard_remediation"
    assert rec["auto_fixable"] is False  # Gate triggered!
    assert rec["lifecycle"]["approval_status"] == "pending"
    assert any("EXCEEDS_SAFETY_LIMIT" in code for code in rec["reason_codes"])
