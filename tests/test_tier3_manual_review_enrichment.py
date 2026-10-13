from __future__ import annotations

import pytest
from typing import Any, Dict

from agent.etl_pipeline.manual_review_catalog import (
    enrich_manual_review_item,
    tag_option_readiness,
    action_for_resolution,
)
from agent.etl_pipeline.manual_review_promote import enrich_plan_manual_review


def test_enrich_manual_review_item_with_tier3_options():
    """Verify that manual review items produced by Tier 3 orchestrator enrich safely without KeyError: 'id'."""
    raw_tier3_mr_entry = {
        "id": "mr_dbo.CustomerMaster_CreditLimit_credit_limit_anomaly",
        "issue_id": "ci_dbo.customermaster_creditlimit_16281e498a01",
        "dataset": "dbo.CustomerMaster",
        "column": "CreditLimit",
        "issue_type": "credit_limit_anomaly",
        "message": "Values exceed permitted range",
        "status": "APPROVAL_REQUIRED",
        "is_blocking": True,
        "llm_recommendation": {
            "proposal_id": "prop_123",
            "action_type": "CLIP_RANGE",
            "parameters": {"min_value": 0, "max_value": 50000},
            "disposition": "FLAG",
            "rationale": "Clip outlier credit limits",
            "confidence": 0.95,
            "risk_level": "MEDIUM",
        },
        "validation_status": "VALIDATED",
        "approval_required": True,
        "allowed_user_actions": [
            "APPROVE",
            "MODIFY",
            "REJECT",
            "DESCRIBE_ALTERNATIVE",
            "WAIVE",
        ],
        "resolution_options": [
            {
                "id": "approve_clip_range",
                "action": "clip_range",
                "label": "Approve AI recommendation: CLIP_RANGE",
                "description": "Apply CLIP_RANGE with validated condition",
                "recommended": True,
                "is_recommended": True,
                "proposal_id": "prop_123",
            },
            {
                "id": "waive",
                "action": "noop",
                "label": "Waive issue (accept risk)",
                "description": "Ignore this finding and continue execution without transformation",
                "recommended": False,
                "is_recommended": False,
            },
            {
                "id": "modify",
                "action": "noop",
                "label": "Modify parameters or condition",
                "description": "Adjust action parameters or condition before execution",
                "recommended": False,
                "is_recommended": False,
            },
        ],
    }

    enriched = enrich_manual_review_item(raw_tier3_mr_entry)
    assert enriched["default_resolution"] == "approve_clip_range"
    assert enriched["status"] == "APPROVAL_REQUIRED"
    assert len(enriched["resolution_options"]) == 3
    for opt in enriched["resolution_options"]:
        assert "id" in opt
        assert "readiness" in opt


def test_enrich_manual_review_item_defensive_against_missing_id():
    """Verify that even if an external caller supplies resolution options completely lacking 'id', it does not raise KeyError."""
    malformed_entry = {
        "id": "mr_test",
        "dataset": "dbo.TestTable",
        "column": "Salary",
        "issue_type": "negative_salary",
        "resolution_options": [
            {
                "action": "fill_nulls",
                "label": "Fill nulls",
                "is_recommended": True,
            },
            {
                "action": "waive",
                "label": "Waive",
            },
        ],
    }

    enriched = enrich_manual_review_item(malformed_entry)
    assert enriched["default_resolution"] == "fill_nulls"
    assert enriched["resolution_options"][0]["id"] == "fill_nulls"
    assert enriched["resolution_options"][0]["recommended"] is True
    assert enriched["resolution_options"][1]["id"] == "waive"


def test_enrich_plan_manual_review_integration():
    """Verify enrich_plan_manual_review runs successfully over a plan with mixed Tier 3 items."""
    plan: Dict[str, Any] = {
        "datasets": {"dbo.CustomerMaster": {"steps": []}},
        "manual_review": [
            {
                "id": "mr_1",
                "dataset": "dbo.CustomerMaster",
                "column": "CreditLimit",
                "issue_type": "range_outlier",
                "resolution_options": [
                    {
                        "id": "approve_clip_range",
                        "action": "range_clip",
                        "label": "Approve",
                        "recommended": True,
                    },
                    {
                        "id": "waive",
                        "action": "noop",
                        "label": "Waive",
                    },
                ],
            }
        ],
    }

    result = enrich_plan_manual_review(plan)
    assert len(result["manual_review"]) == 1
    assert result["manual_review"][0]["default_resolution"] == "approve_clip_range"
    # Verify action_for_resolution works with waive and approve
    act_approve = action_for_resolution("range_outlier", "approve_clip_range", result["manual_review"][0]["resolution_options"])
    assert act_approve == "range_clip"
    act_waive = action_for_resolution("range_outlier", "waive", result["manual_review"][0]["resolution_options"])
    assert act_waive == "noop"
