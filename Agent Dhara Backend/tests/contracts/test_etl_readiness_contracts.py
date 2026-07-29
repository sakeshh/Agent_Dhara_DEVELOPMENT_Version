from __future__ import annotations
import pytest

from agent.etl_readiness_scorer import compute_etl_readiness
from agent.azure_sql_executor import check_requires_approval


def test_high_null_critical_column_always_blocks():
    """
    Contract: A business-critical column (business_importance = 'high') with > 30% nulls
    must generate a high-severity blocker and block ETL generation.
    """
    assessment = {
        "datasets": {
            "customers": {
                "columns": {
                    "email": {
                        "null_percentage": 0.35,
                        "llm_hints": {
                            "business_importance": "high"
                        }
                    }
                }
            }
        }
    }
    
    result = compute_etl_readiness(assessment)
    assert result["score"] < 100
    assert len(result["blockers"]) == 1
    blocker = result["blockers"][0]
    assert blocker["column"] == "email"
    assert blocker["severity"] == "HIGH"
    assert blocker["issue_type"] == "high_null_percentage"
    assert "Fix 1 blocker" in result["etl_recommendation"]


def test_no_destructive_ops_in_source():
    """
    Contract: Destructive SQL operations (DROP, TRUNCATE, DELETE) must be detected
    by the check_requires_approval gate and require explicit approval.
    """
    sql_with_drop = "SELECT * FROM sales; DROP TABLE archive_sales;"
    res = check_requires_approval(sql_with_drop)
    assert res["requires_approval"] is True
    assert "DROP" in res["ops_found"]
    
    sql_safe = "SELECT * FROM sales WHERE amount > 100;"
    res_safe = check_requires_approval(sql_safe)
    assert res_safe["requires_approval"] is False
    assert len(res_safe["ops_found"]) == 0


def test_dedup_requires_valid_key():
    """
    Contract: If a business key duplicate exists, it must create a blocker.
    """
    assessment = {
        "datasets": {
            "customers": {
                "llm_hints": {
                    "business_key_confirmation": {
                        "business_key_cols": ["email"],
                        "business_key_duplicate_count": 5
                    }
                }
            }
        }
    }
    
    result = compute_etl_readiness(assessment)
    assert len(result["blockers"]) == 1
    blocker = result["blockers"][0]
    assert blocker["issue_type"] == "business_key_duplicate"
    assert blocker["severity"] == "HIGH"
