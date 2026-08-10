from __future__ import annotations
import pandas as pd
from agent.auto_cross_field_rules import generate_auto_cross_field_rules
from agent.cross_field_rules import evaluate_cross_field_rules


def test_auto_cross_field_detection():
    assessment = {
        "datasets": {
            "sales": {
                "columns": {
                    "order_date": {
                        "semantic_type": "date",
                        "dtype": "object"
                    },
                    "ship_date": {
                        "semantic_type": "date",
                        "dtype": "object"
                    },
                    "total_amount": {
                        "semantic_type": "numeric",
                        "dtype": "float64"
                    },
                    "order_id": {
                        "semantic_type": "numeric_id",
                        "dtype": "int64"
                    }
                }
            }
        }
    }
    
    rules = generate_auto_cross_field_rules(assessment)
    assert len(rules) >= 2
    
    # Verify date_order rule
    date_rule = next(r for r in rules if r["type"] == "date_order")
    assert date_rule["dataset"] == "sales"
    assert date_rule["start_column"] == "order_date"
    assert date_rule["end_column"] == "ship_date"
    
    # Verify non_negative rule
    non_neg_rule = next(r for r in rules if r["type"] == "non_negative")
    assert non_neg_rule["dataset"] == "sales"
    assert non_neg_rule["column"] == "total_amount"


def test_auto_cross_field_evaluation():
    rules = [
        {
            "dataset": "sales",
            "type": "date_order",
            "start_column": "order_date",
            "end_column": "ship_date",
            "severity": "medium"
        },
        {
            "dataset": "sales",
            "type": "non_negative",
            "column": "total_amount",
            "severity": "high"
        }
    ]
    
    # Create test data containing violations
    df = pd.DataFrame({
        "order_date": ["2026-06-24", "2026-06-25"],
        "ship_date": ["2026-06-23", "2026-06-26"],  # Row 1 is a date order violation (24th > 23rd)
        "total_amount": [-100.0, 50.0]            # Row 1 is a non-negative violation (-100.0)
    })
    
    issues = evaluate_cross_field_rules("sales", df, rules)
    assert len(issues) == 2
    
    date_issue = next(i for i in issues if i["type"] == "cross_field_date_order")
    assert date_issue["column"] == "order_date,ship_date"
    assert date_issue["count"] == 1
    
    non_neg_issue = next(i for i in issues if i["type"] == "cross_field_non_negative")
    assert non_neg_issue["column"] == "total_amount"
    assert non_neg_issue["count"] == 1
