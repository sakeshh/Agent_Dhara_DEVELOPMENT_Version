from __future__ import annotations
import pytest
from typing import Dict, Any

from agent.etl_diff_engine import (
    compare_plan_jsons,
    track_rule_changes,
    detect_semantic_model_drift,
)

def test_compare_plan_jsons():
    plan_v1 = {
        "plan_id": "v1",
        "engine_recommendation": "python",
        "generation_mode": "full",
        "datasets": {
            "customers": {
                "steps": [
                    {"action": "trim", "column": "name"},
                    {"action": "coerce_numeric", "column": "age"},
                ]
            },
            "orders": {
                "steps": [
                    {"action": "parse_dates", "column": "order_date"}
                ]
            }
        }
    }
    
    plan_v2 = {
        "plan_id": "v2",
        "engine_recommendation": "spark",
        "generation_mode": "full",
        "datasets": {
            "customers": {
                "steps": [
                    {"action": "trim", "column": "name"},
                    {"action": "sanitize_email", "column": "email"},
                ]
            },
            "transactions": {
                "steps": [
                    {"action": "coerce_numeric", "column": "amount"}
                ]
            }
        }
    }
    
    diff = compare_plan_jsons(plan_v1, plan_v2)
    assert diff["plan_id_v1"] == "v1"
    assert diff["plan_id_v2"] == "v2"
    assert "transactions" in diff["added_datasets"]
    assert "orders" in diff["removed_datasets"]
    
    # customers common dataset step changes
    step_changes = diff["step_changes"]
    assert len(step_changes) == 1
    cust_change = step_changes[0]
    assert cust_change["dataset"] == "customers"
    # added sanitize_email, removed coerce_numeric age
    added_actions = [x["action"] for x in cust_change["added_steps"]]
    removed_actions = [x["action"] for x in cust_change["removed_steps"]]
    assert "sanitize_email" in added_actions
    assert "coerce_numeric" in removed_actions
    
    # config changes
    config_changes = diff["config_changes"]
    assert "engine_recommendation" in config_changes
    assert config_changes["engine_recommendation"]["before"] == "python"
    assert config_changes["engine_recommendation"]["after"] == "spark"


def test_track_rule_changes():
    rules_v1 = {
        "never_drop_rows": True,
        "required_columns": ["id", "name"],
        "valid_values": {
            "status": ["active", "pending"]
        }
    }
    rules_v2 = {
        "never_drop_rows": False,
        "required_columns": ["id", "name", "email"],
        "valid_values": {
            "status": ["active", "pending", "deleted"],
            "gender": ["M", "F"]
        }
    }
    
    track = track_rule_changes(rules_v1, rules_v2)
    assert track["has_changes"] is True
    changes = track["changes"]
    
    # modified never_drop_rows
    assert changes["modified"]["never_drop_rows"]["before"] is True
    assert changes["modified"]["never_drop_rows"]["after"] is False
    
    # added required_columns email
    assert "email" in changes["added"]["required_columns"]
    
    # added valid_values for gender
    assert "gender" in changes["added"]["valid_values"]
    assert changes["added"]["valid_values"]["gender"] == ["M", "F"]
    
    # modified valid_values for status
    assert "status" in changes["modified"]["valid_values"]
    assert changes["modified"]["valid_values"]["status"]["after"] == ["active", "pending", "deleted"]


def test_detect_semantic_model_drift():
    model_v1 = {
        "overall_semantic_confidence": 0.85,
        "entities": {
            "customer": {"columns": ["customer_id", "email"]}
        },
        "relationships": [
            {"from": "orders.customer_id", "to": "customer.customer_id"}
        ]
    }
    model_v2 = {
        "overall_semantic_confidence": 0.90,
        "entities": {
            "customer": {"columns": ["customer_id", "email"]},
            "order": {"columns": ["order_id", "customer_id"]}
        },
        "relationships": [
            {"from": "orders.customer_id", "to": "customer.customer_id"},
            {"from": "order.customer_id", "to": "customer.customer_id"}
        ]
    }
    
    drift = detect_semantic_model_drift(model_v1, model_v2)
    assert drift["drifted"] is True
    assert "order" in drift["added_entities"]
    assert drift["confidence_delta"] == 0.05
    assert drift["relationships_v1"] == 1
    assert drift["relationships_v2"] == 2
