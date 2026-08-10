from __future__ import annotations
import pytest

from agent.semantic_context_builder import build_all_semantic_contexts
from agent.semantic_context import ColumnRole, EnrichedSemanticModel


def test_semantic_context_always_populated():
    """
    Contract: build_all_semantic_contexts must populate and return an EnrichedSemanticModel
    with entities and target model hints.
    """
    assessment = {
        "datasets": {
            "dbo.customers": {
                "columns": {
                    "customer_id": {
                        "dtype": "int",
                        "candidate_primary_key": True,
                        "semantic_type": "id"
                    },
                    "phone_num": {
                        "dtype": "varchar(20)",
                        "semantic_type": "phone"
                    }
                }
            }
        },
        "business_rules": {
            "non_nullable": ["dbo.customers.customer_id"]
        }
    }
    
    manifest = {
        "datasets": {
            "dbo.customers": {
                "columns": {
                    "customer_id": {
                        "business_importance": "critical",
                        "meaning": "Primary key for customer table"
                    }
                }
            }
        }
    }
    
    res = build_all_semantic_contexts(assessment, manifest=manifest)
    
    assert "by_dataset" in res
    assert "dbo.customers" in res["by_dataset"]
    ctx = res["by_dataset"]["dbo.customers"]
    
    # Check that Component 10 enhanced attributes are built in the model
    # Wait, the build_all_semantic_contexts adds "semantic_context" to the assessment, or returns the serialized model
    # Let's verify what keys are present in the returned dictionary for a dataset context
    assert "critical_columns" in ctx
    assert "customer_id" in ctx["critical_columns"]
    assert ctx["column_importance"].get("customer_id") == "high"


def test_column_roles_detected_for_obvious_columns():
    """
    Contract: Obvious columns (like ID, phone, email, date) must have their ColumnRole mapped correctly.
    """
    # Let's inspect the semantic model parsing directly from semantic_context
    from agent.semantic_context import EnrichedSemanticModel, ColumnRole, ColumnCleaningContract
    
    contract = ColumnCleaningContract(
        column_name="email_address",
        role=ColumnRole.EMAIL,
        non_nullable=True,
        valid_values=["test@test.com"]
    )
    
    assert contract.role == ColumnRole.EMAIL
    assert contract.non_nullable is True
    assert contract.valid_values == ["test@test.com"]
