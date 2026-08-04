"""
relationship_key_normalization_spark.py — PySpark implementation of relationship key normalization.
"""
from __future__ import annotations

from typing import Any
from agent.etl_pipeline.relationship_key_contract import RelationshipKeyContract

def normalize_relationship_key_spark(column_name: str, contract: RelationshipKeyContract) -> Any:
    """
    Returns a PySpark Column expression applying the relationship normalization contract.
    We import PySpark inside the function to ensure Python-only layers can load the module definition.
    """
    from pyspark.sql import functions as F
    
    value = F.col(column_name).cast("string")
    
    if contract.trim:
        value = F.trim(value)
        
    comparison_value = F.lower(value) if contract.case_fold else value
    
    excluded = [token.lower() if contract.case_fold else token for token in contract.null_tokens]
    
    # Check if value is in excluded or is null
    is_null_condition = comparison_value.isin(excluded) | value.isNull()
    if "" in contract.null_tokens:
        is_null_condition = is_null_condition | (value == F.lit(""))
        
    return F.when(is_null_condition, F.lit(None).cast("string")).otherwise(value)
