"""
relationship_key_normalization_pandas.py — Pandas implementation of relationship key normalization.
"""
from __future__ import annotations

import pandas as pd
from agent.etl_pipeline.relationship_key_contract import RelationshipKeyContract

def normalize_relationship_key_pandas(series: pd.Series, contract: RelationshipKeyContract) -> pd.Series:
    """Normalize a Pandas Series using the key contract. Used in assessment/profiler."""
    # Convert series to string representation first
    s = series.astype(str)
    
    # Handle NaN values mapping to string "nan" or "None" before converting
    s = s.mask(series.isna(), None)
    
    # Apply trim if enabled
    if contract.trim:
        s = s.str.strip()
        
    # Apply case folding comparison logic if enabled
    # We still return the original cased values or normalized nulls
    comparison_series = s.str.lower() if contract.case_fold else s
    
    # Identify excluded values
    excluded = [token.lower() if contract.case_fold else token for token in contract.null_tokens]
    
    # For any blank/whitespace values (which strip down to ""), make sure they are treated as null
    # if "" is in our excluded list (or contract.null_tokens)
    is_null_mask = series.isna() | comparison_series.isin(excluded)
    if "" in contract.null_tokens:
        is_null_mask = is_null_mask | (s == "")
        
    return s.mask(is_null_mask, None)
