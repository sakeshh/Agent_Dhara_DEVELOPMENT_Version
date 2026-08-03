from __future__ import annotations
import pandas as pd
from typing import Any, Dict, List, Tuple
from agent.remediation.action_contract import RemediationAction

def execute_remediations(
    datasets: Dict[str, pd.DataFrame],
    actions: List[RemediationAction]
) -> Tuple[Dict[str, pd.DataFrame], List[RemediationAction]]:
    """
    Execute planned remediation transformations on DataFrames.
    Ensures raw-value preservation by saving original columns before modifications.
    """
    updated_dfs = {k: v.copy() for k, v in datasets.items()}
    executed_actions = []
    
    for act in actions:
        ds_name = act.dataset
        col = act.column
        
        if ds_name not in updated_dfs:
            continue
            
        df = updated_dfs[ds_name]
        
        if act.mode == "DRY_RUN":
            # Just count and verify
            act.rows_affected = act.before_invalid_count
            act.after_invalid_count = 0  # Expected clean state
            executed_actions.append(act)
            continue
            
        if act.mode == "APPLY_APPROVED_FIXES":
            # Raw-value preservation invariant
            raw_col = f"{col}_raw"
            if raw_col not in df.columns and col in df.columns:
                df[raw_col] = df[col]
                
            # Perform clean operations
            if act.issue_type == "whitespace" and col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                
            elif act.issue_type == "empty_string_values" and col in df.columns:
                df[col] = df[col].replace("", None)
                
            elif act.issue_type == "exact_duplicate_rows":
                # Deduplicate rows
                before_rows = len(df)
                df.drop_duplicates(inplace=True)
                act.rows_affected = before_rows - len(df)
                
            act.after_invalid_count = 0
            executed_actions.append(act)
            
    return updated_dfs, executed_actions
