from __future__ import annotations
import pandas as pd
from typing import Dict, List
from agent.remediation.action_contract import RemediationAction

def rollback_remediations(
    datasets: Dict[str, pd.DataFrame],
    actions: List[RemediationAction]
) -> Dict[str, pd.DataFrame]:
    """
    Rollback modifications by restoring original raw values from raw-preserved columns.
    """
    restored_dfs = {k: v.copy() for k, v in datasets.items()}
    
    for act in actions:
        ds_name = act.dataset
        col = act.column
        
        if ds_name not in restored_dfs:
            continue
            
        df = restored_dfs[ds_name]
        raw_col = f"{col}_raw"
        
        if raw_col in df.columns:
            # Restore raw values
            df[col] = df[raw_col]
            # Drop the raw preservation column
            df.drop(columns=[raw_col], inplace=True)
            
    return restored_dfs
