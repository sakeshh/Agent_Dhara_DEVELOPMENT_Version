from __future__ import annotations

from typing import Annotated, Any, Dict, List, Optional, TypedDict
import pandas as pd


def merge_dicts(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Reducer function to merge dictionaries in LangGraph state."""
    if not a:
        return b or {}
    if not b:
        return a or {}
    res = dict(a)
    for k, v in b.items():
        if k in res and isinstance(res[k], dict) and isinstance(v, dict):
            res[k] = merge_dicts(res[k], v)
        else:
            res[k] = v
    return res


class AssessmentState(TypedDict):
    # Inputs
    source_cfg: Dict[str, Any]
    additional_data: Optional[Dict[str, pd.DataFrame]]
    max_rows: Optional[int]
    db_connectors: Optional[Dict[str, Any]]
    approved_semantics: Optional[Dict[str, str]]
    business_rules: Optional[Dict[str, Any]]
    thresholds: Optional[Dict[str, Any]]
    job_id: Optional[str]
    run_gx: Optional[bool]

    # Intermediate states (accumulated across nodes with merge_dicts reducer)
    datasets: Annotated[Dict[str, pd.DataFrame], merge_dicts]
    metadata: Annotated[Dict[str, Any], merge_dicts]
    per_dataset_dq: Annotated[Dict[str, Any], merge_dicts]
    relationships: List[Dict[str, Any]]
    global_issues: Dict[str, Any]
    gx_results: Annotated[Dict[str, Any], merge_dicts]

    # Job / Progress
    progress_pct: int
    error: Optional[str]

    # Output
    result: Dict[str, Any]


class DatasetState(TypedDict):
    """Fanned-out state for parallel operations on a single dataset."""
    dataset_name: str
    df: pd.DataFrame
    thresholds: Optional[Dict[str, Any]]
    business_rules: Optional[Dict[str, Any]]
    job_id: Optional[str]
