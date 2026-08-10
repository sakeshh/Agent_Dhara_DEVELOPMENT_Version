"""
Universal Candidate Key Discovery and Key Integrity Engine.
Evaluates candidate primary/composite keys, raw uniqueness vs normalized uniqueness collisions,
and key validity rules.
"""

import re
from typing import Any, Dict, List, Optional, Set, Tuple
import pandas as pd
from pydantic import BaseModel, Field, ConfigDict


class KeyUniquenessResult(BaseModel):
    model_config = ConfigDict(extra="allow")
    key_columns: List[str]
    total_rows: int
    null_rows: int
    raw_unique_count: int
    normalized_unique_count: int
    raw_uniqueness_ratio: float
    normalized_uniqueness_ratio: float
    has_raw_duplicates: bool
    has_normalized_collisions: bool
    duplicate_samples: List[Dict[str, Any]] = Field(default_factory=list)
    collision_samples: List[Dict[str, Any]] = Field(default_factory=list)


def normalize_value_for_matching(val: Any) -> str:
    """
    Normalizes value by trimming whitespace and converting to lowercase.
    """
    if val is None or pd.isna(val):
        return ""
    return str(val).strip().lower()


def evaluate_key_uniqueness(df: pd.DataFrame, key_columns: List[str]) -> KeyUniquenessResult:
    """
    Evaluates raw uniqueness and normalized uniqueness for single or composite keys.
    Detects normalized collisions where raw values are unique but normalized values conflict
    (e.g., 'ABC001' vs 'abc001' vs 'ABC001 ').
    """
    total_rows = len(df)
    if total_rows == 0 or not key_columns or not all(col in df.columns for col in key_columns):
        return KeyUniquenessResult(
            key_columns=key_columns,
            total_rows=total_rows,
            null_rows=0,
            raw_unique_count=0,
            normalized_unique_count=0,
            raw_uniqueness_ratio=0.0,
            normalized_uniqueness_ratio=0.0,
            has_raw_duplicates=False,
            has_normalized_collisions=False
        )

    # Check nulls in key columns
    null_mask = df[key_columns].isna().any(axis=1)
    null_rows = int(null_mask.sum())

    non_null_df = df[~null_mask][key_columns]
    
    # 1. Raw Uniqueness
    raw_tuples = [tuple(row) for row in non_null_df.to_numpy()]
    raw_unique_count = len(set(raw_tuples))
    has_raw_duplicates = raw_unique_count < len(raw_tuples)

    # 2. Normalized Uniqueness
    norm_tuples = [
        tuple(normalize_value_for_matching(val) for val in row)
        for row in non_null_df.to_numpy()
    ]
    normalized_unique_count = len(set(norm_tuples))
    has_normalized_collisions = normalized_unique_count < raw_unique_count

    raw_ratio = round(raw_unique_count / total_rows, 4) if total_rows > 0 else 0.0
    norm_ratio = round(normalized_unique_count / total_rows, 4) if total_rows > 0 else 0.0

    # Collect duplicate samples
    dup_samples = []
    if has_raw_duplicates:
        raw_series = pd.Series(raw_tuples)
        dups = raw_series[raw_series.duplicated()].head(5).tolist()
        dup_samples = [{"key_value": list(d)} for d in dups]

    # Collect normalized collision samples
    coll_samples = []
    if has_normalized_collisions:
        norm_map: Dict[Tuple[str, ...], Set[Tuple[Any, ...]]] = {}
        for raw, norm in zip(raw_tuples, norm_tuples):
            if norm not in norm_map:
                norm_map[norm] = set()
            norm_map[norm].add(raw)

        for norm_key, raw_set in norm_map.items():
            if len(raw_set) > 1:
                coll_samples.append({
                    "normalized_key": list(norm_key),
                    "conflicting_raw_values": [list(r) for r in raw_set]
                })
                if len(coll_samples) >= 5:
                    break

    return KeyUniquenessResult(
        key_columns=key_columns,
        total_rows=total_rows,
        null_rows=null_rows,
        raw_unique_count=raw_unique_count,
        normalized_unique_count=normalized_unique_count,
        raw_uniqueness_ratio=raw_ratio,
        normalized_uniqueness_ratio=norm_ratio,
        has_raw_duplicates=has_raw_duplicates,
        has_normalized_collisions=has_normalized_collisions,
        duplicate_samples=dup_samples,
        collision_samples=coll_samples
    )


def discover_candidate_keys(df: pd.DataFrame) -> List[List[str]]:
    """
    Discovers candidate primary keys based on name heuristics and uniqueness ratios.
    """
    candidate_keys: List[List[str]] = []
    if df.empty:
        return candidate_keys

    # 1. Single column keys with name hint preference
    for col in df.columns:
        col_lower = col.lower()
        res = evaluate_key_uniqueness(df, [col])
        if res.null_rows == 0 and res.normalized_uniqueness_ratio >= 0.99:
            if re.search(r'\b(id|code|uuid|guid|key|pk|number)\b|_id$|_key$', col_lower):
                candidate_keys.append([col])

    # 2. Composite keys if no single candidate key found
    if not candidate_keys and len(df.columns) >= 2:
        cols = list(df.columns)[:5]
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                pair = [cols[i], cols[j]]
                res = evaluate_key_uniqueness(df, pair)
                if res.null_rows == 0 and res.normalized_uniqueness_ratio >= 0.99:
                    candidate_keys.append(pair)

    return candidate_keys
