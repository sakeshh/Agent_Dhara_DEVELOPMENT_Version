from __future__ import annotations
import json
import logging
from typing import Any, Dict, List, Tuple, Optional
import pandas as pd

logger = logging.getLogger("agent.profiler.relationship_registry")

class RelationshipConfigError(ValueError):
    """Custom exception raised when relationship configuration is invalid."""
    pass

def validate_relationship_config(rel: Dict[str, Any]) -> None:
    """Validate a single relationship configuration."""
    errors = []
    if not rel.get("name"):
        errors.append("name is required")
    
    parent = rel.get("parent") or {}
    child = rel.get("child") or {}
    
    p_dataset = parent.get("dataset")
    p_columns = parent.get("columns")
    c_dataset = child.get("dataset")
    c_columns = child.get("columns")
    
    if not p_dataset:
        errors.append("parent.dataset is required")
    if not p_columns or not isinstance(p_columns, list):
        errors.append("parent.columns is required as a list")
    if not c_dataset:
        errors.append("child.dataset is required")
    if not c_columns or not isinstance(c_columns, list):
        errors.append("child.columns is required as a list")
        
    if p_columns and c_columns and len(p_columns) != len(c_columns):
        errors.append("parent and child column counts must match")
        
    if rel.get("source") == "inferred" and rel.get("gating"):
        errors.append("inferred relationships cannot be gating=True")
        
    if rel.get("severity") not in {"high", "medium", "low"}:
        errors.append(f"invalid severity: {rel.get('severity')}")
        
    if errors:
        raise RelationshipConfigError(f"Relationship '{rel.get('name', 'unnamed')}': {'; '.join(errors)}")

def _normalize_key_series(
    series: pd.Series,
    normalization: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Normalize a key column without dropping rows.
    Returns a dict with aligned Series objects so all indexes remain intact.
    """
    result = series.astype("string")

    if normalization.get("trim", True):
        result = result.str.strip()
    if normalization.get("casefold", False):
        result = result.str.casefold()

    configured_tokens = normalization.get("null_tokens", [])
    normalized_tokens = {str(t).strip().casefold() for t in configured_tokens}

    # Mark pandas NAs/nones/empty strings/whitespace
    token_mask = result.str.casefold().isin(normalized_tokens)
    empty_mask = result.fillna("").eq("")
    null_mask = series.isna() | empty_mask
    excluded = null_mask | token_mask

    # Mask excluded values to pd.NA - preserves index, no dropna
    clean = result.mask(excluded)

    return {
        "values": clean,
        "excluded_mask": excluded,
        "placeholder_mask": token_mask,
        "null_mask": null_mask,
    }

def _build_composite_key_series(
    df: pd.DataFrame,
    columns: List[str],
    normalization: Dict[str, Any]
) -> Tuple[pd.Series, pd.Series]:
    """
    Build a tuple-key Series for composite FK relationships.
    Returns (key_series, excluded_mask) both aligned to df.index.
    """
    if len(columns) == 0:
        raise RelationshipConfigError("columns list must not be empty")

    per_col_norms = {}
    excluded_any = pd.Series(False, index=df.index)

    for col in columns:
        if col not in df.columns:
            raise RelationshipConfigError(
                f"Column '{col}' not found in dataset (columns: {list(df.columns)})"
            )
        norm = _normalize_key_series(df[col], normalization)
        per_col_norms[col] = norm
        excluded_any = excluded_any | norm["excluded_mask"]

    # Build tuple key from normalized values; mask if any component is excluded
    key_series = pd.Series(
        [
            tuple(per_col_norms[col]["values"].iloc[i] for col in columns)
            if not excluded_any.iloc[i] else None
            for i in range(len(df))
        ],
        index=df.index,
        dtype=object,
    )

    return key_series, excluded_any

def compute_directional_orphans(
    parent_df: pd.DataFrame,
    parent_columns: List[str],
    child_df: pd.DataFrame,
    child_columns: List[str],
    normalization: Dict[str, Any],
    max_samples: int = 10,
) -> Dict[str, Any]:
    """
    Compute directional orphans and unreferenced parent keys for a parent-child relationship.
    """
    if len(parent_columns) != len(child_columns):
        raise RelationshipConfigError(
            f"Parent ({len(parent_columns)}) and child ({len(child_columns)}) column counts must match."
        )

    parent_keys, parent_excl = _build_composite_key_series(
        parent_df, parent_columns, normalization
    )
    child_keys, child_excl = _build_composite_key_series(
        child_df, child_columns, normalization
    )

    parent_key_set = set(parent_keys.dropna().unique())
    child_key_set = set(child_keys.dropna().unique())

    shared_keys = parent_key_set & child_key_set
    child_orphan_keys = child_key_set - parent_key_set   # FK violation
    unreferenced_parent_keys = parent_key_set - child_key_set    # informational

    # Row-level masks - index-aligned, no dropna needed
    child_orphan_mask = child_keys.isin(child_orphan_keys) & ~child_excl
    unreferenced_parent_mask = parent_keys.isin(unreferenced_parent_keys) & ~parent_excl

    child_orphan_indexes = child_df.index[child_orphan_mask].tolist()
    unreferenced_parent_indexes = parent_df.index[unreferenced_parent_mask].tolist()

    return {
        "shared_distinct_keys": len(shared_keys),
        "child_orphan_distinct_keys": len(child_orphan_keys),
        "child_orphan_row_count": int(child_orphan_mask.sum()),
        "unreferenced_parent_distinct_keys": len(unreferenced_parent_keys),
        "unreferenced_parent_row_count": int(unreferenced_parent_mask.sum()),
        "sample_child_orphans": sorted(str(k) for k in child_orphan_keys)[:max_samples],
        "sample_unreferenced_parent_keys": sorted(str(k) for k in unreferenced_parent_keys)[:max_samples],
        "child_orphan_indexes": child_orphan_indexes,
        "unreferenced_parent_indexes": unreferenced_parent_indexes,
        
        "child_placeholder_excluded": int(
            pd.concat([_normalize_key_series(child_df[c], normalization)["placeholder_mask"]
                       for c in child_columns], axis=1).any(axis=1).sum()
        ),
        "child_null_excluded": int(child_excl.sum()),
        "parent_placeholder_excluded": int(
            pd.concat([_normalize_key_series(parent_df[c], normalization)["placeholder_mask"]
                       for c in parent_columns], axis=1).any(axis=1).sum()
        ),
        "parent_null_excluded": int(parent_excl.sum()),
        "measurement": "exact",
    }

def discover_inferred_relationships(
    datasets: Dict[str, pd.DataFrame],
    discovery_cfg: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Auto-discover potential relationships based on name heuristics, overlap counts, and ratios.
    """
    if not discovery_cfg.get("enabled", True):
        return []

    min_overlap_count = int(discovery_cfg.get("minimum_overlap_count", 10))
    min_overlap_ratio = float(discovery_cfg.get("minimum_overlap_ratio", 0.10))
    suffixes = discovery_cfg.get("suffixes") or ["_id", "_key", "_code", "_sku"]

    def _is_key_like(name: str) -> bool:
        name_lower = name.lower()
        if name_lower == "id" or name_lower == "sku":
            return True
        return any(name_lower.endswith(s) for s in suffixes)

    names = list(datasets.keys())
    discovered = []

    for i in range(len(names)):
        n1, df1 = names[i], datasets[names[i]]
        for j in range(len(names)):
            if i == j:
                continue
            n2, df2 = names[j], datasets[names[j]]

            if df1.empty or df2.empty:
                continue

            # Look for overlapping keys in cols of n1 and n2
            for c1 in df1.columns:
                if not _is_key_like(c1):
                    continue

                for c2 in df2.columns:
                    if not _is_key_like(c2):
                        continue

                    # If names are completely different and neither is 'id', skip unless we have some name similarity
                    c1_lower = c1.lower()
                    c2_lower = c2.lower()
                    
                    # Compute name similarity score
                    if c1_lower == c2_lower:
                        name_sim_score = 1.0
                    elif c1_lower in c2_lower or c2_lower in c1_lower:
                        name_sim_score = 0.7
                    else:
                        name_sim_score = 0.4

                    # Only evaluate if there is some name similarity or at least one is named 'id'
                    if name_sim_score < 0.7 and c1_lower != "id" and c2_lower != "id":
                        continue

                    try:
                        # Normalize columns to compare
                        norm1 = _normalize_key_series(df1[c1], {})["values"]
                        norm2 = _normalize_key_series(df2[c2], {})["values"]
                        
                        s1 = set(norm1.dropna().unique())
                        s2 = set(norm2.dropna().unique())
                        
                        shared = s1 & s2
                        shared_count = len(shared)
                    except Exception:
                        continue

                    if shared_count < min_overlap_count:
                        continue

                    parent_distinct = len(s1)
                    child_distinct = len(s2)
                    
                    overlap_ratio = shared_count / max(1, min(parent_distinct, child_distinct))
                    
                    # Stage 1: Candidate eligibility
                    eligible = (shared_count >= min_overlap_count and overlap_ratio >= min_overlap_ratio)
                    if not eligible:
                        continue

                    # Determine cardinality for cardinality score
                    vc1 = norm1.dropna().value_counts()
                    vc2 = norm2.dropna().value_counts()
                    common_idx = vc1.index.intersection(vc2.index)
                    m1 = int(vc1.reindex(common_idx).fillna(0).max()) if len(common_idx) else 1
                    m2 = int(vc2.reindex(common_idx).fillna(0).max()) if len(common_idx) else 1
                    
                    if m1 <= 1 and m2 <= 1:
                        cardinality_score = 1.0
                        card_str = "one_to_one"
                    elif m1 <= 1 < m2:
                        cardinality_score = 1.0
                        card_str = "one_to_many"
                    elif m2 <= 1 < m1:
                        cardinality_score = 1.0
                        card_str = "many_to_one"
                    else:
                        cardinality_score = 0.3
                        card_str = "many_to_many"

                    # Stage 2: Confidence classification
                    overlap_score = overlap_ratio
                    confidence = (
                        overlap_score      * 0.40 +
                        cardinality_score  * 0.30 +
                        name_sim_score     * 0.30
                    )

                    # Only suggested if confidence >= 0.70
                    if confidence >= 0.70:
                        discovered.append({
                            "relationship_id": f"inferred_{n1}_{c1}_to_{n2}_{c2}",
                            "source": "inferred",
                            "confidence": round(confidence, 2),
                            "confidence_components": {
                                "overlap": round(overlap_score, 2),
                                "cardinality": round(cardinality_score, 2),
                                "name_similarity": round(name_sim_score, 2)
                            },
                            "parent": f"{n1}.{c1}",
                            "child": f"{n2}.{c2}",
                            "shared_distinct_keys": shared_count,
                            "child_orphan_distinct_keys": len(s2 - s1),
                            "unreferenced_parent_distinct_keys": len(s1 - s2),
                            "gating": False,
                            "severity": "medium",
                            "recommendation": "Review and promote to declared relationship if valid.",
                            "eligible_for_discovery": True,
                        })
    return discovered
