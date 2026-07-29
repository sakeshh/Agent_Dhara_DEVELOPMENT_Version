"""
Before/after summaries for ETL (pandas-based).
Also includes plan JSON comparison, rule tracking,
and semantic model version drift detection (Component 14).
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

import pandas as pd


def summarize_frame_diff(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    key_columns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Structural diff: rows/cols, dtype changes, optional key overlap.
    """
    out: Dict[str, Any] = {
        "before_rows": int(len(before)),
        "after_rows": int(len(after)),
        "before_cols": list(before.columns.astype(str)),
        "after_cols": list(after.columns.astype(str)),
        "row_delta": int(len(after) - len(before)),
        "added_columns": sorted(set(after.columns) - set(before.columns), key=str),
        "removed_columns": sorted(set(before.columns) - set(after.columns), key=str),
    }
    common = [c for c in before.columns if c in after.columns]
    dtype_changes = []
    for c in common:
        if str(before[c].dtype) != str(after[c].dtype):
            dtype_changes.append({"column": str(c), "before": str(before[c].dtype), "after": str(after[c].dtype)})
    out["dtype_changes"] = dtype_changes
    if key_columns and all(k in before.columns and k in after.columns for k in key_columns):
        bk = before.set_index(list(key_columns)).index.unique()
        ak = after.set_index(list(key_columns)).index.unique()
        try:
            lost = int(bk.difference(ak).size)
            new = int(ak.difference(bk).size)
            out["key_overlap"] = {"keys_lost": lost, "keys_new": new}
        except Exception:
            out["key_overlap"] = {"error": "could_not_compute"}
    return out


def compare_plan_jsons(
    plan_v1: Dict[str, Any],
    plan_v2: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Compare two ETL plan JSONs and return a structured diff of what changed (v1 → v2).
    Tracks dataset additions/removals, step changes, and configuration differences.
    """
    ds1 = set((plan_v1.get("datasets") or {}).keys())
    ds2 = set((plan_v2.get("datasets") or {}).keys())

    added_datasets = sorted(ds2 - ds1)
    removed_datasets = sorted(ds1 - ds2)
    common_datasets = sorted(ds1 & ds2)

    step_changes: List[Dict[str, Any]] = []
    for ds in common_datasets:
        steps1 = (plan_v1.get("datasets") or {}).get(ds, {}).get("steps") or []
        steps2 = (plan_v2.get("datasets") or {}).get(ds, {}).get("steps") or []

        actions1 = [(s.get("action"), s.get("column")) for s in steps1 if isinstance(s, dict)]
        actions2 = [(s.get("action"), s.get("column")) for s in steps2 if isinstance(s, dict)]

        added_steps = [a for a in actions2 if a not in actions1]
        removed_steps = [a for a in actions1 if a not in actions2]

        if added_steps or removed_steps:
            step_changes.append({
                "dataset": ds,
                "steps_before": len(steps1),
                "steps_after": len(steps2),
                "added_steps": [{"action": a, "column": c} for a, c in added_steps],
                "removed_steps": [{"action": a, "column": c} for a, c in removed_steps],
            })

    # Config diff
    config_changes = {}
    for key in ("engine_recommendation", "etl_intent", "generation_mode"):
        v1_val = plan_v1.get(key)
        v2_val = plan_v2.get(key)
        if v1_val != v2_val:
            config_changes[key] = {"before": v1_val, "after": v2_val}

    return {
        "added_datasets": added_datasets,
        "removed_datasets": removed_datasets,
        "step_changes": step_changes,
        "config_changes": config_changes,
        "plan_id_v1": plan_v1.get("plan_id"),
        "plan_id_v2": plan_v2.get("plan_id"),
    }


def track_rule_changes(
    rules_v1: Dict[str, Any],
    rules_v2: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Track which business rules changed between two runs.
    Returns added, removed, and modified rule keys.
    """
    changes: Dict[str, Any] = {"added": {}, "removed": {}, "modified": {}}

    # Compare flat keys
    for key in ("never_drop_rows", "auto_resolve_pending", "outlier_strategy", "dq_threshold", "notes"):
        v1 = rules_v1.get(key)
        v2 = rules_v2.get(key)
        if v1 != v2:
            changes["modified"][key] = {"before": v1, "after": v2}

    # Compare list keys
    for key in ("required_columns", "non_nullable", "exclude_columns"):
        s1 = set(rules_v1.get(key) or [])
        s2 = set(rules_v2.get(key) or [])
        added = sorted(s2 - s1)
        removed = sorted(s1 - s2)
        if added:
            changes["added"][key] = added
        if removed:
            changes["removed"][key] = removed

    # Compare valid_values dict
    vv1 = rules_v1.get("valid_values") or {}
    vv2 = rules_v2.get("valid_values") or {}
    for col in set(list(vv1.keys()) + list(vv2.keys())):
        if col not in vv1:
            changes["added"].setdefault("valid_values", {})[col] = vv2[col]
        elif col not in vv2:
            changes["removed"].setdefault("valid_values", {})[col] = vv1[col]
        elif set(vv1[col]) != set(vv2[col]):
            changes["modified"].setdefault("valid_values", {})[col] = {
                "before": vv1[col], "after": vv2[col]
            }

    has_changes = any(changes[k] for k in ("added", "removed", "modified"))
    return {"has_changes": has_changes, "changes": changes}


def detect_semantic_model_drift(
    model_v1: Optional[Dict[str, Any]],
    model_v2: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Detect semantic model version drift by comparing two enriched semantic model payloads.
    Returns entity additions/removals, relationship changes, and confidence shifts.
    """
    if not model_v1 and not model_v2:
        return {"drifted": False, "reason": "no_models"}
    if not model_v1:
        return {"drifted": True, "reason": "new_model", "details": {"entities_added": list((model_v2 or {}).get("entities", {}).keys())}}
    if not model_v2:
        return {"drifted": True, "reason": "model_removed"}

    e1 = set((model_v1.get("entities") or {}).keys())
    e2 = set((model_v2.get("entities") or {}).keys())

    added_entities = sorted(e2 - e1)
    removed_entities = sorted(e1 - e2)

    # Compare relationship count
    rels_v1 = len(model_v1.get("relationships") or [])
    rels_v2 = len(model_v2.get("relationships") or [])

    # Schema hash comparison
    def _model_hash(m: Dict[str, Any]) -> str:
        raw = json.dumps(m, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    hash_v1 = _model_hash(model_v1)
    hash_v2 = _model_hash(model_v2)

    # Confidence drift
    conf_v1 = model_v1.get("overall_semantic_confidence", 0.0)
    conf_v2 = model_v2.get("overall_semantic_confidence", 0.0)
    conf_delta = round(conf_v2 - conf_v1, 4)

    drifted = (hash_v1 != hash_v2)

    return {
        "drifted": drifted,
        "schema_hash_v1": hash_v1,
        "schema_hash_v2": hash_v2,
        "added_entities": added_entities,
        "removed_entities": removed_entities,
        "relationships_v1": rels_v1,
        "relationships_v2": rels_v2,
        "confidence_delta": conf_delta,
    }

