"""
Build column-level source → transforms → target lineage from plan + assessment.
"""
from __future__ import annotations

from typing import Any, Dict, List


def _infer_target_dtype(action: str, source_dtype: str) -> str:
    act = (action or "").lower()
    sd = (source_dtype or "unknown").lower()
    if act == "coerce_numeric":
        return "decimal(18,2)"
    if act == "parse_dates":
        return "date"
    if act == "create_normalized_match_key":
        return "string"
    if act in ("trim", "trim_preserve_case", "lowercase", "uppercase", "normalize_phone", "regex_replace", "sanitize_email"):
        return "string"
    if act in ("standardize_boolean",):
        return "boolean"
    if act in ("flag_outliers", "clip_or_flag", "flag_missing_key", "flag_missing_declared_key", "flag_duplicate_candidates", "flag_near_duplicate_cluster"):
        return "boolean"
    if act in ("cast_type", "range_clip", "clip_outliers", "cap_outliers"):
        return "decimal(18,2)" if "int" not in sd else "int64"
    return sd or "unknown"


def build_lineage(plan: Dict[str, Any], assessment: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns { dataset: { column: { source_dtype, transforms[], target_dtype, nullable } } }
    """
    out: Dict[str, Any] = {}

    for ds_name, block in (plan.get("datasets") or {}).items():
        ds_meta = (assessment.get("datasets") or {}).get(ds_name) or {}
        col_meta = ds_meta.get("columns") or {}
        ds_lineage: Dict[str, Any] = {}

        # Add all raw columns first as base entries
        for col_name, meta in col_meta.items():
            src = meta.get("dtype") or meta.get("inferred_type") or "unknown"
            ds_lineage[col_name] = {
                "source_dtype": src,
                "transforms": [],
                "target_dtype": src,
                "nullable": True,
            }

        steps = sorted(
            (block or {}).get("steps") or [],
            key=lambda x: int(x.get("order") or 0),
        )
        for st in steps:
            col = st.get("column")
            if not col:
                continue
            cname = str(col)
            action = str(st.get("action") or "")
            params = st.get("params") or {}
            
            # Identify if this step derives a clean companion column
            output_col = None
            if action in ("coerce_numeric", "parse_dates", "create_normalized_match_key"):
                output_col = params.get("output_column") or f"{cname}_clean"
            elif action == "cast_type" and params.get("output_column"):
                output_col = params.get("output_column")

            if output_col:
                src = ds_lineage.get(cname, {}).get("source_dtype") or "unknown"
                ds_lineage[output_col] = {
                    "source_dtype": src,
                    "transforms": [f"derive_{output_col}_via_{action}"],
                    "target_dtype": _infer_target_dtype(action, src),
                    "nullable": True,
                }
                if cname in ds_lineage:
                    ds_lineage[cname]["transforms"].append(f"{action}_to_{output_col}")
            else:
                if cname in ds_lineage:
                    ds_lineage[cname]["transforms"].append(action)
                    ds_lineage[cname]["target_dtype"] = _infer_target_dtype(
                        action, str(ds_lineage[cname].get("source_dtype") or "unknown")
                    )

        if ds_lineage:
            out[ds_name] = ds_lineage

    return out
