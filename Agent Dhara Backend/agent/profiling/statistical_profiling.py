from __future__ import annotations
import concurrent.futures
import json
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import numpy as np

from agent.profiling.constants import *
from agent.profiling.type_inference import detect_semantic_type, _dtype_inference_for_object, _is_text_dtype

def _to_key(x: Any) -> Any:
    """Convert list/dict/unhashable objects into stable strings for hashing."""
    try:
        hash(x)
        return x
    except Exception:
        try:
            return json.dumps(x, sort_keys=True, ensure_ascii=False, default=str)
        except Exception:
            return repr(x)

def safe_nunique(series: pd.Series) -> int:
    """Safe nunique even when values are lists/dicts/objects."""
    try:
        if len(series) > SAMPLING_THRESHOLD:
            # For very large datasets, we estimate or use a sample to avoid OOM/freeze.
            sample = series.dropna()
            if len(sample) > DEFAULT_SAMPLE_SIZE:
                sample = sample.sample(DEFAULT_SAMPLE_SIZE, random_state=42)
            return int(sample.map(_to_key).nunique(dropna=True))
        return int(series.nunique(dropna=True))
    except Exception:
        # Fallback for unhashable types (list, dict).
        if len(series) > SAMPLING_THRESHOLD:
            sample = series.dropna().sample(DEFAULT_SAMPLE_SIZE, random_state=42)
            return int(sample.map(_to_key).nunique(dropna=True))
        return int(series.dropna().map(_to_key).nunique(dropna=True))

def safe_is_unique(series: pd.Series) -> bool:
    """Safe uniqueness check on unhashables."""
    try:
        if len(series) > SAMPLING_THRESHOLD:
            # Check if nulls exist first (fast)
            if series.isna().any():
                return False
            sample = series.sample(DEFAULT_SAMPLE_SIZE, random_state=42)
            return bool(sample.map(_to_key).is_unique)
        return bool(series.is_unique and series.notna().all())
    except Exception:
        # Fallback for unhashable
        if len(series) > SAMPLING_THRESHOLD:
            # If it's a large unhashable column, it's very unlikely to be a PK candidate 
            # if it contains complex objects. We'll check a sample.
            sample = series.dropna().sample(DEFAULT_SAMPLE_SIZE, random_state=42)
            coerced = sample.map(_to_key)
            return bool(coerced.is_unique and series.notna().all())
        coerced = series.map(_to_key)
        return bool(coerced.is_unique and series.notna().all())

def _strip(x: Any) -> Any:
    return x.strip() if isinstance(x, str) else x

def scalar_type_distribution(series: pd.Series, max_sample: int = 2000) -> Dict[str, Any]:
    """
    Summarize Python scalar types present in a column.
    Useful for JSON-loaded datasets where pandas dtype is 'object' but values mix int/str/etc.
    """
    try:
        s = series.dropna()
    except Exception:
        s = series
    if len(s) > max_sample:
        try:
            s = s.sample(max_sample, random_state=42)
        except Exception:
            s = s.head(max_sample)

    counts: Dict[str, int] = {
        "str": 0,
        "int": 0,
        "float": 0,
        "bool": 0,
        "dict": 0,
        "list": 0,
        "other": 0,
    }
    total = 0
    for v in s.tolist():
        if v is None:
            continue
        total += 1
        if isinstance(v, bool):
            counts["bool"] += 1
        elif isinstance(v, int):
            counts["int"] += 1
        elif isinstance(v, float):
            counts["float"] += 1
        elif isinstance(v, str):
            counts["str"] += 1
        elif isinstance(v, dict):
            counts["dict"] += 1
        elif isinstance(v, list):
            counts["list"] += 1
        else:
            counts["other"] += 1

    pct = {k: (counts[k] / total if total else 0.0) for k in counts}
    return {"counts": counts, "pct": pct, "sample_size": int(total)}

def select_top_priority_columns(df: pd.DataFrame, approved_semantics: Optional[Dict[str, str]] = None, top_n: int = 15) -> List[str]:
    """
    Select the top-N priority columns for deep profiling based on heuristics:
    1. Key identifiers (ID, Key, Code, Ref, PK in name)
    2. Date/Datetime columns
    3. Email/Phone/UUID semantic types (or in name)
    4. Numeric metric columns
    5. Fallback: all other columns
    """
    col_scores = []
    for col in df.columns:
        col_lower = str(col).lower()
        score = 0
        
        # Primary Key/Key identifiers
        is_key = any(x in col_lower for x in ("id", "key", "code", "ref", "pk"))
        # Email/Phone/UUID
        is_contact_or_uuid = any(x in col_lower for x in ("email", "phone", "mobile", "contact", "tel", "uuid", "guid", "uid"))
        # Date/Datetime
        is_date = any(x in col_lower for x in ("date", "time", "dt", "created", "updated", "_at"))
        
        # Approved semantic override
        approved_tag = (approved_semantics or {}).get(col, "").lower() if approved_semantics else ""
        
        if approved_tag in ("id", "pk"):
            score = 10
        elif is_key:
            score = 9
        elif approved_tag in ("email", "phone", "uuid") or is_contact_or_uuid:
            score = 8
        elif approved_tag in ("date", "datetime") or is_date:
            score = 7
        elif approved_tag == "metric" or pd.api.types.is_numeric_dtype(df[col]):
            score = 6
        else:
            score = 1
            
        col_scores.append((score, col))
        
    # Sort descending by score, stable sorting (maintains original order for same score)
    col_scores.sort(key=lambda x: x[0], reverse=True)
    return [col for _, col in col_scores[:top_n]]

def profile_dataframe(
    df: pd.DataFrame,
    job_id: Optional[str] = None,
    thresholds: Optional[Dict[str, Any]] = None,
    approved_semantics: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Returns a consistent profiling dictionary for a DataFrame, including:
    - row_count, column_count, data_volume_bytes
    - columns: { col: { dtype, dtype_inference?, null_percentage, unique_count, semantic_type, candidate_primary_key }}
    """
    from agent.jobs_store import add_event
    if job_id:
        add_event(job_id=job_id, level="info", message="Profiling columns...")
    row_count = int(len(df))
    col_count = int(len(df.columns))

    # Fast memory usage estimate for large DataFrames
    if row_count > SAMPLING_THRESHOLD:
        # Shallow usage
        shallow = df.memory_usage(deep=False).sum()
        # Estimate deep overhead by sampling object columns
        obj_cols = df.select_dtypes(include=["object"]).columns
        deep_overhead = 0
        if not obj_cols.empty:
            sample_size = min(row_count, DEFAULT_SAMPLE_SIZE // 10) # Smaller sample for memory estimate
            sample = df[obj_cols].sample(sample_size, random_state=42)
            # Subtract shallow size of the sample to get deep overhead
            deep_sample = sample.memory_usage(deep=True).sum()
            shallow_sample = sample.memory_usage(deep=False).sum()
            overhead_per_row = (deep_sample - shallow_sample) / sample_size
            deep_overhead = overhead_per_row * row_count
        data_volume_bytes = int(shallow + deep_overhead)
    else:
        data_volume_bytes = int(df.memory_usage(deep=True).sum())

    ext = thresholds.get("extended_checks") or {} if thresholds else {}
    top_n = int(ext.get("top_n_priority_cols", 15))
    priority_cols = set(select_top_priority_columns(df, approved_semantics, top_n))

    profile: Dict[str, Any] = {
        "row_count": row_count,
        "column_count": col_count,
        "data_volume_bytes": data_volume_bytes,
        "sampling_info": f"Full dataset has {row_count:,} rows. Analysis performed on a representative sample of {min(row_count, SAMPLING_THRESHOLD):,} rows." if row_count > SAMPLING_THRESHOLD else "Analysis performed on 100% of rows.",
        "columns": {},
        "priority_columns": list(priority_cols),
    }

    def profile_col(col: str) -> Tuple[str, Dict[str, Any]]:
        s = df[col]
        dtype_str = str(s.dtype)
        semantic = detect_semantic_type(s, col)
        
        is_priority = (col in priority_cols)
        
        if is_priority:
            hint = _dtype_inference_for_object(s) if _is_text_dtype(dtype_str) else None
            if semantic == "numeric_id" and hint == "datetime_like":
                hint = "numeric_like"
            type_dist = scalar_type_distribution(s) if _is_text_dtype(dtype_str) else None
        else:
            hint = None
            type_dist = None

        raw_smp = s.dropna().head(20).astype(str).tolist()

        null_pct = float(s.isna().mean())
        null_count = int(s.isna().sum())
        type_confidence = 0.92 if hint else (0.78 if _is_text_dtype(dtype_str) else 0.95)

        col_profile = {
            "dtype": dtype_str,
            "dtype_inference": hint,
            "type_distribution": type_dist,
            "null_percentage": null_pct,
            "null_count": null_count,
            "type_confidence": round(type_confidence, 3),
            "unique_count": safe_nunique(s),
            "semantic_type": semantic,
            "candidate_primary_key": safe_is_unique(s),
            "raw_samples": raw_smp,
        }
        
        if is_priority:
            n_nonnull = int(s.notna().sum())
            if n_nonnull > 0:
                dupes = int(s.duplicated(keep=False).sum())
                if dupes > 0:
                    col_profile["duplicate_value_count"] = dupes
                try:
                    num = pd.to_numeric(s, errors="coerce")
                    nn = num.dropna()
                    if len(nn) >= 3:
                        col_profile["mean"] = float(nn.mean())
                        col_profile["median"] = float(nn.median())
                        col_profile["std"] = float(nn.std())
                        if len(nn) >= 8:
                            sk = float(nn.skew())
                            col_profile["skew"] = round(sk, 4)
                            col_profile["p5"] = float(nn.quantile(0.05))
                            col_profile["p95"] = float(nn.quantile(0.95))
                except Exception:
                    pass
        return col, col_profile

    # Parallelize column profiling for speed on large datasets
    processed_cols = 0
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(profile_col, col): col for col in df.columns}
        for future in concurrent.futures.as_completed(futures):
            col, col_prof = future.result()
            profile["columns"][col] = col_prof
            processed_cols += 1
            if job_id and col_count > 0:
                pct = int((processed_cols / col_count) * 40) # 0-40% for profiling
                overall_pct = 20 + pct
                try:
                    from agent.jobs_store import update_job_progress
                    update_job_progress(job_id, overall_pct)
                except Exception:
                    pass
                add_event(job_id=job_id, level="info", message=f"Profiling: {pct}% complete")

    return profile

