from __future__ import annotations
import logging
from typing import Any, Dict
import pandas as pd

from agent.profiling.constants import *
from agent.profiling.statistical_profiling import safe_nunique

logger = logging.getLogger("agent.profiling.database_profiler")

def profile_database_table_full(
    connector: Any,
    table: str,
    df_sample: pd.DataFrame,
    job_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Run aggregate SELECT query in-place to profile 100% of database rows instead of downloading them.
    Exclude text/blob columns. Cast bit to int.
    """
    from agent.jobs_store import add_event
    if job_id:
        add_event(job_id=job_id, level="info", message=f"Performing in-database profiling for table: {table}")
        
    try:
        schema = connector.get_table_schema(table)
    except Exception as e:
        if job_id:
            add_event(job_id=job_id, level="warning", message=f"Failed to get table schema for database profiling: {e}")
        schema = [{"name": c, "type": "varchar", "nullable": "YES"} for c in df_sample.columns]

    if not schema:
        return {}

    unsafe_types = {"text", "ntext", "image", "xml", "geography", "geometry", "varbinary", "binary"}
    select_items = ["COUNT(*) AS [__total_rows__]"]
    profiled_cols = []
    
    for col in schema:
        col_name = col["name"]
        col_type = str(col.get("type", "varchar")).lower()
        if col_type in unsafe_types:
            continue
            
        profiled_cols.append((col_name, col_type))
        col_quoted = f"[{col_name}]"
        
        select_items.append(f"SUM(CASE WHEN {col_quoted} IS NULL THEN 1 ELSE 0 END) AS [{col_name}__null_cnt]")
        select_items.append(f"COUNT(DISTINCT {col_quoted}) AS [{col_name}__distinct_cnt]")
        
        if col_type == "bit":
            select_items.append(f"MIN(CAST({col_quoted} AS INT)) AS [{col_name}__min_val]")
            select_items.append(f"MAX(CAST({col_quoted} AS INT)) AS [{col_name}__max_val]")
        else:
            select_items.append(f"MIN({col_quoted}) AS [{col_name}__min_val]")
            select_items.append(f"MAX({col_quoted}) AS [{col_name}__max_val]")
            
    table_quoted = connector._quote_two_part_name(table)
    sql = f"SELECT {', '.join(select_items)} FROM {table_quoted}"
    
    try:
        res_df = connector.execute_select(sql)
        if res_df.empty:
            return {}
        row_data = res_df.iloc[0].to_dict()
    except Exception as e:
        if job_id:
            add_event(job_id=job_id, level="warning", message=f"In-database profiling SQL failed: {e}")
        return {}
        
    total_rows = int(row_data.get("__total_rows__", 0))
    schema_map = {col["name"].lower(): col for col in schema if "name" in col}
    db_profile = {
        "row_count": total_rows,
        "columns": {}
    }
    
    for col_name, col_type in profiled_cols:
        null_cnt = row_data.get(f"{col_name}__null_cnt")
        distinct_cnt = row_data.get(f"{col_name}__distinct_cnt")
        min_val = row_data.get(f"{col_name}__min_val")
        max_val = row_data.get(f"{col_name}__max_val")
        col_schema = schema_map.get(col_name.lower()) or {}
        
        try:
            null_cnt = int(null_cnt) if null_cnt is not None else 0
        except (ValueError, TypeError):
            null_cnt = 0
            
        try:
            distinct_cnt = int(distinct_cnt) if distinct_cnt is not None else 0
        except (ValueError, TypeError):
            distinct_cnt = 0
            
        null_pct = null_cnt / max(1, total_rows)
        is_cpk = (null_cnt == 0 and distinct_cnt == total_rows and total_rows > 0)
        
        db_profile["columns"][col_name] = {
            "null_count": null_cnt,
            "null_percentage": null_pct,
            "unique_count": distinct_cnt,
            "min": min_val,
            "max": max_val,
            "candidate_primary_key": is_cpk,
            "nullable": col_schema.get("nullable", "YES")
        }
        
    return db_profile

def merge_in_db_profile(sample_profile: Dict[str, Any], db_profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Overwrites statistical counts, bounds, and PK flags in the sample profile with full DB stats.
    """
    if not db_profile:
        return sample_profile
        
    sample_profile["row_count"] = db_profile.get("row_count", sample_profile.get("row_count", 0))
    sample_profile["sampling_info"] = f"Full dataset has {sample_profile['row_count']:,} rows. Statistics (nulls, min/max, uniqueness) profiled in-database on 100% of rows."
    
    db_cols = db_profile.get("columns") or {}
    sample_cols = sample_profile.setdefault("columns", {})
    
    for col_name, db_col_info in db_cols.items():
        if col_name not in sample_cols:
            sample_cols[col_name] = {}
        col_prof = sample_cols[col_name]
        
        col_prof["null_percentage"] = db_col_info.get("null_percentage", col_prof.get("null_percentage", 0.0))
        col_prof["unique_count"] = db_col_info.get("unique_count", col_prof.get("unique_count", 0))
        col_prof["candidate_primary_key"] = db_col_info.get("candidate_primary_key", col_prof.get("candidate_primary_key", False))
        
        if "min" in db_col_info:
            col_prof["min"] = db_col_info["min"]
        if "max" in db_col_info:
            col_prof["max"] = db_col_info["max"]
            
        if "null_count" in db_col_info:
            col_prof["null_count"] = db_col_info["null_count"]
            
    return sample_profile

