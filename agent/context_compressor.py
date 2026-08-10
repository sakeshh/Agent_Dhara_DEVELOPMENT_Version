from __future__ import annotations
from typing import Any, Dict

def compress_assessment_for_llm(
    assessment: Dict[str, Any],
    max_issues_per_dataset: int = 10,
    max_columns_per_dataset: int = 30
) -> Dict[str, Any]:
    """
    Compresses a full data assessment dictionary to be suitable for LLM context windows.
    Filters and limits dataset metadata, columns, quality issues, and cross-dataset relationships.
    """
    if not isinstance(assessment, dict):
        return {}

    compressed = {
        "datasets": {},
        "relationships": [],
        "global_issues": {}
    }

    # 1. Compress datasets and columns
    datasets = assessment.get("datasets") or {}
    for ds_name, ds_meta in datasets.items():
        if not isinstance(ds_meta, dict):
            continue
        
        ds_summary = {
            "row_count": ds_meta.get("row_count"),
            "column_count": ds_meta.get("column_count"),
            "drift_score": ds_meta.get("drift_score"),
            "etl_readiness_score": ds_meta.get("etl_readiness_score"),
            "columns": {}
        }
        
        # Keep a capped list of columns to save tokens
        cols = ds_meta.get("columns") or {}
        for col_name, col_meta in list(cols.items())[:max_columns_per_dataset]:
            if not isinstance(col_meta, dict):
                continue
            ds_summary["columns"][col_name] = {
                "data_type": col_meta.get("data_type"),
                "semantic_type": col_meta.get("semantic_type"),
                "null_percentage": col_meta.get("null_percentage"),
                "unique_count": col_meta.get("unique_count")
            }
            
        # Keep a capped list of quality issues
        quality = ds_meta.get("quality") or {}
        issues = quality.get("issues") or []
        compressed_issues = []
        for iss in issues[:max_issues_per_dataset]:
            if not isinstance(iss, dict):
                continue
            compressed_issues.append({
                "type": iss.get("type") or iss.get("issue_type"),
                "column": iss.get("column"),
                "severity": iss.get("severity"),
                "count": iss.get("count"),
                "message": iss.get("message")
            })
            
        ds_summary["quality"] = {
            "summary": quality.get("summary") or {},
            "issues": compressed_issues
        }
        
        compressed["datasets"][ds_name] = ds_summary

    # 2. Compress relationships
    rels = assessment.get("relationships") or []
    for rel in rels[:20]:
        if not isinstance(rel, dict):
            continue
        compressed["relationships"].append({
            "parent_dataset": rel.get("parent_dataset"),
            "parent_column": rel.get("parent_column"),
            "child_dataset": rel.get("child_dataset"),
            "child_column": rel.get("child_column"),
            "cardinality": rel.get("cardinality")
        })

    # 3. Compress global issues
    dq_issues = assessment.get("data_quality_issues") or {}
    global_issues = dq_issues.get("global_issues") or {}
    compressed["global_issues"] = {
        "orphan_foreign_keys": (global_issues.get("orphan_foreign_keys") or [])[:10],
        "relationship_warnings": (global_issues.get("relationship_warnings") or [])[:10],
        "cross_dataset_consistency": (global_issues.get("cross_dataset_consistency") or [])[:10]
    }

    return compressed
