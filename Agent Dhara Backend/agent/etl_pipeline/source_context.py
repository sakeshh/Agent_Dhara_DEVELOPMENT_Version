"""
Derive source_context for ETL engine recommendation from session + assessment.
Supports multi-dataset sessions via `sources` list (connector manifest is authoritative for I/O).
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional


def _ext(name: str) -> str:
    return os.path.splitext(name or "")[1].lower()


def _file_type_from_extension(ext: str) -> str:
    if ext in (".csv", ".tsv"):
        return "csv_file"
    if ext in (".xlsx", ".xls"):
        return "excel"
    if ext in (".json", ".jsonl"):
        return "json"
    if ext == ".parquet":
        return "parquet"
    if ext == ".xml":
        return "xml_file"
    return "csv_file"


def _totals_from_assessment(assessment: Dict[str, Any]) -> tuple[int, float]:
    row_count = 0
    for ds in (assessment.get("datasets") or {}).values():
        if isinstance(ds, dict):
            row_count = max(row_count, int(ds.get("row_count") or 0))
    size_mb = round(row_count * 0.0005, 2) if row_count > 0 else 0.0
    return row_count, size_mb


def _resolve_dataset_source(
    ds_name: str,
    ctx: Dict[str, Any],
    assessment: Dict[str, Any],
    *,
    selected: str,
) -> Dict[str, Any]:
    """Map one assessment dataset name to a source descriptor using SourceDescriptor model."""
    from agent.models import SourceDescriptor

    tables: List[str] = list(ctx.get("selected_tables") or [])
    blob_files: List[str] = list(ctx.get("selected_blob_files") or [])
    local_files: List[str] = list(ctx.get("selected_local_files") or [])
    local_root = str(ctx.get("local_files_root") or "").strip()
    ext = _ext(ds_name)

    loc_dict = {"type": "unknown", "path": ds_name}

    if ds_name in tables or (tables and ds_name.split(".")[-1] in tables):
        if "azure" in selected:
            stype = "database"
            conn = {"driver": "ODBC Driver 17 for SQL Server", "server": "azure_database.windows.net"}
        elif "postgres" in selected:
            stype = "database"
            conn = {"driver": "PostgreSQL Unicode", "server": "localhost"}
        elif "mysql" in selected:
            stype = "database"
            conn = {"driver": "MySQL ODBC", "server": "localhost"}
        else:
            stype = "database"
            conn = {"driver": "ODBC Driver 17 for SQL Server", "server": "localhost"}
        loc_dict = {"type": stype, "connection": conn, "path": ds_name}
    elif ds_name in blob_files:
        loc_dict = {"type": "azure_blob", "path": ds_name}
    elif ds_name in local_files:
        loc_dict = {"type": "filesystem", "path": local_root}
    elif blob_files and len(blob_files) == len((assessment.get("datasets") or {})):
        ds_names = list((assessment.get("datasets") or {}).keys())
        if ds_name in ds_names:
            idx = ds_names.index(ds_name)
            if idx < len(blob_files):
                loc_dict = {"type": "azure_blob", "path": blob_files[idx]}
    elif local_files and len(local_files) == len((assessment.get("datasets") or {})):
        ds_names = list((assessment.get("datasets") or {}).keys())
        if ds_name in ds_names:
            idx = ds_names.index(ds_name)
            if idx < len(local_files):
                loc_dict = {"type": "filesystem", "path": local_root}
    else:
        if ext in (".csv", ".tsv", ".parquet", ".json", ".xlsx", ".xls"):
            loc_dict = {"type": "filesystem", "path": local_root}
        elif "abfss://" in ds_name.lower():
            loc_dict = {"type": "azure_blob", "path": ds_name}
        else:
            loc_dict = {"type": "filesystem", "path": local_root}

    descriptor = SourceDescriptor.from_location_dict(loc_dict, ds_name)
    ds_meta = (assessment.get("datasets") or {}).get(ds_name) or {}
    descriptor.row_count = int(ds_meta.get("row_count") or 0)
    descriptor.size_mb = round(descriptor.row_count * 0.0005, 2) if descriptor.row_count > 0 else 0.0

    res = descriptor.model_dump()
    res.update({
        "dataset": descriptor.dataset_name,
        "type": descriptor.source_type.value.lower() if hasattr(descriptor.source_type, 'value') else str(descriptor.source_type).lower(),
        "location": descriptor.physical_location,
    })

    # Keep backward-compatible type mappings
    if descriptor.source_type in ("LOCAL_CSV", "BLOB_CSV"):
        res["type"] = "csv_file"
    elif descriptor.source_type in ("LOCAL_PARQUET", "BLOB_PARQUET"):
        res["type"] = "parquet"
    elif descriptor.source_type in ("LOCAL_JSON", "BLOB_JSON"):
        res["type"] = "json"
    elif descriptor.source_type == "LOCAL_EXCEL":
        res["type"] = "excel"
    elif descriptor.source_type in ("AZURE_SQL", "SQL_SERVER", "POSTGRES", "MYSQL"):
        res["type"] = descriptor.source_type.value.lower()

    return res


def _build_sources_list(
    ctx: Dict[str, Any],
    assessment: Dict[str, Any],
) -> List[Dict[str, Any]]:
    selected = str(ctx.get("selected_source") or "").lower().strip()
    ds_names = list((assessment.get("datasets") or {}).keys())
    return [_resolve_dataset_source(ds, ctx, assessment, selected=selected) for ds in ds_names]


def build_source_context(
    session_context: Optional[Dict[str, Any]],
    assessment: Dict[str, Any],
    override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build source_context for planner / codegen.
    override wins for explicit API fields; session fills gaps.
    Always includes `sources` (per-dataset) when assessment has datasets.
    """
    ctx = session_context or {}
    ovr = override or {}
    sources = _build_sources_list(ctx, assessment)
    row_count, size_mb = _totals_from_assessment(assessment)

    # Auto-detect Fabric Lakehouse workspace context
    workspace_id = os.getenv("FABRIC_WORKSPACE_ID") or ctx.get("workspace_id")
    lakehouse_id = os.getenv("FABRIC_LAKEHOUSE_NAME") or os.getenv("FABRIC_LAKEHOUSE_ID") or ctx.get("lakehouse_id")
    
    target_platform = ctx.get("target_platform") or ovr.get("target_platform")
    if not target_platform and (workspace_id or lakehouse_id):
        target_platform = "fabric_lakehouse"
        
    target_format = ctx.get("target_format") or ovr.get("target_format")
    if not target_format and target_platform == "fabric_lakehouse":
        target_format = "delta"

    # Derive dataset and relationship counts
    rel = assessment.get("relationships") or []
    if isinstance(rel, dict):
        joins = rel.get("joins") or rel.get("dependencies") or []
    else:
        joins = list(rel)
    rel_count = len(joins)

    base_context = {
        "target_platform": target_platform,
        "target_format": target_format,
        "dataset_count": len(sources) if sources else len(assessment.get("datasets") or {}),
        "relationship_count": rel_count,
    }

    if ovr.get("type"):
        base = dict(ovr)
        base.setdefault("row_count", row_count)
        base.setdefault("size_mb", size_mb)
        if not base.get("extension") and base.get("location"):
            base["extension"] = _ext(str(base["location"]))
        if sources:
            base["sources"] = sources
            base["source_count"] = len(sources)
            base["is_multi_source"] = len(sources) > 1
        for k, v in base_context.items():
            if v is not None:
                base.setdefault(k, v)
        return base

    if sources:
        primary = sources[0]
        types = {s["type"] for s in sources}
        mix = "mixed" if len(types) > 1 else (primary["type"] if primary else "unknown")
        res = {
            "type": primary["type"],
            "location": primary["location"],
            "size_mb": size_mb,
            "row_count": row_count,
            "extension": primary.get("extension") or "",
            "sources": sources,
            "source_count": len(sources),
            "is_multi_source": len(sources) > 1,
            "source_mix": mix,
        }
        res.update(base_context)
        res.update(ovr)
        return res

    res = {
        "type": "unknown",
        "location": "unknown",
        "size_mb": size_mb,
        "row_count": row_count,
        "extension": "",
        "sources": [],
        "source_count": 0,
        "is_multi_source": False,
    }
    res.update(base_context)
    res.update(ovr)
    return res
