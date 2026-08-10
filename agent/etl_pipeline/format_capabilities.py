"""
format_capabilities.py - Registry of PySpark blob format capabilities and prerequisites.
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional

PYSPARK_FORMAT_CAPABILITIES: Dict[str, Dict[str, Any]] = {
    "csv":     {"supported": True,  "fabric_ready": True,  "requires_package": None},
    "tsv":     {"supported": True,  "fabric_ready": True,  "requires_package": None},
    "json":    {"supported": True,  "fabric_ready": True,  "requires_package": None},
    "parquet": {"supported": True,  "fabric_ready": True,  "requires_package": None},
    "xml":     {"supported": True,  "fabric_ready": False, "requires_package": "com.databricks:spark-xml_2.12:0.18.0"},
    "xlsx":    {"supported": True,  "fabric_ready": False, "requires_package": "com.crealytics:spark-excel_2.12:3.5.0_0.20.3"},
    "xls":     {"supported": True,  "fabric_ready": False, "requires_package": "com.crealytics:spark-excel_2.12:3.5.0_0.20.3"},
}

def get_capability(fmt: str) -> Optional[Dict[str, Any]]:
    return PYSPARK_FORMAT_CAPABILITIES.get(str(fmt).strip().lower())

def required_package_for_format(fmt: str) -> Optional[str]:
    cap = get_capability(fmt)
    return cap.get("requires_package") if cap else None

def is_fabric_ready(fmt: str) -> bool:
    cap = get_capability(fmt)
    return bool(cap and cap.get("fabric_ready"))

def get_fabric_hint(fmt: str) -> str:
    fmt_lower = str(fmt).strip().lower()
    if fmt_lower == "xml":
        return "Check options.row_tag and confirm spark-xml package (com.databricks:spark-xml) is attached to the Fabric environment."
    if fmt_lower in ("xlsx", "xls", "excel"):
        return "Check options.sheet_name and confirm spark-excel package (com.crealytics:spark-excel) is attached to the Fabric environment."
    if fmt_lower == "json":
        return "Check for nested/multiline structure. Use options.multiline: true if the file is a pretty-printed single JSON document."
    if fmt_lower in ("csv", "tsv"):
        return "Check column delimiter, header row presence, and schema inference."
    return "Check Lakehouse file path, file permissions, and dataset schema."
