"""
Deterministic file readers dispatcher for PySpark ETL.
"""
from __future__ import annotations
from typing import Any, Dict

def read_csv(path_expr: str, options: Dict[str, Any] | None = None) -> str:
    return f"spark.read.option('header', 'true').option('inferSchema', 'true').csv({path_expr})"

def read_tsv(path_expr: str, options: Dict[str, Any] | None = None) -> str:
    return f"spark.read.option('header', 'true').option('inferSchema', 'true').option('delimiter', '\\t').csv({path_expr})"

def read_json(path_expr: str, options: Dict[str, Any] | None = None) -> str:
    opts = options or {}
    # Fabric JSON files are typically multi-line arrays (e.g. [{...}, {...}]).
    # Plain spark.read.json() treats each line as a separate record (newline-delimited JSON).
    # Default to multiLine=True so array-format files are read correctly in all cases.
    multiline = opts.get("multiline") if opts.get("multiline") is not None else opts.get("multi_line", True)
    if multiline:
        return f'spark.read.option("multiLine", "true").json({path_expr})'
    return f'spark.read.json({path_expr})'



def read_parquet(path_expr: str, options: Dict[str, Any] | None = None) -> str:
    return f"spark.read.parquet({path_expr})"

def read_xml(path_expr: str, row_tag: str = "row", infer_schema: bool = True) -> str:
    infer = "true" if infer_schema else "false"
    return (
        f'spark.read.format("com.databricks.spark.xml")'
        f'.option("rowTag", "{row_tag}")'
        f'.option("inferSchema", "{infer}")'
        f'.load({path_expr})'
    )

def read_excel(path_expr: str, options: Dict[str, Any] | None = None) -> str:
    opts = options or {}
    sheet = opts.get("sheet_name") if opts.get("sheet_name") is not None else opts.get("sheet", 0)
    header = str(opts.get("header", "true")).lower()
    infer = str(opts.get("inferSchema", "true")).lower()
    return (
        f'spark.read.format("com.crealytics.spark.excel")'
        f'.option("sheetName", "{sheet}")'
        f'.option("header", "{header}")'
        f'.option("inferSchema", "{infer}")'
        f'.load({path_expr})'
    )

DISPATCH = {
    "csv": read_csv,
    "tsv": read_tsv,
    "json": read_json,
    "parquet": read_parquet,
    "xml": read_xml,
    "xlsx": read_excel,
    "xls": read_excel,
}

def get_pyspark_read_snippet(path_expr: str, file_format: str, options: Dict[str, Any] | None = None) -> str:
    fmt = str(file_format).strip().lower()
    options = options or {}
    
    if fmt == "xml":
        row_tag = options.get("row_tag") or options.get("rowTag") or "row"
        infer_schema = bool(options.get("inferSchema", True))
        return read_xml(path_expr, row_tag=row_tag, infer_schema=infer_schema)
        
    dispatcher = DISPATCH.get(fmt)
    if dispatcher:
        if fmt in ("xlsx", "xls", "json"):
            return dispatcher(path_expr, options=options)
        return dispatcher(path_expr)
        
    from agent.etl_pipeline.format_capabilities import PYSPARK_FORMAT_CAPABILITIES
    raise ValueError(
        f"Unsupported PySpark blob format: '{fmt}'. "
        f"Supported formats: {sorted(PYSPARK_FORMAT_CAPABILITIES.keys())}"
    )
