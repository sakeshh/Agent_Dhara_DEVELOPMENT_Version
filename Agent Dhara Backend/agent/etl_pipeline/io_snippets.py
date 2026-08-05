"""
Shared I/O path resolution and read/write snippet builders for ETL codegen.
"""
from __future__ import annotations

import os
import re
from typing import Any, Dict


def _escape_path(p: str) -> str:
    return str(p or "").replace("\\", "\\\\").replace('"', '\\"')


def resolve_path_python_helper() -> str:
    """Python helper emitted once at top of generated scripts using blob paths."""
    return '''
def _resolve_data_path(location: str) -> str:
    """Resolve blob/SQL/file path from connector manifest location."""
    import os
    loc = (location or "").strip()
    if not loc or loc == "unknown":
        raise ValueError("connector_manifest location is missing")
    low = loc.lower()
    if low.startswith(("abfss://", "wasbs://", "http" + "s://", "http" + "://", "s3://")):
        return loc
    base = os.environ.get("DHARA_BLOB_BASE_PATH") or os.environ.get("DHARA_BLOB_MOUNT") or "."
    account = os.environ.get("AZURE_STORAGE_ACCOUNT", "").strip()
    container = os.environ.get("DHARA_BLOB_CONTAINER", "").strip()
    if account and container and not os.path.isabs(loc):
        return f"abfss://{container}@{account}.dfs.core.windows.net/{loc.lstrip('/')}"
    return os.path.join(base, loc) if not os.path.isabs(loc) else loc

def _read_blob_pandas(location: str, account: str, container: str) -> pd.DataFrame:
    import os
    import io
    import pandas as pd
    from azure.storage.blob import BlobServiceClient
    key = os.environ.get("AZURE_STORAGE_KEY") or os.environ.get("AZURE_STORAGE_CONNECTION_STRING") or ""
    if "DefaultEndpointsProtocol" in key:
        client = BlobServiceClient.from_connection_string(key)
    else:
        client = BlobServiceClient(account_url=f"https://{account}.blob.core.windows.net", credential=key)
    with client.get_blob_client(container, location) as bc:
        data = bc.download_blob().readall()
        low = location.lower()
        if low.endswith(".json"):
            return pd.read_json(io.BytesIO(data))
        elif low.endswith((".xlsx", ".xls")):
            return pd.read_excel(io.BytesIO(data), sheet_name=0)
        elif low.endswith(".parquet"):
            return pd.read_parquet(io.BytesIO(data))
        elif low.endswith(".xml"):
            return pd.read_xml(io.BytesIO(data))
        else:
            return pd.read_csv(io.BytesIO(data))
'''.strip()


def resolve_path_pyspark_helper() -> str:
    return '''
def _resolve_data_path(location: str) -> str:
    import os
    loc = (location or "").strip()
    if not loc or loc == "unknown":
        raise ValueError("connector_manifest location is missing")
    low = loc.lower()
    if low.startswith(("abfss://", "wasbs://", "http" + "s://", "http" + "://")):
        return loc
    base = os.environ.get("DHARA_BLOB_BASE_PATH") or os.environ.get("DHARA_BLOB_MOUNT") or "."
    account = os.environ.get("AZURE_STORAGE_ACCOUNT", "").strip()
    container = os.environ.get("DHARA_BLOB_CONTAINER", "").strip()
    if account and container and not os.path.isabs(loc):
        return f"abfss://{container}@{account}.dfs.core.windows.net/{loc.lstrip('/')}"
    return os.path.join(base, loc) if not os.path.isabs(loc) else loc
'''.strip()


def resolve_path_fabric_pyspark_helper(workspace_id: Optional[str] = None, lakehouse_id: Optional[str] = None) -> str:
    """Path resolver for code running INSIDE a Fabric Spark Notebook."""
    return '''
def _resolve_data_path(location: str) -> str:
    loc = (location or "").strip()
    if not loc:
        raise ValueError("location is missing")
    if loc.startswith("abfss://"):
        return loc
    clean_loc = loc.lstrip("/")
    if not clean_loc.startswith(("Files/", "Tables/")):
        clean_loc = f"Files/{clean_loc}"
    return clean_loc
'''.strip()


def infer_format_from_ext(ext: str, source_type: str) -> str:
    ext = (ext or "").lower()
    if ext in (".csv",):
        return "csv"
    if ext in (".tsv",):
        return "tsv"
    if ext in (".parquet",):
        return "parquet"
    if ext in (".json", ".jsonl"):
        return "json"
    if ext in (".xml",):
        return "xml"
    if ext in (".xlsx",):
        return "xlsx"
    if ext in (".xls",):
        return "xls"
    if source_type in ("sql_server", "azure_sql", "postgres", "mysql"):
        return "sql_table"
    if source_type == "csv_file" or str(source_type).lower().startswith("csv"):
        return "csv"
    clean_ext = ext.lstrip(".")
    return clean_ext if clean_ext else "unknown"


def output_extension_for_format(fmt: str, fallback_ext: str) -> str:
    mapping = {
        "json": ".json",
        "xml": ".parquet",
        "csv": ".csv",
        "tsv": ".tsv",
        "parquet": ".parquet",
        "excel": ".parquet",
        "xlsx": ".parquet",
        "xls": ".parquet",
    }
    if fmt == "xml":
        return ".parquet"
    return mapping.get(fmt, fallback_ext or ".parquet")


def _get_pandas_blob_read_snippet(loc: str, conn: dict, fmt: str = "csv") -> str:
    account = conn.get("storage_account") or conn.get("account") or "ACCOUNT"
    container = conn.get("container_name") or conn.get("container") or "CONTAINER"
    comment = f"  # uses read_{fmt} internally" if fmt in ("xml", "json", "excel", "xlsx", "xls", "parquet") else ""
    return f'_read_blob_pandas("{loc}", "{account}", "{container}"){comment}'


def python_read_snippet(entry: Dict[str, Any]) -> str:
    loc = _escape_path(entry["location"])
    fmt = entry.get("format") or "csv"
    if entry.get("source_type") == "blob_storage":
        conn = entry.get("resolved_connection") or {}
        return _get_pandas_blob_read_snippet(loc, conn, fmt=fmt)

    if fmt == "sql_table":
        cref = entry.get("connection_ref") or "DHARA_SQL_CONNECTION_STRING"
        table = entry["location"]
        return (
            f'pd.read_sql("SELECT * FROM {table}", '
            f'create_engine(os.environ["{cref}"]))'
        )
    path_expr = f'_resolve_data_path("{loc}")'

    if fmt == "parquet":
        return f"pd.read_parquet({path_expr})"
    if fmt in ("excel", "xlsx", "xls"):
        return f"pd.read_excel({path_expr}, sheet_name=0)"
    if fmt == "json":
        return f"pd.read_json({path_expr})"
    if fmt == "xml":
        return (
            f"pd.read_xml({path_expr}, parser='lxml') "
            f"if 'read_xml' in dir(pd) else pd.read_xml({path_expr})"
        )
    if fmt == "tsv":
        return f"pd.read_csv({path_expr}, sep='\\t')"
    return f"pd.read_csv({path_expr})"


def python_write_snippet(entry: Dict[str, Any]) -> str:
    fmt = entry.get("format") or "csv"
    op = _escape_path(entry.get("output_path") or "cleaned/out.parquet")
    path_expr = f'r"{op}"'
    if fmt == "json":
        return f"df.to_json({path_expr}, orient='records', lines=True, index=False)"
    if fmt in ("xml", "parquet"):
        return f"df.to_parquet({path_expr}, index=False)"
    if fmt in ("excel", "xlsx", "xls"):
        return f"df.to_excel({path_expr}, index=False)"
    return f"df.to_csv({path_expr}, index=False)"


def pyspark_read_snippet(entry: Dict[str, Any]) -> str:
    from agent.etl_pipeline.format_readers import get_pyspark_read_snippet
    from agent.etl_pipeline.format_validators import validate_dataset_format_entry
    validate_dataset_format_entry(entry)
    loc = _escape_path(entry["location"])
    fmt = entry.get("format") or "csv"
    if fmt == "sql_table":
        cref = entry.get("connection_ref") or "DHARA_SQL_CONNECTION_STRING"
        table = entry["location"]
        return (
            'spark.read.format("jdbc").option("url", os.environ["'
            + cref
            + f'"]).option("dbtable", "{table}").load()'
        )
    path_expr = f'_resolve_data_path("{loc}")'
    return get_pyspark_read_snippet(path_expr, fmt, entry.get("options"))


def pyspark_write_snippet(entry: Dict[str, Any]) -> str:
    if entry.get("source_type") == "fabric_files_zone" or entry.get("execution_target") == "fabric":
        table_name = entry.get("clean_table_name") or "dataset_clean"
        return f'df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("{table_name}")'
        
    fmt = entry.get("format") or "csv"
    op = entry.get("output_path") or "cleaned/out.parquet"
    if fmt == "xml" and str(op).lower().endswith(".xml"):
        op = str(op)[:-4] + ".parquet"
    op_esc = _escape_path(op)
    path_expr = f'r"{op_esc}"'
    if fmt == "json":
        return f'df.write.mode("overwrite").json({path_expr})'
    if fmt == "parquet":
        return f'df.write.mode("overwrite").parquet({path_expr})'
    if fmt == "xml":
        return (
            f'# Note: Output format changed (XML -> Parquet, Spark cannot write XML natively)\n'
            f'df.write.mode("overwrite").parquet({path_expr})'
        )
    return f'df.write.mode("overwrite").option("header", "true").csv({path_expr})'


def pyspark_iqr_bounds_helper() -> str:
    return '''
def _iqr_bounds(df, col: str, multiplier: float = 1.5):
    """Return (stats_row, iqr, lower, upper) for outlier transforms."""
    row = df.select(
        F.percentile_approx(F.col(col), 0.25).alias("q1"),
        F.percentile_approx(F.col(col), 0.75).alias("q3"),
        F.percentile_approx(F.col(col), 0.50).alias("median"),
    ).first()
    if not row or row["q1"] is None or row["q3"] is None:
        return row, 0.0, -float('inf'), float('inf')
    iqr = float(row["q3"] - row["q1"])
    lower = float(row["q1"] - multiplier * iqr)
    upper = float(row["q3"] + multiplier * iqr)
    return row, iqr, lower, upper
'''.strip()


def pyspark_production_helpers() -> str:
    return r'''
def _normalize_column_names(df):
  """Strip whitespace and canonicalize column names to lowercase with duplicate checking."""
  norm_names = [c.strip().lower() for c in df.columns]
  dups = {n for n in norm_names if norm_names.count(n) > 1}
  if dups:
    raise ValueError(f"Column-name normalization would create duplicate columns: {sorted(dups)}")
  for orig, norm in zip(df.columns, norm_names):
    if orig != norm:
      df = df.withColumnRenamed(orig, norm)
  return df


def _require_columns(df, required: list, label: str) -> None:
  """Fail fast if required columns are missing."""
  existing_lower = [c.lower() for c in df.columns]
  missing = [c for c in required if c.lower() not in existing_lower]
  if missing:
    raise ValueError(f"{label}: missing required columns: {missing}")


def _parse_timestamp_safely(df, column_name: str):
  """Parse common timestamp formats into a materialized timestamp column with invalid flag tracking.
  
  ANSI-safe: values are parsed using try_to_timestamp (if available) or pre-filtered with a regex gate
  before to_timestamp() to avoid SparkDateTimeException on malformed strings like '00/00/0000', 'TBD',
  or empty sentinels.
  """
  raw_col = f"_raw_{column_name}"
  invalid_flag = f"is_invalid_{column_name}"
  df = df.withColumn(raw_col, F.trim(F.col(column_name).cast("string")))

  if hasattr(F, "try_to_timestamp"):
    parsed = F.coalesce(
      F.try_to_timestamp(F.col(raw_col), F.lit("yyyy-MM-dd HH:mm:ss")),
      F.try_to_timestamp(F.col(raw_col), F.lit("yyyy-MM-dd")),
      F.try_to_timestamp(F.col(raw_col), F.lit("MM/dd/yyyy")),
      F.try_to_timestamp(F.col(raw_col), F.lit("yyyy/MM/dd")),
      F.try_to_timestamp(F.col(raw_col), F.lit("dd/MM/yyyy")),
      F.try_to_timestamp(F.col(raw_col))
    )
  else:
    # Regex gate: only attempt parsing if value looks like a date/timestamp
    _date_like = (
      F.col(raw_col).rlike(r"^\d{4}-\d{2}-\d{2}") |           # yyyy-MM-dd[...]
      F.col(raw_col).rlike(r"^\d{2}/\d{2}/\d{4}") |           # MM/dd/yyyy or dd/MM/yyyy
      F.col(raw_col).rlike(r"^\d{4}/\d{2}/\d{2}")             # yyyy/MM/dd
    )

    parsed = F.when(
      _date_like,
      F.coalesce(
        F.to_timestamp(F.col(raw_col), 'yyyy-MM-dd HH:mm:ss'),
        F.to_timestamp(F.col(raw_col), 'yyyy-MM-dd'),
        F.to_timestamp(F.col(raw_col), 'MM/dd/yyyy'),
        F.to_timestamp(F.col(raw_col), 'yyyy/MM/dd'),
        F.to_timestamp(F.col(raw_col), 'dd/MM/yyyy'),
      )
    ).otherwise(F.lit(None).cast("timestamp"))

  df = df.withColumn(invalid_flag, (F.col(raw_col).isNotNull() & (F.col(raw_col) != '') & parsed.isNull()).cast("boolean"))
  df = df.withColumn(column_name, parsed)
  return df.drop(raw_col)


def _flag_source_duplicates(df):
  """Flag exact duplicate source row groups using _source_row_hash while preserving rows."""
  from pyspark.sql import Window as _DedupWindow
  if "_source_row_hash" in df.columns:
    _dup_w = _DedupWindow.partitionBy("_source_row_hash")
    df = df.withColumn("_duplicate_group_size", F.count(F.lit(1)).over(_dup_w))
    df = df.withColumn("_is_duplicate_row", (F.col("_duplicate_group_size") > 1).cast("boolean"))
  return df


def _warn_duplicate_keys(df, key_col: str, label: str) -> None:
  """Log possible duplicate join keys (single scan — approx distinct, no full shuffle)."""
  import logging
  if key_col not in df.columns:
    return
  row = (
    df.agg(
      F.count(F.col(key_col)).alias("_n"),
      F.approx_count_distinct(F.col(key_col)).alias("_d"),
    )
    .first()
  )
  if not row:
    return
  n, d = int(row["_n"] or 0), int(row["_d"] or 0)
  if n > 0 and d > 0 and n > d:
    logging.getLogger("agent_dhara").warning(
      "%s: ~%d possible duplicate key value(s) on %s (approx_count_distinct)",
      label,
      n - d,
      key_col,
    )


def _warn_nulls_in_columns(df, columns: list, label: str) -> None:
  """Cheap null probe: limit(1) per column — does not scan full table."""
  import logging
  for col in columns or []:
    if col not in df.columns:
      continue
    if df.filter(F.col(col).isNull()).limit(1).count() > 0:
      logging.getLogger("agent_dhara").warning(
        "%s: column %s has null values after transform", label, col
      )


def _log_row_count(df, label: str) -> None:
  """Log row count for audit trail."""
  import logging
  logging.getLogger("agent_dhara").info("%s: row_count=%s", label, df.count())
'''.strip()


def pyspark_prefix_non_key_columns_helper() -> str:
    return '''
def _prefix_columns(df, prefix: str, except_cols: list):
    """Prefix right-side columns before join to avoid duplicate names."""
    for c in df.columns:
        if c not in except_cols:
            df = df.withColumnRenamed(c, f"{prefix}_{c}")
    return df
'''.strip()
