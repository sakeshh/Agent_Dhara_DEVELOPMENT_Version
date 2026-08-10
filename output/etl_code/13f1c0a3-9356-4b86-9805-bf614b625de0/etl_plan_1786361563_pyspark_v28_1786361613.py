"""
plan_id: plan_1786361563
"""
from typing import Optional, Dict
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
import logging

DATASETS = ['factory.json']

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

logger = logging.getLogger("agent_dhara")

def _resolve_data_path(location: str) -> str:
    import os
    loc = (location or "").strip()
    if not loc:
        raise ValueError("location is missing")
    if loc.startswith(("abfss://", "wasbs://", "http://", "https://")):
        return loc
    clean_loc = loc.lstrip("/")
    filename = clean_loc.split("/")[-1]

    # Configure Azure Storage ABFSS key if environment provides credentials
    try:
        _acc = os.environ.get("AZURE_STORAGE_ACCOUNT", "").strip()
        _key = os.environ.get("AZURE_STORAGE_KEY", "").strip() or os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "").strip()
        if _acc and _key and "DefaultEndpointsProtocol" not in _key and "spark" in globals():
            spark.conf.set(f"fs.azure.account.key.{_acc}.dfs.core.windows.net", _key)
    except Exception:
        pass

    candidates = []
    if clean_loc.startswith(("Files/", "Tables/")):
        candidates.append(clean_loc)
        if not clean_loc.startswith("Files/raw/"):
            candidates.append(f"Files/raw/{clean_loc.replace('Files/', '', 1)}")
    else:
        candidates.extend([
            f"Files/{clean_loc}",
            f"Files/raw/{clean_loc}",
            f"Files/landing/{clean_loc}",
            clean_loc
        ])
    if filename and filename != clean_loc:
        candidates.extend([
            f"Files/{filename}",
            f"Files/raw/{filename}"
        ])

    try:
        from notebookutils import mssparkutils  # type: ignore
        for cand in candidates:
            try:
                if mssparkutils.fs.exists(cand):
                    return cand
            except Exception:
                pass
    except Exception:
        pass

    for cand in candidates:
        if os.path.exists(cand):
            return cand

    return candidates[0] if candidates else clean_loc
    clean_loc = loc.lstrip("/")
    filename = clean_loc.split("/")[-1]

    # Configure Azure Storage ABFSS key if environment provides credentials
    try:
        _acc = os.environ.get("AZURE_STORAGE_ACCOUNT", "").strip()
        _key = os.environ.get("AZURE_STORAGE_KEY", "").strip() or os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "").strip()
        if _acc and _key and "DefaultEndpointsProtocol" not in _key and "spark" in globals():
            spark.conf.set(f"fs.azure.account.key.{_acc}.dfs.core.windows.net", _key)
    except Exception:
        pass

    candidates = []
    if clean_loc.startswith(("Files/", "Tables/")):
        candidates.append(clean_loc)
        if not clean_loc.startswith("Files/raw/"):
            candidates.append(f"Files/raw/{clean_loc.replace('Files/', '', 1)}")
    else:
        candidates.extend([
            f"Files/{clean_loc}",
            f"Files/raw/{clean_loc}",
            f"Files/landing/{clean_loc}",
            clean_loc
        ])
    if filename and filename != clean_loc:
        candidates.extend([
            f"Files/{filename}",
            f"Files/raw/{filename}"
        ])

    try:
        from notebookutils import mssparkutils  # type: ignore
        for cand in candidates:
            try:
                if mssparkutils.fs.exists(cand):
                    return cand
            except Exception:
                pass
    except Exception:
        pass

    for cand in candidates:
        if os.path.exists(cand):
            return cand

    return candidates[0] if candidates else clean_loc

def _prefix_columns(df, prefix, except_cols):
    for col in df.columns:
        if col not in except_cols:
            df = df.withColumnRenamed(col, f'{prefix}_{col}')
    return df

def transform_factory_json(df: DataFrame, all_dfs: Optional[dict] = None) -> DataFrame:
    df = _normalize_column_names(df)
    _require_columns(df, ['plant', 'product_id', 'production_id', 'sales_id', 'units_produced'], 'factory.json')
    _src_cols = [F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in df.columns if not c.startswith('_') and not c.startswith('is_')]
    df = df.withColumn('_source_row_hash', F.xxhash64(*_src_cols))
    df = df.withColumn('is_missing_product_id', F.col('product_id').isNull() | (F.trim(F.col('product_id')) == ''))
    df = df.withColumn('is_missing_production_id', F.col('production_id').isNull() | (F.trim(F.col('production_id')) == ''))
    df = df.withColumn('is_missing_sales_id', F.col('sales_id').isNull() | (F.trim(F.col('sales_id')) == ''))
    # Step: trim on [plant]
    df = df.withColumn("plant", F.trim(F.col("plant")))
    
    # Step: trim on [product_id]
    df = df.withColumn("product_id", F.trim(F.col("product_id")))
    
    # Step: trim on [production_id]
    df = df.withColumn("production_id", F.trim(F.col("production_id")))
    
    # Step: trim on [sales_id]
    df = df.withColumn("sales_id", F.trim(F.col("sales_id")))
    
    # Step: coerce_numeric on [units_produced]
    df = df.withColumn("units_produced_clean", F.when(F.trim(F.col("units_produced")).rlike(r'^-?(?:\d+(?:\.\d+)?|\.\d+)$'), F.trim(F.col("units_produced")).cast("decimal(18,2)")).otherwise(F.lit(None).cast("decimal(18,2)")))
    df = df.withColumn("is_imputed_units_produced", F.col("units_produced").isNull().cast("boolean"))
    
    # Step: flag_outliers on [units_produced]
    _stats, _iqr, _lower, _upper = _iqr_bounds(df, 'units_produced_clean')
    df = df.withColumn('units_produced_outlier_flagged', (F.col('units_produced_clean') < _lower) | (F.col('units_produced_clean') > _upper))
    
    return df

def _iqr_bounds(df, col):
    # Calculate IQR bounds for outlier detection
    q1 = df.approxQuantile(col, [0.25], 0.01)[0]
    q3 = df.approxQuantile(col, [0.75], 0.01)[0]
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return (q1, iqr, lower_bound, upper_bound)

# - OUTPUT CONTRACT -
#   factory_json: json -> Tables/factory_json_clean
OUTPUT_PATHS: dict = {}

def run_pipeline(spark):
    dfs = {}
    def _canonical_id(name: str) -> str:
        import re
        return re.sub(r"[^0-9a-zA-Z_]+", "_", name or "x").strip("_")

    active_canons = {_canonical_id(d) for d in DATASETS}
    # Load (connector manifest - use _resolve_data_path for blob/SQL)
    # factory_json: fabric_files_zone (json) @ Files/raw/factory_json.json
    if "factory_json" in active_canons:
        dfs["factory_json"] = spark.read.option("multiLine", "true").json(_resolve_data_path("Files/raw/factory_json.json"))
    if "factory_json" in dfs:
        dfs["factory_json"] = transform_factory_json(dfs["factory_json"], dfs)
        _log_row_count(dfs["factory_json"], "factory_json")
    # Write cleaned per-dataset outputs (format matches manifest; XML -> parquet)
    if "factory_json" in dfs:
        _out = 'Tables/factory_json_clean'  # Fabric managed table (audit path)
        OUTPUT_PATHS['factory_json'] = _out
        logger.info('Writing %s -> Fabric staging table %s', 'factory_json', 'factory_json_clean_staging')
        dfs["factory_json"].write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("factory_json_clean_staging")
    
    return dfs, OUTPUT_PATHS

# Fabric notebooks pre-inject 'spark'; fall back to creating one for local dev.
try:
    spark  # type: ignore[name-defined]
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("AgentDharaETL").getOrCreate()
_dfs, _paths = run_pipeline(spark)