"""
plan_id: plan_1786359237
"""
from pyspark.sql.window import Window

from typing import Optional, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging

DATASETS = ['marketing.csv']

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
    loc = (location or "").strip()
    if not loc:
        raise ValueError("location is missing")
    if loc.startswith("abfss://"):
        return loc
    clean_loc = loc.lstrip("/")
    if not clean_loc.startswith(("Files/", "Tables/")):
        clean_loc = f"Files/{clean_loc}"
    return clean_loc
    clean_loc = loc.lstrip("/")
    if not clean_loc.startswith(("Files/", "Tables/")):
        clean_loc = f"Files/{clean_loc}"
    return clean_loc

def _prefix_columns(df, prefix, except_cols):
    for col in df.columns:
        if col not in except_cols:
            df = df.withColumnRenamed(col, f'{prefix}_{col}')
    return df

def transform_marketing_csv(df: DataFrame, all_dfs: Optional[dict] = None) -> DataFrame:
    df = _normalize_column_names(df)
    _require_columns(df, ['campaign_id', 'channel', 'product_id', 'ad_spend', 'campaign_date'], 'marketing.csv')
    _src_cols = [F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in df.columns if not c.startswith('_') and not c.startswith('is_')]
    df = df.withColumn('_source_row_hash', F.xxhash64(*_src_cols))
    df = df.withColumn('is_missing_campaign_id', F.col('campaign_id').isNull() | (F.trim(F.col('campaign_id')) == ''))
    df = df.withColumn('is_missing_product_id', F.col('product_id').isNull() | (F.trim(F.col('product_id')) == ''))
    # Step: trim on [campaign_id]
    df = df.withColumn("campaign_id", F.trim(F.col("campaign_id")))
    
    # Step: trim on [channel]
    df = df.withColumn("channel", F.trim(F.col("channel")))
    
    # Step: trim on [product_id]
    df = df.withColumn("product_id", F.trim(F.col("product_id")))
    
    # Step: coerce_numeric on [ad_spend]
    df = df.withColumn("ad_spend_clean", F.when(F.trim(F.col("ad_spend")).rlike(r'^-?(?:\d+(?:\.\d+)?|\.\d+)$'), F.trim(F.col("ad_spend")).cast("decimal(18,2)")).otherwise(F.lit(None).cast("decimal(18,2)")))
    
    # Step: parse_dates on [campaign_date]
    df = df.withColumn("campaign_date_clean", F.coalesce(F.try_to_timestamp(F.col("campaign_date")), F.to_date(F.col("campaign_date"))))
    
    # Step: flag_duplicate_candidates on [Row-level]
    df = df.withColumn("_is_duplicate_row", F.row_number().over(Window.partitionBy("product_id", "campaign_id", "campaign_date").orderBy(F.monotonically_increasing_id())) > 1)
    
    return df

# - OUTPUT CONTRACT -
#   marketing_csv: csv -> Tables/marketing_csv_clean
OUTPUT_PATHS: dict = {}

def run_pipeline(spark):
    dfs = {}
    def _canonical_id(name: str) -> str:
        import re
        return re.sub(r"[^0-9a-zA-Z_]+", "_", name or "x").strip("_")

    active_canons = {_canonical_id(d) for d in DATASETS}
    # Load (connector manifest - use _resolve_data_path for blob/SQL)
    # marketing_csv: fabric_files_zone (csv) @ Files/raw/marketing_csv.csv
    if "marketing_csv" in active_canons:
        dfs["marketing_csv"] = spark.read.option('header', 'true').option('inferSchema', 'true').csv(_resolve_data_path("Files/raw/marketing_csv.csv"))
    if "marketing_csv" in dfs:
        dfs["marketing_csv"] = transform_marketing_csv(dfs["marketing_csv"], dfs)
        _log_row_count(dfs["marketing_csv"], "marketing_csv")
    # Write cleaned per-dataset outputs (format matches manifest; XML -> parquet)
    if "marketing_csv" in dfs:
        _out = 'Tables/marketing_csv_clean'  # Fabric managed table (audit path)
        OUTPUT_PATHS['marketing_csv'] = _out
        logger.info('Writing %s -> Fabric staging table %s', 'marketing_csv', 'marketing_csv_clean_staging')
        dfs["marketing_csv"].write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("marketing_csv_clean_staging")
    
    return dfs, OUTPUT_PATHS

# Fabric notebooks pre-inject 'spark'; fall back to creating one for local dev.
try:
    spark  # type: ignore[name-defined]
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("AgentDharaETL").getOrCreate()
_dfs, _paths = run_pipeline(spark)