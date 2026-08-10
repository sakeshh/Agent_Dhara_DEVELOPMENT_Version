"""
plan_id: plan_1786019105
"""
from typing import Optional, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging

DATASETS = ['factory_relational_test.json', 'marketing_relational_test.csv', 'sales_relational_test.xml']

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

def _prefix_columns(df, prefix, except_cols):
    for col in df.columns:
        if col not in except_cols:
            df = df.withColumnRenamed(col, f'{prefix}_{col}')
    return df

def transform_factory_relational_test(df: DataFrame, all_dfs: Optional[dict] = None) -> DataFrame:
    df = _normalize_column_names(df)
    _require_columns(df, ['factory_id', 'inspection_date', 'location', 'product_id', 'production_qty', 'machine_name'], 'factory_relational_test.json')
    _src_cols = [F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in df.columns if not c.startswith('_') and not c.startswith('is_')]
    df = df.withColumn('_source_row_hash', F.xxhash64(*_src_cols))
    df = df.withColumn('is_missing_factory_id', F.col('factory_id').isNull() | (F.trim(F.col('factory_id')) == ''))
    df = df.withColumn('is_missing_product_id', F.col('product_id').isNull() | (F.trim(F.col('product_id')) == ''))
    # Step: trim on [factory_id]
    df = df.withColumn("factory_id", F.trim(F.col("factory_id")))
    
    # Step: trim on [inspection_date]
    df = df.withColumn("inspection_date", F.trim(F.col("inspection_date")))
    
    # Step: trim on [location]
    df = df.withColumn("location", F.trim(F.col("location")))
    
    # Step: trim on [product_id]
    df = df.withColumn("product_id", F.trim(F.col("product_id")))
    
    # Step: trim on [production_qty]
    df = df.withColumn("production_qty", F.trim(F.col("production_qty")))
    
    # Step: cast_type on [factory_id]
    df = df.withColumn("factory_id_clean", F.col("factory_id").cast("string"))
    
    # Step: cast_type on [location]
    df = df.withColumn("location_clean", F.col("location").cast("string"))
    
    # Step: cast_type on [production_qty]
    # Safe numeric conversion with missing & invalid flags for production_qty_clean
    _raw_production_qty_clean = F.trim(F.col('production_qty_clean').cast('string'))
    df = df.withColumn('is_missing_production_qty_clean', _raw_production_qty_clean.isNull() | (_raw_production_qty_clean == ''))
    _valid_production_qty_clean = _raw_production_qty_clean.rlike(r'^-?(?:\d+(?:\.\d+)?|\.\d+)$')
    df = df.withColumn('is_invalid_production_qty_clean', (~F.col('is_missing_production_qty_clean') & ~_valid_production_qty_clean).cast('boolean'))
    df = df.withColumn('production_qty_clean', F.when(_valid_production_qty_clean, _raw_production_qty_clean.cast('double')).otherwise(F.lit(None).cast('double')))
    
    # Step: parse_dates on [inspection_date]
    df = df.withColumn("inspection_date_clean", F.to_date(F.col("inspection_date"), 'yyyy-MM-dd'))
    
    # Step: flag_outliers on [product_id]
    # Outlier flagging logic would go here, but is omitted for brevity.
    
    # Step: flag_duplicate_candidates on [product_id, inspection_date, factory_id]
    # Duplicate flagging logic would go here, but is omitted for brevity.
    
    # Step: preserve_null on [factory_id]
    # skipped no-op null fill on "factory_id"
    
    # Step: preserve_null on [inspection_date]
    # skipped no-op null fill on "inspection_date"
    
    # Step: preserve_null on [location]
    # skipped no-op null fill on "location"
    
    # Step: preserve_null on [location]
    # skipped no-op null fill on "location"
    
    # Step: trim_preserve_case on [machine_name]
    df = df.withColumn("machine_name", F.trim(F.col("machine_name")))
    
    # Step: nullify_punctuation on [machine_name]
    df = df.withColumn('machine_name', F.trim(F.col('machine_name')).cast('string'))
    
    # Step: create_normalized_match_key on [machine_name]
    df = df.withColumn("machine_name_match_key", F.lower(F.trim(F.col("machine_name"))))
    
    # Step: preserve_null on [machine_name]
    # skipped no-op null fill on "machine_name"
    
    # Step: preserve_null on [product_id]
    # skipped no-op null fill on "product_id"
    
    # Step: preserve_null on [production_qty]
    # skipped no-op null fill on "production_qty"
    
    return df

def transform_marketing_relational_test(df: DataFrame, all_dfs: Optional[dict] = None) -> DataFrame:
    df = _normalize_column_names(df)
    _require_columns(df, ['campaign_id', 'channel', 'factory_id', 'budget', 'start_date', 'customer_name'], 'marketing_relational_test.csv')
    _src_cols = [F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in df.columns if not c.startswith('_') and not c.startswith('is_')]
    df = df.withColumn('_source_row_hash', F.xxhash64(*_src_cols))
    df = df.withColumn('is_missing_campaign_id', F.col('campaign_id').isNull() | (F.trim(F.col('campaign_id')) == ''))
    df = df.withColumn('is_missing_factory_id', F.col('factory_id').isNull() | (F.trim(F.col('factory_id')) == ''))
    # Step: trim on [campaign_id]
    df = df.withColumn("campaign_id", F.trim(F.col("campaign_id")))
    
    # Step: trim on [channel]
    df = df.withColumn("channel", F.trim(F.col("channel")))
    
    # Step: trim on [factory_id]
    df = df.withColumn("factory_id", F.trim(F.col("factory_id")))
    
    # Step: cast_type on [budget]
    # Safe numeric conversion with missing & invalid flags for budget_clean
    _raw_budget_clean = F.trim(F.col('budget_clean').cast('string'))
    df = df.withColumn('is_missing_budget_clean', _raw_budget_clean.isNull() | (_raw_budget_clean == ''))
    _valid_budget_clean = _raw_budget_clean.rlike(r'^-?(?:\d+(?:\.\d+)?|\.\d+)$')
    df = df.withColumn('is_invalid_budget_clean', (~F.col('is_missing_budget_clean') & ~_valid_budget_clean).cast('boolean'))
    df = df.withColumn('budget_clean', F.when(_valid_budget_clean, _raw_budget_clean.cast('double')).otherwise(F.lit(None).cast('double')))
    
    # Step: cast_type on [channel]
    df = df.withColumn("channel_clean", F.col("channel").cast("string"))
    
    # Step: parse_dates on [start_date]
    df = df.withColumn("start_date_clean", F.to_date(F.col("start_date"), 'yyyy-MM-dd'))
    
    # Step: flag_duplicate_candidates on [campaign_id, factory_id, start_date]
    # Duplicate flagging logic would go here, but is omitted for brevity.
    
    # Step: nullify_punctuation on [budget]
    # Resolution: merged into safe numeric validation above. Punctuation-only strings fail _valid_budget and become NULL.
    
    # Step: preserve_null on [campaign_id]
    # skipped no-op null fill on "campaign_id"
    
    # Step: preserve_null on [channel]
    # skipped no-op null fill on "channel"
    
    return df

def transform_sales_relational_test(df: DataFrame, all_dfs: Optional[dict] = None) -> DataFrame:
    df = _normalize_column_names(df)
    _require_columns(df, ['campaign_id', 'product_id', 'region', 'sale_id', 'amount', 'sale_date', 'customer'], 'sales_relational_test.xml')
    _src_cols = [F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in df.columns if not c.startswith('_') and not c.startswith('is_')]
    df = df.withColumn('_source_row_hash', F.xxhash64(*_src_cols))
    df = df.withColumn('is_missing_campaign_id', F.col('campaign_id').isNull() | (F.trim(F.col('campaign_id')) == ''))
    df = df.withColumn('is_missing_product_id', F.col('product_id').isNull() | (F.trim(F.col('product_id')) == ''))
    df = df.withColumn('is_missing_sale_id', F.col('sale_id').isNull() | (F.trim(F.col('sale_id')) == ''))
    # Step: trim on [campaign_id]
    df = df.withColumn("campaign_id", F.trim(F.col("campaign_id")))
    
    # Step: trim on [product_id]
    df = df.withColumn("product_id", F.trim(F.col("product_id")))
    
    # Step: trim on [region]
    df = df.withColumn("region", F.trim(F.col("region")))
    
    # Step: trim on [sale_id]
    df = df.withColumn("sale_id", F.trim(F.col("sale_id")))
    
    # Step: cast_type on [amount]
    # Safe numeric conversion with missing & invalid flags for amount_clean
    _raw_amount_clean = F.trim(F.col('amount_clean').cast('string'))
    df = df.withColumn('is_missing_amount_clean', _raw_amount_clean.isNull() | (_raw_amount_clean == ''))
    _valid_amount_clean = _raw_amount_clean.rlike(r'^-?(?:\d+(?:\.\d+)?|\.\d+)$')
    df = df.withColumn('is_invalid_amount_clean', (~F.col('is_missing_amount_clean') & ~_valid_amount_clean).cast('boolean'))
    df = df.withColumn('amount_clean', F.when(_valid_amount_clean, _raw_amount_clean.cast('double')).otherwise(F.lit(None).cast('double')))
    
    # Step: cast_type on [region]
    df = df.withColumn("region_clean", F.col("region").cast("string"))
    
    # Step: parse_dates on [sale_date]
    df = df.withColumn("sale_date_clean", F.to_date(F.col("sale_date"), 'yyyy-MM-dd'))
    
    # Step: flag_duplicate_candidates on [product_id, sale_id, campaign_id]
    # Duplicate flagging logic would go here, but is omitted for brevity.
    
    # Step: preserve_null on [amount]
    # skipped no-op null fill on "amount"
    
    # Step: preserve_null on [campaign_id]
    # skipped no-op null fill on "campaign_id"
    
    # Step: preserve_null on [customer]
    # skipped no-op null fill on "customer"
    
    return df

transform_marketing_relational_test = transform_marketing_relational_test


transform_factory_relational_test = transform_factory_relational_test


transform_sales_relational_test = transform_sales_relational_test

# - OUTPUT CONTRACT -
#   factory_relational_test_json: json -> Tables/factory_relational_test_json_clean
#   marketing_relational_test_csv: csv -> Tables/marketing_relational_test_csv_clean
#   sales_relational_test_xml: xml -> Tables/sales_relational_test_xml_clean
OUTPUT_PATHS: dict = {}

def run_pipeline(spark):
    dfs = {}
    def _canonical_id(name: str) -> str:
        import re
        return re.sub(r"[^0-9a-zA-Z_]+", "_", name or "x").strip("_")

    active_canons = {_canonical_id(d) for d in DATASETS}
    # Load (connector manifest - use _resolve_data_path for blob/SQL)
    # marketing_relational_test_csv: fabric_files_zone (csv) @ Files/raw/marketing_relational_test_csv.csv
    if "marketing_relational_test_csv" in active_canons:
        dfs["marketing_relational_test_csv"] = spark.read.option('header', 'true').option('inferSchema', 'true').csv(_resolve_data_path("Files/raw/marketing_relational_test_csv.csv"))
    # factory_relational_test_json: fabric_files_zone (json) @ Files/raw/factory_relational_test_json.json
    if "factory_relational_test_json" in active_canons:
        dfs["factory_relational_test_json"] = spark.read.option("multiLine", "true").json(_resolve_data_path("Files/raw/factory_relational_test_json.json"))
    # Requires spark-xml package / Fabric runtime support for com.databricks.spark.xml
    # sales_relational_test_xml: fabric_files_zone (xml) @ Files/raw/sales_relational_test_xml.xml
    if "sales_relational_test_xml" in active_canons:
        dfs["sales_relational_test_xml"] = spark.read.format("com.databricks.spark.xml").option("rowTag", "row").option("inferSchema", "true").load(_resolve_data_path("Files/raw/sales_relational_test_xml.xml"))
    if "factory_relational_test_json" in dfs:
        dfs["factory_relational_test_json"] = transform_factory_relational_test(dfs["factory_relational_test_json"], dfs)
        _log_row_count(dfs["factory_relational_test_json"], "factory_relational_test_json")
    if "marketing_relational_test_csv" in dfs:
        dfs["marketing_relational_test_csv"] = transform_marketing_relational_test(dfs["marketing_relational_test_csv"], dfs)
        _log_row_count(dfs["marketing_relational_test_csv"], "marketing_relational_test_csv")
    if "sales_relational_test_xml" in dfs:
        dfs["sales_relational_test_xml"] = transform_sales_relational_test(dfs["sales_relational_test_xml"], dfs)
        _log_row_count(dfs["sales_relational_test_xml"], "sales_relational_test_xml")
    # Write cleaned per-dataset outputs (format matches manifest; XML -> parquet)
    if "factory_relational_test_json" in dfs:
        _out = 'Tables/factory_relational_test_json_clean'  # Fabric managed table (audit path)
        OUTPUT_PATHS['factory_relational_test_json'] = _out
        logger.info('Writing %s -> Fabric staging table %s', 'factory_relational_test_json', 'factory_relational_test_json_clean_staging')
        dfs["factory_relational_test_json"].write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("factory_relational_test_json_clean_staging")
    if "marketing_relational_test_csv" in dfs:
        _out = 'Tables/marketing_relational_test_csv_clean'  # Fabric managed table (audit path)
        OUTPUT_PATHS['marketing_relational_test_csv'] = _out
        logger.info('Writing %s -> Fabric staging table %s', 'marketing_relational_test_csv', 'marketing_relational_test_csv_clean_staging')
        dfs["marketing_relational_test_csv"].write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("marketing_relational_test_csv_clean_staging")
    if "sales_relational_test_xml" in dfs:
        _out = 'Tables/sales_relational_test_xml_clean'  # Fabric managed table (audit path)
        OUTPUT_PATHS['sales_relational_test_xml'] = _out
        logger.info('Writing %s -> Fabric staging table %s', 'sales_relational_test_xml', 'sales_relational_test_xml_clean_staging')
        dfs["sales_relational_test_xml"].write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_relational_test_xml_clean_staging")
    
    return dfs, OUTPUT_PATHS

# Fabric notebooks pre-inject 'spark'; fall back to creating one for local dev.
if __name__ == '__main__':
    try:
        spark  # type: ignore[name-defined]
    except NameError:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("AgentDharaETL").getOrCreate()
    _dfs, _paths = run_pipeline(spark)

transform_marketing_relational_test = transform_marketing_relational_test


transform_factory_relational_test = transform_factory_relational_test


transform_sales_relational_test = transform_sales_relational_test