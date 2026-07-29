# ERROR: None

"""
plan_id: plan_1785230517
"""
from typing import Optional, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging

DATASETS = ['sales_relational_test.xml', 'marketing_relational_test.csv', 'factory_relational_test.json']

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
  """Parse common timestamp formats into a materialized timestamp column with invalid flag tracking."""
  raw_col = f"_raw_{column_name}"
  invalid_flag = f"is_invalid_{column_name}"
  df = df.withColumn(raw_col, F.trim(F.col(column_name).cast("string")))
  parsed = F.coalesce(
    F.expr(f"try_to_timestamp(`{raw_col}`, 'yyyy-MM-dd HH:mm:ss')"),
    F.expr(f"try_to_timestamp(`{raw_col}`, 'yyyy-MM-dd')"),
    F.expr(f"try_to_timestamp(`{raw_col}`, 'MM/dd/yyyy')"),
    F.expr(f"try_to_timestamp(`{raw_col}`, 'yyyy/MM/dd')"),
    F.expr(f"try_to_timestamp(`{raw_col}`, 'dd/MM/yyyy')")
  )
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
  import os
  if key_col not in df.columns:
    return
  if os.environ.get("DHARA_ETL_CHECK_DUP_KEYS", "1").strip().lower() in ("0", "false", "no"):
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

def transform_sales_relational_test(df: DataFrame, all_dfs: Optional[dict] = None) -> DataFrame:
    df = _normalize_column_names(df)
    _require_columns(df, ['campaign_id', 'customer', 'product_id', 'region', 'sale_id', 'amount', 'sale_date'], 'sales_relational_test.xml')
    _src_cols = [F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in df.columns if not c.startswith('_') and not c.startswith('is_')]
    df = df.withColumn('_source_row_hash', F.xxhash64(*_src_cols))
    df = df.withColumn('is_missing_campaign_id', F.col('campaign_id').isNull() | (F.trim(F.col('campaign_id')) == ''))
    df = df.withColumn('is_missing_product_id', F.col('product_id').isNull() | (F.trim(F.col('product_id')) == ''))
    df = df.withColumn('is_missing_sale_id', F.col('sale_id').isNull() | (F.trim(F.col('sale_id')) == ''))

    # Step: trim on [campaign_id]
    df = df.withColumn("campaign_id", F.trim(F.col("campaign_id")))
    
    # Step: trim on [customer]
    df = df.withColumn("customer", F.trim(F.col("customer")))
    
    # Step: trim on [product_id]
    df = df.withColumn("product_id", F.trim(F.col("product_id")))
    
    # Step: trim on [region]
    df = df.withColumn("region", F.trim(F.col("region")))
    
    # Step: trim on [sale_id]
    df = df.withColumn("sale_id", F.trim(F.col("sale_id")))
    
    # Step: lowercase on [customer]
    df = df.withColumn("customer", F.lower(F.col("customer")))
    
    # Step: lowercase on [region]
    df = df.withColumn("region", F.lower(F.col("region")))
    
    # Step: cast_type on [amount]
    # Safe numeric conversion with missing & invalid flags for amount
    _raw_amount = F.trim(F.col('amount').cast('string'))
    df = df.withColumn('is_missing_amount', _raw_amount.isNull() | (_raw_amount == ''))
    _valid_amount = _raw_amount.rlike(r'^-?(?:\d+(?:\.\d+)?|\.\d+)$')
    df = df.withColumn('is_invalid_amount', (~F.col('is_missing_amount') & ~_valid_amount).cast('boolean'))
    df = df.withColumn('amount', F.when(_valid_amount, _raw_amount.cast('double')).otherwise(F.lit(None).cast('double')))
    
    # Step: cast_type on [region]
    df = df.withColumn("region", F.col("region").cast("string"))
    
    # Step: coerce_numeric on [amount] — merged into safe numeric conversion above
    
    # Step: parse_dates on [sale_date]
    # Safe timestamp parsing with try_to_timestamp & invalid flag for sale_date
    df = _parse_timestamp_safely(df, 'sale_date')
    
    # Step: flag_outliers on [product_id]
    # Manual review required for outlier detection on product_id
    
    # Step: fill_or_drop on [amount]
    df = df.withColumn('is_imputed_amount', F.col('amount').isNull().cast('boolean'))
    df = df.fillna({'amount': 2741})
    
    # Step: fill_or_drop on [customer]
    # skipped empty fillna for df
    
    # Step: nullify_punctuation on [customer]
    df = df.withColumn("customer", F.when(~F.col("customer").cast("string").rlike('[a-zA-Z0-9]'), F.lit(None)).otherwise(F.col("customer")))
    
    # Step: fill_or_drop on [sale_date]
    # skipped empty fillna for df
    
    # Step: trim on [sale_id]
    df = df.withColumn("sale_id", F.trim(F.col("sale_id")))
    
    # Step: deduplicate on [Row-level]
    from pyspark.sql import Window as _DedupWindow
    _data_cols = [F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in df.columns if not c.startswith('_') and not c.startswith('is_')]
    _dup_w = _DedupWindow.partitionBy(F.xxhash64(*_data_cols) if _data_cols else F.lit(1)).orderBy(F.monotonically_increasing_id())
    df = df.withColumn('_is_duplicate_row', (F.row_number().over(_dup_w) > 1).cast('boolean'))
    
    return df

def transform_marketing_relational_test(df: DataFrame, all_dfs: Optional[dict] = None) -> DataFrame:
    df = _normalize_column_names(df)
    _require_columns(df, ['budget', 'campaign_id', 'channel', 'customer_name', 'factory_id', 'start_date'], 'marketing_relational_test.csv')
    _src_cols = [F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in df.columns if not c.startswith('_') and not c.startswith('is_')]
    df = df.withColumn('_source_row_hash', F.xxhash64(*_src_cols))
    df = df.withColumn('is_missing_campaign_id', F.col('campaign_id').isNull() | (F.trim(F.col('campaign_id')) == ''))
    df = df.withColumn('is_missing_factory_id', F.col('factory_id').isNull() | (F.trim(F.col('factory_id')) == ''))

    # Step: trim on [budget]
    df = df.withColumn("budget", F.trim(F.col("budget")))
    
    # Step: trim on [campaign_id]
    df = df.withColumn("campaign_id", F.trim(F.col("campaign_id")))
    
    # Step: trim on [channel]
    df = df.withColumn("channel", F.trim(F.col("channel")))
    
    # Step: trim on [customer_name]
    df = df.withColumn("customer_name", F.trim(F.col("customer_name")))
    
    # Step: trim on [factory_id]
    df = df.withColumn("factory_id", F.trim(F.col("factory_id")))
    
    # Step: lowercase on [budget]
    df = df.withColumn("budget", F.lower(F.col("budget")))
    
    # Step: lowercase on [channel]
    df = df.withColumn("channel", F.lower(F.col("channel")))
    
    # Step: lowercase on [customer_name]
    df = df.withColumn("customer_name", F.lower(F.col("customer_name")))
    
    # Step: cast_type on [budget]
    # Safe numeric conversion with missing & invalid flags for budget
    _raw_budget = F.trim(F.col('budget').cast('string'))
    df = df.withColumn('is_missing_budget', _raw_budget.isNull() | (_raw_budget == ''))
    _valid_budget = _raw_budget.rlike(r'^-?(?:\d+(?:\.\d+)?|\.\d+)$')
    df = df.withColumn('is_invalid_budget', (~F.col('is_missing_budget') & ~_valid_budget).cast('boolean'))
    df = df.withColumn('budget', F.when(_valid_budget, _raw_budget.cast('double')).otherwise(F.lit(None).cast('double')))
    
    # Step: cast_type on [channel]
    df = df.withColumn("channel", F.col("channel").cast("string"))
    
    # Step: parse_dates on [start_date]
    # Safe timestamp parsing with try_to_timestamp & invalid flag for start_date
    df = _parse_timestamp_safely(df, 'start_date')
    
    # Step: fill_or_drop on [budget]
    df = df.withColumn('is_imputed_budget', F.col('budget').isNull().cast('boolean'))
    df = df.fillna({'budget': 49084.2338})
    
    # Step: nullify_punctuation on [budget]
    # Resolution: merged into safe numeric validation above. Punctuation-only strings fail _valid_budget and become NULL.
    
    # Step: fill_or_drop on [channel]
    df = df.withColumn('is_imputed_channel', F.col('channel').isNull().cast('boolean'))
    df = df.fillna({'channel': 'unknown'})
    
    # Step: fill_nulls_simple on [customer_name]
    # skipped empty fillna for df
    
    # Step: nullify_punctuation on [customer_name]
    df = df.withColumn("customer_name", F.when(~F.col("customer_name").cast("string").rlike('[a-zA-Z0-9]'), F.lit(None)).otherwise(F.col("customer_name")))
    
    # Step: fill_or_drop on [start_date]
    # skipped empty fillna for df
    
    # Step: deduplicate on [Row-level]
    from pyspark.sql import Window as _DedupWindow
    _data_cols = [F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in df.columns if not c.startswith('_') and not c.startswith('is_')]
    _dup_w = _DedupWindow.partitionBy(F.xxhash64(*_data_cols) if _data_cols else F.lit(1)).orderBy(F.monotonically_increasing_id())
    df = df.withColumn('_is_duplicate_row', (F.row_number().over(_dup_w) > 1).cast('boolean'))
    
    return df

def transform_factory_relational_test(df: DataFrame, all_dfs: Optional[dict] = None) -> DataFrame:
    df = _normalize_column_names(df)
    _require_columns(df, ['factory_id', 'location', 'machine_name', 'product_id', 'production_qty', 'inspection_date'], 'factory_relational_test.json')
    _src_cols = [F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in df.columns if not c.startswith('_') and not c.startswith('is_')]
    df = df.withColumn('_source_row_hash', F.xxhash64(*_src_cols))
    df = df.withColumn('is_missing_factory_id', F.col('factory_id').isNull() | (F.trim(F.col('factory_id')) == ''))
    df = df.withColumn('is_missing_product_id', F.col('product_id').isNull() | (F.trim(F.col('product_id')) == ''))

    # Step: trim on [factory_id]
    df = df.withColumn("factory_id", F.trim(F.col("factory_id")))
    
    # Step: trim on [location]
    df = df.withColumn("location", F.trim(F.col("location")))
    
    # Step: trim on [machine_name]
    df = df.withColumn("machine_name", F.trim(F.col("machine_name")))
    
    # Step: trim on [product_id]
    df = df.withColumn("product_id", F.trim(F.col("product_id")))
    
    # Step: trim on [production_qty]
    df = df.withColumn("production_qty", F.trim(F.col("production_qty")))
    
    # Step: lowercase on [location]
    df = df.withColumn("location", F.lower(F.col("location")))
    
    # Step: lowercase on [machine_name]
    df = df.withColumn("machine_name", F.lower(F.col("machine_name")))
    
    # Step: lowercase on [production_qty]
    df = df.withColumn("production_qty", F.lower(F.col("production_qty")))
    
    # Step: cast_type on [location]
    df = df.withColumn("location", F.col("location").cast("string"))
    
    # Step: cast_type on [production_qty]
    # Safe numeric conversion with missing & invalid flags for production_qty
    _raw_production_qty = F.trim(F.col('production_qty').cast('string'))
    df = df.withColumn('is_missing_production_qty', _raw_production_qty.isNull() | (_raw_production_qty == ''))
    _valid_production_qty = _raw_production_qty.rlike(r'^-?(?:\d+(?:\.\d+)?|\.\d+)$')
    df = df.withColumn('is_invalid_production_qty', (~F.col('is_missing_production_qty') & ~_valid_production_qty).cast('boolean'))
    df = df.withColumn('production_qty', F.when(_valid_production_qty, _raw_production_qty.cast('double')).otherwise(F.lit(None).cast('double')))
    
    # Step: parse_dates on [inspection_date]
    # Safe timestamp parsing with try_to_timestamp & invalid flag for inspection_date
    df = _parse_timestamp_safely(df, 'inspection_date')
    
    # Step: fill_or_drop on [inspection_date]
    # skipped empty fillna for df
    
    # Step: fill_or_drop on [location]
    df = df.withColumn('is_imputed_location', F.col('location').isNull().cast('boolean'))
    df = df.fillna({'location': 'unknown'})
    
    # Step: fill_nulls_simple on [machine_name]
    # skipped empty fillna for df
    
    # Step: fill_or_drop on [production_qty]
    df = df.withColumn('is_imputed_production_qty', F.col('production_qty').isNull().cast('boolean'))
    df = df.fillna({'production_qty': 137})
    
    # Step: deduplicate on [Row-level]
    from pyspark.sql import Window as _DedupWindow
    _data_cols = [F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in df.columns if not c.startswith('_') and not c.startswith('is_')]
    _dup_w = _DedupWindow.partitionBy(F.xxhash64(*_data_cols) if _data_cols else F.lit(1)).orderBy(F.monotonically_increasing_id())
    df = df.withColumn('_is_duplicate_row', (F.row_number().over(_dup_w) > 1).cast('boolean'))
    
    return df

def run_pipeline(spark: SparkSession) -> None:
    # Load datasets
    sales_df = spark.read.format("com.databricks.spark.xml").option("rowTag", "row").option("inferSchema", "true").load(_resolve_data_path("Files/raw/sales_relational_test_xml.xml"))
    marketing_df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(_resolve_data_path("Files/raw/marketing_relational_test_csv.csv"))
    factory_df = spark.read.option("multiLine", "true").json(_resolve_data_path("Files/raw/factory_relational_test_json.json"))
    
    # Transform datasets
    sales_df = transform_sales_relational_test(sales_df)
    marketing_df = transform_marketing_relational_test(marketing_df)
    factory_df = transform_factory_relational_test(factory_df)
    
    # Log row counts
    _log_row_count(sales_df, "sales_relational_test")
    _log_row_count(marketing_df, "marketing_relational_test")
    _log_row_count(factory_df, "factory_relational_test")
    
    # Write cleaned datasets
    sales_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_relational_test_xml_clean")
    marketing_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("marketing_relational_test_csv_clean")
    factory_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("factory_relational_test_json_clean")

run_pipeline(spark)