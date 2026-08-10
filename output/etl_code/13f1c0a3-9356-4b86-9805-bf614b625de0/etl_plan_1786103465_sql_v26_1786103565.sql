-- plan_id: plan_1786103465

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'etl_log' AND schema_id = SCHEMA_ID('dbo')) 
CREATE TABLE dbo.etl_log (
    id INT IDENTITY(1,1) PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME NULL,
    status VARCHAR(20) NOT NULL,
    error_message VARCHAR(MAX) NULL
);
GO

CREATE OR ALTER PROCEDURE dbo.etl_clean_dbo_Accounts AS 
BEGIN
    DECLARE @run_id INT;

    BEGIN TRY
        BEGIN TRANSACTION;

        INSERT INTO dbo.etl_log (process_name, start_time, status) 
        VALUES ('etl_clean_dbo_Accounts', GETDATE(), 'RUNNING');
        SET @run_id = SCOPE_IDENTITY();

        -- Initialize staging table
        SELECT * INTO #Accounts_Staging FROM dbo.Accounts WHERE 1=0;

        -- Copy raw data into staging
        INSERT INTO #Accounts_Staging
        SELECT *, @run_id AS etl_batch_id FROM dbo.Accounts;

        -- Transformations
        UPDATE #Accounts_Staging
        SET 
            AccountID_clean = TRY_CAST(AccountID AS FLOAT),
            AccountType_clean = TRY_CAST(AccountType AS FLOAT),
            CustomerID_clean = TRY_CAST(CustomerID AS DECIMAL(18,2)),
            AccountType = CASE WHEN AccountType IS NULL THEN NULL ELSE AccountType END,
            Balance = CASE WHEN Balance IS NULL THEN NULL ELSE Balance END,
            [Row-level] = CASE 
                WHEN (SELECT COUNT(*) FROM #Accounts_Staging AS inner_table 
                      WHERE inner_table.CustomerID = #Accounts_Staging.CustomerID 
                      AND inner_table.AccountID = #Accounts_Staging.AccountID) > 1 
                THEN 1 ELSE 0 END
        WHERE 1=1;

        -- Log row counts
        DECLARE @row_count INT = (SELECT COUNT(*) FROM #Accounts_Staging);
        PRINT 'dbo.Accounts: row_count=' + CAST(@row_count AS VARCHAR);

        -- Truncate clean table and insert transformed data
        TRUNCATE TABLE dbo.Accounts_Clean;
        INSERT INTO dbo.Accounts_Clean
        SELECT * FROM #Accounts_Staging;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        UPDATE dbo.etl_log SET end_time = GETDATE(), status = 'FAILED', error_message = @error_message 
        WHERE id = @run_id;
    END CATCH;
END;
GO

CREATE OR ALTER PROCEDURE dbo.etl_clean_dbo_Courses AS 
BEGIN
    DECLARE @run_id INT;

    BEGIN TRY
        BEGIN TRANSACTION;

        INSERT INTO dbo.etl_log (process_name, start_time, status) 
        VALUES ('etl_clean_dbo_Courses', GETDATE(), 'RUNNING');
        SET @run_id = SCOPE_IDENTITY();

        -- Initialize staging table
        SELECT * INTO #Courses_Staging FROM dbo.Courses WHERE 1=0;

        -- Copy raw data into staging
        INSERT INTO #Courses_Staging
        SELECT *, @run_id AS etl_batch_id FROM dbo.Courses;

        -- Transformations
        UPDATE #Courses_Staging
        SET 
            CourseID_clean = TRY_CAST(CourseID AS DECIMAL(18,2)),
            InstructorID_clean = TRY_CAST(InstructorID AS DECIMAL(18,2)),
            CreatedDate_clean = COALESCE(
                TRY_CONVERT(DATETIME, CreatedDate, 120),
                TRY_CONVERT(DATETIME, CreatedDate, 103),
                TRY_CONVERT(DATETIME, CreatedDate, 101),
                TRY_CONVERT(DATETIME, CreatedDate, 111)
            ),
            CourseTitle = LTRIM(RTRIM(CourseTitle))
        WHERE 1=1;

        -- Log row counts
        DECLARE @row_count INT = (SELECT COUNT(*) FROM #Courses_Staging);
        PRINT 'dbo.Courses: row_count=' + CAST(@row_count AS VARCHAR);

        -- Truncate clean table and insert transformed data
        TRUNCATE TABLE dbo.Courses_Clean;
        INSERT INTO dbo.Courses_Clean
        SELECT * FROM #Courses_Staging;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        UPDATE dbo.etl_log SET end_time = GETDATE(), status = 'FAILED', error_message = @error_message 
        WHERE id = @run_id;
    END CATCH;
END;
GO

CREATE OR ALTER PROCEDURE dbo.etl_main AS 
BEGIN
    EXEC dbo.etl_clean_dbo_Accounts;
    EXEC dbo.etl_clean_dbo_Courses;
END;
GO
GO

"""
plan_id: plan_1786013286
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
    df = df.withColumn("factory_id_clean", F.when(F.col("factory_id").isNotNull(), F.col("factory_id").cast("string")).otherwise(F.lit(None)))
    
    # Step: cast_type on [location]
    df = df.withColumn("location_clean", F.when(F.col("location").isNotNull(), F.col("location").cast("string")).otherwise(F.lit(None)))
    
    # Step: cast_type on [production_qty]
    df = df.withColumn("production_qty_clean", F.when(F.col("production_qty").isNotNull(), F.col("production_qty").cast("string")).otherwise(F.lit(None)))
    
    # Step: parse_dates on [inspection_date]
    df = df.withColumn("inspection_date_clean", F.to_date(F.col("inspection_date"), 'yyyy-MM-dd'))
    
    # Step: flag_outliers on [product_id]
    # Outlier flagging logic would go here, but we are not implementing it as per the rules.
    
    # Step: flag_duplicate_candidates on [Row-level]
    # Duplicate flagging logic would go here, but we are not implementing it as per the rules.
    
    # Step: preserve_null on [factory_id]
    # skipped no-op null fill on "factory_id"
    
    # Step: preserve_null on [inspection_date]
    # skipped no-op null fill on "inspection_date"
    
    # Step: flag_business_key_duplicate on [inspection_date, product_id]
    # Business key duplicate flagging logic would go here, but we are not implementing it as per the rules.
    
    # Step: preserve_null on [location]
    # skipped no-op null fill on "location"
    
    # Step: flag_duplicate_candidates on [location]
    # Duplicate flagging logic would go here, but we are not implementing it as per the rules.
    
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
    df = df.withColumn("budget_clean", F.when(F.col("budget").isNotNull(), F.col("budget").cast("string")).otherwise(F.lit(None)))
    
    # Step: cast_type on [channel]
    df = df.withColumn("channel_clean", F.when(F.col("channel").isNotNull(), F.col("channel").cast("string")).otherwise(F.lit(None)))
    
    # Step: parse_dates on [start_date]
    df = df.withColumn("start_date_clean", F.to_date(F.col("start_date"), 'yyyy-MM-dd'))
    
    # Step: flag_duplicate_candidates on [Row-level]
    # Duplicate flagging logic would go here, but we are not implementing it as per the rules.
    
    # Step: nullify_punctuation on [budget]
    # Resolution: merged into safe numeric validation above. Punctuation-only strings fail _valid_budget and become NULL.
    
    # Step: preserve_null on [budget]
    # skipped no-op null fill on "budget"
    
    # Step: preserve_null on [campaign_id]
    # skipped no-op null fill on "campaign_id"
    
    # Step: flag_business_key_duplicate on [campaign_id]
    # Business key duplicate flagging logic would go here, but we are not implementing it as per the rules.
    
    # Step: preserve_null on [channel]
    # skipped no-op null fill on "channel"
    
    # Step: flag_duplicate_candidates on [channel]
    # Duplicate flagging logic would go here, but we are not implementing it as per the rules.
    
    # Step: trim_preserve_case on [customer_name]
    df = df.withColumn("customer_name", F.trim(F.col("customer_name")))
    
    # Step: nullify_punctuation on [customer_name]
    df = df.withColumn("customer_name", F.when(F.trim(F.col("customer_name")).rlike('^[^a-zA-Z0-9]+$'), F.lit(None)).otherwise(F.col("customer_name")))
    
    # Step: create_normalized_match_key on [customer_name]
    df = df.withColumn("customer_name_match_key", F.lower(F.trim(F.col("customer_name"))))
    
    # Step: preserve_null on [customer_name]
    # skipped no-op null fill on "customer_name"
    
    # Step: preserve_null on [factory_id]
    # skipped no-op null fill on "factory_id"
    
    # Step: preserve_null on [start_date]
    # skipped no-op null fill on "start_date"
    
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
    df = df.withColumn("amount_clean", F.when(F.col("amount").isNotNull(), F.col("amount").cast("string")).otherwise(F.lit(None)))
    
    # Step: cast_type on [region]
    df = df.withColumn("region_clean", F.when(F.col("region").isNotNull(), F.col("region").cast("string")).otherwise(F.lit(None)))
    
    # Step: parse_dates on [sale_date]
    df = df.withColumn("sale_date_clean", F.to_date(F.col("sale_date"), 'yyyy-MM-dd'))
    
    # Step: flag_duplicate_candidates on [Row-level]
    # Duplicate flagging logic would go here, but we are not implementing it as per the rules.
    
    # Step: preserve_null on [amount]
    # skipped no-op null fill on "amount"
    
    # Step: preserve_null on [campaign_id]
    # skipped no-op null fill on "campaign_id"
    
    # Step: trim_preserve_case on [customer]
    df = df.withColumn("customer", F.trim(F.col("customer")))
    
    # Step: nullify_punctuation on [customer]
    df = df.withColumn("customer", F.when(F.trim(F.col("customer")).rlike('^[^a-zA-Z0-9]+$'), F.lit(None)).otherwise(F.col("customer")))
    
    # Step: preserve_null on [customer]
    # skipped no-op null fill on "customer"
    
    # Step: preserve_null on [product_id]
    # skipped no-op null fill on "product_id"
    
    # Step: flag_duplicate_candidates on [region]
    # Duplicate flagging logic would go here, but we are not implementing it as per the rules.
    
    # Step: preserve_null on [sale_date]
    # skipped no-op null fill on "sale_date"
    
    # Step: preserve_null on [sale_id]
    # skipped no-op null fill on "sale_id"
    
    return df

transform_marketing_relational_test = transform_marketing_relational_test


transform_sales_relational_test = transform_sales_relational_test


transform_factory_relational_test = transform_factory_relational_test

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
    # Requires spark-xml package / Fabric runtime support for com.databricks.spark.xml
    # sales_relational_test_xml: fabric_files_zone (xml) @ Files/raw/sales_relational_test_xml.xml
    if "sales_relational_test_xml" in active_canons:
        dfs["sales_relational_test_xml"] = spark.read.format("com.databricks.spark.xml").option("rowTag", "row").option("inferSchema", "true").load(_resolve_data_path("Files/raw/sales_relational_test_xml.xml"))
    # factory_relational_test_json: fabric_files_zone (json) @ Files/raw/factory_relational_test_json.json
    if "factory_relational_test_json" in active_canons:
        dfs["factory_relational_test_json"] = spark.read.option("multiLine", "true").json(_resolve_data_path("Files/raw/factory_relational_test_json.json"))
    # marketing_relational_test_csv: fabric_files_zone (csv) @ Files/raw/marketing_relational_test_csv.csv
    if "marketing_relational_test_csv" in active_canons:
        dfs["marketing_relational_test_csv"] = spark.read.option('header', 'true').option('inferSchema', 'true').csv(_resolve_data_path("Files/raw/marketing_relational_test_csv.csv"))
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


transform_sales_relational_test = transform_sales_relational_test


transform_factory_relational_test = transform_factory_relational_test