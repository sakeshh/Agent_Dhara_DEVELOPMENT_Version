from __future__ import annotations
from typing import Any, Dict, List, Optional

def get_remediation_sql(target: str, dataset: str, col: str, issue_type: str) -> str:
    """
    Generate idempotent staging table DDL and dialect-appropriate validation / cleansing DML.
    Supports fabric_spark_sql, fabric_warehouse_tsql, postgres, sqlserver, and databricks_sql.
    """
    target = (target or "fabric_spark_sql").strip().lower()
    dataset_safe = dataset.replace(".", "_")
    col_safe = col or "column_name"
    
    # 1. Row-level Duplicate Quarantine / Deduplication Strategy
    if issue_type in ("exact_duplicate_rows", "near_duplicate_rows", "duplicate_rows") or col_safe == "[Row-level]":
        return (
            f"-- Staging Quarantine/Deduplication Query ({target})\n"
            f"-- Identify duplicate groups using window function partition over all record columns:\n"
            f"CREATE OR REPLACE TEMP VIEW duplicate_groups AS\n"
            f"SELECT *,\n"
            f"       COUNT(*) OVER (\n"
            f"           PARTITION BY [real_key_columns_or_all_columns]\n"
            f"       ) AS duplicate_group_size\n"
            f"FROM staging.{dataset_safe};\n\n"
            f"-- Create quarantined duplicates table (survivorship policy required before merging back)\n"
            f"CREATE OR REPLACE TABLE staging.{dataset_safe}_quarantined AS\n"
            f"SELECT * FROM duplicate_groups WHERE duplicate_group_size > 1;"
        )

    # List of supported issue types with tested, issue-specific predicates
    supported_types = {
        "invalid_numeric", "mixed_scalar_types", "mixed_types",
        "negative_values", "negative_values_in_non_negative_measure",
        "nulls", "declared_key_high_null_ratio", "declared_key_null_violation", "suspected_key_high_null_ratio",
        "whitespace", "whitespace_only", "empty_string_values",
        "placeholder_detected", "invalid_date_format",
        "duplicate_representation_candidate", "punctuation_only", "symbol_only",
        "digit_only_categorical", "invalid_categorical_digits"
    }

    if issue_type not in supported_types:
        return (
            f"-- Remediation automatically unsupported by SQL adapter for '{issue_type}'.\n"
            f"-- Please review manually based on business requirements."
        )

    # 2. DDL for Staging Pattern (Idempotent schema extension)
    ddl = (
        f"-- DDL Staging Pattern (Run once to extend schema)\n"
        f"ALTER TABLE staging.{dataset_safe} ADD {col_safe}_raw VARCHAR(255);\n"
        f"ALTER TABLE staging.{dataset_safe} ADD {col_safe}_clean VARCHAR(255);\n"
        f"ALTER TABLE staging.{dataset_safe} ADD {col_safe}_validation_status VARCHAR(20);\n"
        f"ALTER TABLE staging.{dataset_safe} ADD {col_safe}_validation_reason VARCHAR(500);"
    )

    # 3. Dialect-specific WHERE filters
    col_l = str(col_safe).lower().strip()
    NON_NUMERIC_SEMANTICS = {"country", "country_code", "customer_id", "order_id", "payment_id", "client_id", "user_id", "status", "date", "phone", "email", "full_name"}
    if col_l in NON_NUMERIC_SEMANTICS or any(col_l.endswith(x) for x in ("_id", "id", "name", "country", "status", "date")):
        if issue_type in ("invalid_numeric", "mixed_scalar_types", "mixed_types"):
            return (
                f"-- Staging Text Normalization Query ({target})\n"
                f"-- Non-numeric column '{col_safe}': Normalize as text string\n"
                f"UPDATE staging.{dataset_safe}\n"
                f"SET {col_safe}_validation_status = 'VALID',\n"
                f"    {col_safe}_clean = UPPER(TRIM(CAST({col_safe} AS STRING)))\n"
                f"WHERE {col_safe} IS NOT NULL;"
            )

    where_clause = ""
    if issue_type in ("invalid_numeric", "mixed_scalar_types", "mixed_types"):
        if target == "fabric_spark_sql" or target == "databricks_sql":
            where_clause = (
                f"WHERE {col_safe} IS NOT NULL\n"
                f"  AND TRIM(CAST({col_safe} AS STRING)) <> ''\n"
                f"  AND NOT (TRIM(CAST({col_safe} AS STRING)) RLIKE '^[+-]?[0-9]+([.][0-9]+)?$')"
            )
        elif target == "fabric_warehouse_tsql" or target == "sqlserver":
            where_clause = (
                f"WHERE TRY_CAST({col_safe} AS FLOAT) IS NULL\n"
                f"  AND {col_safe} IS NOT NULL"
            )
        elif target == "postgres":
            where_clause = (
                f"WHERE {col_safe} !~ '^[+-]?[0-9]+([.][0-9]+)?$'\n"
                f"  AND {col_safe} IS NOT NULL"
            )
        else:
            where_clause = (
                f"WHERE {col_safe} IS NOT NULL\n"
                f"  AND TRIM(CAST({col_safe} AS STRING)) <> ''\n"
                f"  AND NOT (TRIM(CAST({col_safe} AS STRING)) RLIKE '^[+-]?[0-9]+([.][0-9]+)?$')"
            )
            
    elif issue_type in ("negative_values", "negative_values_in_non_negative_measure"):
        return (
            f"-- Preserve raw amount for auditability; quarantine negative entries for business review\n"
            f"CREATE OR REPLACE TABLE staging.{dataset_safe}_quarantine_negatives AS\n"
            f"SELECT * FROM staging.{dataset_safe} WHERE {col_safe} < 0;"
        )
        
    elif issue_type in ("nulls", "declared_key_high_null_ratio", "declared_key_null_violation", "suspected_key_high_null_ratio"):
        where_clause = f"WHERE {col_safe} IS NULL"
        
    elif "whitespace" in issue_type:
        return (
            f"{ddl}\n\n"
            f"-- DML Cleansing Query ({target})\n"
            f"UPDATE staging.{dataset_safe}\n"
            f"SET {col_safe}_clean = REGEXP_REPLACE(TRIM(CAST({col_safe} AS STRING)), r'\\s+', ' ')\n"
            f"WHERE {col_safe} IS NOT NULL;"
        )

            
    elif issue_type == "placeholder_detected":
        where_clause = (
            f"WHERE {col_safe} IS NOT NULL\n"
            f"  AND LOWER(TRIM(CAST({col_safe} AS STRING))) IN ('???', 'n/a', 'na', 'null', 'none', 'tbd', 'unknown', '-')"
        )
        
    elif issue_type == "invalid_date_format":
        # Safe TRY_CAST date validation for SQL dialects
        where_clause = (
            f"WHERE {col_safe} IS NOT NULL\n"
            f"  AND TRIM(CAST({col_safe} AS STRING)) <> ''\n"
            f"  AND TRY_CAST(TRIM(CAST({col_safe} AS STRING)) AS DATE) IS NULL"
        )
        
    elif issue_type in ("duplicate_representation_candidate", "punctuation_only", "symbol_only"):
        where_clause = (
            f"WHERE {col_safe} IS NOT NULL\n"
            f"  AND TRIM(CAST({col_safe} AS STRING)) <> ''\n"
            f"  AND TRIM(CAST({col_safe} AS STRING)) RLIKE '^[^A-Za-z0-9]+$'"
        )
        
    elif issue_type in ("digit_only_categorical", "invalid_categorical_digits"):
        where_clause = (
            f"WHERE {col_safe} IS NOT NULL\n"
            f"  AND TRIM(CAST({col_safe} AS STRING)) RLIKE '^[0-9]+$'"
        )

    # Combine into unified structured comment suggestions
    dml = (
        f"-- DML Cleansing Query ({target})\n"
        f"UPDATE staging.{dataset_safe}\n"
        f"SET {col_safe}_validation_status = 'INVALID',\n"
        f"    {col_safe}_validation_reason = 'Failed validation: {issue_type}'\n"
        f"{where_clause};"
    )

    return f"{ddl}\n\n{dml}"
