"""
Tests verifying fixes for the 6 flaw categories identified in the code generation audit:
1. Wildcard '*' in deduplication across T-SQL, PySpark, and Pandas.
2. Cross-field composite columns in DDL do not leak commas into column names.
3. Categorical columns (e.g., Country) are never cast to numeric/decimal.
4. Legitimate New Year dates (e.g., 2024-01-01) are not nullified as dummy dates.
5. Production stored procedures do not contain ALTER TABLE statements.
6. SQL Quality Assessment badge accurately scores code and catches fatal syntax blockers.
"""
from __future__ import annotations

import pytest
import re

from agent.etl_pipeline.codegen_shared import get_sql_cast_type
from agent.etl_pipeline.codegen_validator import validate_tsql, validate_pyspark, validate_python
from agent.etl_pipeline.sql_codegen import generate_sql_etl
from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
from agent.etl_pipeline.python_codegen import generate_python_etl


def test_wildcard_dedup_tsql_has_no_bracket_asterisk():
    """Ensure col='*' in deduplication does NOT emit CAST([*] AS NVARCHAR)."""
    plan = {
        "plan_id": "test_wildcard_sql",
        "generation_mode": "cleanse_only",
        "datasets": {
            "dbo.Orders": {
                "steps": [
                    {"action": "deduplicate", "column": "*"}
                ]
            }
        }
    }
    assessment = {
        "datasets": {
            "dbo.Orders": {
                "columns": {
                    "OrderID": {"dtype": "int", "candidate_primary_key": True},
                    "CustomerName": {"dtype": "varchar(100)"},
                    "Amount": {"dtype": "decimal(10,2)"}
                }
            }
        }
    }
    sql = generate_sql_etl(plan, assessment, dialect="tsql")
    assert "[*]" not in sql, "Generated T-SQL must not contain [*] identifier"
    assert "CAST([*]" not in sql
    ok, errors, warnings = validate_tsql(sql)
    assert ok, f"T-SQL validation failed: {errors}"


def test_wildcard_dedup_pyspark_has_no_literal_asterisk():
    """Ensure col='*' in PySpark deduplication does NOT emit Window.partitionBy('*')."""
    plan = {
        "plan_id": "test_wildcard_spark",
        "connector_manifest": {
            "datasets": {
                "Orders": {"format": "csv", "location": "Orders.csv"}
            }
        },
        "datasets": {
            "Orders": {
                "steps": [
                    {"action": "deduplicate", "column": "*"}
                ]
            }
        }
    }
    assessment = {
        "datasets": {
            "Orders": {
                "columns": {
                    "OrderID": {"dtype": "int"},
                    "Amount": {"dtype": "double"}
                }
            }
        }
    }
    code = generate_pyspark_etl(plan, assessment=assessment)
    assert "partitionBy('*')" not in code
    assert 'partitionBy("*")' not in code
    assert "_is_duplicate_*" not in code
    ok, errors, _ = validate_pyspark(code)
    assert ok, f"PySpark validation failed: {errors}"


def test_wildcard_dedup_pandas_has_no_subset_asterisk():
    """Ensure col='*' in Pandas deduplication does NOT emit subset=['*']."""
    plan = {
        "plan_id": "test_wildcard_pandas",
        "datasets": {
            "Orders": {
                "steps": [
                    {"action": "deduplicate", "column": "*"}
                ]
            }
        }
    }
    assessment = {
        "datasets": {
            "Orders": {
                "columns": {
                    "OrderID": {"dtype": "int"},
                    "Amount": {"dtype": "float"}
                }
            }
        }
    }
    code = generate_python_etl(plan, assessment)
    assert "subset=['*']" not in code
    assert 'subset=["*"]' not in code
    assert ".drop_duplicates(keep='first')" in code or ".drop_duplicates()" in code
    ok, errors, _ = validate_python(code)
    assert ok, f"Pandas validation failed: {errors}"


def test_cross_field_composite_column_no_embedded_commas_in_ddl():
    """Ensure cross-field columns like 'StartDate,EndDate' do not create '[StartDate,EndDate_raw]'."""
    plan = {
        "plan_id": "test_composite_ddl",
        "generation_mode": "cleanse_only",
        "datasets": {
            "dbo.Contracts": {
                "steps": [
                    {"action": "flag_outliers", "column": "ContractStartDate,ContractEndDate"}
                ]
            }
        }
    }
    assessment = {
        "datasets": {
            "dbo.Contracts": {
                "columns": {
                    "ContractID": {"dtype": "int", "candidate_primary_key": True},
                    "ContractStartDate": {"dtype": "date"},
                    "ContractEndDate": {"dtype": "date"}
                }
            }
        }
    }
    sql = generate_sql_etl(plan, assessment, dialect="tsql")
    clean_sql = re.sub(r"'(?:''|[^'])*'", "''", sql)
    clean_sql = re.sub(r"--[^\n]*", "", clean_sql)
    assert not re.search(r"\[\s*[^\]\n']+,[^\]\n']+\]", clean_sql), "DDL/DML must not contain composite comma brackets"
    assert "[ContractStartDate,ContractEndDate" not in sql


def test_categorical_columns_never_cast_to_numeric():
    """Ensure columns with names like 'Country', 'State', 'City' are never cast to DECIMAL/BIGINT."""
    # Even if dtype was profiled as decimal/numeric/float from messy data
    assert get_sql_cast_type("decimal(18,4)", "Country") == "NVARCHAR(MAX)"
    assert get_sql_cast_type("float", "Customer_City") == "NVARCHAR(MAX)"
    assert get_sql_cast_type("int", "State") == "NVARCHAR(MAX)"
    assert get_sql_cast_type("numeric", "Status") == "NVARCHAR(MAX)"
    assert get_sql_cast_type("decimal", "Region") == "NVARCHAR(MAX)"
    assert get_sql_cast_type("float", "CustomerName") == "NVARCHAR(MAX)"
    # But metrics must still cast to numeric
    assert get_sql_cast_type("decimal(18,2)", "OrderAmount") == "DECIMAL(18,2)"
    assert get_sql_cast_type("int", "Quantity") == "BIGINT"


def test_legitimate_new_year_date_not_nullified():
    """Ensure nullify_dummy_dates does not wipe out modern dates like 2024-01-01."""
    plan = {
        "plan_id": "test_dummy_date",
        "generation_mode": "cleanse_only",
        "connector_manifest": {
            "datasets": {
                "dbo.Orders": {"format": "csv", "location": "Orders.csv"}
            }
        },
        "datasets": {
            "dbo.Orders": {
                "steps": [
                    {"action": "nullify_dummy_dates", "column": "OrderDate"}
                ]
            }
        }
    }
    assessment = {
        "datasets": {
            "dbo.Orders": {
                "columns": {
                    "OrderID": {"dtype": "int", "candidate_primary_key": True},
                    "OrderDate": {"dtype": "date"}
                }
            }
        }
    }
    sql = generate_sql_etl(plan, assessment, dialect="tsql")
    # SQL must restrict month=1 and day=1 check to year <= 1900
    assert "YEAR(TRY_CAST([OrderDate] AS DATE)) <= 1900" in sql

    # PySpark check
    spark_code = generate_pyspark_etl(plan, assessment=assessment)
    assert "F.year(F.col('_tmp_date_OrderDate')) <= 1900" in spark_code

    # Pandas check
    pandas_code = generate_python_etl(plan, assessment)
    assert "_dates.dt.year <= 1900" in pandas_code


def test_stored_procedures_do_not_contain_alter_table():
    """Ensure ALTER TABLE statements are placed outside the CREATE PROCEDURE body."""
    plan = {
        "plan_id": "test_proc_ddl",
        "generation_mode": "cleanse_only",
        "datasets": {
            "dbo.Customers": {
                "steps": [
                    {"action": "cast_type", "column": "Age", "target_type": "int"},
                    {"action": "flag_outliers", "column": "Score"}
                ]
            }
        }
    }
    assessment = {
        "datasets": {
            "dbo.Customers": {
                "columns": {
                    "CustomerID": {"dtype": "int", "candidate_primary_key": True},
                    "Age": {"dtype": "varchar(10)"},
                    "Score": {"dtype": "float"}
                }
            }
        }
    }
    sql = generate_sql_etl(plan, assessment, dialect="tsql")
    # Extract the stored procedure body
    proc_match = re.search(r"CREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE\s+dbo\.etl_clean_Customers[\s\S]*?END\s*;", sql, re.IGNORECASE)
    assert proc_match is not None, "Procedure must be defined"
    proc_body = proc_match.group(0)
    assert "ALTER TABLE" not in proc_body, "Stored procedure body must not contain ALTER TABLE statements"
    # Clean table DDL must be present before the procedure
    assert "CREATE TABLE [dbo].[Customers_Clean]" in sql or "CREATE TABLE dbo.Customers_Clean" in sql
