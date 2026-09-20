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


def test_composite_deduplicate_and_transforms_no_comma_brackets_in_ddl_dml():
    """Replicate reviewer's exact finding: [EmployeeID, EffectiveFrom_raw] and [EmployeeID, EffectiveFrom_clean]."""
    plan = {
        "plan_id": "test_composite_leak",
        "generation_mode": "cleanse_only",
        "datasets": {
            "dbo.EmployeeHistory": {
                "steps": [
                    {"action": "deduplicate", "column": "EmployeeID, EffectiveFrom"},
                    {"action": "trim", "column": "EmployeeID, EffectiveFrom"},
                    {"action": "replace_sentinel_values", "column": "EmployeeID, EffectiveFrom"}
                ]
            }
        }
    }
    assessment = {
        "datasets": {
            "dbo.EmployeeHistory": {
                "columns": {
                    "EmployeeID": {"dtype": "int", "candidate_primary_key": True},
                    "EffectiveFrom": {"dtype": "date"},
                    "Department": {"dtype": "varchar(50)"}
                }
            }
        }
    }
    sql = generate_sql_etl(plan, assessment, dialect="tsql")
    # Verify exact blocker reported by reviewer is absent:
    assert "[EmployeeID, EffectiveFrom_raw]" not in sql
    assert "[EmployeeID, EffectiveFrom_clean]" not in sql
    assert "[EmployeeID, EffectiveFrom" not in sql
    
    clean_sql = re.sub(r"'(?:''|[^'])*'", "''", sql)
    clean_sql = re.sub(r"--[^\n]*", "", clean_sql)
    assert not re.search(r"\[\s*[^\]\n']+,[^\]\n']+\]", clean_sql), f"DDL/DML must not contain bracketed identifiers with commas"
    ok, errors, warnings = validate_tsql(sql)
    assert ok, f"T-SQL validation failed: {errors}"


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


def test_schema_grounding_and_entity_boundary_enforcement():
    """Verify non-existent columns (EmploymentType) and cross-entity attributes (OfficeLocation) are not emitted."""
    plan = {
        "plan_id": "test_schema_grounding",
        "generation_mode": "cleanse_only",
        "datasets": {
            "dbo.Dhara_Employee": {
                "steps": [
                    {"action": "trim", "column": "EmployeeName"},
                    {"action": "sanitize_email", "column": "Email"},
                    # Non-existent column in this dataset:
                    {"action": "replace_sentinel_values", "column": "EmploymentType"},
                    # Cross-table leaked attribute from OrgReference:
                    {"action": "replace_sentinel_values", "column": "OfficeLocation"},
                    {"action": "cast_type", "column": "ReferenceID", "target_type": "int"}
                ]
            }
        }
    }
    assessment = {
        "datasets": {
            "dbo.Dhara_Employee": {
                "columns": {
                    "EmployeeID": {"dtype": "int", "candidate_primary_key": True},
                    "EmployeeName": {"dtype": "varchar(100)"},
                    "Email": {"dtype": "varchar(100)"}
                }
            }
        },
        "data_quality_issues": {
            "datasets": {
                "dbo.Dhara_Employee": {
                    "issues": [
                        # Issue on non-existent column
                        {"column": "EmploymentType", "type": "null_value", "severity": "high", "id": "ISSUE-orphan-1"},
                        # Leaked issue from another entity
                        {"column": "OfficeLocation", "type": "placeholder", "severity": "low", "id": "ISSUE-leaked-2"}
                    ]
                }
            }
        }
    }
    sql = generate_sql_etl(plan, assessment, dialect="tsql")

    # Schema Grounding: EmploymentType is not in Dhara_Employee schema -> must NOT be emitted
    assert "EmploymentType_validation_status" not in sql, "Non-existent column must not have validation status"
    assert "EmploymentType_raw" not in sql, "Non-existent column must not have companion raw column"
    assert "EmploymentType" not in sql, "Non-existent column must not appear in generated SQL"

    # Entity Boundary: OfficeLocation and ReferenceID must NOT appear in Dhara_Employee procedure
    assert "OfficeLocation" not in sql, "Cross-table leaked attribute must not appear in Dhara_Employee"
    assert "ReferenceID" not in sql, "Cross-table leaked attribute must not appear in Dhara_Employee"

    # Legitimate columns must be present and valid
    assert "EmployeeName_raw" in sql
    assert "Email_validation_status" in sql
    ok, errors, warnings = validate_tsql(sql)
    assert ok, f"T-SQL validation failed: {errors}"


def test_validation_conflict_resolution_and_duplicate_suppression():
    """Verify duplicate issue_ids are suppressed and validation reasons accumulate without overwriting."""
    plan = {
        "plan_id": "test_conflict_and_dedup",
        "generation_mode": "cleanse_only",
        "datasets": {
            "dbo.Users": {
                "steps": [
                    {"action": "sanitize_email", "column": "Email", "issue_id": "ISSUE-50af3a64"},
                    # Duplicate step with same issue_id:
                    {"action": "sanitize_email", "column": "Email", "issue_id": "ISSUE-50af3a64"},
                    {"action": "trim", "column": "Email"}
                ]
            }
        }
    }
    assessment = {
        "datasets": {
            "dbo.Users": {
                "columns": {
                    "UserID": {"dtype": "int", "candidate_primary_key": True},
                    "Email": {"dtype": "varchar(200)"}
                }
            }
        }
    }
    sql = generate_sql_etl(plan, assessment, dialect="tsql")

    # Verify duplicate issue suppression: only one sanitize_email transformation generated
    assert sql.count("COVERS: ISSUE-50af3a64") == 1
    assert sql.count("[Email_clean] =") == 1
    assert sql.count("[Email_validation_status] = CASE WHEN") == 1

    # Verify conflict resolution: accumulation pattern is used
    assert "[Email_validation_reason] IS NULL" in sql
    assert "+ '; '" in sql or "+ ';'" in sql, "Validation reason accumulation pattern must be present"
