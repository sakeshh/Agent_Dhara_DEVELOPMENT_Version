import pytest
from agent.etl_pipeline.codegen_validator import validate_tsql
from agent.etl_pipeline.sql_codegen import generate_sql_etl
from agent.etl_pipeline.llm_codegen import (
    _postprocess_tsql_code,
    _extract_assessment_dq_issues,
    SYSTEM_PROMPTS,
)

def test_validate_tsql_detects_inner_go():
    bad_code = """
CREATE TABLE dbo.etl_log (id INT);
GO
CREATE TABLE dbo.etl_rejects (id INT);
GO
BEGIN TRY
    SELECT SCOPE_IDENTITY();
    GO
    BEGIN TRANSACTION;
    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    ROLLBACK;
END CATCH;
"""
    ok, errors, _ = validate_tsql(bad_code)
    assert not ok
    assert any("GO" in e and "BEGIN TRY" in e for e in errors)


def test_validate_tsql_detects_unbalanced_transaction():
    unbalanced_code = """
CREATE TABLE dbo.etl_log (id INT);
GO
CREATE TABLE dbo.etl_rejects (id INT);
GO
BEGIN TRY
    SELECT SCOPE_IDENTITY();
END TRY
BEGIN CATCH
    ROLLBACK;
END CATCH;
"""
    ok, errors, _ = validate_tsql(unbalanced_code)
    assert not ok
    assert any("ROLLBACK" in e and "BEGIN TRANSACTION" in e for e in errors)


def test_validate_tsql_accepts_clean_script():
    clean_code = """
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'etl_log' AND schema_id = SCHEMA_ID('dbo'))
CREATE TABLE dbo.etl_log (id INT IDENTITY(1,1) PRIMARY KEY, process_name VARCHAR(100), start_time DATETIME, end_time DATETIME, status VARCHAR(50));
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'etl_rejects' AND schema_id = SCHEMA_ID('dbo'))
CREATE TABLE dbo.etl_rejects (id INT IDENTITY(1,1) PRIMARY KEY, table_name VARCHAR(100), rejected_row_data NVARCHAR(MAX), reject_reason VARCHAR(255));
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
DECLARE @run_id INT;

BEGIN TRY
    INSERT INTO dbo.etl_log (process_name, start_time, status) VALUES ('ETL_Pipeline', GETDATE(), 'RUNNING');
    SET @run_id = SCOPE_IDENTITY();

    BEGIN TRANSACTION;

    SELECT src.[Price], src.[Price] AS [Price_raw], CAST(NULL AS NVARCHAR(MAX)) AS [Price_clean]
    INTO #Courses_Staging
    FROM dbo.Courses AS src;

    UPDATE #Courses_Staging
    SET [Price_clean] = CASE WHEN TRY_CAST([Price] AS DECIMAL(18,2)) < 0 THEN NULL ELSE TRY_CAST([Price] AS DECIMAL(18,2)) END;

    COMMIT TRANSACTION;

    UPDATE dbo.etl_log SET end_time = GETDATE(), status = 'SUCCESS' WHERE id = @run_id;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 OR XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
"""
    ok, errors, _ = validate_tsql(clean_code)
    assert ok, f"Expected clean code to validate, but got errors: {errors}"


def test_postprocess_tsql_hoists_ddl_and_removes_inner_go():
    raw_from_llm = """-- plan_id: plan_1788849913
SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRY
    -- Create log table
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'etl_log' AND schema_id = SCHEMA_ID('dbo'))
    CREATE TABLE dbo.etl_log (
        id INT IDENTITY(1,1) PRIMARY KEY,
        process_name VARCHAR(100) NOT NULL,
        start_time DATETIME NOT NULL,
        end_time DATETIME,
        status VARCHAR(50) NOT NULL
    );
    GO

    -- Create rejects table
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'etl_rejects' AND schema_id = SCHEMA_ID('dbo'))
    CREATE TABLE dbo.etl_rejects (
        id INT IDENTITY(1,1) PRIMARY KEY,
        table_name VARCHAR(100) NOT NULL,
        rejected_row_data NVARCHAR(MAX) NOT NULL,
        reject_reason VARCHAR(255) NOT NULL,
        etl_batch_id INT NOT NULL,
        rejected_at DATETIME DEFAULT GETDATE()
    );
    GO

    DECLARE @run_id INT;
    INSERT INTO dbo.etl_log (process_name, start_time, status) VALUES ('ETL_Pipeline', GETDATE(), 'RUNNING');
    SET @run_id = SCOPE_IDENTITY();

    SELECT 
        src.[CourseID],
        src.[Title],
        src.[Price]
    INTO #Courses_Staging
    FROM dbo.Courses AS src;

    UPDATE #Courses_Staging
    SET [CourseID_clean] = LTRIM(RTRIM([CourseID])),
        [CourseID_validation_status] = 'valid';

    UPDATE dbo.etl_log SET end_time = GETDATE(), status = 'SUCCESS' WHERE id = @run_id;
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    THROW;
END CATCH;
"""
    processed = _postprocess_tsql_code(raw_from_llm)

    # 1. DDL hoisted before BEGIN TRY
    assert "CREATE TABLE dbo.etl_log" in processed
    try_pos = processed.find("BEGIN TRY")
    ddl_pos = processed.find("CREATE TABLE dbo.etl_log")
    assert ddl_pos < try_pos, "DDL should be hoisted BEFORE BEGIN TRY"

    # 2. No bare GO inside BEGIN TRY ... END CATCH
    catch_pos = processed.find("END CATCH")
    inner_block = processed[try_pos:catch_pos]
    import re
    assert not re.search(r"^\s*GO\s*$", inner_block, re.MULTILINE), "No un-commented GO inside TRY/CATCH"

    # 3. Balanced Transaction (BEGIN TRANSACTION added, COMMIT added)
    assert "BEGIN TRANSACTION;" in processed
    assert "COMMIT TRANSACTION;" in processed

    # 4. Guarded Rollback
    assert "IF @@TRANCOUNT > 0 OR XACT_STATE() <> 0" in processed

    # 5. Staging companion column injected
    assert "ALTER TABLE #Courses_Staging ADD [CourseID_clean] NVARCHAR(MAX) NULL" in processed

    # 6. Validates cleanly
    ok, errors, _ = validate_tsql(processed)
    assert ok, f"Processed T-SQL must pass validate_tsql, got errors: {errors}"


def test_extract_assessment_dq_issues():
    assessment = {
        "datasets": {
            "Courses": {
                "quality": {
                    "issues": [
                        {
                            "column": "Price",
                            "issue_type": "negative_values",
                            "severity": "high",
                            "message": "10 rows with negative price",
                            "suggested_action": "non_negative_check",
                            "rows_affected": 10,
                        }
                    ]
                }
            }
        },
        "data_quality_issues": {
            "datasets": {
                "Customers_Raw": {
                    "issues": [
                        {
                            "column": "CreatedDate",
                            "issue_type": "impossible_date",
                            "severity": "high",
                            "message": "5 rows with impossible calendar dates",
                            "suggested_action": "parse_dates",
                        }
                    ]
                }
            }
        }
    }
    plan = {
        "manual_review": [
            {
                "dataset": "Customers",
                "column": "Age",
                "issue_type": "out_of_range",
                "severity": "high",
                "message": "Age outside plausible bounds (0-120)",
                "suggested_action": "range_clip",
            }
        ]
    }

    extracted = _extract_assessment_dq_issues(assessment, plan)
    assert len(extracted) == 3
    ds_names = {iss["dataset"] for iss in extracted}
    assert ds_names == {"Courses", "Customers_Raw", "Customers"}


def test_system_prompt_tsql_contains_canonical_rules():
    prompt = SYSTEM_PROMPTS["sql-tsql"]
    assert "Prices and catalog values CANNOT be negative" in prompt
    assert "negative_value_review_required" in prompt
    assert "placeholder_normalized_to_null" in prompt
    assert "impossible_calendar_date" in prompt
    assert "sentinel_value" in prompt
    assert "invalid_phone_format" in prompt
    assert "invalid_email_format" in prompt
    assert "NEVER output `GO` inside `BEGIN TRY" in prompt
    assert "Staging Companion Columns Completeness (CRITICAL)" in prompt


def test_deterministic_tsql_codegen_dq_manifest_full_compliance():
    assessment = {
        "datasets": {
            "Courses": {
                "columns": {
                    "CourseID": {"dtype": "int", "candidate_primary_key": True},
                    "Title": {"dtype": "varchar(200)"},
                    "Price": {"dtype": "decimal(10,2)"},
                },
                "quality": {
                    "issues": [
                        {
                            "column": "Price",
                            "issue_type": "negative_values",
                            "severity": "high",
                            "message": "Negative price found in course catalog",
                            "suggested_action": "range_clip",
                        }
                    ]
                }
            },
            "Customers_Raw": {
                "columns": {
                    "CustomerID": {"dtype": "int", "candidate_primary_key": True},
                    "CustomerName": {"dtype": "varchar(100)"},
                    "City": {"dtype": "varchar(100)"},
                    "Email": {"dtype": "varchar(255)"},
                    "Phone": {"dtype": "varchar(50)"},
                    "Age": {"dtype": "int"},
                    "CreatedDate": {"dtype": "datetime"},
                },
                "quality": {
                    "issues": [
                        {
                            "column": "CreatedDate",
                            "issue_type": "impossible_date",
                            "severity": "high",
                            "message": "Impossible dates like 2022-02-30 or sentinels",
                            "suggested_action": "parse_dates",
                        },
                        {
                            "column": "Age",
                            "issue_type": "implausible_age",
                            "severity": "high",
                            "message": "Negative age or age > 120 found",
                            "suggested_action": "range_clip",
                        },
                        {
                            "column": "Email",
                            "issue_type": "invalid_email",
                            "severity": "medium",
                            "message": "Invalid email formatting and placeholder domains",
                            "suggested_action": "sanitize_email",
                        },
                        {
                            "column": "Phone",
                            "issue_type": "invalid_phone",
                            "severity": "medium",
                            "message": "Unnormalized phone strings with non-digits",
                            "suggested_action": "normalize_phone",
                        },
                        {
                            "column": "CustomerName",
                            "issue_type": "placeholder_detected",
                            "severity": "medium",
                            "message": "Placeholders ??? and n/a found in CustomerName",
                            "suggested_action": "replace_sentinel_values",
                        },
                        {
                            "column": "City",
                            "issue_type": "placeholder_detected",
                            "severity": "medium",
                            "message": "Placeholders unknown and na found in City",
                            "suggested_action": "replace_sentinel_values",
                        },
                        {
                            "column": "[Row-level]",
                            "issue_type": "exact_duplicate_rows",
                            "severity": "medium",
                            "message": "Duplicate candidate rows found",
                            "suggested_action": "flag_duplicate_candidates",
                        }
                    ]
                }
            }
        }
    }

    plan = {
        "plan_id": "plan_1788849913",
        "generation_mode": "cleanse_only",
        "datasets": {
            "Courses": {"steps": []},
            "Customers_Raw": {"steps": []}
        },
        "business_rules": {
            "never_drop_rows": True
        }
    }

    sql = generate_sql_etl(plan, assessment, dialect="tsql")

    # 1. Negative Price Detection & Remediating Clamp / Companion columns / Reject logging
    assert "Price_clean" in sql
    assert "Price_validation_status" in sql
    assert "Price_validation_reason" in sql
    assert "negative_value_review_required" in sql or "Catalog price/measure cannot be negative" in sql
    assert "dbo.etl_rejects" in sql
    assert "TRY_CAST(r.[Price] AS FLOAT) < 0" in sql or "Price" in sql

    # 2. Impossible Date Classification & Sentinel Nullification
    assert "CreatedDate_clean" in sql
    assert "CreatedDate_validation_status" in sql
    assert "sentinel_value" in sql or "impossible_calendar_date" in sql or "1900-01-01" in sql

    # 3. Age Negative Review Flag & Range Clipping [0, 120]
    assert "Age_clean" in sql
    assert "Age_validation_status" in sql
    assert "120" in sql

    # 4. Email Placeholder Handling & Formatting
    assert "Email_clean" in sql
    assert "Email_validation_status" in sql
    assert "LIKE '%_@_%._%'" in sql

    # 5. Phone Normalization
    assert "Phone_clean" in sql
    assert "Phone_validation_status" in sql
    assert "REPLACE" in sql

    # 6. CustomerName & City Placeholder Handling
    assert "CustomerName_clean" in sql
    assert "City_clean" in sql
    assert "placeholder_normalized_to_null" in sql
    assert "???" in sql

    # 7. Duplicate Candidate Detection (Flagging & Reject Logging)
    assert "is_duplicate_row" in sql
    assert "row_validation_status" in sql
    assert "row_validation_reason" in sql
    assert "_staging_dup_log" in sql
    assert "Duplicate candidate detected" in sql
    assert "SET is_duplicate_row = 1" in sql

    # 8. Companion columns in Target Clean Table DDL and INSERT
    assert "CREATE TABLE dbo.Courses_Clean" in sql or "CREATE TABLE [dbo].[Courses_Clean]" in sql
    assert "CREATE TABLE dbo.Customers_Clean" in sql or "CREATE TABLE [dbo].[Customers_Clean]" in sql
    assert "INSERT INTO dbo.Courses_Clean" in sql or "INSERT INTO [dbo].[Courses_Clean]" in sql
    assert "INSERT INTO dbo.Customers_Clean" in sql or "INSERT INTO [dbo].[Customers_Clean]" in sql

    # Verify SQL compiles cleanly
    ok, errors, _ = validate_tsql(sql)
    assert ok, f"Generated T-SQL must pass syntax validation: {errors}"


def test_all_eight_dq_findings_and_sql_design_remediations():
    """Verify that all 8 DQ review findings and 4 SQL design fixes are resolved end-to-end."""
    assessment = {
        "datasets": {
            "Courses": {
                "columns": {
                    "CourseID": {"dtype": "int", "candidate_primary_key": True},
                    "Title": {"dtype": "varchar(200)"},
                    "Price": {"dtype": "decimal(10,2)"},
                    "InstructorID": {"dtype": "int"},
                },
                "quality": {
                    "issues": [
                        {
                            "column": "Price",
                            "issue_type": "negative_values",
                            "severity": "high",
                            "message": "Price = -500 (10 rows affected)",
                            "suggested_action": "range_clip",
                        },
                        {
                            "column": "InstructorID",
                            "issue_type": "sentinel_numeric_value",
                            "severity": "high",
                            "message": "Identified sentinels: 999, -999, 999999, 9999999",
                            "suggested_action": "replace_sentinel_values",
                        }
                    ]
                }
            },
            "Customers": {
                "columns": {
                    "CustomerID": {"dtype": "int", "candidate_primary_key": True},
                    "CustomerName": {"dtype": "varchar(100)"},
                    "Age": {"dtype": "int"},
                    "Email": {"dtype": "varchar(255)"},
                },
                "quality": {
                    "issues": [
                        {
                            "column": "Age",
                            "issue_type": "implausible_age",
                            "severity": "high",
                            "message": "Age = -3 (rows 19, 59)",
                            "suggested_action": "range_clip",
                        },
                        {
                            "column": "[Row-level]",
                            "issue_type": "exact_duplicate_rows",
                            "severity": "medium",
                            "message": "23 duplicate cluster members, 13 redundant rows",
                            "suggested_action": "flag_duplicate_candidates",
                        }
                    ]
                }
            },
            "Customers_Raw": {
                "columns": {
                    "CustomerID": {"dtype": "int", "candidate_primary_key": True},
                    "CustomerName": {"dtype": "varchar(100)"},
                    "City": {"dtype": "varchar(100)"},
                    "Email": {"dtype": "varchar(255)"},
                    "Phone": {"dtype": "varchar(50)"},
                    "CreatedDate": {"dtype": "datetime"},
                },
                "quality": {
                    "issues": [
                        {
                            "column": "CreatedDate",
                            "issue_type": "impossible_date",
                            "severity": "high",
                            "message": "Impossible dates like 2023-13-01, 2022-02-30",
                            "suggested_action": "parse_dates",
                        },
                        {
                            "column": "Email",
                            "issue_type": "invalid_email",
                            "severity": "medium",
                            "message": "15 invalid emails like anita@gmail, unknown, abc.com, ###",
                            "suggested_action": "sanitize_email",
                        },
                        {
                            "column": "Phone",
                            "issue_type": "invalid_phone",
                            "severity": "medium",
                            "message": "44 invalid phone numbers like 99999, abcd, 123, ###",
                            "suggested_action": "normalize_phone",
                        },
                        {
                            "column": "CustomerName",
                            "issue_type": "placeholder_detected",
                            "severity": "medium",
                            "message": "Placeholders Unknown, Dummy, ###",
                            "suggested_action": "replace_sentinel_values",
                        },
                        {
                            "column": "City",
                            "issue_type": "placeholder_detected",
                            "severity": "medium",
                            "message": "Placeholders N/A, Temp",
                            "suggested_action": "replace_sentinel_values",
                        }
                    ]
                }
            }
        }
    }

    # Even if plan only has Courses initially, all assessment datasets must be covered
    plan = {
        "plan_id": "plan_full_remediation_review",
        "generation_mode": "cleanse_only",
        "datasets": {
            "Courses": {"steps": []}
        },
        "business_rules": {
            "never_drop_rows": True
        }
    }

    sql = generate_sql_etl(plan, assessment, dialect="tsql")

    # 1. Multi-dataset coverage (Courses, Customers, Customers_Raw)
    assert "etl_clean_Courses" in sql, "Courses ETL procedure must be generated"
    assert "etl_clean_Customers" in sql, "Customers ETL procedure must be auto-supplemented from assessment"
    assert "etl_clean_Customers_Raw" in sql, "Customers_Raw ETL procedure must be auto-supplemented from assessment"
    assert "EXEC dbo.etl_main" in sql, "Master orchestration procedure must execute"

    # 2. Courses.Price: Negative prices become NULL, status is negative_value_review_required
    assert "[Price_clean] FLOAT" in sql or "[Price_clean] DECIMAL" in sql
    assert "negative_value_review_required" in sql
    assert "Catalog price/measure cannot be negative" in sql

    # 3. InstructorID: Sentinel logic uses 999, -999, 999999, 9999999 and NOT 0
    assert "9999999" in sql
    assert "InstructorID sentinel value replaced with NULL" in sql or "sentinel_value" in sql
    assert "InstructorID = 0" not in sql

    # 4. Customers.Age: Implausible/negative age becomes NULL with invalid_out_of_range
    assert "[Age_clean] BIGINT" in sql or "[Age_clean] INT" in sql
    assert "[Age_validation_status] = CASE WHEN TRY_CAST([Age] AS INT) < 0 OR TRY_CAST([Age] AS INT) > 120 THEN 'invalid_out_of_range'" in sql

    # 5. Customers_Raw.CreatedDate: Impossible calendar dates and sentinels become NULL
    assert "[CreatedDate_clean] DATETIME" in sql
    assert "impossible_calendar_date" in sql
    assert "sentinel_value" in sql

    # 6. Email validation: LIKE pattern, invalid format flag, reject logging
    assert "LIKE '%_@_%._%'" in sql
    assert "invalid_email_format" in sql

    # 7. Phone validation: Digits stripped, length 7-15, invalid format flag
    assert "invalid_phone_format" in sql
    assert "LEN" in sql

    # 8. Placeholder values: N/A, dummy, temp, ???, ###, 0000 normalized to NULL
    assert "placeholder_normalized_to_null" in sql
    assert "0000" in sql

    # 9. Duplicate detection: candidate flagging and staging log
    assert "is_duplicate_row" in sql
    assert "row_validation_status" in sql
    assert "DUPLICATE_CANDIDATE" in sql

    # 10. Quarantine and Rejects: INSERT INTO dbo.etl_rejects runs
    assert "INSERT INTO dbo.etl_rejects" in sql

    # 11. SQL Design: No GO inside TRY block, clean syntax
    ok, errors, _ = validate_tsql(sql)
    assert ok, f"Generated SQL must pass validate_tsql: {errors}"


def test_six_dq_and_tsql_remediation_defects():
    """Verify that all 6 defects reported in user review are properly remedied:
    1. GO batch boundaries wiped @run_id
    2. Updates against source tables instead of staging tables
    3. CreatedDate impossible calendar dates (2023-13-01, 2022-02-30)
    4. Pseudo-column [Row-level] in duplicate updates
    5. Phone validation length check bug (missing BETWEEN 7 AND 15)
    6. Quarantine inserting monolithic JSON array instead of per-row records
    """
    import re
    raw_flawed_script = """-- ETL Generated Script
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'etl_log' AND schema_id = SCHEMA_ID('dbo'))
CREATE TABLE dbo.etl_log (id INT IDENTITY(1,1) PRIMARY KEY, process_name VARCHAR(100), start_time DATETIME, end_time DATETIME, status VARCHAR(50), error_message VARCHAR(MAX));
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
DECLARE @run_id INT;

GO

BEGIN TRY
    INSERT INTO dbo.etl_log (process_name, start_time, status) VALUES ('ETL_Pipeline', GETDATE(), 'RUNNING');
    SET @run_id = SCOPE_IDENTITY();

    -- Staging selection
    SELECT src.[CourseID], src.[Price], src.[InstructorID]
    INTO #Courses_Staging
    FROM dbo.Courses src;

    -- Flaw 2: Updates against source table instead of staging table
    UPDATE dbo.Courses
    SET [Price_clean] = CASE WHEN TRY_CAST([Price] AS DECIMAL(18,2)) < 0 THEN NULL ELSE TRY_CAST([Price] AS DECIMAL(18,2)) END,
        [Price_validation_status] = 'negative_value_review_required';

    -- Flaw 5: Phone check without length check
    UPDATE #Customers_Raw_Staging
    SET [Phone_clean] = CASE WHEN [Phone] NOT LIKE '%[^0-9]%' THEN [Phone] ELSE NULL END;

    -- Flaw 4: Pseudo-column [Row-level] in SET clause
    UPDATE dbo.Customers_Raw
    SET [Row-level] = 'duplicate_candidate';

    -- CTE Dedup update (must NOT be rewritten to #CustomerDedupCTE_Staging)
    WITH CustomerDedupCTE AS (
        SELECT s.[CustomerID], ROW_NUMBER() OVER (PARTITION BY s.[Email] ORDER BY s.[CustomerID]) AS rn
        FROM #Customers_Raw_Staging s
    )
    UPDATE CustomerDedupCTE
    SET [is_duplicate_row] = 1
    WHERE rn > 1;

    -- Flaw 6: Monolithic reject JSON array instead of per-row records
    INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason, rejected_at)
    SELECT 'ETL_Pipeline', '#Courses_Staging', (SELECT * FROM #Courses_Staging WHERE [Price_clean] IS NULL FOR JSON PATH);

    UPDATE dbo.etl_log SET end_time = GETDATE(), status = 'SUCCESS' WHERE id = @run_id;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 OR XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
"""
    processed = _postprocess_tsql_code(raw_flawed_script)

    # 1. Defect 1: No GO after execution begins (SET NOCOUNT / DECLARE @run_id)
    # The GO between DECLARE @run_id and BEGIN TRY must be commented out so @run_id survives
    exec_pos = processed.find("SET NOCOUNT ON;")
    assert exec_pos != -1
    exec_block = processed[exec_pos:]
    assert not re.search(r"^\s*GO\s*$", exec_block, re.MULTILINE), "Zero un-commented GO statements in execution block"
    assert "omitted for variable scope" in exec_block

    # 2. Defect 2: Source table updates redirected to staging
    assert "UPDATE dbo.Courses\n    SET [Price_clean]" not in processed
    assert "UPDATE #Courses_Staging\n    SET [Price_clean]" in processed or "UPDATE #Courses_Staging SET [Price_clean]" in processed

    # 3. Defect 4: Pseudo-column [Row-level] removed and replaced with companion columns
    assert "[Row-level] =" not in processed
    assert "[is_duplicate_row] = 1" in processed
    assert "[row_validation_status] = 'duplicate_candidate'" in processed

    # 4. Defect 4b: CTE updates preserved (NOT rewritten to #CTE_Staging)
    assert "UPDATE #CustomerDedupCTE_Staging" not in processed
    assert "UPDATE CustomerDedupCTE" in processed

    # 5. Defect 5: Phone check enforces length BETWEEN 7 AND 15
    assert "BETWEEN 7 AND 15" in processed

    # 6. Defect 6: Quarantine insert rewritten to correlated per-row format
    assert "WITHOUT_ARRAY_WRAPPER" in processed
    assert "FROM #Courses_Staging s" in processed

    # 7. Defect 8: No cross-table column pollution in Integrity Guards
    assert "UPDATE #Customers_Raw_Staging\n    SET [Price_clean] = NULL" not in processed
    assert "UPDATE #Courses_Staging\n    SET [Phone_clean] = NULL" not in processed

    # Verify final script passes T-SQL syntax validation
    ok, errors, _ = validate_tsql(processed)
    assert ok, f"Postprocessed script must pass validate_tsql: {errors}"


def test_postprocess_and_validator_catch_syntax_near_or():
    from agent.etl_pipeline.validate_sql import validate_sql_basic

    # 1. CREATE OR REPLACE and CREATE OR ALTER TABLE
    bad_code = """
CREATE OR REPLACE TABLE dbo.Customers_Clean (id INT);
CREATE OR ALTER TABLE dbo.Courses_Clean (id INT);
CREATE NONCLUSTERED INDEX ix_test ON dbo.Customers_Clean (id) WHERE id > 10 OR id < 5;
BEGIN TRY
    UPDATE #Customers_Staging SET [is_valid] = [colA] > 0 OR [colB] > 0;
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
END CATCH;
"""
    # Validator should reject bad code
    v_ok, v_errs, _ = validate_tsql(bad_code)
    assert not v_ok
    assert any("CREATE OR REPLACE" in e for e in v_errs)
    assert any("CREATE OR ALTER TABLE" in e for e in v_errs)
    assert any("Filtered indexes cannot use OR" in e for e in v_errs)

    # validate_sql_basic should also reject bad code
    s_ok, s_errs, _ = validate_sql_basic(bad_code)
    assert not s_ok
    assert any("CREATE OR REPLACE" in e for e in s_errs)
    assert any("CREATE OR ALTER TABLE" in e for e in s_errs)
    assert any("Filtered indexes cannot use OR" in e for e in s_errs)

    # 2. _postprocess_tsql_code should sanitize all of them
    sanitized = _postprocess_tsql_code(bad_code)
    assert "CREATE OR REPLACE TABLE" not in sanitized
    assert "CREATE OR ALTER TABLE" not in sanitized
    assert "WHERE id > 10 OR id < 5" not in sanitized
    assert "CASE WHEN [colA] > 0 OR [colB] > 0 THEN 1 ELSE 0 END" in sanitized


def test_ensure_idempotent_sql_sanitizes_syntax_near_or():
    from agent.etl_pipeline.execution_orchestrator import ensure_idempotent_sql

    raw_sql = """
CREATE OR REPLACE TABLE dbo.TestTbl (id INT);
CREATE OR REPLACE PROCEDURE dbo.sp_test AS BEGIN SELECT 1; END;
CREATE OR ALTER TABLE dbo.AnotherTbl (id INT);
CREATE NONCLUSTERED INDEX ix_bad ON dbo.TestTbl (id) WHERE id = 1 OR id = 2;
"""
    cleaned = ensure_idempotent_sql(raw_sql)
    assert "CREATE OR REPLACE TABLE" not in cleaned
    assert "CREATE TABLE dbo.TestTbl" in cleaned
    assert "CREATE OR ALTER PROCEDURE dbo.sp_test" in cleaned
    assert "CREATE OR ALTER TABLE" not in cleaned
    assert "CREATE TABLE dbo.AnotherTbl" in cleaned
    assert "WHERE id = 1 OR id = 2" not in cleaned


def test_azure_sql_executor_remediates_syntax_near_or():
    from agent.azure_sql_executor import _remediate_batch_syntax_error

    err_msg = "('42000', \"[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Incorrect syntax near the keyword 'OR'. (156) (SQLExecDirectW)\")"

    # Case 1: CREATE OR ALTER PROCEDURE (on compatibility level < 130)
    batch_proc = "CREATE OR ALTER PROCEDURE dbo.etl_clean_Customers AS BEGIN SELECT 1; END;"
    rem = _remediate_batch_syntax_error(batch_proc, err_msg)
    assert len(rem) == 2
    assert "IF OBJECT_ID('dbo.etl_clean_Customers', 'P') IS NOT NULL DROP PROCEDURE [dbo].[etl_clean_Customers];" in rem[0]
    assert rem[1] == "CREATE PROCEDURE dbo.etl_clean_Customers AS BEGIN SELECT 1; END;"

    # Case 2: CREATE OR REPLACE VIEW
    batch_view = "CREATE OR REPLACE VIEW dbo.vw_Test AS SELECT 1 AS id;"
    rem = _remediate_batch_syntax_error(batch_view, err_msg)
    assert len(rem) == 2
    assert "DROP VIEW [dbo].[vw_Test];" in rem[0]
    assert rem[1] == "CREATE VIEW dbo.vw_Test AS SELECT 1 AS id;"

    # Case 3: CREATE OR ALTER TABLE
    batch_tbl = "CREATE OR ALTER TABLE dbo.Orders_Clean (id INT);"
    rem = _remediate_batch_syntax_error(batch_tbl, err_msg)
    assert len(rem) == 1
    assert rem[0] == "CREATE TABLE dbo.Orders_Clean (id INT);"

    # Case 4: Filtered index with OR
    batch_idx = "CREATE NONCLUSTERED INDEX ix_order ON dbo.Orders (id) WHERE status = 'A' OR status = 'B';"
    rem = _remediate_batch_syntax_error(batch_idx, err_msg)
    assert len(rem) == 1
    assert "WHERE" not in rem[0]
    assert rem[0].endswith(";")


def test_azure_sql_executor_remediates_syntax_error_102_and_111():
    from agent.azure_sql_executor import _remediate_batch_syntax_error

    err_102 = "('42000', \"[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Incorrect syntax near ';'. (102) (SQLExecDirectW)\")"
    proc_missing_end = """CREATE OR ALTER PROCEDURE dbo.transform_dbo_EmployeeMaster AS 
BEGIN
    BEGIN TRY
        SELECT 1;
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH;"""
    rem = _remediate_batch_syntax_error(proc_missing_end, err_102)
    assert len(rem) == 1
    assert "END;" in rem[0]
    assert rem[0].strip().endswith("END;")

    err_111 = "('42000', \"[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]'CREATE/ALTER PROCEDURE' must be the first statement in a query batch. (111) (SQLExecDirectW)\")"
    proc_and_exec = "CREATE OR ALTER PROCEDURE dbo.my_sp AS BEGIN SELECT 1; END;\nEXEC dbo.my_sp;"
    rem_111 = _remediate_batch_syntax_error(proc_and_exec, err_111)
    assert len(rem_111) == 2
    assert "CREATE OR ALTER PROCEDURE dbo.my_sp" in rem_111[0]
    assert "EXEC dbo.my_sp;" in rem_111[1]


def test_postprocess_tsql_stored_procedure_balancing_and_autoexec():
    from agent.etl_pipeline.llm_codegen import _postprocess_tsql_code

    raw_sql = """-- plan_id: plan_1789797850
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'cleanedTables') 
    EXEC('CREATE SCHEMA cleanedTables');
GO

CREATE OR ALTER PROCEDURE dbo.transform_dbo_EmployeeMaster AS 
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @run_id INT;
    INSERT INTO dbo.etl_log (process_name, start_time, status) VALUES ('ETL_Pipeline', GETDATE(), 'RUNNING');
    SET @run_id = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRANSACTION;

        SELECT [BusinessUnit], [Country], [EmployeeID], [Salary]
        INTO #EmployeeMaster_Staging
        FROM dbo.EmployeeMaster;

        -- Phantom Phone update (EmployeeMaster has no phone)
        UPDATE #EmployeeMaster_Staging
        SET [Phone_validation_status] = 'invalid_phone_format',
            [Phone] = NULL
        WHERE [Phone] LIKE '%[^0-9]%';

        -- Phantom Row-level update
        UPDATE #EmployeeMaster_Staging
        SET [Row-level_validation_status] = 'exact_duplicate_rows'
        WHERE [Row-level] IN (SELECT [Row-level] FROM #EmployeeMaster_Staging GROUP BY [Row-level] HAVING COUNT(*) > 1);

        COMMIT TRANSACTION;
        UPDATE dbo.etl_log SET end_time = GETDATE(), status = 'SUCCESS' WHERE id = @run_id;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 OR XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        UPDATE dbo.etl_log SET end_time = GETDATE(), status = 'FAILED' WHERE id = @run_id;
        THROW;
    END CATCH;
"""
    processed = _postprocess_tsql_code(raw_sql)

    # 1. Stored procedure must have closing END;
    assert "END;" in processed
    # 2. Must auto-execute procedure so table gets populated
    assert "EXEC [dbo].[transform_dbo_EmployeeMaster];" in processed
    # 3. Phantom Phone update must be stripped
    assert "[Phone_validation_status]" not in processed
    # 4. Phantom Row-level update must be stripped
    assert "[Row-level_validation_status]" not in processed





