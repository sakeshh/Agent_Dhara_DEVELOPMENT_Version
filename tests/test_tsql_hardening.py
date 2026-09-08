"""Unit and structural tests for PR 4.7 T-SQL Hardening & Transaction Safety.

Verifies:
- Staging table schema declares companion columns explicitly (raw, clean, validation_status, validation_reason)
- Native raw types preserved without lossy conversions
- Transaction and error semantics: SET XACT_ABORT ON, IF XACT_STATE() <> 0 ROLLBACK TRANSACTION, and THROW
- Explicit column lists for all INSERT targets
- Centralized validation precedence and single atomic CASE expression per field
- Safe bracket identifier quoting ([dbo].[Customers])
- Updated run audit logging (RUNNING -> SUCCESS or FAILED with @run_id)
"""

import pytest
from agent.etl_pipeline.sql_codegen import generate_sql_etl
from agent.etl_pipeline.composite_field_compiler import compile_field_work_package, create_field_work_package


def test_tsql_procedure_transaction_and_error_semantics():
    """T-SQL stored procedures include SET XACT_ABORT ON, CATCH with rollback and THROW."""
    plan = {
        "plan_id": "plan-tsql-01",
        "datasets": {
            "dbo.Customers": {
                "steps": [
                    {"action": "trim", "column": "Name"},
                    {"action": "sanitize_email", "column": "Email"},
                ]
            }
        },
    }
    assessment = {
        "datasets": {
            "dbo.Customers": {
                "columns": {
                    "CustomerID": {"dtype": "int", "candidate_primary_key": True},
                    "Name": {"dtype": "nvarchar"},
                    "Email": {"dtype": "nvarchar"},
                    "Age": {"dtype": "int"},
                }
            }
        }
    }

    sql = generate_sql_etl(plan, assessment, dialect="tsql")

    # Invariant 1: SET XACT_ABORT ON present
    assert "SET XACT_ABORT ON;" in sql

    # Invariant 2: Transaction controls
    assert "BEGIN TRY" in sql
    assert "BEGIN TRAN;" in sql or "BEGIN TRANSACTION;" in sql
    assert "COMMIT;" in sql or "COMMIT TRANSACTION;" in sql

    # Invariant 3: Error handling rethrows and rolls back active transaction
    assert "IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;" in sql
    assert "THROW;" in sql

    # Invariant 4: Audit log tracks single @run_id from RUNNING to SUCCESS or FAILED
    assert "INSERT INTO dbo.etl_log (process_name, start_time, status)" in sql
    assert "VALUES ('etl_clean_Customers', GETDATE(), 'RUNNING');" in sql
    assert "DECLARE @run_id INT = SCOPE_IDENTITY();" in sql
    assert "SET end_time = GETDATE(), status = 'SUCCEEDED'" in sql
    assert "SET end_time = GETDATE(), status = 'FAILED'" in sql
    assert "WHERE id = @run_id;" in sql


def test_tsql_companion_columns_and_explicit_inserts():
    """Staging and target table schemas declare companion columns and use explicit insert columns."""
    plan = {
        "plan_id": "plan-tsql-companion",
        "datasets": {
            "dbo.Customers": {
                "steps": [
                    {"action": "trim", "column": "Email"},
                ]
            }
        },
    }
    assessment = {
        "datasets": {
            "dbo.Customers": {
                "columns": {
                    "CustomerID": {"dtype": "int", "candidate_primary_key": True},
                    "Email": {"dtype": "nvarchar"},
                }
            }
        }
    }

    sql = generate_sql_etl(plan, assessment, dialect="tsql")

    # Invariant 1: Staging table explicitly declared with companion columns
    assert "CREATE TABLE #Customers_Staging" in sql
    assert "[Email_raw] NVARCHAR(MAX)" in sql
    assert "[Email_clean] NVARCHAR(MAX)" in sql
    assert "[Email_validation_status] VARCHAR(50)" in sql
    assert "[Email_validation_reason] VARCHAR(500)" in sql

    # Invariant 2: Clean target table contains companion columns
    assert "IF OBJECT_ID('dbo.Customers_Clean', 'U') IS NULL" in sql
    assert "[Email_raw]" in sql
    assert "[Email_clean]" in sql
    assert "[Email_validation_status]" in sql
    assert "[Email_validation_reason]" in sql

    # Invariant 3: Explicit column list in target INSERT (never INSERT INTO tbl SELECT *)
    assert "INSERT INTO [dbo].[Customers_Clean] (" in sql
    assert "SELECT " in sql


def test_tsql_composite_field_compilation():
    """Composite field compiler generates valid atomic T-SQL CASE expressions."""
    findings = [
        {
            "id": "rec-email-fmt",
            "dataset": "dbo.Customers",
            "column": "Email",
            "issue_type": "invalid_format",
            "action_id": "nullify_clean_value",
            "authority_source": "DETERMINISTIC_REGISTRY",
        }
    ]

    wp = create_field_work_package("dbo.Customers", "Email", findings, semantic_type="email")
    art = compile_field_work_package(wp, dialect="tsql")

    code = art.code
    assert code is not None
    # Atomic CASE expression with centralized precedence
    assert "CASE" in code
    assert "WHEN [Email] IS NULL OR LTRIM(RTRIM(CAST([Email] AS NVARCHAR(MAX)))) = '' THEN 'source_null'" in code
    assert "WHEN LTRIM(RTRIM(CAST([Email] AS NVARCHAR(MAX)))) NOT LIKE '%_@_%._%' THEN 'invalid_email_format'" in code
    assert "[Email_validation_status] = CASE" in code
    assert "[Email_clean] = CASE" in code
    assert "[Email_raw] = [Email]" in code

    # Output metadata and schema contracts
    assert "Email_raw" in art.output_columns
    assert "Email_clean" in art.output_columns
    assert "Email_validation_status" in art.output_columns
    assert "Email_validation_reason" in art.output_columns
    assert art.output_types.get("Email_validation_status") == "VARCHAR(50)"


def test_failure_log_exception_preserves_original_transformation_error():
    """A-05 dedicated test: nested failure logging post-rollback cannot swallow original error."""
    plan = {
        "plan_id": "plan-tsql-rollback-safety",
        "datasets": {
            "dbo.Transactions": {
                "steps": [
                    {"action": "trim", "column": "Description"},
                ]
            }
        },
    }
    assessment = {
        "datasets": {
            "dbo.Transactions": {
                "columns": {
                    "TxID": {"dtype": "int", "candidate_primary_key": True},
                    "Description": {"dtype": "nvarchar"},
                }
            }
        }
    }

    sql = generate_sql_etl(plan, assessment, dialect="tsql")

    # 1. Rollback occurs before failure logging attempt
    rollback_pos = sql.find("IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;")
    assert rollback_pos != -1

    # 2. Failure logging is nested within an inner BEGIN TRY ... BEGIN CATCH block
    catch_block_pos = sql.find("BEGIN CATCH", rollback_pos - 100)
    assert catch_block_pos != -1

    nested_try_pos = sql.find("BEGIN TRY", rollback_pos)
    assert nested_try_pos != -1, "Expected nested BEGIN TRY for audit logging inside main CATCH"

    update_log_pos = sql.find("UPDATE dbo.etl_log", nested_try_pos)
    assert update_log_pos != -1

    nested_catch_pos = sql.find("BEGIN CATCH", update_log_pos)
    assert nested_catch_pos != -1, "Expected nested BEGIN CATCH to absorb telemetry/audit exceptions"

    # 3. Outer THROW remains present after the nested block
    throw_pos = sql.find("THROW;", nested_catch_pos)
    assert throw_pos != -1, "Original exception must be rethrown by outer THROW"

    # 4. Strict sequence assertion: ROLLBACK -> Nested TRY -> UPDATE Log -> Nested CATCH -> THROW
    assert rollback_pos < nested_try_pos < update_log_pos < nested_catch_pos < throw_pos

