"""
Comprehensive End-to-End Tests for Agent Dhara SQL Generation & Execution Pipeline Assembly.
Validates:
1. Canonical infrastructure schema deduplication (etl_log, etl_rejects, etl_watermark)
2. Stored procedure deduplication
3. THROW vs RAISERROR ODBC / Azure SQL compatibility
4. GO statement validation for single-batch Azure executor
5. Single master orchestrator enforcement (etl_main & EXEC etl_main)
6. Dataset isolation (TrainingEnrollments, EmployeeMaster, Courses, Orders)
7. End-to-end execution simulation for Azure SQL ODBC executor
"""

import re
import pytest
from agent.etl_pipeline.sql_codegen import (
    generate_sql_etl,
    generate_dataset_sql,
    generate_master_orchestrator,
)
from agent.etl_pipeline.sql_templates.infrastructure import (
    ETL_LOG_SCHEMA,
    ETL_REJECT_SCHEMA,
    ETL_WATERMARK_SCHEMA,
    get_infrastructure_ddl,
)
from agent.etl_pipeline.validate_sql import (
    validate_sql_assembly,
    validate_sql_basic,
    validate_for_execution,
)


@pytest.fixture
def multi_dataset_plan():
    return {
        "plan_id": "test_multi_dataset_001",
        "generation_mode": "full",
        "datasets": {
            "dbo.TrainingEnrollments": {
                "steps": [
                    {"action": "trim", "column": "CourseName"},
                    {"action": "sanitize_email", "column": "UserEmail"},
                    {"action": "replace_sentinel_values", "column": "Status"},
                ]
            },
            "dbo.EmployeeMaster": {
                "steps": [
                    {"action": "trim", "column": "EmployeeName"},
                    {"action": "normalize_phone", "column": "PhoneNumber"},
                    {"action": "range_clip", "column": "Salary", "params": {"lower_bound": 0.0}},
                ]
            },
            "dbo.Courses": {
                "steps": [
                    {"action": "trim", "column": "CourseTitle"},
                    {"action": "coerce_numeric", "column": "Credits"},
                ]
            },
            "dbo.Orders": {
                "steps": [
                    {"action": "trim", "column": "CustomerName"},
                    {"action": "range_clip", "column": "OrderAmount", "params": {"lower_bound": 0.0}},
                ]
            },
        },
        "business_rules": {
            "never_drop_rows": False,
        }
    }


@pytest.fixture
def multi_dataset_assessment():
    return {
        "datasets": {
            "dbo.TrainingEnrollments": {
                "columns": {
                    "EnrollmentID": {"dtype": "int", "candidate_primary_key": True},
                    "CourseName": {"dtype": "varchar"},
                    "UserEmail": {"dtype": "varchar", "semantic_type": "email"},
                    "Status": {"dtype": "varchar"},
                    "ModifiedDate": {"dtype": "datetime"},
                }
            },
            "dbo.EmployeeMaster": {
                "columns": {
                    "EmployeeID": {"dtype": "int", "candidate_primary_key": True},
                    "EmployeeName": {"dtype": "varchar"},
                    "PhoneNumber": {"dtype": "varchar"},
                    "Salary": {"dtype": "decimal"},
                    "UpdatedAt": {"dtype": "datetime"},
                }
            },
            "dbo.Courses": {
                "columns": {
                    "CourseID": {"dtype": "int", "candidate_primary_key": True},
                    "CourseTitle": {"dtype": "varchar"},
                    "Credits": {"dtype": "int"},
                }
            },
            "dbo.Orders": {
                "columns": {
                    "OrderID": {"dtype": "int", "candidate_primary_key": True},
                    "CustomerName": {"dtype": "varchar"},
                    "OrderAmount": {"dtype": "decimal"},
                }
            },
        }
    }


# ==============================================================================
# 1. DUPLICATE INFRASTRUCTURE DETECTION & ENFORCEMENT
# ==============================================================================

def test_duplicate_etl_log_detection():
    """Verify validate_sql_assembly detects duplicate dbo.etl_log DDL."""
    duplicate_sql = f"""
    {ETL_LOG_SCHEMA}
    GO
    {ETL_LOG_SCHEMA}
    GO
    """
    valid, issues = validate_sql_assembly(duplicate_sql)
    assert not valid
    assert any("Duplicate dbo.etl_log" in iss or "Duplicate CREATE TABLE" in iss for iss in issues)


def test_duplicate_etl_watermark_detection():
    """Verify validate_sql_assembly detects duplicate dbo.etl_watermark DDL."""
    duplicate_sql = f"""
    {ETL_WATERMARK_SCHEMA}
    GO
    {ETL_WATERMARK_SCHEMA}
    GO
    """
    valid, issues = validate_sql_assembly(duplicate_sql)
    assert not valid
    assert any("Duplicate dbo.etl_watermark" in iss or "Duplicate CREATE TABLE" in iss for iss in issues)


def test_canonical_infrastructure_single_emission(multi_dataset_plan, multi_dataset_assessment):
    """Verify generated multi-dataset SQL contains exactly one infrastructure definition each."""
    sql = generate_sql_etl(multi_dataset_plan, multi_dataset_assessment, dialect="tsql")

    # Counts in generated SQL
    assert sql.count("CREATE TABLE dbo.etl_log") == 1
    assert sql.count("CREATE TABLE dbo.etl_rejects") == 1
    assert sql.count("CREATE TABLE dbo.etl_watermark") == 1

    valid, issues = validate_sql_assembly(sql)
    assert valid, f"Assembly validation failed with issues: {issues}"


# ==============================================================================
# 2. DUPLICATE PROCEDURE DETECTION & ENFORCEMENT
# ==============================================================================

def test_duplicate_procedure_detection():
    """Verify validate_sql_assembly detects duplicate CREATE PROCEDURE definitions."""
    duplicate_proc_sql = """
    CREATE OR ALTER PROCEDURE dbo.etl_clean_Customers
    AS BEGIN
        SELECT 1;
    END;
    GO
    CREATE OR ALTER PROCEDURE dbo.etl_clean_Customers
    AS BEGIN
        SELECT 2;
    END;
    GO
    """
    valid, issues = validate_sql_assembly(duplicate_proc_sql)
    assert not valid
    assert any("Duplicate procedure definition" in iss for iss in issues)


def test_single_procedure_emission_per_dataset(multi_dataset_plan, multi_dataset_assessment):
    """Verify exactly one clean and transform procedure per dataset."""
    sql = generate_sql_etl(multi_dataset_plan, multi_dataset_assessment, dialect="tsql")

    for ds in ["TrainingEnrollments", "EmployeeMaster", "Courses", "Orders"]:
        assert sql.count(f"CREATE OR ALTER PROCEDURE dbo.etl_clean_{ds}") == 1
        assert sql.count(f"CREATE OR ALTER PROCEDURE dbo.etl_transform_{ds}") == 1


# ==============================================================================
# 3. SINGLE ORCHESTRATOR ENFORCEMENT
# ==============================================================================

def test_single_orchestrator_enforcement(multi_dataset_plan, multi_dataset_assessment):
    """Verify exactly one dbo.etl_main and one EXEC dbo.etl_main exist in output."""
    sql = generate_sql_etl(multi_dataset_plan, multi_dataset_assessment, dialect="tsql")

    assert sql.count("CREATE OR ALTER PROCEDURE dbo.etl_main") == 1
    assert len(re.findall(r"\bEXEC\s+dbo\.etl_main\b", sql)) == 1

    valid, issues = validate_sql_assembly(sql)
    assert valid, f"Assembly validation failed: {issues}"


def test_multiple_orchestrator_detection():
    """Verify validate_sql_assembly detects multiple etl_main procedures or multiple EXEC etl_main calls."""
    multi_orchestrator_sql = """
    CREATE OR ALTER PROCEDURE dbo.etl_main AS BEGIN SELECT 1; END;
    GO
    EXEC dbo.etl_main;
    GO
    CREATE OR ALTER PROCEDURE dbo.etl_main AS BEGIN SELECT 2; END;
    GO
    EXEC dbo.etl_main;
    GO
    """
    valid, issues = validate_sql_assembly(multi_orchestrator_sql)
    assert not valid
    assert any("Multiple" in iss and "etl_main" in iss for iss in issues)


# ==============================================================================
# 4. THROW VS RAISERROR COMPATIBILITY FOR AZURE SQL / ODBC EXECUTOR
# ==============================================================================

def test_throw_compatibility_validation_azure_sql_target(multi_dataset_plan, multi_dataset_assessment):
    """Verify that when execution_target='AZURE_SQL_EXECUTOR' or 'ODBC', THROW is replaced with RAISERROR."""
    sql_azure = generate_sql_etl(
        multi_dataset_plan,
        multi_dataset_assessment,
        dialect="tsql",
        execution_target="AZURE_SQL_EXECUTOR",
    )

    # In AZURE_SQL_EXECUTOR target, no THROW statement should be emitted
    assert "THROW;" not in sql_azure
    assert "RAISERROR(@err_msg, 16, 1);" in sql_azure
    assert "UPDATE dbo.etl_log" in sql_azure
    assert "SET status = 'FAILED'" in sql_azure

    # Validation must succeed without THROW violation
    valid, issues = validate_sql_assembly(sql_azure, execution_target="AZURE_SQL_EXECUTOR")
    assert valid, f"Validation issues: {issues}"


def test_throw_syntax_rejected_for_odbc_target():
    """Verify validate_sql_assembly rejects THROW when target is ODBC or AZURE_SQL_EXECUTOR."""
    sql_with_throw = """
    BEGIN TRY
        SELECT 1/0;
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
    """
    valid, issues = validate_sql_assembly(sql_with_throw, execution_target="AZURE_SQL_EXECUTOR")
    assert not valid
    assert any("Compatibility Violation: THROW syntax detected" in iss for iss in issues)


# ==============================================================================
# 5. GO STATEMENT VALIDATION FOR AZURE EXECUTOR SINGLE-BATCH MODE
# ==============================================================================

def test_go_statement_validation_single_batch_mode():
    """Verify validate_sql_assembly rejects residual GO statements when target is single batch."""
    sql_with_go = """
    SELECT 1;
    GO
    SELECT 2;
    """
    valid, issues = validate_sql_assembly(sql_with_go, execution_target="AZURE_SQL_SINGLE_BATCH")
    assert not valid
    assert any("Residual GO statement" in iss for iss in issues)


# ==============================================================================
# 6. PIPELINE ISOLATION (SINGLE DATASET VS MASTER ORCHESTRATOR)
# ==============================================================================

def test_dataset_isolation_training_enrollments(multi_dataset_plan, multi_dataset_assessment):
    """Verify generate_dataset_sql generates isolated SQL for TrainingEnrollments only."""
    sql_enrollments = generate_dataset_sql(
        "TrainingEnrollments",
        multi_dataset_plan,
        multi_dataset_assessment,
        dialect="tsql",
        execution_target="AZURE_SQL_EXECUTOR",
    )

    # Must contain TrainingEnrollments objects
    assert "TrainingEnrollments" in sql_enrollments
    assert "etl_clean_TrainingEnrollments" in sql_enrollments

    # Must NOT contain objects from other datasets
    assert "EmployeeMaster" not in sql_enrollments
    assert "Courses" not in sql_enrollments
    assert "Orders" not in sql_enrollments

    valid, issues = validate_sql_assembly(sql_enrollments, execution_target="AZURE_SQL_EXECUTOR")
    assert valid, f"Assembly validation failed: {issues}"


def test_dataset_isolation_employee_master(multi_dataset_plan, multi_dataset_assessment):
    """Verify generate_dataset_sql generates isolated SQL for EmployeeMaster only."""
    sql_employees = generate_dataset_sql(
        "EmployeeMaster",
        multi_dataset_plan,
        multi_dataset_assessment,
        dialect="tsql",
        execution_target="AZURE_SQL_EXECUTOR",
    )

    assert "EmployeeMaster" in sql_employees
    assert "etl_clean_EmployeeMaster" in sql_employees

    assert "TrainingEnrollments" not in sql_employees
    assert "Courses" not in sql_employees
    assert "Orders" not in sql_employees

    valid, issues = validate_sql_assembly(sql_employees, execution_target="AZURE_SQL_EXECUTOR")
    assert valid, f"Assembly validation failed: {issues}"


def test_master_orchestrator_generation(multi_dataset_plan, multi_dataset_assessment):
    """Verify generate_master_orchestrator produces standalone orchestrator referencing all datasets."""
    master_sql = generate_master_orchestrator(
        multi_dataset_plan,
        multi_dataset_assessment,
        dialect="tsql",
        execution_target="AZURE_SQL_EXECUTOR",
        dataset_names=["TrainingEnrollments", "EmployeeMaster", "Courses", "Orders"],
    )

    # Must contain infrastructure and etl_main
    assert "dbo.etl_log" in master_sql
    assert "CREATE OR ALTER PROCEDURE dbo.etl_main" in master_sql
    assert "EXEC dbo.etl_clean_TrainingEnrollments" in master_sql
    assert "EXEC dbo.etl_clean_EmployeeMaster" in master_sql
    assert "EXEC dbo.etl_clean_Courses" in master_sql
    assert "EXEC dbo.etl_clean_Orders" in master_sql
    assert "EXEC dbo.etl_main @load_type = 'FULL';" in master_sql

    # Must NOT use THROW
    assert "THROW;" not in master_sql
    assert "RAISERROR(@err_msg, 16, 1);" in master_sql

    valid, issues = validate_sql_assembly(master_sql, execution_target="AZURE_SQL_EXECUTOR")
    assert valid, f"Master orchestrator validation failed: {issues}"


# ==============================================================================
# 7. EXECUTIVE SCRIPT EXECUTION SIMULATION
# ==============================================================================

def test_executive_script_execution_simulation(multi_dataset_plan, multi_dataset_assessment):
    """
    Simulates Azure SQL ODBC execution environment:
    Splits on batch separators (GO), validates syntax and assembly of each batch.
    """
    from agent.azure_sql_executor import _split_sql_batches

    sql = generate_sql_etl(
        multi_dataset_plan,
        multi_dataset_assessment,
        dialect="tsql",
        execution_target="AZURE_SQL_EXECUTOR",
    )

    # 1. Assembly validation of full artifact
    valid, issues = validate_sql_assembly(sql, execution_target="AZURE_SQL_EXECUTOR")
    assert valid, f"Pre-execution assembly validation failed: {issues}"

    # 2. Split batches as Azure SQL Executor does
    batches = _split_sql_batches(sql)
    assert len(batches) > 1, "Script should contain multiple discrete batches separated by GO"

    # 3. Simulate execution readiness of each batch
    for idx, batch in enumerate(batches):
        # Stored procedures must not have multiple CREATE PROCEDURE in the same batch (Msg 111)
        proc_count = len(re.findall(r"\bCREATE\s+(?:OR\s+ALTER\s+)?(?:PROCEDURE|PROC)\b", batch, re.IGNORECASE))
        assert proc_count <= 1, f"Batch {idx} contains multiple CREATE PROCEDURE statements (Msg 111 violation)"

        # No residual GO inside any batch
        assert not re.search(r"^\s*GO\s*(?:--.*)?$", batch, flags=re.MULTILINE | re.IGNORECASE), f"Batch {idx} contains un-split GO"

        # No THROW statement in any batch
        assert "THROW;" not in batch, f"Batch {idx} contains THROW syntax incompatible with ODBC"

        # Check balanced parens in each batch
        exec_val = validate_for_execution(batch, execution_target="AZURE_SQL_EXECUTOR")
        assert exec_val["valid"], f"Batch {idx} failed validation: {exec_val['issues']}"


# ==============================================================================
# 8. FORBIDDEN GENERATOR TOKENS & DYNAMIC SQL LEAK PREVENTION
# ==============================================================================

def test_forbidden_generator_tokens_rejected_by_validator():
    """Verify validate_sql_assembly strictly rejects SQL containing raw generator tokens."""
    forbidden_samples = [
        "INSERT INTO ' + @clean_table_name + N' (id, val) VALUES (1, 'test')",
        "SELECT * FROM ' + @target_table + ' WHERE id = 1",
        "EXEC sp_executesql @sql = N'SELECT * FROM ' + @table_name + N';'",
        "INSERT INTO dbo.Users (Name) VALUES ('+ @clean_table_name +')",
        "UPDATE dbo.Orders SET val = 1 WHERE name = '+ @target_table'",
        "SELECT * FROM + @table_name +",
        "SELECT * FROM tbl WHERE col = 'abc' + N'",
    ]

    for sample in forbidden_samples:
        valid, issues = validate_sql_assembly(sample)
        assert not valid, f"Expected validator to reject SQL containing token, but passed: {sample}"
        assert any("Dynamic SQL Syntax Bug" in iss or "Unresolved generator token" in iss for iss in issues)


def test_forbidden_generator_tokens_raises_error_in_codegen():
    """Verify _check_forbidden_generator_tokens raises ValueError when raw generator tokens exist."""
    from agent.etl_pipeline.sql_codegen import _check_forbidden_generator_tokens

    bad_sql = "INSERT INTO ' + @clean_table_name + N' (id) VALUES (1);"
    with pytest.raises(ValueError, match="Unresolved dynamic SQL generator token"):
        _check_forbidden_generator_tokens(bad_sql)


def test_generated_sql_contains_no_raw_generator_tokens(multi_dataset_plan, multi_dataset_assessment):
    """Verify that end-to-end generated SQL for any dataset contains no raw generator tokens."""
    for ds_name in multi_dataset_plan["datasets"].keys():
        sql = generate_dataset_sql(
            ds_name,
            multi_dataset_plan,
            multi_dataset_assessment,
            dialect="tsql",
            execution_target="AZURE_SQL_EXECUTOR",
        )
        assert "+ @clean_table_name +" not in sql
        assert "+ @target_table +" not in sql
        assert "+ @table_name +" not in sql
        assert "+ N'" not in sql
        assert "'+ @clean_table_name" not in sql
        assert "'+ @target_table" not in sql
        assert "'+ @table_name" not in sql
        assert "' + @clean_table_name" not in sql
        assert "' + @target_table" not in sql
        assert "' + @table_name" not in sql
        assert "@clean_table_name" not in sql
        assert "@target_table" not in sql
        assert "@table_name" not in sql

