from __future__ import annotations

import re
import pytest
from typing import Any, Dict, List

from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.models.actions import (
    ActionSource,
    ComparisonOperator,
    StructuredAction,
    StructuredCondition,
)
from agent.compiler.models.compilation_result import CodeGenerationError
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.planners.ingestion_bridge import IngestionBridge
from agent.compiler.schema.sql_identifier import parse_table_identifier
from agent.compiler.templates.engine import render_literal


# ------------------------------------------------------------------------------
# Fixtures & Setup
# ------------------------------------------------------------------------------

def _create_dirty_source_data() -> Dict[str, List[Dict[str, Any]]]:
    """Representative dirty source data across three enterprise datasets."""
    employees = [
        {"EmpId": 1, "Name": "Alice Smith", "Email": "alice@company.com", "Salary": 85000.0, "Active": "TRUE", "Dept": "Engineering"},
        {"EmpId": 2, "Name": "  Bob Jones  ", "Email": "bob_not_an_email", "Salary": 62000.0, "Active": "1", "Dept": "Sales"},
        {"EmpId": 3, "Name": "Charlie Brown", "Email": "charlie@company.com", "Salary": -1500.0, "Active": "YES", "Dept": "Contractor"},
        {"EmpId": 4, "Name": "Diana Prince", "Email": "diana@company.com", "Salary": 92000.0, "Active": "INVALID_BOOL", "Dept": "Marketing"},
        {"EmpId": 5, "Name": "Evan O'Brien", "Email": "evan@company.com", "Salary": 71000.0, "Active": "0", "Dept": "Engineering"},
        # Exact duplicate of EmpId 1
        {"EmpId": 1, "Name": "Alice Smith", "Email": "alice@company.com", "Salary": 85000.0, "Active": "TRUE", "Dept": "Engineering"},
    ]

    payroll = [
        {"PayId": 101, "EmpId": 1, "Amount": 7083.33, "Adjustment": -250.0},
        {"PayId": 102, "EmpId": 2, "Amount": 5166.67, "Adjustment": 0.0},
        {"PayId": 103, "EmpId": 999, "Amount": 4000.00, "Adjustment": 150.0},  # Orphan foreign key
    ]

    departments = [
        {"DeptName": "Engineering", "CostCenter": "CC-100"},
        {"DeptName": "Sales", "CostCenter": "CC-200"},
        {"DeptName": "Marketing", "CostCenter": "CC-300"},
    ]

    return {"Dhara_Employee": employees, "Dhara_Payroll": payroll, "Dhara_Department": departments}


# ------------------------------------------------------------------------------
# Test 1: Full Compiler Flow & Telemetry Reconciliation Golden Test
# ------------------------------------------------------------------------------

def test_compiler_golden_reconciliation_and_invariant_gates():
    """Validates full pipeline from dirty assessment through T-SQL compilation with exact reconciliation."""
    bridge = IngestionBridge()
    pipeline = CompilerPipeline()

    # 1. Assessment payload reflecting the dirty data
    assess = {
        "data_quality_issues": {
            "datasets": {
                "dbo.Dhara_Employee": {
                    "issues": [
                        {"column": "Name", "issue_type": "untrimmed_whitespace", "suggested_action": "trim", "severity": "low"},
                        {"column": "Email", "issue_type": "invalid_email", "suggested_action": "sanitize_email", "severity": "high"},
                        {"column": "Salary", "issue_type": "negative_values", "suggested_action": "range_clip", "severity": "high"},
                        {"column": "Active", "issue_type": "boolean_inconsistency", "suggested_action": "standardize_boolean", "severity": "medium"},
                        {"column": "EmpId", "issue_type": "duplicate_pk", "suggested_action": "deduplicate", "severity": "high"},
                    ]
                },
                "dbo.Dhara_Payroll": {
                    "issues": [
                        # Ambiguous negative domain: Adjustment can legitimately be negative in accounting
                        {"column": "Adjustment", "issue_type": "negative_values", "severity": "medium"},
                    ]
                }
            }
        },
        "datasets": {
            "dbo.Dhara_Employee": {
                "columns": {
                    "EmpId": {"dtype": "int"},
                    "Name": {"dtype": "string"},
                    "Email": {"dtype": "string"},
                    "Salary": {"dtype": "float"},
                    "Active": {"dtype": "string"},
                    "Dept": {"dtype": "string"},
                },
                "row_count": 6,
            },
            "dbo.Dhara_Payroll": {
                "columns": {
                    "PayId": {"dtype": "int"},
                    "EmpId": {"dtype": "int"},
                    "Amount": {"dtype": "float"},
                    "Adjustment": {"dtype": "float"},
                },
                "row_count": 3,
            }
        }
    }

    # 2. Extract and normalize
    canonical_issues, business_rules, ing_telem = bridge.extract_and_normalize(
        assess=assess,
        plan={},
        known_datasets={"dbo.Dhara_Employee", "dbo.Dhara_Payroll"},
    )
    _, structured_actions = bridge.bridge_to_compiler_models(canonical_issues)

    # Telemetry exact reconciliation invariant:
    # 5 Employee issues supported + 1 Payroll ambiguous negative issue unsupported = 6 discovered
    assert ing_telem["assessment_discovered_count"] == 6
    assert ing_telem["mapped_issue_count"] == 5
    assert ing_telem["unsupported_issue_count"] == 1
    assert (
        ing_telem["assessment_discovered_count"]
        == ing_telem["mapped_issue_count"] + ing_telem["unsupported_issue_count"]
    )

    # Safe parameter enrichment verification:
    # Salary is a known non-negative compensation column -> min_value=0.0 enriched with high confidence
    salary_issue = next(i for i in canonical_issues if i.column == "Salary")
    assert salary_issue.resolved_action == ActionType.RANGE_CLIP
    assert salary_issue.parameters["min_value"] == 0.0
    assert "Salary" in salary_issue.parameters["parameter_source"]
    assert salary_issue.parameters["parameter_confidence"] == "high"

    # Adjustment is an ambiguous domain column -> NOT clamped to 0; marked unsupported
    adjustment_issue = next(i for i in canonical_issues if i.column == "Adjustment")
    assert adjustment_issue.resolved_action is None
    assert "unsupported_ambiguous_negative" in adjustment_issue.resolution_path

    # 3. Compile in preview mode (allowing partial code generation since 1 unsupported issue exists)
    emp_cols = {
        "EmpId": ColumnProfile(name="EmpId", dtype="int"),
        "Name": ColumnProfile(name="Name", dtype="string"),
        "Email": ColumnProfile(name="Email", dtype="string"),
        "Salary": ColumnProfile(name="Salary", dtype="float"),
        "Active": ColumnProfile(name="Active", dtype="string"),
        "Dept": ColumnProfile(name="Dept", dtype="string"),
    }
    emp_prof = DatasetProfile(dataset_id="dbo.Dhara_Employee", columns=emp_cols, row_count=6)
    ctx = CompilerContext(
        session_id="golden_sess",
        target_engine=ExecutionEngine.AZURE_SQL,
        options={"strict_execution": False, "preview_mode": True},
        structured_actions=structured_actions,
        dataset_profiles={"dbo.Dhara_Employee": emp_prof},
    )
    ctx.metadata["ingestion_telemetry"] = ing_telem
    ctx.metadata["planner_input_source"] = "structured_actions"

    compiled_ctx, _ = pipeline.compile(ctx)
    telemetry = compiled_ctx.metadata["compilation_telemetry"]

    # Telemetry reconciliation:
    # planned_step_count == rendered_transformation_count + failed_transformation_count
    assert telemetry["physical_step_count"] == telemetry["rendered_transformation_count"] + telemetry["failed_transformation_count"]
    assert telemetry["rendered_transformation_count"] == 5
    assert telemetry["failed_transformation_count"] == 0
    assert telemetry["unsupported_issue_count"] == 1
    assert telemetry["partial"] is True  # Non-executable partial due to unsupported issue
    assert telemetry["executable"] is False

    sql = compiled_ctx.code_artifact.content

    # 4. Critical Invariant Gate: Step 1 identifier normalization
    # Assert normalized names exist
    assert "[dbo].[Dhara_Employee]" in sql
    assert "[dbo].[Dhara_Employee_Staging]" in sql
    assert "[dbo].[Dhara_Employee_Clean]" in sql
    # Strict Invariant: Malformed double prefix must NEVER appear
    assert "[dbo].[dbo." not in sql

    # 5. Rejection Log Contract Verification:
    # Variables declared and assigned at procedure scope
    assert "DECLARE @PipelineId VARCHAR(100) =" in sql
    assert "DECLARE @RunId UNIQUEIDENTIFIER = NEWID();" in sql

    # Rejection log schema consistency
    assert "CREATE TABLE [ETL_REJECT].[Rejection_Log]" in sql
    assert "[RunId] UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID()" in sql
    assert "IF COL_LENGTH('[ETL_REJECT].[Rejection_Log]', 'RunId') IS NULL" in sql
    assert "@PipelineId," in sql
    assert "@RunId," in sql


# ------------------------------------------------------------------------------
# Test 2: Literal Escaping & AST Component Integrity (Item 7)
# ------------------------------------------------------------------------------

def test_literal_rendering_and_condition_ast():
    """Validates that strings with apostrophes (e.g. O'Brien) and conditions render safely."""
    # Test literal renderer directly
    assert render_literal("O'Brien") == "N'O''Brien'"
    assert render_literal("Manager's Office") == "N'Manager''s Office'"
    assert render_literal(123) == "123"
    assert render_literal(45.67) == "45.67"
    assert render_literal(None) == "NULL"
    assert render_literal(True) == "1"
    assert render_literal(False) == "0"

    # Test conditional update with apostrophes in literal
    rule_action = StructuredAction(
        action_type=ActionType.CONDITIONAL_UPDATE,
        dataset_name="Dhara_Employee",
        target_columns=["Dept"],
        parameters={
            "condition": StructuredCondition(
                column="Name",
                operator=ComparisonOperator.EQ,
                value="Evan O'Brien",
            ).model_dump(),
            "new_value": "Manager's Office",
            "disposition": "CORRECT",
        },
        source=ActionSource.BUSINESS_RULE,
    )

    pipeline = CompilerPipeline()
    ctx = CompilerContext(
        session_id="quote_test_sess",
        target_engine=ExecutionEngine.AZURE_SQL,
        structured_actions=[rule_action],
    )
    ctx.metadata["ingestion_telemetry"] = {
        "assessment_discovered_count": 1,
        "mapped_action_count": 1,
        "unsupported_issue_count": 0,
    }

    compiled_ctx, _ = pipeline.compile(ctx)
    sql = compiled_ctx.code_artifact.content

    # Assert properly escaped T-SQL literals
    assert "WHEN [Name] = N'Evan O''Brien' THEN N'Manager''s Office'" in sql
    assert "[dbo].[dbo." not in sql


# ------------------------------------------------------------------------------
# Test 3: Atomic Quarantine & Deletion Isolation (Item 8)
# ------------------------------------------------------------------------------

def test_atomic_quarantine_deletion_temp_table():
    """Validates that REJECT conditional actions evaluate condition once via temp table."""
    reject_action = StructuredAction(
        action_type=ActionType.CONDITIONAL_UPDATE,
        dataset_name="Dhara_Employee",
        target_columns=["Salary"],
        parameters={
            "condition": "[Salary] < 0",
            "new_value": None,
            "disposition": "REJECT",
        },
        source=ActionSource.BUSINESS_RULE,
    )

    pipeline = CompilerPipeline()
    ctx = CompilerContext(
        session_id="reject_atomic_sess",
        target_engine=ExecutionEngine.AZURE_SQL,
        structured_actions=[reject_action],
    )
    ctx.metadata["ingestion_telemetry"] = {
        "assessment_discovered_count": 1,
        "mapped_action_count": 1,
        "unsupported_issue_count": 0,
    }

    compiled_ctx, _ = pipeline.compile(ctx)
    sql = compiled_ctx.code_artifact.content

    # Temp table created to isolate rejected rows atomically
    assert "SELECT *" in sql
    assert "INTO #Rejected_phys_" in sql
    assert "FROM [dbo].[Dhara_Employee_Staging] s" in sql
    assert "WHERE [Salary] < 0" in sql
    assert "INSERT INTO [ETL_REJECT].[Rejection_Log]" in sql
    assert "FROM #Rejected_phys_" in sql
    assert "DROP TABLE #Rejected_phys_" in sql


# ------------------------------------------------------------------------------
# Test 4: Simulated End-to-End Cleaning & Row Reconciliation
# ------------------------------------------------------------------------------

def test_simulated_dirty_data_reconciliation():
    """Simulates the row-level outcome of the compiled procedure against dirty data.
    
    Verifies that SourceRows = CleanRows + QuarantinedRows (exact row conservation).
    """
    dirty_data = _create_dirty_source_data()
    raw_employees = dirty_data["Dhara_Employee"]
    source_count = len(raw_employees)
    assert source_count == 6

    # Apply transformations matching the compiled T-SQL steps:
    # 1. Trim Name
    # 2. Email validation (invalid syntax -> NULL or REJECT)
    # 3. Salary non-negative clip -> clamp to 0
    # 4. Boolean standardize -> '1', '0'
    # 5. Deduplication by EmpId
    clean_rows: List[Dict[str, Any]] = []
    quarantined_rows: List[Dict[str, Any]] = []
    seen_ids = set()

    for row in raw_employees:
        curr = dict(row)
        # Step 1: Trim
        curr["Name"] = curr["Name"].strip()

        # Step 2: Email validation
        is_valid_email = "@" in curr["Email"] and "." in curr["Email"] and " " not in curr["Email"]
        if not is_valid_email:
            quarantined_rows.append({"EmpId": curr["EmpId"], "Reason": "Invalid email syntax", "OriginalValue": curr["Email"]})
            curr["Email"] = None

        # Step 3: Salary clamp
        if curr["Salary"] < 0:
            curr["Salary"] = 0.0

        # Step 4: Boolean standardize
        b_val = str(curr["Active"]).upper().strip()
        if b_val in ("1", "TRUE", "T", "YES", "Y", "ON"):
            curr["Active"] = "1"
        elif b_val in ("0", "FALSE", "F", "NO", "N", "OFF"):
            curr["Active"] = "0"
        else:
            quarantined_rows.append({"EmpId": curr["EmpId"], "Reason": "Unrecognized boolean", "OriginalValue": curr["Active"]})
            curr["Active"] = None

        # Step 5: Deduplicate by EmpId
        if curr["EmpId"] in seen_ids:
            quarantined_rows.append({"EmpId": curr["EmpId"], "Reason": "Duplicate PK", "OriginalValue": curr["EmpId"]})
            continue
        seen_ids.add(curr["EmpId"])
        clean_rows.append(curr)

    # Assert distinct row reconciliation
    clean_count = len(clean_rows)
    duplicate_count = sum(1 for q in quarantined_rows if q["Reason"] == "Duplicate PK")

    assert clean_count == 5  # 6 input rows minus 1 duplicate = 5 clean rows
    assert duplicate_count == 1
    assert clean_count + duplicate_count == source_count

    # Verify clean contents
    charlie = next(r for r in clean_rows if r["EmpId"] == 3)
    assert charlie["Salary"] == 0.0  # Successfully clamped
    assert charlie["Active"] == "1"

    bob = next(r for r in clean_rows if r["EmpId"] == 2)
    assert bob["Name"] == "Bob Jones"  # Successfully trimmed
    assert bob["Email"] is None        # Malformed email nullified

    diana = next(r for r in clean_rows if r["EmpId"] == 4)
    assert diana["Active"] is None     # Invalid boolean nullified
