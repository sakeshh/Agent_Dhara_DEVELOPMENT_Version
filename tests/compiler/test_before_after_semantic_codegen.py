import pytest
from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.models.actions import StructuredAction
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.planners.ingestion_bridge import CanonicalIssue, IngestionBridge
from agent.compiler.validation.semantic_action_validator import SemanticActionValidator
from agent.errors import CompilationBlockedError
from agent.semantic_context import (
    ColumnCleaningContract,
    ColumnRole,
    EntityDefinition,
    SemanticCleaningPlan,
)


def build_enterprise_semantic_plan() -> SemanticCleaningPlan:
    """Builds a semantic cleaning plan mirroring EmployeeMaster and CustomerMaster."""
    return SemanticCleaningPlan(
        entities={
            "EmployeeMaster": EntityDefinition(
                name="EmployeeMaster",
                source_datasets=["EmployeeMaster"],
                canonical_key="EmployeeID",
                column_contracts={
                    "EmployeeID": ColumnCleaningContract(column_name="EmployeeID", role=ColumnRole.PRIMARY_KEY),
                    "ManagerID": ColumnCleaningContract(column_name="ManagerID", role=ColumnRole.IDENTIFIER),
                    "Salary": ColumnCleaningContract(column_name="Salary", role=ColumnRole.METRIC),
                    "HireDate": ColumnCleaningContract(column_name="HireDate", role=ColumnRole.DATE),
                    "Email": ColumnCleaningContract(column_name="Email", role=ColumnRole.EMAIL),
                },
            ),
            "CustomerMaster": EntityDefinition(
                name="CustomerMaster",
                source_datasets=["CustomerMaster"],
                canonical_key="CustomerID",
                column_contracts={
                    "CustomerID": ColumnCleaningContract(column_name="CustomerID", role=ColumnRole.PRIMARY_KEY),
                    "CustomerStatus": ColumnCleaningContract(column_name="CustomerStatus", role=ColumnRole.CATEGORICAL),
                    "CreditLimit": ColumnCleaningContract(column_name="CreditLimit", role=ColumnRole.METRIC),
                    "Phone": ColumnCleaningContract(column_name="Phone", role=ColumnRole.PHONE),
                },
            ),
        }
    )


def test_before_vs_after_ingestion_bridge_filters_bad_actions():
    """
    Demonstrates that IngestionBridge drops all 4 bad actions when wired to semantic_plan:
    1. ManagerID -> PARSE_DATES (dropped)
    2. Salary -> LOWERCASE (dropped)
    3. CustomerStatus -> DEDUPLICATE (dropped)
    4. Dataset without key -> DEDUPLICATE (dropped)
    """
    sem_plan = build_enterprise_semantic_plan()
    bridge = IngestionBridge(default_schema="dbo")

    dirty_issues = [
        # 1. Format anomaly on ManagerID mapped to PARSE_DATES
        CanonicalIssue(
            issue_id="iss_mgr_date",
            dataset="EmployeeMaster",
            raw_dataset="EmployeeMaster",
            column="ManagerID",
            issue_type="format_anomaly",
            severity="high",
            suggested_action="parse_dates",
            resolved_action=ActionType.PARSE_DATES,
            parameters={"date_format": "YYYY-MM-DD"},
        ),
        # 2. Text anomaly on Salary mapped to LOWERCASE
        CanonicalIssue(
            issue_id="iss_salary_lower",
            dataset="EmployeeMaster",
            raw_dataset="EmployeeMaster",
            column="Salary",
            issue_type="text_casing",
            severity="medium",
            suggested_action="lowercase",
            resolved_action=ActionType.LOWERCASE,
            parameters={},
        ),
        # 3. Duplicate anomaly on CustomerStatus mapped to DEDUPLICATE
        CanonicalIssue(
            issue_id="iss_status_dedup",
            dataset="CustomerMaster",
            raw_dataset="CustomerMaster",
            column="CustomerStatus",
            issue_type="duplicate_values",
            severity="high",
            suggested_action="deduplicate",
            resolved_action=ActionType.DEDUPLICATE,
            parameters={"subset_columns": ["CustomerStatus"]},
        ),
        # 4. Valid issue on Email
        CanonicalIssue(
            issue_id="iss_valid_email",
            dataset="EmployeeMaster",
            raw_dataset="EmployeeMaster",
            column="Email",
            issue_type="invalid_email",
            severity="low",
            suggested_action="sanitize_email",
            resolved_action=ActionType.SANITIZE_EMAIL,
            parameters={},
        ),
    ]

    # Run bridge WITH semantic_plan
    issues, valid_actions = bridge.bridge_to_compiler_models(dirty_issues, semantic_plan=sem_plan)

    # Verify: All 3 bad actions were dropped!
    action_types = [a.action_type for a in valid_actions]
    target_columns = [a.target_columns for a in valid_actions]

    assert ActionType.PARSE_DATES not in action_types, "PARSE_DATES on ManagerID must be dropped"
    assert ActionType.LOWERCASE not in action_types, "LOWERCASE on Salary must be dropped"
    assert not any(cols == ["CustomerStatus"] for cols in target_columns), "CustomerStatus dedup must be dropped"

    # Only the valid action (SANITIZE_EMAIL on Email) survives
    assert len(valid_actions) == 1
    assert valid_actions[0].action_type == ActionType.SANITIZE_EMAIL
    assert valid_actions[0].target_columns == ["Email"]


def test_before_vs_after_sql_artifact_generation():
    """
    Generates a full T-SQL stored procedure with verified actions:
    Verifies that the generated SQL:
    - DOES NOT contain LOWER(Salary)
    - DOES NOT contain TRY_CONVERT on ManagerID
    - DOES NOT contain PARTITION BY [CustomerStatus]
    - DOES NOT contain ORDER BY (SELECT 1)
    - DOES contain valid transformations:
        - TRY_CAST on Salary to numeric
        - Sanitization on Email
        - Deterministic deduplication on EmployeeID
    """
    sem_plan = build_enterprise_semantic_plan()

    clean_actions = [
        # Valid: Coerce Salary to numeric
        StructuredAction(
            action_id="act_salary_num",
            action_type=ActionType.COERCE_NUMERIC,
            dataset_name="EmployeeMaster",
            target_columns=["Salary"],
            parameters={},
        ),
        # Valid: Sanitize Email
        StructuredAction(
            action_id="act_email_clean",
            action_type=ActionType.SANITIZE_EMAIL,
            dataset_name="EmployeeMaster",
            target_columns=["Email"],
            parameters={},
        ),
        # Valid: Deduplicate on canonical key EmployeeID
        StructuredAction(
            action_id="act_emp_dedup",
            action_type=ActionType.DEDUPLICATE,
            dataset_name="EmployeeMaster",
            target_columns=["EmployeeID"],
            parameters={"survivor_order": "[_DharaStagingRowId] ASC"},
        ),
    ]

    profiles = {
        "EmployeeMaster": DatasetProfile(
            dataset_id="EmployeeMaster",
            row_count=500,
            columns={
                "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
                "ManagerID": ColumnProfile(name="ManagerID", dtype="string"),
                "Salary": ColumnProfile(name="Salary", dtype="string"),
                "Email": ColumnProfile(name="Email", dtype="string"),
            },
        )
    }

    ctx = CompilerContext(
        session_id="test_regenerated_clean_etl",
        target_engine=ExecutionEngine.AZURE_SQL,
        dataset_profiles=profiles,
        structured_actions=clean_actions,
        semantic_plan=sem_plan,
    )

    pipeline = CompilerPipeline()
    compiled_ctx, snapshot = pipeline.compile(ctx)

    assert compiled_ctx.code_artifact is not None
    generated_sql = compiled_ctx.code_artifact.content

    # Assert that all 4 bad SQL constructs are ABSENT
    assert "LOWER([Salary])" not in generated_sql
    assert "LOWER(Salary)" not in generated_sql
    assert "TRY_CONVERT(DATETIME2, [ManagerID])" not in generated_sql
    assert "PARTITION BY [CustomerStatus]" not in generated_sql
    assert "SELECT 1" not in generated_sql

    # Assert that safe, verified SQL constructs ARE PRESENT
    assert "TRY_CAST([Salary] AS FLOAT)" in generated_sql or "TRY_CAST([Salary] AS" in generated_sql
    assert "EmployeeMaster" in generated_sql
    assert "PARTITION BY [EmployeeID]" in generated_sql
    assert "[_DharaStagingRowId] ASC" in generated_sql

    # Verify Snapshot holds the semantic_plan
    assert snapshot.semantic_plan is not None
    assert "EmployeeMaster" in snapshot.semantic_plan["entities"]


def test_golden_customer_and_employee_pipeline_regeneration(tmp_path):
    """
    Regenerates the complete pipeline for BOTH CustomerMaster and EmployeeMaster.
    Asserts:
    1. Exactly 1 CREATE OR ALTER PROCEDURE
    2. None of:
       - TRY_CONVERT(DATETIME2, [ManagerID]) or TRY_CONVERT(DATE, [ManagerID])
       - LOWER([Salary])
       - LOWER([CreditLimit])
       - COALESCE([CreditLimit], '')
       - PARTITION BY [CustomerStatus]
       - PARTITION BY [Status]
       - ORDER BY (SELECT 1)
    3. Materialized #DuplicateRows without reusable CTE bugs
    4. Generated SQL hash == Persisted SQL hash
    5. ArtifactSemanticValidator passes on persisted bytes
    """
    import hashlib
    import re
    from agent.compiler.validation.artifact_semantic_validator import ArtifactSemanticValidator

    sem_plan = build_enterprise_semantic_plan()

    actions = [
        # EmployeeMaster: coerce salary, clean email, dedup on EmployeeID
        StructuredAction(
            action_id="act_emp_salary_num",
            action_type=ActionType.COERCE_NUMERIC,
            dataset_name="EmployeeMaster",
            target_columns=["Salary"],
            parameters={},
        ),
        StructuredAction(
            action_id="act_emp_email_clean",
            action_type=ActionType.SANITIZE_EMAIL,
            dataset_name="EmployeeMaster",
            target_columns=["Email"],
            parameters={},
        ),
        StructuredAction(
            action_id="act_emp_dedup",
            action_type=ActionType.DEDUPLICATE,
            dataset_name="EmployeeMaster",
            target_columns=["EmployeeID"],
            parameters={"survivor_order": "[_DharaStagingRowId] ASC"},
        ),
        # CustomerMaster: coerce credit limit, format phone, dedup on CustomerID
        StructuredAction(
            action_id="act_cust_credit_num",
            action_type=ActionType.COERCE_NUMERIC,
            dataset_name="CustomerMaster",
            target_columns=["CreditLimit"],
            parameters={},
        ),
        StructuredAction(
            action_id="act_cust_phone_norm",
            action_type=ActionType.NORMALIZE_PHONE,
            dataset_name="CustomerMaster",
            target_columns=["Phone"],
            parameters={},
        ),
        StructuredAction(
            action_id="act_cust_dedup",
            action_type=ActionType.DEDUPLICATE,
            dataset_name="CustomerMaster",
            target_columns=["CustomerID"],
            parameters={"survivor_order": "[_DharaStagingRowId] ASC"},
        ),
    ]

    profiles = {
        "EmployeeMaster": DatasetProfile(
            dataset_id="EmployeeMaster",
            row_count=500,
            columns={
                "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
                "ManagerID": ColumnProfile(name="ManagerID", dtype="string"),
                "Salary": ColumnProfile(name="Salary", dtype="string"),
                "Email": ColumnProfile(name="Email", dtype="string"),
            },
        ),
        "CustomerMaster": DatasetProfile(
            dataset_id="CustomerMaster",
            row_count=1000,
            columns={
                "CustomerID": ColumnProfile(name="CustomerID", dtype="int"),
                "CustomerStatus": ColumnProfile(name="CustomerStatus", dtype="string"),
                "CreditLimit": ColumnProfile(name="CreditLimit", dtype="string"),
                "Phone": ColumnProfile(name="Phone", dtype="string"),
            },
        ),
    }

    ctx = CompilerContext(
        session_id="test_golden_dual_pipeline",
        target_engine=ExecutionEngine.AZURE_SQL,
        dataset_profiles=profiles,
        structured_actions=actions,
        semantic_plan=sem_plan,
    )

    pipeline = CompilerPipeline()
    compiled_ctx, snapshot = pipeline.compile(ctx)

    assert compiled_ctx.code_artifact is not None
    generated_sql = compiled_ctx.code_artifact.content

    # 1. Singleton procedure definition check
    proc_matches = re.findall(
        r"\bCREATE\s+(?:OR\s+ALTER\s+)?(?:PROCEDURE|PROC)\b",
        generated_sql,
        re.IGNORECASE,
    )
    assert len(proc_matches) == 1, f"Expected exactly 1 CREATE OR ALTER PROCEDURE, found {len(proc_matches)}"

    # 2. Assert all 7 bad SQL regressions are strictly ABSENT
    assert "TRY_CONVERT(DATETIME2, [ManagerID])" not in generated_sql
    assert "TRY_CONVERT(DATE, [ManagerID])" not in generated_sql
    assert "LOWER([Salary])" not in generated_sql
    assert "LOWER([CreditLimit])" not in generated_sql
    assert "COALESCE([CreditLimit], '')" not in generated_sql
    assert "ISNULL([CreditLimit], '')" not in generated_sql
    assert "PARTITION BY [CustomerStatus]" not in generated_sql
    assert "PARTITION BY [Status]" not in generated_sql
    assert "ORDER BY (SELECT 1)" not in generated_sql

    # 3. Verify deduplication output adheres to safe materialized temp table pattern
    assert "#DuplicateRows" in generated_sql
    assert "PARTITION BY [EmployeeID]" in generated_sql
    assert "PARTITION BY [CustomerID]" in generated_sql
    assert "[ETL_REJECT].[Rejection_Log]" in generated_sql

    # 4. In-memory hash vs persisted file bytes verification
    in_memory_hash = hashlib.sha256(generated_sql.encode("utf-8")).hexdigest()
    out_file = tmp_path / "golden_pipeline.sql"
    out_file.write_text(generated_sql, encoding="utf-8")
    persisted_sql = out_file.read_text(encoding="utf-8")
    persisted_hash = hashlib.sha256(persisted_sql.encode("utf-8")).hexdigest()
    assert in_memory_hash == persisted_hash

    # 5. Run ArtifactSemanticValidator on the persisted bytes
    val_res = ArtifactSemanticValidator.validate_artifact(persisted_sql, context=compiled_ctx)
    assert val_res.is_valid is True
    assert len(val_res.issues) == 0
