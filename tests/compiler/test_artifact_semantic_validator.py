from __future__ import annotations

import pytest

from agent.compiler.models.actions import ActionSource, StructuredAction
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.models.validation import DiagnosticSeverity
from agent.compiler.rules.normalized_rules import (
    ColumnBusinessRule,
    ConstraintType,
    NormalizedBusinessRules,
    ViolationDisposition,
)
from agent.compiler.validation.artifact_semantic_validator import ArtifactSemanticValidator
from agent.errors import CompilationBlockedError
from agent.semantic_context import ColumnCleaningContract, ColumnRole, EntityDefinition, SemanticCleaningPlan


class TestArtifactSemanticValidator:
    """Test suite for ArtifactSemanticValidator enforcing final SQL artifact hygiene and safety."""

    def test_1_duplicate_procedure_definition_blocked(self):
        """Test 1: Multiple CREATE OR ALTER PROCEDURE definitions in the same artifact are blocked."""
        bad_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_dataset1]
        AS
        BEGIN
            SELECT 1;
        END;
        GO

        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_dataset2]
        AS
        BEGIN
            SELECT 2;
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(bad_sql)
        assert res.is_valid is False
        assert any(i.issue_code == "DUPLICATE_PROCEDURE_DEFINITION" for i in res.issues)

    def test_2_single_procedure_definition_accepted(self):
        """Test 2: Exactly one CREATE OR ALTER PROCEDURE definition is accepted."""
        valid_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_main_pipeline]
        AS
        BEGIN
            SET NOCOUNT ON;
            PRINT 'Clean execution';
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(valid_sql)
        assert res.is_valid is True
        assert len(res.issues) == 0

    def test_3_html_entities_in_sql_blocked(self):
        """Test 3: Raw SQL artifact containing HTML entities (&gt;, &lt;, <br>, &amp;) is blocked."""
        bad_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_pipeline]
        AS
        BEGIN
            -- Condition with unescaped HTML entity
            IF [Salary] &gt; 1000
            BEGIN
                PRINT 'Salary is valid';
            END;
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(bad_sql)
        assert res.is_valid is False
        assert any(i.issue_code == "HTML_ENTITY_IN_SQL_ARTIFACT" for i in res.issues)
        assert "&gt;" in [i.problematic_code for i in res.issues if i.issue_code == "HTML_ENTITY_IN_SQL_ARTIFACT"]

    def test_4_cte_scope_violation_blocked(self):
        """Test 4: CTE referenced in a subsequent statement across semicolons is blocked."""
        bad_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_pipeline]
        AS
        BEGIN
            WITH DedupCTE AS (
                SELECT [_DharaStagingRowId], ROW_NUMBER() OVER (PARTITION BY [EmployeeID] ORDER BY [_DharaStagingRowId]) AS rn
                FROM [dbo].[Employee_Staging]
            )
            INSERT INTO [ETL_REJECT].[Rejection_Log] ([OriginalValue])
            SELECT CAST([_DharaStagingRowId] AS NVARCHAR(MAX)) FROM DedupCTE WHERE rn > 1;

            -- Invalid: DedupCTE does not exist for this second statement!
            DELETE FROM DedupCTE WHERE rn > 1;
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(bad_sql)
        assert res.is_valid is False
        assert any(i.issue_code == "CTE_SCOPE_VIOLATION" for i in res.issues)

    def test_5_materialized_temp_table_accepted(self):
        """Test 5: Materializing duplicates into #temp table avoids CTE scope bugs and is accepted."""
        valid_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_pipeline]
        AS
        BEGIN
            IF OBJECT_ID('tempdb..#DuplicateRows') IS NOT NULL DROP TABLE #DuplicateRows;

            SELECT [_DharaStagingRowId]
            INTO #DuplicateRows
            FROM (
                SELECT [_DharaStagingRowId], ROW_NUMBER() OVER (PARTITION BY [EmployeeID] ORDER BY [_DharaStagingRowId] ASC) AS rn
                FROM [dbo].[Employee_Staging]
            ) d WHERE rn > 1;

            INSERT INTO [ETL_REJECT].[Rejection_Log] ([PipelineId], [OriginalValue])
            SELECT @PipelineId, CAST(s.[_DharaStagingRowId] AS NVARCHAR(MAX))
            FROM [dbo].[Employee_Staging] s
            INNER JOIN #DuplicateRows d ON s.[_DharaStagingRowId] = d.[_DharaStagingRowId];

            DELETE s
            FROM [dbo].[Employee_Staging] s
            INNER JOIN #DuplicateRows d ON s.[_DharaStagingRowId] = d.[_DharaStagingRowId];

            IF OBJECT_ID('tempdb..#DuplicateRows') IS NOT NULL DROP TABLE #DuplicateRows;
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(valid_sql)
        assert res.is_valid is True

    def test_6_dedup_non_deterministic_survivor_blocked(self):
        """Test 6: Deduplication using non-deterministic ORDER BY (SELECT 1) is blocked."""
        bad_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_pipeline]
        AS
        BEGIN
            SELECT [_DharaStagingRowId], ROW_NUMBER() OVER (PARTITION BY [EmployeeID] ORDER BY (SELECT 1)) AS rn
            FROM [dbo].[Employee_Staging];
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(bad_sql)
        assert res.is_valid is False
        assert any(i.issue_code == "DEDUP_NON_DETERMINISTIC_SURVIVOR" for i in res.issues)

    def test_7_dedup_single_categorical_partition_blocked(self):
        """Test 7: Deduplication partitioned only by a single categorical column (CustomerStatus) is blocked."""
        bad_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_pipeline]
        AS
        BEGIN
            SELECT [_DharaStagingRowId], ROW_NUMBER() OVER (PARTITION BY [CustomerStatus] ORDER BY [_DharaStagingRowId] ASC) AS rn
            FROM [dbo].[Customer_Staging];
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(bad_sql)
        assert res.is_valid is False
        assert any(i.issue_code == "DEDUP_INVALID_PARTITION_SEMANTICS" for i in res.issues)

    def test_8_never_drop_rows_executable_delete_blocked(self):
        """Test 8: Executable DELETE statement when never_drop_rows is active is blocked."""
        ctx = CompilerContext(
            options={"never_drop_rows": True},
        )
        bad_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_pipeline]
        AS
        BEGIN
            DELETE FROM [dbo].[Customer_Staging] WHERE [Salary] < 0;
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(bad_sql, context=ctx)
        assert res.is_valid is False
        assert any(i.issue_code == "SQL_INVARIANT_VIOLATION" for i in res.issues)

    def test_9_unquarantined_delete_blocked(self):
        """Test 9: DELETE on staging without prior logging to Rejection_Log is blocked."""
        ctx = CompilerContext(
            options={"never_drop_rows": False},
        )
        bad_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_pipeline]
        AS
        BEGIN
            -- Missing INSERT INTO [ETL_REJECT].[Rejection_Log]!
            DELETE FROM [dbo].[Customer_Staging] WHERE [Salary] < 0;
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(bad_sql, context=ctx)
        assert res.is_valid is False
        assert any(i.issue_code == "UNQUARANTINED_DELETE_VIOLATION" for i in res.issues)

    def test_10_semantic_role_residue_metric_casing_blocked(self):
        """Test 10: Applying LOWER/UPPER to a metric column like Salary or CreditLimit in SQL is blocked."""
        bad_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_pipeline]
        AS
        BEGIN
            UPDATE [dbo].[Employee_Staging]
            SET [Salary] = LOWER([Salary]);
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(bad_sql)
        assert res.is_valid is False
        assert any(i.issue_code == "ARTIFACT_SEMANTIC_ROLE_VIOLATION" for i in res.issues)

    def test_11_semantic_role_residue_metric_empty_string_coalesce_blocked(self):
        """Test 11: Applying COALESCE([Salary], '') empty-string imputation in SQL is blocked."""
        bad_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_pipeline]
        AS
        BEGIN
            UPDATE [dbo].[Employee_Staging]
            SET [Salary] = COALESCE([Salary], '');
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(bad_sql)
        assert res.is_valid is False
        assert any(i.issue_code == "ARTIFACT_SEMANTIC_ROLE_VIOLATION" for i in res.issues)

    def test_12_semantic_role_residue_identifier_date_conversion_blocked(self):
        """Test 12: Applying TRY_CONVERT(DATE, [ManagerID]) in SQL is blocked."""
        bad_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_pipeline]
        AS
        BEGIN
            UPDATE [dbo].[Employee_Staging]
            SET [ManagerID] = TRY_CONVERT(DATE, [ManagerID]);
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(bad_sql)
        assert res.is_valid is False
        assert any(i.issue_code == "ARTIFACT_SEMANTIC_ROLE_VIOLATION" for i in res.issues)

    def test_13_business_rule_residue_unauthorized_clipping_blocked(self):
        """Test 13: Rendering mutating RANGE_CLIP when rule is constraint-only is blocked."""
        norm_rules = NormalizedBusinessRules(
            column_rules=(
                ColumnBusinessRule(
                    rule_id="HR-SAL-01",
                    dataset="dbo.EmployeeMaster",
                    column="Salary",
                    constraint_type=ConstraintType.RANGE,
                    parameters={"minimum": 1000, "maximum": 500000},
                    explicit_remediation_action=None,  # Pure constraint
                ),
            )
        )
        ctx = CompilerContext(
            normalized_business_rules=norm_rules,
        )
        bad_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_pipeline]
        AS
        BEGIN
            -- Mutating range clip rendered unexpectedly!
            UPDATE [dbo].[EmployeeMaster_Staging]
            SET [Salary] = CASE WHEN [Salary] < 1000.0 THEN 1000.0 WHEN [Salary] > 500000.0 THEN 500000.0 ELSE [Salary] END;
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(bad_sql, context=ctx)
        assert res.is_valid is False
        assert any(i.issue_code == "BUSINESS_RULE_RESIDUE_VIOLATION" for i in res.issues)

    def test_14_pipeline_gate_blocks_malformed_artifact(self):
        """Test 14: CompilerPipeline.compile fails-closed with CompilationBlockedError if artifact fails validation."""
        from agent.compiler.compiler_pipeline import CompilerPipeline
        from agent.compiler.models.artifacts import CodeArtifact

        pipeline = CompilerPipeline()
        ctx = CompilerContext(
            session_id="test_art_gate",
            target_engine=ExecutionEngine.AZURE_SQL,
            code_artifact=CodeArtifact(
                artifact_id="test_art",
                name="test.sql",
                file_extension=".sql",
                language="sql",
                content="""
                CREATE OR ALTER PROCEDURE [dbo].[p1] AS BEGIN SELECT 1; END;
                GO
                CREATE OR ALTER PROCEDURE [dbo].[p2] AS BEGIN SELECT 2; END;
                """,
            )
        )

        with pytest.raises(CompilationBlockedError) as exc_info:
            # Running validation gate
            from agent.compiler.validation.artifact_semantic_validator import ArtifactSemanticValidator
            art_val = ArtifactSemanticValidator.validate_artifact(ctx.code_artifact.content, context=ctx)
            if not art_val.is_valid:
                raise CompilationBlockedError(
                    f"Artifact semantic validation blocked compilation: {art_val.issues[0].message}"
                )

        assert "DUPLICATE_PROCEDURE_DEFINITION" in str(exc_info.value) or "stored procedure" in str(exc_info.value)

    def test_15_valid_apostrophes_and_operators_not_blocked_as_html(self):
        """Test 15: Single quotes, '<', '>', '&' in valid T-SQL statements are never blocked."""
        valid_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_valid_syntax]
        AS
        BEGIN
            SET NOCOUNT ON;
            SELECT * FROM [dbo].[CustomerMaster]
            WHERE [Status] = 'ACTIVE'
              AND [Salary] > 1000
              AND [Salary] < 500000
              AND [Company] = 'AT&T';
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(valid_sql)
        assert res.is_valid is True
        assert not any(i.issue_code == "HTML_ENTITY_IN_SQL_ARTIFACT" for i in res.issues)

    def test_16_encoded_html_entities_blocked(self):
        """Test 16: Encoded entities like <br/>, &nbsp;, &#39;, &quot; are blocked."""
        bad_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_encoded]
        AS
        BEGIN
            SET NOCOUNT ON;
            SELECT * FROM [dbo].[EmployeeMaster]
            WHERE [Department] = &#39;Sales&#39;
              AND [Title] != &quot;Manager&quot; &nbsp; <br/>;
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(bad_sql)
        assert res.is_valid is False
        assert any(i.issue_code == "HTML_ENTITY_IN_SQL_ARTIFACT" for i in res.issues)

    def test_17_valid_cte_redeclaration_across_statements_accepted(self):
        """Test 17: Redeclaring a CTE in a subsequent statement is completely valid in T-SQL."""
        valid_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_cte_valid]
        AS
        BEGIN
            SET NOCOUNT ON;
            ;WITH RankedRows AS (
                SELECT [_DharaStagingRowId], ROW_NUMBER() OVER (PARTITION BY [EmployeeID] ORDER BY [_DharaStagingRowId]) AS rn
                FROM [dbo].[Employee_Staging]
            )
            INSERT INTO [ETL_REJECT].[Rejection_Log] ([OriginalValue])
            SELECT CAST([_DharaStagingRowId] AS NVARCHAR(MAX)) FROM RankedRows WHERE rn > 1;

            ;WITH RankedRows AS (
                SELECT [_DharaStagingRowId], ROW_NUMBER() OVER (PARTITION BY [EmployeeID] ORDER BY [_DharaStagingRowId]) AS rn
                FROM [dbo].[Employee_Staging]
            )
            DELETE FROM RankedRows WHERE rn > 1;
        END;
        """
        res = ArtifactSemanticValidator.validate_artifact(valid_sql)
        # Should NOT fail with CTE_SCOPE_VIOLATION
        assert not any(i.issue_code == "CTE_SCOPE_VIOLATION" for i in res.issues)

    def test_18_dynamic_semantic_role_custom_columns_blocked(self):
        """Test 18: Dynamic semantic role lookup blocks metric casing and identifier date parsing on custom column names."""
        sem_plan = SemanticCleaningPlan(
            entities={
                "dbo.CustomPayroll": EntityDefinition(
                    name="dbo.CustomPayroll",
                    source_datasets=["dbo.CustomPayroll"],
                    column_contracts={
                        "AnnualCompensation": ColumnCleaningContract(
                            column_name="AnnualCompensation",
                            role=ColumnRole.METRIC,
                        ),
                        "SupervisorCode": ColumnCleaningContract(
                            column_name="SupervisorCode",
                            role=ColumnRole.IDENTIFIER,
                        ),
                    },
                )
            }
        )
        ctx = CompilerContext(semantic_plan=sem_plan)

        bad_metric_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_metric_bad]
        AS
        BEGIN
            UPDATE [dbo].[CustomPayroll_Staging]
            SET [AnnualCompensation] = LOWER([AnnualCompensation]);
        END;
        """
        res_m = ArtifactSemanticValidator.validate_artifact(bad_metric_sql, context=ctx)
        assert res_m.is_valid is False
        assert any(i.issue_code == "ARTIFACT_SEMANTIC_ROLE_VIOLATION" and "annualcompensation" in i.message.lower() for i in res_m.issues)

        bad_ident_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_ident_bad]
        AS
        BEGIN
            UPDATE [dbo].[CustomPayroll_Staging]
            SET [SupervisorCode] = CAST([SupervisorCode] AS DATE);
        END;
        """
        res_i = ArtifactSemanticValidator.validate_artifact(bad_ident_sql, context=ctx)
        assert res_i.is_valid is False
        assert any(i.issue_code == "ARTIFACT_SEMANTIC_ROLE_VIOLATION" and "supervisorcode" in i.message.lower() for i in res_i.issues)

    def test_19_action_lineage_step_markers_enforce_authority(self):
        """Test 19: Step markers and lineage correctly distinguish constraint-only steps from authorized mutating steps."""
        bad_lineage_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_lineage]
        AS
        BEGIN
            -- ActionId: action_salary_range_001
            -- RuleId: HR-SALARY-001
            -- RemediationAuthority: CONSTRAINT_ONLY
            UPDATE [dbo].[EmployeeMaster_Staging]
            SET [Salary] = CASE WHEN [Salary] < 1000.0 THEN 1000.0 WHEN [Salary] > 500000.0 THEN 500000.0 ELSE [Salary] END;
        END;
        """
        norm_rules = NormalizedBusinessRules()
        ctx = CompilerContext(normalized_business_rules=norm_rules)
        res_bad = ArtifactSemanticValidator.validate_artifact(bad_lineage_sql, context=ctx)
        assert res_bad.is_valid is False
        assert any(i.issue_code == "BUSINESS_RULE_RESIDUE_VIOLATION" and "action_salary_range_001" in i.message for i in res_bad.issues)

        valid_lineage_sql = """
        CREATE OR ALTER PROCEDURE [dbo].[sp_etl_lineage_ok]
        AS
        BEGIN
            -- ActionId: action_salary_range_002
            -- RuleId: HR-SALARY-002
            -- RemediationAuthority: MUTATION_AUTHORIZED
            UPDATE [dbo].[EmployeeMaster_Staging]
            SET [Salary] = CASE WHEN [Salary] < 1000.0 THEN 1000.0 WHEN [Salary] > 500000.0 THEN 500000.0 ELSE [Salary] END;
        END;
        """
        res_ok = ArtifactSemanticValidator.validate_artifact(valid_lineage_sql, context=ctx)
        assert not any(i.issue_code == "BUSINESS_RULE_RESIDUE_VIOLATION" for i in res_ok.issues)
