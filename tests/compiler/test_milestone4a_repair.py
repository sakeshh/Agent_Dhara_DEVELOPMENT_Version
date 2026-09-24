from __future__ import annotations

import pytest
from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.models import (
    CodeArtifact,
    ColumnProfile,
    CompilerContext,
    DatasetProfile,
    ExecutionEngine,
    ValidationResult,
)
from agent.compiler.repair import RepairAgent, RepairClassifier
from agent.compiler.repair.strategies.dialect_repair import DialectRepair
from agent.compiler.repair.strategies.import_repair import ImportRepair
from agent.compiler.repair.strategies.reference_repair import ReferenceRepair
from agent.compiler.repair.strategies.syntax_repair import SyntaxRepair
from agent.compiler.validators import PythonValidator, SQLValidator


def test_syntax_repair_sql_bracket():
    broken_sql = "SELECT [customer_id FROM [dbo].[customers];"
    validator = SQLValidator()
    context = CompilerContext(target_engine=ExecutionEngine.AZURE_SQL)
    artifact = CodeArtifact(
        artifact_id="art1",
        name="test.sql",
        language="sql",
        content=broken_sql,
        file_extension=".sql",
    )
    context.code_artifact = artifact
    context.validation_result = validator.validate(artifact, context)
    assert context.validation_result.is_valid is False

    repair_agent = RepairAgent()
    repaired_ctx = repair_agent.repair(context, validator)

    assert repaired_ctx.validation_result.is_valid is True
    assert "[customer_id] FROM" in repaired_ctx.code_artifact.content


def test_syntax_repair_python_missing_colon():
    broken_py = "import pandas as pd\n\ndef run()\n    return 42\n"
    validator = PythonValidator()
    context = CompilerContext(target_engine=ExecutionEngine.PYTHON)
    artifact = CodeArtifact(
        artifact_id="art_py",
        name="test.py",
        language="python",
        content=broken_py,
        file_extension=".py",
    )
    context.code_artifact = artifact
    context.validation_result = validator.validate(artifact, context)
    assert context.validation_result.is_valid is False

    repair_agent = RepairAgent()
    repaired_ctx = repair_agent.repair(context, validator)

    assert repaired_ctx.validation_result.is_valid is True
    assert "def run():" in repaired_ctx.code_artifact.content


def test_import_repair_python_missing_pandas():
    code_without_import = 'def transform(df):\n    return df.astype(str)\n'
    # Force usage of pandas in comment/content so validator triggers missing pandas check
    code_with_pandas_ref = 'def transform(df):\n    # uses pandas\n    return df\n'
    validator = PythonValidator()
    context = CompilerContext(target_engine=ExecutionEngine.PYTHON)
    artifact = CodeArtifact(
        artifact_id="art_imp",
        name="test_imp.py",
        language="python",
        content=code_with_pandas_ref,
        file_extension=".py",
    )
    context.code_artifact = artifact
    context.validation_result = validator.validate(artifact, context)
    assert any(i.issue_code == "PY_MISSING_PANDAS_IMPORT" for i in context.validation_result.issues)

    repair_agent = RepairAgent()
    repaired_ctx = repair_agent.repair(context, validator)

    assert "import pandas as pd" in repaired_ctx.code_artifact.content
    assert not any(i.issue_code == "PY_MISSING_PANDAS_IMPORT" for i in repaired_ctx.validation_result.issues)


def test_reference_repair_column_name():
    dataset = DatasetProfile(
        dataset_name="customers",
        columns={
            "customer_id": ColumnProfile(name="customer_id", dtype="int"),
            "email_address": ColumnProfile(name="email_address", dtype="string"),
        },
    )
    context = CompilerContext(
        target_engine=ExecutionEngine.PYTHON,
        dataset_profiles={"customers": dataset},
    )
    # Code references misspelled 'customerid'
    misspelled_code = 'df["customerid"] = df["customerid"].astype(str)'
    
    from agent.compiler.models.validation import DiagnosticIssue, DiagnosticSeverity, ValidationIssueType
    ref_issue = DiagnosticIssue(
        issue_code="PY_UNKNOWN_COLUMN",
        issue_type=ValidationIssueType.REFERENCE,
        severity=DiagnosticSeverity.ERROR,
        message="Unknown column 'customerid'",
    )

    strategy = ReferenceRepair()
    assert strategy.can_handle(ref_issue) is True
    patched = strategy.patch(misspelled_code, ref_issue, context)

    assert patched is not None
    assert 'df["customer_id"]' in patched


def test_dialect_repair_sql_ifnull_to_coalesce():
    mysql_dialect_sql = """
CREATE OR ALTER PROCEDURE [dbo].[sp_test]
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRANSACTION;
        UPDATE [dbo].[customers_Staging] SET [email] = IFNULL([email], '');
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
"""
    validator = SQLValidator()
    context = CompilerContext(target_engine=ExecutionEngine.AZURE_SQL)
    artifact = CodeArtifact(
        artifact_id="art_dialect",
        name="proc.sql",
        language="sql",
        content=mysql_dialect_sql,
        file_extension=".sql",
    )
    context.code_artifact = artifact
    context.validation_result = validator.validate(artifact, context)
    assert any(i.issue_code == "SQL_DIALECT_IFNULL" for i in context.validation_result.issues)

    repair_agent = RepairAgent()
    repaired_ctx = repair_agent.repair(context, validator)

    assert repaired_ctx.validation_result.is_valid is True
    assert "COALESCE([email], '')" in repaired_ctx.code_artifact.content
