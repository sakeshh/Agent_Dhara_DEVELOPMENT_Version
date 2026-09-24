from __future__ import annotations

import pytest
from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.models import (
    ColumnProfile,
    CompilerContext,
    DatasetProfile,
    ExecutionEngine,
    Issue,
    IssueCategory,
    CodeArtifact,
)
from agent.compiler.snapshot_store import SnapshotStore
from agent.compiler.validators import SQLValidator, SparkValidator


def _build_sample_context(target_engine: ExecutionEngine, session_id: str) -> CompilerContext:
    email_col = ColumnProfile(name="email", dtype="string", null_percentage=8.0)
    customer_id_col = ColumnProfile(name="customer_id", dtype="int", is_primary_key_candidate=True)
    dataset = DatasetProfile(
        dataset_name="customers",
        row_count=2000,
        columns={"email": email_col, "customer_id": customer_id_col},
    )

    return CompilerContext(
        session_id=session_id,
        target_engine=target_engine,
        dataset_profiles={"customers": dataset},
        detected_issues=[
            Issue(
                issue_id="iss_email_trim",
                dataset_name="customers",
                column_name="email",
                category=IssueCategory.UNTRIMMED_WHITESPACE,
                suggested_action="trim",
                message="Whitespace detected in email addresses",
            ),
            Issue(
                issue_id="iss_email_lower",
                dataset_name="customers",
                column_name="email",
                category=IssueCategory.FORMAT_ANOMALY,
                suggested_action="lowercase",
                message="Mixed case email addresses",
            ),
            Issue(
                issue_id="iss_cust_id_dup",
                dataset_name="customers",
                column_name="customer_id",
                category=IssueCategory.DUPLICATE,
                suggested_action="deduplicate",
                message="Duplicate customer IDs found",
            ),
        ],
    )


def test_compiler_pipeline_python_with_snapshot():
    pipeline = CompilerPipeline()
    context = _build_sample_context(ExecutionEngine.PYTHON, "sess_py_snapshot")

    ctx, snapshot = pipeline.compile(context)

    # 1. Verify compilation output
    assert ctx.code_artifact is not None
    assert ctx.code_artifact.language == "python"
    assert "def transform_customers(df: pd.DataFrame)" in ctx.code_artifact.content
    assert ctx.validation_result is not None
    assert ctx.validation_result.is_valid is True

    # 2. Verify snapshot
    assert snapshot.snapshot_id is not None
    assert snapshot.target_engine == "python"
    assert len(snapshot.detected_issues) == 3
    assert snapshot.generated_code is not None

    # Retrieve from store
    stored = SnapshotStore.get_instance().get_snapshot(snapshot.snapshot_id)
    assert stored is not None
    assert stored.pipeline_id == snapshot.pipeline_id


def test_compiler_pipeline_sql_vertical_slice():
    pipeline = CompilerPipeline()
    context = _build_sample_context(ExecutionEngine.AZURE_SQL, "sess_sql_slice")

    ctx, snapshot = pipeline.compile(context)

    # 1. Verify SQL code generation
    assert ctx.code_artifact is not None
    assert ctx.code_artifact.language == "sql"
    sql = ctx.code_artifact.content

    # Check Stored Procedure header and structure
    assert "CREATE OR ALTER PROCEDURE [dbo].[sp_etl_" in sql
    assert "BEGIN TRANSACTION;" in sql
    assert "COMMIT TRANSACTION;" in sql
    assert "[ETL_REJECT].[Rejection_Log]" in sql

    # Check CTE Deduplication strategy
    assert "ROW_NUMBER() OVER (" in sql
    assert "PARTITION BY [customer_id]" in sql

    # Check Fused String Trim & Lower
    assert "LOWER(RTRIM(LTRIM([email])))" in sql

    # 2. Verify SQL Validation passed
    assert ctx.validation_result is not None
    assert ctx.validation_result.is_valid is True
    assert ctx.validation_result.error_count == 0


def test_sql_validator_catches_unbalanced_brackets_and_unsafe_delete():
    validator = SQLValidator()
    
    # 1. Unbalanced bracket
    broken_bracket_art = CodeArtifact(
        artifact_id="art_bracket_err",
        name="test.sql",
        language="sql",
        content="SELECT [column_name FROM [dbo].[table];",
        file_extension=".sql",
    )
    res1 = validator.validate(broken_bracket_art, CompilerContext())
    assert res1.is_valid is False
    assert any(i.issue_code == "SQL_BRACKET_MISMATCH" for i in res1.issues)

    # 2. Unsafe DELETE on core table
    unsafe_delete_art = CodeArtifact(
        artifact_id="art_delete_err",
        name="test.sql",
        language="sql",
        content="DELETE FROM [dbo].[Production_Customers];",
        file_extension=".sql",
    )
    res2 = validator.validate(unsafe_delete_art, CompilerContext())
    assert res2.is_valid is False
    assert any(i.issue_code == "SQL_UNSAFE_DELETE" for i in res2.issues)


def test_compiler_pipeline_pyspark_vertical_slice():
    pipeline = CompilerPipeline()
    context = _build_sample_context(ExecutionEngine.PYSPARK, "sess_spark_slice")

    ctx, snapshot = pipeline.compile(context)

    # 1. Verify PySpark code generation
    assert ctx.code_artifact is not None
    assert ctx.code_artifact.language == "python"
    assert ctx.code_artifact.metadata.get("engine") == "pyspark"
    spark_code = ctx.code_artifact.content

    # Check SparkSession and imports
    assert "from pyspark.sql import DataFrame, SparkSession" in spark_code
    assert "import pyspark.sql.functions as F" in spark_code
    assert "def transform_customers(df: DataFrame)" in spark_code

    # Check Fused operations in PySpark
    assert 'F.lower(F.trim(F.col("email")))' in spark_code

    # Check dropDuplicates in PySpark
    assert 'df_clean.dropDuplicates(subset=["customer_id"])' in spark_code

    # 2. Verify Spark Validation passed
    assert ctx.validation_result is not None
    assert ctx.validation_result.is_valid is True
    assert ctx.validation_result.error_count == 0


def test_spark_validator_warns_on_collect():
    validator = SparkValidator()
    risky_art = CodeArtifact(
        artifact_id="art_risky_spark",
        name="test_spark.py",
        language="python",
        content="def run(df):\n    records = df.collect()\n    return records\n",
        file_extension=".py",
    )
    res = validator.validate(risky_art, CompilerContext())
    assert res.has_performance_anti_patterns is True
    assert any(i.issue_code == "SPARK_PERF_HAZARD" for i in res.issues)
