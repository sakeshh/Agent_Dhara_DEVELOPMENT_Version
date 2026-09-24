from __future__ import annotations

import pytest
import yaml
from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.contracts import ContractAgent
from agent.compiler.lineage import LineageAgent
from agent.compiler.models import (
    ColumnProfile,
    CompilerContext,
    DatasetProfile,
    ExecutionEngine,
    Issue,
    IssueCategory,
)
from agent.compiler.testing import TestGenerator


def _build_context(engine: ExecutionEngine, session_id: str) -> CompilerContext:
    email_col = ColumnProfile(name="email", dtype="string", null_percentage=10.0, semantic_type="email")
    cust_id_col = ColumnProfile(name="customer_id", dtype="int", is_primary_key_candidate=True)
    score_col = ColumnProfile(name="credit_score", dtype="float")
    dataset = DatasetProfile(
        dataset_name="customers",
        row_count=5000,
        columns={"email": email_col, "customer_id": cust_id_col, "credit_score": score_col},
    )

    return CompilerContext(
        session_id=session_id,
        target_engine=engine,
        dataset_profiles={"customers": dataset},
        detected_issues=[
            Issue(
                issue_id="iss_1",
                dataset_name="customers",
                column_name="email",
                category=IssueCategory.UNTRIMMED_WHITESPACE,
                suggested_action="trim",
                message="Untrimmed whitespace",
            ),
            Issue(
                issue_id="iss_2",
                dataset_name="customers",
                column_name="email",
                category=IssueCategory.FORMAT_ANOMALY,
                suggested_action="lowercase",
                message="Mixed case email",
            ),
            Issue(
                issue_id="iss_3",
                dataset_name="customers",
                column_name="email",
                category=IssueCategory.MISSING_VALUE,
                suggested_action="fill_nulls",
                message="Missing email values",
            ),
            Issue(
                issue_id="iss_4",
                dataset_name="customers",
                column_name="customer_id",
                category=IssueCategory.DUPLICATE,
                suggested_action="deduplicate",
                message="Duplicate IDs",
            ),
            Issue(
                issue_id="iss_5",
                dataset_name="customers",
                column_name="credit_score",
                category=IssueCategory.OUTLIER,
                suggested_action="range_clip",
                message="Outlier credit scores",
                parameters={"min_value": 300, "max_value": 850},
            ),
        ],
    )


def test_test_generator_python():
    pipeline = CompilerPipeline()
    context = _build_context(ExecutionEngine.PYTHON, "sess_tg_py")
    context, snapshot = pipeline.compile(context)

    # 1. Verify companion test was generated
    assert "companion_test_artifact" in context.metadata
    test_art = context.metadata["companion_test_artifact"]
    assert test_art["name"] == "test_etl_pipeline.py"
    test_code = test_art["content"]

    # Verify specific assertions exist
    assert "def test_customers_customer_id_no_duplicates(clean_data):" in test_code
    assert "assert df['customer_id'].duplicated().sum() == 0" in test_code
    assert "def test_customers_email_no_nulls(clean_data):" in test_code
    assert "assert df['email'].isna().sum() == 0" in test_code
    assert "def test_customers_credit_score_min_bound(clean_data):" in test_code
    assert "assert (df['credit_score'] < 300).sum() == 0" in test_code
    assert "def test_customers_credit_score_max_bound(clean_data):" in test_code
    assert "assert (df['credit_score'] > 850).sum() == 0" in test_code


def test_test_generator_sql():
    pipeline = CompilerPipeline()
    context = _build_context(ExecutionEngine.AZURE_SQL, "sess_tg_sql")
    context, snapshot = pipeline.compile(context)

    # Verify companion test was generated
    assert "companion_test_artifact" in context.metadata
    test_art = context.metadata["companion_test_artifact"]
    assert test_art["name"] == "test_etl_assertions.sql"
    sql_test = test_art["content"]

    # Verify specific SQL assertions exist
    assert "Assert no duplicates in [customers].[customer_id]" in sql_test
    assert "THROW 50001, 'Duplicate records detected in [customers].[customer_id]'" in sql_test
    assert "Assert no null values in [customers].[email]" in sql_test
    assert "THROW 50002, 'Null values detected in [customers].[email]'" in sql_test
    assert "Assert values >= 300 in [customers].[credit_score]" in sql_test
    assert "Assert values <= 850 in [customers].[credit_score]" in sql_test


def test_data_contract_agent():
    context = _build_context(ExecutionEngine.PYTHON, "sess_contract")
    contract_agent = ContractAgent()
    context = contract_agent.execute(context)

    assert "data_contract_yaml" in context.metadata
    contract_yaml = context.metadata["data_contract_yaml"]

    # Parse and verify YAML structure
    parsed = yaml.safe_load(contract_yaml)
    assert parsed["version"] == "1.0.0"
    assert "customers" in parsed["datasets"]
    cust_spec = parsed["datasets"]["customers"]
    assert cust_spec["columns"]["customer_id"]["primary_key"] is True
    assert cust_spec["columns"]["email"]["nullable"] is True
    assert cust_spec["columns"]["email"]["semantic_type"] == "email"


def test_lineage_agent():
    pipeline = CompilerPipeline()
    context = _build_context(ExecutionEngine.PYTHON, "sess_lineage")
    context, snapshot = pipeline.compile(context)

    assert "lineage_entries" in context.metadata
    assert "lineage_mermaid" in context.metadata

    entries = context.metadata["lineage_entries"]
    assert len(entries) > 0

    # Find email lineage
    email_lineage = next(e for e in entries if e["source_column"] == "customers.email")
    assert email_lineage["target_column"] == "customers_clean.email"
    # email had trim, lowercase, fill_nulls
    assert "trim" in email_lineage["transformations"]
    assert "lowercase" in email_lineage["transformations"]

    # Verify Mermaid graph is valid syntax
    mermaid = context.metadata["lineage_mermaid"]
    assert mermaid.startswith("flowchart LR")
    assert "customers.email" in mermaid
    assert "customers_clean.email" in mermaid


def test_snapshot_includes_all_governance_artifacts():
    pipeline = CompilerPipeline()
    context = _build_context(ExecutionEngine.PYTHON, "sess_full_gov")
    ctx, snapshot = pipeline.compile(context)

    assert snapshot.data_contract_yaml is not None
    assert snapshot.lineage_entries is not None
    assert snapshot.companion_test is not None
    assert snapshot.generated_code is not None
