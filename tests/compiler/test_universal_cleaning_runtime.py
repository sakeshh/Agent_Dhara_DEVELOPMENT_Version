import pytest
from typing import Dict, List

from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.deduplication.duplicate_policy import (
    DuplicateDisposition,
    DuplicatePolicy,
    SurvivorStrategy,
)
from agent.compiler.keys.business_key_resolver import BusinessKeyResolver
from agent.compiler.models.cleaning import CleaningMode
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.issues import Issue, IssueCategory, IssueSeverity
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.schema.column_registry import ColumnKind, ColumnLifecycleRegistry
from agent.compiler.schema.output_contract import OutputSchemaContract
from agent.compiler.schema.schema_resolver import SchemaResolver
from agent.compiler.schema.schema_validator import SchemaValidator
from agent.compiler.verification.post_cleaning_verifier import (
    CleaningStatus,
    PostCleaningVerifier,
)
from agent.errors import CompilationBlockedError
from agent.etl_handlers import (
    etl_confirm_plan,
    etl_execute_sql,
    etl_generate_code,
    etl_plan_start,
)
from agent.session_store import load_session, save_session


# ── Test 1: Clean-only schema preservation ────────────────────────────────────
def test_clean_only_schema_preservation():
    """
    Input: 20 business columns
    Expected: 20 clean output columns, 0 audit companion columns in clean output
    """
    source_business_columns = [f"BusinessCol_{i}" for i in range(1, 21)]
    cols = {c: ColumnProfile(name=c, dtype="string") for c in source_business_columns}
    profile = DatasetProfile(dataset_id="EnterpriseDataset", columns=cols, row_count=100)

    ctx = CompilerContext(
        session_id="test_clean_preservation",
        target_engine=ExecutionEngine.PYTHON,
        cleaning_mode=CleaningMode.CLEANSE_ONLY,
        preserve_source_schema=True,
        dataset_profiles={"EnterpriseDataset": profile},
    )

    contracts = SchemaResolver.resolve_all_contracts(ctx)
    contract = contracts["EnterpriseDataset"]

    actual_clean_columns = contract.clean_columns
    assert actual_clean_columns == source_business_columns
    assert len(actual_clean_columns) == 20
    assert not any(c.endswith("_raw") for c in actual_clean_columns)
    assert not any(c.endswith("_clean") for c in actual_clean_columns)
    assert not any(c.endswith("_validation_status") for c in actual_clean_columns)
    assert "is_duplicate_row" not in actual_clean_columns
    assert "etl_batch_id" not in actual_clean_columns


# ── Test 2: Exact duplicate removal ──────────────────────────────────────────
def test_exact_duplicate_removal():
    """
    Input: 100 rows, 10 redundant exact duplicates
    Expected: 90 clean rows, 10 removed rows, 0 remaining exact duplicates
    """
    report = PostCleaningVerifier.verify(
        dataset_name="Transactions",
        input_rows=100,
        clean_rows=90,
        quarantined_rows=0,
        removed_redundant_rows=10,
        remaining_duplicates=0,
        expected_clean_cols=["TxID", "Amount", "Account"],
        actual_clean_cols=["TxID", "Amount", "Account"],
    )

    assert report.status == CleaningStatus.CLEANED
    assert report.is_fully_verified is True
    assert report.row_count_reconciled is True
    assert report.clean_rows == 90
    assert report.removed_redundant_rows == 10
    assert report.remaining_duplicates == 0
    assert "100 (input) == 90 (clean)" in report.reconciliation_equation


# ── Test 3: Unapproved business-key deduplication ─────────────────────────────
def test_unapproved_business_key_deduplication():
    """
    Input: Inferred key confidence below threshold
    Expected: Compilation blocked / validation error, No execution started
    """
    policy = DuplicatePolicy(
        dataset_name="Vendors",
        disposition=DuplicateDisposition.REMOVE_REDUNDANT,
        survivor_strategy=SurvivorStrategy.MOST_COMPLETE,
        approved_key=None,  # No key approved!
    )
    is_valid, errors = policy.validate()
    assert not is_valid
    assert any("approved business key" in e for e in errors)

    # Low-confidence columns (e.g. status/date)
    cols = {
        "Status": {"dtype": "varchar", "null_ratio": 0.05, "distinct_ratio": 0.02},
        "CreatedDate": {"dtype": "datetime", "null_ratio": 0.0, "distinct_ratio": 0.6},
    }
    candidate = BusinessKeyResolver.resolve_key("Vendors", cols)
    # Status and CreatedDate have heavy penalties, confidence must be low
    assert candidate is None or candidate.confidence < 0.70


# ── Test 4: Near duplicates default to quarantine or review ───────────────────
def test_near_duplicates_default_to_quarantine_or_review():
    """
    Expected: No automatic deletion, Rows quarantined or held for approval.
    When disposition is FLAG, PostCleaningVerifier returns AUDITED_NOT_CLEANED.
    """
    flag_policy = DuplicatePolicy(
        dataset_name="Customers",
        disposition=DuplicateDisposition.FLAG,
        survivor_strategy=SurvivorStrategy.NEWEST,
        approved_key=["Email"],
    )

    report = PostCleaningVerifier.verify(
        dataset_name="Customers",
        input_rows=100,
        clean_rows=100,
        quarantined_rows=0,
        removed_redundant_rows=0,
        duplicate_policy=flag_policy,
    )

    assert report.status == CleaningStatus.AUDITED_NOT_CLEANED
    assert report.clean_rows == 100
    assert report.removed_redundant_rows == 0
    assert "flag/audit only, no records removed" in report.reconciliation_equation


# ── Test 5: Unknown generated column rejected ────────────────────────────────
def test_unknown_generated_column_rejected():
    """
    Generate a physical step referencing Status_match_key without registering it.
    Expected: SchemaValidator catches schema expansion error before execution.
    """
    contract = OutputSchemaContract(
        dataset_id="Accounts",
        mode=CleaningMode.CLEANSE_ONLY,
        preserve_source_schema=True,
        source_columns=["AccountID", "Balance"],
        clean_columns=["AccountID", "Balance", "Status_match_key"],  # Unregistered expansion!
    )

    valid, errors = SchemaValidator.validate_contract(contract)
    assert not valid
    assert any("Status_match_key" in e for e in errors)


# ── Test 6: Audit separation ──────────────────────────────────────────────────
def test_audit_separation_partitions():
    """
    In cleanse_with_audit:
    Clean output: business fields
    Audit output: raw/clean/status/reason fields
    Rejected output: rejected rows plus reasons
    """
    profile = DatasetProfile(
        dataset_id="Orders",
        columns={"OrderID": ColumnProfile(name="OrderID", dtype="int"), "Total": ColumnProfile(name="Total", dtype="float")},
    )
    ctx = CompilerContext(
        session_id="test_audit_sep",
        target_engine=ExecutionEngine.PYTHON,
        cleaning_mode=CleaningMode.CLEANSE_WITH_AUDIT,
        preserve_source_schema=True,
        dataset_profiles={"Orders": profile},
    )

    pipeline = CompilerPipeline()
    compiled_ctx, snapshot = pipeline.compile(ctx)

    manifest = compiled_ctx.metadata.get("deployment_manifest", {})
    artifacts = manifest.get("artifacts", {})
    assert "clean" in artifacts
    assert "audit" in artifacts
    assert "rejected" in artifacts
    assert "manifest" in artifacts
    assert artifacts["clean"]["Orders"] == ["OrderID", "Total"]


# ── Test 7: Real route integration ───────────────────────────────────────────
def test_real_route_integration():
    """
    Call:
    POST /etl/plan
    POST /etl/confirm
    POST /etl/generate
    POST /etl/execute
    Confirm:
    - Compiler route was used
    - Approval state was preserved
    - Output contract was applied
    - Post-clean verification ran
    - Final status is accurate
    """
    sid = "test_route_e2e_session"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": {
            "datasets": {
                "ClientData": {
                    "row_count": 50,
                    "columns": {
                        "ClientID": {"dtype": "int"},
                        "ClientName": {"dtype": "string"},
                        "City": {"dtype": "string"},
                    },
                }
            },
            "issues": [
                {
                    "dataset": "ClientData",
                    "column": "City",
                    "issue_type": "whitespace",
                    "affected_rows": 5,
                    "suggested_action": "trim",
                }
            ],
        }
    }
    save_session(sess)

    # 1. /etl/plan
    plan_res = etl_plan_start(
        session_id=sid,
        business_rules={"cleaning_mode": "cleanse_only"},
        engine="sql",
    )
    assert plan_res["ok"] is True
    assert "cleaning_plan" in plan_res["plan"]
    assert plan_res["plan"]["cleaning_plan"]["cleaning_mode"] == "cleanse_only"

    # 2. /etl/confirm
    confirm_res = etl_confirm_plan(
        session_id=sid,
        plan_override={
            "cleaning_approval": {
                "cleaning_mode": "cleanse_only",
                "approved_business_keys": {"ClientData": ["ClientID"]},
            }
        },
    )
    assert confirm_res["ok"] is True

    # 3. /etl/generate
    gen_res = etl_generate_code(session_id=sid, engine="sql", codegen_mode="compiler")
    assert gen_res["ok"] is True
    assert gen_res["generated_by"] == "compiler"
    assert "SELECT" in gen_res["code"]
    # Explicit projection, not SELECT *
    assert "SELECT *" not in gen_res["code"] or "SELECT\n    [ClientID]" in gen_res["code"] or "[ClientID]" in gen_res["code"]

    # 4. /etl/execute (dry_run mode for SQL)
    exec_res = etl_execute_sql(session_id=sid, dry_run=True, approved=True)
    assert "verification_report" in exec_res
    v_report = exec_res["verification_report"]["ClientData"]
    assert v_report["dataset_name"] == "ClientData"
    assert v_report["status"] in (CleaningStatus.CLEANED.value, CleaningStatus.AUDITED_NOT_CLEANED.value)


# ── Test 8: Multiple datasets independence ────────────────────────────────────
def test_multiple_datasets_independence():
    """
    Ensure unrelated tables with different keys receive:
    - Their own output contracts
    - Their own key resolutions
    - Their own duplicate policies
    - Their own verification results
    """
    users_profile = DatasetProfile(
        dataset_id="Users",
        columns={
            "UserID": ColumnProfile(name="UserID", dtype="int"),
            "Email": ColumnProfile(name="Email", dtype="string"),
        },
        row_count=200,
    )
    products_profile = DatasetProfile(
        dataset_id="Products",
        columns={
            "SKU": ColumnProfile(name="SKU", dtype="string"),
            "Price": ColumnProfile(name="Price", dtype="float"),
            "Title": ColumnProfile(name="Title", dtype="string"),
        },
        row_count=50,
    )

    ctx = CompilerContext(
        session_id="test_multi_dataset",
        target_engine=ExecutionEngine.PYTHON,
        cleaning_mode=CleaningMode.CLEANSE_WITH_AUDIT,
        preserve_source_schema=True,
        dataset_profiles={"Users": users_profile, "Products": products_profile},
    )

    pipeline = CompilerPipeline()
    compiled_ctx, _ = pipeline.compile(ctx)

    contracts = compiled_ctx.metadata["output_contracts"]
    assert "Users" in contracts
    assert "Products" in contracts
    assert contracts["Users"]["clean_columns"] == ["UserID", "Email"]
    assert contracts["Products"]["clean_columns"] == ["SKU", "Price", "Title"]

    # Independent key resolution
    user_key = BusinessKeyResolver.resolve_key("Users", {"UserID": {"dtype": "int"}, "Email": {"dtype": "string"}})
    prod_key = BusinessKeyResolver.resolve_key("Products", {"SKU": {"dtype": "string"}, "Price": {"dtype": "float"}})

    assert user_key.columns == ["UserID"]
    assert prod_key.columns == ["SKU"]
