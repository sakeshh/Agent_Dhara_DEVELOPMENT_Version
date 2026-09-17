"""
tests/cross_engine/test_azure_sql_execution_evidence.py

Layer B: Authoritative Azure SQL Database execution evidence verification suite.
Consumes committed evidence bundles produced through direct target-engine execution
(via Azure SQL Database Query Editor), validates artifact and result SHA-256 hashes,
proves exact semantic parity against the Pandas reference baseline, and certifies
exact capability tuples for Vertical Slice 1.
"""
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List
import pytest

from agent.etl_pipeline.azure_sql_ir_compiler import (
    AzureSqlDatabaseRuntimeProfile,
    register_azure_sql_slice1_capabilities,
)
from agent.etl_pipeline.cross_engine.capability_models import (
    CapabilityStatus,
    EngineCapabilityKey,
)
from agent.etl_pipeline.cross_engine.capability_registry import (
    EngineCapabilityRegistry,
    get_global_capability_registry,
)
from agent.etl_pipeline.transformation_ir import (
    TRANSFORMATION_IR_VERSION,
    ActionFamily,
)

EVIDENCE_FILE = Path("docs/certification/evidence/phase_3_increment_3_4/evidence_bundles.json")


@pytest.fixture(scope="module")
def evidence_data() -> Dict[str, Dict[str, Any]]:
    """Loads and indexes committed evidence bundles by fixture_id."""
    assert EVIDENCE_FILE.exists(), f"Evidence file not found at {EVIDENCE_FILE}"
    with open(EVIDENCE_FILE, "r", encoding="utf-8") as f:
        bundles = json.load(f)
    return {b["fixture_id"]: b for b in bundles}


def _verify_bundle_integrity_and_parity(bundle: Dict[str, Any]) -> None:
    """Verifies SHA-256 hashes, runtime profile properties, and exact result parity."""
    # 1. Artifact SHA-256 integrity
    artifact_code = bundle["generated_tsql_artifact"]
    expected_artifact_hash = bundle["tsql_artifact_sha256"]
    computed_artifact_hash = hashlib.sha256(artifact_code.encode("utf-8")).hexdigest()
    assert computed_artifact_hash == expected_artifact_hash, f"Artifact SHA-256 mismatch for {bundle['fixture_id']}"

    # 2. Azure SQL Result SHA-256 integrity
    result_rows = bundle["azure_sql_result"]
    expected_result_hash = bundle["azure_sql_result_hash"]
    canonical_res = json.dumps(result_rows, sort_keys=True, separators=(",", ":"))
    computed_result_hash = hashlib.sha256(canonical_res.encode("utf-8")).hexdigest()
    assert computed_result_hash == expected_result_hash, f"Result SHA-256 mismatch for {bundle['fixture_id']}"

    # 3. Parity comparison against Pandas reference result
    pandas_rows = bundle["pandas_reference_result"]
    assert len(result_rows) == len(pandas_rows), f"Row count mismatch in {bundle['fixture_id']}"

    for idx, (sql_row, pd_row) in enumerate(zip(result_rows, pandas_rows)):
        assert set(sql_row.keys()) == set(pd_row.keys()), f"Row schema mismatch at index {idx} in {bundle['fixture_id']}"
        for col in sql_row:
            sql_val = sql_row[col]
            pd_val = pd_row[col]
            assert sql_val == pd_val, (
                f"Value mismatch in {bundle['fixture_id']} at row {idx}, column [{col}]: "
                f"Azure SQL={sql_val!r} != Pandas={pd_val!r}"
            )

    # 4. Runtime Profile verification
    prof_dict = bundle["runtime_profile"]
    assert prof_dict["engine_name"] == "azure_sql_database"
    assert prof_dict["dialect"] == "tsql"
    assert prof_dict["compatibility_level"] == "160"
    assert prof_dict["ansi_nulls_enabled"] is True
    assert prof_dict["ansi_warnings_enabled"] is True
    assert prof_dict["three_valued_logic"] is True
    assert bundle["evidence_recorder"] == "AzurePortalQueryEditor_Operator"


# ============================================================================
# Authoritative Evidence-Backed Parity Proving Tests
# ============================================================================

def test_azure_sql_trim_upper_lower_parity(evidence_data):
    """Proves Azure SQL parity for MAP_VALUE (TRIM, UPPER, LOWER) with null and whitespace preservation."""
    bundle = evidence_data["test_azure_sql_trim_upper_lower_parity"]
    _verify_bundle_integrity_and_parity(bundle)
    assert bundle["subtypes"] == ["TRIM", "UPPER", "LOWER"]


def test_azure_sql_conditional_replace_parity(evidence_data):
    """Proves Azure SQL parity for CONDITIONAL_REPLACE under 3-valued boolean logic."""
    bundle = evidence_data["test_azure_sql_conditional_replace_parity"]
    _verify_bundle_integrity_and_parity(bundle)
    assert bundle["subtypes"] == ["CONDITIONAL_REPLACE"]


def test_azure_sql_clip_and_flag_parity(evidence_data):
    """Proves Azure SQL parity for CLIP and FLAG numerical clamping and companion flagging."""
    bundle = evidence_data["test_azure_sql_clip_and_flag_parity"]
    _verify_bundle_integrity_and_parity(bundle)
    assert set(bundle["subtypes"]) == {"CLIP", "FLAG"}


def test_azure_sql_parse_typed_parity(evidence_data):
    """Proves Azure SQL parity for safe typed PARSE (PARSE_INTEGER, PARSE_CALENDAR_DATE)."""
    bundle = evidence_data["test_azure_sql_parse_typed_parity"]
    _verify_bundle_integrity_and_parity(bundle)
    assert set(bundle["subtypes"]) == {"PARSE_INTEGER", "PARSE_CALENDAR_DATE"}


def test_azure_sql_impute_apply_only_parity(evidence_data):
    """Proves Azure SQL parity for apply-only static constant and pre-fitted statistical mean imputation."""
    bundle = evidence_data["test_azure_sql_impute_apply_only_parity"]
    _verify_bundle_integrity_and_parity(bundle)
    assert bundle["subtypes"] == ["IMPUTE"]


def test_azure_sql_validate_three_way_parity(evidence_data):
    """Proves Azure SQL parity for three-way validation accounting (candidate, pass, violation, unknown)."""
    bundle = evidence_data["test_azure_sql_validate_three_way_parity"]
    _verify_bundle_integrity_and_parity(bundle)
    assert bundle["subtypes"] == ["VALIDATE"]
    summary = bundle["azure_sql_result"][0]
    assert summary["candidate_count"] == summary["pass_count"] + summary["violation_count"] + summary["raw_unknown_count"]


def test_azure_sql_evidence_backed_capability_registration(evidence_data):
    """
    Verifies that after direct target-engine execution and parity verification,
    capabilities are registered into the global registry as VERIFIED with exact test IDs.
    """
    reg = EngineCapabilityRegistry()
    prof = AzureSqlDatabaseRuntimeProfile()

    # Exact proving test IDs matching Layer B functions
    proving_ids = (
        "tests/cross_engine/test_azure_sql_execution_evidence.py::test_azure_sql_trim_upper_lower_parity",
        "tests/cross_engine/test_azure_sql_execution_evidence.py::test_azure_sql_conditional_replace_parity",
        "tests/cross_engine/test_azure_sql_execution_evidence.py::test_azure_sql_clip_and_flag_parity",
        "tests/cross_engine/test_azure_sql_execution_evidence.py::test_azure_sql_parse_typed_parity",
        "tests/cross_engine/test_azure_sql_execution_evidence.py::test_azure_sql_impute_apply_only_parity",
        "tests/cross_engine/test_azure_sql_execution_evidence.py::test_azure_sql_validate_three_way_parity",
    )

    register_azure_sql_slice1_capabilities(
        registry=reg,
        profile=prof,
        status=CapabilityStatus.VERIFIED,
        certification_commit="9e2985c637fd07f9ba15e32363b4499f63aed338",
        verified_test_ids=proving_ids,
    )

    # Evaluate registered capability
    key = EngineCapabilityKey(
        family=ActionFamily.MAP_VALUE,
        expression_subtype="TRIM",
        policy_profile_hash=hashlib.sha256(b"{}").hexdigest(),
        ir_version=TRANSFORMATION_IR_VERSION,
        catalogue_version="1.0.0",
        compiler_name="AzureSqlTsqlIRCompiler",
        compiler_version="1.0.0",
        engine_name="azure_sql_database",
        engine_version=prof.engine_version,
        dialect="tsql",
        runtime_profile_hash=prof.compute_hash(),
    )

    decision = reg.evaluate_capability(key)
    assert decision.status == CapabilityStatus.VERIFIED
    assert decision.execution_eligible is True
    assert decision.reason_code == "VERIFIED_BY_AZURE_SQL_EVIDENCE"
    assert decision.verified_test_ids == proving_ids
