"""
tests/cross_engine/test_adf_orchestration.py

Test suite for Azure Data Factory (ADF) Orchestration & Delegation Boundary Adapter.
Proves Mandatory Corrections 7 and 8:
- Mandatory Correction 7:
  * Control plane operations are certified as VERIFIED_ORCHESTRATION (execution_eligible=True).
  * Delegated transformations are ORCHESTRATION_ONLY (execution_eligible=False at the ADF level).
  * Native ADF transformations are NOT_TESTED.
- Mandatory Correction 8:
  * 13-field execution envelope validation, deterministic digest generation, and parameter binding.
"""
import pytest

from agent.etl_pipeline.adf_orchestration_adapter import (
    AdfEnvelopeValidationError,
    AdfExecutionEnvelope,
    AdfOrchestrationAdapter,
)
from agent.etl_pipeline.cross_engine.capability_models import (
    CapabilityStatus,
    EngineCapabilityKey,
)
from agent.etl_pipeline.cross_engine.capability_registry import (
    EngineCapabilityRegistry,
)
from agent.etl_pipeline.transformation_ir import ActionFamily


@pytest.fixture
def adf_registry() -> EngineCapabilityRegistry:
    reg = EngineCapabilityRegistry()
    adapter = AdfOrchestrationAdapter()
    adapter.register_adf_capabilities(reg)
    return reg


@pytest.fixture
def valid_envelope() -> AdfExecutionEnvelope:
    return AdfExecutionEnvelope(
        tenant_id="tenant-prod-101",
        plan_id="plan-etl-daily-999",
        plan_digest="a" * 64,
        artifact_id="nb_customer_enrichment",
        artifact_sha256="b" * 64,
        compiler_name="pyspark_ir_compiler",
        compiler_version="v1.0",
        engine_name="pyspark",
        engine_version="3.5.1",
        runtime_profile_hash="c" * 64,
        capability_decision_ids=("dec-map-01", "dec-filter-02", "dec-join-03"),
        parameter_manifest_hash="d" * 64,
        publication_scope="certified_production",
    )


def test_adf_envelope_13_fields_validation_success(valid_envelope):
    """Proves valid 13-field execution envelope succeeds validation and generates deterministic digest."""
    adapter = AdfOrchestrationAdapter()
    assert adapter.validate_envelope(valid_envelope) is True

    digest1 = valid_envelope.compute_envelope_digest()
    digest2 = valid_envelope.compute_envelope_digest()
    assert len(digest1) == 64
    assert digest1 == digest2


def test_adf_envelope_missing_or_corrupted_fields_rejected():
    """Proves missing or non-conforming fields raise validation errors (Mandatory Correction 8)."""
    # 1. Invalid publication scope
    with pytest.raises(ValueError):
        AdfExecutionEnvelope(
            tenant_id="tenant-1",
            plan_id="plan-1",
            plan_digest="a" * 64,
            artifact_id="art-1",
            artifact_sha256="b" * 64,
            compiler_name="pyspark_ir_compiler",
            compiler_version="v1.0",
            engine_name="pyspark",
            engine_version="3.5.1",
            runtime_profile_hash="c" * 64,
            capability_decision_ids=("dec-1",),
            parameter_manifest_hash="d" * 64,
            publication_scope="unauthorized_public_scope",
        )

    # 2. Non-hex sha256
    with pytest.raises(ValueError):
        AdfExecutionEnvelope(
            tenant_id="tenant-1",
            plan_id="plan-1",
            plan_digest="invalid_short_hash",
            artifact_id="art-1",
            artifact_sha256="b" * 64,
            compiler_name="pyspark_ir_compiler",
            compiler_version="v1.0",
            engine_name="pyspark",
            engine_version="3.5.1",
            runtime_profile_hash="c" * 64,
            capability_decision_ids=("dec-1",),
            parameter_manifest_hash="d" * 64,
            publication_scope="internal_staged",
        )

    # 3. Empty capability decisions
    adapter = AdfOrchestrationAdapter()
    envelope_no_dec = AdfExecutionEnvelope(
        tenant_id="tenant-1",
        plan_id="plan-1",
        plan_digest="a" * 64,
        artifact_id="art-1",
        artifact_sha256="b" * 64,
        compiler_name="pyspark_ir_compiler",
        compiler_version="v1.0",
        engine_name="pyspark",
        engine_version="3.5.1",
        runtime_profile_hash="c" * 64,
        capability_decision_ids=(),
        parameter_manifest_hash="d" * 64,
        publication_scope="internal_staged",
    )
    with pytest.raises(AdfEnvelopeValidationError) as exc:
        adapter.validate_envelope(envelope_no_dec)
    assert exc.value.reason_code == "EMPTY_CAPABILITY_DECISIONS"

    # 4. Engine / Compiler mismatch
    envelope_mismatch = AdfExecutionEnvelope(
        tenant_id="tenant-1",
        plan_id="plan-1",
        plan_digest="a" * 64,
        artifact_id="art-1",
        artifact_sha256="b" * 64,
        compiler_name="pyspark_ir_compiler",
        compiler_version="v1.0",
        engine_name="azure_sql_database",  # Mismatch!
        engine_version="12.0.2000.8",
        runtime_profile_hash="c" * 64,
        capability_decision_ids=("dec-1",),
        parameter_manifest_hash="d" * 64,
        publication_scope="internal_staged",
    )
    with pytest.raises(AdfEnvelopeValidationError) as exc_mismatch:
        adapter.validate_envelope(envelope_mismatch)
    assert exc_mismatch.value.reason_code == "ENGINE_COMPILER_MISMATCH"


def test_adf_control_plane_verified_orchestration_eligible(adf_registry):
    """Proves ADF control plane capabilities are VERIFIED_ORCHESTRATION with execution_eligible=True."""
    adapter = AdfOrchestrationAdapter()
    for subtype in ("PIPELINE_ORCHESTRATION", "ACTIVITY_DISPATCH", "ENVELOPE_BINDING", "RETRY_HANDLING"):
        key = EngineCapabilityKey(
            family=ActionFamily.PASS_THROUGH,
            expression_subtype=subtype,
            policy_profile_hash="00" * 32,
            compiler_name=adapter.COMPILER_NAME,
            compiler_version=adapter.COMPILER_VERSION,
            engine_name=adapter.ENGINE_NAME,
            engine_version=adapter.ENGINE_VERSION,
            dialect=adapter.DIALECT,
            runtime_profile_hash=adapter.RUNTIME_PROFILE_HASH,
        )
        dec = adf_registry.evaluate_capability(key)
        assert dec.status == CapabilityStatus.VERIFIED_ORCHESTRATION
        assert dec.execution_eligible is True


def test_adf_delegated_transformations_orchestration_only_not_eligible(adf_registry):
    """Proves all 18 delegated transformation families are ORCHESTRATION_ONLY with execution_eligible=False."""
    adapter = AdfOrchestrationAdapter()
    transformation_families = [
        ActionFamily.MAP_VALUE,
        ActionFamily.FILTER,
        ActionFamily.JOIN,
        ActionFamily.AGGREGATE,
        ActionFamily.DEDUPLICATE,
        ActionFamily.HASH,
        ActionFamily.MASK,
        ActionFamily.SCHEMA_MUTATION,
        ActionFamily.DERIVE,
        ActionFamily.EXTRACT,
        ActionFamily.PARSE,
        ActionFamily.IMPUTE,
        ActionFamily.CLIP,
        ActionFamily.FLAG,
        ActionFamily.STANDARDIZE,
        ActionFamily.VALIDATE,
        ActionFamily.QUARANTINE,
        ActionFamily.CONDITIONAL_REPLACE,
    ]
    for fam in transformation_families:
        key = EngineCapabilityKey(
            family=fam,
            expression_subtype="DELEGATED_EXECUTION",
            policy_profile_hash="00" * 32,
            compiler_name=adapter.COMPILER_NAME,
            compiler_version=adapter.COMPILER_VERSION,
            engine_name=adapter.ENGINE_NAME,
            engine_version=adapter.ENGINE_VERSION,
            dialect=adapter.DIALECT,
            runtime_profile_hash=adapter.RUNTIME_PROFILE_HASH,
        )
        dec = adf_registry.evaluate_capability(key)
        assert dec.status == CapabilityStatus.ORCHESTRATION_ONLY
        assert dec.execution_eligible is False, f"{fam.value} must NOT be execution-eligible under ADF"


def test_adf_native_transformations_not_tested_not_eligible(adf_registry):
    """Proves native ADF mapping data flows are NOT_TESTED with execution_eligible=False."""
    adapter = AdfOrchestrationAdapter()
    key = EngineCapabilityKey(
        family=ActionFamily.MAP_VALUE,
        expression_subtype="NATIVE_DATA_FLOW",
        policy_profile_hash="00" * 32,
        compiler_name=adapter.COMPILER_NAME,
        compiler_version=adapter.COMPILER_VERSION,
        engine_name=adapter.ENGINE_NAME,
        engine_version=adapter.ENGINE_VERSION,
        dialect="mapping_data_flow",
        runtime_profile_hash=adapter.RUNTIME_PROFILE_HASH,
    )
    dec = adf_registry.evaluate_capability(key)
    assert dec.status == CapabilityStatus.NOT_TESTED
    assert dec.execution_eligible is False


def test_adf_pipeline_dispatch_manifest_synapse_notebook(valid_envelope):
    """Proves pipeline dispatch manifest binds the 13 envelope parameters into Synapse notebook activity."""
    adapter = AdfOrchestrationAdapter()
    manifest = adapter.compile_pipeline_dispatch(valid_envelope, activity_name="RunPySparkNotebook")
    assert manifest.activity_name == "RunPySparkNotebook"
    assert manifest.activity_type == "SynapseNotebook"
    assert manifest.orchestration_status == CapabilityStatus.VERIFIED_ORCHESTRATION
    assert manifest.delegated_transformation_status == CapabilityStatus.ORCHESTRATION_ONLY

    params = manifest.activity_json["typeProperties"]["parameters"]
    assert params["dhara_tenant_id"]["value"] == "tenant-prod-101"
    assert params["dhara_plan_id"]["value"] == "plan-etl-daily-999"
    assert params["dhara_compiler_name"]["value"] == "pyspark_ir_compiler"
    assert params["dhara_envelope_digest"]["value"] == manifest.envelope_digest


def test_adf_pipeline_dispatch_manifest_sql_script():
    """Proves pipeline dispatch manifest binds the envelope parameters into Azure SQL Script activity."""
    sql_envelope = AdfExecutionEnvelope(
        tenant_id="tenant-sql-202",
        plan_id="plan-sql-dim-customer",
        plan_digest="1" * 64,
        artifact_id="sp_transform_customer_dim",
        artifact_sha256="2" * 64,
        compiler_name="azure_sql_tsql_ir_compiler",
        compiler_version="v1.0",
        engine_name="azure_sql_database",
        engine_version="12.0.2000.8",
        runtime_profile_hash="3" * 64,
        capability_decision_ids=("dec-sql-01",),
        parameter_manifest_hash="4" * 64,
        publication_scope="internal_staged",
    )
    adapter = AdfOrchestrationAdapter()
    manifest = adapter.compile_pipeline_dispatch(sql_envelope, activity_name="RunAzureSqlScript")
    assert manifest.activity_name == "RunAzureSqlScript"
    assert manifest.activity_type == "Script"
    assert manifest.orchestration_status == CapabilityStatus.VERIFIED_ORCHESTRATION
    assert manifest.delegated_transformation_status == CapabilityStatus.ORCHESTRATION_ONLY

    params = manifest.activity_json["typeProperties"]["parameters"]
    assert params["@tenant_id"]["value"] == "tenant-sql-202"
    assert params["@envelope_digest"]["value"] == manifest.envelope_digest
