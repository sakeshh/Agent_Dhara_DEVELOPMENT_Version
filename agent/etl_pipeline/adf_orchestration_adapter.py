"""
agent/etl_pipeline/adf_orchestration_adapter.py

Azure Data Factory (ADF) Orchestration & Delegation Boundary Adapter.
Enforces Mandatory Corrections 7 and 8:
- Mandatory Correction 7:
  * ADF pipeline orchestration control plane is certified as VERIFIED_ORCHESTRATION (for pipeline trigger,
    activity sequence, parameter passing, failure handling, retry).
  * Delegated execution (ADF Synapse/Databricks activity dispatching PySpark artifact, or Azure SQL
    stored proc/script activity dispatching T-SQL artifact) is classified as ORCHESTRATION_ONLY.
  * Crucially, at the ADF level, execution_eligible = False for data transformation semantics.
    The transformation capability is verified at the PySpark or Azure SQL compiler layer, NOT at the ADF layer.
  * Native ADF Mapping Data Flows (ADF native transformations) are classified as NOT_TESTED.
- Mandatory Correction 8:
  * ADF Execution Envelope & Manifest binding:
    Enforces the exact 13-field execution envelope:
    1. tenant_id
    2. plan_id
    3. plan_digest
    4. artifact_id
    5. artifact_sha256
    6. compiler_name
    7. compiler_version
    8. engine_name
    9. engine_version
    10. runtime_profile_hash
    11. capability_decision_ids
    12. parameter_manifest_hash
    13. publication_scope
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from pydantic import BaseModel, Field, field_validator

from agent.etl_pipeline.cross_engine.capability_models import (
    CapabilityDecision,
    CapabilityStatus,
    EngineCapabilityKey,
)
from agent.etl_pipeline.cross_engine.capability_registry import (
    EngineCapabilityRegistry,
)
from agent.etl_pipeline.transformation_ir import ActionFamily


class AdfEnvelopeValidationError(Exception):
    """Raised when an ADF execution envelope fails structural, cryptographic, or scope validation."""

    def __init__(self, message: str, *, reason_code: str = "INVALID_ADF_ENVELOPE"):
        super().__init__(f"[{reason_code}] {message}")
        self.reason_code = reason_code


class AdfExecutionEnvelope(BaseModel):
    """
    Mandatory 13-field execution envelope for Azure Data Factory dispatch (Mandatory Correction 8).
    Binds the generated artifact, compiler provenance, runtime profile, and capability decisions
    into an immutable dispatch contract.
    """
    tenant_id: str
    plan_id: str
    plan_digest: str
    artifact_id: str
    artifact_sha256: str
    compiler_name: str
    compiler_version: str
    engine_name: str
    engine_version: str
    runtime_profile_hash: str
    capability_decision_ids: Tuple[str, ...]
    parameter_manifest_hash: str
    publication_scope: str

    @field_validator("publication_scope")
    @classmethod
    def validate_scope(cls, v: str) -> str:
        allowed = {"internal_staged", "certified_production"}
        if v not in allowed:
            raise ValueError(f"publication_scope must be one of {allowed}, got {v!r}")
        return v

    @field_validator("plan_digest", "artifact_sha256", "runtime_profile_hash", "parameter_manifest_hash")
    @classmethod
    def validate_sha256_hex(cls, v: str) -> str:
        if not re.fullmatch(r"[0-9a-fA-F]{64}", v):
            raise ValueError(f"Field value must be a valid 64-character SHA256 hex string, got {v!r}")
        return v.lower()

    def compute_envelope_digest(self) -> str:
        """Deterministic SHA256 digest of all 13 canonical envelope fields."""
        data = {
            "tenant_id": self.tenant_id,
            "plan_id": self.plan_id,
            "plan_digest": self.plan_digest,
            "artifact_id": self.artifact_id,
            "artifact_sha256": self.artifact_sha256,
            "compiler_name": self.compiler_name,
            "compiler_version": self.compiler_version,
            "engine_name": self.engine_name,
            "engine_version": self.engine_version,
            "runtime_profile_hash": self.runtime_profile_hash,
            "capability_decision_ids": list(self.capability_decision_ids),
            "parameter_manifest_hash": self.parameter_manifest_hash,
            "publication_scope": self.publication_scope,
        }
        raw = json.dumps(data, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class AdfPipelineDispatchManifest(BaseModel):
    """
    Synthesized ADF activity definition binding the 13-field envelope to pipeline parameters.
    """
    activity_name: str
    activity_type: str
    envelope: AdfExecutionEnvelope
    envelope_digest: str
    activity_json: Dict[str, Any]
    orchestration_status: CapabilityStatus = CapabilityStatus.VERIFIED_ORCHESTRATION
    delegated_transformation_status: CapabilityStatus = CapabilityStatus.ORCHESTRATION_ONLY


class AdfOrchestrationAdapter:
    """
    Authoritative adapter for Azure Data Factory orchestration boundaries.
    Enforces that:
    1. ADF control plane dispatch is VERIFIED_ORCHESTRATION (execution_eligible=True).
    2. Delegated transformations are ORCHESTRATION_ONLY (execution_eligible=False at ADF level).
    3. Native ADF transformations are NOT_TESTED.
    4. Execution envelope strictly contains all 13 fields.
    """
    ENGINE_NAME: str = "azure_data_factory"
    ENGINE_VERSION: str = "v2"
    DIALECT: str = "adf_pipeline_json"
    COMPILER_NAME: str = "adf_orchestration_adapter"
    COMPILER_VERSION: str = "v1.0"
    RUNTIME_PROFILE_HASH: str = "00adf00orchestration00v200profile00hash0000000000000000000000000000"

    def __init__(self, capability_registry: Optional[EngineCapabilityRegistry] = None):
        self.registry = capability_registry or EngineCapabilityRegistry()

    def validate_envelope(self, envelope: AdfExecutionEnvelope) -> bool:
        """
        Validates the 13-field execution envelope against certified engines and scopes.
        """
        if not envelope.tenant_id or not envelope.plan_id:
            raise AdfEnvelopeValidationError("tenant_id and plan_id must be non-empty", reason_code="MISSING_IDENTITY")

        if not envelope.capability_decision_ids:
            raise AdfEnvelopeValidationError(
                "ADF execution envelope requires at least one verified capability decision ID",
                reason_code="EMPTY_CAPABILITY_DECISIONS",
            )

        certified_compilers = {
            "pyspark_ir_compiler": "pyspark",
            "azure_sql_tsql_ir_compiler": "azure_sql_database",
            "pandas_ir_compiler": "pandas",
        }
        expected_engine = certified_compilers.get(envelope.compiler_name)
        if expected_engine != envelope.engine_name:
            raise AdfEnvelopeValidationError(
                f"Compiler {envelope.compiler_name} cannot dispatch to uncertified engine {envelope.engine_name}",
                reason_code="ENGINE_COMPILER_MISMATCH",
            )

        return True

    def compile_pipeline_dispatch(
        self,
        envelope: AdfExecutionEnvelope,
        *,
        activity_name: str = "DharaExecuteTargetStep",
    ) -> AdfPipelineDispatchManifest:
        """
        Compiles the ADF activity JSON binding the 13 envelope fields to activity parameters.
        """
        self.validate_envelope(envelope)
        digest = envelope.compute_envelope_digest()

        # Select activity type based on target engine
        if envelope.engine_name == "pyspark":
            activity_type = "SynapseNotebook"
            type_props = {
                "notebook": {"referenceName": envelope.artifact_id, "type": "NotebookReference"},
                "parameters": {
                    "dhara_tenant_id": {"value": envelope.tenant_id, "type": "string"},
                    "dhara_plan_id": {"value": envelope.plan_id, "type": "string"},
                    "dhara_plan_digest": {"value": envelope.plan_digest, "type": "string"},
                    "dhara_artifact_sha256": {"value": envelope.artifact_sha256, "type": "string"},
                    "dhara_compiler_name": {"value": envelope.compiler_name, "type": "string"},
                    "dhara_compiler_version": {"value": envelope.compiler_version, "type": "string"},
                    "dhara_engine_name": {"value": envelope.engine_name, "type": "string"},
                    "dhara_engine_version": {"value": envelope.engine_version, "type": "string"},
                    "dhara_runtime_profile_hash": {"value": envelope.runtime_profile_hash, "type": "string"},
                    "dhara_parameter_manifest_hash": {"value": envelope.parameter_manifest_hash, "type": "string"},
                    "dhara_publication_scope": {"value": envelope.publication_scope, "type": "string"},
                    "dhara_envelope_digest": {"value": digest, "type": "string"},
                },
            }
        elif envelope.engine_name == "azure_sql_database":
            activity_type = "Script"
            type_props = {
                "scripts": [
                    {
                        "text": f"-- Dhara Certified Execution: {envelope.artifact_id}\n-- Digest: {digest}",
                        "type": "Query",
                    }
                ],
                "parameters": {
                    "@tenant_id": {"value": envelope.tenant_id, "type": "String"},
                    "@plan_id": {"value": envelope.plan_id, "type": "String"},
                    "@plan_digest": {"value": envelope.plan_digest, "type": "String"},
                    "@artifact_sha256": {"value": envelope.artifact_sha256, "type": "String"},
                    "@envelope_digest": {"value": digest, "type": "String"},
                },
            }
        else:
            activity_type = "Custom"
            type_props = {"command": f"dhara-run --envelope {digest}"}

        activity_json = {
            "name": activity_name,
            "type": activity_type,
            "dependsOn": [],
            "policy": {
                "timeout": "0.12:00:00",
                "retry": 3,
                "retryIntervalInSeconds": 30,
                "secureOutput": False,
                "secureInput": False,
            },
            "userProperties": [
                {"name": "dhara_envelope_digest", "value": digest},
                {"name": "dhara_publication_scope", "value": envelope.publication_scope},
            ],
            "typeProperties": type_props,
        }

        return AdfPipelineDispatchManifest(
            activity_name=activity_name,
            activity_type=activity_type,
            envelope=envelope,
            envelope_digest=digest,
            activity_json=activity_json,
            orchestration_status=CapabilityStatus.VERIFIED_ORCHESTRATION,
            delegated_transformation_status=CapabilityStatus.ORCHESTRATION_ONLY,
        )

    def register_adf_capabilities(
        self,
        registry: EngineCapabilityRegistry,
        *,
        certification_commit: str = "phase3_adf_boundary_certification",
    ) -> None:
        """
        Registers ADF capabilities according to Mandatory Corrections 7 & 8:
        1. PASS_THROUGH orchestration control plane -> VERIFIED_ORCHESTRATION (execution_eligible=True).
        2. Delegated transformations -> ORCHESTRATION_ONLY (execution_eligible=False).
        3. Native transformations -> NOT_TESTED (execution_eligible=False).
        """
        # 1. Control Plane Capabilities (Verified Orchestration)
        orchestration_subtypes = (
            "PIPELINE_ORCHESTRATION",
            "ACTIVITY_DISPATCH",
            "ENVELOPE_BINDING",
            "RETRY_HANDLING",
            "SYNAPSE_DELEGATION",
            "AZURE_SQL_DELEGATION",
        )
        for st in orchestration_subtypes:
            k = EngineCapabilityKey(
                family=ActionFamily.PASS_THROUGH,
                expression_subtype=st,
                policy_profile_hash="00" * 32,
                compiler_name=self.COMPILER_NAME,
                compiler_version=self.COMPILER_VERSION,
                engine_name=self.ENGINE_NAME,
                engine_version=self.ENGINE_VERSION,
                dialect=self.DIALECT,
                runtime_profile_hash=self.RUNTIME_PROFILE_HASH,
            )
            registry.register_capability(
                key=k,
                status=CapabilityStatus.VERIFIED_ORCHESTRATION,
                reason_code="ADF_CONTROL_PLANE_VERIFIED",
                verified_test_ids=(f"test_adf_orchestration_{st.lower()}",),
                certification_commit=certification_commit,
            )

        # 2. Delegated Transformation Capabilities (Orchestration Only - execution_eligible=False)
        # Covers all transformation families when delegated through ADF
        transformation_families = [
            ActionFamily.VALIDATE,
            ActionFamily.QUARANTINE,
            ActionFamily.FLAG,
            ActionFamily.MAP_VALUE,
            ActionFamily.CONDITIONAL_REPLACE,
            ActionFamily.CLIP,
            ActionFamily.IMPUTE,
            ActionFamily.STANDARDIZE,
            ActionFamily.PARSE,
            ActionFamily.EXTRACT,
            ActionFamily.DERIVE,
            ActionFamily.FILTER,
            ActionFamily.JOIN,
            ActionFamily.AGGREGATE,
            ActionFamily.DEDUPLICATE,
            ActionFamily.HASH,
            ActionFamily.MASK,
            ActionFamily.SCHEMA_MUTATION,
        ]
        for fam in transformation_families:
            k_del = EngineCapabilityKey(
                family=fam,
                expression_subtype="DELEGATED_EXECUTION",
                policy_profile_hash="00" * 32,
                compiler_name=self.COMPILER_NAME,
                compiler_version=self.COMPILER_VERSION,
                engine_name=self.ENGINE_NAME,
                engine_version=self.ENGINE_VERSION,
                dialect=self.DIALECT,
                runtime_profile_hash=self.RUNTIME_PROFILE_HASH,
            )
            registry.register_capability(
                key=k_del,
                status=CapabilityStatus.ORCHESTRATION_ONLY,
                reason_code="ADF_TRANSFORMATION_DELEGATED_TO_TARGET_ENGINE",
                limitations=("Transformation authority belongs to delegated target engine, not ADF.",),
                certification_commit=certification_commit,
            )

            # 3. Native ADF Mapping Data Flows (Not Tested - execution_eligible=False)
            k_nat = EngineCapabilityKey(
                family=fam,
                expression_subtype="NATIVE_DATA_FLOW",
                policy_profile_hash="00" * 32,
                compiler_name=self.COMPILER_NAME,
                compiler_version=self.COMPILER_VERSION,
                engine_name=self.ENGINE_NAME,
                engine_version=self.ENGINE_VERSION,
                dialect="mapping_data_flow",
                runtime_profile_hash=self.RUNTIME_PROFILE_HASH,
            )
            registry.register_capability(
                key=k_nat,
                status=CapabilityStatus.NOT_TESTED,
                reason_code="ADF_NATIVE_DATA_FLOW_NOT_TESTED_IN_PHASE_3",
                limitations=("Native ADF data flow compiler not certified in Phase 3.",),
                certification_commit=certification_commit,
            )
