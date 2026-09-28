from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from agent.compiler.models.artifacts import CodeArtifact, CostEstimate
from agent.compiler.models.context import CompilerContext


class DeploymentPackage(BaseModel):
    manifest_version: str = "2.0.0"
    pipeline_id: str
    target_engine: str
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    artifacts: Dict[str, Any] = Field(default_factory=dict)
    validation: Dict[str, Any] = Field(default_factory=dict)
    repair: Dict[str, Any] = Field(default_factory=dict)
    cost_estimate: Optional[CostEstimate] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def to_json(self, indent: int = 2) -> str:
        return self.model_dump_json(indent=indent)


class ArtifactManifestBuilder:
    """Assembles all generated pipeline code, tests, contracts, lineage, and diagnostics into a unified deployment manifest."""

    def build_manifest(self, context: CompilerContext) -> DeploymentPackage:
        pipeline_id = (
            context.logical_plan.plan_id if context.logical_plan else f"pipeline_{context.session_id}"
        )

        # 1. Collect artifacts
        artifacts_bundle: Dict[str, Any] = {}
        if context.code_artifact:
            artifacts_bundle["code"] = {
                "name": context.code_artifact.name,
                "language": context.code_artifact.metadata.get("engine") or context.code_artifact.language,
                "file_extension": context.code_artifact.file_extension,
                "size_bytes": len(context.code_artifact.content),
                "content": context.code_artifact.content,
            }

        companion_test = context.metadata.get("companion_test_artifact")
        if companion_test:
            artifacts_bundle["tests"] = {
                "name": companion_test.get("name"),
                "language": companion_test.get("language"),
                "content": companion_test.get("content"),
            }

        data_contract = context.metadata.get("data_contract_yaml")
        if data_contract:
            artifacts_bundle["contract"] = {
                "format": "yaml",
                "standard": "OpenDataContract-1.0.0",
                "content": data_contract,
            }

        lineage_entries = context.metadata.get("lineage_entries")
        lineage_mermaid = context.metadata.get("lineage_mermaid")
        if lineage_entries or lineage_mermaid:
            artifacts_bundle["lineage"] = {
                "entries": lineage_entries or [],
                "mermaid_dag": lineage_mermaid,
            }

        # 1b. Output Schema Contracts & Separate Output Structures
        output_contracts = context.metadata.get("output_contracts") or {}
        if output_contracts:
            artifacts_bundle["manifest"] = {
                "output_contracts": output_contracts,
                "cleaning_mode": context.cleaning_mode.value,
            }
            artifacts_bundle["clean"] = {
                ds: contract.get("clean_columns", []) for ds, contract in output_contracts.items()
            }
            artifacts_bundle["audit"] = {
                ds: contract.get("audit_columns", []) for ds, contract in output_contracts.items()
            }
            artifacts_bundle["rejected"] = {
                ds: contract.get("rejected_columns", []) for ds, contract in output_contracts.items()
            }

        # 2. Validation status
        validation_info: Dict[str, Any] = {"status": "unvalidated"}
        if context.validation_result:
            validation_info = {
                "status": "passed" if context.validation_result.is_valid else "failed",
                "error_count": context.validation_result.error_count,
                "has_security_hazards": context.validation_result.has_security_hazards,
                "has_performance_anti_patterns": context.validation_result.has_performance_anti_patterns,
                "has_blocking_errors": context.validation_result.has_blocking_errors,
            }

        # 3. Repair analytics
        repair_info = context.metadata.get("repair_analytics", {"attempts": 0, "success": True})

        # 4. Cost estimate
        cost_est_data = context.metadata.get("cost_estimate")
        cost_est = CostEstimate(**cost_est_data) if cost_est_data else None

        meta = {"session_id": context.session_id, "cleaning_mode": context.cleaning_mode.value}
        if "compiler_metrics" in context.metadata:
            meta["compiler_metrics"] = context.metadata["compiler_metrics"]

        return DeploymentPackage(
            pipeline_id=pipeline_id,
            target_engine=context.target_engine.value,
            artifacts=artifacts_bundle,
            validation=validation_info,
            repair=repair_info,
            cost_estimate=cost_est,
            metadata=meta,
        )
