from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class CostEstimate(BaseModel):
    estimated_runtime_seconds: float = 0.0
    estimated_memory_mb: float = 0.0
    estimated_cloud_cost_usd: float = 0.0
    notes: Optional[str] = None


class CodeArtifact(BaseModel):
    artifact_id: str
    name: str
    language: str
    content: str
    file_extension: str
    is_entrypoint: bool = True
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ArtifactManifest(BaseModel):
    manifest_version: str = "2.0.0"
    pipeline_id: str
    target_engine: str
    primary_artifact: CodeArtifact
    companion_artifacts: List[CodeArtifact] = Field(default_factory=list)
    data_contract_yaml: Optional[str] = None
    lineage_graph_json: Optional[Dict[str, Any]] = None
    cost_estimate: Optional[CostEstimate] = None
    created_at: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)
