from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class CanonicalResolutionManifest(BaseModel):
    """Immutable, frozen resolution view authorized by /etl/confirm and consumed by /etl/generate."""
    manifest_version: str = "1.0"
    session_id: str
    created_at: str
    assessment_fingerprint: str
    plan_fingerprint: str
    schema_fingerprint: str
    rules_fingerprint: str
    policy_version: str = "tier3-safety-v1"
    compiler_version: str = "2.0.2"
    target_engine: str = "azure_sql"

    canonical_issues: List[Dict[str, Any]] = Field(default_factory=list)
    issue_alias_map: Dict[str, str] = Field(default_factory=dict)
    structured_actions: List[Dict[str, Any]] = Field(default_factory=list)
    approval_references: List[Any] = Field(default_factory=list)
    waiver_references: List[Any] = Field(default_factory=list)
    manual_resolution_references: List[Any] = Field(default_factory=list)
    reconciliation: Dict[str, Any] = Field(default_factory=dict)
    manifest_hash: Optional[str] = None

    def compute_hash(self) -> str:
        """Calculates a deterministic sha256 fingerprint over all frozen manifest contents."""
        core_dict = {
            "manifest_version": self.manifest_version,
            "session_id": self.session_id,
            "assessment_fingerprint": self.assessment_fingerprint,
            "plan_fingerprint": self.plan_fingerprint,
            "schema_fingerprint": self.schema_fingerprint,
            "rules_fingerprint": self.rules_fingerprint,
            "policy_version": self.policy_version,
            "compiler_version": self.compiler_version,
            "target_engine": self.target_engine,
            "canonical_issues": self.canonical_issues,
            "issue_alias_map": self.issue_alias_map,
            "structured_actions": self.structured_actions,
            "approval_references": self.approval_references,
            "waiver_references": self.waiver_references,
            "manual_resolution_references": self.manual_resolution_references,
            "reconciliation": self.reconciliation,
        }
        blob = json.dumps(core_dict, sort_keys=True, default=str)
        return "sha256:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def bind_hash(self) -> str:
        """Computes and assigns manifest_hash."""
        h = self.compute_hash()
        self.manifest_hash = h
        return h

    def verify_hash(self, expected_hash: Optional[str] = None) -> bool:
        computed = self.compute_hash()
        if expected_hash is not None:
            return expected_hash == computed
        return self.manifest_hash == computed
