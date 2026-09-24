from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from agent.compiler.models.profile import DatasetProfile
from agent.compiler.models.rules import BusinessRule
from agent.compiler.models.issues import Issue
from agent.compiler.models.logical import LogicalPlan
from agent.compiler.models.physical import PhysicalPlan, ExecutionEngine
from agent.compiler.models.artifacts import CodeArtifact, ArtifactManifest
from agent.compiler.models.validation import ValidationResult


class CompilerContext(BaseModel):
    session_id: str = "default"
    target_engine: ExecutionEngine = ExecutionEngine.PYTHON
    dataset_profiles: Dict[str, DatasetProfile] = Field(default_factory=dict)
    business_rules: List[BusinessRule] = Field(default_factory=list)
    detected_issues: List[Issue] = Field(default_factory=list)
    logical_plan: Optional[LogicalPlan] = None
    physical_plan: Optional[PhysicalPlan] = None
    code_artifact: Optional[CodeArtifact] = None
    validation_result: Optional[ValidationResult] = None
    artifact_manifest: Optional[ArtifactManifest] = None
    options: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def get_dataset(self, name: str) -> Optional[DatasetProfile]:
        return self.dataset_profiles.get(name)

    def add_issue(self, issue: Issue) -> None:
        self.detected_issues.append(issue)

    def get_rule_for_dataset(self, rule_name: str, dataset_name: str) -> Optional[BusinessRule]:
        for r in self.business_rules:
            if r.name == rule_name and (r.dataset_name is None or r.dataset_name == dataset_name):
                return r
        return None
