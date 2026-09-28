from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
import uuid
from pydantic import BaseModel, Field

from agent.compiler.models.actions import ActionSource, StructuredAction
from agent.compiler.models.logical import ActionType
from agent.compiler.models.profile import DatasetProfile


class RuleLifecycleState(str, Enum):
    """
    Five-stage lifecycle state for promoted transformation rules.
    Prevents unvalidated knowledge contamination across datasets.
    """
    STAGED = "staged"                    # Candidate rule: testing / dry-run only. Cannot auto-execute in production.
    DATASET_SCOPED = "dataset_scoped"    # Governed rule approved for a specific dataset and schema context.
    GLOBAL_PROMOTED = "global_promoted"  # Proven universal transformation pattern approved across multiple datasets.
    SUSPENDED = "suspended"              # Regressed or contested rule temporarily blocked from execution.
    RETIRED = "retired"                  # Superseded or deprecated rule; cannot be selected.


class SchemaSignature(BaseModel):
    """Compatibility fingerprint for dataset schemas to guard against schema drift."""
    columns: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    def is_compatible(
        self, dataset_profile: Optional[DatasetProfile], target_columns: List[str]
    ) -> tuple[bool, str]:
        if not dataset_profile:
            return True, "No profile provided; skipping signature check"

        for col in target_columns:
            # 1. Missing target column check
            if col not in dataset_profile.columns:
                return False, f"Target column '{col}' is missing in target dataset schema."

            # 2. Type compatibility check
            expected_info = self.columns.get(col)
            if expected_info:
                expected_type = str(expected_info.get("dtype") or "").lower()
                actual_type = dataset_profile.columns[col].dtype.lower()

                # Basic family checks
                if "int" in expected_type and "int" not in actual_type:
                    return False, f"Column '{col}' datatype drifted: expected '{expected_type}', found '{actual_type}'."
                if any(s in expected_type for s in ("char", "string")) and not any(s in actual_type for s in ("char", "string")):
                    return False, f"Column '{col}' datatype drifted: expected '{expected_type}', found '{actual_type}'."
                if "date" in expected_type and "date" not in actual_type:
                    return False, f"Column '{col}' datatype drifted: expected '{expected_type}', found '{actual_type}'."

        return True, "Schema signature is compatible"


class PromotedRule(BaseModel):
    """
    Governed Promoted Rule definition containing transformation semantics,
    schema constraints, and lifecycle state.
    """
    rule_id: str
    version: int = 1
    name: str
    description: Optional[str] = None
    action_type: ActionType
    parameters: Dict[str, Any] = Field(default_factory=dict)
    target_columns: List[str] = Field(default_factory=list)
    dataset_scope: Optional[str] = None  # Specific dataset name, or None if global
    schema_signature: SchemaSignature = Field(default_factory=SchemaSignature)
    status: RuleLifecycleState = RuleLifecycleState.STAGED
    rule_intent_pattern: Optional[str] = None  # Normalized rule text for lookup
    created_by: str = "compiler_learning"
    approved_by: Optional[str] = None
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_structured_action(self) -> StructuredAction:
        return StructuredAction(
            action_type=self.action_type,
            dataset_name=self.dataset_scope or "unknown",
            target_columns=list(self.target_columns),
            parameters=dict(self.parameters),
            source=ActionSource.PROMOTED_RULE,
            source_reference=f"{self.rule_id}_v{self.version}",
            extraction_method="promoted_rule",
            confidence=1.0,
            metadata={
                "rule_id": self.rule_id,
                "version": self.version,
                "lifecycle_status": self.status.value,
            },
        )


class PromotionEvidence(BaseModel):
    """
    Execution evidence record validating why a rule is trusted.
    Prevents naive single-percentage promotions.
    """
    evidence_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    rule_id: str
    rule_version: int
    run_id: str
    dataset: str
    schema_signature: Dict[str, Any] = Field(default_factory=dict)
    success: bool
    rows_processed: int
    rows_failed: int = 0
    rows_quarantined: int = 0
    reconciliation_passed: bool = True
    safety_status: str = "passed"
    governance_decision: str = "approved"
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class ExtensionCandidate(BaseModel):
    """
    Tracks recurring UNSUPPORTED business rules requiring controlled developer extension.
    Never executes automatically without human review.
    """
    candidate_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    original_rule: str
    dataset_name: str
    suggested_action_name: Optional[str] = None
    frequency: int = 1
    status: str = "PENDING_DEVELOPER_REVIEW"
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
