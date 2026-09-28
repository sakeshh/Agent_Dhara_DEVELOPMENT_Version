from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from agent.compiler.models.logical import ActionType
from agent.compiler.policies.row_disposition import RiskLevel


class DecisionOutcome(str, Enum):
    APPROVED = "approved"
    APPROVAL_REQUIRED = "approval_required"
    APPROVED_WITH_QUARANTINE = "approved_with_quarantine"
    SKIP_RULE = "skip_rule"
    BLOCKED = "blocked"


class NotificationType(str, Enum):
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
    BATCH_COMPLETED_WITH_QUARANTINE = "BATCH_COMPLETED_WITH_QUARANTINE"
    EXECUTION_BLOCKED = "EXECUTION_BLOCKED"
    CIRCUIT_BREAKER_TRIGGERED = "CIRCUIT_BREAKER_TRIGGERED"
    TRANSFORMATION_APPROVED = "TRANSFORMATION_APPROVED"


class ExecutionDecision(BaseModel):
    """
    Explicit, auditable governance decision for an ETL transformation step.
    Answers: 'Why did Agent Dhara execute, quarantine, or block this transformation?'
    """
    decision: DecisionOutcome
    reason: str
    risk_level: RiskLevel
    requires_user_approval: bool
    affected_rows: int = 0
    clean_rows: int = 0
    quarantined_rows: int = 0
    dropped_rows: int = 0
    merged_rows: int = 0
    policy_reference: Optional[str] = None
    decision_source: str = "policy"  # "policy" | "interactive_user" | "circuit_breaker"
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    step_id: Optional[str] = None
    dataset_name: Optional[str] = None
    action_type: Optional[ActionType] = None
    target_columns: List[str] = Field(default_factory=list)
    safety_metrics: Dict[str, Any] = Field(default_factory=dict)
    provenance: Dict[str, Any] = Field(default_factory=dict)
    approval_metadata: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class GovernanceEvent(BaseModel):
    """Structured audit event emitted by the governance layer for observability."""
    event_id: str
    event_type: NotificationType
    run_id: str
    dataset: str
    step_id: str
    action: str
    risk: str
    execution_mode: str
    decision: str
    input_rows: int = 0
    clean_rows: int = 0
    failed_rows: int = 0
    quarantined_rows: int = 0
    dropped_rows: int = 0
    merged_rows: int = 0
    reconciled: bool = True
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    payload: Dict[str, Any] = Field(default_factory=dict)
