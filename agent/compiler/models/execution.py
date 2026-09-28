from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from agent.compiler.models.logical import ActionType
from agent.compiler.policies.row_disposition import RiskLevel, RowDisposition


class ExecutionMode(str, Enum):
    """Execution context defining how non-trivial or novel rules are handled."""
    INTERACTIVE = "interactive"  # Pauses for user approval when required
    BATCH = "batch"              # Unattended; applies automated policy rules (quarantine/skip/block)


class FailureBehavior(str, Enum):
    """What the pipeline or operator does when a rule or data row encounters a violation/failure."""
    BLOCK = "block"                    # Abort execution immediately
    WARN = "warn"                      # Log a diagnostic warning and proceed
    SKIP_RULE = "skip_rule"            # Skip this transformation rule entirely
    QUARANTINE_ROW = "quarantine_row"  # Divert offending row to audit/quarantine store
    CONTINUE = "continue"              # Ignore and proceed


class TransformationPolicy(BaseModel):
    """
    Governance policy contract governing how an action behaves in production.
    Separates transformation intent (ActionType) from physical row effect (RowDisposition)
    and operational policy enforcement (FailureBehavior).
    """
    action_type: ActionType
    row_disposition: RowDisposition = RowDisposition.TRANSFORM
    risk_level: RiskLevel = RiskLevel.LOW
    requires_approval: bool = False
    execution_mode: ExecutionMode = ExecutionMode.INTERACTIVE
    on_failure: FailureBehavior = FailureBehavior.QUARANTINE_ROW
    max_failed_rows: Optional[int] = Field(default=None, description="Absolute threshold of failed rows allowed before aborting")
    max_failure_percentage: float = Field(default=0.0, ge=0.0, le=100.0, description="Percentage threshold of failed rows allowed")
    metadata: Dict[str, Any] = Field(default_factory=dict)
