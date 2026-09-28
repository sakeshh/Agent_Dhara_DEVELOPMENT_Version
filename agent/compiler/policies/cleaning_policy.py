from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, Field
from agent.compiler.policies.row_disposition import RiskLevel, RowDisposition
from agent.compiler.policies.risk_classifier import RiskClassifier


class StepPolicyDecision(BaseModel):
    action: str
    row_disposition: RowDisposition
    risk_level: RiskLevel
    requires_approval: bool
    is_auto_applicable: bool
    reasons: List[str] = Field(default_factory=list)


class CleaningPolicy:
    """
    Enforces that retain_all never overrides remediation.
    Every logical step declares its physical effect on data rows.
    """

    @staticmethod
    def evaluate_step(
        action: str,
        row_disposition: RowDisposition,
        is_user_approved: bool = False,
    ) -> StepPolicyDecision:
        risk = RiskClassifier.classify_action(action, row_disposition)
        reasons = []

        if risk == RiskLevel.HIGH and not is_user_approved:
            requires_approval = True
            is_auto = False
            reasons.append(f"High-risk action '{action}' with disposition '{row_disposition.value}' requires explicit user approval.")
        elif risk == RiskLevel.MEDIUM and not is_user_approved:
            requires_approval = True
            is_auto = False
            reasons.append(f"Medium-risk action '{action}' flagged for review.")
        else:
            requires_approval = False
            is_auto = True
            reasons.append(f"Action '{action}' is eligible for execution under current policy.")

        return StepPolicyDecision(
            action=action,
            row_disposition=row_disposition,
            risk_level=risk,
            requires_approval=requires_approval,
            is_auto_applicable=is_auto,
            reasons=reasons,
        )
