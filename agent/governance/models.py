from __future__ import annotations

from enum import Enum
import time
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ApprovalTier(str, Enum):
    AUTO_APPROVED = "auto_approved"
    WARNING_REQUIRED = "warning_required"
    MANDATORY_HUMAN_APPROVAL = "mandatory_human_approval"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ApprovalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    AUTO_APPROVED = "auto_approved"


class ApprovalDecision(BaseModel):
    """
    Phase 9 Human Approval & Audit Record.
    Enforces enterprise safety gates prior to any destructive operation execution.
    """
    approval_id: str
    session_id: str
    pattern_id: Optional[str] = None
    requestor: str = "engineer"
    approver: Optional[str] = None
    approval_tier: ApprovalTier = ApprovalTier.AUTO_APPROVED
    risk_level: RiskLevel = RiskLevel.LOW
    status: ApprovalStatus = ApprovalStatus.AUTO_APPROVED
    reasons: List[str] = Field(default_factory=list)
    destructive_actions: List[str] = Field(default_factory=list)
    code_summary: str = ""
    timestamp: float = Field(default_factory=time.time)
    decided_at: Optional[float] = None
    audit_log: List[str] = Field(default_factory=list)
