from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class ApprovalDecision(str, Enum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    CHANGES_REQUESTED = "CHANGES_REQUESTED"
    EXPIRED = "EXPIRED"
    REVOKED = "REVOKED"


@dataclass(frozen=True)
class ApprovalRecord:
    """
    Cryptographically and semantically binds a steward decision to an immutable Tier 3 proposal fingerprint.
    """
    approval_id: str
    proposal_id: str
    proposal_fingerprint: str
    decision: ApprovalDecision
    approver_id: str
    approver_role: str
    decided_at: datetime
    reason: str
    policy_version: str = "1.0"
    schema_fingerprint: Optional[str] = None
    rule_version: Optional[str] = None
    target_engine: Optional[str] = "AZURE_SQL"
    approval_source: str = "HUMAN"  # "HUMAN" | "POLICY_ENGINE"
    is_revoked: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)
