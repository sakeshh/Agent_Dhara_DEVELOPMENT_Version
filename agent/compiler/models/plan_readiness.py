from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


class ReadinessState(str, Enum):
    READY_TO_CONFIRM = "READY_TO_CONFIRM"
    MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
    UNSUPPORTED_BLOCKING = "UNSUPPORTED_BLOCKING"
    INVALID_PLAN = "INVALID_PLAN"
    MODEL_UNAVAILABLE = "MODEL_UNAVAILABLE"


class TerminalResolutionStatus(str, Enum):
    COMPILED_ACTION_READY = "COMPILED_ACTION_READY"
    MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
    UNSUPPORTED = "UNSUPPORTED"
    WAIVED = "WAIVED"
    INFORMATIONAL = "INFORMATIONAL"
    INVALID = "INVALID"
    MODEL_UNAVAILABLE = "MODEL_UNAVAILABLE"


BLOCKING_TERMINAL_STATUSES: Set[TerminalResolutionStatus] = {
    TerminalResolutionStatus.MANUAL_REVIEW_REQUIRED,
    TerminalResolutionStatus.APPROVAL_REQUIRED,
    TerminalResolutionStatus.UNSUPPORTED,
    TerminalResolutionStatus.INVALID,
    TerminalResolutionStatus.MODEL_UNAVAILABLE,
}


@dataclass(frozen=True)
class BlockingIssue:
    issue_id: str
    canonical_issue_id: str
    dataset: str
    column: Optional[str]
    severity: str
    category: str
    resolution_status: str
    reason: str
    requires_approval: bool
    source_issue_ids: Tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "issue_id": self.issue_id,
            "canonical_issue_id": self.canonical_issue_id,
            "dataset": self.dataset,
            "column": self.column,
            "severity": self.severity,
            "category": self.category,
            "resolution_status": self.resolution_status,
            "reason": self.reason,
            "requires_approval": self.requires_approval,
            "source_issue_ids": list(self.source_issue_ids),
        }


@dataclass(frozen=True)
class PlanReadinessResult:
    ready_for_confirmation: bool
    readiness_state: ReadinessState

    assessment_issue_count: int
    canonical_issue_count: int
    resolved_issue_count: int

    unresolved_blocking_count: int
    blocking_manual_review_count: int
    approval_pending_blocking_count: int
    unsupported_blocking_count: int
    blocking_issue_count: int

    blocking_issues: Tuple[BlockingIssue, ...] = field(default_factory=tuple)
    warnings: Tuple[Dict[str, Any], ...] = field(default_factory=tuple)
    canonical_manifest: Optional[Dict[str, Any]] = None

    manifest_hash: Optional[str] = None
    schema_fingerprint: Optional[str] = None
    policy_version: str = "tier3-safety-v1"
    compiler_version: str = "2.0.2"

    def __post_init__(self):
        # Strict invariant verification:
        expected_blocking = (
            self.unresolved_blocking_count
            + self.blocking_manual_review_count
            + self.approval_pending_blocking_count
            + self.unsupported_blocking_count
        )
        if self.blocking_issue_count != expected_blocking:
            raise ValueError(
                f"Readiness invariant violated: blocking_issue_count ({self.blocking_issue_count}) "
                f"!= sum of mutually exclusive buckets ({expected_blocking})"
            )
        if self.ready_for_confirmation and self.blocking_issue_count > 0:
            raise ValueError(
                f"Readiness invariant violated: ready_for_confirmation is True but "
                f"blocking_issue_count is {self.blocking_issue_count}"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ready_for_confirmation": self.ready_for_confirmation,
            "readiness_state": self.readiness_state.value,
            "assessment_issue_count": self.assessment_issue_count,
            "canonical_issue_count": self.canonical_issue_count,
            "resolved_issue_count": self.resolved_issue_count,
            "unresolved_blocking_count": self.unresolved_blocking_count,
            "blocking_manual_review_count": self.blocking_manual_review_count,
            "approval_pending_blocking_count": self.approval_pending_blocking_count,
            "unsupported_blocking_count": self.unsupported_blocking_count,
            "blocking_issue_count": self.blocking_issue_count,
            "blocking_issues": [b.to_dict() for b in self.blocking_issues],
            "warnings": list(self.warnings),
            "canonical_manifest": self.canonical_manifest,
            "manifest_hash": self.manifest_hash,
            "schema_fingerprint": self.schema_fingerprint,
            "policy_version": self.policy_version,
            "compiler_version": self.compiler_version,
        }
