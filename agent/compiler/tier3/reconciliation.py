from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set


class ResolutionBucket(str, Enum):
    TIER1_COMPILED = "TIER1_COMPILED"
    TIER2_COMPILED = "TIER2_COMPILED"
    TIER3_VALIDATION_FAILED = "TIER3_VALIDATION_FAILED"
    TIER3_APPROVAL_REQUIRED = "TIER3_APPROVAL_REQUIRED"
    TIER3_APPROVAL_REJECTED = "TIER3_APPROVAL_REJECTED"
    TIER3_APPROVED = "TIER3_APPROVED"
    TIER3_COMPILED = "TIER3_COMPILED"
    MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
    WAIVED = "WAIVED"
    UNSUPPORTED = "UNSUPPORTED"
    MODEL_UNAVAILABLE = "MODEL_UNAVAILABLE"


@dataclass
class IssueReconciliationTracker:
    """
    Maintains strict mathematical issue accounting across compilation tiers.
    Guarantees:
      - Every discovered issue is tracked in exactly one current resolution bucket.
      - Lifecycle progression (e.g. TIER3_APPROVED -> TIER3_COMPILED) updates state without duplicating issues.
      - Distinguishes terminal outcome buckets from pending/lifecycle states.
      - Invariant: total_discovered == sum(issue_count_by_bucket.values()).
    """
    TERMINAL_BUCKETS: Set[ResolutionBucket] = field(default_factory=lambda: {
        ResolutionBucket.TIER1_COMPILED,
        ResolutionBucket.TIER2_COMPILED,
        ResolutionBucket.TIER3_COMPILED,
        ResolutionBucket.MANUAL_REVIEW_REQUIRED,
        ResolutionBucket.WAIVED,
        ResolutionBucket.UNSUPPORTED,
        ResolutionBucket.MODEL_UNAVAILABLE,
    })

    PENDING_BUCKETS: Set[ResolutionBucket] = field(default_factory=lambda: {
        ResolutionBucket.TIER3_VALIDATION_FAILED,
        ResolutionBucket.TIER3_APPROVAL_REQUIRED,
        ResolutionBucket.TIER3_APPROVAL_REJECTED,
        ResolutionBucket.TIER3_APPROVED,
    })

    _issue_states: Dict[str, ResolutionBucket] = field(default_factory=dict)
    _issue_history: Dict[str, List[ResolutionBucket]] = field(default_factory=dict)

    def record_issue(self, issue_id: str, bucket: ResolutionBucket) -> None:
        """Records or transitions an issue into a resolution bucket."""
        self._issue_states[issue_id] = bucket
        self._issue_history.setdefault(issue_id, []).append(bucket)

    def get_status(self, issue_id: str) -> Optional[ResolutionBucket]:
        return self._issue_states.get(issue_id)

    def get_history(self, issue_id: str) -> List[ResolutionBucket]:
        return list(self._issue_history.get(issue_id, []))

    @property
    def total_discovered(self) -> int:
        return len(self._issue_states)

    def counts_by_bucket(self) -> Dict[ResolutionBucket, int]:
        counts = {b: 0 for b in ResolutionBucket}
        for b in self._issue_states.values():
            counts[b] += 1
        return counts

    def terminal_counts(self) -> Dict[ResolutionBucket, int]:
        return {b: count for b, count in self.counts_by_bucket().items() if b in self.TERMINAL_BUCKETS}

    def pending_counts(self) -> Dict[ResolutionBucket, int]:
        return {b: count for b, count in self.counts_by_bucket().items() if b in self.PENDING_BUCKETS}

    def verify_accounting_invariants(self) -> bool:
        counts = self.counts_by_bucket()
        summed = sum(counts.values())
        if summed != self.total_discovered:
            raise ValueError(
                f"Reconciliation invariant violated: total_discovered ({self.total_discovered}) != sum of buckets ({summed})"
            )
        return True
