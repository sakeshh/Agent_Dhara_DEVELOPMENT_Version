"""
Dhara Unified Recovery Framework & State Transitions Contract.
Enforces the product-wide invariant:
"Every non-terminal Agent Dhara failure must have at least one explicit recovery transition.
A failed correction attempt never corrupts or replaces the last approved plan."
"""

from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional
import uuid


@dataclass
class RecoveryTransition:
    transition_id: str          # e.g., 'EDIT', 'REBASE', 'REVALIDATE', 'QUARANTINE_AFFECTED', 'APPROVE', 'RETRY_CHECKPOINT'
    label: str                  # Button label, e.g. "Edit Resolution", "Rebase Plan", "Quarantine Rows"
    is_recommended: bool        # True for recommended default path
    explanation: str            # What this recovery action will do
    target_endpoint: Optional[str] = None # API endpoint if directly invokable
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecoveryEnvelope:
    status: str                         # e.g., 'REQUIRES_CORRECTION', 'STALE_PLAN_VERSION', 'SCHEMA_DRIFT', 'CIRCUIT_BREAKER_TRIGGERED'
    lifecycle_state: str                # e.g., 'CORRECTION_REQUIRED', 'REBASE_REQUIRED', 'REVALIDATION_REQUIRED'
    recoverable: bool                   # True for all non-terminal states
    what_happened: str                  # Plain-English diagnosis of what went wrong
    what_protected: str                 # Explicit statement of what invariant Dhara protected
    what_needs_attention: str           # Scoped summary of items requiring human input
    available_transitions: List[RecoveryTransition]
    afterward_explanation: str          # What happens after the recovery transition is executed
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "lifecycle_state": self.lifecycle_state,
            "recoverable": self.recoverable,
            "what_happened": self.what_happened,
            "what_protected": self.what_protected,
            "what_needs_attention": self.what_needs_attention,
            "available_transitions": [asdict(t) for t in self.available_transitions],
            "afterward_explanation": self.afterward_explanation,
            "details": self.details,
        }


def build_stale_plan_recovery_envelope(
    current_plan_version: int,
    expected_plan_version: int,
    details: Optional[Dict[str, Any]] = None
) -> RecoveryEnvelope:
    """Surfaces concurrency conflict with safe rebase transition."""
    return RecoveryEnvelope(
        status="STALE_PLAN_VERSION",
        lifecycle_state="REBASE_REQUIRED",
        recoverable=True,
        what_happened=f"Plan was updated to version {current_plan_version} by another process while you were editing version {expected_plan_version}.",
        what_protected=f"Protected Plan v{current_plan_version} from being overwritten by stale changes from v{expected_plan_version}.",
        what_needs_attention=f"Plan version out of sync (current: v{current_plan_version}, your base: v{expected_plan_version}).",
        available_transitions=[
            RecoveryTransition(
                transition_id="REBASE",
                label="Refresh and Rebase Plan",
                is_recommended=True,
                explanation="Fetches latest Plan v" + str(current_plan_version) + " and re-applies non-conflicting selections.",
                target_endpoint="/etl/plan",
            ),
            RecoveryTransition(
                transition_id="DISCARD_CANDIDATE",
                label="Discard Stale Draft",
                is_recommended=False,
                explanation="Abandons candidate draft and aligns directly with the active approved plan.",
            ),
        ],
        afterward_explanation="Your review workspace will be updated with the latest approved plan state without data corruption.",
        details=details or {"current_plan_version": current_plan_version, "expected_plan_version": expected_plan_version}
    )


def build_schema_drift_recovery_envelope(
    affected_datasets: List[str],
    current_hash: str,
    original_hash: str,
    details: Optional[Dict[str, Any]] = None
) -> RecoveryEnvelope:
    """Surfaces database schema changes after planning with safe revalidation."""
    ds_str = ", ".join(affected_datasets) if affected_datasets else "datasets"
    return RecoveryEnvelope(
        status="SCHEMA_DRIFT",
        lifecycle_state="REVALIDATION_REQUIRED",
        recoverable=True,
        what_happened=f"Underlying schema fingerprint changed for {ds_str} after planning was completed.",
        what_protected="Blocked code generation against outdated column structures to prevent runtime execution failures.",
        what_needs_attention=f"Schema changes detected across {len(affected_datasets) or 1} dataset(s). Planned steps must be validated against the new schema.",
        available_transitions=[
            RecoveryTransition(
                transition_id="REVALIDATE",
                label="Revalidate Plan Against New Schema",
                is_recommended=True,
                explanation="Re-evaluates planned operations against updated database schema and identifies any affected steps.",
                target_endpoint="/etl/revalidate-plan",
            ),
            RecoveryTransition(
                transition_id="REBUILD_PLAN",
                label="Regenerate Plan",
                is_recommended=False,
                explanation="Re-runs assessment-driven planning on the updated schema from scratch.",
                target_endpoint="/etl/plan",
            )
        ],
        afterward_explanation="Compatible transformations will be preserved; only columns affected by schema changes will require review.",
        details=details or {"affected_datasets": affected_datasets, "current_hash": current_hash, "original_hash": original_hash}
    )


def build_circuit_breaker_recovery_envelope(
    error_rate: float,
    threshold: float,
    dataset: str,
    column: Optional[str] = None,
    failure_sample: Optional[List[Any]] = None,
    details: Optional[Dict[str, Any]] = None
) -> RecoveryEnvelope:
    """Surfaces runtime conversion or execution failures with quarantine or policy overrides."""
    target = f"{dataset}.{column}" if column else dataset
    return RecoveryEnvelope(
        status="CIRCUIT_BREAKER_TRIGGERED",
        lifecycle_state="SAFE_CHECKPOINT_RECOVERY",
        recoverable=True,
        what_happened=f"Execution dry-run encountered {error_rate*100:.1f}% invalid values on {target}, exceeding the safety threshold of {threshold*100:.1f}%.",
        what_protected="Safety circuit breaker halted execution before committing corrupt data to destination tables.",
        what_needs_attention=f"{target} has unconvertible values in dry-run batch.",
        available_transitions=[
            RecoveryTransition(
                transition_id="QUARANTINE_AFFECTED_ROWS",
                label="Quarantine Affected Rows",
                is_recommended=True,
                explanation="Routes failing rows to a quarantine dead-letter table and proceeds with processing clean rows.",
            ),
            RecoveryTransition(
                transition_id="BLOCK_PIPELINE",
                label="Block Execution",
                is_recommended=False,
                explanation="Halts pipeline execution and returns control to data engineering for source remediation.",
            ),
            RecoveryTransition(
                transition_id="ALLOW_WITH_WARNING",
                label="Accept Warning & Proceed",
                is_recommended=False,
                explanation="Overrides safety threshold if business accepts the conversion data loss rate.",
            ),
        ],
        afterward_explanation="Pipeline resumes from the last safe checkpoint under the selected governance policy.",
        details=details or {"error_rate": error_rate, "threshold": threshold, "dataset": dataset, "column": column, "sample": failure_sample}
    )
