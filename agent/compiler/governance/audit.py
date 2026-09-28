from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional
import uuid

from agent.compiler.governance.models import (
    DecisionOutcome,
    ExecutionDecision,
    GovernanceEvent,
    NotificationType,
)
from agent.compiler.governance.reconciliation import ReconciliationResult

logger = logging.getLogger("agent.compiler.governance.audit")


class NotificationSink:
    """Pluggable notification interface for governance events."""
    def emit(self, event: GovernanceEvent) -> None:
        pass


class LoggingNotificationSink(NotificationSink):
    """Default logger sink that logs governance alerts."""
    def emit(self, event: GovernanceEvent) -> None:
        logger.info(f"[GovernanceEvent:{event.event_type.value}] run_id={event.run_id} dataset={event.dataset} decision={event.decision}")


class GovernanceAuditLog:
    """
    Central governance audit logger and event dispatcher.
    Preserves decision history, safety metrics, provenance, and row reconciliation equations.
    """

    def __init__(self, sinks: Optional[List[NotificationSink]] = None):
        self._decisions: List[ExecutionDecision] = []
        self._events: List[GovernanceEvent] = []
        self._sinks: List[NotificationSink] = sinks or [LoggingNotificationSink()]

    def register_sink(self, sink: NotificationSink) -> None:
        self._sinks.append(sink)

    def record_decision(
        self,
        decision: ExecutionDecision,
        run_id: str,
        execution_mode: str = "interactive",
        reconciliation: Optional[ReconciliationResult] = None,
    ) -> GovernanceEvent:
        self._decisions.append(decision)

        # Map decision to notification event type
        if decision.decision == DecisionOutcome.APPROVAL_REQUIRED:
            n_type = NotificationType.APPROVAL_REQUIRED
        elif decision.decision_source == "circuit_breaker" or decision.decision == DecisionOutcome.BLOCKED:
            n_type = NotificationType.CIRCUIT_BREAKER_TRIGGERED if decision.decision_source == "circuit_breaker" else NotificationType.EXECUTION_BLOCKED
        elif decision.decision == DecisionOutcome.APPROVED_WITH_QUARANTINE:
            n_type = NotificationType.BATCH_COMPLETED_WITH_QUARANTINE
        else:
            n_type = NotificationType.TRANSFORMATION_APPROVED

        event = GovernanceEvent(
            event_id=str(uuid.uuid4()),
            event_type=n_type,
            run_id=run_id,
            dataset=decision.dataset_name or "unknown",
            step_id=decision.step_id or "unknown",
            action=decision.action_type.value if decision.action_type else "unknown",
            risk=decision.risk_level.value,
            execution_mode=execution_mode,
            decision=decision.decision.value,
            input_rows=decision.affected_rows,
            clean_rows=decision.clean_rows,
            failed_rows=decision.quarantined_rows,
            quarantined_rows=decision.quarantined_rows,
            dropped_rows=decision.dropped_rows,
            merged_rows=decision.merged_rows,
            reconciled=reconciliation.is_reconciled if reconciliation else True,
            payload={
                "reason": decision.reason,
                "decision_source": decision.decision_source,
                "safety_metrics": decision.safety_metrics,
                "provenance": decision.provenance,
                "approval_metadata": decision.approval_metadata,
                "reconciliation_equation": reconciliation.equation if reconciliation else "",
            },
        )

        self._events.append(event)
        for sink in self._sinks:
            try:
                sink.emit(event)
            except Exception as e:
                logger.error(f"Error dispatching governance event to sink: {e}")

        return event

    def get_decisions(self) -> List[ExecutionDecision]:
        return list(self._decisions)

    def get_events(self) -> List[GovernanceEvent]:
        return list(self._events)
