from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional

from agent.compiler.governance.models import DecisionOutcome, ExecutionDecision
from agent.compiler.models.actions import StructuredAction
from agent.compiler.models.execution import ExecutionMode, FailureBehavior, TransformationPolicy
from agent.compiler.models.logical import ActionType, LogicalStep
from agent.compiler.policies.cleaning_policy import CleaningPolicy
from agent.compiler.policies.risk_classifier import RiskClassifier
from agent.compiler.policies.row_disposition import RiskLevel, RowDisposition
from agent.compiler.safety.models import SafetyStatus, StepSafetyMetrics

logger = logging.getLogger("agent.compiler.governance.authorizer")


class RuntimeAuthorizer:
    """
    Authorizes ETL execution decisions based on risk classification, safety metrics,
    transformation policies, and execution mode (interactive vs unattended batch).
    Answers: 'Is Agent Dhara authorized to execute, quarantine, or block this transformation?'
    """

    @classmethod
    def authorize(
        cls,
        step: LogicalStep | StructuredAction,
        policy: Optional[TransformationPolicy] = None,
        safety_metrics: Optional[StepSafetyMetrics] = None,
        execution_mode: ExecutionMode = ExecutionMode.INTERACTIVE,
        user_approved: Optional[bool] = None,
        user_identity: Optional[str] = None,
        ambiguity_detected: bool = False,
        total_input_rows: int = 0,
        dropped_rows: int = 0,
        merged_rows: int = 0,
    ) -> ExecutionDecision:
        # Extract common fields
        if isinstance(step, StructuredAction):
            step_id = (step.metadata or {}).get("step_id") or f"action_{step.action_type.value}"
            dataset_name = step.dataset_name
            action_type = step.action_type
            target_columns = step.target_columns
            provenance = {
                "source": step.source.value if step.source else "unknown",
                "source_reference": step.source_reference,
                "extraction_method": step.extraction_method,
            }
        else:
            step_id = step.step_id
            dataset_name = step.dataset_name
            action_type = step.action
            target_columns = step.target_columns
            provenance = {
                "source": step.source or "unknown",
                "source_reference": step.source_reference,
                "extraction_method": step.extraction_method,
            }

        # Resolve policy
        if not policy:
            policy = TransformationPolicy(
                action_type=action_type,
                execution_mode=execution_mode,
            )

        # Risk classification
        risk = RiskClassifier.classify_action(action_type.value, policy.row_disposition)
        if policy.risk_level != RiskLevel.LOW:
            risk = policy.risk_level

        failed_rows = safety_metrics.failed_rows if safety_metrics else 0
        input_rows = total_input_rows or (safety_metrics.input_rows if safety_metrics else 0)

        # 1. Circuit Breaker Precedence
        # If Phase 4 triggered a BLOCK, Phase 5 CANNOT override it!
        if safety_metrics and safety_metrics.status == SafetyStatus.BLOCKED:
            return ExecutionDecision(
                decision=DecisionOutcome.BLOCKED,
                reason="Circuit breaker triggered; execution blocked by safety limits.",
                risk_level=risk,
                requires_user_approval=True,
                affected_rows=failed_rows,
                clean_rows=0,
                quarantined_rows=0,
                dropped_rows=0,
                merged_rows=0,
                decision_source="circuit_breaker",
                step_id=step_id,
                dataset_name=dataset_name,
                action_type=action_type,
                target_columns=target_columns,
                safety_metrics=safety_metrics.model_dump() if safety_metrics else {},
                provenance=provenance,
            )

        # 2. Ambiguity & Unidentifiable Rows Check
        # If violation is ambiguous or affected rows cannot be identified, cannot pretend to quarantine
        if ambiguity_detected:
            return ExecutionDecision(
                decision=DecisionOutcome.BLOCKED,
                reason="Ambiguous violation with unidentifiable affected rows cannot be quarantined.",
                risk_level=RiskLevel.HIGH,
                requires_user_approval=True,
                affected_rows=input_rows,
                clean_rows=0,
                quarantined_rows=0,
                dropped_rows=0,
                merged_rows=0,
                decision_source="policy",
                step_id=step_id,
                dataset_name=dataset_name,
                action_type=action_type,
                target_columns=target_columns,
                provenance=provenance,
            )

        # 3. Interactive Mode Decision Path
        if execution_mode == ExecutionMode.INTERACTIVE:
            # Low-risk transforms without explicit approval requirement are auto-approved
            if risk == RiskLevel.LOW and not policy.requires_approval:
                clean_count = max(0, input_rows - failed_rows - dropped_rows - merged_rows)
                return ExecutionDecision(
                    decision=DecisionOutcome.APPROVED,
                    reason="Known deterministic transformation auto-approved under interactive policy.",
                    risk_level=risk,
                    requires_user_approval=False,
                    affected_rows=input_rows,
                    clean_rows=clean_count,
                    quarantined_rows=0,
                    dropped_rows=dropped_rows,
                    merged_rows=merged_rows,
                    decision_source="policy",
                    step_id=step_id,
                    dataset_name=dataset_name,
                    action_type=action_type,
                    target_columns=target_columns,
                    safety_metrics=safety_metrics.model_dump() if safety_metrics else {},
                    provenance=provenance,
                )

            # Medium or High risk requires explicit user review
            if user_approved is True:
                clean_count = max(0, input_rows - failed_rows - dropped_rows - merged_rows)
                return ExecutionDecision(
                    decision=DecisionOutcome.APPROVED,
                    reason=f"Approved by user '{user_identity or 'interactive_user'}'.",
                    risk_level=risk,
                    requires_user_approval=True,
                    affected_rows=input_rows,
                    clean_rows=clean_count,
                    quarantined_rows=0,
                    dropped_rows=dropped_rows,
                    merged_rows=merged_rows,
                    decision_source="interactive_user",
                    step_id=step_id,
                    dataset_name=dataset_name,
                    action_type=action_type,
                    target_columns=target_columns,
                    approval_metadata={
                        "approver": user_identity or "interactive_user",
                        "approved_at": datetime.now(timezone.utc).isoformat(),
                    },
                    safety_metrics=safety_metrics.model_dump() if safety_metrics else {},
                    provenance=provenance,
                )
            elif user_approved is False:
                return ExecutionDecision(
                    decision=DecisionOutcome.BLOCKED,
                    reason=f"Rejected by user '{user_identity or 'interactive_user'}'.",
                    risk_level=risk,
                    requires_user_approval=True,
                    affected_rows=input_rows,
                    clean_rows=0,
                    quarantined_rows=0,
                    dropped_rows=0,
                    merged_rows=0,
                    decision_source="interactive_user",
                    step_id=step_id,
                    dataset_name=dataset_name,
                    action_type=action_type,
                    target_columns=target_columns,
                    approval_metadata={
                        "rejector": user_identity or "interactive_user",
                        "rejected_at": datetime.now(timezone.utc).isoformat(),
                    },
                    safety_metrics=safety_metrics.model_dump() if safety_metrics else {},
                    provenance=provenance,
                )
            else:
                return ExecutionDecision(
                    decision=DecisionOutcome.APPROVAL_REQUIRED,
                    reason=f"Action with risk level '{risk.value}' requires explicit user approval before execution.",
                    risk_level=risk,
                    requires_user_approval=True,
                    affected_rows=input_rows,
                    clean_rows=0,
                    quarantined_rows=0,
                    dropped_rows=dropped_rows,
                    merged_rows=merged_rows,
                    decision_source="policy",
                    step_id=step_id,
                    dataset_name=dataset_name,
                    action_type=action_type,
                    target_columns=target_columns,
                    safety_metrics=safety_metrics.model_dump() if safety_metrics else {},
                    provenance=provenance,
                )

        # 4. Batch Mode Decision Path (Unattended)
        else:
            # High-risk transformations are strictly blocked in unattended batch mode
            if risk == RiskLevel.HIGH:
                return ExecutionDecision(
                    decision=DecisionOutcome.BLOCKED,
                    reason="High-risk transformation blocked in unattended batch mode.",
                    risk_level=RiskLevel.HIGH,
                    requires_user_approval=True,
                    affected_rows=input_rows,
                    clean_rows=0,
                    quarantined_rows=0,
                    dropped_rows=0,
                    merged_rows=0,
                    decision_source="policy",
                    step_id=step_id,
                    dataset_name=dataset_name,
                    action_type=action_type,
                    target_columns=target_columns,
                    safety_metrics=safety_metrics.model_dump() if safety_metrics else {},
                    provenance=provenance,
                )

            # Check failures from safety metrics
            if failed_rows > 0:
                if policy.on_failure == FailureBehavior.QUARANTINE_ROW:
                    clean_count = max(0, input_rows - failed_rows - dropped_rows - merged_rows)
                    return ExecutionDecision(
                        decision=DecisionOutcome.APPROVED_WITH_QUARANTINE,
                        reason=f"Identifiable bad rows ({failed_rows}) diverted to quarantine; {clean_count} clean rows approved.",
                        risk_level=risk,
                        requires_user_approval=False,
                        affected_rows=input_rows,
                        clean_rows=clean_count,
                        quarantined_rows=failed_rows,
                        dropped_rows=dropped_rows,
                        merged_rows=merged_rows,
                        decision_source="policy",
                        step_id=step_id,
                        dataset_name=dataset_name,
                        action_type=action_type,
                        target_columns=target_columns,
                        safety_metrics=safety_metrics.model_dump() if safety_metrics else {},
                        provenance=provenance,
                    )
                elif policy.on_failure == FailureBehavior.SKIP_RULE:
                    return ExecutionDecision(
                        decision=DecisionOutcome.SKIP_RULE,
                        reason="Rule skipped under batch SKIP_RULE policy after encountering conversion anomalies.",
                        risk_level=risk,
                        requires_user_approval=False,
                        affected_rows=input_rows,
                        clean_rows=input_rows,
                        quarantined_rows=0,
                        dropped_rows=0,
                        merged_rows=0,
                        decision_source="policy",
                        step_id=step_id,
                        dataset_name=dataset_name,
                        action_type=action_type,
                        target_columns=target_columns,
                        safety_metrics=safety_metrics.model_dump() if safety_metrics else {},
                        provenance=provenance,
                    )
                elif policy.on_failure == FailureBehavior.BLOCK:
                    return ExecutionDecision(
                        decision=DecisionOutcome.BLOCKED,
                        reason=f"Encountered {failed_rows} failed rows under batch BLOCK policy.",
                        risk_level=risk,
                        requires_user_approval=True,
                        affected_rows=input_rows,
                        clean_rows=0,
                        quarantined_rows=0,
                        dropped_rows=0,
                        merged_rows=0,
                        decision_source="policy",
                        step_id=step_id,
                        dataset_name=dataset_name,
                        action_type=action_type,
                        target_columns=target_columns,
                        safety_metrics=safety_metrics.model_dump() if safety_metrics else {},
                        provenance=provenance,
                    )
                elif policy.on_failure == FailureBehavior.WARN:
                    clean_count = max(0, input_rows - failed_rows - dropped_rows - merged_rows)
                    return ExecutionDecision(
                        decision=DecisionOutcome.APPROVED,
                        reason=f"Approved with warning under batch WARN policy ({failed_rows} failures logged).",
                        risk_level=risk,
                        requires_user_approval=False,
                        affected_rows=input_rows,
                        clean_rows=clean_count,
                        quarantined_rows=0,
                        dropped_rows=dropped_rows,
                        merged_rows=merged_rows,
                        decision_source="policy",
                        step_id=step_id,
                        dataset_name=dataset_name,
                        action_type=action_type,
                        target_columns=target_columns,
                        safety_metrics=safety_metrics.model_dump() if safety_metrics else {},
                        provenance=provenance,
                    )

            # No failures encountered in low/medium risk
            clean_count = max(0, input_rows - dropped_rows - merged_rows)
            return ExecutionDecision(
                decision=DecisionOutcome.APPROVED,
                reason="Known deterministic transformation auto-approved in batch mode.",
                risk_level=risk,
                requires_user_approval=False,
                affected_rows=input_rows,
                clean_rows=clean_count,
                quarantined_rows=0,
                dropped_rows=dropped_rows,
                merged_rows=merged_rows,
                decision_source="policy",
                step_id=step_id,
                dataset_name=dataset_name,
                action_type=action_type,
                target_columns=target_columns,
                safety_metrics=safety_metrics.model_dump() if safety_metrics else {},
                provenance=provenance,
            )
