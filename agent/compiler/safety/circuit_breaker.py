from __future__ import annotations

import logging
from typing import Dict, List, Optional

from agent.compiler.models.execution import FailureBehavior, TransformationPolicy
from agent.compiler.safety.models import DryRunReport, SafetyStatus, StepSafetyMetrics
from agent.errors import CircuitBreakerError

logger = logging.getLogger("agent.compiler.safety.circuit_breaker")


class CircuitBreaker:
    """
    Evaluates runtime transformation safety metrics against configured TransformationPolicy thresholds.
    Enforces circuit-breaking behaviors: BLOCK, QUARANTINE_ROW, WARN, SKIP_RULE, or CONTINUE.
    """

    @classmethod
    def evaluate_step(
        cls,
        metrics: StepSafetyMetrics,
        policy: TransformationPolicy,
        raise_on_block: bool = True,
    ) -> StepSafetyMetrics:
        threshold_violated = False
        reasons: List[str] = []

        # 1. Absolute failed rows check
        if policy.max_failed_rows is not None and metrics.failed_rows > policy.max_failed_rows:
            threshold_violated = True
            reasons.append(
                f"Failed rows ({metrics.failed_rows}) exceeded absolute threshold ({policy.max_failed_rows})"
            )

        # 2. Percentage threshold check
        # (Only evaluate if policy specifies a non-trivial or 0.0 strict threshold with non-zero failed rows)
        if metrics.input_rows > 0 and metrics.failed_rows > 0:
            if metrics.failure_percentage > policy.max_failure_percentage:
                threshold_violated = True
                reasons.append(
                    f"Failure percentage ({metrics.failure_percentage:.2f}%) exceeded allowed threshold ({policy.max_failure_percentage:.2f}%)"
                )

        if threshold_violated:
            metrics.violations.extend(reasons)
            msg = f"Circuit breaker triggered on step '{metrics.step_id}' ({metrics.action_type.value}): {'; '.join(reasons)}"

            if policy.on_failure == FailureBehavior.BLOCK:
                metrics.status = SafetyStatus.BLOCKED
                logger.error(msg)
                if raise_on_block:
                    raise CircuitBreakerError(
                        message=msg,
                        step_id=metrics.step_id,
                        failed_count=metrics.failed_rows,
                        threshold=policy.max_failed_rows if policy.max_failed_rows is not None else policy.max_failure_percentage,
                        action=metrics.action_type.value,
                    )
            elif policy.on_failure == FailureBehavior.QUARANTINE_ROW:
                metrics.status = SafetyStatus.QUARANTINED
                metrics.quarantined_rows = metrics.failed_rows
                logger.warning(f"{msg}. Failed rows marked for quarantine.")
            elif policy.on_failure == FailureBehavior.WARN:
                metrics.status = SafetyStatus.WARNING
                logger.warning(f"{msg}. Proceeding with diagnostic warning.")
            elif policy.on_failure == FailureBehavior.SKIP_RULE:
                metrics.status = SafetyStatus.SKIPPED
                logger.warning(f"{msg}. Rule transformation skipped.")
            elif policy.on_failure == FailureBehavior.CONTINUE:
                metrics.status = SafetyStatus.PASSED
                logger.info(f"{msg}. Ignored under CONTINUE policy.")
        else:
            metrics.status = SafetyStatus.PASSED

        return metrics

    @classmethod
    def evaluate_pipeline(
        cls,
        report: DryRunReport,
        policies: Optional[Dict[str, TransformationPolicy]] = None,
        raise_on_block: bool = True,
    ) -> DryRunReport:
        policies = policies or {}
        has_blocking = False

        for step in report.step_metrics:
            policy = policies.get(step.step_id) or TransformationPolicy(
                action_type=step.action_type,
                max_failed_rows=0,
                max_failure_percentage=0.0,
                on_failure=FailureBehavior.BLOCK,
            )
            try:
                cls.evaluate_step(step, policy, raise_on_block=raise_on_block)
            except CircuitBreakerError as e:
                has_blocking = True
                report.circuit_breaker_triggered = True
                report.status = SafetyStatus.BLOCKED
                report.is_safe_to_execute = False
                report.violations.append(str(e))
                if raise_on_block:
                    raise

            if step.status == SafetyStatus.BLOCKED:
                has_blocking = True

        if has_blocking:
            report.status = SafetyStatus.BLOCKED
            report.is_safe_to_execute = False
            report.circuit_breaker_triggered = True
        elif any(s.status == SafetyStatus.QUARANTINED for s in report.step_metrics):
            report.status = SafetyStatus.QUARANTINED
            report.is_safe_to_execute = True
        elif any(s.status == SafetyStatus.WARNING for s in report.step_metrics):
            report.status = SafetyStatus.WARNING
            report.is_safe_to_execute = True
        else:
            report.status = SafetyStatus.PASSED
            report.is_safe_to_execute = True

        return report
