from __future__ import annotations

import logging
from typing import Optional

from agent.compiler.base.agent import BaseAgent
from agent.compiler.base.validation import BaseValidationAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.validation import DiagnosticSeverity
from agent.compiler.repair.repair_classifier import RepairClassifier

logger = logging.getLogger("agent.compiler.repair")


class RepairAgent(BaseAgent):
    """Orchestrates closed-loop, multi-turn self-healing code repair."""

    def __init__(self, classifier: Optional[RepairClassifier] = None, max_attempts: int = 3):
        self.classifier = classifier or RepairClassifier()
        self.max_attempts = max_attempts

    def repair(
        self,
        context: CompilerContext,
        validator: BaseValidationAgent,
    ) -> CompilerContext:
        if not context.code_artifact or not context.validation_result:
            return context

        if context.validation_result.is_valid:
            return context

        history = context.metadata.setdefault("repair_history", [])

        for attempt in range(1, self.max_attempts + 1):
            if context.validation_result.is_valid:
                break

            logger.info(f"Self-healing repair attempt {attempt}/{self.max_attempts} for session '{context.session_id}'")
            patched_any = False

            # Find all errors
            errors = [i for i in context.validation_result.issues if i.severity == DiagnosticSeverity.ERROR]

            for error in errors:
                strategy = self.classifier.classify(error)
                if not strategy:
                    continue

                current_code = context.code_artifact.content
                patched_code = strategy.patch(current_code, error, context)

                if patched_code and patched_code != current_code:
                    context.code_artifact.content = patched_code
                    patched_any = True

                    EventBus.get_instance().emit(
                        event_type=CompilerEventType.REPAIR_ATTEMPTED,
                        session_id=context.session_id,
                        payload={
                            "attempt": attempt,
                            "issue_code": error.issue_code,
                            "strategy": strategy.__class__.__name__,
                        },
                        source_agent=self.__class__.__name__,
                    )

                    # Revalidate
                    context.validation_result = validator.validate(context.code_artifact, context)
                    history.append({
                        "attempt": attempt,
                        "strategy": strategy.__class__.__name__,
                        "issue_code": error.issue_code,
                        "now_valid": context.validation_result.is_valid,
                    })

                    if context.validation_result.is_valid:
                        logger.info(f"Self-healing successfully resolved errors on attempt {attempt}")
                        EventBus.get_instance().emit(
                            event_type=CompilerEventType.REPAIR_SUCCEEDED,
                            session_id=context.session_id,
                            payload={"attempt": attempt, "resolved_issue": error.issue_code},
                            source_agent=self.__class__.__name__,
                        )
                        break

            if not patched_any:
                # No strategies could apply a patch
                break

        # Compute repair effectiveness analytics
        total_attempts = len(history)
        is_success = context.validation_result.is_valid if context.validation_result else False
        strategies_used = list({h.get("strategy") for h in history if h.get("strategy")})
        context.metadata["repair_analytics"] = {
            "attempts": total_attempts,
            "success": is_success,
            "strategies_used": strategies_used,
            "dialect": context.target_engine.value,
        }

        return context

    def execute(self, context: CompilerContext) -> CompilerContext:
        # Standalone execute pass-through
        return context
