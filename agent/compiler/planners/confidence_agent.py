from __future__ import annotations

from typing import List
from agent.compiler.base.agent import BaseAgent
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType, LogicalStep


class ConfidenceAgent(BaseAgent):
    """Assigns deterministic confidence scores and calculates human-approval requirements."""

    # Deterministic confidence baselines (0.0 to 1.0)
    ACTION_CONFIDENCE_BASE: dict[ActionType, float] = {
        ActionType.TRIM: 0.99,
        ActionType.LOWERCASE: 0.98,
        ActionType.UPPERCASE: 0.98,
        ActionType.CAST_TYPE: 0.95,
        ActionType.ZERO_TO_NULL: 0.92,
        ActionType.REPLACE_SENTINELS: 0.90,
        ActionType.PARSE_DATES: 0.88,
        ActionType.SANITIZE_EMAIL: 0.88,
        ActionType.NORMALIZE_PHONE: 0.85,
        ActionType.STANDARDIZE_BOOLEAN: 0.92,
        ActionType.RANGE_CLIP: 0.82,
        ActionType.CAP_OUTLIERS: 0.80,
        ActionType.FILL_NULLS: 0.78,
        ActionType.MASK_PHONE: 0.95,
        ActionType.DEDUPLICATE: 0.75,
        ActionType.VALIDATE_REFERENTIAL_INTEGRITY: 0.85,
        ActionType.DROP_COLUMN: 0.60,
        ActionType.CUSTOM_EXPRESSION: 0.65,
    }

    def score_step(self, step: LogicalStep, context: CompilerContext) -> LogicalStep:
        base_confidence = self.ACTION_CONFIDENCE_BASE.get(step.action, 0.70)
        
        # Adjust confidence based on rules or invariants
        never_drop = context.options.get("never_drop_rows", False)
        if never_drop and step.action == ActionType.DEDUPLICATE:
            # If never drop rows is true, deduplication requires quarantine or explicit review
            base_confidence = min(base_confidence, 0.68)
            
        step.confidence = round(base_confidence, 2)
        
        # Governance tiers:
        # >= 0.90: Auto-Apply
        # 0.70 - 0.89: Recommend
        # < 0.70: Requires User Approval
        if step.confidence < 0.70 or step.action == ActionType.DROP_COLUMN:
            step.requires_user_approval = True
        else:
            step.requires_user_approval = False

        return step

    def execute(self, context: CompilerContext) -> CompilerContext:
        if not context.logical_plan:
            return context

        scored_steps: List[LogicalStep] = []
        for step in context.logical_plan.steps:
            scored = self.score_step(step, context)
            scored_steps.append(scored)

        context.logical_plan.steps = scored_steps
        return context
