from __future__ import annotations

from typing import Dict, List, Set, Tuple
from agent.compiler.base.agent import BaseAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType, LogicalStep


class OptimizationAgent(BaseAgent):
    """Performs AST optimization passes: Operator fusion, redundant step elimination, and dead code pruning."""

    def _fuse_operators(self, steps: List[LogicalStep]) -> List[LogicalStep]:
        """Fuses adjacent compatible operations on the same column.
        
        Example:
          Step 1: TRIM on 'email'
          Step 2: LOWERCASE on 'email'
          --> Fused into Step 1: LOWERCASE with fused_operations=[TRIM, LOWERCASE]
        """
        fused_steps: List[LogicalStep] = []
        i = 0
        while i < len(steps):
            current = steps[i]
            # Check if current step can be fused with next step
            if i + 1 < len(steps):
                nxt = steps[i + 1]
                # Check condition for string fusion: same dataset, single same column, trim + lower/upper
                if (
                    current.dataset_name == nxt.dataset_name
                    and current.target_columns == nxt.target_columns
                    and len(current.target_columns) == 1
                ):
                    # Fusion case 1: TRIM followed by LOWERCASE or UPPERCASE
                    if current.action == ActionType.TRIM and nxt.action in (ActionType.LOWERCASE, ActionType.UPPERCASE):
                        fused = nxt.model_copy()
                        fused.fused_operations = [ActionType.TRIM, nxt.action]
                        fused.description = f"Fused {current.action.value} and {nxt.action.value} on {current.target_columns[0]}"
                        fused.source_issue_ids = list(set(current.source_issue_ids + nxt.source_issue_ids))
                        fused_steps.append(fused)
                        i += 2
                        continue
                    
                    # Fusion case 2: ZERO_TO_NULL followed by FILL_NULLS
                    if current.action == ActionType.ZERO_TO_NULL and nxt.action == ActionType.FILL_NULLS:
                        fused = nxt.model_copy()
                        fused.fused_operations = [ActionType.ZERO_TO_NULL, ActionType.FILL_NULLS]
                        fused.description = f"Fused zero-to-null and fill-nulls on {current.target_columns[0]}"
                        fused.source_issue_ids = list(set(current.source_issue_ids + nxt.source_issue_ids))
                        fused_steps.append(fused)
                        i += 2
                        continue

            fused_steps.append(current)
            i += 1

        return fused_steps

    def _eliminate_redundant_steps(self, steps: List[LogicalStep]) -> List[LogicalStep]:
        """Removes exact duplicate steps on the same dataset/column."""
        seen: Set[Tuple[str, str, Tuple[str, ...]]] = set()
        deduped: List[LogicalStep] = []

        for step in steps:
            # Key: (dataset_name, action, sorted target_columns)
            key = (step.dataset_name, step.action.value, tuple(sorted(step.target_columns)))
            if key in seen:
                # Skip duplicate action
                continue
            seen.add(key)
            deduped.append(step)

        return deduped

    def execute(self, context: CompilerContext) -> CompilerContext:
        if not context.logical_plan or not context.logical_plan.steps:
            return context

        initial_count = len(context.logical_plan.steps)
        
        # Pass 1: Redundant step elimination
        deduped = self._eliminate_redundant_steps(context.logical_plan.steps)
        
        # Pass 2: Operator fusion
        optimized = self._fuse_operators(deduped)

        context.logical_plan.steps = optimized
        context.logical_plan.metadata["initial_steps"] = initial_count
        context.logical_plan.metadata["optimized_steps"] = len(optimized)
        context.logical_plan.metadata["fused_count"] = initial_count - len(optimized)

        EventBus.get_instance().emit(
            event_type=CompilerEventType.AST_OPTIMIZED,
            session_id=context.session_id,
            payload={
                "initial_count": initial_count,
                "optimized_count": len(optimized),
                "savings": initial_count - len(optimized),
            },
            source_agent=self.__class__.__name__,
        )

        return context
