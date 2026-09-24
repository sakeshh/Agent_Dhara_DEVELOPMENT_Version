from __future__ import annotations

import time
from typing import Dict, List
from agent.compiler.base.agent import BaseAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType, LogicalPlan, LogicalStep
from agent.compiler.planners.confidence_agent import ConfidenceAgent
from agent.compiler.planners.issue_compiler import IssueCompiler


class LogicalPlanner(BaseAgent):
    """Builds and dependency-orders an engine-neutral LogicalPlan from issues and business rules."""

    ACTION_PRIORITY: Dict[ActionType, int] = {
        ActionType.TRIM: 5,
        ActionType.LOWERCASE: 8,
        ActionType.UPPERCASE: 8,
        ActionType.ZERO_TO_NULL: 30,
        ActionType.REPLACE_SENTINELS: 31,
        ActionType.CAST_TYPE: 35,
        ActionType.COERCE_NUMERIC: 40,
        ActionType.PARSE_DATES: 45,
        ActionType.SANITIZE_EMAIL: 50,
        ActionType.NORMALIZE_PHONE: 55,
        ActionType.MASK_PHONE: 57,
        ActionType.RANGE_CLIP: 65,
        ActionType.CAP_OUTLIERS: 65,
        ActionType.STANDARDIZE_BOOLEAN: 70,
        ActionType.FILL_NULLS: 80,
        ActionType.DROP_COLUMN: 85,
        ActionType.DEDUPLICATE: 200,
        ActionType.VALIDATE_REFERENTIAL_INTEGRITY: 300,
        ActionType.CUSTOM_EXPRESSION: 150,
    }

    def __init__(self):
        self.issue_compiler = IssueCompiler()
        self.confidence_agent = ConfidenceAgent()

    def execute(self, context: CompilerContext) -> CompilerContext:
        steps: List[LogicalStep] = []
        dataset_names = set(context.dataset_profiles.keys())
        
        # 1. Compile detected issues into logical steps
        for idx, issue in enumerate(context.detected_issues):
            step = self.issue_compiler.issue_to_logical_step(issue, idx)
            step.priority = self.ACTION_PRIORITY.get(step.action, 100)
            steps.append(step)
            dataset_names.add(issue.dataset_name)

        # 2. Add business rules steps if applicable
        for rule in context.business_rules:
            if not rule.enabled:
                continue
            if rule.name == "never_drop_rows":
                context.options["never_drop_rows"] = True

        # 3. Sort topologically by priority
        steps.sort(key=lambda s: (s.dataset_name, s.priority))

        plan_id = f"plan_{context.session_id}_{int(time.time())}"
        logical_plan = LogicalPlan(
            plan_id=plan_id,
            dataset_names=sorted(list(dataset_names)),
            steps=steps,
            invariants={"never_drop_rows": context.options.get("never_drop_rows", False)},
            metadata={"step_count": len(steps)},
        )

        context.logical_plan = logical_plan
        
        # 4. Score steps for confidence and governance
        context = self.confidence_agent.execute(context)

        # Emit event
        EventBus.get_instance().emit(
            event_type=CompilerEventType.LOGICAL_PLAN_CREATED,
            session_id=context.session_id,
            payload={"plan_id": plan_id, "step_count": len(steps)},
            source_agent=self.__class__.__name__,
        )

        return context
