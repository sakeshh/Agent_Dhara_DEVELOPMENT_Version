from __future__ import annotations

import time
from typing import Dict, List, Optional
from agent.compiler.base.agent import BaseAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.actions import StructuredAction
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType, DataEffect, LogicalPlan, LogicalStep
from agent.compiler.models.profile import DatasetProfile
from agent.compiler.planners.confidence_agent import ConfidenceAgent
from agent.compiler.planners.fingerprint import compute_plan_fingerprint
from agent.compiler.planners.issue_compiler import IssueCompiler
from agent.compiler.validation.action_validator import ActionValidator


class LogicalPlanner(BaseAgent):
    """Builds and dependency-orders an engine-neutral LogicalPlan from issues, business rules, and structured actions."""

    # Semantic ordering groups:
    # 1. Canonicalization: TRIM, LOWER/UPPER, PARSE_DATES, SANITIZE
    # 2. Type Normalization: CAST_TYPE, COERCE_NUMERIC
    # 3. Value Transformation (Level 2 Composable): PATTERN_REPLACE, VALUE_MAP, PAD, PREFIX_ADD, SUFFIX_ADD, CONDITIONAL_UPDATE
    # 4. Missing-Value Handling: ZERO_TO_NULL, REPLACE_SENTINELS, FILL_NULLS
    # 5. Constraint / Range Remediation: RANGE_CLIP, CAP_OUTLIERS, STANDARDIZE_BOOLEAN
    # 6. Dataset-Level Operations: DROP_COLUMN, DEDUPLICATE, VALIDATE_REFERENTIAL_INTEGRITY
    ACTION_PRIORITY: Dict[ActionType, int] = {
        # Group 1: Canonicalization
        ActionType.TRIM: 5,
        ActionType.LOWERCASE: 8,
        ActionType.UPPERCASE: 8,
        ActionType.PARSE_DATES: 12,
        ActionType.SANITIZE_EMAIL: 15,
        ActionType.NORMALIZE_PHONE: 18,
        ActionType.MASK_PHONE: 20,

        # Group 2: Type Normalization
        ActionType.CAST_TYPE: 35,
        ActionType.COERCE_NUMERIC: 40,

        # Group 3: Value Transformation (Level 2 Composable)
        ActionType.PATTERN_REPLACE: 50,
        ActionType.VALUE_MAP: 52,
        ActionType.PAD: 54,
        ActionType.PREFIX_ADD: 56,
        ActionType.SUFFIX_ADD: 58,
        ActionType.CONDITIONAL_UPDATE: 60,

        # Group 4: Missing-Value Handling
        ActionType.ZERO_TO_NULL: 70,
        ActionType.REPLACE_SENTINELS: 72,
        ActionType.FILL_NULLS: 80,

        # Group 5: Constraint / Range Remediation
        ActionType.RANGE_CLIP: 90,
        ActionType.CAP_OUTLIERS: 90,
        ActionType.STANDARDIZE_BOOLEAN: 92,

        # Group 6: Dataset-Level Operations
        ActionType.DROP_COLUMN: 110,
        ActionType.DEDUPLICATE: 200,
        ActionType.VALIDATE_REFERENTIAL_INTEGRITY: 300,

        # Legacy / Governed Only
        ActionType.CUSTOM_EXPRESSION: 150,
    }

    def __init__(self):
        self.issue_compiler = IssueCompiler()
        self.confidence_agent = ConfidenceAgent()
        self.action_validator = ActionValidator()

    def action_to_logical_step(
        self,
        action: StructuredAction,
        step_index: int,
        dataset_profiles: Optional[Dict[str, DatasetProfile]] = None,
    ) -> LogicalStep:
        """
        Adapts a validated StructuredAction into an engine-neutral LogicalStep.
        Rejects invalid actions before logical planning continues.
        """
        val_result = self.action_validator.validate(action, dataset_profiles=dataset_profiles)
        if not val_result.is_valid:
            errors = [f"{i.issue_code}: {i.message}" for i in val_result.issues]
            raise ValueError(f"Invalid StructuredAction cannot enter LogicalPlanner: {'; '.join(errors)}")

        effect = DataEffect.TRANSFORM_IN_PLACE
        if action.action_type == ActionType.DEDUPLICATE:
            effect = DataEffect.FILTER_ROWS
        elif action.action_type == ActionType.DROP_COLUMN:
            effect = DataEffect.DROP_COLUMN
        elif action.action_type == ActionType.VALIDATE_REFERENTIAL_INTEGRITY:
            effect = DataEffect.QUARANTINE_ROWS

        deps = list(action.metadata.get("depends_on_step_ids") or [])
        source_act_id = getattr(action, "action_id", None) or f"action_{action.dataset_name}_{action.action_type.value}_{step_index}"
        source_issues = list(getattr(action, "source_issue_ids", []) or [])
        if not source_issues and action.source_reference:
            source_issues = [action.source_reference]

        return LogicalStep(
            step_id=f"step_{action.dataset_name}_{action.action_type.value}_{step_index}",
            dataset_name=action.dataset_name,
            action=action.action_type,
            target_columns=list(action.target_columns),
            parameters=dict(action.parameters),
            effect=effect,
            priority=self.ACTION_PRIORITY.get(action.action_type, 100),
            confidence=action.confidence if action.confidence is not None else 1.0,
            requires_user_approval=action.requires_approval,
            source_action_id=source_act_id,
            source_issue_ids=source_issues,
            description=f"Apply {action.action_type.value} on {', '.join(action.target_columns)}",
            depends_on_step_ids=deps,
            source=action.source.value if action.source else None,
            source_reference=action.source_reference,
            extraction_method=action.extraction_method,
            metadata=dict(action.metadata or {}),
        )

    def _topological_sort(self, steps: List[LogicalStep]) -> List[LogicalStep]:
        """
        Hybrid topological ordering:
          1. Explicit dependency graph (edges from dependency -> dependent step).
          2. ACTION_PRIORITY semantic group as deterministic tie-breaker.
          3. Original insertion index as final stable tie-breaker.
        Detects dependency cycles and raises ValueError if a cycle exists.
        """
        if not steps:
            return []

        dataset_map: Dict[str, List[LogicalStep]] = {}
        for s in steps:
            dataset_map.setdefault(s.dataset_name, []).append(s)

        sorted_all_steps: List[LogicalStep] = []
        for ds_name in sorted(dataset_map.keys()):
            ds_steps = dataset_map[ds_name]
            step_by_id = {s.step_id: s for s in ds_steps}
            index_by_id = {s.step_id: idx for idx, s in enumerate(ds_steps)}

            # Build dependency adjacency and in-degree map
            adj: Dict[str, List[str]] = {s.step_id: [] for s in ds_steps}
            in_degree: Dict[str, int] = {s.step_id: 0 for s in ds_steps}

            for s in ds_steps:
                for dep_id in s.depends_on_step_ids:
                    if dep_id in step_by_id:
                        adj[dep_id].append(s.step_id)
                        in_degree[s.step_id] += 1

            # Ready queue (in-degree == 0)
            ready = [s for s in ds_steps if in_degree[s.step_id] == 0]
            # Priority and stable index as tie-breaker
            ready.sort(key=lambda s: (s.priority, index_by_id[s.step_id]))

            ds_sorted: List[LogicalStep] = []
            while ready:
                curr = ready.pop(0)
                ds_sorted.append(curr)

                for nxt_id in adj[curr.step_id]:
                    in_degree[nxt_id] -= 1
                    if in_degree[nxt_id] == 0:
                        ready.append(step_by_id[nxt_id])

                # Maintain deterministic priority tie-breaker in ready queue
                ready.sort(key=lambda s: (s.priority, index_by_id[s.step_id]))

            # Cycle detection
            if len(ds_sorted) < len(ds_steps):
                remaining = [s.step_id for s in ds_steps if s not in ds_sorted]
                raise ValueError(f"Dependency cycle detected in dataset '{ds_name}' involving steps: {remaining}")

            sorted_all_steps.extend(ds_sorted)

        return sorted_all_steps

    def execute(self, context: CompilerContext) -> CompilerContext:
        steps: List[LogicalStep] = []
        dataset_names = set(context.dataset_profiles.keys())
        
        # Primary compilation: structured actions
        if context.structured_actions:
            action_source = "structured_actions"
            for idx, action in enumerate(context.structured_actions):
                if isinstance(action, StructuredAction):
                    step = self.action_to_logical_step(action, idx, context.dataset_profiles)
                elif isinstance(action, dict):
                    act_obj = StructuredAction.model_validate(action)
                    step = self.action_to_logical_step(act_obj, idx, context.dataset_profiles)
                else:
                    continue
                steps.append(step)
                dataset_names.add(step.dataset_name)
        elif context.detected_issues:
            action_source = "legacy_detected_issues"
            # Fallback compilation for legacy contexts without structured actions
            for idx, issue in enumerate(context.detected_issues):
                step = self.issue_compiler.issue_to_logical_step(issue, idx)
                step.priority = self.ACTION_PRIORITY.get(step.action, 100)
                step.source_action_id = f"action_synthetic_{issue.issue_id}"
                step.source_issue_ids = [issue.issue_id]
                steps.append(step)
                dataset_names.add(issue.dataset_name)
        else:
            action_source = "none"

        context.metadata["planner_input_source"] = action_source

        # 3. Add business rules steps if applicable
        for rule in context.business_rules:
            if not rule.enabled:
                continue
            if rule.name == "never_drop_rows":
                context.options["never_drop_rows"] = True

        # 4. Hybrid Topological & Semantic Sorting (with cycle detection)
        sorted_steps = self._topological_sort(steps)

        fingerprint = compute_plan_fingerprint(context, sorted_steps)
        sess = context.session_id or "default"
        plan_id = f"plan_{sess}_{fingerprint}"
        logical_plan = LogicalPlan(
            plan_id=plan_id,
            dataset_names=sorted(list(dataset_names)),
            steps=sorted_steps,
            invariants={"never_drop_rows": context.options.get("never_drop_rows", False)},
            metadata={"step_count": len(sorted_steps), "fingerprint": fingerprint},
        )

        context.logical_plan = logical_plan
        
        # 5. Score steps for confidence and governance
        context = self.confidence_agent.execute(context)

        # Emit event
        EventBus.get_instance().emit(
            event_type=CompilerEventType.LOGICAL_PLAN_CREATED,
            session_id=context.session_id,
            payload={"plan_id": plan_id, "step_count": len(sorted_steps)},
            source_agent=self.__class__.__name__,
        )

        return context
