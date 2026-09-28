from __future__ import annotations

from typing import List, Optional

from agent.compiler.learning.promoted_models import PromotedRule, PromotionEvidence, RuleLifecycleState
from agent.compiler.models.logical import ActionType


class PromotionEvaluator:
    """
    Evaluates whether a candidate rule satisfies strict multi-dimensional criteria
    for promotion from STAGED -> DATASET_SCOPED and DATASET_SCOPED -> GLOBAL_PROMOTED.
    Prevents single-percentage overfitting.
    """

    GENERIC_ACTIONS = {
        ActionType.TRIM,
        ActionType.LOWERCASE,
        ActionType.UPPERCASE,
        ActionType.CAST_TYPE,
        ActionType.PARSE_DATES,
        ActionType.FILL_NULLS,
        ActionType.ZERO_TO_NULL,
    }

    @classmethod
    def evaluate_for_dataset_scoped(
        cls,
        rule: PromotedRule,
        evidence_list: List[PromotionEvidence],
        min_evidence_count: int = 1,
    ) -> tuple[bool, str]:
        if len(evidence_list) < min_evidence_count:
            return False, f"Insufficient execution evidence: {len(evidence_list)} runs recorded (minimum {min_evidence_count} required)."

        for ev in evidence_list:
            # 1. BLOCKED runs cannot count as successful evidence
            if ev.governance_decision.lower() == "blocked" or ev.safety_status.lower() == "blocked":
                return False, f"Rule has blocked executions (run_id: {ev.run_id}); cannot promote."

            # 2. Reconciliation failure strictly prevents promotion
            if not ev.reconciliation_passed:
                return False, f"Reconciliation failed in run '{ev.run_id}'. Invariant violations prevent promotion."

            # 3. Execution success check
            if not ev.success:
                return False, f"Execution failed in run '{ev.run_id}'."

        return True, "Satisfies all criteria for DATASET_SCOPED promotion"

    @classmethod
    def evaluate_for_global_promoted(
        cls,
        rule: PromotedRule,
        evidence_list: List[PromotionEvidence],
        approved_by: Optional[str] = None,
        min_distinct_datasets: int = 2,
    ) -> tuple[bool, str]:
        # 1. Action type / Semantics universality check
        # Domain business keys and dataset-specific prefixes cannot be globally promoted
        if rule.action_type not in cls.GENERIC_ACTIONS:
            return False, f"Action '{rule.action_type.value}' carries domain-specific business assumptions and cannot be promoted globally."

        # 2. Multi-dataset evidence check
        distinct_datasets = set(ev.dataset for ev in evidence_list if ev.success)
        if len(distinct_datasets) < min_distinct_datasets:
            return False, f"Tested across only {len(distinct_datasets)} dataset(s). Global promotion requires at least {min_distinct_datasets} distinct datasets."

        # 3. Explicit governance approval check
        if not approved_by and not rule.approved_by:
            return False, "Global promotion requires explicit governance approval by a designated authority."

        # 4. Reconciliation check across all runs
        for ev in evidence_list:
            if not ev.reconciliation_passed or not ev.success:
                return False, f"Unresolved execution failure or reconciliation discrepancy in run '{ev.run_id}'."

        return True, "Satisfies all criteria for GLOBAL_PROMOTED promotion"
