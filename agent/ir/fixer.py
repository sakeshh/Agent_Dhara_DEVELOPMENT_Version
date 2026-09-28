from __future__ import annotations

import difflib
import logging
import time
from typing import Dict, List, Optional, Tuple
from agent.compiler.models.profile import DatasetProfile
from agent.ir.errors import IrErrorCode, IrValidationResult
from agent.ir.models import IrMetadata, IrPlan, IrRepairMetrics, RepairRecord
from agent.ir.normalizer import IrNormalizer
from agent.ir.validator import IrValidator

logger = logging.getLogger("agent.ir.fixer")


class IrFixer:
    """
    Self-healing fixer for Intermediate Representation plans.
    Applies multi-level deterministic, heuristic, and optional LLM repairs based on schema validation errors.

    Levels:
    - Level 1: Deterministic case, whitespace, and alias resolution.
    - Level 2: Unambiguous schema similarity (single high-confidence fuzzy match).
    - Level 3: Conflict detection (aborts guessing if multiple candidates are equally likely).
    - Level 4: Optional LLM repair for complex semantic errors (only as last resort).
    """

    @classmethod
    def _is_unambiguous_match(
        cls,
        target: str,
        suggestions: List[str],
        confidence_threshold: float = 0.70,
        gap_threshold: float = 0.15,
    ) -> Tuple[Optional[str], float]:
        """
        Evaluates whether suggestions contain a single high-confidence winner.
        Returns:
            Tuple of (winning_string_or_None, confidence_score)
        """
        if not suggestions:
            return None, 0.0

        if len(suggestions) == 1:
            score = difflib.SequenceMatcher(None, target.lower(), suggestions[0].lower()).ratio()
            return (suggestions[0], round(score, 3)) if score >= confidence_threshold else (None, round(score, 3))

        # Score all suggestions
        scored = []
        for s in suggestions:
            score = difflib.SequenceMatcher(None, target.lower(), s.lower()).ratio()
            scored.append((score, s))

        scored.sort(key=lambda x: x[0], reverse=True)
        top_score, top_match = scored[0]
        runner_up_score, _ = scored[1]

        # Must exceed threshold and have a clear gap over the second candidate
        if top_score >= confidence_threshold and (top_score - runner_up_score) >= gap_threshold:
            return top_match, round(top_score, 3)

        # Conflict detected: do not guess
        return None, round(top_score, 3)

    @classmethod
    def _apply_column_replacement(
        cls,
        plan: IrPlan,
        table_name: Optional[str],
        old_col: str,
        new_col: str,
    ) -> None:
        """Substitutes old_col with new_col across sources, logical steps, and business rules."""
        # Sources
        for src in plan.sources:
            if not table_name or src.table.lower() == table_name.lower():
                src.columns = [new_col if c.lower() == old_col.lower() else c for c in src.columns]

        # Steps
        for step in plan.logical_plan:
            if step.source_node and (not table_name or step.source_node.table.lower() == table_name.lower()):
                step.source_node.columns = [
                    new_col if c.lower() == old_col.lower() else c for c in step.source_node.columns
                ]

            if step.filter_node and (not table_name or (step.filter_node.table and step.filter_node.table.lower() == table_name.lower())):
                if step.filter_node.column and step.filter_node.column.lower() == old_col.lower():
                    step.filter_node.column = new_col
                if step.filter_node.rule and step.filter_node.rule.column and step.filter_node.rule.column.lower() == old_col.lower():
                    step.filter_node.rule.column = new_col

            if step.join_node:
                jn = step.join_node
                if not table_name or jn.left_table.lower() == table_name.lower():
                    jn.left_keys = [new_col if k.lower() == old_col.lower() else k for k in jn.left_keys]
                if not table_name or jn.right_table.lower() == table_name.lower():
                    jn.right_keys = [new_col if k.lower() == old_col.lower() else k for k in jn.right_keys]

            if step.aggregate_node and (not table_name or (step.aggregate_node.table and step.aggregate_node.table.lower() == table_name.lower())):
                step.aggregate_node.group_by = [
                    new_col if g.lower() == old_col.lower() else g for g in step.aggregate_node.group_by
                ]
                for agg in step.aggregate_node.aggregations:
                    if agg.column.lower() == old_col.lower():
                        agg.column = new_col

            if step.projection_node and (not table_name or (step.projection_node.table and step.projection_node.table.lower() == table_name.lower())):
                step.projection_node.columns = [
                    new_col if c.lower() == old_col.lower() else c for c in step.projection_node.columns
                ]
                if old_col in step.projection_node.aliases:
                    alias = step.projection_node.aliases.pop(old_col)
                    step.projection_node.aliases[new_col] = alias

        # Business rules
        for rule in plan.business_rules:
            if not table_name or (rule.target_table and rule.target_table.lower() == table_name.lower()):
                if rule.column and rule.column.lower() == old_col.lower():
                    rule.column = new_col

    @classmethod
    def _apply_table_replacement(cls, plan: IrPlan, old_table: str, new_table: str) -> None:
        """Substitutes old_table with new_table across sources, steps, rules, and lineage."""
        for src in plan.sources:
            if src.table.lower() == old_table.lower():
                src.table = new_table

        for step in plan.logical_plan:
            if step.source_node and step.source_node.table.lower() == old_table.lower():
                step.source_node.table = new_table
            if step.filter_node and step.filter_node.table and step.filter_node.table.lower() == old_table.lower():
                step.filter_node.table = new_table
            if step.join_node:
                if step.join_node.left_table.lower() == old_table.lower():
                    step.join_node.left_table = new_table
                if step.join_node.right_table.lower() == old_table.lower():
                    step.join_node.right_table = new_table
            if step.aggregate_node and step.aggregate_node.table and step.aggregate_node.table.lower() == old_table.lower():
                step.aggregate_node.table = new_table
            if step.projection_node and step.projection_node.table and step.projection_node.table.lower() == old_table.lower():
                step.projection_node.table = new_table

        for rule in plan.business_rules:
            if rule.target_table and rule.target_table.lower() == old_table.lower():
                rule.target_table = new_table

        plan.lineage.source_tables = [
            new_table if t.lower() == old_table.lower() else t for t in plan.lineage.source_tables
        ]

    @classmethod
    def fix(
        cls,
        plan: IrPlan,
        validation_result: IrValidationResult,
        dataset_profiles: Dict[str, DatasetProfile],
    ) -> Tuple[IrPlan, IrValidationResult, bool]:
        """
        Single-pass self-healing repair on an invalid IrPlan.
        Records an audit trail of applied repairs in `plan.metadata.repair_history`.
        """
        if validation_result.valid:
            return plan, validation_result, True

        plan_copy = IrPlan.model_validate(plan.model_dump())
        repairs_applied = 0

        # Iterate over validation errors
        for err in validation_result.errors:
            if err.error_code == IrErrorCode.MISSING_TABLE and err.target_table:
                winner, confidence = cls._is_unambiguous_match(err.target_table, err.suggestions)
                if winner:
                    logger.info(f"[IrFixer] Repairing table: '{err.target_table}' -> '{winner}' (confidence={confidence})")
                    cls._apply_table_replacement(plan_copy, err.target_table, winner)
                    plan_copy.metadata.repair_history.append(
                        RepairRecord(
                            original=err.target_table,
                            corrected=winner,
                            strategy="fuzzy_match",
                            confidence=confidence,
                            target_entity="table",
                            table=winner,
                        )
                    )
                    repairs_applied += 1

            elif err.error_code in (IrErrorCode.MISSING_COLUMN, IrErrorCode.INVALID_RULE) and err.target_column:
                winner, confidence = cls._is_unambiguous_match(err.target_column, err.suggestions)
                if winner:
                    logger.info(
                        f"[IrFixer] Repairing column in table '{err.target_table}': '{err.target_column}' -> '{winner}' (confidence={confidence})"
                    )
                    cls._apply_column_replacement(plan_copy, err.target_table, err.target_column, winner)
                    plan_copy.metadata.repair_history.append(
                        RepairRecord(
                            original=err.target_column,
                            corrected=winner,
                            strategy="fuzzy_match",
                            confidence=confidence,
                            target_entity="column",
                            table=err.target_table,
                        )
                    )
                    repairs_applied += 1

        if repairs_applied == 0:
            logger.info("[IrFixer] No deterministic or unambiguous repairs could be applied.")
            return plan, validation_result, False

        # Re-normalize and re-validate after repairs
        normalized_plan = IrNormalizer.normalize(plan_copy, dataset_profiles)
        final_validation = IrValidator.validate(normalized_plan, dataset_profiles)

        is_fixed = final_validation.valid
        if is_fixed:
            logger.info(f"[IrFixer] Plan successfully repaired ({repairs_applied} fixes applied).")
        else:
            logger.warning(
                f"[IrFixer] Plan partially repaired ({repairs_applied} fixes applied), but {len(final_validation.errors)} errors remain."
            )

        return normalized_plan, final_validation, is_fixed

    @classmethod
    def heal_plan(
        cls,
        plan: IrPlan,
        dataset_profiles: Dict[str, DatasetProfile],
        max_attempts: int = 2,
    ) -> Tuple[IrPlan, IrValidationResult, bool]:
        """
        Bounded multi-pass self-healing loop with telemetry and hard limit.
        Guarantees loop terminates within `max_attempts` passes.
        """
        t0 = time.perf_counter()
        current_plan = plan
        val_result = IrValidator.validate(current_plan, dataset_profiles)

        metrics = IrRepairMetrics(
            validation_attempts=1,
            fixer_invocations=0,
            repairs_applied=0,
            ambiguous_conflicts=0,
        )

        if val_result.valid:
            metrics.final_validation_status = "passed"
            metrics.repair_duration_ms = (time.perf_counter() - t0) * 1000
            current_plan.metadata.repair_metrics = metrics
            logger.info("[IrFixer] repairs=%d", metrics.repairs_applied)
            return current_plan, val_result, True

        # Multi-pass repair loop with hard cap
        for attempt in range(1, max_attempts + 1):
            metrics.fixer_invocations += 1
            prev_repairs_count = len(current_plan.metadata.repair_history)

            repaired_plan, next_val, is_fixed = cls.fix(current_plan, val_result, dataset_profiles)
            current_plan = repaired_plan
            val_result = next_val
            metrics.validation_attempts += 1

            newly_applied = len(current_plan.metadata.repair_history) - prev_repairs_count
            metrics.repairs_applied += newly_applied

            if is_fixed:
                metrics.final_validation_status = "repaired"
                break

            if newly_applied == 0:
                # No progress could be made deterministically
                metrics.ambiguous_conflicts = len(val_result.errors)
                metrics.final_validation_status = "failed"
                break

        if not val_result.valid and metrics.final_validation_status != "repaired":
            metrics.final_validation_status = "failed"
            metrics.ambiguous_conflicts = len(val_result.errors)

        metrics.repair_duration_ms = (time.perf_counter() - t0) * 1000
        current_plan.metadata.repair_metrics = metrics
        logger.info("[IrFixer] repairs=%d", metrics.repairs_applied)
        return current_plan, val_result, val_result.valid
