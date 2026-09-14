"""
Assessment Orchestration Bridge and Typed Evidence Evaluator.

Implements Integration 2 of the Business Rules Pipeline:
- Consumes assessable claims from CanonicalRuleRegistry.
- Evaluates rules against DataFrames with strict six-part population accounting:
    total = pass + violation + null_unknown + operator_unknown + technical_excluded + out_of_scope
- Honors notes_assessment_mode:
    - 'planning_only': un-evaluated rules receive NOT_EVALUATED, violation_count = None (NEVER zero!).
    - 'pre_assessment': rules evaluated directly against source datasets.
- Emits immutable RuleAssessmentEvidence and projects backward-compatible DQ issues for legacy consumers.
"""
from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Mapping, Optional, Set, Tuple

import pandas as pd
from pydantic import BaseModel, Field

from agent.rules.assessment_models import (
    EvaluationCompleteness,
    EvaluationPopulationCounts,
    RuleAssessmentEvidence,
    calculate_completeness_metrics,
)
from agent.rules.rule_models import (
    AssessmentLifecycleStatus,
    CanonicalExpression,
    CanonicalRule,
    CanonicalRuleRegistry,
    ColumnOperand,
    DataRuleOutcome,
    LiteralOperand,
    NullPolicy,
    RuleAction,
    RuleType,
)

logger = logging.getLogger("agent.rules.assessment_bridge")


class AssessmentBridgeResult(BaseModel):
    """Aggregate result from evaluating canonical rules against dataset DataFrames."""
    run_id: str
    evaluated_at: datetime
    evidence_records: Tuple[RuleAssessmentEvidence, ...] = ()
    not_evaluated_rules: Tuple[Dict[str, Any], ...] = ()
    legacy_dq_issues: Dict[str, List[Dict[str, Any]]] = Field(default_factory=dict)
    summary: Dict[str, Any] = Field(default_factory=dict)


def _evaluate_column_assertion(
    df: pd.DataFrame,
    rule: CanonicalRule,
) -> EvaluationPopulationCounts:
    """
    Evaluates a column assertion against a DataFrame using strict six-part row accounting.
    """
    total_rows = len(df)
    if total_rows == 0:
        return EvaluationPopulationCounts(
            total_source_rows=0,
            pass_rows=0,
            violation_rows=0,
            null_unknown_rows=0,
            operator_unknown_rows=0,
            technical_excluded_rows=0,
            out_of_scope_rows=0,
        )

    mat = rule.material
    expr = mat.expression
    cols = mat.scope.columns

    # 1. Check if column exists in DataFrame
    if not cols or cols[0] not in df.columns:
        # Technical exclusion: missing column in dataset
        return EvaluationPopulationCounts(
            total_source_rows=total_rows,
            pass_rows=0,
            violation_rows=total_rows,
            null_unknown_rows=0,
            operator_unknown_rows=0,
            technical_excluded_rows=0,
            out_of_scope_rows=0,
        )

    target_col = cols[0]
    series = df[target_col]

    # 2. Check nulls under NullPolicy
    null_mask = series.isna()
    null_count = int(null_mask.sum())

    non_null_series = series[~null_mask]
    non_null_count = len(non_null_series)

    # 3. Evaluate operator on non-null rows
    op = expr.operator.upper()
    pass_count = 0
    viol_count = 0
    op_unknown_count = 0

    if op == "IS_NOT_NULL":
        pass_count = non_null_count
        viol_count = null_count
        null_unknown = 0
        return EvaluationPopulationCounts(
            total_source_rows=total_rows,
            pass_rows=pass_count,
            violation_rows=viol_count,
            null_unknown_rows=null_unknown,
            operator_unknown_rows=0,
            technical_excluded_rows=0,
            out_of_scope_rows=0,
        )

    if op == "IS_NULL":
        pass_count = null_count
        viol_count = non_null_count
        return EvaluationPopulationCounts(
            total_source_rows=total_rows,
            pass_rows=pass_count,
            violation_rows=viol_count,
            null_unknown_rows=0,
            operator_unknown_rows=0,
            technical_excluded_rows=0,
            out_of_scope_rows=0,
        )

    if mat.null_policy == NullPolicy.SEPARATE_VIOLATION:
        null_unknown = null_count
    else:
        null_unknown = 0
        viol_count += null_count

    # Extract literal value for comparison
    lit_val: Any = None
    for operand in expr.operands:
        if isinstance(operand, LiteralOperand):
            lit_val = operand.value
            break

    try:
        if op == "IN" and isinstance(lit_val, (list, tuple, set)):
            allowed_set = {str(x).strip().lower() for x in lit_val}
            str_series = non_null_series.astype(str).str.strip().str.lower()
            is_pass = str_series.isin(allowed_set)
            pass_count = int(is_pass.sum())
            viol_count += int((~is_pass).sum())

        elif op in (">", ">=", "<", "<="):
            num_series = pd.to_numeric(non_null_series, errors="coerce")
            num_invalid = num_series.isna()
            op_unknown_count = int(num_invalid.sum())
            valid_num_series = num_series[~num_invalid]
            num_threshold = float(lit_val)

            if op == ">":
                is_pass = valid_num_series > num_threshold
            elif op == ">=":
                is_pass = valid_num_series >= num_threshold
            elif op == "<":
                is_pass = valid_num_series < num_threshold
            elif op == "<=":
                is_pass = valid_num_series <= num_threshold

            pass_count = int(is_pass.sum())
            viol_count += int((~is_pass).sum())

        elif op == "==":
            str_series = non_null_series.astype(str).str.strip().str.lower()
            is_pass = str_series == str(lit_val).strip().lower()
            pass_count = int(is_pass.sum())
            viol_count += int((~is_pass).sum())

        elif op == "!=":
            str_series = non_null_series.astype(str).str.strip().str.lower()
            is_pass = str_series != str(lit_val).strip().lower()
            pass_count = int(is_pass.sum())
            viol_count += int((~is_pass).sum())

        else:
            # Fallback for unhandled operators
            op_unknown_count = non_null_count

    except Exception as exc:
        logger.warning(f"Error evaluating rule {rule.rule_id}: {exc}")
        op_unknown_count = non_null_count

    # Partition validation
    return EvaluationPopulationCounts(
        total_source_rows=total_rows,
        pass_rows=pass_count,
        violation_rows=viol_count,
        null_unknown_rows=null_unknown,
        operator_unknown_rows=op_unknown_count,
        technical_excluded_rows=0,
        out_of_scope_rows=0,
    )


def evaluate_canonical_registry(
    registry: CanonicalRuleRegistry,
    datasets: Dict[str, pd.DataFrame],
    notes_assessment_mode: Literal["pre_assessment", "planning_only"] = "pre_assessment",
    run_id: Optional[str] = None,
) -> AssessmentBridgeResult:
    """
    Evaluates assessable rules from the CanonicalRuleRegistry against dataframes.
    Returns typed RuleAssessmentEvidence records and legacy DQ issue projections.
    """
    active_run_id = run_id or f"run-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)

    evidence_list: List[RuleAssessmentEvidence] = []
    not_evaluated: List[Dict[str, Any]] = []
    legacy_issues_by_dataset: Dict[str, List[Dict[str, Any]]] = {}

    for rule_id, rule in registry.rules.items():
        mat = rule.material
        ds_name = mat.scope.dataset

        # Check if rule is assessable
        if mat.rule_type not in (
            RuleType.VALIDATION_ASSERTION,
            RuleType.SCHEMA_CONSTRAINT,
            RuleType.REFERENTIAL_INTEGRITY,
            RuleType.ANOMALY_DETECTION,
        ):
            continue

        # Handle notes_assessment_mode == 'planning_only'
        # Rules derived purely from unstructured notes in planning_only mode are marked NOT_EVALUATED
        is_note_rule = any(
            occ.source_type == "notes"
            for occ_id in rule.source_occurrences
            if (occ := registry.occurrences.get(occ_id))
        )

        if is_note_rule and notes_assessment_mode == "planning_only":
            not_evaluated.append({
                "rule_id": rule_id,
                "logical_rule_id": rule.logical_rule_id,
                "dataset": ds_name,
                "assessment_status": AssessmentLifecycleStatus.NOT_EVALUATED.value,
                "violation_count": None,  # NEVER ZERO!
                "reason": "Rule was interpreted after assessment in planning_only mode",
            })
            continue

        # Locate dataset dataframe
        df = datasets.get(ds_name)
        if df is None:
            # Try case-insensitive lookup
            for d_key, d_val in datasets.items():
                if d_key.lower() == ds_name.lower():
                    df = d_val
                    break

        if df is None:
            not_evaluated.append({
                "rule_id": rule_id,
                "logical_rule_id": rule.logical_rule_id,
                "dataset": ds_name,
                "assessment_status": AssessmentLifecycleStatus.NOT_EVALUATED.value,
                "violation_count": None,
                "reason": f"Dataset '{ds_name}' not loaded in current assessment context",
            })
            continue

        # Evaluate rule
        start_time = time.perf_counter()
        counts = _evaluate_column_assertion(df, rule)
        duration_ms = (time.perf_counter() - start_time) * 1000.0

        ev_id = f"ev-{uuid.uuid4().hex[:12]}"
        evidence = RuleAssessmentEvidence.create(
            evidence_id=ev_id,
            rule_id=rule_id,
            counts=counts,
            evaluated_at=now,
            execution_duration_ms=round(duration_ms, 2),
        )
        evidence_list.append(evidence)

        # Build legacy issue projection for backward compatibility
        if counts.violation_rows > 0:
            target_col = mat.scope.columns[0] if mat.scope.columns else "unknown"
            issue_dict = {
                "severity": "high" if mat.rule_type == RuleType.SCHEMA_CONSTRAINT else "medium",
                "issue_type": f"canonical_{mat.rule_type.value}",
                "message": f"Rule {rule.logical_rule_id} failed: {counts.violation_rows} violation rows",
                "column": target_col,
                "count": counts.violation_rows,
                "evidence_id": ev_id,
                "rule_id": rule_id,
                "dataset": ds_name,
            }
            legacy_issues_by_dataset.setdefault(ds_name, []).append(issue_dict)

    # Summary metrics
    total_evaluated = len(evidence_list)
    passed_rules = sum(1 for e in evidence_list if e.data_rule_outcome == DataRuleOutcome.PASS)
    failed_rules = sum(1 for e in evidence_list if e.data_rule_outcome == DataRuleOutcome.FAIL)

    summary = {
        "total_rules_in_registry": len(registry.rules),
        "total_rules_evaluated": total_evaluated,
        "rules_passed": passed_rules,
        "rules_failed": failed_rules,
        "not_evaluated_count": len(not_evaluated),
        "run_id": active_run_id,
    }

    return AssessmentBridgeResult(
        run_id=active_run_id,
        evaluated_at=now,
        evidence_records=tuple(evidence_list),
        not_evaluated_rules=tuple(not_evaluated),
        legacy_dq_issues=legacy_issues_by_dataset,
        summary=summary,
    )
