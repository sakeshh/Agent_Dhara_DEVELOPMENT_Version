from __future__ import annotations

import copy
from datetime import datetime
import logging
import re
from typing import Any, Dict, List, Optional, Set

from agent.compiler.models.actions import StructuredAction
from agent.compiler.models.execution import FailureBehavior, TransformationPolicy
from agent.compiler.models.logical import ActionType, LogicalStep
from agent.compiler.safety.circuit_breaker import CircuitBreaker
from agent.compiler.safety.models import (
    ConversionFailure,
    DryRunReport,
    SafetyStatus,
    StepSafetyMetrics,
)
from agent.errors import CircuitBreakerError

logger = logging.getLogger("agent.compiler.safety.dry_run")


class InMemoDryRunSimulator:
    """
    In-memory simulation and pre-flight validation on sample data.
    Detects conversion anomalies, unexpected null generation, and evaluates circuit-breaker thresholds
    before production execution.
    """

    @classmethod
    def simulate(
        cls,
        steps: List[StructuredAction] | List[LogicalStep],
        sample_records: List[Dict[str, Any]],
        policies: Optional[Dict[str, TransformationPolicy]] = None,
        pipeline_id: str = "dry_run_pipeline",
        raise_on_block: bool = True,
    ) -> DryRunReport:
        policies = policies or {}
        working_records = [copy.deepcopy(r) for r in sample_records]
        quarantined_records: List[Dict[str, Any]] = []
        step_metrics_list: List[StepSafetyMetrics] = []
        total_failed_count = 0
        circuit_triggered = False
        violations: List[str] = []

        total_input_rows = len(sample_records)

        for idx, step in enumerate(steps):
            if isinstance(step, StructuredAction):
                step_id = (step.metadata or {}).get("step_id") or f"step_{idx}_{step.action_type.value}"
                action_type = step.action_type
                target_columns = step.target_columns
                parameters = step.parameters
            else:
                step_id = step.step_id
                action_type = step.action
                target_columns = step.target_columns
                parameters = step.parameters

            policy = policies.get(step_id) or policies.get(action_type.value) or TransformationPolicy(
                action_type=action_type,
                max_failed_rows=0,
                max_failure_percentage=0.0,
                on_failure=FailureBehavior.BLOCK,
            )

            metrics = StepSafetyMetrics(
                step_id=step_id,
                action_type=action_type,
                target_columns=target_columns,
                input_rows=len(working_records),
            )

            # Snapshot working records in case step needs to be reverted on SKIP_RULE
            pre_step_snapshot = [copy.deepcopy(r) for r in working_records]
            failed_row_indices: Set[int] = set()
            unexpected_nulls = 0

            # Execute step simulation
            working_records, failed_row_indices, unexpected_nulls = cls._apply_step(
                action_type=action_type,
                target_columns=target_columns,
                parameters=parameters,
                records=working_records,
                metrics=metrics,
            )

            metrics.failed_rows = len(failed_row_indices)
            metrics.passed_rows = metrics.input_rows - metrics.failed_rows
            metrics.unexpected_nulls = unexpected_nulls
            if metrics.input_rows > 0:
                metrics.failure_percentage = (metrics.failed_rows / metrics.input_rows) * 100.0

            total_failed_count += metrics.failed_rows

            # Evaluate through CircuitBreaker
            try:
                metrics = CircuitBreaker.evaluate_step(metrics, policy, raise_on_block=raise_on_block)
            except CircuitBreakerError as e:
                circuit_triggered = True
                violations.append(str(e))
                metrics.status = SafetyStatus.BLOCKED
                step_metrics_list.append(metrics)
                if raise_on_block:
                    raise

            if metrics.status == SafetyStatus.BLOCKED:
                circuit_triggered = True
                step_metrics_list.append(metrics)
                return DryRunReport(
                    pipeline_id=pipeline_id,
                    status=SafetyStatus.BLOCKED,
                    is_safe_to_execute=False,
                    step_metrics=step_metrics_list,
                    total_input_rows=total_input_rows,
                    total_output_rows=len(working_records),
                    total_failed_rows=total_failed_count,
                    circuit_breaker_triggered=True,
                    violations=violations,
                    quarantined_records=quarantined_records,
                    clean_records=working_records,
                )
            elif metrics.status == SafetyStatus.QUARANTINED:
                # Divert failed rows to quarantine (preserving original raw data)
                new_working = []
                for r_idx, row in enumerate(working_records):
                    if r_idx in failed_row_indices:
                        quarantined_records.append(pre_step_snapshot[r_idx])
                    else:
                        new_working.append(row)
                working_records = new_working
            elif metrics.status == SafetyStatus.SKIPPED:
                # Revert records to pre-step snapshot
                working_records = pre_step_snapshot

            step_metrics_list.append(metrics)

        # Determine overall pipeline status
        if circuit_triggered or any(s.status == SafetyStatus.BLOCKED for s in step_metrics_list):
            overall_status = SafetyStatus.BLOCKED
            is_safe = False
        elif any(s.status == SafetyStatus.QUARANTINED for s in step_metrics_list):
            overall_status = SafetyStatus.QUARANTINED
            is_safe = True
        elif any(s.status == SafetyStatus.WARNING for s in step_metrics_list):
            overall_status = SafetyStatus.WARNING
            is_safe = True
        elif any(s.status == SafetyStatus.SKIPPED for s in step_metrics_list):
            overall_status = SafetyStatus.WARNING
            is_safe = True
        else:
            overall_status = SafetyStatus.PASSED
            is_safe = True

        return DryRunReport(
            pipeline_id=pipeline_id,
            status=overall_status,
            is_safe_to_execute=is_safe,
            step_metrics=step_metrics_list,
            total_input_rows=total_input_rows,
            total_output_rows=len(working_records),
            total_failed_rows=total_failed_count,
            circuit_breaker_triggered=circuit_triggered,
            violations=violations,
            quarantined_records=quarantined_records,
            clean_records=working_records,
        )

    @classmethod
    def _apply_step(
        cls,
        action_type: ActionType,
        target_columns: List[str],
        parameters: Dict[str, Any],
        records: List[Dict[str, Any]],
        metrics: StepSafetyMetrics,
    ) -> tuple[List[Dict[str, Any]], Set[int], int]:
        failed_indices: Set[int] = set()
        unexpected_nulls = 0

        for idx, row in enumerate(records):
            for col in target_columns:
                val = row.get(col)

                if action_type == ActionType.TRIM:
                    if val is not None and isinstance(val, str):
                        side = (parameters.get("side") or "both").lower()
                        if side == "left":
                            row[col] = val.lstrip()
                        elif side == "right":
                            row[col] = val.rstrip()
                        else:
                            row[col] = val.strip()

                elif action_type == ActionType.LOWERCASE:
                    if val is not None and isinstance(val, str):
                        row[col] = val.lower()

                elif action_type == ActionType.UPPERCASE:
                    if val is not None and isinstance(val, str):
                        row[col] = val.upper()

                elif action_type == ActionType.PREFIX_ADD:
                    prefix = str(parameters.get("prefix", ""))
                    only_if_missing = bool(parameters.get("only_if_missing", True))
                    if val is not None:
                        s_val = str(val)
                        if only_if_missing and s_val.startswith(prefix):
                            row[col] = s_val
                        else:
                            row[col] = prefix + s_val

                elif action_type == ActionType.SUFFIX_ADD:
                    suffix = str(parameters.get("suffix", ""))
                    only_if_missing = bool(parameters.get("only_if_missing", True))
                    if val is not None:
                        s_val = str(val)
                        if only_if_missing and s_val.endswith(suffix):
                            row[col] = s_val
                        else:
                            row[col] = s_val + suffix

                elif action_type == ActionType.PAD:
                    length = int(parameters.get("length", 0))
                    fill_char = str(parameters.get("fill_character", "0"))[:1]
                    side = str(parameters.get("side", "left")).lower()
                    if val is not None:
                        s_val = str(val)
                        if len(s_val) >= length:
                            row[col] = s_val
                        elif side == "right":
                            row[col] = s_val.ljust(length, fill_char)
                        else:
                            row[col] = s_val.rjust(length, fill_char)

                elif action_type == ActionType.PATTERN_REPLACE:
                    pat = str(parameters.get("pattern", ""))
                    repl = str(parameters.get("replacement", ""))
                    is_regex = bool(parameters.get("regex", False))
                    if val is not None:
                        s_val = str(val)
                        if is_regex:
                            row[col] = re.sub(pat, repl, s_val)
                        else:
                            row[col] = s_val.replace(pat, repl)

                elif action_type == ActionType.VALUE_MAP:
                    mappings = parameters.get("mappings") or {}
                    preserve = bool(parameters.get("preserve_unmapped", True))
                    if val is not None:
                        s_val = str(val)
                        if s_val in mappings:
                            row[col] = mappings[s_val]
                        elif not preserve:
                            row[col] = None
                            unexpected_nulls += 1

                elif action_type == ActionType.CONDITIONAL_UPDATE:
                    raw_cond = parameters.get("condition") or {}
                    new_val = parameters.get("new_value")
                    else_val = parameters.get("else_value")

                    cond_matched = False
                    if isinstance(raw_cond, dict):
                        c_col = raw_cond.get("column")
                        op = str(raw_cond.get("operator") or "EQ").upper()
                        exp_val = raw_cond.get("value")
                        curr_c_val = row.get(c_col)

                        if op == "EQ":
                            cond_matched = (str(curr_c_val) == str(exp_val))
                        elif op == "NEQ":
                            cond_matched = (str(curr_c_val) != str(exp_val))
                        elif op == "IS_NULL":
                            cond_matched = (curr_c_val is None or curr_c_val == "")
                        elif op == "IS_NOT_NULL":
                            cond_matched = (curr_c_val is not None and curr_c_val != "")
                        elif op in ("GT", "GTE", "LT", "LTE"):
                            try:
                                f_curr = float(curr_c_val)
                                f_exp = float(exp_val)
                                if op == "GT": cond_matched = (f_curr > f_exp)
                                elif op == "GTE": cond_matched = (f_curr >= f_exp)
                                elif op == "LT": cond_matched = (f_curr < f_exp)
                                elif op == "LTE": cond_matched = (f_curr <= f_exp)
                            except (ValueError, TypeError):
                                cond_matched = False

                    if cond_matched:
                        row[col] = new_val
                    elif else_val is not None:
                        row[col] = else_val

                elif action_type == ActionType.CAST_TYPE:
                    target_type = str(parameters.get("target_type", "string")).lower()
                    if val is not None:
                        try:
                            if "int" in target_type or "bigint" in target_type:
                                row[col] = int(float(str(val)))
                            elif any(t in target_type for t in ("float", "decimal", "double", "numeric")):
                                row[col] = float(str(val))
                            elif "bool" in target_type:
                                s_bool = str(val).lower().strip()
                                row[col] = s_bool in ("true", "1", "t", "yes", "y")
                            elif "date" in target_type or "datetime" in target_type:
                                dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
                                row[col] = dt.isoformat()
                            else:
                                row[col] = str(val)
                        except Exception as e:
                            # Conversion failure detected!
                            failed_indices.add(idx)
                            unexpected_nulls += 1
                            row[col] = None
                            metrics.conversion_failures.append(
                                ConversionFailure(
                                    column=col,
                                    original_value=val,
                                    target_type=target_type,
                                    row_index=idx,
                                    reason=f"Failed to cast '{val}' to {target_type}: {e}",
                                )
                            )

                elif action_type == ActionType.COERCE_NUMERIC:
                    if val is not None:
                        try:
                            row[col] = float(str(val))
                        except Exception:
                            failed_indices.add(idx)
                            unexpected_nulls += 1
                            row[col] = None
                            metrics.conversion_failures.append(
                                ConversionFailure(
                                    column=col,
                                    original_value=val,
                                    target_type="NUMERIC",
                                    row_index=idx,
                                    reason=f"Failed to coerce numeric value '{val}'",
                                )
                            )

                elif action_type == ActionType.PARSE_DATES:
                    if val is not None:
                        try:
                            s_date = str(val).strip()
                            dt = datetime.fromisoformat(s_date.replace("Z", "+00:00"))
                            # Bounds check (Azure SQL DATETIME range: 1753 to 9999)
                            if dt.year < 1753 or dt.year > 9999:
                                raise ValueError(f"Year {dt.year} outside valid range [1753, 9999]")
                            row[col] = dt.strftime("%Y-%m-%d")
                        except Exception as e:
                            failed_indices.add(idx)
                            unexpected_nulls += 1
                            row[col] = None
                            metrics.conversion_failures.append(
                                ConversionFailure(
                                    column=col,
                                    original_value=val,
                                    target_type="DATE",
                                    row_index=idx,
                                    reason=f"Unparseable date or out-of-bounds: '{val}' ({e})",
                                )
                            )

                elif action_type == ActionType.FILL_NULLS:
                    fill_val = parameters.get("value")
                    if val is None:
                        row[col] = fill_val

                elif action_type == ActionType.ZERO_TO_NULL:
                    if val in (0, "0", 0.0):
                        row[col] = None
                        unexpected_nulls += 1

                elif action_type == ActionType.RANGE_CLIP:
                    min_val = parameters.get("min_value")
                    max_val = parameters.get("max_value")
                    if val is not None:
                        try:
                            f_val = float(val)
                            if min_val is not None and f_val < float(min_val):
                                row[col] = min_val
                            elif max_val is not None and f_val > float(max_val):
                                row[col] = max_val
                        except (ValueError, TypeError):
                            pass

        # Deduplication step if applicable
        if action_type == ActionType.DEDUPLICATE:
            seen: Set[tuple] = set()
            unique_records = []
            for r_idx, row in enumerate(records):
                key = tuple(row.get(c) for c in (target_columns or list(row.keys())))
                if key in seen:
                    failed_indices.add(r_idx)
                else:
                    seen.add(key)
                    unique_records.append(row)
            return unique_records, failed_indices, unexpected_nulls

        return records, failed_indices, unexpected_nulls
