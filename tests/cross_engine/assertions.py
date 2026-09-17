"""
tests/cross_engine/assertions.py

Category-specific semantic comparators for cross-engine parity verification.
Rejects monolithic equality checks in favor of strict semantic category contracts.
"""
from __future__ import annotations

from collections import Counter
from decimal import Decimal
import math
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from agent.etl_pipeline.cross_engine.capability_models import NormalizedFailure
from agent.etl_pipeline.cross_engine.parity_models import (
    ComparatorResult,
    ParityComparatorType,
)
from agent.etl_pipeline.transformation_ir import DecimalCanonicalizationMode
from tests.cross_engine.normalization import (
    normalize_decimal,
    normalize_row_occurrences,
    normalize_timestamp_instant,
)


def assert_byte_parity(
    expected: bytes | str,
    actual: bytes | str,
    field_name: str = "byte_field",
) -> ComparatorResult:
    """Exact byte equality for canonical digests, HMACs, and byte-defined outputs."""
    exp_b = expected.encode("utf-8") if isinstance(expected, str) else bytes(expected)
    act_b = actual.encode("utf-8") if isinstance(actual, str) else bytes(actual)
    passed = (exp_b == act_b)
    return ComparatorResult(
        comparator_type=ParityComparatorType.EXACT_BYTE,
        passed=passed,
        field_name=field_name,
        expected=exp_b.hex(),
        actual=act_b.hex(),
        details={"match": passed, "length_match": len(exp_b) == len(act_b)},
    )


def assert_decimal_parity(
    expected: Any,
    actual: Any,
    mode: DecimalCanonicalizationMode = DecimalCanonicalizationMode.NUMERIC_VALUE,
    declared_scale: Optional[int] = None,
    field_name: str = "decimal_field",
) -> ComparatorResult:
    """Decimal parity comparing canonicalized sign, unscaled integer magnitude, and scale."""
    if expected is None and actual is None:
        return ComparatorResult(
            comparator_type=ParityComparatorType.DECIMAL,
            passed=True,
            field_name=field_name,
            expected=None,
            actual=None,
        )
    if (expected is None) != (actual is None):
        return ComparatorResult(
            comparator_type=ParityComparatorType.DECIMAL,
            passed=False,
            field_name=field_name,
            expected=str(expected),
            actual=str(actual),
            details={"error": "Nullness mismatch"},
        )

    exp_norm = normalize_decimal(expected, mode=mode, declared_scale=declared_scale)
    act_norm = normalize_decimal(actual, mode=mode, declared_scale=declared_scale)
    passed = (exp_norm == act_norm)
    return ComparatorResult(
        comparator_type=ParityComparatorType.DECIMAL,
        passed=passed,
        field_name=field_name,
        expected=exp_norm,
        actual=act_norm,
        details={"mode": mode.value, "declared_scale": declared_scale},
    )


def assert_floating_parity(
    expected: Any,
    actual: Any,
    abs_tol: float = 1e-9,
    rel_tol: float = 1e-7,
    field_name: str = "float_field",
) -> ComparatorResult:
    """Floating-point tolerance comparator handling NaN, infinities, and -0.0."""
    if expected is None and actual is None:
        return ComparatorResult(
            comparator_type=ParityComparatorType.FLOATING_TOLERANCE,
            passed=True,
            field_name=field_name,
            expected=None,
            actual=None,
        )
    if (expected is None) != (actual is None):
        return ComparatorResult(
            comparator_type=ParityComparatorType.FLOATING_TOLERANCE,
            passed=False,
            field_name=field_name,
            expected=expected,
            actual=actual,
            details={"error": "Nullness mismatch"},
        )

    exp_f = float(expected)
    act_f = float(actual)

    # Both NaN
    if math.isnan(exp_f) and math.isnan(act_f):
        return ComparatorResult(
            comparator_type=ParityComparatorType.FLOATING_TOLERANCE,
            passed=True,
            field_name=field_name,
            expected="NaN",
            actual="NaN",
        )
    if math.isnan(exp_f) or math.isnan(act_f):
        return ComparatorResult(
            comparator_type=ParityComparatorType.FLOATING_TOLERANCE,
            passed=False,
            field_name=field_name,
            expected=exp_f,
            actual=act_f,
            details={"error": "One operand is NaN"},
        )

    # Infinities
    if math.isinf(exp_f) or math.isinf(act_f):
        passed = (exp_f == act_f)
        return ComparatorResult(
            comparator_type=ParityComparatorType.FLOATING_TOLERANCE,
            passed=passed,
            field_name=field_name,
            expected=exp_f,
            actual=act_f,
        )

    # Zero sign canonicalization
    if exp_f == 0.0:
        exp_f = 0.0
    if act_f == 0.0:
        act_f = 0.0

    diff = abs(act_f - exp_f)
    passed = (diff <= abs_tol)
    if not passed and exp_f != 0.0:
        rel_diff = diff / abs(exp_f)
        passed = (rel_diff <= rel_tol)

    return ComparatorResult(
        comparator_type=ParityComparatorType.FLOATING_TOLERANCE,
        passed=passed,
        field_name=field_name,
        expected=exp_f,
        actual=act_f,
        details={"abs_diff": diff, "abs_tol": abs_tol, "rel_tol": rel_tol},
    )


def assert_temporal_parity(
    expected: Any,
    actual: Any,
    dataset_tz: Optional[str] = "UTC",
    field_name: str = "temporal_field",
) -> ComparatorResult:
    """Temporal instant comparator comparing UTC epoch nanoseconds."""
    exp_nat = expected is pd.NaT or (isinstance(expected, (pd.Timestamp, np.datetime64)) and pd.isna(expected))
    act_nat = actual is pd.NaT or (isinstance(actual, (pd.Timestamp, np.datetime64)) and pd.isna(actual))

    if exp_nat and act_nat:
        return ComparatorResult(
            comparator_type=ParityComparatorType.TEMPORAL,
            passed=True,
            field_name=field_name,
            expected="NaT",
            actual="NaT",
        )
    if exp_nat != act_nat:
        return ComparatorResult(
            comparator_type=ParityComparatorType.TEMPORAL,
            passed=False,
            field_name=field_name,
            expected="NaT" if exp_nat else str(expected),
            actual="NaT" if act_nat else str(actual),
            details={"error": "NaT domain mismatch"},
        )

    if expected is None and actual is None:
        return ComparatorResult(
            comparator_type=ParityComparatorType.TEMPORAL,
            passed=True,
            field_name=field_name,
            expected=None,
            actual=None,
        )
    if (expected is None) != (actual is None):
        return ComparatorResult(
            comparator_type=ParityComparatorType.TEMPORAL,
            passed=False,
            field_name=field_name,
            expected=str(expected),
            actual=str(actual),
            details={"error": "Nullness mismatch"},
        )

    exp_nanos = normalize_timestamp_instant(expected, dataset_tz=dataset_tz)
    act_nanos = normalize_timestamp_instant(actual, dataset_tz=dataset_tz)
    passed = (exp_nanos == act_nanos)
    return ComparatorResult(
        comparator_type=ParityComparatorType.TEMPORAL,
        passed=passed,
        field_name=field_name,
        expected=exp_nanos,
        actual=act_nanos,
        details={"dataset_tz": dataset_tz},
    )


def assert_boolean_parity(
    expected: Any,
    actual: Any,
    field_name: str = "boolean_field",
) -> ComparatorResult:
    """Three-valued boolean comparator: True, False, or None."""
    if expected is None and actual is None:
        return ComparatorResult(
            comparator_type=ParityComparatorType.BOOLEAN,
            passed=True,
            field_name=field_name,
            expected=None,
            actual=None,
        )
    if (expected is None) != (actual is None):
        return ComparatorResult(
            comparator_type=ParityComparatorType.BOOLEAN,
            passed=False,
            field_name=field_name,
            expected=expected,
            actual=actual,
            details={"error": "Nullness mismatch"},
        )
    exp_b = bool(expected)
    act_b = bool(actual)
    passed = (exp_b == act_b)
    return ComparatorResult(
        comparator_type=ParityComparatorType.BOOLEAN,
        passed=passed,
        field_name=field_name,
        expected=exp_b,
        actual=act_b,
    )


def assert_schema_parity(
    expected_columns: List[str] | Set[str] | Dict[str, Any],
    actual_columns: List[str] | Set[str] | Dict[str, Any],
    ordered: bool = False,
    field_name: str = "dataset_schema",
) -> ComparatorResult:
    """Compare dataset schemas without relying on incidental column order unless declared."""
    exp_cols = list(expected_columns)
    act_cols = list(actual_columns)

    if ordered:
        passed = (exp_cols == act_cols)
    else:
        passed = (set(exp_cols) == set(act_cols) and len(exp_cols) == len(act_cols))

    missing = set(exp_cols) - set(act_cols)
    unexpected = set(act_cols) - set(exp_cols)

    return ComparatorResult(
        comparator_type=ParityComparatorType.SCHEMA,
        passed=passed,
        field_name=field_name,
        expected=exp_cols,
        actual=act_cols,
        details={"missing_columns": list(missing), "unexpected_columns": list(unexpected)},
    )


def assert_row_occurrence_parity(
    input_ids: Iterable[Any],
    primary_ids: Iterable[Any],
    secondary_ids: Optional[Iterable[Any]] = None,
    mode: str = "partition_quarantine",
    field_name: str = "row_occurrences",
) -> ComparatorResult:
    """
    Mode-specific row occurrence multiset conservation comparator:
    - partition_quarantine: Counter(curated) + Counter(quarantine) == Counter(input), and disjoint
    - copy_quarantine: Counter(primary) == Counter(input) and Counter(quarantine) <= Counter(input)
    - filter_with_dropped: Counter(retained) + Counter(dropped) == Counter(input)
    - filter_without_dropped: Counter(retained) <= Counter(input)
    - deduplicate_with_removed: Counter(survivor) + Counter(removed) == Counter(input)
    """
    in_cnt = normalize_row_occurrences(input_ids)
    prim_cnt = normalize_row_occurrences(primary_ids)
    sec_cnt = normalize_row_occurrences(secondary_ids or ())

    passed = False
    details: Dict[str, Any] = {"mode": mode, "total_input_count": sum(in_cnt.values())}

    if mode == "partition_quarantine":
        combined = prim_cnt + sec_cnt
        overlap = set(prim_cnt.keys()) & set(sec_cnt.keys())
        passed = (combined == in_cnt and len(overlap) == 0)
        details["combined_equals_input"] = (combined == in_cnt)
        details["is_disjoint"] = (len(overlap) == 0)
        details["overlap_count"] = len(overlap)
    elif mode == "copy_quarantine":
        primary_match = (prim_cnt == in_cnt)
        is_submultiset = all(sec_cnt[k] <= in_cnt[k] for k in sec_cnt)
        passed = primary_match and is_submultiset
        details["primary_equals_input"] = primary_match
        details["quarantine_is_submultiset"] = is_submultiset
    elif mode == "filter_with_dropped":
        combined = prim_cnt + sec_cnt
        passed = (combined == in_cnt)
        details["retained_plus_dropped_equals_input"] = passed
    elif mode == "filter_without_dropped":
        is_submultiset = all(prim_cnt[k] <= in_cnt[k] for k in prim_cnt)
        passed = is_submultiset
        details["retained_is_submultiset"] = is_submultiset
    elif mode == "deduplicate_with_removed":
        combined = prim_cnt + sec_cnt
        passed = (combined == in_cnt)
        details["survivor_plus_removed_equals_input"] = passed
    else:
        raise ValueError(f"Unknown routing occurrence mode: '{mode}'")

    return ComparatorResult(
        comparator_type=ParityComparatorType.ROW_OCCURRENCE,
        passed=passed,
        field_name=field_name,
        expected=dict(in_cnt),
        actual={"primary": dict(prim_cnt), "secondary": dict(sec_cnt)},
        details=details,
    )


def assert_validation_metrics_parity(
    expected: Dict[str, int],
    actual: Dict[str, int],
    field_name: str = "validation_metrics",
) -> ComparatorResult:
    """Compare exact validation metric counts (candidate, pass, violation, raw unknown, effective)."""
    keys = ("candidate_count", "pass_count", "violation_count", "raw_unknown_count", "effective_violation_count")
    passed = True
    mismatches: Dict[str, Any] = {}
    for k in keys:
        exp_v = expected.get(k, 0)
        act_v = actual.get(k, 0)
        if exp_v != act_v:
            passed = False
            mismatches[k] = {"expected": exp_v, "actual": act_v}

    return ComparatorResult(
        comparator_type=ParityComparatorType.VALIDATION_METRIC,
        passed=passed,
        field_name=field_name,
        expected=expected,
        actual=actual,
        details={"mismatches": mismatches},
    )


def assert_failure_parity(
    expected: NormalizedFailure,
    actual: NormalizedFailure,
    field_name: str = "normalized_failure",
) -> ComparatorResult:
    """Compare typed normalized failure envelopes across engines."""
    category_match = (expected.category == actual.category)
    reason_match = (expected.reason_code == actual.reason_code)
    family_match = (expected.family == actual.family)
    retry_match = (expected.retryable == actual.retryable)

    passed = (category_match and reason_match and family_match and retry_match)
    return ComparatorResult(
        comparator_type=ParityComparatorType.FAILURE,
        passed=passed,
        field_name=field_name,
        expected=expected.model_dump(),
        actual=actual.model_dump(),
        details={
            "category_match": category_match,
            "reason_match": reason_match,
            "family_match": family_match,
            "retry_match": retry_match,
        },
    )
