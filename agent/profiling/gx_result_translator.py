"""
GX Result Translator for Agent Dhara Engine 3.x.
Translates raw Great Expectations validation results into Dhara's canonical
7-state RuleEvaluationResult model.

Seven states:
  PASS                 — expectation executed and succeeded
  FAIL                 — expectation executed and failed
  NOT_APPLICABLE       — rule does not apply to resolved semantic type
  NOT_EVALUATED        — batch or dataset unavailable
  CONFIGURATION_ERROR  — invalid suite or expectation configuration
  EXECUTION_ERROR      — GX runtime exception
  INSUFFICIENT_EVIDENCE — semantic confidence below threshold for rule planning
"""
from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Dict, List, Optional

from agent.profiling.rule_status import RuleStatus, RuleEvaluationResult
from agent.profiling.gx_capability_registry import (
    canonical_rule_id_for_issue,
    ISSUE_TYPE_TO_RULE_ID,
    is_privacy_issue,
)
from agent.runtime_component_manifest import log_component_startup

logger = logging.getLogger("agent.gx_result_translator")

log_component_startup("GXResultTranslator", "3.1.0")


# ── GX expectation_type → canonical Dhara rule_id ─────────────────────────────
# ── GX expectation_type → canonical Dhara rule_id ─────────────────────────────
GX_EXPECTATION_TO_RULE_ID: Dict[str, str] = {
    "expect_column_values_to_not_be_null":                "column.not_null",
    "column_values_to_not_be_null":                       "column.not_null",
    "column values to not be null":                       "column.not_null",
    "expect_column_values_to_be_unique":                  "column.unique",
    "column_values_to_be_unique":                         "column.unique",
    "column values to be unique":                         "column.unique",
    "expect_compound_columns_to_be_unique":               "compound_key.unique",
    "compound_columns_to_be_unique":                      "compound_key.unique",
    "compound columns to be unique":                      "compound_key.unique",
    "expect_column_values_to_be_in_set":                  "column.allowed_values",
    "column_values_to_be_in_set":                         "column.allowed_values",
    "column values to be in set":                         "column.allowed_values",
    "expect_column_values_to_match_regex":                "column.regex",
    "column_values_to_match_regex":                       "column.regex",
    "column values to match regex":                       "column.regex",
    "expect_column_values_to_be_between":                 "column.numeric_range",
    "column_values_to_be_between":                        "column.numeric_range",
    "column values to be between":                        "column.numeric_range",
    "expect_column_value_lengths_to_be_between":          "column.length",
    "column_value_lengths_to_be_between":                 "column.length",
    "column value lengths to be between":                 "column.length",
    "expect_table_columns_to_match_ordered_list":         "table.required_columns",
    "table_columns_to_match_ordered_list":                "table.required_columns",
    "table columns to match ordered list":                "table.required_columns",
    "expect_table_columns_to_match_set":                  "table.required_columns",
    "table_columns_to_match_set":                         "table.required_columns",
    "expect_table_row_count_to_be_between":               "table.row_count",
    "table_row_count_to_be_between":                      "table.row_count",
    "table row count to be between":                      "table.row_count",
    "expect_column_values_to_be_dateutil_parseable":      "column.date_parseable",
    "column_values_to_be_dateutil_parseable":             "column.date_parseable",
    "column values to be dateutil parseable":              "column.date_parseable",
    "expect_column_values_to_be_in_type_list":            "column.type_check",
    "column_values_to_be_in_type_list":                   "column.type_check",
    "column values to be in type list":                   "column.type_check",
    "expect_column_values_to_not_be_in_set":              "column.sentinel_values",
    "column_values_to_not_be_in_set":                     "column.sentinel_values",
    "column values to not be in set":                     "column.sentinel_values",
    "invalid_email":                                      "column.regex",
    "invalid_phone":                                      "column.regex",
    "invalid_uuid":                                       "column.regex",
    "negative_values":                                    "column.numeric_range",
    "sentinel_numeric_value":                             "column.sentinel_values",
    "suspicious_zero":                                    "column.sentinel_values",
    "invalid_date_format":                                "column.date_parseable",
    "future_dates":                                       "column.date_range",
    "ancient_dates":                                      "column.date_range",
}

# ── GX expectation_type → DQ dimension ───────────────────────────────────────
GX_EXPECTATION_TO_DIMENSION: Dict[str, str] = {
    "expect_column_values_to_not_be_null":           "completeness",
    "column_values_to_not_be_null":                  "completeness",
    "column values to not be null":                  "completeness",
    "expect_column_values_to_be_unique":             "uniqueness",
    "column_values_to_be_unique":                    "uniqueness",
    "column values to be unique":                    "uniqueness",
    "expect_compound_columns_to_be_unique":          "uniqueness",
    "compound_columns_to_be_unique":                 "uniqueness",
    "compound columns to be unique":                 "uniqueness",
    "expect_column_values_to_be_in_set":             "conformity",
    "column_values_to_be_in_set":                    "conformity",
    "column values to be in set":                    "conformity",
    "expect_column_values_to_match_regex":           "conformity",
    "column_values_to_match_regex":                  "conformity",
    "column values to match regex":                  "conformity",
    "expect_column_values_to_be_between":            "validity",
    "column_values_to_be_between":                   "validity",
    "column values to be between":                   "validity",
    "expect_column_value_lengths_to_be_between":     "validity",
    "column_value_lengths_to_be_between":            "validity",
    "column value lengths to be between":            "validity",
    "expect_table_columns_to_match_ordered_list":    "conformity",
    "table_columns_to_match_ordered_list":           "conformity",
    "table columns to match ordered list":           "conformity",
    "expect_table_columns_to_match_set":             "conformity",
    "table_columns_to_match_set":                    "conformity",
    "expect_table_row_count_to_be_between":          "completeness",
    "table_row_count_to_be_between":                 "completeness",
    "table row count to be between":                 "completeness",
    "expect_column_values_to_be_dateutil_parseable": "validity",
    "column_values_to_be_dateutil_parseable":        "validity",
    "column values to be dateutil parseable":         "validity",
    "expect_column_values_to_be_in_type_list":       "validity",
    "column_values_to_be_in_type_list":              "validity",
    "column values to be in type list":              "validity",
    "expect_column_values_to_not_be_in_set":         "accuracy",
    "column_values_to_not_be_in_set":                "accuracy",
    "column values to not be in set":                "accuracy",
    "invalid_email":                                 "conformity",
    "invalid_phone":                                 "conformity",
    "invalid_uuid":                                  "conformity",
    "negative_values":                               "validity",
    "sentinel_numeric_value":                        "accuracy",
    "suspicious_zero":                               "accuracy",
    "invalid_date_format":                           "validity",
    "future_dates":                                  "timeliness",
    "ancient_dates":                                 "timeliness",
}


def _extract_gx_kwargs(expectation_config: Any) -> Dict[str, Any]:
    """Safely extract kwargs from a GX ExpectationConfiguration (dict or object)."""
    if expectation_config is None:
        return {}
    if isinstance(expectation_config, dict):
        return expectation_config.get("kwargs", {}) or {}
    # GX 1.x object
    kwargs = getattr(expectation_config, "kwargs", {})
    if isinstance(kwargs, dict):
        return kwargs
    try:
        return dict(kwargs)
    except Exception:
        return {}


def _extract_gx_exp_type(expectation_config: Any) -> str:
    """Safely extract expectation_type from a GX ExpectationConfiguration."""
    if expectation_config is None:
        return "unknown"
    if isinstance(expectation_config, dict):
        return expectation_config.get("expectation_type") or expectation_config.get("type") or "unknown"
    return getattr(expectation_config, "expectation_type", None) or getattr(expectation_config, "type", "unknown")


def _execution_id(dataset_id: str, col: str, rule_id: str) -> str:
    raw = f"{dataset_id}::{col}::{rule_id}::{time.time_ns()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def translate_gx_result_list(
    gx_result_list: List[Any],
    dataset_id: str,
    *,
    execution_metadata: Optional[Dict[str, Any]] = None,
    gx_provider_version: str = "unknown",
    semantic_classifications: Optional[Dict[str, Any]] = None,
    max_samples: int = 25,
    max_row_locators: int = 1000,
    n_rows: Optional[int] = None,
) -> List[RuleEvaluationResult]:
    """
    Converts a raw list of GX result objects (from validator.validate()) into
    a list of canonical RuleEvaluationResult, applying the 7-state mapping.

    Invariants enforced:
    - A missing/None GX result NEVER becomes PASS.
    - Every result has exactly one of the 7 explicit states.
    - GX exceptions → EXECUTION_ERROR (never silently dropped).
    - Config problems → CONFIGURATION_ERROR (never PASS).
    """
    translated: List[RuleEvaluationResult] = []
    exec_meta = execution_metadata or {}
    sem = semantic_classifications or {}

    # ── Guard: batch unavailable → all NOT_EVALUATED ──────────────────────────
    if exec_meta.get("batch_unavailable"):
        logger.warning("[DQ3] GX batch unavailable for dataset=%s → NOT_EVALUATED", dataset_id)
        return [
            RuleEvaluationResult(
                rule_id="gx.batch",
                status=RuleStatus.NOT_EVALUATED,
                dataset=dataset_id,
                column=None,
                dimension="validity",
                severity="HIGH",
                confidence=1.0,
                error_message="GX batch or data asset was unavailable for this dataset.",
                evidence={"execution_metadata": exec_meta},
            )
        ]

    # ── Guard: GX initialization error → EXECUTION_ERROR ─────────────────────
    if exec_meta.get("gx_init_error"):
        logger.error("[DQ3] GX initialization error for dataset=%s: %s",
                     dataset_id, exec_meta.get("gx_init_error"))
        return [
            RuleEvaluationResult(
                rule_id="gx.context",
                status=RuleStatus.EXECUTION_ERROR,
                dataset=dataset_id,
                column=None,
                dimension="validity",
                severity="HIGH",
                confidence=1.0,
                error_message=str(exec_meta.get("gx_init_error")),
                evidence={"execution_metadata": exec_meta},
            )
        ]

    for r in gx_result_list:
        exp_cfg = getattr(r, "expectation_config", None) if not isinstance(r, dict) else r.get("expectation_config")
        if exp_cfg:
            kwargs = _extract_gx_kwargs(exp_cfg)
            exp_type = _extract_gx_exp_type(exp_cfg)
            col = kwargs.get("column") or "-"
            if col == "-" and "column_list" in kwargs:
                col = ",".join(str(c) for c in kwargs["column_list"])
        else:
            exp_type = r.get("expectation_type") or r.get("expectation") or "unknown"
            col = r.get("column") or "-"
            kwargs = {"column": col}

        rule_id = GX_EXPECTATION_TO_RULE_ID.get(exp_type, f"gx.{exp_type}")
        dimension = GX_EXPECTATION_TO_DIMENSION.get(exp_type, "validity")

        # ── Extract result details ────────────────────────────────────────────
        if isinstance(r, dict):
            success = r.get("success")
            res_dict = r.get("result", {}) or {}
            if not res_dict:
                res_dict = {
                    "unexpected_count": r.get("unexpected_count", 0),
                    "element_count": r.get("element_count", 0),
                    "unexpected_index_list": r.get("unexpected_index_list", []),
                    "unexpected_list": r.get("unexpected_values", []),
                }
        else:
            success = getattr(r, "success", None)
            res_obj = getattr(r, "result", None)
            res_dict = dict(res_obj) if res_obj else {}

        # ── Map to 7 states ───────────────────────────────────────────────────
        if success is None:
            # GX returned a result object but success is unknown → config error
            status = RuleStatus.CONFIGURATION_ERROR
            error_msg = f"GX expectation {exp_type!r} returned success=None. Check expectation configuration."
            logger.warning("[DQ3] %s for dataset=%s col=%s", error_msg, dataset_id, col)
        elif isinstance(success, Exception):
            status = RuleStatus.EXECUTION_ERROR
            error_msg = str(success)
        elif success is True:
            status = RuleStatus.PASS
            error_msg = None
        else:
            status = RuleStatus.FAIL
            error_msg = None

        # ── Semantic applicability guard ──────────────────────────────────────
        col_semantic = (sem.get(col) or {}).get("semantic_type", "") if col != "-" else ""
        not_applicable = _is_rule_not_applicable(rule_id, col_semantic, exp_type, kwargs)
        if not_applicable and status == RuleStatus.FAIL:
            # Wrong rule was planned for this semantic type → NOT_APPLICABLE, not FAIL
            status = RuleStatus.NOT_APPLICABLE
            error_msg = (
                f"Rule {rule_id!r} is not applicable to semantic type {col_semantic!r} "
                f"on column {col!r}. Rule planning error — semantic-aware planning should "
                f"have excluded this expectation."
            )
            logger.warning("[DQ3] NOT_APPLICABLE: %s", error_msg)

        # ── Extract counts and evidence ───────────────────────────────────────
        unexp_count = int(res_dict.get("unexpected_count", 0) or 0)
        eval_count = int(res_dict.get("element_count", 0) or 0)

        # Invariant guard: an evaluated check must have eval_count >= unexp_count
        if eval_count == 0 and n_rows and n_rows > 0:
            eval_count = n_rows
        if eval_count < unexp_count:
            eval_count = max(eval_count, unexp_count)
        if eval_count == 0 and status in (RuleStatus.FAIL, RuleStatus.PASS):
            eval_count = max(1, unexp_count)

        unexp_count = min(unexp_count, eval_count)
        failure_rate = round(unexp_count / max(1, eval_count), 4) if eval_count else 0.0

        raw_rows = res_dict.get("unexpected_index_list") or []
        raw_samples = res_dict.get("unexpected_list") or []

        # Bound evidence to configured limits
        row_locators = [int(i) for i in (raw_rows or [])[:max_row_locators]]
        samples = [str(v) for v in (raw_samples or [])[:max_samples]]

        # ── Severity heuristic ────────────────────────────────────────────────
        severity = _rule_severity(rule_id, col, failure_rate)

        result = RuleEvaluationResult(
            rule_id=rule_id,
            rule_version="3.1.0",
            status=status,
            dataset=dataset_id,
            column=col if col != "-" else None,
            dimension=dimension,
            severity=severity,
            confidence=1.0,
            evaluated_count=eval_count,
            failed_count=unexp_count,
            failure_rate=failure_rate,
            affected_rows=row_locators,
            samples=samples,
            error_message=error_msg,
            evidence={
                "gx_expectation_type": exp_type,
                "gx_kwargs": {k: v for k, v in kwargs.items() if k not in ("column", "column_list")},
                "gx_provider_version": gx_provider_version,
                "execution_metadata": exec_meta,
            },
        )
        translated.append(result)

    return translated


def _is_rule_not_applicable(
    rule_id: str,
    semantic_type: str,
    exp_type: str,
    kwargs: Dict[str, Any],
) -> bool:
    """
    Returns True when a GX rule should not have been applied to this semantic type.
    This guards against the primary historical bug: numeric rules applied to identifiers and countries.
    """
    if not semantic_type:
        return False

    sem = semantic_type.lower()

    # Numeric range rules must not apply to identifiers, categories, status, or country columns
    if rule_id == "column.numeric_range":
        if sem in {"identifier", "country", "status", "category", "postal_code",
                   "free_text", "person_name", "organization", "email", "phone",
                   "url", "ip_address", "sensitive_identifier"}:
            return True
        # Also catch the GX expect_column_values_to_be_between used for sentinel checks on ID columns
        if sem == "identifier" and exp_type == "expect_column_values_to_be_between":
            return True

    # Type-list checks on string-semantic columns that happen to have int dtype
    if rule_id == "column.type_check":
        if sem in {"identifier", "postal_code", "phone"}:
            return True

    # Date parseability must not apply to non-date columns
    if rule_id == "column.date_parseable":
        if sem not in {"date", "datetime", "timestamp", "unknown", ""}:
            return True

    return False


def _rule_severity(rule_id: str, column: str, failure_rate: float) -> str:
    """Derive canonical severity from rule type and failure rate."""
    if rule_id in ("column.not_null", "column.unique", "compound_key.unique",
                   "table.required_columns"):
        return "HIGH" if failure_rate >= 0.05 else "MEDIUM"
    if rule_id in ("column.allowed_values", "column.regex"):
        return "HIGH" if failure_rate >= 0.20 else "MEDIUM"
    if rule_id in ("column.numeric_range",):
        return "HIGH" if failure_rate >= 0.10 else "MEDIUM"
    return "LOW"
