"""
GX Suite Builder for Agent Dhara.
Compiles Dhara contracts + semantic inference output → GX Expectation Suites.

Key design rules:
  - Semantic inference runs BEFORE expectation planning.
  - Country, identifier, status, category columns NEVER receive numeric rules.
  - Identifier columns are treated as strings, never cast to float/int.
  - Contract declarations take precedence over inferred semantics (confidence 1.0).
  - Suite name and hash are deterministic for reproducibility.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.profiling.gx_capability_registry import RULE_OWNERSHIP, PRIVACY_ISSUE_TYPES
from agent.runtime_component_manifest import log_component_startup

logger = logging.getLogger("agent.gx_suite_builder")

log_component_startup("GXSuiteBuilder", "3.1.0")


# ── Semantic type → allowed GX expectation types ─────────────────────────────────
#
# For each semantic type we declare which GX expectations are PERMITTED.
# Anything not in this allowlist will NOT be added to the suite, preventing
# the historical bug of numeric rules being applied to country/identifier/status columns.

_IDENTIFIER_EXPECTATIONS: Set[str] = {
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_unique",
    "expect_column_values_to_match_regex",       # for known patterns like UUID or C00001
    "expect_column_value_lengths_to_be_between",
}

_CATEGORY_EXPECTATIONS: Set[str] = {
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_in_set",
    "expect_column_value_lengths_to_be_between",
}

_DATE_EXPECTATIONS: Set[str] = {
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_dateutil_parseable",
}

_NUMERIC_EXPECTATIONS: Set[str] = {
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_between",
    "expect_column_values_to_not_be_in_set",     # sentinel values
    "expect_column_values_to_be_in_type_list",
}

_EMAIL_EXPECTATIONS: Set[str] = {
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_match_regex",
}

_PHONE_EXPECTATIONS: Set[str] = {
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_match_regex",
}

_COUNTRY_EXPECTATIONS: Set[str] = {
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_in_set",         # ISO country codes
    "expect_column_value_lengths_to_be_between",
}

_TEXT_EXPECTATIONS: Set[str] = {
    "expect_column_values_to_not_be_null",
    "expect_column_value_lengths_to_be_between",
}

_SEMANTIC_ALLOWED_EXPECTATIONS: Dict[str, Set[str]] = {
    "identifier":           _IDENTIFIER_EXPECTATIONS,
    "primary_key":          _IDENTIFIER_EXPECTATIONS,
    "foreign_key":          _IDENTIFIER_EXPECTATIONS,
    "sensitive_identifier": _IDENTIFIER_EXPECTATIONS,
    "email":                _EMAIL_EXPECTATIONS,
    "phone":                _PHONE_EXPECTATIONS,
    "date":                 _DATE_EXPECTATIONS,
    "datetime":             _DATE_EXPECTATIONS,
    "timestamp":            _DATE_EXPECTATIONS,
    "country":              _COUNTRY_EXPECTATIONS,
    "status":               _CATEGORY_EXPECTATIONS,
    "category":             _CATEGORY_EXPECTATIONS,
    "postal_code":          _TEXT_EXPECTATIONS,
    "person_name":          _TEXT_EXPECTATIONS,
    "organization":         _TEXT_EXPECTATIONS,
    "free_text":            _TEXT_EXPECTATIONS,
    "url":                  _TEXT_EXPECTATIONS,
    "ip_address":           _TEXT_EXPECTATIONS,
    "currency_amount":      _NUMERIC_EXPECTATIONS,
    "quantity":             _NUMERIC_EXPECTATIONS,
    "percentage":           _NUMERIC_EXPECTATIONS,
}


def _allowed_expectations_for_semantic(semantic_type: str) -> Set[str]:
    """Returns the set of permitted GX expectation types for a given semantic type."""
    return _SEMANTIC_ALLOWED_EXPECTATIONS.get(
        semantic_type.lower(),
        {  # default: basic structural checks only
            "expect_column_values_to_not_be_null",
            "expect_column_value_lengths_to_be_between",
        }
    )


def build_expectation_specs(
    dataset_id: str,
    df_columns: List[str],
    profile: Dict[str, Any],
    semantic_classifications: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
    business_rules: Optional[Dict[str, Any]] = None,
    contract_id: str = "DEFAULT",
) -> List[Dict[str, Any]]:
    """
    Returns a list of canonical expectation spec dicts that can be compiled into GX expectations.

    Each spec:
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "column": "customer_id",
      "kwargs": {...},
      "dhara_rule_id": "column.not_null",
      "dimension": "completeness",
      "semantic_type": "identifier",
      "semantic_confidence": 0.95,
      "source": "contract" | "inferred",
    }
    """
    thresholds = thresholds or {}
    business_rules = business_rules or {}
    specs: List[Dict[str, Any]] = []
    cols_meta = profile.get("columns", {}) or {}

    non_nullable_set: Set[str] = set()
    for c in (business_rules.get("non_nullable") or []):
        non_nullable_set.add(str(c).lower())
    for c in (business_rules.get("required_columns") or []):
        non_nullable_set.add(str(c).lower())

    # Inlined lookup maps (avoid circular import in nested closure)
    _RULE_ID_MAP: Dict[str, str] = {
        "expect_column_values_to_not_be_null":           "column.not_null",
        "expect_column_values_to_be_unique":             "column.unique",
        "expect_compound_columns_to_be_unique":          "compound_key.unique",
        "expect_column_values_to_be_in_set":             "column.allowed_values",
        "expect_column_values_to_match_regex":           "column.regex",
        "expect_column_values_to_be_between":            "column.numeric_range",
        "expect_column_value_lengths_to_be_between":     "column.length",
        "expect_table_columns_to_match_ordered_list":    "table.required_columns",
        "expect_table_columns_to_match_set":             "table.required_columns",
        "expect_table_row_count_to_be_between":          "table.row_count",
        "expect_column_values_to_be_dateutil_parseable": "column.date_parseable",
        "expect_column_values_to_be_in_type_list":       "column.type_check",
        "expect_column_values_to_not_be_in_set":         "column.sentinel_values",
    }
    _DIM_MAP: Dict[str, str] = {
        "expect_column_values_to_not_be_null":           "completeness",
        "expect_column_values_to_be_unique":             "uniqueness",
        "expect_compound_columns_to_be_unique":          "uniqueness",
        "expect_column_values_to_be_in_set":             "conformity",
        "expect_column_values_to_match_regex":           "conformity",
        "expect_column_values_to_be_between":            "validity",
        "expect_column_value_lengths_to_be_between":     "validity",
        "expect_table_columns_to_match_ordered_list":    "conformity",
        "expect_table_columns_to_match_set":             "conformity",
        "expect_table_row_count_to_be_between":          "completeness",
        "expect_column_values_to_be_dateutil_parseable": "validity",
        "expect_column_values_to_be_in_type_list":       "validity",
        "expect_column_values_to_not_be_in_set":         "validity",
    }

    for col_name in df_columns:
        col_meta = cols_meta.get(col_name, {}) or {}
        col_sem_cls = semantic_classifications.get(col_name, {}) or {}

        semantic_type = (
            col_sem_cls.get("semantic_type")
            or col_meta.get("semantic_type")
            or "unknown"
        ).lower()
        semantic_confidence = float(col_sem_cls.get("semantic_confidence", 0.5))
        semantic_source = col_sem_cls.get("semantic_source", "inferred")

        allowed = _allowed_expectations_for_semantic(semantic_type)

        is_non_null = (
            col_name.lower() in non_nullable_set
            or col_meta.get("nullable") == "NO"
            or col_meta.get("candidate_primary_key") is True
        )

        def _spec(exp_type: str, _col=col_name, _sem=semantic_type, _src=semantic_source,
                  _conf=semantic_confidence, **kwargs: Any) -> Dict[str, Any]:
            if exp_type not in allowed:
                return {}  # Blocked by semantic allowlist
            return {
                "expectation_type": exp_type,
                "column": _col,
                "kwargs": {k: v for k, v in kwargs.items() if v is not None},
                "dhara_rule_id": _RULE_ID_MAP.get(exp_type, f"gx.{exp_type}"),
                "dimension": _DIM_MAP.get(exp_type, "validity"),
                "semantic_type": _sem,
                "semantic_confidence": _conf,
                "source": _src,
                "contract_id": contract_id,
            }

        # ── Not-null expectation ──────────────────────────────────────────────
        if is_non_null or "expect_column_values_to_not_be_null" in allowed:
            s = _spec("expect_column_values_to_not_be_null")
            if s:
                specs.append(s)

        # ── Uniqueness ────────────────────────────────────────────────────────
        if col_meta.get("candidate_primary_key") is True:
            s = _spec("expect_column_values_to_be_unique")
            if s:
                specs.append(s)

        # ── Allowed values (low-cardinality) ──────────────────────────────────
        vv = (business_rules.get("valid_values") or {}).get(col_name)
        if not vv:
            unique_count = int(col_meta.get("unique_count") or 0)
            if 0 < unique_count <= 25 and semantic_type in {"status", "category", "country"}:
                # Let the existing profiling determine the set — defer to GX specialist
                pass
            elif semantic_type in {"status", "category"} and unique_count <= 30:
                pass
        if vv and isinstance(vv, list):
            s = _spec("expect_column_values_to_be_in_set", value_set=vv)
            if s:
                specs.append(s)

        # ── Regex patterns ────────────────────────────────────────────────────
        pattern = col_meta.get("llm_hints", {}).get("expected_pattern") or col_meta.get("pattern")
        if pattern and semantic_type in {"identifier", "email", "phone"}:
            s = _spec("expect_column_values_to_match_regex", regex=pattern)
            if s:
                specs.append(s)

        # ── Numeric range (only for true numeric semantic types) ───────────────
        if semantic_type in {"currency_amount", "quantity", "percentage"}:
            min_val = col_meta.get("min_value")
            max_val = col_meta.get("max_value")
            if min_val is not None or max_val is not None:
                s = _spec("expect_column_values_to_be_between",
                          min_value=min_val, max_value=max_val)
                if s:
                    specs.append(s)

        # ── Date parseability ─────────────────────────────────────────────────
        if semantic_type in {"date", "datetime", "timestamp"}:
            s = _spec("expect_column_values_to_be_dateutil_parseable")
            if s:
                specs.append(s)

    # Filter out empty specs (blocked by semantic allowlist returned {})
    return [sp for sp in specs if sp]


def suite_hash(specs: List[Dict[str, Any]]) -> str:
    """
    Deterministic SHA-256 hash of the expectation suite content.
    Identical contracts always produce the same hash.
    """
    canonical = json.dumps(
        [dict(sorted(sp.items())) for sp in specs],
        sort_keys=True, ensure_ascii=True
    )
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()


def suite_name(dataset_id: str, contract_id: str = "DEFAULT") -> str:
    """Returns the canonical GX suite name for a dataset + contract."""
    safe_ds = re.sub(r"[^a-zA-Z0-9_]", "_", dataset_id)
    safe_ct = re.sub(r"[^a-zA-Z0-9_]", "_", contract_id)
    return f"dhara_{safe_ds}_{safe_ct}"
