"""
Validation Capability Registry for Agent Dhara.
Declares which rules are owned by which provider (GX vs native vs privacy pipeline).
Prevents double-execution and double-scoring of the same defect.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("agent.capability_registry")

# ── Canonical rule ownership table ───────────────────────────────────────────────
#
# Format:
#   rule_id: {
#       "primary":   "great_expectations" | "dhara_native" | "privacy_engine",
#       "fallback":  "dhara_native" | None,
#       "backends":  list of supported backends for primary provider
#       "score_family": "quality" | "privacy" | "governance" | None (= no score contribution)
#   }
#
# Provider names must NOT be embedded in finding identity keys —
# the rule_id + dataset_id + column uniquely identifies a defect regardless of who found it.

RULE_OWNERSHIP: Dict[str, Dict[str, Any]] = {
    # ── GX-primary: deterministic column rules ───────────────────────────────
    "column.not_null": {
        "primary":      "great_expectations",
        "fallback":     "dhara_native",
        "backends":     ["pandas", "spark", "sql"],
        "score_family": "quality",
        "dimension":    "completeness",
    },
    "column.unique": {
        "primary":      "great_expectations",
        "fallback":     "dhara_native",
        "backends":     ["pandas", "spark", "sql"],
        "score_family": "quality",
        "dimension":    "uniqueness",
    },
    "compound_key.unique": {
        "primary":      "great_expectations",
        "fallback":     "dhara_native",
        "backends":     ["pandas", "spark", "sql"],
        "score_family": "quality",
        "dimension":    "uniqueness",
    },
    "column.allowed_values": {
        "primary":      "great_expectations",
        "fallback":     "dhara_native",
        "backends":     ["pandas", "spark", "sql"],
        "score_family": "quality",
        "dimension":    "conformity",
    },
    "column.regex": {
        "primary":      "great_expectations",
        "fallback":     "dhara_native",
        "backends":     ["pandas", "spark", "sql"],
        "score_family": "quality",
        "dimension":    "conformity",
    },
    "column.numeric_range": {
        "primary":      "great_expectations",
        "fallback":     "dhara_native",
        "backends":     ["pandas", "spark", "sql"],
        "score_family": "quality",
        "dimension":    "validity",
    },
    "column.type_numeric": {
        "primary":      "great_expectations",
        "fallback":     "dhara_native",
        "backends":     ["pandas", "spark", "sql"],
        "score_family": "quality",
        "dimension":    "validity",
    },
    "column.type_cast": {
        "primary":      "dhara_native",
        "fallback":     "great_expectations",
        "backends":     ["pandas", "spark", "sql"],
        "score_family": "quality",
        "dimension":    "conformity",
    },
    "column.length": {
        "primary":      "great_expectations",
        "fallback":     "dhara_native",
        "backends":     ["pandas", "spark", "sql"],
        "score_family": "quality",
        "dimension":    "validity",
    },
    "table.required_columns": {
        "primary":      "great_expectations",
        "fallback":     "dhara_native",
        "backends":     ["pandas", "spark", "sql"],
        "score_family": "quality",
        "dimension":    "conformity",
    },
    "table.row_count": {
        "primary":      "great_expectations",
        "fallback":     "dhara_native",
        "backends":     ["pandas", "spark", "sql"],
        "score_family": "quality",
        "dimension":    "completeness",
    },
    "column.date_parseable": {
        "primary":      "great_expectations",
        "fallback":     "dhara_native",
        "backends":     ["pandas"],
        "score_family": "quality",
        "dimension":    "validity",
    },
    # ── Dhara-native: semantic and contextual rules ──────────────────────────
    "semantic.inference": {
        "primary":      "dhara_native",
        "fallback":     None,
        "backends":     ["pandas", "spark"],
        "score_family": None,        # classification, not a scored finding
        "dimension":    None,
    },
    "key.normalized_collision": {
        "primary":      "dhara_native",
        "fallback":     None,
        "backends":     ["pandas", "spark"],
        "score_family": "quality",
        "dimension":    "uniqueness",
    },
    "key.candidate_discovery": {
        "primary":      "dhara_native",
        "fallback":     None,
        "backends":     ["pandas", "spark"],
        "score_family": None,
        "dimension":    None,
    },
    "relationship.orphan": {
        "primary":      "dhara_native",
        "fallback":     None,
        "backends":     ["pandas", "spark"],
        "score_family": "quality",
        "dimension":    "referential_integrity",
    },
    "relationship.direction": {
        "primary":      "dhara_native",
        "fallback":     None,
        "backends":     ["pandas", "spark"],
        "score_family": None,
        "dimension":    None,
    },
    "duplicate.near_match": {
        "primary":      "dhara_native",
        "fallback":     None,
        "backends":     ["pandas"],
        "score_family": "quality",
        "dimension":    "uniqueness",
    },
    "placeholder.contextual": {
        "primary":      "dhara_native",
        "fallback":     None,
        "backends":     ["pandas", "spark"],
        "score_family": "quality",
        "dimension":    "completeness",
    },
    "unicode.mixed_script": {
        "primary":      "dhara_native",
        "fallback":     None,
        "backends":     ["pandas"],
        "score_family": "quality",
        "dimension":    "validity",
    },
    # ── Privacy pipeline: separate score family ──────────────────────────────
    "privacy.email": {
        "primary":      "privacy_engine",
        "fallback":     None,
        "backends":     ["pandas", "spark"],
        "score_family": "privacy",
        "dimension":    None,
    },
    "privacy.phone": {
        "primary":      "privacy_engine",
        "fallback":     None,
        "backends":     ["pandas", "spark"],
        "score_family": "privacy",
        "dimension":    None,
    },
    "privacy.pii_sensitive": {
        "primary":      "privacy_engine",
        "fallback":     None,
        "backends":     ["pandas", "spark"],
        "score_family": "privacy",
        "dimension":    None,
    },
    "privacy.classification": {
        "primary":      "privacy_engine",
        "fallback":     None,
        "backends":     ["pandas", "spark"],
        "score_family": "privacy",
        "dimension":    None,
    },
}

# ── Issue-type → canonical rule_id mapper ────────────────────────────────────────
# Maps legacy dq_checks issue types to their canonical rule IDs for deduplication.
ISSUE_TYPE_TO_RULE_ID: Dict[str, str] = {
    "nulls":                           "column.not_null",
    "empty_string_values":             "column.not_null",
    "declared_key_null_violation":     "column.not_null",
    "declared_key_high_null_ratio":    "column.not_null",
    "suspected_key_high_null_ratio":   "column.not_null",
    "exact_duplicate_rows":            "compound_key.unique",
    "declared_key_violation":          "column.unique",
    "candidate_key_violation":         "column.unique",
    "duplicate_primary_key":           "column.unique",
    "invalid_email":                   "column.regex",
    "invalid_phone":                   "column.regex",
    "invalid_uuid":                    "column.regex",
    "invalid_url":                     "column.regex",
    "invalid_numeric":                 "column.type_numeric",
    "invalid_numeric_values":          "column.type_numeric",
    "numeric_parse_failure":           "column.type_numeric",
    "negative_values":                 "column.numeric_range",
    "negative_values_in_non_negative_measure": "column.numeric_range",
    "negative_values_review_required": "column.numeric_range",
    "out_of_range":                    "column.numeric_range",
    "custom_one_of":                   "column.allowed_values",
    "invalid_lookup_value":            "column.allowed_values",
    "custom_regex":                    "column.regex",
    "invalid_date_format":             "column.date_parseable",
    "placeholder_detected":            "placeholder.contextual",
    "near_duplicate_rows":             "duplicate.near_match",
    "orphan_foreign_key_rows":         "relationship.orphan",
    "intra_dataset_orphan_fk":         "relationship.orphan",
    "pii_email":                       "privacy.email",
    "pii_phone":                       "privacy.phone",
    "pii_sensitive":                   "privacy.pii_sensitive",
    "integer_stored_as_float":         "column.type_cast",
}

# Issue types that belong to the PRIVACY pipeline and must NOT contribute to DQ scores
PRIVACY_ISSUE_TYPES: frozenset = frozenset({
    "pii_email", "pii_phone", "pii_sensitive", "pii_credit_card",
    "pii_iban", "pii_national_id", "pii_ip_address", "pii_passport",
    "pii_ssn", "pii_dob",
})


def resolve_provider(rule_id: str, backend: str = "pandas") -> Dict[str, Any]:
    """
    Returns the ownership record for a rule_id, respecting backend support.
    If GX does not support the backend, returns dhara_native as effective primary.
    """
    ownership = RULE_OWNERSHIP.get(rule_id)
    if not ownership:
        return {
            "primary": "dhara_native",
            "fallback": None,
            "backends": ["pandas", "spark"],
            "score_family": "quality",
            "dimension": "validity",
        }

    effective_primary = ownership["primary"]
    if effective_primary == "great_expectations" and backend not in ownership.get("backends", []):
        effective_primary = ownership.get("fallback") or "dhara_native"

    return {**ownership, "effective_primary": effective_primary}


def is_privacy_issue(issue_type: str) -> bool:
    """Returns True if the issue type belongs to the privacy pipeline, not DQ."""
    return issue_type in PRIVACY_ISSUE_TYPES


def canonical_rule_id_for_issue(issue_type: str) -> str:
    """Maps legacy issue type strings to their canonical rule_id."""
    return ISSUE_TYPE_TO_RULE_ID.get(issue_type, f"dhara_native.{issue_type}")
