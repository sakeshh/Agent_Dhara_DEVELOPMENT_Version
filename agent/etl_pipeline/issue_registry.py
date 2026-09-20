from __future__ import annotations
from typing import Any, Dict

ISSUE_TYPE_ALIASES: Dict[str, str] = {
    # Referential integrity variants
    "orphan_foreign_key": "orphan_foreign_key_rows",
    "orphan_foreign_keys": "orphan_foreign_key_rows",
    "referential_integrity_violation": "orphan_foreign_key_rows",
    # Null variants
    "missing_values": "nulls",
    "null_values": "nulls",
    "high_null_percentage": "suspected_key_high_null_ratio",
    "high_null_ratio": "suspected_key_high_null_ratio",
    # Duplicate variants
    "duplicate_rows": "exact_duplicate_rows",
    "near_duplicates": "near_duplicate_rows",
    "duplicate_primary_key": "business_key_duplicate",
    # Numeric variants — all map to invalid_numeric canonical type
    "invalid_numeric_values": "invalid_numeric",
    "invalid_number": "invalid_numeric",
    "non_numeric_value": "invalid_numeric",
    "numeric_serialized_as_string": "numeric_value_serialized_as_string",
    # Negative variants
    "negative_values": "cross_field_non_negative",
    "negative_values_in_measure": "negative_values_in_non_negative_measure",
    # Declared key variants
    "declared_key_null": "declared_key_null_violation",
    "key_null_violation": "declared_key_null_violation",
    # Proactive hygiene variants
    "proactive_trim": "whitespace",
    "proactive_lowercase": "case_inconsistency",
}

ISSUE_REGISTRY: Dict[str, Dict[str, str]] = {
    "placeholder_detected": {
        "canonical_type": "placeholder_detected",
        "policy_group": "POLICY_PLACEHOLDER",
        "phase1_action": "normalize_registered_placeholder",
    },
    "exact_duplicate_rows": {
        "canonical_type": "exact_duplicate_rows",
        "policy_group": "POLICY_EXACT_DUPLICATE",
        "phase1_action": "flag_duplicate_candidates",
    },
    "near_duplicate_rows": {
        "canonical_type": "near_duplicate_rows",
        "policy_group": "POLICY_NEAR_DUPLICATE",
        "phase1_action": "flag_near_duplicate_cluster",
    },
    "duplicate_primary_key": {
        "canonical_type": "duplicate_primary_key",
        "policy_group": "POLICY_BUSINESS_KEY_DUPLICATE",
        "phase1_action": "flag_duplicate_candidates",
    },
    "business_key_duplicate": {
        "canonical_type": "business_key_duplicate",
        "policy_group": "POLICY_BUSINESS_KEY_DUPLICATE",
        "phase1_action": "flag_duplicate_candidates",
    },
    "declared_key_null_violation": {
        "canonical_type": "declared_key_null_violation",
        "policy_group": "POLICY_DECLARED_KEY_NULL",
        "phase1_action": "flag_missing_declared_key",
    },
    "declared_key_high_null_ratio": {
        "canonical_type": "declared_key_high_null_ratio",
        "policy_group": "POLICY_DECLARED_KEY_NULL",
        "phase1_action": "flag_missing_declared_key",
    },
    "suspected_key_high_null_ratio": {
        "canonical_type": "suspected_key_high_null_ratio",
        "policy_group": "POLICY_SUSPECTED_KEY_NULL",
        "phase1_action": "flag_missing_key",
    },
    "orphan_foreign_key_rows": {
        "canonical_type": "orphan_foreign_key_rows",
        "policy_group": "POLICY_REFERENTIAL_INTEGRITY",
        "phase1_action": "validate_and_partition_relationship",
    },
    "numeric_value_serialized_as_string": {
        "canonical_type": "numeric_value_serialized_as_string",
        "policy_group": "POLICY_NUMERIC_SERIALIZATION",
        "phase1_action": "coerce_numeric",
    },
    # invalid_numeric = values that cannot be parsed as the declared numeric type
    # (e.g. 'N/A', 'error', '' in an amount column). Phase 1 action: coerce with
    # preserve_raw so the original string is kept for audit.
    "invalid_numeric": {
        "canonical_type": "invalid_numeric",
        "policy_group": "POLICY_NUMERIC_PARSE",
        "phase1_action": "coerce_numeric",
    },
    "cross_field_non_negative": {
        "canonical_type": "cross_field_non_negative",
        "policy_group": "POLICY_NON_NEGATIVE",
        "phase1_action": "flag_cross_field_violation",
    },
    "negative_values_in_non_negative_measure": {
        "canonical_type": "negative_values_in_non_negative_measure",
        "policy_group": "POLICY_NON_NEGATIVE",
        "phase1_action": "flag_cross_field_violation",
    },
    "negative_values_review_required": {
        "canonical_type": "negative_values_review_required",
        "policy_group": "POLICY_NON_NEGATIVE",
        "phase1_action": "flag_cross_field_violation",
    },
    "declared_non_negative_rule_violation": {
        "canonical_type": "declared_non_negative_rule_violation",
        "policy_group": "POLICY_NON_NEGATIVE",
        "phase1_action": "flag_cross_field_violation",
    },
    "nulls": {
        "canonical_type": "nulls",
        "policy_group": "POLICY_NULL_DISPOSITION",
        "phase1_action": "noop",
    },
    "invalid_date_format": {
        "canonical_type": "invalid_date_format",
        "policy_group": "POLICY_DATE_PARSING",
        "phase1_action": "parse_dates",
    },
    "date_range_violation": {
        "canonical_type": "date_range_violation",
        "policy_group": "POLICY_DATE_PARSING",
        "phase1_action": "parse_dates",
    },
    "future_dates": {
        "canonical_type": "future_dates",
        "policy_group": "POLICY_DATE_PARSING",
        "phase1_action": "parse_dates",
    },
    "very_high_cardinality": {
        "canonical_type": "very_high_cardinality",
        "policy_group": "POLICY_HIGH_CARDINALITY",
        "phase1_action": "hash_phone",
    },
    "constant_column": {
        "canonical_type": "constant_column",
        "policy_group": "POLICY_CONSTANT_COLUMN",
        "phase1_action": "noop",
    },
    "potential_primary_key": {
        "canonical_type": "potential_primary_key",
        "policy_group": "POLICY_BUSINESS_KEY_DUPLICATE",
        "phase1_action": "flag_duplicate_candidates",
    },
    "duplicate_column_names": {
        "canonical_type": "duplicate_column_names",
        "policy_group": "POLICY_COLUMN_COLLISION",
        "phase1_action": "noop",
    },
    "case_insensitive_column_collision": {
        "canonical_type": "case_insensitive_column_collision",
        "policy_group": "POLICY_COLUMN_COLLISION",
        "phase1_action": "noop",
    },
    "very_wide_table": {
        "canonical_type": "very_wide_table",
        "policy_group": "POLICY_WIDE_TABLE",
        "phase1_action": "noop",
    },
    "column_name_whitespace": {
        "canonical_type": "column_name_whitespace",
        "policy_group": "POLICY_WHITESPACE_NORMALIZATION",
        "phase1_action": "trim_preserve_case",
    },
    "integer_stored_as_float": {
        "canonical_type": "integer_stored_as_float",
        "policy_group": "POLICY_NUMERIC_PRECISION",
        "phase1_action": "cast_type",
    },
    "impossible_date": {
        "canonical_type": "impossible_date",
        "policy_group": "POLICY_DATE_PARSING",
        "default_disposition": "MANUAL_REVIEW_REQUIRED",
        "approval_required": True,
        "execution_eligible": False,
        "phase1_action": None,
    },
    "sentinel_date": {
        "canonical_type": "sentinel_date",
        "policy_group": "POLICY_DATE_PARSING",
        "default_disposition": "MANUAL_REVIEW_REQUIRED",
        "approval_required": True,
        "execution_eligible": False,
        "phase1_action": None,
    },
    "metadata_manifest_mismatch": {
        "canonical_type": "metadata_manifest_mismatch",
        "policy_group": "POLICY_METADATA_MISMATCH",
        "phase1_action": "update_manifest",
    },
}

def canonicalize_issue_type(issue_type: str) -> str:
    it = str(issue_type or "").strip().lower()
    norm = ISSUE_TYPE_ALIASES.get(it, it)
    if norm in ISSUE_REGISTRY:
        return norm
    # Fallback to check literal matching in registry keys
    for k in ISSUE_REGISTRY:
        if k.lower() == it:
            return k
    return norm
