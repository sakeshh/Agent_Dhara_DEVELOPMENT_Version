"""
Remediation Capability Registry for Agent Dhara.
Declares which issues can be remediated, under what risk level, with which supported actions,
and whether auto-fix is eligible under objective safety policies.
"""
from __future__ import annotations

import logging
from typing import Dict, Optional

from agent.etl_pipeline.remediation_models import (
    CapabilityEntry,
    ImpactPolicy,
)

logger = logging.getLogger("agent.remediation_capability_registry")

# ── Action-Specific Impact Policies ───────────────────────────────────────────
TRIM_IMPACT_POLICY = ImpactPolicy(
    max_affected_percentage=10.0,
    max_affected_rows=50000,
    max_quarantine_percentage=0.0,
    allow_row_drop=False,
)

NULLIFY_IMPACT_POLICY = ImpactPolicy(
    max_affected_percentage=2.0,
    max_affected_rows=5000,
    max_quarantine_percentage=0.0,
    allow_row_drop=False,
)

QUARANTINE_IMPACT_POLICY = ImpactPolicy(
    max_affected_percentage=1.0,
    max_affected_rows=1000,
    max_quarantine_percentage=1.0,
    allow_row_drop=False,
)

STANDARD_LOW_RISK_POLICY = ImpactPolicy(
    max_affected_percentage=5.0,
    max_affected_rows=10000,
    max_quarantine_percentage=0.0,
    allow_row_drop=False,
)

NEVER_AUTO_POLICY = ImpactPolicy(
    max_affected_percentage=0.0,
    max_affected_rows=0,
    max_quarantine_percentage=0.0,
    allow_row_drop=False,
    requires_explicit_reapproval=True,
)


# ── Canonical Capability Registry ─────────────────────────────────────────────
REMEDIATION_CAPABILITY_REGISTRY: Dict[str, CapabilityEntry] = {
    # ── Text Formatting (Safe, reversible transformations) ────────────────────
    "whitespace": CapabilityEntry(
        canonical_issue_type="whitespace",
        detection_certainty="high",
        remediation_certainty="high",
        risk_level="low",
        default_action="trim",
        supported_actions=["trim", "keep_as_is"],
        generator_ids={"pandas": "trim", "pyspark": "trim", "sql": "trim"},
        approval_policy="low_risk_auto",
        impact_policy=TRIM_IMPACT_POLICY,
        cluster_group="text_formatting",
        context_tags=["whitespace", "trim", "string_hygiene"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=True,
    ),
    "internal_whitespace": CapabilityEntry(
        canonical_issue_type="internal_whitespace",
        detection_certainty="high",
        remediation_certainty="high",
        risk_level="low",
        default_action="trim",
        supported_actions=["trim", "keep_as_is"],
        generator_ids={"pandas": "trim", "pyspark": "trim", "sql": "trim"},
        approval_policy="low_risk_auto",
        impact_policy=TRIM_IMPACT_POLICY,
        cluster_group="text_formatting",
        context_tags=["whitespace", "compression", "string_hygiene"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=True,
    ),
    "case_inconsistency": CapabilityEntry(
        canonical_issue_type="case_inconsistency",
        detection_certainty="high",
        remediation_certainty="high",
        risk_level="low",
        default_action="lowercase",
        supported_actions=["lowercase", "uppercase", "trim_preserve_case", "keep_as_is"],
        generator_ids={"pandas": "lowercase", "pyspark": "lowercase", "sql": "lowercase", "trim_preserve_case": "trim"},
        approval_policy="low_risk_auto",
        impact_policy=STANDARD_LOW_RISK_POLICY,
        cluster_group="text_formatting",
        context_tags=["case", "normalization"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=True,
    ),
    "control_characters_in_text": CapabilityEntry(
        canonical_issue_type="control_characters_in_text",
        detection_certainty="high",
        remediation_certainty="high",
        risk_level="low",
        default_action="regex_replace",
        supported_actions=["regex_replace", "keep_as_is"],
        generator_ids={"pandas": "regex_replace", "pyspark": "regex_replace", "sql": "regex_replace"},
        approval_policy="low_risk_auto",
        impact_policy=STANDARD_LOW_RISK_POLICY,
        cluster_group="text_formatting",
        context_tags=["control_chars", "encoding"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=True,
    ),
    "html_tags_in_text": CapabilityEntry(
        canonical_issue_type="html_tags_in_text",
        detection_certainty="high",
        remediation_certainty="high",
        risk_level="low",
        default_action="regex_replace",
        supported_actions=["regex_replace", "keep_as_is"],
        generator_ids={"pandas": "regex_replace", "pyspark": "regex_replace", "sql": "regex_replace"},
        approval_policy="low_risk_auto",
        impact_policy=STANDARD_LOW_RISK_POLICY,
        cluster_group="text_formatting",
        context_tags=["html", "sanitization"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=True,
    ),

    # ── Completeness / Null Values (Contextual decisions) ─────────────────────
    "placeholder_detected": CapabilityEntry(
        canonical_issue_type="placeholder_detected",
        detection_certainty="high",
        remediation_certainty="high",
        risk_level="low",
        default_action="normalize_registered_placeholder",
        supported_actions=["normalize_registered_placeholder", "keep_as_is"],
        generator_ids={"pandas": "normalize_registered_placeholder", "pyspark": "normalize_registered_placeholder", "sql": "normalize_registered_placeholder"},
        approval_policy="low_risk_auto",
        impact_policy=STANDARD_LOW_RISK_POLICY,
        cluster_group="completeness",
        context_tags=["placeholder", "null_tokens"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=True,
    ),
    "empty_string_values": CapabilityEntry(
        canonical_issue_type="empty_string_values",
        detection_certainty="high",
        remediation_certainty="high",
        risk_level="low",
        default_action="preserve_null_and_flag",
        supported_actions=["preserve_null_and_flag", "fill_constant", "keep_as_is"],
        generator_ids={"pandas": "preserve_null_and_flag", "pyspark": "preserve_null_and_flag", "sql": "preserve_null_and_flag"},
        approval_policy="low_risk_auto",
        impact_policy=STANDARD_LOW_RISK_POLICY,
        cluster_group="completeness",
        context_tags=["empty_strings", "nulls"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=True,
    ),
    "nulls": CapabilityEntry(
        canonical_issue_type="nulls",
        detection_certainty="high",
        remediation_certainty="low",
        risk_level="medium",
        default_action="preserve_null_and_flag",
        supported_actions=[
            "preserve_null_and_flag",
            "fill_constant",
            "impute_numeric",
            "quarantine_null_rows",
            "accept_optional",
            "keep_as_is",
        ],
        generator_ids={"pandas": "preserve_null_and_flag", "pyspark": "preserve_null_and_flag", "sql": "preserve_null_and_flag"},
        approval_policy="always_approval",
        impact_policy=NEVER_AUTO_POLICY,
        cluster_group="completeness",
        context_tags=["missing_data", "nulls"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=False,
    ),
    "suspected_key_high_null_ratio": CapabilityEntry(
        canonical_issue_type="suspected_key_high_null_ratio",
        detection_certainty="high",
        remediation_certainty="low",
        risk_level="high",
        default_action="flag_missing_key",
        supported_actions=["flag_missing_key", "quarantine_missing_key", "drop_column", "accept_optional", "keep_as_is"],
        generator_ids={"pandas": "flag_missing_key", "pyspark": "flag_missing_key", "sql": "flag_missing_key"},
        approval_policy="always_approval",
        impact_policy=NEVER_AUTO_POLICY,
        cluster_group="completeness",
        context_tags=["null_rate", "high_nulls", "column_usability"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=False,
    ),
    "declared_key_null_violation": CapabilityEntry(
        canonical_issue_type="declared_key_null_violation",
        detection_certainty="high",
        remediation_certainty="low",
        risk_level="critical",
        default_action="flag_missing_declared_key",
        supported_actions=["flag_missing_declared_key", "quarantine_missing_key", "preserve_missing_suspected_key", "keep_as_is"],
        generator_ids={"pandas": "flag_missing_declared_key", "pyspark": "flag_missing_declared_key", "sql": "flag_missing_declared_key"},
        approval_policy="always_approval",
        impact_policy=NEVER_AUTO_POLICY,
        cluster_group="primary_keys",
        context_tags=["key_integrity", "primary_key", "null_key"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=False,
    ),

    # ── Identifiers & Format Validation (Medium risk, nullification impact) ───
    "invalid_email": CapabilityEntry(
        canonical_issue_type="invalid_email",
        detection_certainty="high",
        remediation_certainty="low",
        risk_level="medium",
        default_action="sanitize_email",
        supported_actions=["sanitize_email", "preserve_null_and_flag", "keep_as_is"],
        generator_ids={"pandas": "sanitize_email", "pyspark": "sanitize_email", "sql": "sanitize_email"},
        approval_policy="policy_dependent",
        impact_policy=NULLIFY_IMPACT_POLICY,
        cluster_group="contact_identifiers",
        context_tags=["email", "syntax", "pii"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=False,  # Contextual remediation: flag vs nullify vs quarantine
    ),
    "invalid_phone": CapabilityEntry(
        canonical_issue_type="invalid_phone",
        detection_certainty="high",
        remediation_certainty="low",
        risk_level="medium",
        default_action="normalize_phone",
        supported_actions=["normalize_phone", "preserve_null_and_flag", "keep_as_is"],
        generator_ids={"pandas": "normalize_phone", "pyspark": "normalize_phone", "sql": "normalize_phone"},
        approval_policy="policy_dependent",
        impact_policy=NULLIFY_IMPACT_POLICY,
        cluster_group="contact_identifiers",
        context_tags=["phone", "formatting", "pii"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=False,
    ),

    # ── Measures & Financial Values (Contextual business domain) ──────────────
    "cross_field_non_negative": CapabilityEntry(
        canonical_issue_type="cross_field_non_negative",
        detection_certainty="high",
        remediation_certainty="low",
        risk_level="medium",
        default_action="flag_only",
        supported_actions=["flag_only", "nullify_clean_value", "quarantine_row", "flag_outliers", "clip_outliers", "keep_as_is", "skip_etl"],
        generator_ids={"pandas": "flag_outliers", "pyspark": "flag_outliers", "sql": "flag_outliers"},
        approval_policy="always_approval",
        impact_policy=NEVER_AUTO_POLICY,
        cluster_group="financial_measures",
        context_tags=["negative_values", "price", "revenue", "returns"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=False,
    ),
    "implausible_age": CapabilityEntry(
        canonical_issue_type="implausible_age",
        detection_certainty="high",
        remediation_certainty="low",
        risk_level="medium",
        default_action="flag_only",
        supported_actions=["flag_only", "nullify_clean_value", "flag_outliers", "clip_outliers", "keep_as_is", "skip_etl"],
        generator_ids={"pandas": "flag_outliers", "pyspark": "flag_outliers", "sql": "flag_outliers"},
        approval_policy="always_approval",
        impact_policy=NEVER_AUTO_POLICY,
        cluster_group="demographic_measures",
        context_tags=["age", "range", "outliers"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=False,
    ),
    "implausible_percentage": CapabilityEntry(
        canonical_issue_type="implausible_percentage",
        detection_certainty="high",
        remediation_certainty="low",
        risk_level="medium",
        default_action="range_clip",
        supported_actions=["range_clip", "keep_as_is"],
        generator_ids={"pandas": "range_clip", "pyspark": "range_clip", "sql": "range_clip"},
        approval_policy="always_approval",
        impact_policy=NEVER_AUTO_POLICY,
        cluster_group="measure_bounds",
        context_tags=["percentage", "rate", "clip"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=False,
    ),

    # ── Uniqueness / Deduplication (High-risk data loss) ──────────────────────
    "exact_duplicate_rows": CapabilityEntry(
        canonical_issue_type="exact_duplicate_rows",
        detection_certainty="high",
        remediation_certainty="low",
        risk_level="high",
        default_action="flag_duplicate_candidates",
        supported_actions=["flag_duplicate_candidates", "deduplicate", "keep_as_is"],
        generator_ids={"pandas": "flag_duplicate_candidates", "pyspark": "flag_duplicate_candidates", "sql": "flag_duplicate_candidates"},
        approval_policy="always_approval",
        impact_policy=NEVER_AUTO_POLICY,
        cluster_group="uniqueness",
        context_tags=["duplicates", "rows", "dedup"],
        reversible=False,
        row_destructive=True,
        schema_mutating=False,
        auto_fix_eligible=False,
    ),
    "business_key_duplicate": CapabilityEntry(
        canonical_issue_type="business_key_duplicate",
        detection_certainty="high",
        remediation_certainty="low",
        risk_level="critical",
        default_action="flag_business_key_duplicate",
        supported_actions=["flag_business_key_duplicate", "deduplicate_last", "deduplicate_first", "keep_as_is"],
        generator_ids={"pandas": "flag_business_key_duplicate", "pyspark": "flag_business_key_duplicate", "sql": "flag_business_key_duplicate"},
        approval_policy="always_approval",
        impact_policy=NEVER_AUTO_POLICY,
        cluster_group="uniqueness",
        context_tags=["primary_key", "business_key", "duplicates"],
        reversible=False,
        row_destructive=True,
        schema_mutating=False,
        auto_fix_eligible=False,
    ),

    # ── Referential Integrity (Cross-dataset relations) ───────────────────────
    "orphan_foreign_key_rows": CapabilityEntry(
        canonical_issue_type="orphan_foreign_key_rows",
        detection_certainty="high",
        remediation_certainty="low",
        risk_level="critical",
        default_action="reject_orphans",
        supported_actions=["reject_orphans", "keep_as_is"],
        generator_ids={"pandas": "reject_orphans", "pyspark": "reject_orphans", "sql": "reject_orphans"},
        approval_policy="always_approval",
        impact_policy=NEVER_AUTO_POLICY,
        cluster_group="referential_integrity",
        context_tags=["foreign_key", "relationships", "orphans"],
        reversible=False,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=False,
    ),

    # ── Types & Date Conversions (Deterministic when standard) ────────────────
    "mixed_date_formats": CapabilityEntry(
        canonical_issue_type="mixed_date_formats",
        detection_certainty="high",
        remediation_certainty="high",
        risk_level="low",
        default_action="parse_dates",
        supported_actions=["parse_dates", "keep_as_is"],
        generator_ids={"pandas": "parse_dates", "pyspark": "parse_dates", "sql": "parse_dates"},
        approval_policy="low_risk_auto",
        impact_policy=STANDARD_LOW_RISK_POLICY,
        cluster_group="datetime_formatting",
        context_tags=["dates", "iso_parse"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=True,
    ),
    "binary_like_column": CapabilityEntry(
        canonical_issue_type="binary_like_column",
        detection_certainty="high",
        remediation_certainty="high",
        risk_level="low",
        default_action="standardize_boolean",
        supported_actions=["standardize_boolean", "keep_as_is"],
        generator_ids={"pandas": "standardize_boolean", "pyspark": "standardize_boolean", "sql": "standardize_boolean"},
        approval_policy="low_risk_auto",
        impact_policy=STANDARD_LOW_RISK_POLICY,
        cluster_group="data_types",
        context_tags=["boolean", "binary", "cast"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=True,
    ),
    "integer_stored_as_float": CapabilityEntry(
        canonical_issue_type="integer_stored_as_float",
        detection_certainty="high",
        remediation_certainty="medium",
        risk_level="medium",
        default_action="cast_to_integer",
        supported_actions=["cast_to_integer", "flag_only", "keep_as_is"],
        generator_ids={"pandas": "cast_to_integer", "pyspark": "cast_to_integer", "sql": "cast_to_integer"},
        approval_policy="always_approval",
        impact_policy=STANDARD_LOW_RISK_POLICY,
        cluster_group="data_types",
        context_tags=["numeric", "cast", "precision"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=False,
        execution_scope="field_transform",
    ),
    "impossible_date": CapabilityEntry(
        canonical_issue_type="impossible_date",
        detection_certainty="high",
        remediation_certainty="high",
        risk_level="high",
        default_action="flag_only",
        supported_actions=["flag_only", "nullify_date", "quarantine_row", "keep_as_is"],
        generator_ids={"pandas": "flag_only", "pyspark": "flag_only", "sql": "flag_only"},
        approval_policy="always_approval",
        impact_policy=NULLIFY_IMPACT_POLICY,
        cluster_group="date_validation",
        context_tags=["date", "calendar", "syntax"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=False,
        execution_scope="field_transform",
    ),
    "sentinel_date": CapabilityEntry(
        canonical_issue_type="sentinel_date",
        detection_certainty="high",
        remediation_certainty="high",
        risk_level="medium",
        default_action="flag_only",
        supported_actions=["flag_only", "map_to_null", "keep_as_is"],
        generator_ids={"pandas": "flag_only", "pyspark": "flag_only", "sql": "flag_only"},
        approval_policy="always_approval",
        impact_policy=NULLIFY_IMPACT_POLICY,
        cluster_group="date_validation",
        context_tags=["date", "sentinel", "placeholder"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=False,
        execution_scope="field_transform",
    ),
    "metadata_manifest_mismatch": CapabilityEntry(
        canonical_issue_type="metadata_manifest_mismatch",
        detection_certainty="high",
        remediation_certainty="high",
        risk_level="medium",
        default_action="update_manifest",
        supported_actions=["update_manifest", "ignore_mismatch"],
        generator_ids={},
        approval_policy="always_approval",
        impact_policy=NEVER_AUTO_POLICY,
        cluster_group="configuration",
        context_tags=["manifest", "metadata", "schema"],
        reversible=True,
        row_destructive=False,
        schema_mutating=False,
        auto_fix_eligible=False,
        execution_scope="configuration_change",
    ),
}

# ── Safe Unknown Fallback ─────────────────────────────────────────────────────
UNKNOWN_CAPABILITY_FALLBACK = CapabilityEntry(
    canonical_issue_type="unknown_profiler_issue",
    detection_certainty="unknown",
    remediation_certainty="unknown",
    risk_level="high",
    default_action="keep_as_is",
    supported_actions=["keep_as_is"],
    generator_ids={},
    approval_policy="always_approval",
    impact_policy=NEVER_AUTO_POLICY,
    cluster_group="unclassified",
    context_tags=["unknown", "unclassified"],
    reversible=True,
    row_destructive=False,
    schema_mutating=False,
    auto_fix_eligible=False,
    execution_scope="no_op",
)


def get_capability_entry(canonical_issue_type: str) -> CapabilityEntry:
    """Retrieve the capability entry for a canonical issue type or return the safe unknown fallback."""
    return REMEDIATION_CAPABILITY_REGISTRY.get(canonical_issue_type, UNKNOWN_CAPABILITY_FALLBACK)


def validate_capability_registry() -> bool:
    """
    Validates architectural invariants across the registry:
    1. Auto-fix is never eligible for high or critical risk.
    2. Auto-fix requires high remediation certainty.
    3. Default action is always in supported actions.
    4. Destructive or schema mutating actions require explicit approval.
    """
    for itype, cap in REMEDIATION_CAPABILITY_REGISTRY.items():
        if cap.auto_fix_eligible:
            if cap.risk_level in ("high", "critical"):
                raise ValueError(f"Registry violation: {itype} has risk_level '{cap.risk_level}' but auto_fix_eligible=True")
            if cap.remediation_certainty != "high":
                raise ValueError(f"Registry violation: {itype} has remediation_certainty '{cap.remediation_certainty}' but auto_fix_eligible=True")
            if cap.row_destructive:
                raise ValueError(f"Registry violation: {itype} is row_destructive but auto_fix_eligible=True")
        if cap.default_action not in cap.supported_actions:
            raise ValueError(f"Registry violation: {itype} default_action '{cap.default_action}' not in supported_actions {cap.supported_actions}")

    # Enforce UNKNOWN_CAPABILITY_FALLBACK invariants: review-only, never auto-fixable
    if UNKNOWN_CAPABILITY_FALLBACK.auto_fix_eligible:
        raise ValueError("Registry violation: UNKNOWN_CAPABILITY_FALLBACK must have auto_fix_eligible=False")
    if UNKNOWN_CAPABILITY_FALLBACK.approval_policy != "always_approval":
        raise ValueError("Registry violation: UNKNOWN_CAPABILITY_FALLBACK must have approval_policy='always_approval'")
    if UNKNOWN_CAPABILITY_FALLBACK.impact_policy.max_affected_rows != 0 or UNKNOWN_CAPABILITY_FALLBACK.impact_policy.max_affected_percentage != 0:
        raise ValueError("Registry violation: UNKNOWN_CAPABILITY_FALLBACK must have 0 impact limits")

    return True


# Run validation at import time to ensure registry integrity
validate_capability_registry()
