"""
action_registry.py — Single canonical source of truth for ETL action taxonomy.

Every action in the ETL pipeline must be registered here.
phase_classifier.py derives action phases from this registry.
classify_steps.py derives auto/review buckets from this registry.

Fields per action:
  phase           : 1 (Phase 1 cleanse), 2 (Phase 2 transform), None (invalid — never classify)
  auto_eligible   : True, False, or "CONDITIONAL" (requires parameter validation before auto)
  destructive     : True if the action can remove or overwrite rows/columns
  requires_review : True if the action always requires manual steward approval
  invalid         : True if the action is ambiguous and must never reach code generation
  required_params : List of parameter keys that must be present for the action to be valid
  changes_row_population : True if the action can alter the number of rows in the output
  changes_business_meaning : True if the action can alter the semantic interpretation of a value
  minimum_phase   : Alias for `phase`, used for schema compatibility
"""
from __future__ import annotations

from typing import Any, Dict

ACTION_REGISTRY_VERSION = "1.1.0"

# fmt: off
ACTION_REGISTRY: Dict[str, Dict[str, Any]] = {

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 1 — Representation-preserving cleaning operations
    # Safe: preserve source rows, raw columns, business meaning
    # ─────────────────────────────────────────────────────────────────────

    "trim": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Trim leading/trailing whitespace from string columns.",
    },
    "trim_preserve_case": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Trim only — do not alter character casing. Used for name/identifier columns.",
    },
    "noop": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "No operation — column passes through unchanged.",
    },
    "keep_as_is": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Retain data as-is without ETL transformation or cleansing code.",
    },
    "skip_etl": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Skip column from ETL cleansing pipeline.",
    },
    "flag_only": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Preserve source row and clean value; record validation status and reason without quarantine.",
    },
    "nullify_clean_value": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Preserve raw value, set clean companion value to NULL on violation, retain row.",
    },
    "quarantine_row": {
        "phase": 1, "auto_eligible": False, "destructive": True,
        "requires_review": True, "changes_row_population": True,
        "changes_business_meaning": False,
        "description": "Route violating rows to quarantine dataframe and exclude from clean dataframe.",
    },

    # Type coercion (Phase 1 only when preserving raw value in companion column)
    "cast_type": {
        "phase": 1, "auto_eligible": "CONDITIONAL", "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "required_params": ["target_type"],
        "description": "Cast column to target type. Auto-eligible only when target_type is provided.",
    },
    "coerce_numeric": {
        "phase": 1, "auto_eligible": "CONDITIONAL", "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "required_params": ["target_type"],
        "description": "Parse numeric value from string representation into companion column.",
    },
    "standardize_boolean": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Normalise boolean representations (1/0, true/false, yes/no) to a canonical type.",
    },

    # Date parsing (Phase 1 — writes companion clean column, not overwrite)
    "parse_dates": {
        "phase": 1, "auto_eligible": "CONDITIONAL", "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "required_params": [],
        "description": "Parse date strings into a typed companion column; flag failures.",
    },
    "parse_dates_safe": {
        "phase": 1, "auto_eligible": "CONDITIONAL", "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Safe date parse with explicit audit flag column.",
    },

    # Nullification (Phase 1 — sets value to null, does not remove the row)
    "nullify_punctuation": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Replace punctuation-only strings with semantic null.",
    },
    "nullify_date": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Replace impossible or sentinel calendar dates with semantic null in clean companion column.",
    },
    "nullify_dummy_dates": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Replace known dummy/sentinel dates (e.g. 1900-01-01) with null.",
    },
    "replace_sentinel_values": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Replace registered sentinel numeric values with null.",
    },
    "normalize_registered_placeholder": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Replace registered placeholder strings (e.g. 'NA', '?', 'UNKNOWN', '-1') with null.",
    },
    "zero_to_null": {
        "phase": 1, "auto_eligible": "CONDITIONAL", "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Replace zero with null. Auto-eligible only when explicitly registered as placeholder norm.",
    },
    "replace_values": {
        "phase": 1, "auto_eligible": "CONDITIONAL", "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
        "required_params": ["replace_values"],
        "description": "Replace specific values via a declared mapping dictionary.",
    },

    # Flag-only (Phase 1 — adds boolean columns, never mutates or removes data)
    "flag_outliers": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Flag statistical outlier rows with a boolean column.",
    },
    "flag_domain_violation": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Flag categorical columns containing digit-only values.",
    },
    "fill_nulls_flag": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Flag invalid numeric values and optionally emit a clean companion column.",
    },
    "flag_missing_key": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Add is_missing_{col} flag; preserve null. Never fill.",
    },
    "flag_missing_declared_key": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Detection action for declared-key null violation. Policy decides disposition.",
    },
    "flag_high_null_key": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Flag high null percentage on an identifier/key column. Never impute.",
    },
    "flag_duplicate_candidates": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Assign stable duplicate_group_id and is_duplicate_group_member. Never drop rows.",
    },
    "flag_near_duplicate_cluster": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Assign near_dup_cluster_id and similarity metadata. Never drop rows.",
    },
    "flag_business_key_duplicate": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Flag business-key duplicate groups with stable group IDs. No survivorship.",
    },
    "flag_range_violation": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Flag values outside declared range with boolean column. No value mutation.",
    },
    "flag_cross_field_violation": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Flag rows violating a cross-field rule (e.g. non-negative constraint).",
    },

    # Quarantine (Phase 1 — partitions rows into separate output; all source rows accounted for)
    "quarantine_key_null": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Route null declared-key rows to quarantine. Requires quarantine_mandatory_keys policy.",
    },
    "quarantine_orphan_fk": {
        "phase": 2, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Route orphan FK rows to quarantine after anti-join validation.",
    },

    # Referential integrity (Phase 2 — requires multiple datasets)
    "validate_and_partition_relationship": {
        "phase": 2, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": (
            "Validate FK relationship using left_semi (valid) and left_anti (orphan) joins "
            "with shared normalization. Routes null-key, valid, and orphan rows to separate outputs."
        ),
    },
    "validate_referential_integrity_or_stage": {
        "phase": 2, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Legacy alias for validate_and_partition_relationship.",
    },
    # create_normalized_match_key: adds a lowercased + trimmed companion column
    # for join/dedup matching. Does NOT modify or remove the original value.
    # This is the correct Phase 1 response to duplicate_insensitive_values.
    "create_normalized_match_key": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "required_params": ["output_column"],
        "description": (
            "Create a lowercased, trimmed companion column for comparison/join keys. "
            "The original display value is preserved. No rows are modified or removed."
        ),
    },

    # Case and privacy transforms
    "sanitize_email": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
    },
    "normalize_phone": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
    },
    "hash_phone": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
    },
    "mask_phone": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
    },
    "regex_replace": {
        "phase": 1, "auto_eligible": "CONDITIONAL", "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
        "required_params": ["regex_pattern"],
    },
    "review_manually": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": False,
    },

    "preserve_null": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Preserve null values unchanged in text/optional columns.",
    },
    "fill_constant": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
        "required_params": [],
        "description": "Fill missing values with an explicit approved constant.",
    },
    "preserve_null_and_flag": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": False,
        "required_params": [],
        "description": (
            "Preserve null value and add validation status + reason columns. "
            "When promoted via manual review, execution mode is APPROVED_MANUAL."
        ),
    },
    "impute_numeric": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "requires_impact_preview": True,
        "changes_row_population": False,
        "changes_business_meaning": True,
        "dependency_scope": "SINGLE_DATASET",
        "required_params": ["strategy", "preserve_raw", "output_column", "policy_id"],
        "semantic_prohibitions": ["primary_key", "business_key", "foreign_key", "identifier"],
        "description": (
            "Statistical imputation for numeric non-key columns. "
            "strategy: mean|median|mode|constant|grouped_mean|grouped_median|rule_based. "
            "Prohibited on key/identifier columns."
        ),
    },
    "impute_date": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
        "required_params": [],
        "description": "Impute date values under an approved deterministic policy.",
    },
    "quarantine_missing_key": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Route rows with null keys to quarantine/staging output.",
    },
    "preserve_missing_suspected_key": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Preserve null keys unchanged for downstream triage.",
    },

    # ─────────────────────────────────────────────────────────────────────
    # EXPLICIT NULL DISPOSITION ACTIONS — Phase 1 canonical replacements
    # for fill_nulls_simple. Each encodes exactly one approved policy.
    # ─────────────────────────────────────────────────────────────────────

    "fill_null_with_constant": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
        "required_params": ["value", "policy_id", "preserve_raw"],
        "semantic_prohibitions": ["primary_key", "business_key", "foreign_key", "identifier"],
        "description": (
            "Fill null with an approved registered constant. "
            "Requires policy_id and preserve_raw=True. Prohibited on key/identifier columns."
        ),
    },
    "quarantine_null_rows": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": True,
        "changes_business_meaning": False,
        "required_params": ["reason_code", "policy_id"],
        "description": (
            "Route rows with null in a mandatory column to quarantine partition. "
            "Source rows are partitioned — not deleted. Requires policy_id."
        ),
    },
    "accept_null_as_optional": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": False,
        "action_kind": "POLICY_DISPOSITION",
        "emits_code": False,
        "description": (
            "Accept null as valid for optional columns. "
            "Emits no code — records the approved policy disposition only."
        ),
    },

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 2 — Business transformation operations
    # Require explicit authorization. May change row population or semantics.
    # ─────────────────────────────────────────────────────────────────────

    "deduplicate": {
        "phase": 1, "auto_eligible": False, "destructive": True,
        "requires_review": True, "changes_row_population": True,
        "changes_business_meaning": True,
        "required_params": ["key_columns", "survivorship_policy"],
        "description": "Remove duplicate rows per survivorship policy. Phase 1 preparation.",
    },
    "drop_rows": {
        "phase": 1, "auto_eligible": False, "destructive": True,
        "requires_review": True, "changes_row_population": True,
        "changes_business_meaning": True,
        "description": "Remove rows matching a filter predicate. Phase 1 preparation.",
    },
    "drop_column": {
        "phase": 1, "auto_eligible": False, "destructive": True,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
        "description": "Remove a column from the output. Phase 1 preparation.",
    },
    "exclude_column": {
        "phase": 1, "auto_eligible": False, "destructive": True,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
        "description": "Exclude a column from the output schema. Phase 1 preparation.",
    },
    "range_clip": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
        "required_params": ["min_value"],
        "description": "Clip values to declared range. Alters business values — Phase 1 preparation.",
    },
    "clip_outliers": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
        "description": "Clip statistical outliers to IQR bounds. Phase 1 preparation.",
    },
    "cap_outliers": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
        "description": "Cap outliers to median or declared value. Phase 1 preparation.",
    },
    "clip_or_flag": {
        "phase": None, "auto_eligible": False, "destructive": None,
        "requires_review": None, "invalid": True, "legacy": True,
        "migration_required": True,
        "description": (
            "INVALID LEGACY ACTION: does not specify clip vs. flag disposition. "
            "Replace with: flag_outliers (safe Phase 1 default) or clip_outliers (requires approval). "
            "Migration: if saved outlier_method='flag' → flag_outliers; 'clip' → clip_outliers; "
            "else AMBIGUOUS_OUTLIER_DISPOSITION — return to manual review."
        ),
    },
    "nullify_future_dates": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
        "description": (
            "Null future dates in the source-facing column. "
            "Phase 1 because it operates on a single column of a single dataset."
        ),
    },
    "lowercase": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
        "description": (
            "Lowercase a column value. Casing normalization is representation-preserving."
        ),
    },
    "uppercase": {
        "phase": 1, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": False,
        "changes_business_meaning": True,
    },
    "force_unlock": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Force gate unlock. Administrative action, no mutations."
    },
    "skip_requirement": {
        "phase": 1, "auto_eligible": True, "destructive": False,
        "requires_review": False, "changes_row_population": False,
        "changes_business_meaning": False,
        "description": "Bypass/skip data quality requirements on column."
    },
    "join_datasets": {
        "phase": 2, "auto_eligible": False, "destructive": False,
        "requires_review": True, "changes_row_population": True,
        "changes_business_meaning": True,
        "description": "Enrich join between datasets. Phase 2 enrichment only.",
    },

    # ─────────────────────────────────────────────────────────────────────
    # INVALID — Ambiguous actions that must never reach code generation
    # ─────────────────────────────────────────────────────────────────────

    "fill_or_drop": {
        "phase": None, "auto_eligible": False, "destructive": None,
        "requires_review": None, "invalid": True, "legacy": True,
        "migration_required": True,
        "description": (
            "INVALID LEGACY ACTION: does not specify whether to fill or drop. "
            "Replace with: preserve_null_and_flag, fill_null_with_constant, "
            "quarantine_null_rows, or accept_null_as_optional."
        ),
    },
    "fill_nulls_simple": {
        "phase": None, "auto_eligible": False, "destructive": None,
        "requires_review": None, "invalid": True, "legacy": True,
        "migration_required": True,
        "description": (
            "INVALID LEGACY ACTION: does not specify null disposition. "
            "Replace with: preserve_null_and_flag, fill_null_with_constant, "
            "impute_numeric, quarantine_null_rows, or accept_null_as_optional. "
            "Migration: if saved fill_strategy+value → fill_null_with_constant; "
            "if saved fill_strategy='mean'/'median' → impute_numeric; "
            "else AMBIGUOUS_NULL_DISPOSITION — return to manual review."
        ),
    },
}
# fmt: on


# ─────────────────────────────────────────────────────────────────────────────
# Convenience sets derived from the registry (single source of truth)
# ─────────────────────────────────────────────────────────────────────────────

PHASE_1_ACTIONS: frozenset[str] = frozenset(
    k for k, v in ACTION_REGISTRY.items()
    if v.get("phase") == 1 and not v.get("invalid")
)

PHASE_2_ACTIONS: frozenset[str] = frozenset(
    k for k, v in ACTION_REGISTRY.items()
    if v.get("phase") == 2 and not v.get("invalid")
)

INVALID_ACTIONS: frozenset[str] = frozenset(
    k for k, v in ACTION_REGISTRY.items() if v.get("invalid")
)

AUTO_ELIGIBLE_ACTIONS: frozenset[str] = frozenset(
    k for k, v in ACTION_REGISTRY.items()
    if v.get("auto_eligible") is True and not v.get("invalid")
)

ALWAYS_REVIEW_ACTIONS: frozenset[str] = frozenset(
    k for k, v in ACTION_REGISTRY.items()
    if v.get("requires_review") and not v.get("invalid")
)

DESTRUCTIVE_ACTIONS: frozenset[str] = frozenset(
    k for k, v in ACTION_REGISTRY.items()
    if v.get("destructive") and not v.get("invalid")
)


def get_action_meta(action: str) -> Dict[str, Any]:
    """Return registry entry for an action, or a default unknown entry."""
    return ACTION_REGISTRY.get((action or "").strip().lower(), {
        "phase": None, "auto_eligible": False, "destructive": None,
        "requires_review": True, "invalid": False,
        "description": f"Unknown action: '{action}' — not registered.",
        "_unknown": True,
    })


def is_valid_action(action: str) -> bool:
    """True if the action is registered and not marked invalid."""
    meta = get_action_meta(action)
    return not meta.get("invalid") and not meta.get("_unknown")


def is_phase1_action(action: str) -> bool:
    return get_action_meta(action).get("phase") == 1 and not get_action_meta(action).get("invalid")


def is_phase2_action(action: str) -> bool:
    return get_action_meta(action).get("phase") == 2


def is_invalid_action(action: str) -> bool:
    return bool(get_action_meta(action).get("invalid"))


def is_legacy_action(action: str) -> bool:
    """True if the action is a recognized legacy alias that requires migration."""
    return bool(get_action_meta(action).get("legacy"))


def get_semantic_prohibitions(action: str) -> list:
    """Return the list of semantic role prohibitions for an action (e.g. 'primary_key')."""
    return list(get_action_meta(action).get("semantic_prohibitions") or [])


def requires_impact_preview(action: str) -> bool:
    """True if the action requires an impact preview before execution."""
    return bool(get_action_meta(action).get("requires_impact_preview"))
