from __future__ import annotations
from typing import Any, Dict

_DEFAULT_REC = (
    "Review with domain owners; document the expected rule; add validation at ingest or in the warehouse."
)

DQ_ISSUE_RECOMMENDATIONS: Dict[str, str] = {
    "nulls": "Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults.",
    "duplicate_primary_key": "Duplicates detected on key column. Technical deduplication is possible (keep_first / keep_last / keep_most_complete). However, a survivorship policy (which record wins on conflict) must be defined by domain owners before automated removal. Do NOT blindly drop records.",
    "duplicate_rows": "Exact duplicate physical records detected. Technical deduplication is possible. However, two identical transaction rows may represent distinct real-world events if no unique identifier or timestamp exists. A survivorship policy must be defined by domain owners.",
    "exact_duplicate_rows": "Exact duplicate physical records detected. Technical deduplication is possible (keep_first / keep_last / keep_most_complete). HOWEVER: two identical transaction rows may represent distinct real-world events if no unique identifier or timestamp exists. A survivorship policy must be defined by domain owners. Do NOT blindly drop without a declared ingestion rule.",
    "declared_key_violation": "Duplicates detected on a declared key column. A survivorship policy must be defined by domain owners before automated removal. Do NOT blindly drop records.",
    "candidate_key_violation": "Duplicates detected on an inferred candidate key column. A survivorship policy must be defined by domain owners before automated removal.",
    "orphan_foreign_key_rows": "Investigate parent-child referential integrity; check if parent records were deleted without cascading or if child was mis-keyed.",
    "whitespace": "Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input.",
    "invalid_email": "Correct formatting errors if minor, or reject record and request re-entry; add regex validation on email input.",
    "invalid_phone": "Standardize phone format (e.g. E.164); check for missing country codes or area codes.",
    "invalid_numeric": "Cast column values to float/int; investigate why non-numeric characters are present in numeric fields.",
    "mixed_types": "Enforce a single schema data type (e.g. string or numeric) in ETL and handle formatting exceptions gracefully.",
    "negative_values": "Preserve the raw value. Quarantine the record, nullify the clean companion value, or correct it from an authoritative source under an approved policy.",
    "declared_non_negative_rule_violation": "Preserve the raw value. Quarantine the record, nullify the clean companion value, or correct it from an authoritative source under an approved policy.",
    "negative_values_review_required": "Preserve the raw value. Quarantine the record, nullify the clean companion value, or correct it from an authoritative source under an approved policy.",
    "negative_values_in_non_negative_measure": "Preserve the raw value. Quarantine the record, nullify the clean companion value, or correct it from an authoritative source under an approved policy.",
    "dominant_value_skew": "Investigate if this column has high constant behavior; consider normalizing or grouping low-frequency values.",
    "numeric_outliers_iqr": "Investigate values beyond 1.5x IQR; check if they represent extreme cases, fraud, or data entry errors.",
    "skewed_distribution": "Apply log transform for analytics or stratify reporting; check for contamination.",
    "integer_stored_as_float": "Cast to integer type (Int64 nullable) to avoid float drift.",
    "future_dates": "Correct clock skew or data entry; set max date validation at source.",
    "ancient_dates": "Fix century typos or replace sentinel dates with NULL.",
    "very_wide_date_span": "Split historical vs operational feeds if span is implausible for one entity.",
    "extremely_long_strings": "Truncate with audit trail, or move large text to blob/document store.",
    "empty_string_values": "Normalize empty string to NULL for consistent SQL semantics.",
    "control_characters_in_text": "Strip non-printable chars in ETL; fix export encoding (UTF-8).",
    "mixed_scalar_types": "Standardize to a single scalar type in ETL (e.g. cast all IDs to string or integer) and enforce it at ingest.",
    "invalid_date_format": "Accepted formats: [\"yyyy-MM-dd\"]. Rule source: declared. Configuration version: 2026.08.1. Sentinel values: [\"1900-01-01\", \"9999-12-31\"]. Standardize format using standard date parsing companion functions.",
    "case_inconsistency": "Normalize case in ETL (e.g. UPPER/LOWER) and consider adding a canonical mapping table for display.",
    "name_format_inconsistency": "Normalize name presentation (trim, collapse spaces, title-case) in staging; preserve raw in an audit column if needed.",
    "mixed_phone_formats": "Normalize phones to a single canonical format (prefer E.164) and validate length/country rules at capture time.",
    "systematic_placeholder": "Investigate upstream defaults; replace placeholders with NULL and enforce domain constraints at source.",
    "out_of_range": "Clip/correct out-of-range values or quarantine rows; confirm expected business bounds with domain owners.",
    "id_type_drift_across_datasets": "Align serialization across formats: IDs should use a consistent type (string or integer) across JSON/CSV/XML exports.",
    "duplicate_representation_candidate": "These datasets likely represent the same entities in different formats; avoid double-counting and choose a system-of-record.",
    "custom_one_of": "Map invalid values to allowed enum or reject rows per data contract.",
    "custom_range": "Clip to bounds or reject; align with business limits.",
    "custom_regex": "Fix format at source or apply regex replace in staging.",
    "custom_not_null": "Backfill from upstream or drop incomplete rows per policy.",
    # --- NEW CHECKS ---
    "placeholder_detected": "Replace placeholder strings ('???', 'N/A', 'unknown', 'tbd') with NULL during ETL cleansing.",
    "string_with_only_digits_in_text_column": "Investigate digit strings in categorical text columns; confirm whether numeric IDs were placed in domain text fields.",
    "invalid_url": "Validate URL format at entry; normalize with urllib.parse; reject structurally invalid URLs.",
    "html_tags_in_text": "Strip HTML/XML tags with BeautifulSoup or regex before storing in a plain-text column.",
    "punctuation_only_value": "Replace symbol-only strings with NULL; investigate upstream export bugs.",
    "sentinel_numeric_value": "Replace sentinel values (-999, 9999999, etc.) with NULL; enforce domain constraints at source.",
    "boolean_inconsistency": "Standardize to a single boolean representation (True/False or 1/0) across the pipeline.",
    "internal_whitespace": "Collapse consecutive spaces with REGEXP_REPLACE or str.strip in ETL.",
    "non_ascii_characters": "Normalize to UTF-8; strip or transliterate unexpected non-ASCII if column is meant to be ASCII.",
    "invalid_uuid": "Enforce UUID v4 format at source; regenerate malformed UUIDs.",
    "leading_zeros_on_numeric_id": "Store leading-zero IDs as VARCHAR/TEXT to prevent data loss on integer cast.",
    "numeric_outliers_zscore": "Investigate extreme z-score outliers (>4 std devs); likely data entry errors, test records, or fraud signals.",
    "round_number_anomaly": "Suspiciously round numbers may indicate estimates or placeholders; validate exact source values.",
    "date_clumping_jan1": "Dates clustering on Jan 1 often indicate default/dummy dates; replace with actual dates or NULL.",
    "date_clumping_month_end": "Dates clustering on month-end may indicate estimated or rolled-up dates; verify with source.",
    "all_caps_values": "Inconsistent all-caps entries may indicate data entry from legacy systems; normalize case in ETL.",
    "string_length_outlier": "Strings significantly longer than the column average may contain concatenated data or free-text errors.",
    "numeric_precision_anomaly": "Mixing integers with high-precision floats suggests inconsistent data capture; standardize precision.",
    "impossible_date": "Dates like 2000-02-30 or 2001-13-01 are structurally invalid; fix upstream date parsing.",
    "weekend_date_anomaly": "Business transactions on weekends may be legitimate or may indicate date errors; verify with domain.",
    "duplicate_insensitive_values": "Values that differ only by case/whitespace produce false uniqueness; deduplicate after normalization.",
    "low_variance_numeric": "Near-constant numeric column may indicate fill/default behavior rather than real data.",
    "high_null_ratio_in_key_column": "Key or ID columns with high null rates cannot reliably join; backfill or reject at ingest.",
    "mixed_date_formats": "Multiple date formats in the same column (e.g. DD/MM/YYYY vs YYYY-MM-DD) cause silent parse errors.",
    "implausible_age": "Age values outside the range 0-150 are likely data entry errors; validate at source.",
    "implausible_percentage": "Percentage values outside 0-100 are structurally invalid; add range constraint at ingest.",
    "timezone_inconsistency": "Datetime values mixing timezone-aware and timezone-naive records cause comparison errors.",
    "string_with_only_digits_in_text_column": "Text columns containing only digits may indicate a schema mismatch or misrouted data.",
    "repeated_token_in_string": "Values with repeated words/tokens (e.g. 'test test') often indicate data entry errors.",
    "near_duplicate_rows": "Rows that are identical except for one or two fields may be erroneous duplicates; deduplicate or merge.",
}

FIXABILITY_TIERS: Dict[str, str] = {
    "AUTO_FIXABLE":          "Deterministic safe transform; no human review required.",
    "CONDITIONALLY_FIXABLE": "Technically automatable; requires a declared policy first.",
    "HUMAN_REVIEW_REQUIRED": "Quarantine and escalate; domain expert must decide.",
    "ROOT_CAUSE_ONLY":       "Fix at source system; pipeline cannot remediate.",
}

FIXABILITY_BY_ISSUE_TYPE: Dict[str, str] = {
    # AUTO_FIXABLE - safe deterministic transforms (deliberately minimal)
    "whitespace":                        "AUTO_FIXABLE",
    "empty_string_values":               "AUTO_FIXABLE",

    # Everything else is conditional or stricter
    "internal_whitespace":               "CONDITIONALLY_FIXABLE",  # "New York" must not be collapsed
    "html_tags_in_text":                 "CONDITIONALLY_FIXABLE",  # only safe on declared plain-text cols
    "integer_stored_as_float":           "CONDITIONALLY_FIXABLE",  # only safe if field declared integral
    "id_type_drift_across_datasets":     "CONDITIONALLY_FIXABLE",  # leading zeros, precision risks
    "case_inconsistency":                "CONDITIONALLY_FIXABLE",  # corrupts IDs/acronyms
    "boolean_inconsistency":             "CONDITIONALLY_FIXABLE",
    "all_caps_values":                   "CONDITIONALLY_FIXABLE",
    "duplicate_insensitive_values":      "CONDITIONALLY_FIXABLE",
    "mixed_scalar_types":                "CONDITIONALLY_FIXABLE",
    "invalid_numeric":                   "CONDITIONALLY_FIXABLE",
    "invalid_date_format":               "CONDITIONALLY_FIXABLE",
    "placeholder_detected":              "CONDITIONALLY_FIXABLE",
    "sentinel_numeric_value":            "CONDITIONALLY_FIXABLE",

    # exact_duplicate_rows: CONDITIONALLY_FIXABLE
    "exact_duplicate_rows":              "CONDITIONALLY_FIXABLE",
    "declared_key_violation":            "CONDITIONALLY_FIXABLE",
    "candidate_key_violation":           "CONDITIONALLY_FIXABLE",
    "duplicate_primary_key":             "CONDITIONALLY_FIXABLE",  # backward compat alias

    "nulls":                             "HUMAN_REVIEW_REQUIRED",
    "declared_key_high_null_ratio":      "HUMAN_REVIEW_REQUIRED",
    "declared_key_null_violation":       "HUMAN_REVIEW_REQUIRED",
    "suspected_key_high_null_ratio":     "HUMAN_REVIEW_REQUIRED",

    "orphan_foreign_key_rows":           "HUMAN_REVIEW_REQUIRED",
    "invalid_email":                     "HUMAN_REVIEW_REQUIRED",
    "negative_values_review_required":   "HUMAN_REVIEW_REQUIRED",
    "negative_values_in_non_negative_measure": "HUMAN_REVIEW_REQUIRED",
    "near_duplicate_rows":               "HUMAN_REVIEW_REQUIRED",

    "orphan_foreign_key":                "ROOT_CAUSE_ONLY",
    "invalid_uuid":                      "ROOT_CAUSE_ONLY",
    "duplicate_representation_candidate":"ROOT_CAUSE_ONLY",
}
