"""
Structured resolution options for manual_review plan items.
Each option maps to a codegen action (or noop for keep-as-is).
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, model_validator

ResolutionOption = Dict[str, Any]

_SKIP_ACTIONS = frozenset({"noop", "keep_as_is"})


class OptionReadiness(str, Enum):
    ADVISORY_ONLY = "advisory_only"
    REQUIRES_PARAMETERS = "requires_parameters"
    REQUIRES_HUMAN_APPROVAL = "requires_human_approval"
    REQUIRES_POLICY_APPROVAL = "requires_policy_approval"
    READY_FOR_PLAN_PROPOSAL = "ready_for_plan_proposal"
    INVALID_PROPOSAL = "invalid_proposal"


class RemediationIntent(BaseModel):
    intent_id: str
    review_item_id: str
    decision_id: str
    action: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    authorized: bool = False
    authorized_by: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ManualReviewDecision(BaseModel):
    review_item_id: str
    selected_option_id: str
    selected_action: str
    readiness: OptionReadiness
    approved_by: Optional[str] = None
    approval_timestamp: Optional[datetime] = None
    decision_rationale: Optional[str] = None
    proposal_eligible: bool = False
    execution_eligible: bool = False

    @model_validator(mode="after")
    def validate_decision(self) -> "ManualReviewDecision":
        if self.execution_eligible:
            raise ValueError("Manual review decisions cannot be executed directly; they must promote to RemediationIntent")
        if self.readiness == OptionReadiness.ADVISORY_ONLY and self.proposal_eligible:
            raise ValueError("Advisory-only decisions cannot be plan proposal eligible")
        if self.readiness in (OptionReadiness.REQUIRES_HUMAN_APPROVAL, OptionReadiness.REQUIRES_POLICY_APPROVAL) and not self.approved_by:
            raise ValueError("Items requiring approval must specify approved_by before proposal promotion")
        return self

    def to_remediation_intent(self) -> RemediationIntent:
        """Promotes an approved decision to a typed RemediationIntent (does NOT execute directly)."""
        if not self.proposal_eligible or self.readiness == OptionReadiness.ADVISORY_ONLY:
            raise ValueError("Decision is not eligible for remediation intent promotion")
        return RemediationIntent(
            intent_id=f"intent_{self.review_item_id}",
            review_item_id=self.review_item_id,
            decision_id=self.selected_option_id,
            action=self.selected_action,
            authorized=bool(self.approved_by),
            authorized_by=self.approved_by,
        )


class SourceSystemDeferral(BaseModel):
    deferral_id: str
    evidence_id: str
    source_system: str
    target_ticket_id: Optional[str] = None
    expected_fix_version: Optional[str] = None
    deferred_by: str
    deferred_at: datetime
    rationale: str
    status: Literal["active", "resolved", "re-evaluated"] = "active"


def tag_option_readiness(opt: Dict[str, Any]) -> Dict[str, Any]:
    # Defensive synthesis of id and recommendation flags
    if not opt.get("id"):
        opt["id"] = opt.get("action") or opt.get("proposal_id") or "keep_as_is"
    if "is_recommended" in opt and "recommended" not in opt:
        opt["recommended"] = bool(opt["is_recommended"])

    action = str(opt.get("action") or "").strip()
    if opt.get("proposal_eligible") is False or action == "unsupported_action" or opt.get("readiness") == OptionReadiness.ADVISORY_ONLY.value:
        opt["readiness"] = OptionReadiness.ADVISORY_ONLY.value
        opt["proposal_eligible"] = False
        opt["execution_eligible"] = False
    elif (
        opt.get("requires_human_approval")
        or opt.get("destructive")
        or action in ("deduplicate", "quarantine_row", "quarantine_missing_key", "quarantine_null_rows", "drop_column", "exclude_column")
        or opt.get("readiness") == OptionReadiness.REQUIRES_HUMAN_APPROVAL.value
    ):
        opt["readiness"] = OptionReadiness.REQUIRES_HUMAN_APPROVAL.value
        opt["proposal_eligible"] = True
        opt["execution_eligible"] = False
    else:
        opt["readiness"] = opt.get("readiness") or OptionReadiness.READY_FOR_PLAN_PROPOSAL.value
        opt["proposal_eligible"] = True
        opt["execution_eligible"] = False
    return opt

RESOLUTION_TO_ACTION_MAP: Dict[str, str] = {
    # ── Deduplication resolution IDs → Phase 1 preparation actions ───────────────────
    "deduplicate_last":               "deduplicate",
    "deduplicate_first":              "deduplicate",
    "deduplicate_near_duplicates":    "flag_near_duplicate_cluster",
    "deduplicate_business_key":       "flag_business_key_duplicate",
    "deduplicate_insensitive_values": "create_normalized_match_key",
    # ── Explicit null disposition IDs (canonical — one policy per ID) ────────────
    "preserve_and_flag":              "preserve_null_and_flag",
    "fill_constant":                  "fill_null_with_constant",
    "impute_mean":                    "impute_numeric",
    "impute_median":                  "impute_numeric",
    "impute_mode":                    "impute_numeric",
    "impute_numeric":                 "impute_numeric",
    "quarantine_null_rows":           "quarantine_null_rows",
    "accept_optional":                "accept_null_as_optional",
    # ── Explicit outlier disposition IDs ────────────────────────────────────
    "flag_outliers":                  "flag_outliers",
    "clip_outliers":                  "clip_outliers",
    "keep_outliers":                  "noop",
    # ── Relationship resolution IDs ──────────────────────────────────────────
    "quarantine_all_null":            "validate_and_partition_relationship",
    "reject_orphans":                 "validate_and_partition_relationship",
    "validate_referential_integrity_or_stage": "validate_and_partition_relationship",
    # ── Phase 1 canonical pass-through IDs ──────────────────────────────────
    "flag_near_duplicate_cluster":         "flag_near_duplicate_cluster",
    "flag_duplicate_candidates":           "flag_duplicate_candidates",
    "flag_business_key_duplicate":         "flag_business_key_duplicate",
    "flag_missing_declared_key":           "flag_missing_declared_key",
    "flag_missing_key":                    "flag_missing_key",
    "create_normalized_match_key":         "create_normalized_match_key",
    "validate_and_partition_relationship": "validate_and_partition_relationship",
    "coerce_numeric":                      "coerce_numeric",
    "parse_dates_safe":                    "parse_dates_safe",
    "trim_preserve_case":                  "trim_preserve_case",
    # ── No-ops / policy-only actions ────────────────────────────────────────
    "abort_pipeline":    "noop",
    "accept_risk":       "noop",
    "force_unlock":      "force_unlock",
    "skip_requirement":  "skip_requirement",
    "keep_as_is":        "keep_as_is",
    "skip_etl":          "skip_etl",
    "flag_only":         "flag_only",
    "nullify_clean_value": "nullify_clean_value",
    "quarantine_row":    "quarantine_row",
    "cast_to_integer":   "cast_type",
    "nullify_date":      "nullify_date",
    "map_to_null":       "map_to_null",
    "update_manifest":   "update_manifest",
    "ignore_mismatch":   "noop",
    "range_clip":        "range_clip",
    "flag_range_violation": "flag_range_violation",
    "waive":             "noop",
    "modify":            "noop",
}

# Legacy resolution IDs that are ambiguous and must not silently map to any action.
# When encountered during promotion, these trigger MIGRATION_REVIEW_REQUIRED.
LEGACY_AMBIGUOUS_RESOLUTIONS: frozenset = frozenset({
    "fill_nulls",
    "fill_null",
    "fill_nulls_simple",
    "fill_or_drop",
    "flag",        # ambiguous: could be flag_outliers OR flag_missing_key OR other
    "clip_or_flag",
})


def _opt(
    opt_id: str,
    label: str,
    action: str,
    *,
    recommended: bool = False,
    description: str = "",
    requires_human_approval: bool = False,
) -> ResolutionOption:
    opt: ResolutionOption = {
        "id": opt_id,
        "label": label,
        "action": action,
        "recommended": recommended,
        "description": description,
    }
    if requires_human_approval:
        opt["requires_human_approval"] = True
    return tag_option_readiness(opt)


_DEFAULT_OPTIONS: List[ResolutionOption] = [
    _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop", description="No transform; document in runbook."),
]

_CATALOG: Dict[str, List[ResolutionOption]] = {
    "very_high_cardinality": [
        _opt("hash_sha256", "Hash (SHA-256)", "hash_phone", recommended=True, description="One-way hash for PII-like identifiers."),
        _opt("mask_last4", "Mask (last 4 digits)", "mask_phone", description="Show only last four digits."),
        _opt("exclude_column", "Exclude from output", "exclude_column", description="Drop column before write."),
        _opt("keep_as_is", "Keep raw (accept risk)", "noop"),
    ],
    "future_dates": [
        _opt("nullify_future", "Nullify future dates", "nullify_future_dates", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "invalid_date_format": [
        _opt("parse_dates", "Standardize date formats", "parse_dates", recommended=True, description="Convert and parse date strings in various formats, and quarantine invalid dates."),
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop"),
    ],
    "date_range_violation": [
        _opt("nullify_out_of_range", "Nullify out-of-range dates", "nullify_future_dates", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    # invalid_numeric: values that cannot be parsed as declared numeric type
    # Phase 1: coerce with preserve_raw=True (keep original for audit)
    "invalid_numeric": [
        _opt("coerce_numeric", "Coerce to numeric (preserve raw)", "coerce_numeric", recommended=True,
             description="Attempt numeric parse; keep original value in raw column for audit."),
        _opt("fill_nulls_flag", "Flag as invalid and preserve raw", "fill_nulls_flag",
             description="Mark value as invalid, keep raw value unchanged."),
        _opt("keep_as_is", "Keep as-is (accept risk)", "noop"),
    ],
    # duplicate_insensitive_values: multiple text values that normalize to the same
    # comparison form (e.g. "Email" / "email" / " email "). Phase 1: create a
    # normalized companion key — do NOT remove rows.
    "duplicate_insensitive_values": [
        _opt("create_normalized_match_key", "Create normalized match key (preserve raw)", "create_normalized_match_key",
             recommended=True,
             description="Add a lowercased, trimmed companion column for matching. Original value is preserved."),
        _opt("trim_preserve_case", "Trim whitespace only (preserve case)", "trim_preserve_case",
             description="Remove leading/trailing whitespace. Case is preserved for display fields."),
        _opt("keep_as_is", "Keep as-is (accept risk)", "noop"),
    ],
    "constant_column": [
        _opt("drop_column", "Drop column", "drop_column", recommended=True),
        _opt("keep_as_is", "Keep column", "noop"),
    ],
    "potential_primary_key": [
        _opt("deduplicate", "Deduplicate on column", "deduplicate", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "duplicate_column_names": [
        _opt("exclude_column", "Exclude duplicate columns", "exclude_column", recommended=True, description="Drop colliding columns to prevent ETL load crashes."),
        _opt("keep_as_is", "Rename in source (manual)", "noop"),
    ],
    "case_insensitive_column_collision": [
        _opt("exclude_column", "Exclude colliding columns", "exclude_column", recommended=True, description="Exclude duplicate case-insensitive colliding columns."),
        _opt("keep_as_is", "Standardize names in source (manual)", "noop"),
    ],
    "very_wide_table": [
        _opt("keep_as_is", "Review with stakeholders (skip ETL)", "noop", recommended=True),
    ],
    "column_name_whitespace": [
        _opt(
            "keep_as_is",
            "Rename columns in source (manual)",
            "noop",
            recommended=True,
            description="Whitespace in column names must be fixed at ingest/schema mapping.",
        ),
    ],
    "duplicate_primary_key": [
        _opt("deduplicate", "Deduplicate on primary key", "deduplicate", recommended=True, description="Remove duplicate rows keeping first occurrence per primary key."),
        _opt("keep_as_is", "Keep as-is (allow duplicates)", "noop"),
    ],
    "numeric_outliers_iqr": [
        _opt("flag_outliers", "Flag IQR outliers", "flag_outliers", recommended=True, description="Add audit column flagging statistical IQR outliers."),
        _opt("clip_outliers", "Clip to IQR bounds", "clip_outliers", description="Cap values at IQR statistical boundaries."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "suspicious_zero": [
        _opt("zero_to_null", "Nullify suspicious zeros", "zero_to_null", recommended=True, description="Replace suspicious zero values with NULL."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "dominant_value_skew": [
        _opt("flag_outliers", "Flag dominant value rows", "flag_outliers", recommended=True, description="Add audit column flagging rows with dominant/fill values."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "skewed_distribution": [
        _opt("flag_outliers", "Flag extreme values", "flag_outliers", recommended=True, description="Add audit column for extreme distribution values."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "invalid_gstin": [
        _opt("regex_replace", "Flag/nullify invalid GSTIN", "regex_replace", recommended=True, description="Nullify values that fail GSTIN format validation (15-char alphanumeric)."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "invalid_pan": [
        _opt("regex_replace", "Flag/nullify invalid PAN", "regex_replace", recommended=True, description="Nullify values that fail PAN format validation (AAAAA0000A)."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "invalid_aadhaar": [
        _opt("regex_replace", "Flag/nullify invalid Aadhaar", "regex_replace", recommended=True, description="Nullify values that fail 12-digit Aadhaar validation."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "invalid_ifsc": [
        _opt("regex_replace", "Flag/nullify invalid IFSC", "regex_replace", recommended=True, description="Nullify values that fail IFSC format validation (AAAA0000000)."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "invalid_cin": [
        _opt("regex_replace", "Flag/nullify invalid CIN", "regex_replace", recommended=True, description="Nullify values that fail CIN format validation."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "invalid_url": [
        _opt("regex_replace", "Nullify malformed URLs", "regex_replace", recommended=True, description="Replace malformed URL strings with NULL."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "round_number_anomaly": [
        _opt("flag_outliers", "Flag round-number anomalies", "flag_outliers", recommended=True, description="Add audit column flagging suspiciously round numbers."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "weekend_date_anomaly": [
        _opt("flag_outliers", "Flag weekend date anomalies", "flag_outliers", recommended=True, description="Add audit column flagging business dates on weekends."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "date_clumping_month_end": [
        _opt("flag_outliers", "Flag month-end date clumping", "flag_outliers", recommended=True, description="Add audit column flagging suspicious month-end date clumping."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "repeated_token_in_string": [
        _opt("regex_replace", "Clean repeated tokens", "regex_replace", recommended=True, description="Remove repeated words/tokens in text strings."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "encoding_corruption": [
        _opt("regex_replace", "Strip encoding artifacts (best-effort)", 
             "regex_replace", recommended=True,
             description="Remove mojibake using regex — partial recovery only."),
        _opt("exclude_column", "Exclude corrupted column from output", "exclude_column"),
        _opt("accept_risk", "Accept as-is (acknowledge corruption)", "noop"),
    ],
    "disposable_email": [
        _opt("sanitize_email", "Flag/nullify disposable emails", "sanitize_email", recommended=True, description="Nullify known disposable email domain addresses."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "very_wide_date_span": [
        _opt("parse_dates", "Parse dates consistently", "parse_dates", recommended=True, description="Normalise mixed date formats across a wide date span."),
        _opt("flag_outliers", "Flag date span outliers", "flag_outliers"),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "empty_dataset": [
        _opt("abort_pipeline", "Abort pipeline for this dataset", 
             "noop", recommended=True,
             description="Skip ETL generation for this dataset until source data is available."),
        _opt("accept_risk", "Generate ETL anyway (empty source risk)", "noop"),
    ],
    "non_nullable_fill": [
        _opt("preserve_and_flag", "Preserve null and flag for review", "preserve_null_and_flag",
             recommended=True,
             description="Keep null as-is, add validation status column. Safe Phase 1 default."),
        _opt("fill_constant", "Fill with approved constant (e.g. 'UNKNOWN')", "fill_null_with_constant",
             description="Replace null with an approved registered constant. Requires policy ID."),
        _opt("impute_numeric", "Statistical imputation (numeric non-key columns only)", "impute_numeric",
             description="Fill with column median/mean. Prohibited on key/identifier columns."),
        _opt("quarantine_null_rows", "Quarantine rows with null mandatory value", "quarantine_null_rows",
             description="Route rows with null to quarantine partition. Requires policy ID."),
        _opt("keep_as_is", "Accept nulls (document risk)", "noop"),
    ],
    "null_values": [
        _opt("preserve_and_flag", "Preserve null and flag for review", "preserve_null_and_flag",
             recommended=True,
             description="Keep null as-is, add validation status column. Safe Phase 1 default."),
        _opt("fill_constant", "Fill with approved constant (e.g. 'UNKNOWN')", "fill_null_with_constant",
             description="Replace null with an approved registered constant. Requires policy ID."),
        _opt("impute_numeric", "Statistical imputation (numeric non-key columns only)", "impute_numeric",
             description="Fill with column median/mean. Prohibited on key/identifier columns."),
        _opt("quarantine_null_rows", "Quarantine rows with null mandatory value", "quarantine_null_rows",
             description="Route rows with null to quarantine partition. Requires policy ID."),
        _opt("accept_optional", "Accept as optional (no transform)", "accept_null_as_optional",
             description="Record that null is acceptable for this column. No code emitted."),
        _opt("keep_as_is", "Keep as-is (accept risk)", "noop"),
    ],
    "sentinel_numeric_value": [
        _opt("zero_to_null", "Nullify sentinel values", "zero_to_null", recommended=True, description="Replace numeric sentinel values (e.g. -999, 999999) with NULL."),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "punctuation_only_value": [
        _opt("nullify_punctuation", "Nullify punctuation placeholders", "nullify_punctuation", recommended=True, description="Replace punctuation-only text (e.g. '###') with NULL using dynamic pattern matching."),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "multivariate_outliers": [
        _opt("flag_outliers", "Flag multivariate outliers", "flag_outliers", recommended=True, description="Add boolean audit column flagging row outlier status."),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "all_caps_values": [
        _opt("lowercase", "Standardize to lowercase", "lowercase", recommended=True, description="Convert all values in this column to lowercase."),
        _opt("uppercase", "Standardize to uppercase", "uppercase", description="Convert all values in this column to uppercase."),
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop"),
    ],
    "numeric_outliers_zscore": [
        _opt("flag_outliers", "Flag extreme z-score outliers", "flag_outliers", recommended=True, description="Add a boolean audit column flagging statistical outliers."),
        _opt("clip_outliers", "Clip outliers to IQR bounds", "clip_outliers", description="Cap values at statistical IQR boundaries."),
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop"),
    ],
    "string_length_outlier": [
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop", recommended=True, description="Keep long strings as-is."),
        _opt("exclude_column", "Exclude column from output", "exclude_column", description="Drop column before writing to target."),
    ],
    "custom_rule_violation": [
        _opt("flag_outliers", "Flag rule violations", "flag_outliers", recommended=True, description="Add audit column flagging rows that violate custom business rules."),
        _opt("keep_as_is", "Keep as-is (accept risk)", "noop"),
    ],
    "near_duplicate_rows": [
        _opt("deduplicate", "Deduplicate near-duplicates", "deduplicate", recommended=True, description="Deduplicate rows using primary/composite keys."),
        _opt("keep_as_is", "Keep duplicates as-is", "noop"),
    ],
    "date_format_inconsistency": [
        _opt("parse_dates", "Standardize date formats", "parse_dates", recommended=True, description="Convert and parse mixed date strings into standard ISO dates."),
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop"),
    ],
    "mixed_date_formats": [
        _opt("parse_dates", "Standardize mixed date formats", "parse_dates", recommended=True, description="Convert and parse mixed date strings into standard ISO dates."),
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop"),
    ],
    "at_least_one": [
        _opt("quarantine_all_null", "Quarantine rows where all are null", "at_least_one", recommended=True, description="Move rows to rejects table where all specified columns are NULL."),
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop"),
    ],
    "missing_required_column": [
        _opt("skip_requirement", "Skip requirement (remove from rules)", "skip_requirement", recommended=True, description="Remove this column from the list of required columns for this ingestion."),
        _opt("keep_as_is", "Keep as-is (requires external fix)", "noop", description="Accept the requirement without resolving it in the pipeline (will fail validation)."),
    ],
    "integer_stored_as_float": [
        _opt("cast_to_integer", "Cast float values to integer", "cast_type", recommended=True, description="Cast floating-point representation to integer (Int64 nullable)."),
        _opt("flag_only", "Flag precision anomaly without altering data", "flag_only", description="Add validation status column without casting."),
        _opt("keep_as_is", "Keep as-is", "noop", description="Retain float data type."),
    ],
    "impossible_date": [
        _opt("flag_only", "Preserve and flag impossible dates for review", "flag_only", recommended=True, description="Add validation flag without mutating date values. Safe non-destructive default."),
        _opt("nullify_date", "Nullify impossible dates (requires approval)", "nullify_date", recommended=False, description="Replace calendar impossible dates with NULL after human review and approval."),
        _opt("quarantine_row", "Quarantine rows with impossible dates", "quarantine_row", recommended=False, description="Route rows with impossible dates to quarantine table."),
        _opt("keep_as_is", "Keep as-is", "noop", description="Do not modify date values."),
    ],
    "sentinel_date": [
        _opt("flag_only", "Preserve and flag sentinel dates", "flag_only", recommended=True, description="Flag sentinel dates without mutating date values. Safe non-destructive default."),
        _opt("map_to_null", "Map sentinel dates to NULL (requires approval)", "map_to_null", recommended=False, description="Map sentinel/placeholder dates (e.g. 9999-12-31, 1900-01-01) to NULL after human review and approval."),
        _opt("keep_as_is", "Keep as-is", "noop", description="Retain sentinel dates."),
    ],
    "metadata_manifest_mismatch": [
        _opt("update_manifest", "Update metadata manifest configuration", "update_manifest", recommended=True, description="Synchronize config/metadata_manifest.yaml with current database schema."),
        _opt("ignore_mismatch", "Acknowledge and ignore mismatch", "noop", description="Acknowledge schema difference without modifying manifest."),
    ],
    "business_key_duplicate": [
        _opt("deduplicate_last", "Deduplicate — keep latest record", 
             "deduplicate", recommended=True,
             description="Dedup by business key, keeping the most recent record."),
        _opt("deduplicate_first", "Deduplicate — keep first record", 
             "deduplicate", description="Keep oldest record per business key."),
        _opt("accept_risk", "Allow duplicates (accept business risk)", 
             "noop", description="Pass duplicate business keys to target — document in runbook."),
    ],
    "high_null_percentage": [
        _opt("preserve_and_flag", "Preserve null and flag for review", "preserve_null_and_flag",
             recommended=True,
             description="Keep null as-is, add validation status column. Safe Phase 1 default."),
        _opt("fill_constant", "Fill with approved constant", "fill_null_with_constant",
             description="Replace null with an approved registered constant. Requires policy ID."),
        _opt("impute_numeric", "Statistical imputation (numeric non-key columns only)", "impute_numeric",
             description="Fill with column median/mean. Prohibited on key/identifier columns."),
        _opt("drop_column", "Drop column (>50% null is unreliable)", "drop_column"),
        _opt("accept_optional", "Accept nulls as optional", "accept_null_as_optional"),
    ],
    "orphan_foreign_keys": [
        _opt("reject_orphans", "Reject orphan records (quarantine)", 
             "validate_referential_integrity_or_stage", recommended=True,
             description="Route orphan FK rows to a rejects/staging table."),
        _opt("accept_risk", "Accept risk (document and proceed)", 
             "noop", description="Acknowledge the FK gap and proceed without enforcement."),
    ],
    "dq_gate_warning": [
        _opt("force_unlock", "Acknowledge and proceed (force unlock)", "force_unlock", recommended=True, description="Override the data quality gate for this dataset to allow phase 2 transformations."),
        _opt("keep_as_is", "Keep blocked (resolve DQ issues first)", "noop", description="Keep phase 2 transformations blocked until quality issues are resolved."),
    ],
    "whitespace": [
        _opt("trim", "Trim leading/trailing whitespace", "trim", recommended=True, description="Remove leading and trailing whitespace from string values."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "nulls": [
        _opt("preserve_and_flag", "Preserve null and flag for review", "preserve_null_and_flag",
             recommended=True,
             description="Keep null as-is, add validation status column. Safe Phase 1 default."),
        _opt("fill_constant", "Fill with approved constant", "fill_null_with_constant",
             description="Replace null with an approved registered constant. Requires policy ID."),
        _opt("impute_numeric", "Statistical imputation (numeric non-key columns only)", "impute_numeric",
             description="Fill with column median/mean. Prohibited on key/identifier columns."),
        _opt("quarantine_null_rows", "Quarantine rows with null mandatory value", "quarantine_null_rows",
             description="Route rows with null to quarantine partition. Requires policy ID."),
        _opt("accept_optional", "Accept as optional (no transform)", "accept_null_as_optional",
             description="Record that null is acceptable for this column. No code emitted."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "duplicate_rows": [
        _opt("deduplicate", "Deduplicate (keep first row)", "deduplicate", recommended=True, description="Remove duplicate rows, keeping only the first occurrence."),
        _opt("keep_as_is", "Keep as-is (allow duplicate rows)", "noop"),
    ],
    "empty_string_values": [
        _opt("preserve_and_flag", "Convert to NULL and flag for review", "preserve_null_and_flag",
             recommended=True,
             description="Convert empty strings to NULL and add validation status flag."),
        _opt("fill_constant", "Fill with approved constant", "fill_null_with_constant",
             description="Replace empty strings with an approved constant. Requires policy ID."),
        _opt("keep_as_is", "Keep empty strings as-is", "noop"),
    ],
    "case_inconsistency": [
        _opt("lowercase", "Standardize to lowercase", "lowercase", recommended=True, description="Convert string values to lowercase to resolve case inconsistencies."),
        _opt("uppercase", "Standardize to uppercase", "uppercase", description="Convert string values to uppercase to resolve case inconsistencies."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "invalid_email": [
        _opt("sanitize_email", "Sanitize email format", "sanitize_email", recommended=True, description="Clean whitespace, lowercase, and validate email syntax, nullifying invalid ones."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "invalid_phone": [
        _opt("normalize_phone", "Normalize phone numbers", "normalize_phone", recommended=True, description="Standardize phone formats by removing non-numeric characters."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "invalid_numeric": [
        _opt("coerce_numeric", "Coerce values to numeric", "coerce_numeric", recommended=True, description="Convert string representations of numbers to numeric datatype and nullify/flag invalid ones."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "negative_values": [
        _opt("flag_outliers", "Flag negative values for review", "flag_outliers",
             recommended=True,
             description="Add audit flag for negative values. Does not modify the value. Safe Phase 1 default."),
        _opt("clip_outliers", "Clip to zero (values below 0 → 0)", "clip_outliers",
             description="Cap negative values at zero. Changes business values — approve carefully."),
        _opt("keep_as_is", "Keep as-is (accept negative values)", "noop"),
    ],
    "mixed_scalar_types": [
        _opt("coerce_numeric", "Coerce values to numeric", "coerce_numeric", recommended=True, description="Coerce mixed data types to numeric values and nullify/flag invalid ones."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "mixed_phone_formats": [
        _opt("normalize_phone", "Normalize mixed phone numbers", "normalize_phone", recommended=True, description="Standardize phone formats by removing non-numeric characters."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "internal_whitespace": [
        _opt("trim", "Trim leading/trailing and clean internal whitespace", "trim", recommended=True, description="Standardize whitespace by trimming and compressing consecutive spaces."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "html_tags_in_text": [
        _opt("regex_replace", "Strip HTML tags", "regex_replace", recommended=True, description="Strip HTML tags using regex replacement."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "non_ascii_characters": [
        _opt("regex_replace", "Remove non-ASCII characters", "regex_replace", recommended=True, description="Strip non-ASCII characters using regex replacement."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "control_characters_in_text": [
        _opt("regex_replace", "Remove control characters", "regex_replace", recommended=True, description="Strip control characters from text columns."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "timezone_inconsistency": [
        _opt("parse_dates", "Standardize date-times to UTC / local", "parse_dates", recommended=True, description="Parse date-times to resolve mixed timezone representations."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "implausible_age": [
        _opt("flag_outliers", "Flag out-of-range ages", "flag_outliers", recommended=True,
             description="Add audit column flagging age values outside [0, 150]. Safe Phase 1 default."),
        _opt("clip_outliers", "Clip age to valid range [0, 150]", "clip_outliers",
             description="Cap age values to [0, 150]. Changes business values — approve carefully."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "implausible_percentage": [
        _opt("range_clip", "Clip percentage values to [0, 100]", "range_clip", recommended=True, description="Cap/clip values strictly to range [0, 100]."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "binary_like_column": [
        _opt("standardize_boolean", "Cast to BOOLEAN (Y/N, 1/0, yes/no)", "standardize_boolean", recommended=True, description="Standardize boolean representations to boolean datatype."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "boolean_inconsistency": [
        _opt("standardize_boolean", "Standardize boolean values", "standardize_boolean", recommended=True, description="Standardize boolean representations to boolean datatype."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "ambiguous_boolean": [
        _opt("standardize_boolean", "Standardize boolean values", "standardize_boolean", recommended=True, description="Standardize boolean representations to boolean datatype."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "date_clumping_jan1": [
        _opt("nullify_dummy_dates", "Nullify dummy Jan 1st dates", "nullify_dummy_dates", recommended=True, description="Nullify dates that clump on Jan 1st or are exactly 1900-01-01."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "exact_duplicate_rows": [
        _opt("flag_duplicate_candidates", "Flag exact duplicate group membership", "flag_duplicate_candidates", recommended=True, description="Flag group members; preserve all source rows; write review evidence."),
        _opt("preserve_all_rows", "Preserve all source rows", "noop"),
        _opt("write_review_evidence", "Write review evidence", "noop"),
    ],
    "near_duplicate_rows": [
        _opt("flag_near_duplicate_cluster", "Retain all rows and attach near-duplicate cluster evidence", "flag_near_duplicate_cluster", recommended=True, description="Attach near-duplicate cluster ID metadata; preserve all rows."),
        _opt("retain_without_meta", "Keep rows without attaching review metadata", "noop", description="Retain rows without flags."),
    ],
    "duplicate_primary_key": [
        _opt("flag_duplicate_candidates", "Flag business-key duplicates", "flag_duplicate_candidates", recommended=True, description="Flag primary key duplicates; retain all rows; accept duplicates with warning."),
        _opt("retain_all_rows", "Retain all rows", "noop"),
        _opt("accept_with_warning", "Accept duplicate keys with documented warning", "noop"),
    ],
    "business_key_duplicate": [
        _opt("flag_duplicate_candidates", "Flag business-key duplicates", "flag_duplicate_candidates", recommended=True, description="Flag primary key duplicates; retain all rows; accept duplicates with warning."),
        _opt("retain_all_rows", "Retain all rows", "noop"),
        _opt("accept_with_warning", "Accept duplicate keys with documented warning", "noop"),
    ],
    "suspected_key_high_null_ratio": [
        _opt("flag_missing_key", "Flag missing identifier and preserve null", "flag_missing_key", recommended=True, description="Add flag indicating missing suspected identifier; preserve the null value."),
        _opt("accept_optional", "Accept as optional for this run", "noop", description="Accept null identifiers as optional."),
        _opt("request_owner_declaration", "Request owner declaration", "noop", description="Hold for owner clarification."),
    ],
    "declared_key_null_violation": [
        _opt("flag_missing_declared_key", "Flag missing declared key", "flag_missing_declared_key", recommended=True, description="Add flag indicating missing declared mandatory key."),
        _opt("quarantine_missing_key_rows", "Quarantine missing-key rows", "noop", description="Separate rows with missing declared key into quarantine partition."),
        _opt("accept_waiver", "Accept under approved waiver", "noop", description="Accept missing declared key under official waiver document."),
    ],
    "declared_key_high_null_ratio": [
        _opt("flag_missing_declared_key", "Flag missing declared key", "flag_missing_declared_key", recommended=True, description="Add flag indicating missing declared mandatory key."),
        _opt("quarantine_missing_key_rows", "Quarantine missing-key rows", "noop", description="Separate rows with missing declared key into quarantine partition."),
        _opt("accept_waiver", "Accept under approved waiver", "noop", description="Accept missing declared key under official waiver document."),
    ],
    "placeholder_detected": [
        _opt("normalize_registered_placeholder", "Replace placeholder with NULL", "normalize_registered_placeholder", recommended=True, description="Replace registered placeholders (e.g., 'NA', '?', '-1') with NULL in Phase 1."),
        _opt("keep_as_is", "Keep as-is (allow placeholder)", "noop"),
    ],
    "numeric_value_serialized_as_string": [
        _opt("coerce_numeric", "Coerce numeric values", "coerce_numeric", recommended=True, description="Coerce numeric strings to actual numbers and preserve raw source."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "cross_field_non_negative": [
        _opt("flag_cross_field_violation", "Flag negative value rule violation", "flag_cross_field_violation", recommended=True, description="Flag negative numeric violations; preserve values in Phase 1."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "negative_values_in_non_negative_measure": [
        _opt("flag_cross_field_violation", "Flag negative value rule violation", "flag_cross_field_violation", recommended=True, description="Flag negative numeric violations; preserve values in Phase 1."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "negative_values_review_required": [
        _opt("flag_cross_field_violation", "Flag negative value rule violation", "flag_cross_field_violation", recommended=True, description="Flag negative numeric violations; preserve values in Phase 1."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "orphan_foreign_key_rows": [
        _opt("validate_and_partition_relationship", "Flag relationship orphans", "validate_and_partition_relationship", recommended=True, description="Validate relationship key; partition into valid and quarantine tables."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],

    # ── Proactive suggestions — emitted by transformation_suggester for well-known column patterns ──
    # These have clear, deterministic actions and should never fall through to the dynamic "Unknown" fallback.
    "proactive_trim": [
        _opt("trim", "Trim leading/trailing whitespace (auto)", "trim",
             recommended=True,
             description="Remove leading and trailing whitespace. Safe, non-destructive operation."),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "proactive_parse_dates": [
        _opt("parse_dates", "Parse and standardize date format", "parse_dates",
             recommended=True,
             description="Convert and validate date strings into standard ISO format. Quarantines invalid dates."),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "proactive_sanitize_email": [
        _opt("sanitize_email", "Sanitize and validate email addresses", "sanitize_email",
             recommended=True,
             description="Lowercase, trim, validate email syntax and nullify malformed addresses. Reduces PII exposure."),
        _opt("keep_as_is", "Keep as-is (accept privacy risk)", "noop"),
    ],
    "proactive_normalize_phone": [
        _opt("normalize_phone", "Normalize phone number format", "normalize_phone",
             recommended=True,
             description="Standardize phone numbers by stripping non-numeric characters. Reduces PII exposure."),
        _opt("keep_as_is", "Keep as-is (accept privacy risk)", "noop"),
    ],
    "proactive_lowercase": [
        _opt("lowercase", "Normalize to lowercase", "lowercase",
             recommended=True,
             description="Convert text column values to lowercase for consistent matching and deduplication."),
        _opt("trim_preserve_case", "Trim only (preserve case)", "trim_preserve_case",
             description="Remove whitespace without altering case. Suitable for display fields."),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
}


def manual_review_item_id(dataset: Optional[str], column: Optional[str], issue_type: Optional[str]) -> str:
    ds = (dataset or "_global").strip()
    if ds.lower() == "global":
        ds = "_global"
    col = (column or "*").strip()
    it = (issue_type or "unknown").strip()
    return f"{ds}|{col}|{it}"


def get_resolution_options(issue_type: Optional[str], *, phase: int = 1) -> List[ResolutionOption]:
    from agent.etl_pipeline.issue_registry import canonicalize_issue_type
    from agent.etl_pipeline.action_registry import ACTION_REGISTRY
    norm_type = canonicalize_issue_type(issue_type)
    opts = list(_CATALOG.get(norm_type) or _DEFAULT_OPTIONS)
    
    # In Phase 1: mark Phase 2 options as disabled (not selectable) rather than removing them.
    # This preserves option count for UI display while preventing Phase 2 selection.
    result = []
    for o in opts:
        act = o.get("action")
        act_defn = ACTION_REGISTRY.get(act)
        if act_defn and act_defn.get("phase") is not None:
            if phase == 1 and act_defn.get("phase") != 1:
                # Mark as Phase 2 only — disabled in Phase 1
                o = dict(o)
                o["disabled"] = True
                o["disabled_reason"] = "Available in Phase 2 only"
                o["recommended"] = False
        result.append(o)
        
    if not result:
        result = list(_DEFAULT_OPTIONS)
        
    if not any(o.get("recommended") for o in result):
        # Prefer first non-disabled option as recommended
        for i, o in enumerate(result):
            if not o.get("disabled"):
                result[i] = {**o, "recommended": True}
                break
        else:
            result[0] = {**result[0], "recommended": True}
    return result


def enrich_manual_review_item(
    item: Dict[str, Any],
    llm_recommendation: Optional[Dict[str, Any]] = None,
    conflict: Optional[Any] = None,
) -> Dict[str, Any]:
    """Attach id, resolution_options, default_resolution, status, and optional LLM recommendation or conflict information."""
    out = dict(item)
    iid = out.get("id") or manual_review_item_id(
        out.get("dataset"), out.get("column"), out.get("issue_type")
    )
    out["id"] = iid
    out.setdefault("risk_tier", "standard")
    out.setdefault("status", "pending")
    out.setdefault("selected_resolution", None)

    if (out.get("resolution_options")
        and not llm_recommendation
        and not conflict):
        out["resolution_options"] = [tag_option_readiness(dict(o)) for o in out["resolution_options"]]
        default = next((o.get("id") for o in out["resolution_options"] if (o.get("recommended") or o.get("is_recommended")) and o.get("id")),
                       (out["resolution_options"][0].get("id") or out["resolution_options"][0].get("action") or "keep_as_is") if out["resolution_options"] else "keep_as_is")
        out.setdefault("default_resolution", default)
        out.setdefault("status", "pending")
        out.setdefault("selected_resolution", None)
        return out

    issue_type = str(out.get("issue_type") or "")

    if conflict:
        # Build options from the conflict rules
        if hasattr(conflict, "rules"):
            rules_list = conflict.rules
        elif isinstance(conflict, dict):
            rules_list = conflict.get("rules") or []
        else:
            rules_list = []

        # Sort the rules by priority (provenance)
        def get_prov_val(r):
            if hasattr(r, "provenance"):
                val = r.provenance
            elif isinstance(r, dict):
                val = r.get("provenance")
            else:
                val = 3
            if isinstance(val, int):
                return val
            if hasattr(val, "value"):
                return val.value
            return 3

        sorted_rules = sorted(rules_list, key=get_prov_val)
        
        opts = []
        seen_actions = set()
        
        best_action = None
        if sorted_rules:
            best_rule = sorted_rules[0]
            if hasattr(best_rule, "action"):
                best_action = best_rule.action
            elif isinstance(best_rule, dict):
                best_action = best_rule.get("action")

        for idx, r in enumerate(sorted_rules):
            prov = None
            action = None
            detail = ""
            if hasattr(r, "provenance"):
                prov = r.provenance
                action = r.action
                detail = r.source_detail or ""
            elif isinstance(r, dict):
                prov = r.get("provenance")
                action = r.get("action")
                detail = r.get("source_detail") or ""
            
            prov_val = get_prov_val(r)
            if prov_val == 1:
                prov_label = "📋 Business"
            elif prov_val == 2:
                prov_label = "🧠 Semantic"
            else:
                prov_label = "🔍 Auto"
                
            action_name = str(action or "noop")
            opt_id = f"conflict_opt_{prov_val}_{action_name}"
            
            if action_name in seen_actions:
                continue
            seen_actions.add(action_name)
            
            is_recommended = (action_name == best_action)
            opts.append({
                "id": opt_id,
                "label": f"{prov_label}: {action_name}",
                "action": action_name,
                "recommended": is_recommended,
                "description": f"Conflict resolution option. Source detail: {detail}"
            })
            
        if "noop" not in seen_actions:
            opts.append({
                "id": "keep_as_is",
                "label": "Keep as-is",
                "action": "noop",
                "recommended": not opts,
                "description": "Keep column as-is (skip transformation)."
            })
            
        out["resolution_options"] = opts
        out["conflict_info"] = conflict.model_dump() if hasattr(conflict, "model_dump") else conflict
    else:
        from agent.etl_pipeline.issue_registry import canonicalize_issue_type
        norm_type = canonicalize_issue_type(issue_type)
        col_name = str(out.get("column") or "")
        is_key = any(k in col_name.lower() for k in ("id", "key", "code", "ref"))
        
        if norm_type in ("nulls", "null_values", "high_null_percentage", "empty_string_values", "declared_key_null_violation", "suspected_key_high_null_ratio"):
            if is_key or norm_type in ("declared_key_null_violation", "suspected_key_high_null_ratio"):
                # Key null resolutions: flag, quarantine, or preserve raw
                opts = [
                    _opt("flag_missing_key" if norm_type != "declared_key_null_violation" else "flag_missing_declared_key",
                         "Flag missing key (preserve NULL)",
                         "flag_missing_key" if norm_type != "declared_key_null_violation" else "flag_missing_declared_key",
                         recommended=True,
                         description="Flag the missing key with a companion column. Original NULL is preserved."),
                    _opt("quarantine_missing_key", "Quarantine missing key rows", "quarantine_missing_key",
                         description="Route rows with missing keys to rejects output."),
                    _opt("preserve_missing_suspected_key", "Preserve missing key as-is", "preserve_missing_suspected_key",
                         description="Keep the key as-is for downstream processing."),
                ]
            else:
                # Non-key null resolutions: preserve, impute or constant
                is_numeric = any(n in col_name.lower() for n in ("qty", "qty_clean", "amount", "price", "total", "count", "quantity", "number", "budget"))
                is_date = any(d in col_name.lower() for d in ("date", "time", "inspection", "inspected"))
                if is_numeric:
                    opts = [
                        _opt("preserve_null_and_flag", "Preserve NULL and flag indicator", "preserve_null_and_flag", recommended=True,
                             description="Add boolean flag and preserve NULL value."),
                        _opt("impute_numeric", "Impute numeric (approved median/mean/mode)", "impute_numeric",
                             description="Impute NULL value with explicit strategy and grouping."),
                        _opt("keep_as_is", "Keep as-is (accept risk)", "noop"),
                    ]
                elif is_date:
                    opts = [
                        _opt("preserve_null_and_flag", "Preserve NULL and flag indicator", "preserve_null_and_flag", recommended=True,
                             description="Add boolean flag and preserve NULL value."),
                        _opt("impute_date", "Impute date (approved deterministic policy)", "impute_date",
                             description="Impute NULL date under an approved deterministic policy."),
                        _opt("keep_as_is", "Keep as-is (accept risk)", "noop"),
                    ]
                else:
                    opts = [
                        _opt("preserve_null", "Preserve NULL (no transform)", "preserve_null", recommended=True,
                             description="Keep NULL values as-is. Original string value is preserved."),
                        _opt("fill_constant", "Fill with approved constant", "fill_constant",
                             description="Replace NULL value with an explicit constant."),
                        _opt("keep_as_is", "Keep as-is (accept risk)", "noop"),
                    ]
        elif norm_type in ("exact_duplicate_rows", "duplicate_rows", "near_duplicate_rows", "business_key_duplicate", "duplicate_primary_key"):
            if norm_type == "near_duplicate_rows":
                opts = [
                    _opt("flag_near_duplicate_cluster", "Flag near-duplicate cluster (preserve raw)", "flag_near_duplicate_cluster", recommended=True,
                         description="Assign near_dup_cluster_id and similarity metadata. Never drop rows."),
                    _opt("keep_as_is", "Keep duplicates as-is (accept risk)", "noop"),
                ]
            elif norm_type in ("business_key_duplicate", "duplicate_primary_key"):
                opts = [
                    _opt("flag_business_key_duplicate", "Flag business-key duplicates (preserve raw)", "flag_business_key_duplicate", recommended=True,
                         description="Flag duplicate business keys with stable group IDs. No survivorship."),
                    _opt("deduplicate_last", "Deduplicate — keep latest record", "deduplicate",
                         description="Dedup by business key, keeping the most recent record."),
                    _opt("deduplicate_first", "Deduplicate — keep first record", "deduplicate",
                         description="Keep oldest record per business key."),
                    _opt("keep_as_is", "Keep duplicates as-is (accept risk)", "noop"),
                ]
            else:
                opts = [
                    _opt("flag_duplicate_candidates", "Flag duplicate candidates (preserve raw)", "flag_duplicate_candidates", recommended=True,
                         description="Assign duplicate_group_id. Never drop rows."),
                    _opt("deduplicate", "Deduplicate (keep first row)", "deduplicate",
                         description="Remove exact duplicate rows, keeping only the first occurrence."),
                    _opt("keep_as_is", "Keep duplicates as-is (accept risk)", "noop"),
                ]
        else:
            if norm_type in _CATALOG:
                opts = get_resolution_options(norm_type)
            else:
                opts = get_dynamic_resolution_options(issue_type, out)

        if llm_recommendation:
            if isinstance(llm_recommendation, str):
                llm_recommendation = {
                    "suggested_fix": llm_recommendation,
                    "why_it_matters": f"LLM recommended fix: {llm_recommendation}",
                    "risk": "standard",
                    "mapped_action": llm_recommendation
                }
            out["llm_recommendation"] = llm_recommendation
            from agent.etl_pipeline.llm_rec_mapper import map_llm_recommendation_to_action, compute_llm_confidence
            llm_action = map_llm_recommendation_to_action(llm_recommendation)
            confidence = compute_llm_confidence(llm_recommendation)

            for opt in opts:
                opt["recommended"] = False

            authority = llm_recommendation.get("authority_source") or "LLM_ADVISORY"
            is_llm_advisory = authority == "LLM_ADVISORY"
            rec_label = (
                f"AI Recommended: {llm_recommendation.get('suggested_fix')}"
                if is_llm_advisory
                else f"Deterministic recommendation: {llm_recommendation.get('suggested_fix')}"
            )
            llm_opt = {
                "id": "llm_suggested",
                "label": rec_label,
                "action": llm_action or "noop",
                "recommended": True,
                "description": f"Why it matters: {llm_recommendation.get('why_it_matters')}. Risk: {llm_recommendation.get('risk')}",
                "authority_source": authority,
                "llm_metadata": {
                    "example_sql": llm_recommendation.get("example_sql"),
                    "example_pandas": llm_recommendation.get("example_pandas"),
                    "confidence": confidence if is_llm_advisory else None,
                }
            }
            opts = [llm_opt] + [opt for opt in opts if opt.get("id") != "llm_suggested"]

        out["resolution_options"] = opts

    opts = [tag_option_readiness(dict(o)) for o in out.get("resolution_options", [])]
    out["resolution_options"] = opts
    default = next((o.get("id") for o in opts if (o.get("recommended") or o.get("is_recommended")) and o.get("id")),
                   (opts[0].get("id") or opts[0].get("action") or "keep_as_is") if opts else "keep_as_is")
    out.setdefault("default_resolution", default)
    out.setdefault("status", "pending")
    out.setdefault("selected_resolution", None)
    return out


import os
import json

_CACHE_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "config",
    "dynamic_options_cache.json"
)

def _load_dynamic_options_cache() -> Dict[str, List[ResolutionOption]]:
    try:
        if os.path.exists(_CACHE_FILE):
            with open(_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def _save_dynamic_options_cache(cache: Dict[str, List[ResolutionOption]]):
    try:
        os.makedirs(os.path.dirname(_CACHE_FILE), exist_ok=True)
        with open(_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)
    except Exception:
        pass

_DYNAMIC_OPTIONS_CACHE = _load_dynamic_options_cache()


from agent.etl_pipeline.llm_resolution_fallback import (
    get_cached_resolution,
    resolve_unmapped_issue_async,
    LLMResolutionBudget,
    DHARA_LLM_RESOLUTION_CONCURRENCY,
    _make_budget_exhausted_fallback,
)
from agent.etl_pipeline.issue_identity import make_resolution_signature


def get_dynamic_resolution_options(
    issue_type: str,
    item: Dict[str, Any],
    *,
    allow_llm_call: bool = False
) -> List[ResolutionOption]:
    """
    Check cache for unmapped issue type resolutions.
    Always returns immediately without blocking (NO asyncio.run() in library code).
    """
    col_sem = item.get("semantic_type") or (item.get("col_stats") or {}).get("semantic_type")
    msg = item.get("message") or item.get("guidance")
    sig = make_resolution_signature(issue_type, col_sem, msg)

    cached = get_cached_resolution(sig)
    if cached:
        return cached

    # Safe fallback if not yet resolved by async Phase B
    return _make_budget_exhausted_fallback(issue_type, "unmapped_pending_resolution")


async def enrich_unmapped_items_with_llm(
    items: List[Dict[str, Any]],
    *,
    max_calls: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Phase B: Asynchronously enrich unmapped manual review items using LLM resolution fallback.
    Respects budget, deduplication by signature, concurrency semaphore, and timeouts.
    """
    budget = LLMResolutionBudget(max_calls=max_calls) if max_calls else LLMResolutionBudget()
    semaphore = asyncio.Semaphore(DHARA_LLM_RESOLUTION_CONCURRENCY)

    tasks = []
    unmapped_indices = []

    for idx, item in enumerate(items):
        opts = item.get("resolution_options") or []
        # Check if item is unmapped (options come from unmapped fallback)
        is_unmapped = (
            not opts
            or any(o.get("resolution_source") in ("unmapped_pending_resolution", "default_safe") for o in opts)
            or (len(opts) == 1 and opts[0].get("id") == "keep_as_is")
        )
        if is_unmapped:
            it = item.get("issue_type") or "unknown"
            col = item.get("column")
            col_sem = item.get("semantic_type") or (item.get("col_stats") or {}).get("semantic_type")
            msg = item.get("message") or item.get("guidance")
            tasks.append(
                resolve_unmapped_issue_async(
                    it, col, col_sem, msg, budget, semaphore
                )
            )
            unmapped_indices.append(idx)

    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, res in zip(unmapped_indices, results):
            if isinstance(res, list) and res:
                res_tagged = [tag_option_readiness(dict(o)) for o in res]
                items[idx]["resolution_options"] = res_tagged
                rec = next((o.get("id") for o in res_tagged if (o.get("recommended") or o.get("is_recommended")) and o.get("id")),
                           (res_tagged[0].get("id") or res_tagged[0].get("action") or "keep_as_is") if res_tagged else "keep_as_is")
                items[idx]["default_resolution"] = rec

    return items


def enrich_manual_review_item_static(
    item: Dict[str, Any],
    *,
    llm_recommendation: Optional[Any] = None
) -> Dict[str, Any]:
    """Pure synchronous static catalog enrichment (Phase A)."""
    return enrich_manual_review_item(item, llm_recommendation=llm_recommendation)


def action_for_resolution(issue_type: str, resolution_id: str, options: Optional[List[ResolutionOption]] = None) -> Optional[str]:
    rid = str(resolution_id or "").strip()
    
    # Search provided options first (most specific)
    search_pools = []
    if options:
        search_pools.append(options)
    # Then catalog
    catalog_opts = _CATALOG.get(str(issue_type or "").lower()) or []
    if catalog_opts:
        search_pools.append(catalog_opts)
    # Finally default
    search_pools.append(_DEFAULT_OPTIONS)
    
    for pool in search_pools:
        for o in pool:
            if o.get("id") == rid:
                return str(o.get("action") or "noop")
    
    # "skip" alias → find first noop/keep_as_is in options
    if rid == "skip":
        for pool in search_pools:
            for o in pool:
                if o.get("action") in ("noop", "keep_as_is") or o.get("id") in ("keep_as_is", "skip_requirement"):
                    return str(o.get("action") or "noop")
        return "noop"

    # "recommended" alias → find first recommended option in options or catalog
    if rid in ("recommended", "recommended_option"):
        for pool in search_pools:
            for o in pool:
                if o.get("recommended") and not o.get("disabled") and o.get("action"):
                    return str(o.get("action"))
        for pool in search_pools:
            for o in pool:
                if not o.get("disabled") and o.get("action") and o.get("action") != "noop":
                    return str(o.get("action"))

    if rid in RESOLUTION_TO_ACTION_MAP:
        return RESOLUTION_TO_ACTION_MAP[rid]

    # If rid is already a valid action name registered in ACTION_REGISTRY, accept it directly
    from agent.etl_pipeline.action_registry import ACTION_REGISTRY
    if rid in ACTION_REGISTRY:
        return rid

    return None
def is_skip_action(action: str) -> bool:
    return (action or "").strip().lower() in _SKIP_ACTIONS


def get_catalog_guidance(issue_type: str) -> str:
    """
    Returns the recommended resolution description from _CATALOG for a given issue type.
    """
    if not issue_type:
        return ""
    options = _CATALOG.get(issue_type) or []
    rec_opt = next((o for o in options if o.get("recommended")), None)
    if rec_opt:
        return rec_opt.get("description") or rec_opt.get("label") or ""
    return ""

