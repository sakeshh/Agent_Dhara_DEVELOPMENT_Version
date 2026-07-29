"""
Structured resolution options for manual_review plan items.
Each option maps to a codegen action (or noop for keep-as-is).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

ResolutionOption = Dict[str, Any]

_SKIP_ACTIONS = frozenset({"noop", "keep_as_is"})


def _opt(
    opt_id: str,
    label: str,
    action: str,
    *,
    recommended: bool = False,
    description: str = "",
) -> ResolutionOption:
    return {
        "id": opt_id,
        "label": label,
        "action": action,
        "recommended": recommended,
        "description": description,
    }


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
        _opt("fill_nulls", "Fill nulls (median/mean)", "fill_nulls_simple", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
    ],
    "null_values": [
        _opt("fill_nulls", "Fill nulls (median/mean)", "fill_nulls_simple", recommended=True),
        _opt("keep_as_is", "Keep as-is (skip)", "noop"),
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
    "duplicate_insensitive_values": [
        _opt("lowercase", "Standardize case (lowercase) and trim", "lowercase", recommended=True, description="Trim whitespace and standardize case to lowercase to eliminate duplicates."),
        _opt("uppercase", "Standardize case (uppercase) and trim", "uppercase", description="Trim whitespace and standardize case to uppercase to eliminate duplicates."),
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
        _opt("fill_nulls", "Fill nulls (median/mean/mode)", 
             "fill_nulls_simple", recommended=True),
        _opt("drop_column", "Drop column (>50% null is unreliable)", "drop_column"),
        _opt("accept_risk", "Accept nulls (pass through as-is)", "noop"),
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
        _opt("fill_nulls", "Fill nulls (median/mean/mode/constant)", "fill_nulls_simple", recommended=True, description="Impute null values using column statistics or a default value."),
        _opt("keep_as_is", "Keep as-is", "noop"),
    ],
    "duplicate_rows": [
        _opt("deduplicate", "Deduplicate (keep first row)", "deduplicate", recommended=True, description="Remove duplicate rows, keeping only the first occurrence."),
        _opt("keep_as_is", "Keep as-is (allow duplicate rows)", "noop"),
    ],
    "empty_string_values": [
        _opt("fill_nulls", "Convert to NULL and impute", "fill_nulls_simple", recommended=True, description="Convert empty/blank strings to NULL and fill them."),
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
        _opt("clip_or_flag", "Clip or flag negative values", "clip_or_flag", recommended=True, description="Handle negative values based on outlier strategy (clip to zero or flag)."),
        _opt("keep_as_is", "Keep negative values as-is", "noop"),
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
        _opt("clip_or_flag", "Clip or flag out-of-range ages", "clip_or_flag", recommended=True, description="Clip age values to range [0, 150] or flag them."),
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
}


def manual_review_item_id(dataset: Optional[str], column: Optional[str], issue_type: Optional[str]) -> str:
    ds = (dataset or "_global").strip()
    if ds.lower() == "global":
        ds = "_global"
    col = (column or "*").strip()
    it = (issue_type or "unknown").strip()
    return f"{ds}|{col}|{it}"


def get_resolution_options(issue_type: Optional[str]) -> List[ResolutionOption]:
    it = (issue_type or "").strip().lower()
    opts = list(_CATALOG.get(it) or _DEFAULT_OPTIONS)
    if not any(o.get("recommended") for o in opts):
        opts[0] = {**opts[0], "recommended": True}
    return opts


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
        default = next((o["id"] for o in out["resolution_options"] if o.get("recommended")),
                       out["resolution_options"][0]["id"] if out["resolution_options"] else "keep_as_is")
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
        if issue_type.strip().lower() in _CATALOG:
            opts = get_resolution_options(issue_type)
        else:
            opts = get_dynamic_resolution_options(issue_type, out)

        if llm_recommendation:
            out["llm_recommendation"] = llm_recommendation
            from agent.etl_pipeline.llm_rec_mapper import map_llm_recommendation_to_action, compute_llm_confidence
            llm_action = map_llm_recommendation_to_action(llm_recommendation)
            confidence = compute_llm_confidence(llm_recommendation)

            for opt in opts:
                opt["recommended"] = False

            llm_opt = {
                "id": "llm_suggested",
                "label": f"AI Recommended: {llm_recommendation.get('suggested_fix')}",
                "action": llm_action or "noop",
                "recommended": True,
                "description": f"Why it matters: {llm_recommendation.get('why_it_matters')}. Risk: {llm_recommendation.get('risk')}",
                "llm_metadata": {
                    "example_sql": llm_recommendation.get("example_sql"),
                    "example_pandas": llm_recommendation.get("example_pandas"),
                    "confidence": confidence,
                }
            }
            opts = [llm_opt] + [opt for opt in opts if opt.get("id") != "llm_suggested"]

        out["resolution_options"] = opts

    default = next((o["id"] for o in opts if o.get("recommended")), opts[0]["id"] if opts else "keep_as_is")
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


def get_dynamic_resolution_options(
    issue_type: str,
    item: Dict[str, Any],
    *,
    allow_llm_call: bool = False
) -> List[ResolutionOption]:
    """
    allow_llm_call=False (default): return cached or fallback immediately.
    allow_llm_call=True: may make LLM call (only from background worker).
    """
    cache_key = issue_type.strip().lower()
    global _DYNAMIC_OPTIONS_CACHE
    _DYNAMIC_OPTIONS_CACHE = _load_dynamic_options_cache()
    if cache_key in _DYNAMIC_OPTIONS_CACHE:
        return _DYNAMIC_OPTIONS_CACHE[cache_key]

    if not allow_llm_call:
        # Return minimal safe fallback — never block plan build
        fallback = [
            _opt("keep_as_is", "Keep as-is (review manually)", "noop",
                 recommended=True,
                 description=f"Unknown issue type '{issue_type}' — review with your data team.")
        ]
        return fallback

    # LLM call logic (only when allow_llm_call=True)
    import json
    import logging
    from agent.model_config import load_llm_config

    opts = [
        _opt("keep_as_is", "Keep as-is (skip in ETL)", "noop", recommended=True, description="No transform; document in runbook.")
    ]

    cfg = load_llm_config()
    if not cfg:
        _DYNAMIC_OPTIONS_CACHE[cache_key] = opts
        _save_dynamic_options_cache(_DYNAMIC_OPTIONS_CACHE)
        return opts

    client = None
    try:
        if cfg.provider == "azure_openai":
            from openai import AzureOpenAI
            client = AzureOpenAI(
                azure_endpoint=cfg.endpoint,
                api_key=cfg.api_key,
                api_version=cfg.api_version or "2024-02-01",
            )
        elif cfg.provider == "openai":
            from openai import OpenAI
            client = OpenAI(api_key=cfg.api_key)
    except Exception as e:
        logger = logging.getLogger("agent.manual_review_catalog")
        logger.error(f"Failed to initialize OpenAI client: {e}")
        _DYNAMIC_OPTIONS_CACHE[cache_key] = opts
        _save_dynamic_options_cache(_DYNAMIC_OPTIONS_CACHE)
        return opts

    if not client:
        _DYNAMIC_OPTIONS_CACHE[cache_key] = opts
        _save_dynamic_options_cache(_DYNAMIC_OPTIONS_CACHE)
        return opts

    system_prompt = (
        "You are an expert ETL engineer. We detected a data quality issue in a dataset.\n"
        "Generate 1 or 2 appropriate cleanup actions from the following allowed set of standard actions:\n"
        "- noop: Keep as-is, do not modify or transform the data.\n"
        "- drop_column: Drop the entire column from the dataset.\n"
        "- exclude_column: Exclude this column from output.\n"
        "- deduplicate: Deduplicate rows based on this column.\n"
        "- flag_outliers: Add a boolean audit column to flag these outlier values.\n"
        "- fill_nulls_simple: Fill missing values with a default value (e.g. median/mean/mode/constant).\n"
        "- zero_to_null: Replace sentinel/magic/placeholder values (e.g. -999, '###') with NULL.\n"
        "- lowercase: Standardize string case to lowercase.\n"
        "- uppercase: Standardize string case to uppercase.\n"
        "- parse_dates: Standardize/parse mixed date strings into clean ISO dates.\n\n"
        "Return a JSON object exactly formatted like this:\n"
        "{\n"
        "  \"options\": [\n"
        "    {\n"
        "      \"id\": \"unique_id_for_option\",\n"
        "      \"label\": \"User-friendly label\",\n"
        "      \"action\": \"one of the allowed standard actions\",\n"
        "      \"recommended\": true/false,\n"
        "      \"description\": \"Description of the cleanup action\"\n"
        "    }\n"
        "  ]\n"
        "}"
    )

    user_prompt = f"Issue Type: {issue_type}\nIssue Context: {json.dumps(item)}"

    try:
        resp = client.chat.completions.create(
            model=cfg.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content or "{}"
        parsed = json.loads(raw)
        llm_opts = parsed.get("options") or []
        for opt in llm_opts:
            if isinstance(opt, dict) and opt.get("id") and opt.get("label") and opt.get("action"):
                action = opt.get("action")
                allowed_actions = {
                    "noop", "drop_column", "exclude_column", "deduplicate",
                    "flag_outliers", "fill_nulls_simple", "zero_to_null",
                    "lowercase", "uppercase", "parse_dates"
                }
                if action not in allowed_actions:
                    action = "noop"

                if opt.get("id") == "keep_as_is" or action == "noop":
                    continue

                opts.append({
                    "id": opt.get("id"),
                    "label": opt.get("label"),
                    "action": action,
                    "recommended": bool(opt.get("recommended")),
                    "description": opt.get("description", "")
                })
    except Exception as e:
        logger = logging.getLogger("agent.manual_review_catalog")
        logger.error(f"Failed to generate dynamic resolution options: {e}")

    if not any(o.get("recommended") for o in opts):
        opts[0]["recommended"] = True

    _DYNAMIC_OPTIONS_CACHE[cache_key] = opts
    _save_dynamic_options_cache(_DYNAMIC_OPTIONS_CACHE)
    return opts


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

    # Alias map for option IDs that do not match action names
    _RESOLUTION_ID_FALLBACK: Dict[str, str] = {
        "deduplicate_last":  "deduplicate",
        "deduplicate_first": "deduplicate",
        "fill_nulls":        "fill_nulls_simple",
        "abort_pipeline":    "noop",
        "accept_risk":       "noop",
        "quarantine_all_null": "validate_referential_integrity_or_stage",
        "reject_orphans":    "validate_referential_integrity_or_stage",
        "flag_outliers":     "flag_outliers",
        "flag":              "clip_or_flag",
        "fill_null":         "fill_or_drop",
        "force_unlock":      "force_unlock",
        "skip_requirement":  "skip_requirement",
        "keep_as_is":        "noop",
    }
    if rid in _RESOLUTION_ID_FALLBACK:
        return _RESOLUTION_ID_FALLBACK[rid]
    
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

