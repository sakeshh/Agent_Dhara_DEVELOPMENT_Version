"""
LLM-assisted DQ cleaning recommendations.

Consumes merged DQ issues and produces a prioritized cleaning plan.
Designed for production: returns structured JSON; falls back to rule-based hints if LLM not configured.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from agent.model_config import load_llm_config
from agent.openai_usage import usage_dict_from_response
from agent.session_store import SessionJSONEncoder

logger = logging.getLogger("agent.dq_recommendations")


@dataclass(frozen=True)
class DQRecommendations:
    recommendations: List[Dict[str, Any]]
    summary: Dict[str, Any]


_SYSTEM = """
You are a data quality remediation planner.
Given data quality issues across datasets, output a prioritized, actionable cleaning plan.

Return ONLY valid JSON. No markdown.

JSON schema:
{
  "recommendations": [
    {
      "priority": 1,
      "dataset": "string|null",
      "column": "string|null",
      "issue_type": "string",
      "severity": "high|medium|low",
      "why_it_matters": "string",
      "suggested_fix": "string",
      "mapped_action": "trim|fill_nulls_simple|cast_type|regex_replace|deduplicate|parse_dates|sanitize_email|normalize_phone|clip_or_flag|flag_outliers|zero_to_null|lowercase|uppercase|exclude_column|drop_column|noop",
      "example_sql": "string|null",
      "example_pandas": "string|null",
      "risk": "string"
    }
  ],
  "summary": {
    "top_themes": ["string", "..."],
    "notes": "string"
  }
}
""".strip()


def get_pandas_code(
    col: Optional[str],
    issue_type: str,
    action_id: Optional[str] = None,
    related_ds: Optional[str] = None,
    related_col: Optional[str] = None
) -> Optional[str]:
    # No-op actions generate no transformation or quarantine code
    if action_id in ("keep_as_is", "skip_etl", "noop"):
        return None

    col_safe = col or "column_name"
    col_l = col_safe.lower().strip()
    itype = issue_type.lower()
    aid = (action_id or "").lower()
    
    is_flag_only = (aid == "flag_only")
    is_nullify = (aid == "nullify_clean_value")
    is_quarantine = (aid in ("quarantine_row", "quarantine", "drop_invalid_rows"))

    NON_NUMERIC_SEMANTICS = {
        "country", "country_code", "customer_id", "order_id", "payment_id",
        "client_id", "user_id", "status", "date", "phone", "email", "full_name"
    }

    is_date_col = any(k in col_l for k in ["date", "created", "timestamp", "time", "dob", "birth"])

    # 1a. Dedicated Date Sentinel Remediation (e.g. CreatedDate = 'N/A', 'NA', 'NULL')
    if itype == "sentinel_date" or (is_date_col and "sentinel" in itype):
        clean_row_clause = "clean_df = df.copy()\nquarantine_df = df.iloc[0:0].copy()" if not is_quarantine else "quarantine_df = df[is_sentinel_date].copy()\nclean_df = df[~is_sentinel_date].copy()"
        quarantine_reason_clause = "df['quarantine_reason'] = np.where(is_sentinel_date, 'Configured sentinel date detected', None)\n" if is_quarantine else ""
        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"# Normalize configured sentinel dates without numeric casting\n"
            f"sentinel_dates = ['n/a', 'na', 'null', 'none', 'unknown', '9999-12-31', '1900-01-01']\n"
            f"raw = df['{col_safe}']\n"
            f"normalized = raw.astype(str).str.strip().str.lower()\n"
            f"is_sentinel_date = normalized.isin(sentinel_dates)\n"
            f"df['{col_safe}_raw'] = raw\n"
            f"df['{col_safe}_clean'] = pd.to_datetime(raw.where(~is_sentinel_date, None), errors='coerce')\n"
            f"df['{col_safe}_validation_status'] = np.select(\n"
            f"    [raw.isna(), is_sentinel_date],\n"
            f"    ['source_null', 'sentinel_date'],\n"
            f"    default='valid'\n"
            f")\n"
            f"df['{col_safe}_validation_reason'] = np.where(is_sentinel_date, 'Configured sentinel or placeholder date', None)\n"
            f"{quarantine_reason_clause}"
            f"{clean_row_clause}"
        )

    # 1. Sentinel / Magic Number Remediation (e.g. InstructorID = 999)
    if "sentinel" in itype:
        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"# Remediate configured sentinel identifiers (e.g. 999, -999, 9999999)\n"
            f"sentinel_values = [999, -999, 9999999]\n"
            f"raw = df['{col_safe}']\n"
            f"coerced = pd.to_numeric(raw, errors='coerce')\n"
            f"is_sentinel = coerced.isin(sentinel_values)\n"
            f"df['{col_safe}_raw'] = raw\n"
            f"df['{col_safe}_clean'] = coerced.where(~is_sentinel, None)\n"
            f"df['{col_safe}_validation_status'] = np.select(\n"
            f"    [raw.isna(), is_sentinel],\n"
            f"    ['source_null', 'sentinel_value'],\n"
            f"    default='valid'\n"
            f")\n"
            f"df['{col_safe}_validation_reason'] = np.where(is_sentinel, 'Configured sentinel identifier', None)\n"
            f"df['quarantine_reason'] = np.where(is_sentinel, 'Configured sentinel identifier detected', None)\n"
            f"quarantine_df = df[is_sentinel].copy()\n"
            f"clean_df = df[~is_sentinel].copy()"
        )

    # 2. Implausible Age Remediation
    if "age" in itype or "implausible_age" in itype or (col_l == "age" and ("range" in itype or "plausib" in itype or "implausible" in itype or "outlier" in itype or "validity" in itype)):
        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"# DRY_RUN: Validate Age within [0, 150], preserve raw values, isolate nulls vs implausible domain violations\n"
            f"raw_age = df['{col_safe}']\n"
            f"num_age = pd.to_numeric(raw_age, errors='coerce')\n"
            f"is_null = raw_age.isna()\n"
            f"is_unparseable = raw_age.notna() & num_age.isna()\n"
            f"is_implausible = num_age.notna() & ((num_age < 0) | (num_age > 150))\n"
            f"df['{col_safe}_raw'] = raw_age\n"
            f"df['{col_safe}_clean'] = num_age.where(~is_implausible, None)\n"
            f"df['{col_safe}_validation_status'] = np.select(\n"
            f"    [is_null, is_unparseable, is_implausible],\n"
            f"    ['source_null', 'unparseable_age', 'implausible_age'],\n"
            f"    default='valid'\n"
            f")\n"
            f"df['{col_safe}_validation_reason'] = np.select(\n"
            f"    [is_unparseable, is_implausible],\n"
            f"    ['Value cannot be parsed as integer age', 'Age out of plausible range [0, 150]'],\n"
            f"    default=None\n"
            f")\n"
            f"df['quarantine_reason'] = np.where(is_implausible, 'Age out of plausible range [0, 150]', None)\n"
            f"quarantine_df = df[is_implausible].copy()\n"
            f"clean_df = df[~is_implausible].copy()"
        )

    # 3. Integer Stored as Float (Conformity)
    if "integer_stored_as_float" in itype or "float_stored_as_integer" in itype:
        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"# Conformity: Cast floating-point integer representations to nullable Int64\n"
            f"raw = df['{col_safe}']\n"
            f"df['{col_safe}_raw'] = raw\n"
            f"df['{col_safe}_clean'] = pd.to_numeric(raw, errors='coerce').astype('Int64')\n"
            f"df['{col_safe}_validation_status'] = np.where(raw.isna(), 'source_null', 'conformity_cast_int64')\n"
            f"df['{col_safe}_validation_reason'] = np.where(raw.notna(), 'Cleanly converted float representation to nullable Int64', None)"
        )

    # 4. Invalid Phone Number Remediation
    if "phone" in itype:
        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"# Validate phone format and length, preserve raw values and isolate invalid entries\n"
            f"raw = df['{col_safe}']\n"
            f"cleaned_str = raw.astype('string').str.strip()\n"
            f"digits = cleaned_str.str.replace(r'[^0-9]', '', regex=True)\n"
            f"is_null = raw.isna() | (cleaned_str == '')\n"
            f"is_placeholder = cleaned_str.str.lower().isin(['unknown', 'n/a', 'na', 'none', 'null', '###', '-'])\n"
            f"is_repeated = digits.str.match(r'^(\\d)\\1{{4,}}$').fillna(False)\n"
            f"is_valid = ~is_null & ~is_placeholder & ~is_repeated & (digits.str.len() >= 10) & (digits.str.len() <= 15)\n"
            f"is_invalid = ~is_null & ~is_placeholder & ~is_valid\n"
            f"df['{col_safe}_raw'] = raw\n"
            f"df['{col_safe}_clean'] = digits.where(is_valid, None)\n"
            f"df['{col_safe}_validation_status'] = np.select(\n"
            f"    [is_null, is_placeholder, is_repeated, is_invalid],\n"
            f"    ['source_null', 'placeholder_phone', 'repeated_digit_phone', 'invalid_phone_format'],\n"
            f"    default='valid'\n"
            f")\n"
            f"df['{col_safe}_validation_reason'] = np.select(\n"
            f"    [is_repeated, is_invalid, is_placeholder],\n"
            f"    ['Implausible repeated digits phone number', 'Phone number format or length violation', 'Configured placeholder phone'],\n"
            f"    default=None\n"
            f")\n"
            f"df['quarantine_reason'] = np.where(is_invalid | is_repeated, 'Invalid phone number format', None)\n"
            f"quarantine_df = df[is_invalid | is_repeated].copy()\n"
            f"clean_df = df[~(is_invalid | is_repeated)].copy()"
        )

    # 5. Invalid Email Remediation
    if "email" in itype:
        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"# Validate email format, preserve raw values and isolate invalid entries\n"
            f"raw = df['{col_safe}']\n"
            f"cleaned_str = raw.astype('string').str.strip().str.lower()\n"
            f"is_null = raw.isna() | (cleaned_str == '')\n"
            f"is_placeholder = cleaned_str.isin(['unknown', 'n/a', 'na', 'none', 'null', 'test'])\n"
            f"is_valid = cleaned_str.str.match(r'^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$').fillna(False)\n"
            f"is_invalid = ~is_null & ~is_placeholder & ~is_valid\n"
            f"df['{col_safe}_raw'] = raw\n"
            f"df['{col_safe}_clean'] = cleaned_str.where(is_valid, None)\n"
            f"df['{col_safe}_validation_status'] = np.select(\n"
            f"    [is_null, is_placeholder, is_invalid],\n"
            f"    ['source_null', 'placeholder_email', 'invalid_email_format'],\n"
            f"    default='valid'\n"
            f")\n"
            f"df['{col_safe}_validation_reason'] = np.select(\n"
            f"    [is_invalid, is_placeholder],\n"
            f"    ['Email address format violation (missing domain, invalid TLD, or consecutive @)', 'Configured placeholder email'],\n"
            f"    default=None\n"
            f")\n"
            f"df['quarantine_reason'] = np.where(is_invalid, 'Invalid email format', None)\n"
            f"quarantine_df = df[is_invalid].copy()\n"
            f"clean_df = df[~is_invalid].copy()"
        )

    # 6. Impossible / Invalid Calendar Date Remediation
    if "impossible_date" in itype or "calendar_date" in itype:
        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"# Isolate impossible calendar dates (e.g. 2022-02-30, 2024-05-40, 2023-13-01) from placeholders/format errors\n"
            f"raw = df['{col_safe}']\n"
            f"clean_str = raw.astype('string').str.strip()\n"
            f"is_null = raw.isna() | (clean_str == '')\n"
            f"is_placeholder = clean_str.str.lower().isin(['n/a', 'na', 'null', 'none', 'unknown', '???', '###'])\n"
            f"is_iso_like = clean_str.str.match(r'^\\d{{4}}-\\d{{2}}-\\d{{2}}$').fillna(False)\n"
            f"parsed = pd.to_datetime(clean_str, format='%Y-%m-%d', errors='coerce')\n"
            f"is_impossible = is_iso_like & parsed.isna()\n"
            f"is_format_error = ~is_null & ~is_placeholder & ~is_iso_like\n"
            f"df['{col_safe}_raw'] = raw\n"
            f"df['{col_safe}_clean'] = parsed\n"
            f"df['{col_safe}_validation_status'] = np.select(\n"
            f"    [is_null, is_placeholder, is_impossible, is_format_error],\n"
            f"    ['source_null', 'placeholder_date', 'impossible_calendar_date', 'invalid_date_format'],\n"
            f"    default='valid'\n"
            f")\n"
            f"df['{col_safe}_validation_reason'] = np.select(\n"
            f"    [is_impossible, is_format_error, is_placeholder],\n"
            f"    ['Impossible calendar date (invalid month or day for month)', 'Date structure does not conform to expected format', 'Configured placeholder token'],\n"
            f"    default=None\n"
            f")\n"
            f"df['quarantine_reason'] = np.where(is_impossible, 'Impossible calendar date', None)\n"
            f"quarantine_df = df[is_impossible].copy()\n"
            f"clean_df = df[~is_impossible].copy()"
        )

    # 7. Non-Negative Measure / Price Remediation
    if "negative" in itype:
        neg_msg = "Age cannot be negative" if "age" in col_l else ("Catalog price cannot be negative" if any(k in col_l for k in ["price", "cost", "amount", "charge", "rate", "production_qty"]) else f"{col_safe} cannot be negative")
        if is_flag_only:
            clean_col_assign = f"df['{col_safe}_clean'] = num_val\n"
            quarantine_reason_clause = ""
            clean_row_clause = f"clean_df = df.copy()\nquarantine_df = df.iloc[0:0].copy()"
        elif is_nullify:
            clean_col_assign = f"df['{col_safe}_clean'] = num_val.where(~is_negative, None)\n"
            quarantine_reason_clause = ""
            clean_row_clause = f"clean_df = df.copy()\nquarantine_df = df.iloc[0:0].copy()"
        else:
            clean_col_assign = f"df['{col_safe}_clean'] = num_val.where(~is_negative, None)\n"
            quarantine_reason_clause = f"df['quarantine_reason'] = np.where(is_negative, 'Non-negative measure violation (negative value)', None)\n"
            clean_row_clause = f"# Filter using validated boolean predicates, never raw column condition\nquarantine_df = df[is_negative | is_unparseable].copy()\nclean_df = df[~is_negative & ~is_unparseable].copy()"

        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"# Preserve raw amount for auditability; isolate negative entries from nulls/unparseable values\n"
            f"raw_val = df['{col_safe}']\n"
            f"num_val = pd.to_numeric(raw_val, errors='coerce')\n"
            f"is_null = raw_val.isna()\n"
            f"is_unparseable = raw_val.notna() & num_val.isna()\n"
            f"is_negative = num_val.notna() & (num_val < 0)\n"
            f"df['{col_safe}_raw'] = raw_val\n"
            f"{clean_col_assign}"
            f"df['{col_safe}_validation_status'] = np.select(\n"
            f"    [is_null, is_unparseable, is_negative],\n"
            f"    ['source_null', 'unparseable_numeric', 'negative_value_review_required'],\n"
            f"    default='valid'\n"
            f")\n"
            f"df['{col_safe}_validation_reason'] = np.select(\n"
            f"    [is_unparseable, is_negative],\n"
            f"    ['Value cannot be parsed as decimal', '{neg_msg}'],\n"
            f"    default=None\n"
            f")\n"
            f"{quarantine_reason_clause}"
            f"{clean_row_clause}"
        )

    # 8. Non-Destructive Whitespace Remediation
    if "whitespace" in itype:
        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"# Non-destructive whitespace normalization: preserves raw values in audit column\n"
            f"raw = df['{col_safe}']\n"
            f"clean = raw.astype('string').str.strip().str.replace(r'\\s+', ' ', regex=True)\n"
            f"has_ws_issue = raw.notna() & (raw.astype(str) != clean.astype(str))\n"
            f"df['{col_safe}_raw'] = raw\n"
            f"df['{col_safe}_clean'] = clean\n"
            f"df['{col_safe}_validation_status'] = np.where(has_ws_issue, 'whitespace_normalized', 'valid')\n"
            f"df['{col_safe}_validation_reason'] = np.where(has_ws_issue, 'Leading/trailing or multiple internal whitespace trimmed', None)"
        )

    # 9. Configured Placeholders Remediation
    if "placeholder" in itype:
        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"# Configured placeholder tokens for this run: ???, N/A, unknown, tbd, not available, dummy, temp, null, none, -, --, 0000, test\n"
            f"placeholder_tokens = ['???', 'n/a', 'unknown', 'tbd', 'not available', 'dummy', 'temp', 'null', 'none', '-', '--', '0000', 'test']\n"
            f"raw = df['{col_safe}']\n"
            f"normalized = raw.astype('string').str.strip().str.lower()\n"
            f"is_placeholder = normalized.isin(placeholder_tokens)\n"
            f"df['{col_safe}_raw'] = raw\n"
            f"df['{col_safe}_clean'] = df['{col_safe}'].where(~is_placeholder, None)\n"
            f"df['{col_safe}_validation_status'] = np.where(is_placeholder, 'placeholder_value', 'valid')\n"
            f"df['{col_safe}_validation_reason'] = np.where(is_placeholder, 'Configured placeholder token detected', None)\n"
            f"df['quarantine_reason'] = np.where(is_placeholder, 'Configured placeholder value detected', None)\n"
            f"quarantine_df = df[is_placeholder].copy()\n"
            f"clean_df = df[~is_placeholder].copy()"
        )

    # 10. Punctuation-Only Remediation
    if "punctuation" in itype:
        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"# Remediate symbol/punctuation-only values like '###', '---', '@@@', '...'\n"
            f"raw = df['{col_safe}']\n"
            f"is_punct = raw.astype('string').str.strip().str.match(r'^[\\W_]+$').fillna(False)\n"
            f"df['{col_safe}_raw'] = raw\n"
            f"df['{col_safe}_clean'] = df['{col_safe}'].where(~is_punct, None)\n"
            f"df['{col_safe}_validation_status'] = np.where(is_punct, 'punctuation_only', 'valid')\n"
            f"df['{col_safe}_validation_reason'] = np.where(is_punct, 'Symbol or punctuation-only string', None)\n"
            f"df['quarantine_reason'] = np.where(is_punct, 'Punctuation/symbol-only value', None)\n"
            f"quarantine_df = df[is_punct].copy()\n"
            f"clean_df = df[~is_punct].copy()"
        )

    if col_l in NON_NUMERIC_SEMANTICS or any(col_l.endswith(x) for x in ("_id", "id", "name", "country", "status", "date")):
        if "numeric" in itype or "mixed" in itype:
            return (
                f"import pandas as pd\n"
                f"import numpy as np\n"
                f"# Step: text normalization for '{col_safe}' (preserving string/identifier format)\n"
                f"raw = df['{col_safe}']\n"
                f"df['{col_safe}_raw'] = raw\n"
                f"df['{col_safe}_clean'] = raw.astype('string').str.strip().str.upper()\n"
                f"df['{col_safe}_validation_status'] = np.select([\n"
                f"    raw.isna(),\n"
                f"    raw.astype(str).str.strip() == ''\n"
                f"], ['source_null', 'empty'], default='valid')\n"
                f"df['{col_safe}_validation_reason'] = np.where(raw.isna(), 'Source null', None)"
            )

    if "numeric" in itype or "mixed" in itype:
        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"raw = df['{col_safe}']\n"
            f"clean = pd.to_numeric(raw, errors='coerce')\n"
            f"df['{col_safe}_raw'] = raw\n"
            f"df['{col_safe}_clean'] = clean\n"
            f"df['{col_safe}_validation_status'] = np.select(\n"
            f"    condlist=[\n"
            f"        raw.isna(),\n"
            f"        raw.astype(str).str.strip() == '',\n"
            f"        clean.isna()\n"
            f"    ],\n"
            f"    choicelist=['source_null', 'empty', 'invalid_numeric'],\n"
            f"    default='valid'\n"
            f")\n"
            f"df['{col_safe}_validation_reason'] = np.where(clean.isna() & raw.notna(), 'Value cannot be parsed as numeric', None)"
        )
        
    elif "date" in itype:
        return (
            f"import pandas as pd\n"
            f"import numpy as np\n"
            f"raw = df['{col_safe}']\n"
            f"parsed = pd.to_datetime(raw, format='%Y-%m-%d', errors='coerce')\n"
            f"df['{col_safe}_raw'] = raw\n"
            f"df['{col_safe}_clean'] = parsed\n"
            f"df['{col_safe}_validation_status'] = np.select(\n"
            f"    condlist=[raw.isna(), parsed.isna()],\n"
            f"    choicelist=['source_null', 'invalid_or_impossible_date'],\n"
            f"    default='valid'\n"
            f")\n"
            f"df['{col_safe}_validation_reason'] = np.where(parsed.isna() & raw.notna(), 'Invalid date structure or impossible calendar date', None)\n"
            f"quarantine_df = df[parsed.isna() & raw.notna()].copy()\n"
            f"clean_df = df[parsed.notna() | raw.isna()].copy()"
        )

    elif "orphan" in itype or "relationship" in itype:
        p_ds_safe = (related_ds or "parent_dataset").replace(".", "_").replace("-", "_")
        p_col_safe = related_col or "parent_column"
        return (
            f"# Quarantine the child rows. Remove only under approved retention & remediation policy.\n"
            f"parent_keys = set({p_ds_safe}_df['{p_col_safe}'].dropna().unique())\n"
            f"orphan_{col_safe}_rows = df[~df['{col_safe}'].isin(parent_keys)]\n"
            f"valid_{col_safe}_rows = df[df['{col_safe}'].isin(parent_keys)]"
        )

    elif "null" in itype or "violation" in itype:
        if "suspected" in itype:
            return (
                f"# Profile and review these rows. Do not quarantine automatically until key is declared mandatory.\n"
                f"view_uniques = df['{col_safe}'].value_counts()"
            )
        is_key = col_safe.lower().endswith(("_id", "_key", "_code")) or col_safe.lower() == "id" or "key" in itype
        if is_key:
            return (
                f"invalid_key_rows = df[df['{col_safe}'].isna()].copy()\n"
                f"valid_rows = df[df['{col_safe}'].notna()].copy()"
            )
        else:
            return (
                f"# Preserve as null, quarantine if required, or impute only under a declared field-specific policy.\n"
                f"# Apply only the field-specific imputation function approved by the data owner."
            )
            
    else:
        return f"# Remediate {issue_type} manually"


def get_pyspark_code(
    col: Optional[str],
    issue_type: str,
    action_id: Optional[str] = None,
    related_ds: Optional[str] = None,
    related_col: Optional[str] = None
) -> Optional[str]:
    # No-op actions generate no transformation or quarantine code
    if action_id in ("keep_as_is", "skip_etl", "noop"):
        return None

    col_safe = col or "column_name"
    col_l = col_safe.lower().strip()
    itype = issue_type.lower()
    aid = (action_id or "").lower()
    
    is_flag_only = (aid == "flag_only")
    is_nullify = (aid == "nullify_clean_value")
    is_quarantine = (aid in ("quarantine_row", "quarantine", "drop_invalid_rows"))

    NON_NUMERIC_SEMANTICS = {
        "country", "country_code", "customer_id", "order_id", "payment_id",
        "client_id", "user_id", "status", "date", "phone", "email", "full_name"
    }

    is_date_col = any(k in col_l for k in ["date", "created", "timestamp", "time", "dob", "birth"])

    # 1a. Dedicated Date Sentinel Remediation (e.g. CreatedDate = 'N/A', 'NA', 'NULL')
    if itype == "sentinel_date" or (is_date_col and "sentinel" in itype):
        clean_row_clause = "clean_df = df\nquarantine_df = df.limit(0)" if not is_quarantine else "quarantine_df = df.filter(is_sentinel_date)\nclean_df = df.filter(~is_sentinel_date)"
        quarantine_reason_clause = "    .withColumn('quarantine_reason', F.when(is_sentinel_date, 'Configured sentinel date detected').otherwise(None))\n" if is_quarantine else ""
        return (
            f"import pyspark.sql.functions as F\n"
            f"# Normalize configured sentinel dates without numeric casting\n"
            f"sentinel_dates = ['n/a', 'na', 'null', 'none', 'unknown', '9999-12-31', '1900-01-01']\n"
            f"raw = F.col('{col_safe}')\n"
            f"normalized = F.lower(F.trim(raw.cast('string')))\n"
            f"is_sentinel_date = normalized.isin(sentinel_dates)\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_raw', raw)\n"
            f"    .withColumn('{col_safe}_clean', F.when(is_sentinel_date, F.lit(None).cast('date')).otherwise(F.to_date(F.trim(raw.cast('string')), 'yyyy-MM-dd')))\n"
            f"    .withColumn('{col_safe}_validation_status',\n"
            f"        F.when(raw.isNull(), 'source_null')\n"
            f"         .when(is_sentinel_date, 'sentinel_date')\n"
            f"         .otherwise('valid')\n"
            f"    )\n"
            f"    .withColumn('{col_safe}_validation_reason',\n"
            f"        F.when(is_sentinel_date, 'Configured sentinel or placeholder date').otherwise(None)\n"
            f"    )\n"
            f"{quarantine_reason_clause}"
            f")\n"
            f"{clean_row_clause}"
        )

    # 1. Sentinel / Magic Number Remediation (e.g. InstructorID = 999)
    if "sentinel" in itype:
        return (
            f"import pyspark.sql.functions as F\n"
            f"# Remediate configured sentinel identifiers (e.g. 999, -999, 9999999)\n"
            f"sentinel_values = [999, -999, 9999999]\n"
            f"raw = F.col('{col_safe}')\n"
            f"is_sentinel = raw.cast('long').isin(sentinel_values)\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_raw', raw)\n"
            f"    .withColumn('{col_safe}_clean', F.when(is_sentinel, F.lit(None).cast('long')).otherwise(raw.cast('long')))\n"
            f"    .withColumn('{col_safe}_validation_status',\n"
            f"        F.when(raw.isNull(), 'source_null')\n"
            f"         .when(is_sentinel, 'sentinel_value')\n"
            f"         .otherwise('valid')\n"
            f"    )\n"
            f"    .withColumn('{col_safe}_validation_reason',\n"
            f"        F.when(is_sentinel, 'Configured sentinel identifier').otherwise(None)\n"
            f"    )\n"
            f"    .withColumn('quarantine_reason', F.when(is_sentinel, 'Configured sentinel identifier detected').otherwise(None))\n"
            f")\n"
            f"quarantine_df = df.filter(is_sentinel)\n"
            f"clean_df = df.filter((is_sentinel == False) | is_sentinel.isNull())"
        )

    # 2. Implausible Age Remediation
    if "age" in itype or "implausible_age" in itype or (col_l == "age" and ("range" in itype or "plausib" in itype or "implausible" in itype or "outlier" in itype or "validity" in itype)):
        return (
            f"import pyspark.sql.functions as F\n"
            f"# DRY_RUN: Validate Age within [0, 150], preserve raw values, isolate nulls from implausible domain violations\n"
            f"raw_age = F.col('{col_safe}')\n"
            f"num_age = raw_age.cast('integer')\n"
            f"is_null = raw_age.isNull()\n"
            f"is_unparseable = raw_age.isNotNull() & num_age.isNull()\n"
            f"is_implausible = num_age.isNotNull() & ((num_age < 0) | (num_age > 150))\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_raw', raw_age)\n"
            f"    .withColumn('{col_safe}_clean', F.when(is_implausible, None).otherwise(num_age))\n"
            f"    .withColumn('{col_safe}_validation_status',\n"
            f"        F.when(is_null, 'source_null')\n"
            f"         .when(is_unparseable, 'unparseable_age')\n"
            f"         .when(is_implausible, 'implausible_age')\n"
            f"         .otherwise('valid')\n"
            f"    )\n"
            f"    .withColumn('{col_safe}_validation_reason',\n"
            f"        F.when(is_unparseable, 'Value cannot be parsed as integer age')\n"
            f"         .when(is_implausible, 'Age out of plausible range [0, 150]')\n"
            f"         .otherwise(None)\n"
            f"    )\n"
            f"    .withColumn('quarantine_reason', F.when(is_implausible, 'Age out of plausible range [0, 150]').otherwise(None))\n"
            f")\n"
            f"quarantine_df = df.filter(is_implausible)\n"
            f"clean_df = df.filter((is_implausible == False) | is_implausible.isNull())"
        )

    # 3. Integer Stored as Float (Conformity)
    if "integer_stored_as_float" in itype or "float_stored_as_integer" in itype:
        return (
            f"import pyspark.sql.functions as F\n"
            f"# Safe cast: Validate integer-like semantics (num == floor(num)) before converting to long\n"
            f"raw = F.col('{col_safe}')\n"
            f"num = raw.cast('double')\n"
            f"is_integer_like = num.isNotNull() & (num == F.floor(num))\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_raw', raw)\n"
            f"    .withColumn('{col_safe}_clean', F.when(is_integer_like, num.cast('long')).otherwise(None))\n"
            f"    .withColumn('{col_safe}_validation_status', F.when(is_integer_like, 'float_to_int_cast').when(raw.isNull(), 'source_null').otherwise('invalid_fractional_integer'))\n"
            f"    .withColumn('{col_safe}_validation_reason', F.when(is_integer_like, 'Float-serialized integer converted to canonical long').when(raw.isNull(), None).otherwise('Value contains fractional part, cannot cast to integer'))\n"
            f")"
        )

    # 4. Invalid Phone Number Remediation
    if "phone" in itype:
        return (
            f"import pyspark.sql.functions as F\n"
            f"# Validate phone format and length, preserve raw values and isolate invalid entries\n"
            f"raw = F.col('{col_safe}').cast('string')\n"
            f"trimmed = F.trim(raw)\n"
            f"digits_only = F.regexp_replace(trimmed, r'[^0-9]', '')\n"
            f"is_null = raw.isNull() | (trimmed == '')\n"
            f"is_placeholder = F.lower(trimmed).isin(['unknown', 'n/a', 'na', 'none', 'null', '###', '-'])\n"
            f"is_repeated = digits_only.rlike(r'^(\\d)\\1{{4,}}$')\n"
            f"is_valid = ~is_null & ~is_placeholder & ~is_repeated & (F.length(digits_only) >= 10) & (F.length(digits_only) <= 15)\n"
            f"is_invalid = ~is_null & ~is_placeholder & ~is_valid\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_raw', raw)\n"
            f"    .withColumn('{col_safe}_clean', F.when(is_valid, digits_only).otherwise(None))\n"
            f"    .withColumn('{col_safe}_validation_status',\n"
            f"        F.when(is_null, 'source_null')\n"
            f"         .when(is_placeholder, 'placeholder_phone')\n"
            f"         .when(is_repeated, 'repeated_digit_phone')\n"
            f"         .when(is_invalid, 'invalid_phone_format')\n"
            f"         .otherwise('valid')\n"
            f"    )\n"
            f"    .withColumn('{col_safe}_validation_reason',\n"
            f"        F.when(is_repeated, 'Implausible repeated digits phone number')\n"
            f"         .when(is_invalid, 'Phone number format or length violation')\n"
            f"         .when(is_placeholder, 'Configured placeholder phone')\n"
            f"         .otherwise(None)\n"
            f"    )\n"
            f"    .withColumn('quarantine_reason', F.when(is_invalid | is_repeated, 'Invalid phone number format').otherwise(None))\n"
            f")\n"
            f"quarantine_df = df.filter(is_invalid | is_repeated)\n"
            f"clean_df = df.filter(((is_invalid | is_repeated) == False) | is_invalid.isNull())"
        )

    # 5. Invalid Email Remediation
    if "email" in itype:
        return (
            f"import pyspark.sql.functions as F\n"
            f"# Validate email format, preserve raw values and isolate invalid entries\n"
            f"raw = F.col('{col_safe}').cast('string')\n"
            f"trimmed = F.trim(raw)\n"
            f"norm_email = F.lower(trimmed)\n"
            f"is_null = raw.isNull() | (trimmed == '')\n"
            f"is_placeholder = norm_email.isin(['unknown', 'n/a', 'na', 'none', 'null', 'test'])\n"
            f"is_valid_email = norm_email.rlike(r'^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$')\n"
            f"is_invalid_email = ~is_null & ~is_placeholder & ~is_valid_email\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_raw', raw)\n"
            f"    .withColumn('{col_safe}_clean', F.when(is_valid_email, norm_email).otherwise(None))\n"
            f"    .withColumn('{col_safe}_validation_status',\n"
            f"        F.when(is_null, 'source_null')\n"
            f"         .when(is_placeholder, 'placeholder_email')\n"
            f"         .when(is_invalid_email, 'invalid_email_format')\n"
            f"         .otherwise('valid')\n"
            f"    )\n"
            f"    .withColumn('{col_safe}_validation_reason',\n"
            f"        F.when(is_invalid_email, 'Email address format violation (missing domain, invalid TLD, or consecutive @)')\n"
            f"         .when(is_placeholder, 'Configured placeholder email')\n"
            f"         .otherwise(None)\n"
            f"    )\n"
            f"    .withColumn('quarantine_reason', F.when(is_invalid_email, 'Invalid email format').otherwise(None))\n"
            f")\n"
            f"quarantine_df = df.filter(is_invalid_email)\n"
            f"clean_df = df.filter((is_invalid_email == False) | is_invalid_email.isNull())"
        )

    # 6. Impossible / Invalid Calendar Date Remediation
    if "impossible_date" in itype or "calendar_date" in itype:
        return (
            f"import pyspark.sql.functions as F\n"
            f"# Isolate impossible calendar dates (e.g. 2022-02-30, 2024-05-40, 2023-13-01) from placeholders/format errors\n"
            f"raw = F.col('{col_safe}').cast('string')\n"
            f"trimmed = F.trim(raw)\n"
            f"is_null = raw.isNull() | (trimmed == '')\n"
            f"is_placeholder = F.lower(trimmed).isin(['n/a', 'na', 'null', 'none', 'unknown', '???', '###'])\n"
            f"is_iso_like = trimmed.rlike(r'^\\d{{4}}-\\d{{2}}-\\d{{2}}$')\n"
            f"parsed = F.to_date(trimmed, 'yyyy-MM-dd')\n"
            f"is_impossible = is_iso_like & parsed.isNull()\n"
            f"is_format_error = ~is_null & ~is_placeholder & ~is_iso_like\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_raw', raw)\n"
            f"    .withColumn('{col_safe}_clean', parsed)\n"
            f"    .withColumn('{col_safe}_validation_status',\n"
            f"        F.when(is_null, 'source_null')\n"
            f"         .when(is_placeholder, 'placeholder_date')\n"
            f"         .when(is_impossible, 'impossible_calendar_date')\n"
            f"         .when(is_format_error, 'invalid_date_format')\n"
            f"         .otherwise('valid')\n"
            f"    )\n"
            f"    .withColumn('{col_safe}_validation_reason',\n"
            f"        F.when(is_impossible, 'Impossible calendar date (invalid month or day for month)')\n"
            f"         .when(is_format_error, 'Date structure does not conform to expected format')\n"
            f"         .when(is_placeholder, 'Configured placeholder token')\n"
            f"         .otherwise(None)\n"
            f"    )\n"
            f"    .withColumn('quarantine_reason', F.when(is_impossible, 'Impossible calendar date').otherwise(None))\n"
            f")\n"
            f"quarantine_df = df.filter(is_impossible)\n"
            f"clean_df = df.filter((is_impossible == False) | is_impossible.isNull())"
        )

    # 7. Non-Negative Measure / Price Remediation
    if "negative" in itype:
        neg_msg = "Age cannot be negative" if "age" in col_l else ("Catalog price cannot be negative" if any(k in col_l for k in ["price", "cost", "amount", "charge", "rate", "production_qty"]) else f"{col_safe} cannot be negative")
        if is_flag_only:
            clean_col_assign = f"    .withColumn('{col_safe}_clean', num_val)\n"
            quarantine_reason_clause = ""
            clean_row_clause = f"clean_df = df\nquarantine_df = df.limit(0)"
        elif is_nullify:
            clean_col_assign = f"    .withColumn('{col_safe}_clean', F.when(is_negative, None).otherwise(num_val))\n"
            quarantine_reason_clause = ""
            clean_row_clause = f"clean_df = df\nquarantine_df = df.limit(0)"
        else:
            clean_col_assign = f"    .withColumn('{col_safe}_clean', F.when(is_negative, None).otherwise(num_val))\n"
            quarantine_reason_clause = f"    .withColumn('quarantine_reason', F.when(is_negative, 'Non-negative measure violation (negative value)').otherwise(None))\n"
            clean_row_clause = (
                f"# Filter using validated boolean predicates, never raw column condition\n"
                f"quarantine_df = df.filter(is_negative | is_unparseable)\n"
                f"clean_df = df.filter(~is_negative & ~is_unparseable)"
            )

        return (
            f"import pyspark.sql.functions as F\n"
            f"# Preserve raw amount for auditability; isolate negative entries from nulls/unparseable values\n"
            f"raw_val = F.col('{col_safe}')\n"
            f"num_val = raw_val.cast('double')\n"
            f"is_null = raw_val.isNull()\n"
            f"is_unparseable = raw_val.isNotNull() & num_val.isNull()\n"
            f"is_negative = num_val.isNotNull() & (num_val < 0)\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_raw', raw_val)\n"
            f"{clean_col_assign}"
            f"    .withColumn('{col_safe}_validation_status',\n"
            f"        F.when(is_null, 'source_null')\n"
            f"         .when(is_unparseable, 'unparseable_numeric')\n"
            f"         .when(is_negative, 'negative_value_review_required')\n"
            f"         .otherwise('valid')\n"
            f"    )\n"
            f"    .withColumn('{col_safe}_validation_reason',\n"
            f"        F.when(is_unparseable, 'Value cannot be parsed as decimal')\n"
            f"         .when(is_negative, '{neg_msg}')\n"
            f"         .otherwise(None)\n"
            f"    )\n"
            f"{quarantine_reason_clause}"
            f")\n"
            f"{clean_row_clause}"
        )

    # 8. Non-Destructive Whitespace Remediation
    if "whitespace" in itype:
        return (
            f"import pyspark.sql.functions as F\n"
            f"# Non-destructive whitespace normalization: preserves raw values in audit column\n"
            f"raw = F.col('{col_safe}')\n"
            f"clean = F.regexp_replace(F.trim(F.col('{col_safe}').cast('string')), r'\\s+', ' ')\n"
            f"has_ws_issue = raw.isNotNull() & (raw.cast('string') != clean)\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_raw', raw)\n"
            f"    .withColumn('{col_safe}_clean', clean)\n"
            f"    .withColumn('{col_safe}_validation_status', F.when(has_ws_issue, 'whitespace_normalized').otherwise('valid'))\n"
            f"    .withColumn('{col_safe}_validation_reason', F.when(has_ws_issue, 'Leading/trailing or multiple internal whitespace trimmed').otherwise(None))\n"
            f")"
        )

    # 9. Configured Placeholders Remediation
    if "placeholder" in itype:
        return (
            f"import pyspark.sql.functions as F\n"
            f"# Configured placeholder tokens for this run: ???, N/A, unknown, tbd, not available, dummy, temp, null, none, -, --, 0000, test\n"
            f"placeholder_tokens = ['???', 'n/a', 'unknown', 'tbd', 'not available', 'dummy', 'temp', 'null', 'none', '-', '--', '0000', 'test']\n"
            f"raw = F.col('{col_safe}')\n"
            f"normalized = F.lower(F.trim(raw.cast('string')))\n"
            f"is_placeholder = normalized.isin(placeholder_tokens)\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_raw', raw)\n"
            f"    .withColumn('{col_safe}_clean', F.when(is_placeholder, None).otherwise(raw))\n"
            f"    .withColumn('{col_safe}_validation_status', F.when(is_placeholder, 'placeholder_value').otherwise('valid'))\n"
            f"    .withColumn('{col_safe}_validation_reason', F.when(is_placeholder, 'Configured placeholder token detected').otherwise(None))\n"
            f"    .withColumn('quarantine_reason', F.when(is_placeholder, 'Configured placeholder value detected').otherwise(None))\n"
            f")\n"
            f"quarantine_df = df.filter(is_placeholder)\n"
            f"clean_df = df.filter(~is_placeholder)"
        )

    # 10. Punctuation-Only Remediation
    if "punctuation" in itype:
        return (
            f"import pyspark.sql.functions as F\n"
            f"raw = F.col('{col_safe}').cast('string')\n"
            f"# Remediate symbol/punctuation-only values like '###', '---', '@@@', '...'\n"
            f"is_punct = F.trim(raw).rlike(r'^[\\W_]+$')\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_raw', F.col('{col_safe}'))\n"
            f"    .withColumn('{col_safe}_clean', F.when(is_punct, None).otherwise(F.col('{col_safe}')))\n"
            f"    .withColumn('{col_safe}_validation_status', F.when(is_punct, 'punctuation_only').otherwise('valid'))\n"
            f"    .withColumn('{col_safe}_validation_reason', F.when(is_punct, 'Symbol or punctuation-only string').otherwise(None))\n"
            f"    .withColumn('quarantine_reason', F.when(is_punct, 'Punctuation/symbol-only value').otherwise(None))\n"
            f")\n"
            f"quarantine_df = df.filter(is_punct)\n"
            f"clean_df = df.filter(~is_punct)"
        )

    if col_l in NON_NUMERIC_SEMANTICS or any(col_l.endswith(x) for x in ("_id", "id", "name", "country", "status", "date")):
        if "numeric" in itype or "mixed" in itype:
            return (
                f"import pyspark.sql.functions as F\n"
                f"# Step: text normalization for '{col_safe}' (preserving string/identifier format)\n"
                f"raw = F.col('{col_safe}')\n"
                f"df = (\n"
                f"    df\n"
                f"    .withColumn('{col_safe}_raw', raw)\n"
                f"    .withColumn('{col_safe}_clean', F.upper(F.trim(raw.cast('string'))))\n"
                f"    .withColumn(\n"
                f"        '{col_safe}_validation_status',\n"
                f"        F.when(raw.isNull(), 'source_null')\n"
                f"         .when(F.trim(raw.cast('string')) == '', 'empty')\n"
                f"         .otherwise('valid')\n"
                f"    )\n"
                f"    .withColumn('{col_safe}_validation_reason', F.when(raw.isNull(), 'Source null').otherwise(None))\n"
                f")"
            )

    if "numeric" in itype or "mixed" in itype:
        return (
            f"import pyspark.sql.functions as F\n"
            f"raw = F.col('{col_safe}')\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_raw', raw)\n"
            f"    .withColumn('{col_safe}_clean', raw.cast('double'))\n"
            f"    .withColumn(\n"
            f"        '{col_safe}_validation_status',\n"
            f"        F.when(raw.isNull(), 'source_null')\n"
            f"         .when(F.trim(raw.cast('string')) == '', 'empty')\n"
            f"         .when(raw.cast('double').isNull(), 'invalid_numeric')\n"
            f"         .otherwise('valid')\n"
            f"    )\n"
            f"    .withColumn('{col_safe}_validation_reason', F.when(raw.isNotNull() & raw.cast('double').isNull(), 'Value cannot be parsed as numeric').otherwise(None))\n"
            f")"
        )
        
    elif "date" in itype:
        return (
            f"import pyspark.sql.functions as F\n"
            f"raw = F.col('{col_safe}')\n"
            f"parsed = F.to_date(raw, 'yyyy-MM-dd')\n"
            f"df = (\n"
            f"    df\n"
            f"    .withColumn('{col_safe}_raw', raw)\n"
            f"    .withColumn('{col_safe}_clean', parsed)\n"
            f"    .withColumn(\n"
            f"        '{col_safe}_validation_status',\n"
            f"        F.when(raw.isNull(), 'source_null')\n"
            f"         .when(parsed.isNull(), 'invalid_or_impossible_date')\n"
            f"         .otherwise('valid')\n"
            f"    )\n"
            f"    .withColumn('{col_safe}_validation_reason', F.when(raw.isNotNull() & parsed.isNull(), 'Invalid date structure or impossible calendar date').otherwise(None))\n"
            f"    .withColumn('quarantine_reason', F.when(raw.isNotNull() & parsed.isNull(), 'Invalid or impossible calendar date').otherwise(None))\n"
            f")\n"
            f"quarantine_df = df.filter(raw.isNotNull() & parsed.isNull())\n"
            f"clean_df = df.filter(raw.isNull() | parsed.isNotNull())"
        )

    elif "orphan" in itype or "relationship" in itype:
        p_ds_safe = (related_ds or "parent_dataset").replace(".", "_").replace("-", "_")
        p_col_safe = related_col or "parent_column"
        return (
            f"# Quarantine the child rows. Remove only under approved retention & remediation policy.\n"
            f"parent_keys = {p_ds_safe}_df.select('{p_col_safe}').dropDuplicates()\n"
            f"orphan_{col_safe}_rows = df.join(parent_keys, on='{col_safe}', how='left_anti')\n"
            f"valid_{col_safe}_rows = df.join(parent_keys, on='{col_safe}', how='left_semi')"
        )

    elif "null" in itype or "violation" in itype:
        if "suspected" in itype:
            return (
                f"# Profile and review these rows. Do not quarantine automatically until key is declared mandatory.\n"
                f"# df.select('{col_safe}').groupBy('{col_safe}').count().show()"
            )
        is_key = col_safe.lower().endswith(("_id", "_key", "_code")) or col_safe.lower() == "id" or "key" in itype
        if is_key:
            return (
                f"quarantine_df = df.filter(df['{col_safe}'].isNull())\n"
                f"clean_df = df.filter(df['{col_safe}'].isNotNull())"
            )
        else:
            return (
                f"# Preserve as null or impute under approved policy\n"
                f"# Apply only the field-specific imputation function approved by the data owner."
            )
            
    else:
        return f"# Remediate {issue_type} using PySpark manually"


ACTION_OUTCOME_METADATA: Dict[str, Dict[str, Any]] = {
    "trim": {
        "preserves_raw": True,
        "creates_clean": True,
        "invalid_value_effect": "strip leading/trailing whitespace and compress multiple inner spaces",
        "status_code": "trimmed_whitespace",
        "drops_rows": False,
        "description": "Standardizes text by trimming outer whitespace and compressing consecutive spaces.",
    },
    "lowercase": {
        "preserves_raw": True,
        "creates_clean": True,
        "invalid_value_effect": "convert text to uniform lowercase",
        "status_code": "standardized_lowercase",
        "drops_rows": False,
        "description": "Normalizes text values to lowercase.",
    },
    "uppercase": {
        "preserves_raw": True,
        "creates_clean": True,
        "invalid_value_effect": "convert text to uniform uppercase",
        "status_code": "standardized_uppercase",
        "drops_rows": False,
        "description": "Normalizes text values to uppercase.",
    },
    "regex_replace": {
        "preserves_raw": True,
        "creates_clean": True,
        "invalid_value_effect": "strip non-conforming or disallowed characters",
        "status_code": "sanitized_text",
        "drops_rows": False,
        "description": "Sanitizes text by stripping pattern-matched control characters or markup.",
    },
    "normalize_registered_placeholder": {
        "preserves_raw": True,
        "creates_clean": True,
        "invalid_value_effect": "convert placeholder tokens to NULL",
        "status_code": "placeholder_normalized_to_null",
        "drops_rows": False,
        "description": "Identifies configured placeholder tokens (e.g. 'N/A', 'NULL', '999') and sets clean value to NULL.",
    },
    "preserve_null_and_flag": {
        "preserves_raw": True,
        "creates_clean": True,
        "invalid_value_effect": "preserve source NULL and record indicator flag",
        "status_code": "source_null",
        "drops_rows": False,
        "description": "Preserves source NULL without data loss and tags validation status for auditing.",
    },
    "sanitize_email": {
        "preserves_raw": True,
        "creates_clean": True,
        "invalid_value_effect": "set clean value to NULL for invalid formats",
        "status_code": "invalid_email_format",
        "drops_rows": False,
        "description": "Validates email syntax against configured format rules, preserving original in _raw.",
    },
    "normalize_phone": {
        "preserves_raw": True,
        "creates_clean": True,
        "invalid_value_effect": "standardize numeric digits or set to NULL if invalid length",
        "status_code": "invalid_phone_format",
        "drops_rows": False,
        "description": "Normalizes phone digits and tags malformed numbers.",
    },
    "coerce_numeric": {
        "preserves_raw": True,
        "creates_clean": True,
        "invalid_value_effect": "cast convertible numbers and set unconvertible strings to NULL",
        "status_code": "coerced_numeric",
        "drops_rows": False,
        "description": "Converts string numbers to numeric datatype, flagging unparseable values.",
    },
    "parse_dates": {
        "preserves_raw": True,
        "creates_clean": True,
        "invalid_value_effect": "standardize mixed formats to ISO-8601 strings",
        "status_code": "parsed_iso_date",
        "drops_rows": False,
        "description": "Parses mixed date representations into standardized ISO date format.",
    },
    "standardize_boolean": {
        "preserves_raw": True,
        "creates_clean": True,
        "invalid_value_effect": "standardize varied boolean tokens to boolean datatype",
        "status_code": "standardized_boolean",
        "drops_rows": False,
        "description": "Normalizes varied boolean tokens (Y/N, 1/0, Yes/No) into standard boolean values.",
    },
    "flag_only": {
        "preserves_raw": True,
        "creates_clean": True,
        "invalid_value_effect": "flag invalid records with status and reason without mutating values or quarantining rows",
        "status_code": "flagged_only",
        "drops_rows": False,
        "description": "Preserves source rows and clean values, recording validation status and reason without quarantine.",
    },
    "nullify_clean_value": {
        "preserves_raw": True,
        "creates_clean": True,
        "invalid_value_effect": "preserve raw value and set clean companion column to NULL on validation failure",
        "status_code": "clean_nullified",
        "drops_rows": False,
        "description": "Retains the row in clean_df while setting the clean companion value to NULL, preserving the raw value for auditing.",
    },
    "quarantine_row": {
        "preserves_raw": True,
        "creates_clean": False,
        "invalid_value_effect": "exclude affected row from clean dataset and route to quarantine partition with evidence",
        "status_code": "quarantined_row",
        "drops_rows": False,
        "description": "Routes violating rows to quarantine_df with quarantine_reason and excludes them from clean_df.",
    },
    "skip_etl": {
        "preserves_raw": True,
        "creates_clean": False,
        "invalid_value_effect": "no ETL cleansing transformation applied",
        "status_code": "retained_as_is",
        "drops_rows": False,
        "description": "Retains data as-is without pipeline transformation.",
    },
    "flag_outliers": {
        "preserves_raw": True,
        "creates_clean": False,
        "invalid_value_effect": "record statistical outlier status without altering source value",
        "status_code": "statistical_outlier",
        "drops_rows": False,
        "description": "Flags values outside expected range in audit columns without modifying business data.",
    },
    "clip_outliers": {
        "preserves_raw": True,
        "creates_clean": True,
        "invalid_value_effect": "clip values to configured upper/lower bounds",
        "status_code": "clipped_to_boundary",
        "drops_rows": False,
        "description": "Caps values exceeding configured boundaries to threshold limits.",
    },
    "flag_duplicate_candidates": {
        "preserves_raw": True,
        "creates_clean": False,
        "invalid_value_effect": "assign duplicate group identifier without dropping rows",
        "status_code": "duplicate_candidate",
        "drops_rows": False,
        "description": "Assigns stable duplicate group IDs for operational inspection without row deletion.",
    },
    "flag_business_key_duplicate": {
        "preserves_raw": True,
        "creates_clean": False,
        "invalid_value_effect": "assign business key duplicate group ID",
        "status_code": "business_key_duplicate",
        "drops_rows": False,
        "description": "Flags duplicate business keys with stable group IDs for business steward review.",
    },
    "flag_missing_key": {
        "preserves_raw": True,
        "creates_clean": False,
        "invalid_value_effect": "tag missing key indicator",
        "status_code": "missing_key",
        "drops_rows": False,
        "description": "Flags missing identifier keys without row loss.",
    },
    "flag_missing_declared_key": {
        "preserves_raw": True,
        "creates_clean": False,
        "invalid_value_effect": "tag missing primary key violation",
        "status_code": "declared_key_null",
        "drops_rows": False,
        "description": "Flags missing primary key violations in validation status.",
    },
    "reject_orphans": {
        "preserves_raw": True,
        "creates_clean": False,
        "invalid_value_effect": "route unlinked records to quarantine partition",
        "status_code": "orphan_foreign_key",
        "drops_rows": False,
        "description": "Partitions orphan foreign key records to quarantine table for referential audit.",
    },
    "keep_as_is": {
        "preserves_raw": True,
        "creates_clean": False,
        "invalid_value_effect": "no transformation applied",
        "status_code": "retained_as_is",
        "drops_rows": False,
        "description": "Retains data as-is without pipeline transformation.",
    },
}


def build_data_aware_explanation(
    dataset: str,
    column: Optional[str],
    canonical_issue_type: str,
    action: str,
    affected_rows: Optional[int] = None,
    total_rows: Optional[int] = None,
    affected_pct: Optional[float] = None,
    is_standard: bool = True,
) -> Tuple[str, str]:
    """
    Builds data-aware 'why_it_matters' and 'suggested_fix' derived directly
    from the transformation contract and populated with real metrics.
    Avoids unsupported standard claims (e.g. avoids RFC 5322 references).
    """
    col_label = f"'{column}'" if column else "the dataset"
    ds_col = f"{dataset}.{column}" if column else dataset
    meta = ACTION_OUTCOME_METADATA.get(action, ACTION_OUTCOME_METADATA["keep_as_is"])
    status_code = meta["status_code"]
    effect = meta["invalid_value_effect"]

    # Format metrics portion
    metrics_clause = ""
    if affected_rows is not None and total_rows and total_rows > 0:
        pct_display = f"{affected_pct:.2f}%" if affected_pct is not None else f"{(affected_rows / total_rows * 100):.2f}%"
        metrics_clause = f"{affected_rows:,} of {total_rows:,} records ({pct_display})"
    elif affected_rows is not None:
        metrics_clause = f"{affected_rows:,} records"
    elif affected_pct is not None:
        metrics_clause = f"{affected_pct:.2f}% of records"

    rule_name = canonical_issue_type.replace("_", " ")

    if metrics_clause:
        why = (
            f"{metrics_clause} in {ds_col} do not conform to the configured {rule_name} rule. "
            f"Unresolved occurrences cause data ambiguity, pipeline errors, and untrusted downstream reporting."
        )
    else:
        why = (
            f"Values in {ds_col} do not conform to the configured {rule_name} rule. "
            f"Unresolved occurrences cause data ambiguity and downstream pipeline errors."
        )

    col_target = column or "record"
    if is_standard and meta["creates_clean"]:
        fix = (
            f"Standard remediation preserves original values in '{col_target}_raw', generates sanitized values "
            f"in '{col_target}_clean' ({effect}), and records '{status_code}' in '{col_target}_validation_status'."
        )
    elif is_standard and not meta["creates_clean"]:
        fix = (
            f"Standard remediation preserves original values without alteration, "
            f"tagging '{status_code}' in '{col_target}_validation_status'."
        )
    else:
        fix = (
            f"Decision required: multiple remediation policies exist for {rule_name}. "
            f"Recommended baseline is to {effect} and record '{status_code}' in '{col_target}_validation_status' "
            f"pending reviewer approval."
        )

    return why, fix


def _fallback(dq: Dict[str, Any]) -> DQRecommendations:
    """
    Capability-governed fallback providing data-aware explanations,
    decoupled lifecycle states, and objective impact gate evaluations.
    """
    try:
        from agent.profiling.assessment_orchestrator import load_thresholds
        thresholds = load_thresholds()
    except Exception:
        thresholds = {}
    platform_target = thresholds.get("platform", {}).get("target", "fabric_spark_sql")

    from agent.profiling.sql_dialect_adapter import get_remediation_sql
    from agent.etl_pipeline.remediation_router import route_finding

    recs: List[Dict[str, Any]] = []
    datasets = (dq.get("datasets") or {}) if isinstance(dq.get("datasets"), dict) else {}
    for ds_name, block in datasets.items():
        total_rows = None
        if isinstance(block, dict):
            total_rows = block.get("total_rows") or block.get("row_count") or block.get("rows")
        issues = (block.get("issues") or []) if isinstance(block, dict) else []
        for it in issues[:60]:
            if not isinstance(it, dict):
                continue

            routing = route_finding(it, dataset=ds_name, total_rows=total_rows)
            mapped = routing.capability.default_action

            col_name = it.get("column")
            related_ds = it.get("related_dataset")
            related_col = it.get("related_column")
            eff_action = getattr(routing, 'suggested_action', routing.capability.default_action)
            if eff_action in ("keep_as_is", "skip_etl", "noop"):
                example_sql = None
                example_pandas = None
                example_pyspark = None
            else:
                example_sql = get_remediation_sql(platform_target, ds_name, col_name, routing.canonical_issue_type)
                example_pandas = get_pandas_code(col_name, routing.canonical_issue_type, action_id=eff_action, related_ds=related_ds, related_col=related_col)
                example_pyspark = get_pyspark_code(col_name, routing.canonical_issue_type, action_id=eff_action, related_ds=related_ds, related_col=related_col)

            is_standard = routing.recommendation_category == "standard_remediation"
            why_it_matters, suggested_fix = build_data_aware_explanation(
                dataset=ds_name,
                column=col_name,
                canonical_issue_type=routing.canonical_issue_type,
                action=mapped,
                affected_rows=routing.affected_rows,
                total_rows=total_rows,
                affected_pct=routing.affected_percentage,
                is_standard=is_standard,
            )

            # Compute priority:
            # High severity gating issues first (P0/P1), medium (P2), low (P3)
            is_gating = it.get("gating", True)
            sev = (it.get("severity") or "medium").lower()
            is_rel = any(k in routing.canonical_issue_type for k in ["relationship", "orphan", "foreign_key"])

            if is_gating and sev == "high" and is_rel:
                priority = 0
            elif is_gating and sev == "high":
                priority = 1
            elif sev == "medium":
                priority = 2
            elif sev == "low":
                priority = 3
            else:
                priority = 4

            aff_count = routing.affected_rows if routing.affected_rows is not None else 0
            source_tag = "standard_remediation" if is_standard else "deterministic_catalog"
            authority_src = "DETERMINISTIC_REGISTRY" if is_standard else "CATALOG_FALLBACK"
            ai_status = "not_applicable" if is_standard else "unavailable"

            recs.append(
                {
                    "priority": priority,
                    "dataset": ds_name,
                    "column": col_name,
                    "issue_type": routing.canonical_issue_type,
                    "raw_issue_type": routing.raw_issue_type,
                    "related_dataset": related_ds,
                    "related_column": related_col,
                    "severity": it.get("severity") or "medium",
                    "affected_count": aff_count,
                    "affected_percentage": routing.affected_percentage,
                    "why_it_matters": why_it_matters,
                    "suggested_fix": suggested_fix,
                    "mapped_action": mapped,
                    "auto_fixable": routing.auto_fix_eligible,
                    "capability_supported": routing.canonical_issue_type != "unknown_profiler_issue",
                    "authority_source": authority_src,
                    "ai_assistance_status": ai_status,
                    "recommendation_category": routing.recommendation_category,
                    "lifecycle": routing.lifecycle,
                    "primary_reason_code": routing.primary_reason_code,
                    "reason_codes": routing.reason_codes,
                    "cluster_key": list(routing.cluster_key),
                    "evaluated_policy": {
                        "risk": routing.capability.risk_level,
                        "risk_level": routing.capability.risk_level,
                        "percentage_limit": routing.applied_impact_limit_pct,
                        "row_limit": routing.applied_impact_limit_rows,
                        "approval_policy": routing.capability.approval_policy,
                    },
                    "observed": {
                        "affected_rows": routing.affected_rows,
                        "affected_percentage": routing.affected_percentage,
                    },
                    "outcome_metadata": ACTION_OUTCOME_METADATA.get(mapped, ACTION_OUTCOME_METADATA.get("keep_as_is", {})),
                    "example_sql": example_sql,
                    "example_pandas": example_pandas,
                    "example_pyspark": example_pyspark,
                    "risk": f"{routing.capability.risk_level.capitalize()} risk transformation. Reversible: {routing.capability.reversible}.",
                    "recommendation_source": source_tag,
                }
            )

    # Sort recommendations by priority (P0 to P4)
    recs.sort(key=lambda r: r.get("priority", 99))

    return DQRecommendations(
        recommendations=recs[:80],
        summary={
            "top_themes": [],
            "notes": f"Capability-governed recommendations mapped to {platform_target} target.",
            "disclosure": f"Showing top {min(len(recs), 80)} prioritized remediation recommendations. See dataset sections and cleaning_manifest.json for complete findings.",
        },
    )


# ── Bounded Concurrency & Cluster Settings ────────────────────────────────────
MAX_CONCURRENT_LLM_CALLS = 4
PER_CLUSTER_TIMEOUT_SECONDS = 10.0
OVERALL_LLM_BUDGET_SECONDS = 20.0
CLUSTER_RECOMMENDATION_MAX_OUTPUT_TOKENS = 2048
MAX_FINDINGS_PER_CLUSTER_REQUEST = 10
MAX_ISSUES_PER_CLUSTER = 10
MAX_SPLIT_DEPTH = 1
MAX_ATTEMPTS_PER_SUBCLUSTER = 1
MAX_TOTAL_CALLS_PER_ORIGINAL_CLUSTER = 3

class LLMResponseTruncatedError(Exception):
    """Raised when LLM response finishes with finish_reason == 'length'."""
    pass

class LLMResponseInvalidError(Exception):
    """Raised when LLM response contains no choices or empty text."""
    pass

class LLMResponseSchemaInvalidError(Exception):
    """Raised when LLM response is not valid JSON."""
    pass

_CLUSTER_SYSTEM = """
You are an expert Data Quality Remediation Advisor for Agent Dhara.
For each finding in this dataset context cluster, select the most appropriate remediation option ONLY from the provided allowable_options.
DO NOT invent new action IDs or option IDs.
Return ONLY valid JSON matching this schema:
{
  "recommendations": [
    {
      "column": "string|null",
      "issue_type": "string",
      "selected_option_id": "string",
      "why_it_matters": "string",
      "expected_outcome": "string",
      "risk": "low|medium|high|critical"
    }
  ],
  "cluster_summary": "string"
}
""".strip()

_CLUSTER_CACHE: Dict[str, Any] = {}


def _build_cluster_catalog_fallback(
    cluster_key: Tuple[str, str],
    findings: List[Dict[str, Any]],
    platform_target: str,
) -> List[Dict[str, Any]]:
    """Build deterministic CATALOG_FALLBACK recommendation entries for an unranked cluster."""
    from agent.etl_pipeline.manual_review_catalog import get_resolution_options
    from agent.profiling.sql_dialect_adapter import get_remediation_sql

    ds_name, cluster_group = cluster_key
    findings_slice = findings[:MAX_ISSUES_PER_CLUSTER]
    recs: List[Dict[str, Any]] = []

    for it in findings_slice:
        can_type = it.get("canonical_issue_type") or it.get("issue_type") or "issue"
        col = it.get("column")
        catalog_opts = get_resolution_options(can_type)
        default_opt = next((o for o in catalog_opts if o.get("recommended")), catalog_opts[0] if catalog_opts else {"id": "keep_as_is", "action": "noop", "label": "Keep as-is"})
        action_name = default_opt.get("action", "noop")

        recs.append({
            "priority": 1 if (it.get("severity") or "").lower() == "high" else 2,
            "dataset": ds_name,
            "column": col,
            "issue_type": can_type,
            "raw_issue_type": it.get("raw_issue_type") or can_type,
            "severity": it.get("severity") or "medium",
            "affected_count": it.get("affected_count", 0),
            "affected_percentage": it.get("affected_percentage"),
            "why_it_matters": it.get("why_it_matters") or f"Decision required on {ds_name}.{col}",
            "suggested_fix": f"Catalog options available: {default_opt['label']} (AI ranking unavailable).",
            "mapped_action": action_name,
            "selected_option_id": default_opt["id"],
            "options": catalog_opts,
            "auto_fixable": False,
            "capability_supported": can_type != "unknown_profiler_issue",
            "authority_source": "CATALOG_FALLBACK",
            "ai_assistance_status": "unavailable",
            "recommendation_category": "decision_required",
            "lifecycle": {
                "recommendation_status": "catalog_options_available",
                "approval_status": "pending",
                "codegen_status": "not_started",
                "validation_status": "not_started",
                "execution_status": "not_started",
            },
            "cluster_key": list(cluster_key),
            "primary_reason_code": it.get("primary_reason_code") or (it.get("reason_codes", ["CONTEXTUAL_REMEDIATION"])[0]),
            "reason_codes": it.get("reason_codes", []),
            "evaluated_policy": it.get("evaluated_policy", {}),
            "observed": it.get("observed", {
                "affected_rows": it.get("affected_count"),
                "affected_percentage": it.get("affected_percentage"),
            }),
            "outcome_metadata": ACTION_OUTCOME_METADATA.get(action_name, ACTION_OUTCOME_METADATA.get("keep_as_is", {})),
            "example_sql": None if action_name in ("keep_as_is", "skip_etl", "noop") else get_remediation_sql(platform_target, ds_name, col, can_type),
            "example_pandas": get_pandas_code(col, can_type, action_id=action_name),
            "example_pyspark": get_pyspark_code(col, can_type, action_id=action_name),
            "risk": "Catalog options provided. Manual approval required.",
            "recommendation_source": "deterministic_catalog",
        })

    return recs


def _process_cluster(
    cluster_key: Tuple[str, str],
    findings: List[Dict[str, Any]],
    cfg: Any,
    platform_target: str,
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, int]]]:
    """
    Processes a single dataset-context cluster using the LLM with option allowlisting.
    If the LLM call fails, falls back gracefully to unranked catalog options.
    """
    import hashlib
    from agent.etl_pipeline.manual_review_catalog import get_resolution_options
    from agent.profiling.sql_dialect_adapter import get_remediation_sql

    ds_name, cluster_group = cluster_key
    findings_slice = findings[:MAX_ISSUES_PER_CLUSTER]

    # Build prompt payload with allowable options from catalog
    cluster_findings_payload = []
    options_by_finding: Dict[str, List[Dict[str, Any]]] = {}

    for it in findings_slice:
        can_type = it.get("canonical_issue_type") or it.get("issue_type") or "issue"
        col = it.get("column")
        k = f"{col}_{can_type}"
        catalog_opts = get_resolution_options(can_type)
        options_by_finding[k] = catalog_opts
        
        cluster_findings_payload.append({
            "column": col,
            "issue_type": can_type,
            "affected_count": it.get("affected_count"),
            "affected_percentage": it.get("affected_percentage"),
            "severity": it.get("severity") or "medium",
            "allowable_options": [
                {"id": o["id"], "label": o["label"], "action": o.get("action", "noop"), "description": o.get("description", "")}
                for o in catalog_opts
            ],
        })

    # Cache check incorporating full context
    cache_payload = {
        "dataset": ds_name,
        "cluster": cluster_group,
        "findings": cluster_findings_payload,
        "registry_version": "v1.0",
        "prompt_version": "v1.0",
        "model": str(getattr(cfg, "model", "")),
        "provider": str(getattr(cfg, "provider", "")),
        "platform_target": platform_target,
    }
    cache_str = json.dumps(cache_payload, sort_keys=True)
    cache_key = hashlib.sha256(cache_str.encode("utf-8")).hexdigest()
    if cache_key in _CLUSTER_CACHE:
        return _CLUSTER_CACHE[cache_key], None

    prompt_data = {
        "dataset": ds_name,
        "cluster_group": cluster_group,
        "findings": cluster_findings_payload,
    }
    prompt = json.dumps(prompt_data, ensure_ascii=False, cls=SessionJSONEncoder)

    recs: List[Dict[str, Any]] = []
    usage = None
    llm_rec_map: Dict[str, Any] = {}

    def _call_llm_cluster_api(
        p_str: str,
        f_cnt: int,
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, int]]]:
        if cfg.provider == "azure_openai":
            from openai import AzureOpenAI  # type: ignore
            client = AzureOpenAI(
                api_key=cfg.api_key,
                api_version=cfg.api_version or "2024-02-01",
                azure_endpoint=cfg.endpoint,
                timeout=PER_CLUSTER_TIMEOUT_SECONDS,
            )
            resp = client.chat.completions.create(
                model=cfg.model,
                messages=[{"role": "system", "content": _CLUSTER_SYSTEM}, {"role": "user", "content": p_str}],
                temperature=0.1,
                max_tokens=CLUSTER_RECOMMENDATION_MAX_OUTPUT_TOKENS,
                timeout=PER_CLUSTER_TIMEOUT_SECONDS,
            )
        else:
            from openai import OpenAI  # type: ignore
            client = OpenAI(api_key=cfg.api_key, timeout=PER_CLUSTER_TIMEOUT_SECONDS)
            resp = client.chat.completions.create(
                model=cfg.model,
                messages=[{"role": "system", "content": _CLUSTER_SYSTEM}, {"role": "user", "content": p_str}],
                temperature=0.1,
                max_tokens=CLUSTER_RECOMMENDATION_MAX_OUTPUT_TOKENS,
                timeout=PER_CLUSTER_TIMEOUT_SECONDS,
            )
        u = usage_dict_from_response(resp)
        if not resp.choices:
            raise LLMResponseInvalidError("LLM returned no choices")
        choice = resp.choices[0]
        if getattr(choice, "finish_reason", None) == "length":
            raise LLMResponseTruncatedError(f"Cluster recommendation output was truncated; finding_count={f_cnt}")
        raw = (choice.message.content or "").strip()
        if not raw:
            raise LLMResponseInvalidError("LLM returned an empty response")
        try:
            parsed_obj = json.loads(raw)
        except Exception as je:
            raise LLMResponseSchemaInvalidError("LLM returned malformed JSON") from je
        return parsed_obj, u

    try:
        obj, usage = _call_llm_cluster_api(prompt, len(findings_slice))
        llm_recs = obj.get("recommendations") or []
        llm_rec_map = {f"{r.get('column')}_{r.get('issue_type')}": r for r in llm_recs if isinstance(r, dict)}
    except LLMResponseTruncatedError as te:
        logger.warning("Cluster recommendation truncated; attempting split-and-retry: %s", te)
        if len(findings_slice) > 1:
            mid = len(findings_slice) // 2
            for sub in (findings_slice[:mid], findings_slice[mid:]):
                sub_payload = [
                    {
                        "column": it.get("column"),
                        "issue_type": it.get("canonical_issue_type") or it.get("issue_type") or "issue",
                        "affected_count": it.get("affected_count"),
                        "affected_percentage": it.get("affected_percentage"),
                        "severity": it.get("severity") or "medium",
                        "allowable_options": [
                            {"id": o["id"], "label": o["label"], "action": o.get("action", "noop"), "description": o.get("description", "")}
                            for o in (options_by_finding.get(f"{it.get('column')}_{it.get('canonical_issue_type') or it.get('issue_type') or 'issue'}") or [])
                        ],
                    }
                    for it in sub
                ]
                sub_prompt = json.dumps({"dataset": ds_name, "cluster_group": cluster_group, "findings": sub_payload}, ensure_ascii=False, cls=SessionJSONEncoder)
                try:
                    sub_obj, _ = _call_llm_cluster_api(sub_prompt, len(sub))
                    for r in (sub_obj.get("recommendations") or []):
                        if isinstance(r, dict):
                            llm_rec_map[f"{r.get('column')}_{r.get('issue_type')}"] = r
                except Exception as sub_e:
                    logger.warning("Subcluster retry failed: %s; falling back to catalogue.", sub_e)
        else:
            logger.warning("Single finding cluster truncated; using deterministic catalog fallback.")
    except Exception as e:
        logger.warning(f"Cluster LLM failed for {cluster_key}: {e}; falling back to catalog options.")
        return _build_cluster_catalog_fallback(cluster_key, findings_slice, platform_target), None

    for it in findings_slice:
        can_type = it.get("canonical_issue_type") or it.get("issue_type") or "issue"
        col = it.get("column")
        k = f"{col}_{can_type}"
        catalog_opts = options_by_finding.get(k) or get_resolution_options(can_type)
        llm_item = llm_rec_map.get(k)

        selected_opt = None
        is_verified_llm = False
        if llm_item:
            sel_id = str(llm_item.get("selected_option_id") or "").strip()
            selected_opt = next((o for o in catalog_opts if o["id"] == sel_id), None)
            if selected_opt:
                is_verified_llm = True

        if not selected_opt:
            selected_opt = next((o for o in catalog_opts if o.get("recommended")), catalog_opts[0] if catalog_opts else {"id": "keep_as_is", "action": "noop", "label": "Keep as-is"})

        action_name = selected_opt.get("action", "noop")
        why = (llm_item.get("why_it_matters") if (is_verified_llm and llm_item) else "") or it.get("why_it_matters") or f"Contextual DQ issue in {ds_name}.{col}"
        outcome = (llm_item.get("expected_outcome") if (is_verified_llm and llm_item) else "") or f"Applies {selected_opt['label']} under human governance."
        if is_verified_llm:
            suggested = f"AI-assisted option: {selected_opt['label']} - {outcome}"
            authority_src = "LLM_ADVISORY"
            ai_status = "available"
            rec_status = "ai_options_available"
            rec_src = "ai_assisted_options"
        else:
            suggested = f"Catalog option: {selected_opt['label']} (AI-assisted ranking was unavailable)."
            authority_src = "CATALOG_FALLBACK"
            ai_status = "unavailable"
            rec_status = "catalog_options_available"
            rec_src = "deterministic_catalog"

        recs.append({
            "priority": 1 if (it.get("severity") or "").lower() == "high" else 2,
            "dataset": ds_name,
            "column": col,
            "issue_type": can_type,
            "raw_issue_type": it.get("raw_issue_type") or can_type,
            "severity": it.get("severity") or "medium",
            "affected_count": it.get("affected_count", 0),
            "affected_percentage": it.get("affected_percentage"),
            "why_it_matters": why,
            "suggested_fix": suggested,
            "expected_outcome": outcome,
            "mapped_action": action_name,
            "selected_option_id": selected_opt["id"],
            "options": catalog_opts,
            "auto_fixable": False,  # Contextual issue always requires approval
            "capability_supported": can_type != "unknown_profiler_issue",
            "authority_source": authority_src,
            "proposal_type": "AdvisoryExtensionProposal",
            "advisory_only": True,
            "execution_eligible": False,
            "ai_assistance_status": ai_status,
            "recommendation_category": "decision_required",
            "lifecycle": {
                "recommendation_status": rec_status,
                "approval_status": "pending",
                "codegen_status": "not_started",
                "validation_status": "not_started",
                "execution_status": "not_started",
            },
            "cluster_key": list(cluster_key),
            "primary_reason_code": it.get("primary_reason_code") or (it.get("reason_codes", ["CONTEXTUAL_REMEDIATION"])[0]),
            "reason_codes": it.get("reason_codes", []),
            "evaluated_policy": it.get("evaluated_policy", {}),
            "observed": it.get("observed", {
                "affected_rows": it.get("affected_count"),
                "affected_percentage": it.get("affected_percentage"),
            }),
            "outcome_metadata": ACTION_OUTCOME_METADATA.get(action_name, ACTION_OUTCOME_METADATA.get("keep_as_is", {})),
            "example_sql": None if action_name in ("keep_as_is", "skip_etl", "noop") else get_remediation_sql(platform_target, ds_name, col, can_type),
            "example_pandas": get_pandas_code(col, can_type, action_id=action_name),
            "example_pyspark": get_pyspark_code(col, can_type, action_id=action_name),
            "risk": f"Contextual transformation requiring decision. Options: {len(catalog_opts)} available.",
            "recommendation_source": rec_src,
        })

    _CLUSTER_CACHE[cache_key] = recs
    return recs, usage


class DQRecommendationsAgent:
    def recommend(
        self,
        *,
        merged_dq: Dict[str, Any],
        user_intent: str = "",
    ) -> Tuple[DQRecommendations, Optional[Dict[str, int]]]:
        cfg = load_llm_config(purpose="dq_recommendations")
        if cfg is None:
            return _fallback(merged_dq), None

        from agent.etl_pipeline.remediation_router import route_finding
        from agent.profiling.sql_dialect_adapter import get_remediation_sql
        import concurrent.futures

        try:
            from agent.profiling.assessment_orchestrator import load_thresholds
            thresholds = load_thresholds()
        except Exception:
            thresholds = {}
        platform_target = thresholds.get("platform", {}).get("target", "fabric_spark_sql")

        standard_recs: List[Dict[str, Any]] = []
        cluster_findings: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

        datasets = (merged_dq.get("datasets") or {}) if isinstance(merged_dq.get("datasets"), dict) else {}
        for ds_name, block in datasets.items():
            total_rows = None
            if isinstance(block, dict):
                total_rows = block.get("total_rows") or block.get("row_count") or block.get("rows")
            issues = (block.get("issues") or []) if isinstance(block, dict) else []

            for it in issues[:60]:
                if not isinstance(it, dict):
                    continue

                routing = route_finding(it, dataset=ds_name, total_rows=total_rows)
                col_name = it.get("column")
                related_ds = it.get("related_dataset")
                related_col = it.get("related_column")

                if routing.recommendation_category == "standard_remediation":
                    # Standard issues bypass LLM calls entirely
                    mapped = routing.capability.default_action
                    why_it_matters, suggested_fix = build_data_aware_explanation(
                        dataset=ds_name,
                        column=col_name,
                        canonical_issue_type=routing.canonical_issue_type,
                        action=mapped,
                        affected_rows=routing.affected_rows,
                        total_rows=total_rows,
                        affected_pct=routing.affected_percentage,
                        is_standard=True,
                    )
                    standard_recs.append({
                        "priority": 1 if (it.get("severity") or "").lower() == "high" else 2,
                        "dataset": ds_name,
                        "column": col_name,
                        "issue_type": routing.canonical_issue_type,
                        "raw_issue_type": routing.raw_issue_type,
                        "related_dataset": related_ds,
                        "related_column": related_col,
                        "severity": it.get("severity") or "medium",
                        "affected_count": routing.affected_rows or 0,
                        "affected_percentage": routing.affected_percentage,
                        "why_it_matters": why_it_matters,
                        "suggested_fix": suggested_fix,
                        "mapped_action": mapped,
                        "auto_fixable": routing.auto_fix_eligible,
                        "capability_supported": routing.canonical_issue_type != "unknown_profiler_issue",
                        "authority_source": "DETERMINISTIC_REGISTRY",
                        "ai_assistance_status": "not_applicable",
                        "recommendation_category": "standard_remediation",
                        "lifecycle": routing.lifecycle,
                        "primary_reason_code": routing.primary_reason_code,
                        "reason_codes": routing.reason_codes,
                        "cluster_key": list(routing.cluster_key),
                        "evaluated_policy": {
                            "risk": routing.capability.risk_level,
                            "risk_level": routing.capability.risk_level,
                            "percentage_limit": routing.applied_impact_limit_pct,
                            "row_limit": routing.applied_impact_limit_rows,
                            "approval_policy": routing.capability.approval_policy,
                        },
                        "observed": {
                            "affected_rows": routing.affected_rows,
                            "affected_percentage": routing.affected_percentage,
                        },
                        "outcome_metadata": ACTION_OUTCOME_METADATA.get(mapped, ACTION_OUTCOME_METADATA.get("keep_as_is", {})),
                        "example_sql": None if mapped in ("keep_as_is", "skip_etl", "noop") else get_remediation_sql(platform_target, ds_name, col_name, routing.canonical_issue_type),
                        "example_pandas": get_pandas_code(col_name, routing.canonical_issue_type, action_id=mapped, related_ds=related_ds, related_col=related_col),
                        "example_pyspark": get_pyspark_code(col_name, routing.canonical_issue_type, action_id=mapped, related_ds=related_ds, related_col=related_col),
                        "risk": f"Standard low-risk transformation. Reversible: {routing.capability.reversible}.",
                        "recommendation_source": "standard_remediation",
                    })
                else:
                    # Ambiguous / contextual issue added to cluster
                    c_key = routing.cluster_key
                    if c_key not in cluster_findings:
                        cluster_findings[c_key] = []
                    finding_item = dict(it)
                    finding_item["canonical_issue_type"] = routing.canonical_issue_type
                    finding_item["raw_issue_type"] = routing.raw_issue_type
                    finding_item["affected_count"] = routing.affected_rows or 0
                    finding_item["affected_percentage"] = routing.affected_percentage
                    finding_item["reason_codes"] = routing.reason_codes
                    finding_item["primary_reason_code"] = routing.primary_reason_code
                    finding_item["evaluated_policy"] = {
                        "risk": routing.capability.risk_level,
                        "risk_level": routing.capability.risk_level,
                        "percentage_limit": routing.applied_impact_limit_pct,
                        "row_limit": routing.applied_impact_limit_rows,
                        "approval_policy": routing.capability.approval_policy,
                    }
                    finding_item["observed"] = {
                        "affected_rows": routing.affected_rows,
                        "affected_percentage": routing.affected_percentage,
                    }
                    finding_item["capability_supported"] = routing.canonical_issue_type != "unknown_profiler_issue"
                    cluster_findings[c_key].append(finding_item)

        # If no contextual issues exist, return standard recommendations directly (0 LLM calls!)
        if not cluster_findings:
            standard_recs.sort(key=lambda r: r.get("priority", 99))
            return DQRecommendations(
                recommendations=standard_recs[:80],
                summary={
                    "top_themes": ["standard_remediation_complete"],
                    "notes": "All findings resolved via standard deterministic remediations (0 LLM calls required).",
                    "disclosure": f"Showing {len(standard_recs)} standard recommendations.",
                }
            ), None

        # Execute cluster calls with bounded concurrency
        cluster_recs: List[Dict[str, Any]] = []
        total_usage: Dict[str, int] = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        completed_clusters = set()

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_LLM_CALLS) as executor:
            future_to_cluster = {
                executor.submit(_process_cluster, c_key, items, cfg, platform_target): c_key
                for c_key, items in cluster_findings.items()
            }
            try:
                for future in concurrent.futures.as_completed(future_to_cluster, timeout=OVERALL_LLM_BUDGET_SECONDS):
                    c_key = future_to_cluster[future]
                    completed_clusters.add(c_key)
                    try:
                        recs_for_cluster, usage = future.result(timeout=PER_CLUSTER_TIMEOUT_SECONDS)
                        cluster_recs.extend(recs_for_cluster)
                        if usage:
                            for k, v in usage.items():
                                total_usage[k] = total_usage.get(k, 0) + v
                    except Exception as e:
                        logger.warning(f"Failed to retrieve cluster result for {c_key}: {e}")
                        fallback_recs = _build_cluster_catalog_fallback(c_key, cluster_findings[c_key], platform_target)
                        cluster_recs.extend(fallback_recs)
            except concurrent.futures.TimeoutError:
                logger.warning(f"Global LLM budget of {OVERALL_LLM_BUDGET_SECONDS}s exceeded; falling back remaining clusters to catalog.")
                for f, c_key in future_to_cluster.items():
                    if c_key not in completed_clusters:
                        f.cancel()
                        fallback_recs = _build_cluster_catalog_fallback(c_key, cluster_findings[c_key], platform_target)
                        cluster_recs.extend(fallback_recs)

        # Combine standard and AI-assisted recommendations
        combined = standard_recs + cluster_recs

        def _rec_sort_key(r):
            sev = str(r.get("severity") or "medium").lower()
            is_rel = any(k in str(r.get("issue_type", "")).lower() for k in ["relationship", "orphan", "foreign_key"])
            if sev == "high" and is_rel:
                base_p = 0
            elif sev == "high":
                base_p = 1
            elif sev == "medium":
                base_p = 2
            elif sev == "low":
                base_p = 3
            else:
                base_p = 4
            cnt = int(r.get("affected_count") or 0)
            return (base_p, -cnt)

        combined.sort(key=_rec_sort_key)
        for i, r in enumerate(combined):
            r["priority"] = i + 1

        summary = {
            "top_themes": [f"{len(standard_recs)} standard", f"{len(cluster_recs)} decision-required"],
            "notes": f"Processed {len(standard_recs)} standard findings deterministically and {len(cluster_findings)} context clusters via AI assistance.",
            "disclosure": f"Showing top {min(len(combined), 80)} prioritized remediation recommendations.",
        }

        return DQRecommendations(recommendations=combined[:100], summary=summary), total_usage


def dq_recommendations_to_dict(r: DQRecommendations) -> Dict[str, Any]:
    return {"recommendations": r.recommendations, "summary": r.summary}


