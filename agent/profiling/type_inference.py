from __future__ import annotations
import re
from typing import Any, Optional
import pandas as pd

from agent.profiling.constants import *
from agent.profiling.constants import (
    _PHONE_NAME_HINTS,
    _UUID_RE,
    _IP4_RE,
    _URL_RE,
    _BOOL_VALS,
)

def _strip(x: Any) -> Any:
    return x.strip() if isinstance(x, str) else x

def _is_text_dtype(dtype) -> bool:
    ds = str(dtype).lower()
    return "object" in ds or "string" in ds or "str" in ds or "category" in ds

def _is_actual_numeric_column(col_name: str, approved_semantic_tag: Optional[str] = None) -> bool:
    """
    Check if a column is semantically numeric, filtering out identifiers,
    phones, emails, zipcodes, dates, etc.
    If approved_semantic_tag is provided, it overrides the default heuristics.
    """
    if approved_semantic_tag is not None:
        tag_lower = approved_semantic_tag.lower()
        if tag_lower == "metric":
            return True
        if tag_lower in ("id", "categorical", "date", "text"):
            return False

    c_lower = str(col_name).lower()
    if any(x in c_lower for x in ("phone", "email", "ssn", "zip", "postal", "date", "time", "dob", "stamp")) or c_lower.endswith("_at"):
        return False
    if c_lower.endswith("id") or c_lower.endswith("key") or c_lower.endswith("code"):
        return False
    if any(x in c_lower for x in ("student_id", "course_id", "instructor_id", "batch_id", "run_id")):
        return False
    return True

def detect_semantic_type(values: pd.Series, col_name: str = "") -> str:
    """
    Detect semantic type from values + column name.
    Returns one of:
    date | email | uuid | url | ip_address | boolean_like |
    numeric_id | phone | free_text | categorical | unknown
    """
    col_lower = col_name.lower() if col_name else ""
    # Column-name hint (fastest — no value scan needed)
    if any(hint in col_lower for hint in _PHONE_NAME_HINTS):
        return "phone"

    non_null_vals = values.dropna()
    if len(non_null_vals) > 200:
        sample = non_null_vals.sample(n=200, random_state=42).astype(str)
    else:
        sample = non_null_vals.astype(str)

    if sample.empty:
        return "unknown"

    total = len(sample)

    # UUID — check before numeric_id (UUIDs contain digits)
    if (sample.str.match(_UUID_RE).sum() / total) >= 0.7:
        return "uuid"

    # IP address
    if (sample.str.match(_IP4_RE).sum() / total) >= 0.6:
        return "ip_address"

    # URL
    if (sample.str.match(_URL_RE).sum() / total) >= 0.5:
        return "url"

    # Email
    if sample.str.contains("@", na=False).sum() / total >= 0.5:
        return "email"

    # Boolean-like
    if (sample.str.strip().str.lower().isin(_BOOL_VALS).sum() / total) >= 0.8:
        return "boolean_like"

    # Date (ISO-8601 first, then broader)
    if sample.str.match(r'^\d{4}-\d{2}-\d{2}').sum() / total >= 0.5:
        return "date"

    # Broader date detection using dateutil
    try:
        from dateutil import parser as du_parser
        parsed_ok = 0
        is_date_hint = bool(re.search(r'(?:\b|_)(date|time|dt|created|updated|dob|birth|bday|birthday)(?:\b|_)|(_at\b|\bat\b)', col_lower))
        for v in sample.head(30):
            # If it's a simple numeric value, don't parse as date unless column name hints date or len is 8 (YYYYMMDD)
            val_strip = v.strip()
            if val_strip.replace(".", "", 1).isdigit():
                val_clean = val_strip.split(".")[0]
                if not (is_date_hint or len(val_clean) == 8):
                    continue
            try:
                du_parser.parse(v, fuzzy=False)
                parsed_ok += 1
            except Exception:
                pass
        if parsed_ok / min(30, total) >= 0.7:
            return "date"
    except ImportError:
        pass

    # India-specific identifier semantic types (Component 13)
    from agent.validators.india_domain import INDIA_COLUMN_PATTERNS
    for pattern, validator in INDIA_COLUMN_PATTERNS.items():
        if pattern == col_lower or f"_{pattern}" in col_lower or f"{pattern}_" in col_lower:
            non_null_sample = [v for v in sample if v and str(v).lower() != "nan" and str(v).lower() != "null"]
            if non_null_sample:
                valid_count = sum(1 for v in non_null_sample if validator(v))
                if valid_count / len(non_null_sample) >= 0.4:
                    return pattern

    # Numeric ID
    if sample.str.fullmatch(r'\d+').sum() / total >= 0.9:
        return "numeric_id"

    # Free text vs categorical: use mean length
    mean_len = sample.str.len().mean()
    if mean_len > 50:
        return "free_text"

    return "categorical"

def _dtype_inference_for_object(series: pd.Series) -> Optional[str]:
    """
    For object dtype, give a human hint for UI:
    - "string" | "numeric_like" | "datetime_like" | "boolean_like" | "nested" | "mixed" | "unknown"
    """
    s = series.dropna().map(_strip)
    if len(s) > 10000:
        s = s.sample(10000, random_state=42)

    # nested?
    try:
        if s.apply(lambda v: isinstance(v, (list, dict))).any():
            return "nested"
    except Exception:
        pass

    # boolean-like
    booleans = {"true", "false", "yes", "no", "0", "1"}
    try:
        if (s.astype(str).str.lower().isin(booleans).mean() > 0.8):
            return "boolean_like"
    except Exception:
        pass

    # numeric-like
    try:
        num = pd.to_numeric(s, errors="coerce")
        if (1.0 - float(num.isna().mean())) > 0.8:
            return "numeric_like"
    except Exception:
        pass

    # datetime-like (guarded: require date-ish separators; avoid numeric IDs being miscast)
    try:
        as_str = s.astype(str)
        # Require at least some obvious date delimiters in the sample.
        # This prevents numeric IDs like 1/2/3... from being interpreted as datetimes by pandas.
        if (as_str.str.contains(r"[-/:T]", regex=True).mean() >= 0.20):
            import warnings
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="Could not infer format, so each element will be parsed individually",
                )
                dt_coerced = pd.to_datetime(s, errors="coerce")
            if (1.0 - float(dt_coerced.isna().mean())) > 0.8:
                return "datetime_like"
    except Exception:
        pass

    # plain strings?
    try:
        if s.apply(lambda v: isinstance(v, str)).mean() > 0.8:
            return "string"
    except Exception:
        pass

    try:
        if not s.empty:
            return "mixed"
    except Exception:
        pass
    return "unknown"

