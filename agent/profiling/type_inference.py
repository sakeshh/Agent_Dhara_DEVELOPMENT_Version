from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Any, FrozenSet, Optional, Set
import pandas as pd

from agent.profiling.constants import *
from agent.profiling.constants import (
    _PHONE_NAME_HINTS,
    _UUID_RE,
    _IP4_RE,
    _URL_RE,
    _BOOL_VALS,
)

@dataclass(frozen=True)
class NormalizedColumnName:
    original: str
    canonical: str
    compact: str
    tokens: FrozenSet[str]

def normalize_column_name(column_name: str) -> NormalizedColumnName:
    """Normalizes column names into canonical snake_case, compact, and token set."""
    orig = str(column_name or "").strip()
    if not orig:
        return NormalizedColumnName(original="", canonical="", compact="", tokens=frozenset())
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", orig).lower()
    canonical = re.sub(r"[^a-z0-9]+", "_", snake).strip("_")
    compact = re.sub(r"[^a-z0-9]", "", canonical)
    tokens = frozenset(t for t in canonical.split("_") if t)
    return NormalizedColumnName(
        original=orig,
        canonical=canonical,
        compact=compact,
        tokens=tokens,
    )

def normalize_column_tokens(column_name: str) -> Set[str]:
    """Compatibility helper returning a set of column tokens."""
    return set(normalize_column_name(column_name).tokens)

_CATEGORICAL_MASTER_COLUMNS: Set[str] = {
    "country", "nation", "region", "state", "city", "status", "category",
    "costcenter", "cost_center", "department", "dept", "gender", "title",
    "type", "grade", "tier", "employment_type", "work_mode", "business_unit"
}

_IDENTIFIER_PATTERNS: Set[str] = {
    "employee_id", "manager_id", "reference_id", "status_event_id", "cost_center",
    "customer_id", "student_id", "course_id", "instructor_id", "batch_id", "run_id",
    "order_id", "product_id", "vendor_id"
}

_TEMPORAL_END_NAMES: Set[str] = {
    "effective_to", "valid_to", "end_date", "end_time", "expiry_date",
    "expiration_date", "termination_date", "closed_at", "closed_on",
    "expires_at", "inactive_from", "record_effective_to"
}

_TEMPORAL_END_COMPACT_NAMES: Set[str] = {
    "effectiveto", "validto", "enddate", "endtime", "expirydate",
    "expirationdate", "terminationdate", "closedat", "closedon",
    "expiresat", "inactivefrom", "recordeffectiveto"
}

_TECHNICAL_SURROGATE_NAMES: Set[str] = {
    "rowid", "rownumber", "rownum", "sequenceid", "recordid", "internalid",
    "surrogatekey", "technicalkey", "occurrenceid", "employeerowid"
}

_SURROGATE_SEMANTIC_ROLES: Set[str] = {
    "TECHNICAL_ROW_ID", "SURROGATE_KEY", "ROUTING_IDENTITY", "OCCURRENCE_ID"
}

@dataclass
class NumericClassification:
    physical_numeric: bool
    semantic_numeric: bool
    stored_as_text: bool
    eligible_for_numeric_quality_checks: bool
    eligible_for_automatic_cast: bool
    confidence: float
    authoritative_source: str

def classify_numeric_column(
    col_name: str,
    source_declared_type: Optional[str] = None,
    approved_semantic_role: Optional[str] = None,
    sample_values: Optional[pd.Series] = None,
) -> NumericClassification:
    """
    Classifies a column as physical numeric, semantic numeric, or text.
    Strict precedence:
    1. Approved semantic role
    2. Exact canonical column hints (master data, categorical, identifier)
    3. Source database physical type
    4. Name heuristics
    5. Observed value patterns
    """
    norm = normalize_column_name(col_name)
    s_type = str(source_declared_type or "").upper().strip()
    sem_role = str(approved_semantic_role or "").upper().strip()

    # Precedence 1: Approved semantic role
    if sem_role:
        if sem_role in ("METRIC", "MEASURE", "AMOUNT", "CURRENCY", "NUMERIC"):
            return NumericClassification(
                physical_numeric=any(t in s_type for t in ("INT", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "MONEY")),
                semantic_numeric=True,
                stored_as_text=any(t in s_type for t in ("VARCHAR", "NVARCHAR", "CHAR", "TEXT", "STRING")),
                eligible_for_numeric_quality_checks=True,
                eligible_for_automatic_cast=False,
                confidence=1.0,
                authoritative_source="approved_semantic_role"
            )
        if sem_role in ("BUSINESS_IDENTIFIER", "CONTROLLED_CATEGORY", "COUNTRY", "EMAIL", "PHONE",
                        "FOREIGN_BUSINESS_IDENTIFIER", "TEMPORAL_END", "TEMPORAL_START", "DATE", "TEXT", "CATEGORICAL"):
            return NumericClassification(
                physical_numeric=False,
                semantic_numeric=False,
                stored_as_text=True,
                eligible_for_numeric_quality_checks=False,
                eligible_for_automatic_cast=False,
                confidence=1.0,
                authoritative_source="approved_semantic_role"
            )

    # Precedence 2: Master data / categorical / identifier matching on canonical or compact names
    if norm.canonical in _CATEGORICAL_MASTER_COLUMNS or norm.compact in _CATEGORICAL_MASTER_COLUMNS or norm.canonical in _IDENTIFIER_PATTERNS:
        return NumericClassification(
            physical_numeric=any(t in s_type for t in ("INT", "DECIMAL", "NUMERIC", "FLOAT", "REAL")),
            semantic_numeric=False,
            stored_as_text=True,
            eligible_for_numeric_quality_checks=False,
            eligible_for_automatic_cast=False,
            confidence=0.95,
            authoritative_source="canonical_column_name_hint"
        )
    if any(tok in _CATEGORICAL_MASTER_COLUMNS for tok in norm.tokens) and not any(m in norm.tokens for m in ("amount", "cost", "price", "rate", "score", "hours", "count", "num", "pct", "percent", "salary")):
        return NumericClassification(
            physical_numeric=False,
            semantic_numeric=False,
            stored_as_text=True,
            eligible_for_numeric_quality_checks=False,
            eligible_for_automatic_cast=False,
            confidence=0.90,
            authoritative_source="canonical_column_token_match"
        )

    # Precedence 3: Physical database type
    if s_type:
        is_phys_num = any(t in s_type for t in ("INT", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "DOUBLE", "MONEY"))
        is_text_type = any(t in s_type for t in ("VARCHAR", "NVARCHAR", "CHAR", "TEXT", "STRING"))
        if is_phys_num:
            if norm.canonical.endswith("_id") or norm.compact.endswith("id") or norm.canonical.endswith("_key") or norm.compact.endswith("key"):
                return NumericClassification(
                    physical_numeric=True,
                    semantic_numeric=False,
                    stored_as_text=False,
                    eligible_for_numeric_quality_checks=True,
                    eligible_for_automatic_cast=False,
                    confidence=0.85,
                    authoritative_source="physical_numeric_identifier"
                )
            return NumericClassification(
                physical_numeric=True,
                semantic_numeric=True,
                stored_as_text=False,
                eligible_for_numeric_quality_checks=True,
                eligible_for_automatic_cast=False,
                confidence=0.95,
                authoritative_source="source_database_type"
            )
        elif is_text_type:
            if any(num_tok in norm.tokens for num_tok in ("amount", "salary", "balance", "total", "rate", "cost", "price", "revenue", "count", "quantity")):
                return NumericClassification(
                    physical_numeric=False,
                    semantic_numeric=True,
                    stored_as_text=True,
                    eligible_for_numeric_quality_checks=True,
                    eligible_for_automatic_cast=False,
                    confidence=0.80,
                    authoritative_source="text_stored_numeric_hint"
                )
            return NumericClassification(
                physical_numeric=False,
                semantic_numeric=False,
                stored_as_text=True,
                eligible_for_numeric_quality_checks=False,
                eligible_for_automatic_cast=False,
                confidence=0.85,
                authoritative_source="source_database_type"
            )

    # Precedence 4: Fallback heuristic
    c_lower = norm.canonical
    if any(x in c_lower for x in ("phone", "email", "ssn", "zip", "postal", "date", "time", "dob", "stamp", "country", "status", "category", "gender")) or c_lower.endswith("_at"):
        return NumericClassification(
            physical_numeric=False,
            semantic_numeric=False,
            stored_as_text=True,
            eligible_for_numeric_quality_checks=False,
            eligible_for_automatic_cast=False,
            confidence=0.80,
            authoritative_source="name_heuristic"
        )
    if norm.canonical.endswith("_id") or norm.compact.endswith("id") or norm.canonical.endswith("_key") or norm.compact.endswith("key") or norm.canonical.endswith("_code") or norm.compact.endswith("code"):
        return NumericClassification(
            physical_numeric=False,
            semantic_numeric=False,
            stored_as_text=True,
            eligible_for_numeric_quality_checks=False,
            eligible_for_automatic_cast=False,
            confidence=0.80,
            authoritative_source="identifier_heuristic"
        )

    # Precedence 5: Value sampling
    if sample_values is not None and not sample_values.empty:
        non_null = sample_values.dropna()
        if len(non_null) > 0:
            num_parsed = pd.to_numeric(non_null, errors="coerce")
            valid_ratio = num_parsed.notna().sum() / len(non_null)
            if valid_ratio >= 0.8:
                return NumericClassification(
                    physical_numeric=False,
                    semantic_numeric=True,
                    stored_as_text=True,
                    eligible_for_numeric_quality_checks=True,
                    eligible_for_automatic_cast=False,
                    confidence=0.75,
                    authoritative_source="value_sampling"
                )

    return NumericClassification(
        physical_numeric=False,
        semantic_numeric=False,
        stored_as_text=True,
        eligible_for_numeric_quality_checks=False,
        eligible_for_automatic_cast=False,
        confidence=0.50,
        authoritative_source="default_fallback"
    )

def is_actual_numeric_column(
    col_name: str,
    source_declared_type: Optional[str] = None,
    approved_semantic_role: Optional[str] = None,
    approved_semantic_tag: Optional[str] = None,
) -> bool:
    """
    Check if a column is semantically numeric, filtering out identifiers,
    phones, emails, zipcodes, dates, etc.
    """
    role = approved_semantic_role or approved_semantic_tag
    return classify_numeric_column(
        col_name,
        source_declared_type=source_declared_type,
        approved_semantic_role=role
    ).eligible_for_numeric_quality_checks

_is_actual_numeric_column = is_actual_numeric_column

def is_temporal_end_column(col_name: str) -> bool:
    """Returns True if the column name represents a temporal end date/time."""
    norm = normalize_column_name(col_name)
    return norm.canonical in _TEMPORAL_END_NAMES or norm.compact in _TEMPORAL_END_COMPACT_NAMES

def _strip(x: Any) -> Any:
    return x.strip() if isinstance(x, str) else x

def _is_text_dtype(dtype) -> bool:
    ds = str(dtype).lower()
    return "object" in ds or "string" in ds or "str" in ds or "category" in ds

def detect_semantic_type(values: pd.Series, col_name: str = "") -> str:
    """
    Detect semantic type from values + column name.
    Returns one of:
    date | temporal_end | email | uuid | url | ip_address | boolean_like |
    numeric_id | phone | free_text | categorical | unknown
    """
    col_lower = col_name.lower() if col_name else ""
    # Column-name hint (fastest — no value scan needed)
    if any(hint in col_lower for hint in _PHONE_NAME_HINTS):
        return "phone"

    if is_temporal_end_column(col_name):
        return "temporal_end"

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
        is_date_hint = bool(re.search(r'(?:\b|_)(date|time|dt|created|updated|dob|birth|bday|birthday|effective|valid|expiry|closed)(?:\b|_)|(_at\b|\bat\b)', col_lower)) or is_temporal_end_column(col_name)
        for v in sample.head(30):
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

