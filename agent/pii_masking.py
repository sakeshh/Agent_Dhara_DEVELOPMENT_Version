from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional

import pandas as pd


_EMAIL_RE = re.compile(r"^([^@]{2})[^@]*(@.*)$")

# PII detection regex patterns for scanning ALL text columns (Gap 10)
_PII_PATTERNS = {
    "email": re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"),
    "phone": re.compile(r"\b(?:\+?\d{1,3}[\s\-.]?)?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}\b"),
    "ssn": re.compile(r"\b\d{3}[\s\-]?\d{2}[\s\-]?\d{4}\b"),
    "credit_card": re.compile(r"\b(?:\d{4}[\s\-]?){3}\d{4}\b"),
    "aadhaar": re.compile(r"\b[2-9]\d{3}[\s\-]?\d{4}[\s\-]?\d{4}\b"),
    "ip_address": re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"),
    "pan": re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b"),
}


def _mask_email(s: str) -> str:
    m = _EMAIL_RE.match(s.strip())
    if not m:
        return "***"
    return f"{m.group(1)}***{m.group(2)}"


def _mask_phone(s: str) -> str:
    digits = re.sub(r"\D+", "", s)
    if len(digits) <= 4:
        return "***"
    return f"***{digits[-4:]}"


def _mask_generic(s: str) -> str:
    if len(s) <= 4:
        return "***"
    return s[:2] + "***" + s[-2:]


def is_sensitive_column(name: str, sample_values: Optional[List[Any]] = None) -> bool:
    n = (name or "").lower()
    
    # 1. Heuristic name check
    sensitive_keywords = (
        "password", "passwd", "secret", "token", "api_key", "apikey",
        "ssn", "pan", "credit", "card", "email", "phone", "mobile",
        "address", "aadhaar", "aadhar", "notes", "comment", "desc", "description", "text", "message"
    )
    if not any(k in n for k in sensitive_keywords):
        return False
        
    # Exclude obvious false-positive name patterns if sample_values are not provided
    if sample_values is None:
        if n in ("card_type", "card_category", "address_type", "pan_region", "span", "japan_region"):
            return False
        return True
        
    # 2. Content confirmation checks when sample_values are available
    non_null_samples = [str(x) for x in sample_values if x is not None and str(x).strip() != ""]
    if not non_null_samples:
        return False
        
    pattern = None
    if "email" in n:
        pattern = _PII_PATTERNS.get("email")
    elif "phone" in n or "mobile" in n:
        pattern = _PII_PATTERNS.get("phone")
    elif "ssn" in n:
        pattern = _PII_PATTERNS.get("ssn")
    elif "card" in n or "credit" in n:
        pattern = _PII_PATTERNS.get("credit_card")
    elif "aadhaar" in n or "aadhar" in n:
        pattern = _PII_PATTERNS.get("aadhaar")
    elif "pan" in n:
        pattern = _PII_PATTERNS.get("pan")
    elif "address" in n:
        if "ip" in n:
            pattern = _PII_PATTERNS.get("ip_address")
        else:
            match_count = sum(1 for v in non_null_samples[:100] if len(v) > 5)
            match_rate = match_count / len(non_null_samples[:100])
            return match_rate >= 0.3
            
    if pattern:
        check_samples = non_null_samples[:100]
        match_count = sum(1 for v in check_samples if pattern.search(v))
        match_rate = match_count / len(check_samples)
        return match_rate >= 0.3
        
    return True


def mask_value(col: str, v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (int, float, bool)):
        return v
    s = str(v)
    c = (col or "").lower()
    if "email" in c:
        return _mask_email(s)
    if "phone" in c or "mobile" in c:
        return _mask_phone(s)
    if any(k in c for k in ("password", "secret", "token", "api_key", "apikey")):
        return "***"
    return _mask_generic(s)


def mask_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Allow callers/operators to disable masking when operating on synthetic/non-sensitive data.
    # Default remains masked to avoid accidental exposure in UI previews.
    if (os.environ.get("DISABLE_PII_MASKING") or "").strip().lower() in ("1", "true", "yes", "y", "on"):
        return rows or []
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        rr: Dict[str, Any] = {}
        for k, v in r.items():
            if is_sensitive_column(str(k)):
                rr[str(k)] = mask_value(str(k), v)
            else:
                rr[str(k)] = v
        out.append(rr)
    return out


def scan_text_column_for_pii(
    series: "pd.Series",
    column_name: str,
    sample_size: int = 200,
) -> List[Dict[str, Any]]:
    """
    Scan a text column for embedded PII patterns (Gap 10).
    Scans ALL text columns, not just those with name heuristics.
    Returns list of detected PII issues.
    """
    issues: List[Dict[str, Any]] = []
    try:
        # Only scan object/string columns
        if str(series.dtype) not in ("object", "string", "str"):
            return issues

        # Sample for efficiency
        sample = series.dropna()
        if len(sample) > sample_size:
            sample = sample.sample(sample_size, random_state=42)

        for pii_type, pattern in _PII_PATTERNS.items():
            matches = 0
            sample_matches: List[str] = []
            for val in sample:
                s = str(val)
                if len(s) > 5 and pattern.search(s):
                    matches += 1
                    if len(sample_matches) < 3:
                        # Mask the matched value for safety
                        sample_matches.append(_mask_generic(s[:50]))

            if matches > 0:
                pct = round(matches / len(sample) * 100, 1)
                issues.append({
                    "column": column_name,
                    "pii_type": pii_type,
                    "matches_in_sample": matches,
                    "sample_size": len(sample),
                    "estimated_pct": pct,
                    "severity": "HIGH" if pct > 10 else "MEDIUM",
                    "message": f"Embedded {pii_type} detected in '{column_name}': ~{pct}% of sampled values",
                })
    except Exception:
        pass
    return issues


def scan_dataframe_for_pii(
    df: "pd.DataFrame",
    *,
    skip_columns: Optional[List[str]] = None,
    sample_size: int = 200,
) -> List[Dict[str, Any]]:
    """
    Scan ALL text columns of a DataFrame for embedded PII (Gap 10).
    Returns aggregated list of PII findings across all columns.
    """
    skip = set(c.lower() for c in (skip_columns or []))
    all_issues: List[Dict[str, Any]] = []

    for col in df.columns:
        if str(col).lower() in skip:
            continue
        # Skip known-safe dtypes
        if str(df[col].dtype) not in ("object", "string", "str"):
            continue
        # Enforce name heuristic + content check
        if not is_sensitive_column(str(col), df[col].dropna().tolist()):
            continue
        col_issues = scan_text_column_for_pii(df[col], str(col), sample_size=sample_size)
        all_issues.extend(col_issues)

    return all_issues


