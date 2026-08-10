from __future__ import annotations
import re
from typing import Any, Dict
import pandas as pd

from agent.profiling.constants import *

def _validate_phone_phonenumbers(val: str, default_region: str = "IN") -> bool:
    """
    Validate phone using Google's libphonenumber.
    Falls back to regex if library not available.
    default_region: ISO 3166-1 alpha-2 (e.g. "IN", "US", "GB").
    Used when number has no + prefix.
    """
    val = str(val).strip()
    if val.endswith(".0"):
        val = val[:-2]
    elif "." in val:
        try:
            parts = val.split(".")
            if len(parts) == 2 and all(c == '0' for c in parts[1]):
                val = parts[0]
        except Exception:
            pass
    try:
        import phonenumbers
        # Try parsing with + prefix (international) first
        try:
            pn = phonenumbers.parse(val, None)
        except Exception:
            # Try with default region fallback
            try:
                pn = phonenumbers.parse(val, default_region)
            except Exception:
                return False
        return phonenumbers.is_valid_number(pn)
    except ImportError:
        # Graceful fallback to existing regex
        return bool(PHONE_RE.match(val))

def _detect_phone_formats(series: pd.Series) -> Dict[str, int]:
    """
    Categorize phone values into format buckets.
    Returns counts per format type.
    """
    try:
        import phonenumbers
        buckets = {"e164": 0, "national": 0, "invalid": 0, "empty": 0}
        for val in series.dropna().astype(str).head(500):
            v = val.strip()
            if v.endswith(".0"):
                v = v[:-2]
            elif "." in v:
                try:
                    parts = v.split(".")
                    if len(parts) == 2 and all(c == '0' for c in parts[1]):
                        v = parts[0]
                except Exception:
                    pass
            if not v:
                buckets["empty"] += 1
                continue
            try:
                pn = phonenumbers.parse(v, "IN")
                fmt = phonenumbers.format_number(pn, phonenumbers.PhoneNumberFormat.E164)
                if v == fmt:
                    buckets["e164"] += 1
                else:
                    buckets["national"] += 1
            except Exception:
                buckets["invalid"] += 1
        return buckets
    except ImportError:
        return {}

def _detect_date_formats(series: pd.Series) -> Dict[str, Any]:
    """
    Analyzes date patterns using regex-based buckets to flag inconsistencies.
    """
    formats = {
        "YYYY-MM-DD": r"^\d{4}-\d{2}-\d{2}$",
        "MM/DD/YYYY": r"^\d{1,2}/\d{1,2}/\d{4}$",
        "DD-MM-YYYY": r"^\d{2}-\d{2}-\d{4}$",
        "YYYY/MM/DD": r"^\d{4}/\d{2}/\d{2}$",
        "other/timestamp": r".+"
    }
    counts = {k: 0 for k in formats}
    unparsed_cnt = 0
    total_non_null = 0
    
    # We run dateutil parser to confirm it is actually parseable as date
    from dateutil import parser as du_parser
    for val in series.dropna().astype(str).head(1000):
        v = val.strip()
        if not v: continue
        total_non_null += 1
        try:
            du_parser.parse(v, fuzzy=False)
            matched = False
            for fmt_name, regex in formats.items():
                if fmt_name != "other/timestamp" and re.match(regex, v):
                    counts[fmt_name] += 1
                    matched = True
                    break
            if not matched:
                counts["other/timestamp"] += 1
        except Exception:
            unparsed_cnt += 1
            
    return {
        "counts": counts,
        "unparsed_count": unparsed_cnt,
        "total_non_null": total_non_null
    }


def is_disposable_email(email_str: str) -> bool:
    """
    Check if email domain belongs to a list of known disposable email providers.
    """
    email_str = str(email_str).strip().lower()
    if "@" not in email_str:
        return False
    domain = email_str.split("@")[-1]
    disposable_domains = {
        "mailinator.com", "guerrillamail.com", "yopmail.com", "tempmail.com", 
        "10minutemail.com", "sharklasers.com", "guerrillamailblock.com",
        "guerrillamail.net", "guerrillamail.org", "guerrillamail.biz",
        "dispostable.com", "getairmail.com", "tempmail.net", "yopmail.fr",
        "yopmail.net"
    }
    return domain in disposable_domains


def has_mojibake(value: Any) -> bool:
    """
    Detect common mojibake sequences (e.g. â€™, Ã©, â€”).
    """
    if not isinstance(value, str):
        return False
    # Common mojibake regex patterns
    mojibake_patterns = [
        r"â€™", r"Ã©", r"â€”", r"â€“", r"Ã ", r"Ã¡", r"Ã³", r"Ãº", r"Ã±",
        r"ï¿½", r"Â"
    ]
    for pattern in mojibake_patterns:
        if re.search(pattern, value):
            return True
    return False


