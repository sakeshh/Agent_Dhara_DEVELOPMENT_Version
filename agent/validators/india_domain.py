from __future__ import annotations
import re
from typing import Any, Dict, List

# Verhoeff algorithm tables for Aadhaar checksum validation
_d = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    [1, 2, 3, 4, 0, 6, 7, 8, 9, 5],
    [2, 3, 4, 0, 1, 7, 8, 9, 5, 6],
    [3, 4, 0, 1, 2, 8, 9, 5, 6, 7],
    [4, 0, 1, 2, 3, 9, 5, 6, 7, 8],
    [5, 9, 8, 7, 6, 0, 4, 3, 2, 1],
    [6, 5, 9, 8, 7, 1, 0, 4, 3, 2],
    [7, 6, 5, 9, 8, 2, 1, 0, 4, 3],
    [8, 7, 6, 5, 9, 3, 2, 1, 0, 4],
    [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
]
_p = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    [1, 5, 7, 6, 2, 8, 3, 0, 9, 4],
    [5, 8, 0, 3, 7, 9, 6, 1, 4, 2],
    [8, 9, 1, 6, 0, 4, 3, 5, 2, 7],
    [9, 4, 5, 3, 1, 2, 6, 8, 7, 0],
    [4, 2, 8, 6, 5, 7, 3, 9, 0, 1],
    [2, 7, 9, 3, 8, 0, 6, 4, 1, 5],
    [7, 0, 4, 6, 9, 1, 3, 2, 5, 8]
]

def validate_verhoeff(number_str: str) -> bool:
    """Validate Verhoeff checksum algorithm for Aadhaar digits validation."""
    try:
        num = number_str.replace(" ", "").replace("-", "")
        if not num.isdigit():
            return False
        c = 0
        for i, item in enumerate(reversed(num)):
            c = _d[c][_p[i % 8][int(item)]]
        return c == 0
    except Exception:
        return False

def validate_gstin(value: Any) -> bool:
    """
    Validate Goods and Services Tax Identification Number (GSTIN).
    Format: 2 digits state code + 10 PAN characters + 1 entity code + Z (fixed) + 1 check digit.
    """
    if not value:
        return False
    s = str(value).replace(" ", "").strip()
    return bool(re.match(r"^\d{2}[a-zA-Z]{5}\d{4}[a-zA-Z]{1}[a-zA-Z0-9]{1}[zZ][a-zA-Z0-9]{1}$", s))

def validate_pan(value: Any) -> bool:
    """
    Validate Permanent Account Number (PAN).
    Format: 5 letters + 4 digits + 1 letter.
    """
    if not value:
        return False
    s = str(value).replace(" ", "").strip()
    return bool(re.match(r"^[a-zA-Z]{5}\d{4}[a-zA-Z]{1}$", s))

def validate_aadhaar(value: Any) -> bool:
    """
    Validate Aadhaar Number (UID).
    Format: 12 digits, first digit cannot be 0 or 1, validated via Verhoeff checksum.
    """
    if not value:
        return False
    s = str(value).replace(" ", "").replace("-", "").strip()
    if not re.match(r"^[2-9]\d{11}$", s):
        return False
    return validate_verhoeff(s)

def validate_ifsc(value: Any) -> bool:
    """
    Validate Indian Financial System Code (IFSC).
    Format: 4 letters + 0 + 6 alphanumeric characters.
    """
    if not value:
        return False
    s = str(value).replace(" ", "").strip()
    return bool(re.match(r"^[a-zA-Z]{4}0[a-zA-Z0-9]{6}$", s))

def validate_cin(value: Any) -> bool:
    """
    Validate Corporate Identification Number (CIN).
    Format: L/U (listed/unlisted) + 5 digits code + 2 letters state + 4 digits year + 3 letters type + 6 digits reg.
    """
    if not value:
        return False
    s = str(value).replace(" ", "").strip()
    return bool(re.match(r"^[uUlL]\d{5}[a-zA-Z]{2}\d{4}[a-zA-Z]{3}\d{6}$", s))

INDIA_COLUMN_PATTERNS = {
    "gstin": validate_gstin,
    "gst_no": validate_gstin,
    "gst": validate_gstin,
    "pan": validate_pan,
    "pan_card": validate_pan,
    "aadhaar": validate_aadhaar,
    "aadhar": validate_aadhaar,
    "aadhaar_no": validate_aadhaar,
    "ifsc": validate_ifsc,
    "ifsc_code": validate_ifsc,
    "cin": validate_cin,
    "cin_no": validate_cin
}
