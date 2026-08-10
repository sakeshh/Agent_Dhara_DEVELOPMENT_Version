"""
Context-Aware Placeholder & Character Classification Engine.
Differentiates generic null placeholders from semantic sentinels,
formatted monetary values, structured identifiers, and Unicode symbols/emojis.
"""

import re
import unicodedata
from typing import Any, Dict, List, Optional, Set, Tuple
import pandas as pd
from pydantic import BaseModel, Field, ConfigDict

from agent.profiling.semantic_inference import SemanticType


GENERAL_PLACEHOLDERS: Set[str] = {
    "null", "none", "n/a", "na", "unknown", "not available", "not_available",
    "tbd", "-", "--", "???", "missing", "undefined", "blank", "."
}

SEMANTIC_PLACEHOLDERS: Dict[str, Set[str]] = {
    SemanticType.DATE: {"0000-00-00", "1900-01-01", "1970-01-01", "9999-12-31"},
    SemanticType.PHONE: {"0000000000", "1234567890", "+0000000000", "000-000-0000"},
    SemanticType.EMAIL: {"test@test.com", "none@none.com", "a@a.com", "example@example.com"},
    SemanticType.QUANTITY: {"-999", "-9999", "999999", "nan", "inf", "-inf"},
    SemanticType.CURRENCY_AMOUNT: {"-999", "-999.0", "-1.0"},
    SemanticType.IDENTIFIER: {"0", "-1", "00000000-0000-0000-0000-000000000000"}
}

MONETARY_REGEX = re.compile(
    r'^\s*([\$₹€£¥]|USD|EUR|GBP|INR|JPY|CAD|AUD)?\s*\(?\s*[\+\-]?\d{1,3}(?:[,\.\s]\d{3})*(?:[\.\,]\d{1,4})?\s*\)?\s*([\$₹€£¥]|USD|EUR|GBP|INR|JPY|CAD|AUD)?\s*$',
    re.IGNORECASE
)

IDENTIFIER_REGEX = re.compile(
    r'^[a-zA-Z0-9_\-\.\:\/]{2,64}$'
)


class PlaceholderClassification(BaseModel):
    model_config = ConfigDict(extra="allow")
    is_placeholder: bool = False
    category: str = "none"  # generic_null, semantic_sentinel, valid_value, formatted_value
    original_value: Any = None
    matched_sentinel: Optional[str] = None
    specific_diagnosis: str = "valid"


def parse_monetary_value(s_val: str) -> bool:
    """Detects if a string is a formatted currency/monetary value."""
    if not s_val:
        return False
    # Must contain at least one digit and currency symbol/code or formatted separator
    has_digit = any(c.isdigit() for c in s_val)
    if not has_digit:
        return False
    return bool(MONETARY_REGEX.match(s_val))


def classify_character_unicode(char: str) -> str:
    """Classifies a character into PUNCTUATION, SYMBOL, or EMOJI using Unicode categories."""
    if not char:
        return "UNKNOWN"
    cat = unicodedata.category(char)
    if cat.startswith("P"):
        return "PUNCTUATION"
    elif cat.startswith("S"):
        return "SYMBOL"
    elif cat.startswith("L"):
        return "LETTER"
    elif cat.startswith("N"):
        return "NUMBER"
    elif cat.startswith("Z"):
        return "SEPARATOR"
    return "OTHER"


def classify_placeholder_value(
    val: Any,
    semantic_type: Optional[str] = None
) -> PlaceholderClassification:
    """
    Classifies whether a value is a placeholder based on diagnosis precedence:
    1. Null / Missing
    2. Explicit Placeholder Token
    3. Valid Structured Identifier (e.g. c00001, CUS-0001, AB_123, 000123)
    4. Formatted Monetary Value (e.g. $1,000, ₹1,000.00, EUR 1.000,50)
    5. Semantic Sentinel Value
    6. Valid Value
    """
    # 1. Null
    if val is None or pd.isna(val):
        return PlaceholderClassification(
            is_placeholder=True,
            category="generic_null",
            original_value=val,
            matched_sentinel="NULL",
            specific_diagnosis="explicit_null"
        )

    s_val = str(val).strip()
    s_val_lower = s_val.lower()

    # 2. Generic Placeholder Tokens
    if s_val_lower in GENERAL_PLACEHOLDERS:
        return PlaceholderClassification(
            is_placeholder=True,
            category="generic_null",
            original_value=val,
            matched_sentinel=s_val_lower,
            specific_diagnosis="general_placeholder"
        )

    # 3. Currency / Monetary format guard
    if (semantic_type in (SemanticType.CURRENCY_AMOUNT, SemanticType.QUANTITY) or '$' in s_val or '₹' in s_val or '€' in s_val or '£' in s_val) and parse_monetary_value(s_val):
        return PlaceholderClassification(
            is_placeholder=False,
            category="formatted_value",
            original_value=val,
            specific_diagnosis="formatted_currency_value"
        )

    # 4. Structured Identifier guard (e.g., c00001, o000001, p000001, CUS-0001)
    if semantic_type == SemanticType.IDENTIFIER or (semantic_type is None and IDENTIFIER_REGEX.match(s_val) and any(c.isdigit() for c in s_val) and s_val_lower not in GENERAL_PLACEHOLDERS):
        return PlaceholderClassification(
            is_placeholder=False,
            category="valid_value",
            original_value=val,
            specific_diagnosis="valid_identifier"
        )

    # 5. Semantic Sentinel
    if semantic_type and semantic_type in SEMANTIC_PLACEHOLDERS:
        sentinels = SEMANTIC_PLACEHOLDERS[semantic_type]
        if s_val in sentinels or s_val_lower in sentinels:
            return PlaceholderClassification(
                is_placeholder=True,
                category="semantic_sentinel",
                original_value=val,
                matched_sentinel=s_val,
                specific_diagnosis=f"sentinel_value_for_{semantic_type}"
            )

    return PlaceholderClassification(
        is_placeholder=False,
        category="valid_value",
        original_value=val,
        specific_diagnosis="valid"
    )
