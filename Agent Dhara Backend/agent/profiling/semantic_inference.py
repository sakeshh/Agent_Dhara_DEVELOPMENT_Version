"""
Hybrid Multi-Signal Semantic Type Inference Engine.
Separates physical storage types from semantic meanings with explicit confidence scoring,
contract precedence, and candidate hypotheses.
"""

import re
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
from pydantic import BaseModel, Field, ConfigDict


class PhysicalType:
    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    FLOAT = "float"
    BOOLEAN = "boolean"
    NULL = "null"
    DATE_NATIVE = "date_native"
    BINARY = "binary"
    ARRAY = "array"
    OBJECT = "object"


class SemanticType:
    IDENTIFIER = "identifier"
    PERSON_NAME = "person_name"
    ORGANIZATION = "organization"
    EMAIL = "email"
    PHONE = "phone"
    ADDRESS = "address"
    COUNTRY = "country"
    REGION = "region"
    POSTAL_CODE = "postal_code"
    DATE = "date"
    DATETIME = "datetime"
    CURRENCY_AMOUNT = "currency_amount"
    PERCENTAGE = "percentage"
    QUANTITY = "quantity"
    STATUS = "status"
    CATEGORY = "category"
    URL = "url"
    IP_ADDRESS = "ip_address"
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    FREE_TEXT = "free_text"
    SENSITIVE_IDENTIFIER = "sensitive_identifier"
    UNKNOWN = "unknown"


class SemanticCandidate(BaseModel):
    model_config = ConfigDict(extra="allow")
    semantic_type: str
    confidence: float
    reason: str


class ColumnTypeClassification(BaseModel):
    model_config = ConfigDict(extra="allow")
    column_name: str
    physical_types: Dict[str, float] = Field(default_factory=dict)
    primary_physical_type: str = PhysicalType.STRING
    semantic_type: str = SemanticType.UNKNOWN
    semantic_confidence: float = 0.0
    semantic_source: str = "inferred"  # contract, catalog, inferred, fallback
    candidates: List[SemanticCandidate] = Field(default_factory=list)


def analyze_physical_types(series: pd.Series) -> Dict[str, float]:
    """
    Computes distribution of physical storage types in a column.
    """
    total = len(series)
    if total == 0:
        return {PhysicalType.NULL: 1.0}

    counts = {
        PhysicalType.NULL: 0,
        PhysicalType.INTEGER: 0,
        PhysicalType.FLOAT: 0,
        PhysicalType.BOOLEAN: 0,
        PhysicalType.STRING: 0,
        PhysicalType.DATE_NATIVE: 0
    }

    for val in series.dropna():
        if pd.isna(val):
            counts[PhysicalType.NULL] += 1
        elif isinstance(val, bool):
            counts[PhysicalType.BOOLEAN] += 1
        elif isinstance(val, (int, pd.Int64Dtype)):
            counts[PhysicalType.INTEGER] += 1
        elif isinstance(val, float):
            counts[PhysicalType.FLOAT] += 1
        elif isinstance(val, (pd.Timestamp, pd.DatetimeIndex)):
            counts[PhysicalType.DATE_NATIVE] += 1
        else:
            s_val = str(val).strip()
            if s_val == "":
                counts[PhysicalType.NULL] += 1
            elif s_val.isdigit() or (s_val.startswith("-") and s_val[1:].isdigit()):
                counts[PhysicalType.INTEGER] += 1
            else:
                try:
                    float(s_val)
                    counts[PhysicalType.FLOAT] += 1
                except ValueError:
                    if s_val.lower() in ("true", "false", "yes", "no", "y", "n"):
                        counts[PhysicalType.BOOLEAN] += 1
                    else:
                        counts[PhysicalType.STRING] += 1

    null_count = total - sum(counts.values()) + counts[PhysicalType.NULL]
    counts[PhysicalType.NULL] = null_count

    return {k: round(v / total, 4) for k, v in counts.items() if v > 0}


def infer_semantic_type_with_confidence(
    column_name: str,
    series: pd.Series,
    contract_override: Optional[str] = None
) -> ColumnTypeClassification:
    """
    Multi-signal hybrid semantic type inference.
    """
    phys_dist = analyze_physical_types(series)
    primary_phys = max(phys_dist.items(), key=lambda x: x[1])[0] if phys_dist else PhysicalType.STRING

    # 1. Precedence: Contract override
    if contract_override:
        return ColumnTypeClassification(
            column_name=column_name,
            physical_types=phys_dist,
            primary_physical_type=primary_phys,
            semantic_type=contract_override,
            semantic_confidence=1.0,
            semantic_source="contract",
            candidates=[SemanticCandidate(semantic_type=contract_override, confidence=1.0, reason="Declared in contract")]
        )

    col_lower = column_name.lower().strip()
    non_null_series = series.dropna().astype(str).str.strip()
    sample = non_null_series.head(200)
    sample_len = len(sample)

    name_signal = 0.0
    value_pattern_signal = 0.0
    candidate_type = SemanticType.UNKNOWN
    reasons = []

    # Name-based signals
    if re.search(r'\b(id|code|uuid|guid|key|pk|fk|num|number)\b|_id$|_key$|^id_|^id$', col_lower):
        name_signal += 0.8
        candidate_type = SemanticType.IDENTIFIER
        reasons.append("identifier_name")
    elif re.search(r'email|mail', col_lower):
        name_signal += 0.9
        candidate_type = SemanticType.EMAIL
        reasons.append("email_name")
    elif re.search(r'phone|mobile|tel|fax', col_lower):
        name_signal += 0.85
        candidate_type = SemanticType.PHONE
        reasons.append("phone_name")
    elif re.search(r'date|time|created_at|updated_at|timestamp|dob', col_lower):
        name_signal += 0.85
        candidate_type = SemanticType.DATE
        reasons.append("date_name")
    elif re.search(r'price|cost|amount|salary|revenue|fee|tax|total|balance|payment|settle', col_lower):
        name_signal += 0.85
        candidate_type = SemanticType.CURRENCY_AMOUNT
        reasons.append("currency_name")
    elif re.search(r'country|nation', col_lower):
        name_signal += 0.85
        candidate_type = SemanticType.COUNTRY
        reasons.append("country_name")
    elif re.search(r'zip|postal|postcode', col_lower):
        name_signal += 0.8
        candidate_type = SemanticType.POSTAL_CODE
        reasons.append("postal_name")
    elif re.search(r'\bstatus\b|\bstate\b|\bflag\b|\btype\b|\bcategory\b|\bkind\b', col_lower):
        name_signal += 0.75
        candidate_type = SemanticType.STATUS
        reasons.append("status_name")

    # Pattern-based signals
    if sample_len > 0:
        email_matches = sum(1 for v in sample if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', v))
        url_matches = sum(1 for v in sample if re.match(r'^(http|https)://[^\s/$.?#].[^\s]*$', v, re.IGNORECASE))
        ip_matches = sum(1 for v in sample if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', v))
        uuid_matches = sum(1 for v in sample if re.match(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$', v))

        if email_matches / sample_len >= 0.7:
            value_pattern_signal = email_matches / sample_len
            candidate_type = SemanticType.EMAIL
        elif url_matches / sample_len >= 0.7:
            value_pattern_signal = url_matches / sample_len
            candidate_type = SemanticType.URL
        elif ip_matches / sample_len >= 0.7:
            value_pattern_signal = ip_matches / sample_len
            candidate_type = SemanticType.IP_ADDRESS
        elif uuid_matches / sample_len >= 0.7:
            value_pattern_signal = uuid_matches / sample_len
            candidate_type = SemanticType.IDENTIFIER

    # Combine weighted multi-signals.
    # Key rule: a high-confidence name signal (>= 0.7) is sufficient on its own
    # even when there is no pattern signal (pattern_signal=0 for short samples).
    # Old formula: confidence = name*0.50 + pattern*0.50 wrongly penalized
    # identifier/country/status columns with few rows.
    if value_pattern_signal > 0:
        semantic_confidence = min(1.0, round(name_signal * 0.50 + value_pattern_signal * 0.50, 2))
    elif name_signal >= 0.7:
        # Name alone is sufficient for high-signal names
        semantic_confidence = min(1.0, round(name_signal * 0.90, 2))
    elif name_signal > 0:
        semantic_confidence = min(1.0, round(name_signal * 0.65, 2))
    else:
        semantic_confidence = 0.0

    if semantic_confidence < 0.50:
        if primary_phys in (PhysicalType.INTEGER, PhysicalType.FLOAT):
            candidate_type = SemanticType.QUANTITY
            semantic_confidence = 0.55
        elif primary_phys == PhysicalType.STRING:
            candidate_type = SemanticType.FREE_TEXT
            semantic_confidence = 0.50
        else:
            candidate_type = SemanticType.UNKNOWN
            semantic_confidence = 0.0

    return ColumnTypeClassification(
        column_name=column_name,
        physical_types=phys_dist,
        primary_physical_type=primary_phys,
        semantic_type=candidate_type,
        semantic_confidence=semantic_confidence,
        semantic_source="inferred",
        candidates=[
            SemanticCandidate(
                semantic_type=candidate_type,
                confidence=semantic_confidence,
                reason=f"Multi-signal inference (name_signal={name_signal}, pattern_signal={value_pattern_signal})"
            )
        ]
    )
