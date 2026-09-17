"""
tests/cross_engine/normalization.py

Shared normalization utilities for cross-engine parity testing.
Calls and verifies against the authoritative canonical encoding implementation
in agent.etl_pipeline.canonical_encoding without redefining the protocol.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, Optional, Tuple

import numpy as np
import pandas as pd

from agent.etl_pipeline.canonical_encoding import (
    CANONICAL_ENCODING_VERSION,
    TAG_BOOL_FALSE,
    TAG_BOOL_TRUE,
    TAG_BYTES_PREFIX,
    TAG_DATE_PREFIX,
    TAG_DECIMAL_PREFIX,
    TAG_FLOAT_PREFIX,
    TAG_INT_PREFIX,
    TAG_NAT,
    TAG_NULL,
    TAG_STRING_PREFIX,
    TAG_TIMESTAMP_PREFIX,
    canonical_encode_scalar_v1,
)
from agent.etl_pipeline.transformation_ir import DecimalCanonicalizationMode


def normalize_scalar_to_canonical_bytes(
    v: Any,
    mode: DecimalCanonicalizationMode = DecimalCanonicalizationMode.NUMERIC_VALUE,
    norm_form: str = "NFC",
    declared_scale: Optional[int] = None,
    dataset_tz: Optional[str] = None,
) -> bytes:
    """Normalize any scalar to canonical bytes using the authoritative implementation."""
    return canonical_encode_scalar_v1(
        v,
        mode=mode,
        norm_form=norm_form,
        declared_scale=declared_scale,
        dataset_tz=dataset_tz,
    )


def normalize_decimal(
    v: Any,
    mode: DecimalCanonicalizationMode = DecimalCanonicalizationMode.NUMERIC_VALUE,
    declared_scale: Optional[int] = None,
) -> Tuple[int, int, int]:
    """
    Normalize a Decimal value into a canonical tuple:
    (sign: 0 for non-negative / 1 for negative, unscaled_magnitude_int, scale)
    Canonical zero always yields (0, 0, 0) in NUMERIC_VALUE mode.
    """
    if v is None:
        raise ValueError("Cannot normalize None as Decimal")
    dec = Decimal(str(v)) if not isinstance(v, Decimal) else v
    if not dec.is_finite():
        raise ValueError(f"Non-finite Decimal '{dec}' cannot be normalized")

    if dec.is_zero():
        target_scale = 0 if mode == DecimalCanonicalizationMode.NUMERIC_VALUE else (declared_scale if declared_scale is not None else 0)
        return (0, 0, target_scale)

    sign, digits, exp = dec.as_tuple()
    int_mag = 0
    for d in digits:
        int_mag = int_mag * 10 + d

    if mode == DecimalCanonicalizationMode.NUMERIC_VALUE:
        scale = -exp
        while scale > 0 and int_mag % 10 == 0:
            int_mag //= 10
            scale -= 1
        if scale < 0:
            int_mag *= (10 ** (-scale))
            scale = 0
    else:
        scale = declared_scale if declared_scale is not None else -exp
        current_scale = -exp
        if current_scale < scale:
            int_mag *= (10 ** (scale - current_scale))
        elif current_scale > scale:
            divisor = 10 ** (current_scale - scale)
            if int_mag % divisor != 0:
                raise ValueError(f"Decimal {dec} would lose precision in scale {scale}")
            int_mag //= divisor

    sign_bit = 1 if sign != 0 else 0
    return (sign_bit, int_mag, scale)


def normalize_timestamp_instant(
    v: Any,
    dataset_tz: Optional[str] = None,
) -> int:
    """Normalize timestamp to epoch nanoseconds in UTC."""
    if v is None or v is pd.NaT:
        raise ValueError("Cannot normalize null/NaT as valid timestamp instant")
    if isinstance(v, np.datetime64):
        ts = pd.Timestamp(v)
    elif isinstance(v, (datetime, pd.Timestamp)):
        ts = pd.Timestamp(v)
    else:
        ts = pd.Timestamp(str(v))

    if ts.tzinfo is None:
        if dataset_tz is None:
            raise ValueError("Naive timestamp requires dataset_tz for instant normalization")
        ts = ts.tz_localize(dataset_tz).tz_convert("UTC")
    else:
        ts = ts.tz_convert("UTC")

    return int(ts.value)


def normalize_row_occurrences(ids: Iterable[Any]) -> Counter:
    """Extract multiset occurrence counter from iterable of occurrence IDs."""
    return Counter(ids)
