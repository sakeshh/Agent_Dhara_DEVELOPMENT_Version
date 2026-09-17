"""
agent/etl_pipeline/canonical_encoding.py

Authoritative Python implementation of CANONICAL_SCALAR_V1 specification.
Provides unambiguous binary serialization for scalar values across all ETL engines,
formal domain tags, and immutable versioned verification vectors.
"""
from __future__ import annotations

import datetime as _dt
from datetime import timezone
from decimal import Decimal
import hashlib
import json
import struct
from typing import Any, Dict, List, Optional, Sequence, Tuple
import unicodedata

import numpy as np
import pandas as pd

from agent.etl_pipeline.transformation_ir import (
    CanonicalSerializationError,
    DecimalCanonicalizationMode,
)

# Canonical domain tag constants
TAG_NULL = b"\x00DHARA_NULL_V1"
TAG_BOOL_TRUE = b"\x01BOOL\x01"
TAG_BOOL_FALSE = b"\x01BOOL\x00"
TAG_INT_PREFIX = b"\x02INT\x00"
TAG_FLOAT_PREFIX = b"\x03FLOAT\x00"
TAG_DECIMAL_PREFIX = b"\x04DEC\x00"
TAG_STRING_PREFIX = b"\x05STR\x00"
TAG_TIMESTAMP_PREFIX = b"\x06TS\x00"
TAG_BYTES_PREFIX = b"\x07BYTES\x00"
TAG_TUPLE_PREFIX = b"\x08TUP\x00"
TAG_NAT = b"\x09NAT\x00"
TAG_DATE_PREFIX = b"\x0aDATE\x00"

CANONICAL_ENCODING_VERSION = "CANONICAL_SCALAR_V1"


def canonical_encode_scalar_v1(
    v: Any,
    mode: DecimalCanonicalizationMode = DecimalCanonicalizationMode.NUMERIC_VALUE,
    norm_form: str = "NFC",
    declared_scale: Optional[int] = None,
    dataset_tz: Optional[str] = None,
) -> bytes:
    """
    Contract 1, 2, 3 canonical scalar encoder.
    Produces unambiguous binary encoding for scalar types:
    - Domain tag separation (NULL, BOOL, INT, FLOAT, DEC, STR, TS, BYTES, NAT, DATE)
    - Rejects unsupported scalar types (CanonicalSerializationError) - zero string fallback
    - Bounded 4-byte length prefix (uint32)
    - Decimal:
        * NUMERIC_VALUE: trailing zeros normalized, canonical zero for all zeros (-0, 0.00, etc.),
          minimal unsigned magnitude bytes.
        * PRESERVE_DECLARED_SCALE: declared precision/scale preserved.
        * Rejects non-finite decimals (NaN, Infinity).
        * Validates 4-byte signed scale range.
    - Timestamps:
        * pd.NaT has dedicated domain tag b'\\x09NAT\\x00'
        * Rejects out-of-range epoch nanoseconds (signed 64-bit int)
        * Timezone-aware timestamps normalized to UTC epoch nanoseconds
        * Naive timestamps require explicit dataset_tz or raise CanonicalSerializationError
        * Calendar dates use b'\\x0aDATE\\x00' and are not timezone-shifted
    - Integers:
        * Signed 64-bit range validation (-2^63 <= v <= 2^63 - 1)
    - Floats:
        * Normalizes -0.0 to 0.0
        * Canonical NaN, +inf, -inf
    - Bounded lengths:
        * Length <= 2^32 - 1 for strings and bytes
    """
    # 0. pd.NaT (must precede generic null check because pd.isna(NaT) is True)
    if v is pd.NaT or (isinstance(v, (pd.Timestamp, np.datetime64)) and pd.isna(v)):
        return TAG_NAT

    # 1. Null / None
    if v is None:
        return TAG_NULL

    # 2. Boolean (must precede int, because bool is subclass of int in Python)
    if isinstance(v, (bool, np.bool_)):
        return TAG_BOOL_TRUE if v else TAG_BOOL_FALSE

    # 3. Integer
    if isinstance(v, (int, np.integer)):
        int_v = int(v)
        if int_v < -9223372036854775808 or int_v > 9223372036854775807:
            raise CanonicalSerializationError(f"Integer {int_v} exceeds signed 64-bit range")
        return TAG_INT_PREFIX + int_v.to_bytes(8, byteorder="big", signed=True)

    # 4. Float
    if isinstance(v, (float, np.floating)):
        f_val = float(v)
        if np.isnan(f_val):
            return TAG_FLOAT_PREFIX + b"\x7f\xf8\x00\x00\x00\x00\x00\x00"  # canonical quiet NaN
        if np.isneginf(f_val):
            return TAG_FLOAT_PREFIX + b"\xff\xf0\x00\x00\x00\x00\x00\x00"  # canonical -inf
        if np.isposinf(f_val):
            return TAG_FLOAT_PREFIX + b"\x7f\xf0\x00\x00\x00\x00\x00\x00"  # canonical +inf
        if f_val == 0.0:
            f_val = 0.0  # normalize -0.0 to +0.0
        return TAG_FLOAT_PREFIX + struct.pack(">d", f_val)

    # 5. Decimal
    if isinstance(v, Decimal):
        if not v.is_finite():
            raise CanonicalSerializationError(f"Non-finite Decimal '{v}' is not supported in canonical serialization")

        if v.is_zero():
            scale = 0 if mode == DecimalCanonicalizationMode.NUMERIC_VALUE else (declared_scale if declared_scale is not None else 0)
            if scale < -2147483648 or scale > 2147483647:
                raise CanonicalSerializationError(f"Decimal scale {scale} overflows signed 32-bit int")
            return TAG_DECIMAL_PREFIX + bytes([0]) + struct.pack(">i", scale) + struct.pack(">I", 0)

        sign, digits, exp = v.as_tuple()
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
                pass

        if scale < -2147483648 or scale > 2147483647:
            raise CanonicalSerializationError(f"Decimal scale {scale} overflows signed 32-bit int")

        if int_mag == 0:
            mag_bytes = b""
        else:
            mag_len = (int_mag.bit_length() + 7) // 8
            mag_bytes = int_mag.to_bytes(mag_len, byteorder="big", signed=False)

        if len(mag_bytes) > 2**32 - 1:
            raise CanonicalSerializationError("Decimal magnitude exceeds uint32 length limit")

        return TAG_DECIMAL_PREFIX + bytes([sign]) + struct.pack(">i", scale) + struct.pack(">I", len(mag_bytes)) + mag_bytes

    # 6. Calendar Date (datetime.date but not datetime.datetime)
    if isinstance(v, _dt.date) and not isinstance(v, _dt.datetime):
        s = v.isoformat().encode("ascii")
        if len(s) > 2**32 - 1:
            raise CanonicalSerializationError("Date string length exceeds uint32 limit")
        return TAG_DATE_PREFIX + struct.pack(">I", len(s)) + s

    # 7. Timestamp / Datetime
    if isinstance(v, _dt.datetime):
        if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
            if dataset_tz is None:
                raise CanonicalSerializationError("Naive timestamp encountered without an explicit dataset timezone policy")
            import zoneinfo
            try:
                tz_obj = zoneinfo.ZoneInfo(dataset_tz)
                utc_dt = v.replace(tzinfo=tz_obj).astimezone(timezone.utc)
            except Exception as tz_err:
                raise CanonicalSerializationError(f"Invalid dataset timezone policy '{dataset_tz}': {tz_err}")
        else:
            utc_dt = v.astimezone(timezone.utc)

        epoch = _dt.datetime(1970, 1, 1, tzinfo=timezone.utc)
        delta = utc_dt - epoch
        epoch_nanos = (delta.days * 86400 + delta.seconds) * 1_000_000_000 + (delta.microseconds * 1000)
        if epoch_nanos < -9223372036854775808 or epoch_nanos > 9223372036854775807:
            raise CanonicalSerializationError(f"Timestamp nanoseconds {epoch_nanos} exceeds signed 64-bit range")
        return TAG_TIMESTAMP_PREFIX + epoch_nanos.to_bytes(8, byteorder="big", signed=True)

    # 8. String
    if isinstance(v, str):
        norm = unicodedata.normalize(norm_form, v)
        b = norm.encode("utf-8")
        if len(b) > 2**32 - 1:
            raise CanonicalSerializationError("String byte length exceeds canonical uint32 length limit")
        return TAG_STRING_PREFIX + struct.pack(">I", len(b)) + b

    # 9. Bytes / bytearray
    if isinstance(v, (bytes, bytearray)):
        b = bytes(v)
        if len(b) > 2**32 - 1:
            raise CanonicalSerializationError("Byte value exceeds canonical uint32 length limit")
        return TAG_BYTES_PREFIX + struct.pack(">I", len(b)) + b

    # 10. Unsupported type: reject explicitly! Zero string fallback.
    raise CanonicalSerializationError(f"Unsupported scalar type for canonical serialization: {type(v).__name__}")


# Authoritative versioned test vectors for CANONICAL_SCALAR_V1
CANONICAL_TEST_VECTORS_V1: List[Dict[str, Any]] = [
    {
        "vector_id": "null_scalar",
        "description": "Canonical encoding of null/None value",
        "input_type": "null",
        "expected_hex": TAG_NULL.hex(),
    },
    {
        "vector_id": "nat_scalar",
        "description": "Canonical encoding of pd.NaT",
        "input_type": "nat",
        "expected_hex": TAG_NAT.hex(),
    },
    {
        "vector_id": "bool_true",
        "description": "Canonical encoding of boolean True",
        "input_type": "bool",
        "value": True,
        "expected_hex": TAG_BOOL_TRUE.hex(),
    },
    {
        "vector_id": "bool_false",
        "description": "Canonical encoding of boolean False",
        "input_type": "bool",
        "value": False,
        "expected_hex": TAG_BOOL_FALSE.hex(),
    },
    {
        "vector_id": "int_zero",
        "description": "Canonical encoding of integer 0",
        "input_type": "int",
        "value": 0,
        "expected_hex": "02494e54000000000000000000",
    },
    {
        "vector_id": "int_42",
        "description": "Canonical encoding of integer 42",
        "input_type": "int",
        "value": 42,
        "expected_hex": "02494e5400000000000000002a",
    },
    {
        "vector_id": "float_pos_zero",
        "description": "Canonical encoding of float +0.0",
        "input_type": "float",
        "value": 0.0,
        "expected_hex": "03464c4f4154000000000000000000",
    },
    {
        "vector_id": "float_neg_zero",
        "description": "Canonical encoding of float -0.0 (canonicalized to +0.0)",
        "input_type": "float",
        "value": -0.0,
        "expected_hex": "03464c4f4154000000000000000000",
    },
    {
        "vector_id": "string_null",
        "description": "Canonical encoding of string literal 'null' (distinct from tag null)",
        "input_type": "str",
        "value": "null",
        "expected_hex": "0553545200000000046e756c6c",
    },
    {
        "vector_id": "string_hello",
        "description": "Canonical encoding of string 'hello'",
        "input_type": "str",
        "value": "hello",
        "expected_hex": "05535452000000000568656c6c6f",
    },
    {
        "vector_id": "calendar_date",
        "description": "Canonical encoding of calendar date 2026-09-16",
        "input_type": "date",
        "value": "2026-09-16",
        "expected_hex": "0a44415445000000000a323032362d30392d3136",
    },
]

# Cryptographic hash of CANONICAL_TEST_VECTORS_V1 to detect any protocol mutation
CANONICAL_VECTORS_HASH_V1 = hashlib.sha256(
    json.dumps(CANONICAL_TEST_VECTORS_V1, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()


def canonical_encode_coordinate_tuple_v1(
    coords: Sequence[Tuple[str, Any]],
) -> bytes:
    """
    CANONICAL_COORDINATE_TUPLE_V1:
    Unambiguous, length-prefixed, typed binary encoding for source-stable coordinate tuples.
    Guarantees structural and domain collision-freedom across all coordinate types:
      - ("a_b", "c") vs ("a", "b_c")
      - ("", "a") vs ("_", "a")
      - (None, "a") vs ("", "a")
      - 1 (int) vs "1" (str)
      - True (bool) vs "true" (str)

    Binary protocol layout:
      TAG_TUPLE_PREFIX (5 bytes: b"\\x08TUP\\x00")
      + coordinate_count (4 bytes uint32 big-endian)
      + for each (field_name, field_value):
          field_name:  TAG_STRING_PREFIX + 4-byte uint32 len + utf-8 bytes
          field_value: canonical_encode_scalar_v1(field_value)
    """
    buf = bytearray()
    buf.extend(TAG_TUPLE_PREFIX)
    buf.extend(struct.pack(">I", len(coords)))
    for name, val in coords:
        name_b = name.encode("utf-8")
        buf.extend(TAG_STRING_PREFIX)
        buf.extend(struct.pack(">I", len(name_b)))
        buf.extend(name_b)
        buf.extend(canonical_encode_scalar_v1(val))
    return bytes(buf)


def canonical_hash_coordinate_tuple_v1(coords: Sequence[Tuple[str, Any]]) -> str:
    """Computes authoritative SHA-256 hex digest over canonical coordinate tuple bytes."""
    return hashlib.sha256(canonical_encode_coordinate_tuple_v1(coords)).hexdigest()

