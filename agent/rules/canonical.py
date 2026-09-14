"""
Deterministic Canonical Serialization Engine for Agent Dhara.

Implements the application-specific tagged JSON canonicalization format
(CANONICALIZATION_SCHEME = "agent-dhara-tagged-json", CANONICALIZATION_VERSION = "1.0").

Guarantees:
1. Exact numeric equivalence: 1, 1.0, 1.00, and Decimal("1.000") produce identical hashes.
2. Scientific notation & negative zero normalization: 1000 == 1e3, 0 == -0.0.
3. No type collision: numeric 1, string "1", boolean True, and user mappings produce distinct representations.
4. User mapping structural separation: user dictionaries wrapped in {"$type": "mapping", "$value": ...}.
5. Pure two-stage normalization: Python Object -> Tagged Tree -> Deterministic JSON Encoding.
6. Strict NFC unicode normalization on all strings and mapping keys.
7. Timezone-aware ISO-8601 UTC microsecond timestamps for datetimes.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import unicodedata
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

from pydantic import BaseModel

CANONICALIZATION_SCHEME: str = "agent-dhara-tagged-json"
CANONICALIZATION_VERSION: str = "1.0"


class CanonicalSerializationError(ValueError):
    """Raised when an unsupported type or ambiguous structure cannot be canonicalized."""
    pass


def _canonicalize_decimal(d: Decimal) -> str:
    """Renders Decimal in standard canonical string form: no scientific notation, trailing zeros stripped."""
    if d == 0:
        return "0"
    normalized = d.normalize()
    sign, digits, exponent = normalized.as_tuple()
    if exponent == 0:
        return str(normalized)
    s = f"{normalized:f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _normalize(obj: Any) -> Any:
    """
    Stage 1: Pure normalization of Python objects into a tagged, deterministic canonical tree.
    Guarantees structural separation between internal type tags and user dictionaries.
    """
    if obj is None:
        return None

    # CRITICAL: bool inherits from int in Python; check bool FIRST
    if isinstance(obj, bool):
        return {"$type": "boolean", "$value": obj}

    if isinstance(obj, int):
        return {"$type": "number", "$value": _canonicalize_decimal(Decimal(obj))}

    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise CanonicalSerializationError(f"Non-finite float value forbidden: {obj}")
        return {"$type": "number", "$value": _canonicalize_decimal(Decimal(str(obj)))}

    if isinstance(obj, Decimal):
        if not obj.is_finite():
            raise CanonicalSerializationError(f"Non-finite Decimal value forbidden: {obj}")
        return {"$type": "number", "$value": _canonicalize_decimal(obj)}

    if isinstance(obj, str):
        return {"$type": "string", "$value": unicodedata.normalize("NFC", obj)}

    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            raise CanonicalSerializationError(f"Naive datetime forbidden in canonical representation: {obj}")
        utc_dt = obj.astimezone(timezone.utc)
        iso_str = utc_dt.isoformat(timespec="microseconds").replace("+00:00", "Z")
        return {"$type": "datetime", "$value": iso_str}

    if isinstance(obj, Enum):
        return _normalize(obj.value)

    if isinstance(obj, BaseModel):
        return _normalize(obj.model_dump(mode="python"))

    if dataclasses.is_dataclass(obj):
        return _normalize(dataclasses.asdict(obj))

    if isinstance(obj, (set, frozenset)):
        # Normalize items once, then sort using Stage 2 encoder without re-normalizing
        normalized_items = [_normalize(item) for item in obj]
        normalized_items.sort(key=_encode_normalized_json)
        return {
            "$type": "set",
            "$value": normalized_items,
        }

    if isinstance(obj, (list, tuple)):
        return [_normalize(item) for item in obj]

    # NORMATIVE ERRATA: Wrap user mappings in a dedicated '$type': 'mapping' tag
    if isinstance(obj, (dict, Mapping)):
        entries: dict[str, Any] = {}
        for k, v in obj.items():
            if not isinstance(k, str):
                raise CanonicalSerializationError(f"Canonical mapping keys must be strings, got {type(k).__name__}")
            norm_k = unicodedata.normalize("NFC", k)
            if norm_k in entries:
                raise CanonicalSerializationError(f"Normalized mapping key collision: '{norm_k}'")
            entries[norm_k] = _normalize(v)

        return {
            "$type": "mapping",
            "$value": dict(sorted(entries.items())),
        }

    raise CanonicalSerializationError(f"Unsupported type for canonicalization: {type(obj).__name__}")


def _encode_normalized_json(normalized: Any) -> str:
    """Stage 2: Deterministic JSON encoding of an already normalized tagged tree."""
    return json.dumps(
        normalized,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_json(obj: Any) -> str:
    """
    Produces Agent Dhara's versioned, deterministic canonical JSON
    representation for identity and integrity hashing.

    This is an application-specific tagged canonical format (CANONICALIZATION_SCHEME = "agent-dhara-tagged-json").
    It is not RFC 8785 JSON Canonicalization Scheme output.
    """
    return _encode_normalized_json(_normalize(obj))


def compute_canonical_hash(obj: Any) -> str:
    """Computes SHA-256 hex digest of the canonical JSON representation of an object."""
    canonical_str = canonical_json(obj)
    return hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()
