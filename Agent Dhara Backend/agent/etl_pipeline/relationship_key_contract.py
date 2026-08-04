"""
relationship_key_contract.py — Pure Python declarative schema for relationship key normalization.
This file contains no engine (Pandas/Spark) imports to avoid coupling.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Tuple

@dataclass(frozen=True)
class RelationshipKeyContract:
    trim: bool = True
    case_fold: bool = False
    null_tokens: Tuple[str, ...] = (
        "",
        "n/a",
        "unknown",
        "none",
        "null",
        "na",
        "tbd",
        "not available",
    )
    version: str = "1.0.0"

    def contract_hash(self) -> str:
        """Return a stable SHA-256 hash of the contract configuration."""
        data = {
            "trim": self.trim,
            "case_fold": self.case_fold,
            "null_tokens": sorted(list(self.null_tokens)),
            "version": self.version,
        }
        dumped = json.dumps(data, sort_keys=True)
        return "sha256:" + hashlib.sha256(dumped.encode("utf-8")).hexdigest()

DEFAULT_CONTRACT = RelationshipKeyContract()
