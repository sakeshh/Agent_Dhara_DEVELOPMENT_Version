from __future__ import annotations

from enum import Enum


class CleaningMode(str, Enum):
    """Execution mode declaring how data cleaning operations treat audit and rejected data."""
    CLEANSE_ONLY = "cleanse_only"
    CLEANSE_WITH_AUDIT = "cleanse_with_audit"
    AUDIT_ONLY = "audit_only"
    QUARANTINE = "quarantine"
