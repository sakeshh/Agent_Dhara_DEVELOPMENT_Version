from __future__ import annotations

from typing import List
from agent.compiler.keys.key_candidate import KeyResolution


class KeyValidator:
    """Validates that a business key meets integrity and safety invariants before deduplication."""

    @staticmethod
    def validate_resolution(resolution: KeyResolution) -> tuple[bool, List[str]]:
        errors = []

        if not resolution.columns:
            errors.append("Business key resolution contains an empty column list.")

        if resolution.confidence < 80.0 and not resolution.approved:
            errors.append(
                f"Business key confidence is {resolution.confidence:.1f}% (< 80%). "
                "Destructive deduplication requires explicit human approval."
            )

        if not resolution.is_usable_for_destructive_dedup and not resolution.approved:
            errors.append("Inferred key is not authorized for destructive row removal without approval.")

        return (len(errors) == 0, errors)
