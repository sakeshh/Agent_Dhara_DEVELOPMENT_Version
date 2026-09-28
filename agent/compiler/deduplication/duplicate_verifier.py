from __future__ import annotations

from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class DuplicateVerificationResult(BaseModel):
    dataset_name: str
    verified: bool
    remaining_exact_duplicates: int = 0
    remaining_business_key_duplicates: int = 0
    actual_rows_removed: int = 0
    expected_rows_removed: int = 0
    violations: List[str] = Field(default_factory=list)


class DuplicateVerifier:
    """Verifies that duplicate cleaning was actually achieved without data corruption."""

    @staticmethod
    def verify(
        dataset_name: str,
        initial_row_count: int,
        final_row_count: int,
        expected_removed: int,
        remaining_exact: int = 0,
        remaining_key_dups: int = 0,
    ) -> DuplicateVerificationResult:
        violations = []
        actual_removed = initial_row_count - final_row_count

        if remaining_exact > 0:
            violations.append(f"{remaining_exact} exact duplicate rows still remain after deduplication.")

        if remaining_key_dups > 0:
            violations.append(f"{remaining_key_dups} business-key duplicate rows still remain.")

        if expected_removed > 0 and actual_removed != expected_removed:
            violations.append(
                f"Row removal mismatch: expected {expected_removed} removed rows, but actual difference is {actual_removed}."
            )

        return DuplicateVerificationResult(
            dataset_name=dataset_name,
            verified=(len(violations) == 0),
            remaining_exact_duplicates=remaining_exact,
            remaining_business_key_duplicates=remaining_key_dups,
            actual_rows_removed=actual_removed,
            expected_rows_removed=expected_removed,
            violations=violations,
        )
