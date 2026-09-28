from __future__ import annotations

from typing import Dict, List, Optional
from pydantic import BaseModel, Field

from agent.errors import ReconciliationError


class ReconciliationResult(BaseModel):
    dataset_name: str
    is_reconciled: bool
    input_rows: int
    clean_rows: int
    quarantined_rows: int = 0
    dropped_rows: int = 0
    merged_rows: int = 0
    rejected_rows: int = 0
    unaccounted_rows: int = 0
    equation: str
    violations: List[str] = Field(default_factory=list)


class ReconciliationLedger:
    """
    Enforces the mathematical row-count invariant:
    Input Rows == Clean + Quarantined + Dropped + Merged + Rejected.
    Guarantees that zero rows silently disappear without an explicit disposition.
    """

    @classmethod
    def reconcile(
        cls,
        dataset_name: str,
        input_rows: int,
        clean_rows: int,
        quarantined_rows: int = 0,
        dropped_rows: int = 0,
        merged_rows: int = 0,
        rejected_rows: int = 0,
        raise_on_mismatch: bool = True,
    ) -> ReconciliationResult:
        accounted = clean_rows + quarantined_rows + dropped_rows + merged_rows + rejected_rows
        delta = input_rows - accounted
        is_reconciled = (delta == 0)

        eq = (
            f"{input_rows} (input) == {clean_rows} (clean) + {quarantined_rows} (quarantined) + "
            f"{dropped_rows} (dropped) + {merged_rows} (merged) + {rejected_rows} (rejected)"
        )

        violations: List[str] = []
        if not is_reconciled:
            msg = (
                f"Row reconciliation invariant violated for dataset '{dataset_name}': "
                f"{input_rows} input != {accounted} accounted ({clean_rows} clean, {quarantined_rows} quarantined, "
                f"{dropped_rows} dropped, {merged_rows} merged, {rejected_rows} rejected). "
                f"{abs(delta)} rows {'unaccounted for (lost)' if delta > 0 else 'unexpectedly generated'}."
            )
            violations.append(msg)
            if raise_on_mismatch:
                raise ReconciliationError(
                    message=msg,
                    input_rows=input_rows,
                    accounted_rows=accounted,
                    delta=delta,
                )

        return ReconciliationResult(
            dataset_name=dataset_name,
            is_reconciled=is_reconciled,
            input_rows=input_rows,
            clean_rows=clean_rows,
            quarantined_rows=quarantined_rows,
            dropped_rows=dropped_rows,
            merged_rows=merged_rows,
            rejected_rows=rejected_rows,
            unaccounted_rows=delta,
            equation=eq,
            violations=violations,
        )
