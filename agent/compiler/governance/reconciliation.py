from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from agent.errors import ReconciliationError


class ReconciliationResult(BaseModel):
    dataset_name: str
    is_reconciled: bool
    source_distinct_rows: int
    clean_distinct_rows: int
    flagged_and_retained_distinct_rows: int = 0
    quarantined_distinct_rows: int = 0
    rejected_distinct_rows: int = 0
    dropped_distinct_rows: int = 0
    merged_distinct_rows: int = 0
    unaccounted_rows: int = 0
    quality_event_count: int = 0
    missing_source_row_count: int = 0
    duplicated_terminal_row_count: int = 0
    irreversibly_lost_row_count: int = 0
    equation: str
    violations: List[str] = Field(default_factory=list)

    # Backwards-compatible aliases
    @property
    def input_rows(self) -> int:
        return self.source_distinct_rows

    @property
    def clean_rows(self) -> int:
        return self.clean_distinct_rows

    @property
    def quarantined_rows(self) -> int:
        return self.quarantined_distinct_rows

    @property
    def rejected_rows(self) -> int:
        return self.rejected_distinct_rows

    @property
    def dropped_rows(self) -> int:
        return self.dropped_distinct_rows

    @property
    def merged_rows(self) -> int:
        return self.merged_distinct_rows

    def to_summary_dict(self) -> Dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "source_distinct_rows": self.source_distinct_rows,
            "clean_distinct_rows": self.clean_distinct_rows,
            "flagged_and_retained_distinct_rows": self.flagged_and_retained_distinct_rows,
            "quarantined_distinct_rows": self.quarantined_distinct_rows,
            "rejected_distinct_rows": self.rejected_distinct_rows,
            "quality_event_count": self.quality_event_count,
            "missing_source_row_count": self.missing_source_row_count,
            "duplicated_terminal_row_count": self.duplicated_terminal_row_count,
            "irreversibly_lost_row_count": self.irreversibly_lost_row_count,
            "is_reconciled": self.is_reconciled,
            "equation": self.equation,
        }


class ReconciliationLedger:
    """
    Enforces the mathematical row-identity and terminal-destination invariant:
    
    Terminal Destinations (mutually exclusive per row):
    Source Row IDs == Clean Row IDs U Quarantined Row IDs U Rejected Row IDs U Dropped Row IDs U Merged Row IDs.
    
    Audit Annotations (subsets, NOT separate row destinations):
    Flagged-and-retained rows are a subset of Clean Rows. Adding flagged rows to terminal
    counts would double-count them.
    
    Guarantees that:
    1. Zero rows silently disappear without an explicit terminal disposition.
    2. Zero rows appear in multiple terminal destinations.
    3. Retained flagged rows annotate clean rows without corrupting cardinality.
    """

    @classmethod
    def reconcile(
        cls,
        dataset_name: str,
        source_distinct_rows: Optional[int] = None,
        clean_distinct_rows: Optional[int] = None,
        flagged_and_retained_distinct_rows: int = 0,
        quarantined_distinct_rows: int = 0,
        rejected_distinct_rows: int = 0,
        dropped_distinct_rows: int = 0,
        merged_distinct_rows: int = 0,
        quality_event_count: int = 0,
        raise_on_mismatch: bool = True,
        # Backwards compatible keyword arguments
        input_rows: Optional[int] = None,
        clean_rows: Optional[int] = None,
        quarantined_rows: Optional[int] = None,
        rejected_rows: Optional[int] = None,
        dropped_rows: Optional[int] = None,
        merged_rows: Optional[int] = None,
    ) -> ReconciliationResult:
        # Resolve aliases
        src_rows = input_rows if input_rows is not None else (source_distinct_rows or 0)
        cln_rows = clean_rows if clean_rows is not None else (clean_distinct_rows or 0)
        quar_rows = quarantined_rows if quarantined_rows is not None else quarantined_distinct_rows
        rej_rows = rejected_rows if rejected_rows is not None else rejected_distinct_rows
        drop_rows = dropped_rows if dropped_rows is not None else dropped_distinct_rows
        mrg_rows = merged_rows if merged_rows is not None else merged_distinct_rows

        terminal_accounted = cln_rows + quar_rows + rej_rows + drop_rows + mrg_rows
        delta = src_rows - terminal_accounted
        is_reconciled = (delta == 0)

        missing_source_count = max(0, delta)
        duplicated_terminal_count = max(0, -delta)
        irreversibly_lost_count = missing_source_count

        if flagged_and_retained_distinct_rows > 0:
            eq = (
                f"{src_rows} (source) == {cln_rows} (clean, inc. {flagged_and_retained_distinct_rows} flagged) + "
                f"{quar_rows} (quarantined) + {drop_rows} (dropped) + {mrg_rows} (merged) + {rej_rows} (rejected) "
                f"[Quality events: {quality_event_count}]"
            )
        else:
            eq = (
                f"{src_rows} (input) == {cln_rows} (clean) + {quar_rows} (quarantined) + "
                f"{drop_rows} (dropped) + {mrg_rows} (merged) + {rej_rows} (rejected)"
            )

        violations: List[str] = []
        if not is_reconciled:
            loss_detail = (
                f"{abs(delta)} rows unaccounted for (lost), {abs(delta)} rows irreversibly lost."
                if delta > 0
                else f"{abs(delta)} rows unexpectedly generated."
            )
            msg = (
                f"Row reconciliation invariant violated for dataset '{dataset_name}': "
                f"{src_rows} input != {terminal_accounted} accounted ({cln_rows} clean, {quar_rows} quarantined, "
                f"{drop_rows} dropped, {mrg_rows} merged, {rej_rows} rejected). "
                f"{loss_detail}"
            )
            violations.append(msg)
            if raise_on_mismatch:
                raise ReconciliationError(
                    message=msg,
                    input_rows=src_rows,
                    accounted_rows=terminal_accounted,
                    delta=delta,
                )

        return ReconciliationResult(
            dataset_name=dataset_name,
            is_reconciled=is_reconciled,
            source_distinct_rows=src_rows,
            clean_distinct_rows=cln_rows,
            flagged_and_retained_distinct_rows=flagged_and_retained_distinct_rows,
            quarantined_distinct_rows=quar_rows,
            rejected_distinct_rows=rej_rows,
            dropped_distinct_rows=drop_rows,
            merged_distinct_rows=mrg_rows,
            unaccounted_rows=delta,
            quality_event_count=quality_event_count,
            missing_source_row_count=missing_source_count,
            duplicated_terminal_row_count=duplicated_terminal_count,
            irreversibly_lost_row_count=irreversibly_lost_count,
            equation=eq,
            violations=violations,
        )
