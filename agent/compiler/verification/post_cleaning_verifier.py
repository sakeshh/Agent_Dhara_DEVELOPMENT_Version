from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class CleaningStatus(str, Enum):
    CLEANED = "cleaned"
    PARTIALLY_CLEANED = "partially_cleaned"
    AUDITED_NOT_CLEANED = "audited_not_cleaned"
    BLOCKED = "blocked"
    FAILED = "failed"


class VerificationReport(BaseModel):
    dataset_name: str
    status: CleaningStatus
    is_fully_verified: bool
    input_rows: int
    clean_rows: int
    quarantined_rows: int = 0
    removed_redundant_rows: int = 0
    rejected_rows: int = 0
    merged_rows: int = 0
    row_count_reconciled: bool
    remaining_duplicates: int = 0
    schema_contract_passed: bool = True
    violations: List[str] = Field(default_factory=list)
    reconciliation_equation: str = ""


class PostCleaningVerifier:
    """
    Guarantees that technical execution success is never mistaken for successful cleaning.
    Verifies output schema conformance, exact duplicate removal, and mathematical row-count reconciliation.
    """

    @classmethod
    def verify(
        cls,
        dataset_name: str,
        input_rows: int,
        clean_rows: int,
        quarantined_rows: int = 0,
        removed_redundant_rows: int = 0,
        rejected_rows: int = 0,
        merged_rows: int = 0,
        remaining_duplicates: int = 0,
        expected_clean_cols: Optional[List[str]] = None,
        actual_clean_cols: Optional[List[str]] = None,
        duplicate_policy: Optional[Any] = None,
        is_audit_only: bool = False,
    ) -> VerificationReport:
        violations = []

        # Check duplicate policy disposition: FLAG only means audited not cleaned
        is_flag_disposition = False
        if duplicate_policy:
            disp = getattr(duplicate_policy, "disposition", None)
            disp_val = disp.value if hasattr(disp, "value") else str(disp or "")
            if disp_val.lower() == "flag":
                is_flag_disposition = True

        if is_audit_only or is_flag_disposition:
            return VerificationReport(
                dataset_name=dataset_name,
                status=CleaningStatus.AUDITED_NOT_CLEANED,
                is_fully_verified=True,
                input_rows=input_rows,
                clean_rows=clean_rows,
                quarantined_rows=quarantined_rows,
                removed_redundant_rows=removed_redundant_rows,
                rejected_rows=rejected_rows,
                merged_rows=merged_rows,
                row_count_reconciled=True,
                reconciliation_equation=f"{input_rows} input == {clean_rows} audited (flag/audit only, no records removed)",
            )

        # 1. Duplicate Verification
        if remaining_duplicates > 0:
            violations.append(f"{remaining_duplicates} targeted duplicates remain unresolved in clean output.")

        # 2. Schema Verification
        schema_passed = True
        if expected_clean_cols and actual_clean_cols:
            if set(expected_clean_cols) != set(actual_clean_cols):
                schema_passed = False
                diff = set(actual_clean_cols) - set(expected_clean_cols)
                if diff:
                    violations.append(f"Unexpected extra columns in clean output: {diff}")
                missing = set(expected_clean_cols) - set(actual_clean_cols)
                if missing:
                    violations.append(f"Missing expected columns in clean output: {missing}")

        # 3. Row-Count Reconciliation: Input == Clean + Quarantined + Removed + Rejected + Merged
        expected_input = clean_rows + quarantined_rows + removed_redundant_rows + rejected_rows + merged_rows
        reconciled = (input_rows == expected_input)
        reconciliation_eq = (
            f"{input_rows} (input) == {clean_rows} (clean) + {quarantined_rows} (quarantined) + "
            f"{removed_redundant_rows} (removed) + {rejected_rows} (rejected) + {merged_rows} (merge adj)"
        )

        if not reconciled:
            violations.append(
                f"Row reconciliation failed: {input_rows} input != {clean_rows} clean + {quarantined_rows} quarantined + "
                f"{removed_redundant_rows} removed + {rejected_rows} rejected + {merged_rows} merge adj."
            )

        # 4. Resolve Final Status
        if violations:
            status = CleaningStatus.FAILED if not reconciled else CleaningStatus.PARTIALLY_CLEANED
        else:
            status = CleaningStatus.CLEANED

        return VerificationReport(
            dataset_name=dataset_name,
            status=status,
            is_fully_verified=(len(violations) == 0),
            input_rows=input_rows,
            clean_rows=clean_rows,
            quarantined_rows=quarantined_rows,
            removed_redundant_rows=removed_redundant_rows,
            rejected_rows=rejected_rows,
            merged_rows=merged_rows,
            row_count_reconciled=reconciled,
            remaining_duplicates=remaining_duplicates,
            schema_contract_passed=schema_passed,
            violations=violations,
            reconciliation_equation=reconciliation_eq,
        )
