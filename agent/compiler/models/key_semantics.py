from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from agent.compiler.models.profile import DatasetProfile


class EvidenceAuthority(str, Enum):
    DECLARED_PK = "DECLARED_PK"
    DECLARED_FK = "DECLARED_FK"
    DATASET_CONTRACT = "DATASET_CONTRACT"
    BUSINESS_RULE = "BUSINESS_RULE"
    PROFILING_EVIDENCE = "PROFILING_EVIDENCE"
    POTENTIAL_IDENTIFIER = "POTENTIAL_IDENTIFIER"
    NON_KEY = "NON_KEY"


NON_KEY_ATTRIBUTES: Set[str] = {
    "reasoncode",
    "statuscode",
    "postalcode",
    "countrycode",
    "candidatename",
    "paidamount",
    "amount",
    "zipcode",
    "areacode",
    "description",
    "comment",
    "comments",
    "notes",
    "message",
    "status",
    "category",
    "type",
}


@dataclass(frozen=True)
class KeySemantics:
    is_primary_key: bool
    is_foreign_key: bool
    is_unique: bool | None
    is_nullable: bool
    referenced_dataset: str | None
    referenced_column: str | None
    authority: EvidenceAuthority
    confidence: str

    @property
    def is_authoritative_key(self) -> bool:
        """Returns True if key status is backed by declared/contractual authority rather than heuristics."""
        return self.authority in (
            EvidenceAuthority.DECLARED_PK,
            EvidenceAuthority.DECLARED_FK,
            EvidenceAuthority.DATASET_CONTRACT,
            EvidenceAuthority.BUSINESS_RULE,
        )


def classify_column_semantics(
    column_name: Optional[str],
    dataset_profile: Optional[DatasetProfile] = None,
    business_rules: Optional[List[Any]] = None,
) -> KeySemantics:
    """Classifies column semantics with strict metadata-driven precedence:
    1. Declared primary key in DatasetProfile
    2. Declared foreign key in DatasetProfile or business rules
    3. Approved dataset contract / business rules
    4. Unique/non-null profiling evidence
    5. Naming heuristic classified strictly as POTENTIAL_IDENTIFIER (non-blocking suggestion)
    """
    if not column_name:
        return KeySemantics(
            is_primary_key=False,
            is_foreign_key=False,
            is_unique=None,
            is_nullable=True,
            referenced_dataset=None,
            referenced_column=None,
            authority=EvidenceAuthority.NON_KEY,
            confidence="NONE",
        )

    col_clean = column_name.strip().strip("[]")
    col_lower = col_clean.lower().replace("_", "").replace("-", "").replace(" ", "")

    # Explicit exclusions for common false-positive substring matches (e.g. ReasonCode, PostalCode, PaidAmount)
    if col_lower in NON_KEY_ATTRIBUTES:
        return KeySemantics(
            is_primary_key=False,
            is_foreign_key=False,
            is_unique=False,
            is_nullable=True,
            referenced_dataset=None,
            referenced_column=None,
            authority=EvidenceAuthority.NON_KEY,
            confidence="HIGH",
        )

    # 1. Declared primary key
    if dataset_profile and dataset_profile.primary_key_columns:
        pk_cols_lower = [p.lower().strip("[]") for p in dataset_profile.primary_key_columns]
        if col_clean.lower() in pk_cols_lower:
            return KeySemantics(
                is_primary_key=True,
                is_foreign_key=False,
                is_unique=True,
                is_nullable=False,
                referenced_dataset=None,
                referenced_column=None,
                authority=EvidenceAuthority.DECLARED_PK,
                confidence="AUTHORITATIVE",
            )

    # 2. Declared foreign key in business rules
    if business_rules:
        for r in business_rules:
            r_col = getattr(r, "column_name", None) or getattr(r, "column", None)
            if r_col and str(r_col).strip("[]").lower() == col_clean.lower():
                r_type = getattr(r, "rule_type", "") or getattr(r, "type", "")
                if "foreign_key" in str(r_type).lower() or "referential" in str(r_type).lower():
                    params = getattr(r, "parameters", {}) or {}
                    return KeySemantics(
                        is_primary_key=False,
                        is_foreign_key=True,
                        is_unique=None,
                        is_nullable=params.get("allow_null", True),
                        referenced_dataset=params.get("parent_dataset"),
                        referenced_column=params.get("parent_key"),
                        authority=EvidenceAuthority.BUSINESS_RULE,
                        confidence="AUTHORITATIVE",
                    )

    # 3. Profiling evidence
    if dataset_profile and dataset_profile.columns:
        col_prof = dataset_profile.get_column(col_clean)
        if col_prof:
            is_unique = (
                col_prof.unique_percentage >= 99.9
                or (dataset_profile.row_count > 0 and col_prof.unique_count == dataset_profile.row_count)
            )
            is_nullable = col_prof.null_count > 0 or col_prof.null_percentage > 0.0

            if col_prof.is_primary_key_candidate and is_unique and not is_nullable:
                return KeySemantics(
                    is_primary_key=True,
                    is_foreign_key=False,
                    is_unique=True,
                    is_nullable=False,
                    referenced_dataset=None,
                    referenced_column=None,
                    authority=EvidenceAuthority.PROFILING_EVIDENCE,
                    confidence="STRONG_STATISTICAL",
                )
            if col_prof.is_foreign_key_candidate:
                return KeySemantics(
                    is_primary_key=False,
                    is_foreign_key=True,
                    is_unique=is_unique,
                    is_nullable=is_nullable,
                    referenced_dataset=None,
                    referenced_column=None,
                    authority=EvidenceAuthority.PROFILING_EVIDENCE,
                    confidence="MODERATE_STATISTICAL",
                )

    # 4. Naming heuristic -> classify only as POTENTIAL_IDENTIFIER (not declared key)
    # Check if ends with id or pk or fk (as distinct word / suffix, not substring of 'paid' or 'code')
    is_id_suffix = (
        col_clean.lower().endswith(("_id", ".id", "id"))
        or col_clean.lower().endswith(("_pk", "_fk"))
        or col_clean.lower() in ("id", "pk", "fk", "key")
    )
    if is_id_suffix:
        return KeySemantics(
            is_primary_key=False,
            is_foreign_key=False,
            is_unique=None,
            is_nullable=True,
            referenced_dataset=None,
            referenced_column=None,
            authority=EvidenceAuthority.POTENTIAL_IDENTIFIER,
            confidence="HEURISTIC_SUGGESTION",
        )

    return KeySemantics(
        is_primary_key=False,
        is_foreign_key=False,
        is_unique=None,
        is_nullable=True,
        referenced_dataset=None,
        referenced_column=None,
        authority=EvidenceAuthority.NON_KEY,
        confidence="DEFAULT",
    )
