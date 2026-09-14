"""
Assessment Accounting, Population Partition Verification, and Evidence Models.

Implements the frozen v9.3 assessment specification:
- Strict six-part mutually exclusive, collectively exhaustive population partition.
- Non-negative validated counts deriving evaluated_rows = pass_rows + violation_rows.
- Dual completeness metrics:
    1. Operator Evaluation Completeness & Coverage Pct
    2. Population Evaluation Completeness & Coverage Pct
- Zero-row safe arithmetic returning NOT_EVALUATED and None coverage.
- Deeply immutable, verifiable RuleAssessmentEvidence.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional, Tuple

from pydantic import BaseModel, Field, field_validator, model_validator

from agent.rules.canonical import (
    CANONICALIZATION_SCHEME,
    CANONICALIZATION_VERSION,
    canonical_json,
    compute_canonical_hash,
)
from agent.rules.rule_models import DataRuleOutcome


class AssessmentAccountingError(ValueError):
    """Raised when population partition accounting invariants are violated."""
    pass


class EvaluationCompleteness(str, Enum):
    """Level of rule evaluation coverage over applicable rows."""
    COMPLETE = "complete"
    PARTIAL = "partial"
    NOT_EVALUATED = "not_evaluated"


class EvaluationPopulationCounts(BaseModel):
    """
    Strict six-part partition of the total dataset population for a rule assessment:
    total_source_rows = pass_rows + violation_rows + null_unknown_rows
                        + operator_unknown_rows + technical_excluded_rows + out_of_scope_rows
    (or five in-scope categories when out_of_scope_rows is excluded from source population)
    """
    total_source_rows: int = Field(ge=0)
    pass_rows: int = Field(ge=0)
    violation_rows: int = Field(ge=0)
    null_unknown_rows: int = Field(ge=0)
    operator_unknown_rows: int = Field(ge=0)
    technical_excluded_rows: int = Field(ge=0)
    out_of_scope_rows: int = Field(ge=0)

    @property
    def evaluated_rows(self) -> int:
        """Definitively classified rows that received a true pass or violation determination."""
        return self.pass_rows + self.violation_rows

    @model_validator(mode="after")
    def validate_partition(self) -> "EvaluationPopulationCounts":
        partition_sum = (
            self.pass_rows
            + self.violation_rows
            + self.null_unknown_rows
            + self.operator_unknown_rows
            + self.technical_excluded_rows
            + self.out_of_scope_rows
        )
        if partition_sum != self.total_source_rows:
            raise AssessmentAccountingError(
                f"Row partition sum ({partition_sum}) does not equal total_source_rows ({self.total_source_rows}): "
                f"pass={self.pass_rows}, violation={self.violation_rows}, null_unknown={self.null_unknown_rows}, "
                f"operator_unknown={self.operator_unknown_rows}, technical_excluded={self.technical_excluded_rows}, "
                f"out_of_scope={self.out_of_scope_rows}"
            )
        return self


def calculate_completeness_metrics(
    counts: EvaluationPopulationCounts,
) -> Tuple[EvaluationCompleteness, EvaluationCompleteness, Optional[float], Optional[float]]:
    """
    Computes operator and population completeness metrics with full floating-point precision:
    Returns (op_comp, pop_comp, op_pct, pop_pct)
    """
    # 1. Operator Completeness
    # Rows that legitimately were presented to the operator (excluding out-of-scope, technical errors, and nulls)
    operator_candidate_rows = (
        counts.total_source_rows
        - counts.out_of_scope_rows
        - counts.technical_excluded_rows
        - counts.null_unknown_rows
    )

    if operator_candidate_rows <= 0:
        op_comp = EvaluationCompleteness.NOT_EVALUATED
        op_pct = None
    else:
        operator_evaluated = operator_candidate_rows - counts.operator_unknown_rows
        op_pct = (operator_evaluated / operator_candidate_rows) * 100.0
        op_comp = EvaluationCompleteness.COMPLETE if counts.operator_unknown_rows == 0 else EvaluationCompleteness.PARTIAL

    # 2. Population Completeness
    # Total population intended to be governed by business policy (excluding legitimate out-of-scope rows)
    in_scope_population = counts.total_source_rows - counts.out_of_scope_rows

    if in_scope_population <= 0:
        pop_comp = EvaluationCompleteness.NOT_EVALUATED
        pop_pct = None
    else:
        pop_pct = (counts.evaluated_rows / in_scope_population) * 100.0
        pop_comp = (
            EvaluationCompleteness.COMPLETE
            if (counts.null_unknown_rows == 0 and counts.operator_unknown_rows == 0 and counts.technical_excluded_rows == 0)
            else EvaluationCompleteness.PARTIAL
        )

    return op_comp, pop_comp, op_pct, pop_pct


class RuleAssessmentEvidence(BaseModel):
    """Immutable audit record detailing the quantitative assessment results for one rule."""
    evidence_id: str
    rule_id: str
    counts: EvaluationPopulationCounts

    operator_evaluation_completeness: EvaluationCompleteness
    population_evaluation_completeness: EvaluationCompleteness
    operator_evaluation_coverage_pct: Optional[float] = None
    population_evaluation_coverage_pct: Optional[float] = None

    data_rule_outcome: DataRuleOutcome
    evaluated_at: datetime
    execution_duration_ms: float = Field(ge=0.0)

    canonicalization_scheme: str = CANONICALIZATION_SCHEME
    canonicalization_version: str = CANONICALIZATION_VERSION

    @field_validator("evaluated_at")
    @classmethod
    def require_tz(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("evaluated_at must be timezone-aware")
        return v.astimezone(timezone.utc)

    @classmethod
    def create(
        cls,
        evidence_id: str,
        rule_id: str,
        counts: EvaluationPopulationCounts,
        evaluated_at: datetime,
        execution_duration_ms: float = 0.0,
    ) -> "RuleAssessmentEvidence":
        op_comp, pop_comp, op_pct, pop_pct = calculate_completeness_metrics(counts)

        # Determine data rule outcome
        if counts.evaluated_rows == 0:
            outcome = DataRuleOutcome.UNKNOWN
        elif counts.violation_rows > 0:
            outcome = DataRuleOutcome.FAIL
        else:
            outcome = DataRuleOutcome.PASS

        return cls(
            evidence_id=evidence_id,
            rule_id=rule_id,
            counts=counts,
            operator_evaluation_completeness=op_comp,
            population_evaluation_completeness=pop_comp,
            operator_evaluation_coverage_pct=op_pct,
            population_evaluation_coverage_pct=pop_pct,
            data_rule_outcome=outcome,
            evaluated_at=evaluated_at,
            execution_duration_ms=execution_duration_ms,
        )

    def compute_evidence_hash(self) -> str:
        """Computes deterministic hash over all evidence attributes."""
        payload = self.model_dump(mode="python")
        return compute_canonical_hash(payload)
