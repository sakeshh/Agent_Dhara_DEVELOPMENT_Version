"""
Dedicated Relationship and Referential Integrity Engine.
Evaluates declared, catalog-discovered, and statistically inferred relationships with
direction inference, raw & normalized matching, and distinct orphan tracking.
"""

from typing import Any, Dict, List, Optional, Set, Tuple
import pandas as pd
from pydantic import BaseModel, Field, ConfigDict

from agent.profiling.key_engine import normalize_value_for_matching


class RelationshipEvaluationState:
    VALIDATED_PASS = "VALIDATED_PASS"
    VALIDATED_FAIL = "VALIDATED_FAIL"
    PARTIAL_PASS = "PARTIAL_PASS"
    NOT_EVALUATED = "NOT_EVALUATED"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    EXECUTION_ERROR = "EXECUTION_ERROR"


RELATIONSHIP_GATING_MAP = {
    RelationshipEvaluationState.VALIDATED_PASS: "PASS",
    RelationshipEvaluationState.VALIDATED_FAIL: "FAIL",
    RelationshipEvaluationState.PARTIAL_PASS: "PARTIAL_PASS",
    RelationshipEvaluationState.NOT_EVALUATED: "UNKNOWN",
    RelationshipEvaluationState.CONFIGURATION_ERROR: "UNKNOWN",
    RelationshipEvaluationState.EXECUTION_ERROR: "UNKNOWN",
    RelationshipEvaluationState.INSUFFICIENT_EVIDENCE: "UNKNOWN",
}


class RelationshipResult(BaseModel):
    model_config = ConfigDict(extra="allow")
    parent_dataset: str
    parent_column: str
    child_dataset: str
    child_column: str
    relationship_source: str = "declared"  # declared, metadata, inferred
    status: str = RelationshipEvaluationState.NOT_EVALUATED
    child_rows_evaluated: int = 0
    null_foreign_key_rows: int = 0
    matched_child_rows: int = 0
    unmatched_child_rows: int = 0
    distinct_orphan_keys: int = 0
    parent_keys_with_no_children: int = 0
    raw_match_rate: float = 0.0
    normalized_match_rate: float = 0.0
    orphan_rate: float = 0.0
    orphan_samples: List[Any] = Field(default_factory=list)
    error_message: Optional[str] = None

    @property
    def gating_result(self) -> str:
        g_res = RELATIONSHIP_GATING_MAP.get(self.status, "UNKNOWN")
        if self.status != RelationshipEvaluationState.VALIDATED_PASS and g_res == "PASS":
            raise ValueError(f"Relationship status {self.status} illegally mapped to PASS gating")
        if self.status == RelationshipEvaluationState.NOT_EVALUATED and g_res == "PASS":
            raise ValueError("NOT_EVALUATED relationship cannot have PASS gating")
        return g_res



def evaluate_referential_integrity(
    parent_df: pd.DataFrame,
    parent_col: str,
    child_df: pd.DataFrame,
    child_col: str,
    parent_dataset_name: str = "parent",
    child_dataset_name: str = "child",
    relationship_source: str = "declared"
) -> RelationshipResult:
    """
    Evaluates referential integrity between parent dataset key and child foreign key.
    Calculates both raw and normalized match rates and distinct orphan keys.
    """
    # 1. Configuration Validation
    if parent_df is None or child_df is None:
        return RelationshipResult(
            parent_dataset=parent_dataset_name,
            parent_column=parent_col,
            child_dataset=child_dataset_name,
            child_column=child_col,
            relationship_source=relationship_source,
            status=RelationshipEvaluationState.CONFIGURATION_ERROR,
            error_message="One or both DataFrames are None"
        )

    if parent_col not in parent_df.columns or child_col not in child_df.columns:
        return RelationshipResult(
            parent_dataset=parent_dataset_name,
            parent_column=parent_col,
            child_dataset=child_dataset_name,
            child_column=child_col,
            relationship_source=relationship_source,
            status=RelationshipEvaluationState.CONFIGURATION_ERROR,
            error_message=f"Columns missing: {parent_col} in {parent_dataset_name} or {child_col} in {child_dataset_name}"
        )

    child_series = child_df[child_col]
    parent_series = parent_df[parent_col]

    child_total = len(child_series)
    null_fk_count = int(child_series.isna().sum())
    non_null_child = child_series.dropna()

    if len(non_null_child) == 0:
        return RelationshipResult(
            parent_dataset=parent_dataset_name,
            parent_column=parent_col,
            child_dataset=child_dataset_name,
            child_column=child_col,
            relationship_source=relationship_source,
            status=RelationshipEvaluationState.INSUFFICIENT_EVIDENCE,
            child_rows_evaluated=child_total,
            null_foreign_key_rows=null_fk_count,
            error_message="No non-null child values to evaluate"
        )

    # Raw sets
    raw_parent_keys: Set[Any] = set(parent_series.dropna())
    raw_child_values = list(non_null_child)

    raw_matched = sum(1 for val in raw_child_values if val in raw_parent_keys)

    # Normalized sets
    norm_parent_keys: Set[str] = {normalize_value_for_matching(v) for v in parent_series.dropna()}
    norm_child_values = [normalize_value_for_matching(v) for v in raw_child_values]

    norm_matched = sum(1 for val in norm_child_values if val in norm_parent_keys)
    unmatched_count = len(norm_child_values) - norm_matched

    orphan_keys = set(val for val in norm_child_values if val not in norm_parent_keys)
    parent_no_children = len(norm_parent_keys - set(norm_child_values))

    raw_match_rate = round(raw_matched / len(raw_child_values), 4) if raw_child_values else 0.0
    norm_match_rate = round(norm_matched / len(raw_child_values), 4) if raw_child_values else 0.0
    orphan_rate = round(unmatched_count / len(raw_child_values), 4) if raw_child_values else 0.0

    if unmatched_count == 0:
        status = RelationshipEvaluationState.VALIDATED_PASS
    elif norm_match_rate >= 0.90:
        status = RelationshipEvaluationState.PARTIAL_PASS
    else:
        status = RelationshipEvaluationState.VALIDATED_FAIL

    return RelationshipResult(
        parent_dataset=parent_dataset_name,
        parent_column=parent_col,
        child_dataset=child_dataset_name,
        child_column=child_col,
        relationship_source=relationship_source,
        status=status,
        child_rows_evaluated=child_total,
        null_foreign_key_rows=null_fk_count,
        matched_child_rows=norm_matched,
        unmatched_child_rows=unmatched_count,
        distinct_orphan_keys=len(orphan_keys),
        parent_keys_with_no_children=parent_no_children,
        raw_match_rate=raw_match_rate,
        normalized_match_rate=norm_match_rate,
        orphan_rate=orphan_rate,
        orphan_samples=list(orphan_keys)[:10]
    )
