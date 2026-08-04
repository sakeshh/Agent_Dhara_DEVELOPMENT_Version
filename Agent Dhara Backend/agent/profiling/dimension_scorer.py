"""
Dimension-Based Quality Scoring and Readiness Gating Engine.
Calculates weighted DQ scores across 8 core quality dimensions and applies gating overrides.
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from agent.profiling.rule_status import RuleStatus, RuleEvaluationResult


DEFAULT_DIMENSION_WEIGHTS: Dict[str, float] = {
    "completeness": 0.15,
    "validity": 0.20,
    "uniqueness": 0.15,
    "consistency": 0.10,
    "conformity": 0.10,
    "referential_integrity": 0.15,
    "accuracy": 0.10,
    "timeliness": 0.05
}


class DimensionScoreBreakdown(BaseModel):
    model_config = ConfigDict(extra="allow")
    dimension: str
    status: str = "EVALUATED"  # EVALUATED, NOT_EVALUATED
    score: Optional[float] = None
    configured_weight: float
    effective_weight: float = 0.0
    evaluated_rules_count: int = 0
    passed_rules_count: int = 0
    failed_rules_count: int = 0


class OverallQualityScore(BaseModel):
    model_config = ConfigDict(extra="allow")
    methodology: str = "dimension_scorer_v3"
    quality_score: Optional[float] = None
    score_coverage: float = 0.0
    assessment_confidence: float = 100.0
    readiness: str = "READY"  # READY, BLOCKED, UNKNOWN
    dimension_scores: Dict[str, DimensionScoreBreakdown] = Field(default_factory=dict)
    gating_blockers: List[str] = Field(default_factory=list)
    unevaluated_dimensions: List[str] = Field(default_factory=list)
    policy_version: str = "2026.08.2"


def calculate_dimension_quality_scores(
    rule_results: List[RuleEvaluationResult],
    has_missing_primary_key: bool = False,
    custom_weights: Optional[Dict[str, float]] = None,
    minimum_score_coverage: float = 0.80,
    has_partial_relationship_gating: bool = False
) -> OverallQualityScore:
    """
    Computes dimension-based quality scores and evaluates gating overrides.
    Zero-evaluated dimensions return score=None and effective_weight=0.0.
    """
    weights = custom_weights or DEFAULT_DIMENSION_WEIGHTS
    dim_results: Dict[str, List[RuleEvaluationResult]] = {dim: [] for dim in weights}

    total_rules = len(rule_results)
    exec_errors = 0

    for res in rule_results:
        if res.status == RuleStatus.EXECUTION_ERROR:
            exec_errors += 1
        dim = res.dimension.lower()
        if dim in dim_results:
            dim_results[dim].append(res)
        else:
            dim_results["validity"].append(res)

    dim_breakdowns: Dict[str, DimensionScoreBreakdown] = {}
    weighted_score_sum = 0.0
    total_weight_used = 0.0
    configured_total_weight = sum(weights.values())
    unevaluated_dims: List[str] = []

    for dim, weight in weights.items():
        results = dim_results.get(dim, [])
        eval_rules = [r for r in results if r.status in (RuleStatus.PASS, RuleStatus.FAIL)]
        
        if not eval_rules:
            dim_breakdowns[dim] = DimensionScoreBreakdown(
                dimension=dim,
                status="NOT_EVALUATED",
                score=None,
                configured_weight=weight,
                effective_weight=0.0,
                evaluated_rules_count=0,
                passed_rules_count=0,
                failed_rules_count=0
            )
            unevaluated_dims.append(dim)
            continue

        passed = [r for r in eval_rules if r.status == RuleStatus.PASS]
        failed = [r for r in eval_rules if r.status == RuleStatus.FAIL]
        total_eval_units = sum(r.evaluated_count for r in eval_rules)
        total_failed_units = sum(r.failed_count for r in eval_rules)

        if total_eval_units > 0:
            dim_score = round(100.0 * (1.0 - (total_failed_units / total_eval_units)), 2)
        else:
            dim_score = round(100.0 * (len(passed) / len(eval_rules)), 2)

        dim_breakdowns[dim] = DimensionScoreBreakdown(
            dimension=dim,
            status="EVALUATED",
            score=dim_score,
            configured_weight=weight,
            effective_weight=weight,
            evaluated_rules_count=len(eval_rules),
            passed_rules_count=len(passed),
            failed_rules_count=len(failed)
        )

        weighted_score_sum += dim_score * weight
        total_weight_used += weight

    overall_score = (
        round(weighted_score_sum / total_weight_used, 2)
        if total_weight_used > 0 else None
    )
    score_coverage = (
        round(total_weight_used / configured_total_weight, 4)
        if configured_total_weight > 0 else 0.0
    )

    # Assessment confidence
    confidence = round(100.0 * (1.0 - (exec_errors / total_rules)), 2) if total_rules > 0 else 100.0

    # Gating Overrides
    readiness = "READY"
    blockers: List[str] = []

    if overall_score is None:
        readiness = "UNKNOWN"
        blockers.append("NO_SCORE_AVAILABLE")
    elif score_coverage < minimum_score_coverage:
        readiness = "UNKNOWN"
        blockers.append(f"INSUFFICIENT_SCORE_COVERAGE: {score_coverage:.2f} < {minimum_score_coverage:.2f}")

    if has_missing_primary_key:
        readiness = "BLOCKED"
        blockers.append("Missing primary key on dataset")

    if has_partial_relationship_gating and readiness != "UNKNOWN":
        readiness = "UNKNOWN"
        blockers.append("RELATIONSHIP_PARTIALLY_EVALUATED")

    if exec_errors > 0 and (exec_errors / max(1, total_rules)) >= 0.2:
        readiness = "UNKNOWN"
        blockers.append(f"High rule execution error rate ({exec_errors}/{total_rules})")

    if overall_score is not None and overall_score < 50.0 and readiness != "UNKNOWN":
        readiness = "BLOCKED"
        blockers.append(f"Quality score below threshold: {overall_score} < 50.0")

    return OverallQualityScore(
        methodology="dimension_scorer_v3",
        quality_score=overall_score,
        score_coverage=score_coverage,
        assessment_confidence=confidence,
        readiness=readiness,
        dimension_scores=dim_breakdowns,
        gating_blockers=blockers,
        unevaluated_dimensions=unevaluated_dims,
        policy_version="2026.08.2"
    )

