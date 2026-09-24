"""Success scoring algorithms for cleaning strategies and pattern matching."""

from typing import Dict, Any


def calculate_strategy_score(
    historical_success_rate: float,
    usage_count: int,
    approval_count: int = 0,
    rejection_count: int = 0,
) -> float:
    """Computes a Bayesian-weighted confidence score between 0.0 and 100.0."""
    total_reviews = approval_count + rejection_count
    review_multiplier = 1.0
    if total_reviews > 0:
        approval_ratio = approval_count / total_reviews
        review_multiplier = 0.8 + (0.4 * approval_ratio)

    usage_weight = min(1.0, usage_count / 50.0)
    score = (historical_success_rate * 0.7) + (usage_weight * 30.0)
    final_score = min(99.9, max(50.0, score * review_multiplier))
    return round(final_score, 1)


def calculate_pattern_match_confidence(
    matched_columns_count: int,
    total_user_columns: int,
    exact_matches: int,
) -> float:
    """Calculates pattern matching confidence percentage."""
    if total_user_columns == 0 or matched_columns_count == 0:
        return 0.0
    user_precision = matched_columns_count / max(1, total_user_columns)
    exact_ratio = exact_matches / max(1, matched_columns_count)
    # 3+ matches with high precision produces 90-98% confidence
    volume_bonus = min(15.0, matched_columns_count * 5.0)
    confidence = (user_precision * 55.0) + (exact_ratio * 30.0) + volume_bonus
    return round(min(98.5, max(45.0, confidence)), 1)
