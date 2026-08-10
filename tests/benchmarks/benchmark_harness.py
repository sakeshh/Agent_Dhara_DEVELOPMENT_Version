"""
Continuous Benchmarking Framework for High-Accuracy Data Quality Engine.
Calculates Precision, Recall, and F1 score against known mutation logs.
"""

from typing import Any, Dict, List
import pandas as pd
from pydantic import BaseModel, Field, ConfigDict

from tests.benchmarks.mutation_generator import MutationLog
from agent.profiling.rule_engine import QualityRuleContext
from agent.profiling.rule_status import RuleStatus, RuleEvaluationResult


class BenchmarkMetrics(BaseModel):
    model_config = ConfigDict(extra="allow")
    true_positives: int = 0
    false_positives: int = 0
    false_negatives: int = 0
    true_negatives: int = 0
    precision: float = 0.0
    recall: float = 0.0
    f1_score: float = 0.0


def evaluate_benchmark_performance(
    detected_affected_rows: List[int],
    actual_mutated_rows: List[int],
    total_dataset_rows: int
) -> BenchmarkMetrics:
    """
    Computes Precision, Recall, and F1 metrics for rule evaluation against known mutation logs.
    """
    detected_set = set(detected_affected_rows)
    actual_set = set(actual_mutated_rows)

    tp = len(detected_set.intersection(actual_set))
    fp = len(detected_set - actual_set)
    fn = len(actual_set - detected_set)
    tn = total_dataset_rows - (tp + fp + fn)

    precision = tp / (tp + fp) if (tp + fp) > 0 else (1.0 if fn == 0 else 0.0)
    recall = tp / (tp + fn) if (tp + fn) > 0 else (1.0 if fp == 0 else 0.0)
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0

    return BenchmarkMetrics(
        true_positives=tp,
        false_positives=fp,
        false_negatives=fn,
        true_negatives=max(0, tn),
        precision=round(precision, 4),
        recall=round(recall, 4),
        f1_score=round(f1, 4)
    )
