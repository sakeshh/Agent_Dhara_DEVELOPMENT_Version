"""
Test suite for Sprint 10: Mutation-Based Benchmarking & Precision/Recall Targets.
"""

import pytest
import pandas as pd

from tests.benchmarks.mutation_generator import inject_null_defects
from tests.benchmarks.benchmark_harness import evaluate_benchmark_performance
from agent.profiling.rule_engine import QualityRuleContext
from tests.test_rule_execution_states import DummyNullCheckRule


def test_null_injection_benchmark_precision_recall():
    clean_df = pd.DataFrame({
        "id": list(range(1, 101)),
        "email": [f"user{i}@test.com" for i in range(1, 101)]
    })

    mutated_df, logs = inject_null_defects(clean_df, "email", count=10)
    actual_rows = [log.row_index for log in logs]

    rule = DummyNullCheckRule()
    ctx = QualityRuleContext(
        dataset_name="benchmark_ds",
        df=mutated_df,
        column_name="email"
    )
    res = rule.execute(ctx)

    metrics = evaluate_benchmark_performance(
        detected_affected_rows=res.affected_rows,
        actual_mutated_rows=actual_rows,
        total_dataset_rows=len(mutated_df)
    )

    assert metrics.precision >= 0.99
    assert metrics.recall >= 0.99
    assert metrics.f1_score >= 0.99
