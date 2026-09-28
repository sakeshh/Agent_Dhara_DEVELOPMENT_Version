import pytest
from agent.compiler.deduplication.duplicate_policy import (
    DuplicateDisposition,
    DuplicateResolutionPlan,
    DuplicateType,
    SurvivorStrategy,
)
from agent.compiler.deduplication.survivor_selector import SurvivorSelector
from agent.compiler.deduplication.duplicate_verifier import DuplicateVerifier


def test_survivor_selector_latest_timestamp():
    plan = DuplicateResolutionPlan(
        dataset_name="events",
        duplicate_type=DuplicateType.BUSINESS_KEY,
        keys=["event_id"],
        disposition=DuplicateDisposition.REMOVE,
        survivor_strategy=SurvivorStrategy.LATEST,
        timestamp_column="event_time",
        approved=True,
    )
    sql_order = SurvivorSelector.get_sql_order_clause(plan)
    assert sql_order == "[event_time] DESC"

    sort_cols, ascending = SurvivorSelector.get_python_sort_criteria(plan)
    assert sort_cols == ["event_time"]
    assert ascending == [False]


def test_duplicate_verifier_catches_remaining_duplicates():
    result = DuplicateVerifier.verify(
        dataset_name="orders",
        initial_row_count=1000,
        final_row_count=950,
        expected_removed=50,
        remaining_exact=2,  # 2 duplicates still remain!
        remaining_key_dups=0,
    )
    assert result.verified is False
    assert any("2 exact duplicate rows still remain" in v for v in result.violations)


def test_duplicate_verifier_success():
    result = DuplicateVerifier.verify(
        dataset_name="customers",
        initial_row_count=500,
        final_row_count=480,
        expected_removed=20,
        remaining_exact=0,
        remaining_key_dups=0,
    )
    assert result.verified is True
    assert len(result.violations) == 0
    assert result.actual_rows_removed == 20
