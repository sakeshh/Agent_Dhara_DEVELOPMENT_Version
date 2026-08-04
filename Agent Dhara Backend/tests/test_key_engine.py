"""
Test suite for Sprint 3: Key Engine, Raw/Normalized Collisions, and Composite Keys.
"""

import pytest
import pandas as pd

from agent.profiling.key_engine import (
    evaluate_key_uniqueness,
    discover_candidate_keys,
    normalize_value_for_matching
)


def test_normalized_key_collision_detection():
    # ABC001, abc001, ABC001  are raw-unique, but normalized-identical!
    df = pd.DataFrame({
        "cust_id": ["ABC001", "abc001", "ABC001 ", "XYZ999"]
    })

    res = evaluate_key_uniqueness(df, ["cust_id"])

    assert res.has_raw_duplicates is False
    assert res.has_normalized_collisions is True
    assert res.raw_unique_count == 4
    assert res.normalized_unique_count == 2
    assert len(res.collision_samples) == 1


def test_composite_key_discovery():
    df = pd.DataFrame({
        "store_id": [1, 1, 2, 2],
        "item_id": [101, 102, 101, 102],
        "quantity": [5, 10, 15, 20]
    })

    candidates = discover_candidate_keys(df)
    assert ["store_id", "item_id"] in candidates or any(set(c) == {"store_id", "item_id"} for c in candidates)
