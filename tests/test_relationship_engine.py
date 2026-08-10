"""
Test suite for Sprint 4: Dedicated Relationship & Referential Integrity Engine.
"""

import pytest
import pandas as pd

from agent.profiling.relationship_engine import (
    evaluate_referential_integrity,
    RelationshipEvaluationState
)


def test_referential_integrity_pass():
    parents = pd.DataFrame({"cust_id": ["C1", "C2", "C3"]})
    children = pd.DataFrame({"cust_id": ["c1", "C2", "c1", None]})

    res = evaluate_referential_integrity(parents, "cust_id", children, "cust_id")

    assert res.status == RelationshipEvaluationState.VALIDATED_PASS
    assert res.child_rows_evaluated == 4
    assert res.null_foreign_key_rows == 1
    assert res.matched_child_rows == 3
    assert res.unmatched_child_rows == 0
    assert res.distinct_orphan_keys == 0


def test_referential_integrity_orphans():
    parents = pd.DataFrame({"cust_id": ["C1", "C2"]})
    children = pd.DataFrame({"cust_id": ["C1", "C99", "C100"]})

    res = evaluate_referential_integrity(parents, "cust_id", children, "cust_id")

    assert res.status == RelationshipEvaluationState.VALIDATED_FAIL
    assert res.matched_child_rows == 1
    assert res.unmatched_child_rows == 2
    assert res.distinct_orphan_keys == 2
    assert "c99" in res.orphan_samples or "C99" in [s.upper() for s in res.orphan_samples]
