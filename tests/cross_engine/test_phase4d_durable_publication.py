"""
tests/cross_engine/test_phase4d_durable_publication.py

Phase 4D Test Suite: Durable Publication, Concurrency Fencing, and Crash Recovery (PROD-3).
Verifies:
1. Two-phase staging with atomic visibility guarantee.
2. Postcondition validation failure triggers abort and preserves production state.
3. Manifest fencing token rejects concurrent writers (OCC conflict detection).
4. Idempotent commit retry semantics (identical receipt, zero duplicate writes).
5. Deterministic abort and cleanup of staging buffers.
6. Crash recovery and orphan staging reconciliation.
"""
from __future__ import annotations

import datetime
import hashlib
import time
from typing import Any, Dict

import pandas as pd
import pytest

from agent.etl_pipeline.durable_publisher import (
    ConcurrentWriterConflictError,
    DurablePublicationCoordinator,
    IdempotencyConflictError,
    PublicationPostconditionError,
    PublicationState,
    StagingHandleNotFoundError,
)


@pytest.fixture
def coordinator():
    return DurablePublicationCoordinator()


def test_two_phase_staging_and_atomic_visibility(coordinator: DurablePublicationCoordinator):
    """Verifies that staged data is invisible to production until atomic commit."""
    target_ref = "analytics.customers_daily"
    handle = coordinator.begin_staging(
        staging_id="stage-001",
        target_ref=target_ref,
        idempotency_key="run-prod-001",
        manifest_digest="m" * 64,
    )

    df_out = pd.DataFrame({"cust_id": [1, 2, 3], "revenue": [10.5, 20.0, 30.5]})
    coordinator.stage_dataset(handle, "customers", df_out)

    # Invariant: Invisible in production prior to commit
    assert coordinator.get_published_data(target_ref) == {}
    assert coordinator.get_current_fence_token(target_ref) == 0

    # Validate postconditions
    val_res = coordinator.validate_postconditions(
        handle=handle,
        row_count_bounds={"customers": (1, 10)},
        non_null_columns={"customers": ["cust_id", "revenue"]},
    )
    assert val_res.is_valid is True
    assert val_res.passed_checks == 4

    # Atomic commit
    receipt = coordinator.commit(handle)
    assert receipt.fence_token == 1
    assert receipt.output_keys == ["customers"]
    assert receipt.output_row_counts["customers"] == 3

    # Now visible atomically in production
    pub_data = coordinator.get_published_data(target_ref)
    assert "customers" in pub_data
    assert len(pub_data["customers"]) == 3
    assert coordinator.get_current_fence_token(target_ref) == 1


def test_postcondition_failure_aborts_and_protects_production(coordinator: DurablePublicationCoordinator):
    """Postcondition check failures must prevent commit and keep production state pristine."""
    target_ref = "analytics.transactions"
    
    # Establish existing baseline in production
    handle_init = coordinator.begin_staging(
        staging_id="stage-base",
        target_ref=target_ref,
        idempotency_key="key-base",
        manifest_digest="b" * 64,
    )
    coordinator.stage_dataset(handle_init, "tx", pd.DataFrame({"id": [1], "val": [100]}))
    coordinator.validate_postconditions(handle_init)
    coordinator.commit(handle_init)
    assert coordinator.get_current_fence_token(target_ref) == 1

    # Now run bad transaction containing forbidden nulls
    handle_bad = coordinator.begin_staging(
        staging_id="stage-bad",
        target_ref=target_ref,
        idempotency_key="key-bad",
        manifest_digest="m" * 64,
    )
    bad_df = pd.DataFrame({"id": [2, 3], "val": [None, 200]})
    coordinator.stage_dataset(handle_bad, "tx", bad_df)

    val_res = coordinator.validate_postconditions(
        handle=handle_bad,
        non_null_columns={"tx": ["val"]},
    )
    assert val_res.is_valid is False
    assert any("contains prohibited null values" in err for err in val_res.errors)

    # Attempting to commit invalid handle must fail
    with pytest.raises(Exception, match="Must be VALIDATED"):
        coordinator.commit(handle_bad)

    # Abort
    coordinator.abort(handle_bad, reason="POSTCONDITION_FAILED")

    # Production remains unchanged at fence token 1 with original data
    assert coordinator.get_current_fence_token(target_ref) == 1
    pub = coordinator.get_published_data(target_ref)
    assert len(pub["tx"]) == 1
    assert pub["tx"]["id"].tolist() == [1]


def test_manifest_fencing_rejects_concurrent_writers(coordinator: DurablePublicationCoordinator):
    """Manifest generation token fencing prevents concurrent writers from corrupting state."""
    target_ref = "analytics.orders"

    # Writer A and Writer B begin concurrently against fence token 0
    handle_a = coordinator.begin_staging(
        staging_id="stage-writer-a",
        target_ref=target_ref,
        idempotency_key="key-writer-a",
        manifest_digest="a" * 64,
    )
    handle_b = coordinator.begin_staging(
        staging_id="stage-writer-b",
        target_ref=target_ref,
        idempotency_key="key-writer-b",
        manifest_digest="b" * 64,
    )

    assert handle_a.expected_fence_token == 0
    assert handle_b.expected_fence_token == 0

    coordinator.stage_dataset(handle_a, "orders", pd.DataFrame({"order_id": [101]}))
    coordinator.stage_dataset(handle_b, "orders", pd.DataFrame({"order_id": [102]}))

    coordinator.validate_postconditions(handle_a)
    coordinator.validate_postconditions(handle_b)

    # Writer A commits first, advancing token to 1
    receipt_a = coordinator.commit(handle_a)
    assert receipt_a.fence_token == 1
    assert coordinator.get_current_fence_token(target_ref) == 1

    # Writer B attempts commit with stale expected token 0 -> rejected!
    with pytest.raises(ConcurrentWriterConflictError) as exc_info:
        coordinator.commit(handle_b)

    assert "Concurrent writer conflict detected" in str(exc_info.value)
    assert exc_info.value.reason_code == "CONCURRENT_WRITER_CONFLICT"

    # Verify Writer A data won and Writer B was rejected
    pub = coordinator.get_published_data(target_ref)
    assert pub["orders"]["order_id"].tolist() == [101]


def test_idempotent_retry_semantics(coordinator: DurablePublicationCoordinator):
    """Repeated commits with same idempotency key return identical receipts without side-effects."""
    target_ref = "analytics.inventory"
    idempotency_key = "run-inventory-001"
    manifest_digest = "i" * 64

    handle = coordinator.begin_staging(
        staging_id="stage-inv-1",
        target_ref=target_ref,
        idempotency_key=idempotency_key,
        manifest_digest=manifest_digest,
    )
    coordinator.stage_dataset(handle, "items", pd.DataFrame({"sku": ["A1", "B2"]}))
    coordinator.validate_postconditions(handle)
    receipt_1 = coordinator.commit(handle)

    assert receipt_1.fence_token == 1

    # Retry with same idempotency key
    handle_retry = coordinator.begin_staging(
        staging_id="stage-inv-2",
        target_ref=target_ref,
        idempotency_key=idempotency_key,
        manifest_digest=manifest_digest,
    )
    receipt_2 = coordinator.commit(handle_retry)

    # Must be identical receipt and no new fence token bump
    assert receipt_2.commit_id == receipt_1.commit_id
    assert receipt_2.fence_token == receipt_1.fence_token
    assert coordinator.get_current_fence_token(target_ref) == 1

    # Conflicting reuse of idempotency key must raise
    with pytest.raises(IdempotencyConflictError):
        coordinator.begin_staging(
            staging_id="stage-inv-3",
            target_ref=target_ref,
            idempotency_key=idempotency_key,
            manifest_digest="different_manifest" + "0" * 46,
        )


def test_abort_and_cleanup_guarantee(coordinator: DurablePublicationCoordinator):
    """Calling abort clears staging buffer and leaves target untouched."""
    target_ref = "analytics.logs"
    handle = coordinator.begin_staging(
        staging_id="stage-abort-test",
        target_ref=target_ref,
        idempotency_key="key-abort",
        manifest_digest="l" * 64,
    )
    coordinator.stage_dataset(handle, "logs", pd.DataFrame({"msg": ["event1"]}))

    coordinator.abort(handle, reason="MANUAL_CLEANUP")

    assert handle.staging_id not in coordinator._staging_store
    assert coordinator.get_published_data(target_ref) == {}
    assert coordinator._handles[handle.staging_id].state == PublicationState.ABORTED


def test_crash_recovery_and_orphan_reconciliation(coordinator: DurablePublicationCoordinator):
    """Simulates abandoned staging handle and verifies crash recovery sweeps it."""
    target_ref = "analytics.metrics"
    
    handle = coordinator.begin_staging(
        staging_id="stage-crashed-run",
        target_ref=target_ref,
        idempotency_key="key-crashed",
        manifest_digest="c" * 64,
    )
    coordinator.stage_dataset(handle, "metrics", pd.DataFrame({"cpu": [99.9]}))

    # Manually backdate the created_at to simulate older orphaned stage
    old_time = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=2)).isoformat()
    coordinator._handles[handle.staging_id].created_at = old_time

    # Run crash recovery reconciliation
    stats = coordinator.recover_and_reconcile(orphan_timeout_seconds=300.0)

    assert stats["orphans_cleaned"] == 1
    assert handle.staging_id not in coordinator._staging_store
    assert coordinator._handles[handle.staging_id].state == PublicationState.ABORTED
    assert coordinator.get_published_data(target_ref) == {}
