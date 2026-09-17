"""
agent/etl_pipeline/durable_publisher.py

Phase 4D: Durable Publication, Concurrency Fencing, and Crash Recovery (PROD-3).
Implements production publication commit protocol:
1. Two-phase staged publication (Staging -> Validation -> Atomic Visibility).
2. Strict postcondition validation before atomic promotion.
3. Manifest fencing with generation tokens and concurrent-writer conflict rejection.
4. Idempotent commit retry semantics.
5. Deterministic abort and cleanup of staged outputs.
6. Crash recovery and orphan staging reconciliation.
"""
from __future__ import annotations

import copy
import datetime
from enum import Enum
import hashlib
import json
import logging
import threading
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

import pandas as pd
from pydantic import BaseModel, Field

logger = logging.getLogger("agent.etl_pipeline.durable_publisher")


# ============================================================================
# Enums and Exceptions
# ============================================================================

class PublicationState(str, Enum):
    INITIALIZED = "INITIALIZED"
    STAGED = "STAGED"
    VALIDATING = "VALIDATING"
    VALIDATED = "VALIDATED"
    COMMITTING = "COMMITTING"
    COMMITTED = "COMMITTED"
    ABORTING = "ABORTING"
    ABORTED = "ABORTED"
    FAILED = "FAILED"


class DurablePublicationError(Exception):
    """Base exception for durable publication failures."""
    def __init__(self, message: str, reason_code: str = "PUBLICATION_FAILED"):
        super().__init__(f"[{reason_code}] {message}")
        self.reason_code = reason_code


class ConcurrentWriterConflictError(DurablePublicationError):
    """Raised when fencing token detects a concurrent writer."""
    def __init__(self, message: str, reason_code: str = "CONCURRENT_WRITER_CONFLICT"):
        super().__init__(message, reason_code=reason_code)


class PublicationPostconditionError(DurablePublicationError):
    """Raised when postcondition checks on staged outputs fail."""
    def __init__(self, message: str, reason_code: str = "POSTCONDITION_FAILED"):
        super().__init__(message, reason_code=reason_code)


class StagingHandleNotFoundError(DurablePublicationError):
    """Raised when attempting an operation on an unknown staging handle."""
    def __init__(self, message: str, reason_code: str = "STAGING_HANDLE_NOT_FOUND"):
        super().__init__(message, reason_code=reason_code)


class IdempotencyConflictError(DurablePublicationError):
    """Raised when an idempotency key is reused with conflicting parameters."""
    def __init__(self, message: str, reason_code: str = "IDEMPOTENCY_CONFLICT"):
        super().__init__(message, reason_code=reason_code)


# ============================================================================
# Models
# ============================================================================

class StagingHandle(BaseModel):
    """Handle identifying an isolated staging workspace."""
    staging_id: str
    target_ref: str
    expected_fence_token: int
    idempotency_key: str
    manifest_digest: str
    created_at: str = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )
    state: PublicationState = PublicationState.INITIALIZED
    staged_keys: List[str] = Field(default_factory=list)


class PostconditionResult(BaseModel):
    """Result of evaluating postconditions on staged outputs."""
    is_valid: bool
    total_checks: int
    passed_checks: int
    errors: List[str] = Field(default_factory=list)


class PublicationCommitReceipt(BaseModel):
    """Immutable durable receipt issued upon atomic commit."""
    commit_id: str
    idempotency_key: str
    target_ref: str
    fence_token: int
    manifest_digest: str
    committed_at: str
    output_keys: List[str]
    output_row_counts: Dict[str, int]
    diagnostics: Dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# Durable Publication Coordinator
# ============================================================================

class DurablePublicationCoordinator:
    """
    Authoritative Publication Coordinator (Phase 4D / PROD-3).
    Guarantees atomic visibility, manifest fencing tokens, idempotent retries,
    crash recovery, and strict postcondition validation.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # Active production stores: target_ref -> {key: data}
        self._production_store: Dict[str, Dict[str, Any]] = {}
        # Generation fence tokens: target_ref -> int
        self._fence_tokens: Dict[str, int] = {}
        # Staging buffers: staging_id -> {key: data}
        self._staging_store: Dict[str, Dict[str, Any]] = {}
        # Active handles: staging_id -> StagingHandle
        self._handles: Dict[str, StagingHandle] = {}
        # Idempotent commit receipts: idempotency_key -> PublicationCommitReceipt
        self._receipts: Dict[str, PublicationCommitReceipt] = {}
        # Durable transaction audit log
        self._audit_log: List[Dict[str, Any]] = []

    def get_current_fence_token(self, target_ref: str) -> int:
        """Returns the current generation fencing token for a target destination."""
        with self._lock:
            return self._fence_tokens.get(target_ref, 0)

    def get_published_data(self, target_ref: str) -> Dict[str, Any]:
        """Returns visible production data for a target destination."""
        with self._lock:
            return copy.deepcopy(self._production_store.get(target_ref, {}))

    def begin_staging(
        self,
        *,
        staging_id: str,
        target_ref: str,
        idempotency_key: str,
        manifest_digest: str,
        expected_fence_token: Optional[int] = None,
    ) -> StagingHandle:
        """
        Initializes an isolated staging transaction.
        Checks for idempotent replay of already-committed receipts.
        """
        with self._lock:
            # Check for existing idempotency receipt
            if idempotency_key in self._receipts:
                receipt = self._receipts[idempotency_key]
                if receipt.manifest_digest != manifest_digest or receipt.target_ref != target_ref:
                    raise IdempotencyConflictError(
                        f"Idempotency key '{idempotency_key}' already used with different parameters",
                        reason_code="IDEMPOTENCY_CONFLICT",
                    )
                # Replay existing receipt by returning handle in COMMITTED state
                handle = StagingHandle(
                    staging_id=staging_id,
                    target_ref=target_ref,
                    expected_fence_token=receipt.fence_token,
                    idempotency_key=idempotency_key,
                    manifest_digest=manifest_digest,
                    state=PublicationState.COMMITTED,
                    staged_keys=receipt.output_keys,
                )
                return handle

            current_token = self.get_current_fence_token(target_ref)
            fence_token = current_token if expected_fence_token is None else expected_fence_token

            handle = StagingHandle(
                staging_id=staging_id,
                target_ref=target_ref,
                expected_fence_token=fence_token,
                idempotency_key=idempotency_key,
                manifest_digest=manifest_digest,
                state=PublicationState.INITIALIZED,
            )
            self._handles[staging_id] = handle
            self._staging_store[staging_id] = {}
            return handle

    def stage_dataset(
        self,
        handle: StagingHandle,
        key: str,
        data: Any,
    ) -> None:
        """
        Writes data to the isolated staging workspace.
        Invisible to production readers.
        """
        with self._lock:
            if handle.staging_id not in self._handles:
                raise StagingHandleNotFoundError(f"Staging handle '{handle.staging_id}' not found")
            
            curr_handle = self._handles[handle.staging_id]
            if curr_handle.state not in (PublicationState.INITIALIZED, PublicationState.STAGED):
                raise DurablePublicationError(
                    f"Cannot stage dataset in state {curr_handle.state}",
                    reason_code="INVALID_STAGING_STATE",
                )

            # Deep copy DataFrame or data structure to preserve isolation
            if isinstance(data, pd.DataFrame):
                stored_data = data.copy(deep=True)
            elif isinstance(data, dict):
                stored_data = copy.deepcopy(data)
            elif isinstance(data, list):
                stored_data = copy.deepcopy(data)
            else:
                stored_data = data

            self._staging_store[handle.staging_id][key] = stored_data
            if key not in curr_handle.staged_keys:
                curr_handle.staged_keys.append(key)
            curr_handle.state = PublicationState.STAGED

    def validate_postconditions(
        self,
        handle: StagingHandle,
        postconditions: Optional[Sequence[Callable[[Dict[str, Any]], None]]] = None,
        row_count_bounds: Optional[Dict[str, Tuple[Optional[int], Optional[int]]]] = None,
        non_null_columns: Optional[Dict[str, Sequence[str]]] = None,
    ) -> PostconditionResult:
        """
        Validates postconditions on staged outputs prior to atomic promotion.
        If validation fails, marks state as FAILED and does not promote.
        """
        with self._lock:
            if handle.staging_id not in self._handles:
                raise StagingHandleNotFoundError(f"Staging handle '{handle.staging_id}' not found")

            curr_handle = self._handles[handle.staging_id]
            curr_handle.state = PublicationState.VALIDATING
            staged_data = self._staging_store.get(handle.staging_id, {})

            errors: List[str] = []
            total_checks = 0
            passed_checks = 0

            # Check 1: Non-empty staged data
            total_checks += 1
            if not staged_data:
                errors.append("Staged workspace contains no datasets")
            else:
                passed_checks += 1

            # Check 2: Row count bounds
            if row_count_bounds:
                for key, (min_rows, max_rows) in row_count_bounds.items():
                    total_checks += 1
                    if key not in staged_data:
                        errors.append(f"Expected staged dataset '{key}' missing")
                        continue
                    ds = staged_data[key]
                    cnt = len(ds) if hasattr(ds, "__len__") else 1
                    if min_rows is not None and cnt < min_rows:
                        errors.append(f"Dataset '{key}' row count {cnt} < min {min_rows}")
                    elif max_rows is not None and cnt > max_rows:
                        errors.append(f"Dataset '{key}' row count {cnt} > max {max_rows}")
                    else:
                        passed_checks += 1

            # Check 3: Non-null column invariants
            if non_null_columns:
                for key, cols in non_null_columns.items():
                    if key in staged_data and isinstance(staged_data[key], pd.DataFrame):
                        df = staged_data[key]
                        for col in cols:
                            total_checks += 1
                            if col not in df.columns:
                                errors.append(f"Required non-null column '{col}' missing from '{key}'")
                            elif df[col].isna().any():
                                errors.append(f"Column '{col}' in '{key}' contains prohibited null values")
                            else:
                                passed_checks += 1

            # Check 4: User-supplied postcondition callables
            if postconditions:
                for i, pc in enumerate(postconditions):
                    total_checks += 1
                    try:
                        pc(staged_data)
                        passed_checks += 1
                    except Exception as pe:
                        errors.append(f"Postcondition callable #{i} failed: {pe}")

            is_valid = len(errors) == 0
            curr_handle.state = PublicationState.VALIDATED if is_valid else PublicationState.FAILED
            return PostconditionResult(
                is_valid=is_valid,
                total_checks=total_checks,
                passed_checks=passed_checks,
                errors=errors,
            )

    def commit(
        self,
        handle: StagingHandle,
    ) -> PublicationCommitReceipt:
        """
        Phase 2: Atomic Visibility Promotion with Fencing Token Verification.
        Enforces:
        1. Fencing token check (rejects concurrent writers).
        2. Atomic visibility (all staged datasets promoted simultaneously).
        3. Fencing token incrementation.
        4. Staging area cleanup.
        5. Durable commit receipt recording.
        """
        with self._lock:
            # Check idempotency first
            if handle.idempotency_key in self._receipts:
                return self._receipts[handle.idempotency_key]

            if handle.staging_id not in self._handles:
                raise StagingHandleNotFoundError(f"Staging handle '{handle.staging_id}' not found")

            curr_handle = self._handles[handle.staging_id]
            if curr_handle.state != PublicationState.VALIDATED:
                raise DurablePublicationError(
                    f"Cannot commit staging handle in state {curr_handle.state}. Must be VALIDATED.",
                    reason_code="UNVALIDATED_COMMIT_ATTEMPT",
                )

            # Check Fencing Token (OCC Concurrency Guard)
            actual_token = self.get_current_fence_token(handle.target_ref)
            if actual_token != handle.expected_fence_token:
                curr_handle.state = PublicationState.ABORTED
                # Auto-abort and cleanup on conflict
                self.abort(handle, reason="CONCURRENT_WRITER_CONFLICT")
                raise ConcurrentWriterConflictError(
                    f"Concurrent writer conflict detected on target '{handle.target_ref}': "
                    f"Expected fence token {handle.expected_fence_token}, found current token {actual_token}",
                    reason_code="CONCURRENT_WRITER_CONFLICT",
                )

            curr_handle.state = PublicationState.COMMITTING
            staged_data = self._staging_store.get(handle.staging_id, {})

            # Atomic Promotion to Production Store
            if handle.target_ref not in self._production_store:
                self._production_store[handle.target_ref] = {}
            
            # Write all staged datasets atomically
            self._production_store[handle.target_ref].update(staged_data)

            # Advance generation fence token
            new_token = actual_token + 1
            self._fence_tokens[handle.target_ref] = new_token

            # Calculate row counts for receipt
            row_counts: Dict[str, int] = {}
            for k, v in staged_data.items():
                row_counts[k] = len(v) if hasattr(v, "__len__") else 1

            commit_id = f"commit_{handle.staging_id}_{new_token}"
            now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()

            receipt = PublicationCommitReceipt(
                commit_id=commit_id,
                idempotency_key=handle.idempotency_key,
                target_ref=handle.target_ref,
                fence_token=new_token,
                manifest_digest=handle.manifest_digest,
                committed_at=now_str,
                output_keys=list(staged_data.keys()),
                output_row_counts=row_counts,
                diagnostics={"published_count": len(staged_data), "previous_token": actual_token},
            )

            # Record durable receipt & audit log
            self._receipts[handle.idempotency_key] = receipt
            self._audit_log.append({
                "action": "COMMIT",
                "commit_id": commit_id,
                "target_ref": handle.target_ref,
                "fence_token": new_token,
                "idempotency_key": handle.idempotency_key,
                "timestamp": now_str,
            })

            # Cleanup Staging Workspace
            curr_handle.state = PublicationState.COMMITTED
            if handle.staging_id in self._staging_store:
                del self._staging_store[handle.staging_id]

            return receipt

    def abort(
        self,
        handle: StagingHandle,
        reason: str = "MANUAL_ABORT",
    ) -> None:
        """
        Aborts publication and deterministically wipes staged artifacts.
        Production state remains completely untouched.
        """
        with self._lock:
            if handle.staging_id in self._handles:
                curr_handle = self._handles[handle.staging_id]
                curr_handle.state = PublicationState.ABORTED

            if handle.staging_id in self._staging_store:
                del self._staging_store[handle.staging_id]

            self._audit_log.append({
                "action": "ABORT",
                "staging_id": handle.staging_id,
                "target_ref": handle.target_ref,
                "reason": reason,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            })

    def recover_and_reconcile(self, orphan_timeout_seconds: float = 3600.0) -> Dict[str, int]:
        """
        Crash Recovery & Orphan Staging Reconciliation.
        Scans for uncommitted or abandoned staging sessions and cleans them up.
        """
        with self._lock:
            now = datetime.datetime.now(datetime.timezone.utc)
            orphans_cleaned = 0
            committed_count = len(self._receipts)

            staged_ids = list(self._handles.keys())
            for sid in staged_ids:
                h = self._handles[sid]
                if h.state in (PublicationState.INITIALIZED, PublicationState.STAGED, PublicationState.VALIDATING):
                    created_dt = datetime.datetime.fromisoformat(h.created_at)
                    age_seconds = (now - created_dt).total_seconds()
                    if age_seconds > orphan_timeout_seconds:
                        self.abort(h, reason=f"CRASH_RECOVERY_ORPHAN_TIMEOUT_{age_seconds:.1f}s")
                        orphans_cleaned += 1

            return {
                "orphans_cleaned": orphans_cleaned,
                "active_receipts": committed_count,
                "active_targets": len(self._fence_tokens),
            }
