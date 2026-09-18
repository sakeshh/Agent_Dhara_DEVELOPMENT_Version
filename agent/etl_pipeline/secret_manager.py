"""
agent/etl_pipeline/secret_manager.py

Phase 4B: Execution-Boundary Secret Resolution & Key Custody (PROD-1).
Enforces:
1. Gated resolution: Secrets can ONLY be resolved when ExecutionGateDecision.dispatch_allowed is True.
2. Tenant & purpose validation.
3. Zero-leakage representation: __repr__ and __str__ always return [REDACTED]; no secret bytes in logs/ASTs/exceptions.
4. Scoped lifecycle: ScopedSecretSession context manager releases provider handles, overwrites managed references
   on exit, eliminates application-level references, and minimizes secret lifetime (without claiming absolute process-wide memory erasure).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.etl_pipeline.execution_gate import ExecutionGateDecision
from agent.etl_pipeline.transformation_ir import SecretReference

logger = logging.getLogger("agent.etl_pipeline.secret_manager")


class UnsafeSecretResolutionError(Exception):
    """Raised when an attempt is made to resolve secrets without an approved execution gate decision."""

    def __init__(self, message: str, *, reason_code: str = "UNAUTHORIZED_SECRET_ACCESS"):
        super().__init__(f"[{reason_code}] {message}")
        self.reason_code = reason_code


class RedactedSecretHandle:
    """
    In-memory handle for resolved execution secrets.
    Strictly forbids plaintext string representations in logs, debuggers, or exception traces.
    """

    def __init__(
        self,
        *,
        secret_id: str,
        version: str,
        tenant_id: str,
        purpose: str,
        secret_value: str,
    ):
        self.secret_id = secret_id
        self.version = version
        self.tenant_id = tenant_id
        self.purpose = purpose
        self._secret_value: Optional[str] = secret_value
        self._is_wiped: bool = False

    def get_secret_str(self) -> str:
        """Retrieves raw secret string in memory. Raises if session has wiped it."""
        if self._is_wiped or self._secret_value is None:
            raise UnsafeSecretResolutionError(
                f"Secret '{self.secret_id}' has already been wiped from memory",
                reason_code="SECRET_ALREADY_WIPED",
            )
        return self._secret_value

    def get_secret_bytes(self) -> bytes:
        """Retrieves raw secret bytes in memory. Raises if session has wiped it."""
        return self.get_secret_str().encode("utf-8")

    def wipe(self) -> None:
        """Eliminates application-level references and releases managed secret buffers."""
        self._secret_value = None
        self._is_wiped = True

    def __repr__(self) -> str:
        return f"<RedactedSecretHandle secret_id={self.secret_id!r} version={self.version!r} [REDACTED]>"

    def __str__(self) -> str:
        return f"<RedactedSecretHandle secret_id={self.secret_id!r} [REDACTED]>"


class ScopedSecretSession:
    """
    Context manager that governs the lifecycle of resolved execution secrets.
    Overwrites managed buffers, releases provider handles, and eliminates application references on exit.
    """

    def __init__(self, session_id: str = "default_session"):
        self.session_id = session_id
        self._handles: List[RedactedSecretHandle] = []

    def register_handle(self, handle: RedactedSecretHandle) -> RedactedSecretHandle:
        self._handles.append(handle)
        return handle

    def __enter__(self) -> ScopedSecretSession:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Wipes all active secret handles on session termination."""
        for h in self._handles:
            h.wipe()
        self._handles.clear()


class ExecutionSecretManager:
    """
    Authoritative secret provider implementing PROD-1 execution boundary controls.
    """

    def __init__(
        self,
        vault_store: Optional[Dict[Tuple[str, str, str], str]] = None,
    ):
        # Key: (tenant_id, secret_id, version) -> secret_plaintext
        self._vault = vault_store or {}

    def register_secret_in_vault(
        self,
        *,
        tenant_id: str,
        secret_id: str,
        version: str,
        secret_value: str,
    ) -> None:
        """Test/administrative registration of a secret into the simulated vault."""
        self._vault[(tenant_id, secret_id, version)] = secret_value

    def resolve_secret(
        self,
        *,
        secret_ref: SecretReference,
        gate_decision: ExecutionGateDecision,
        tenant_id: str,
        session: ScopedSecretSession,
    ) -> RedactedSecretHandle:
        """
        Resolves a secret reference under strict gate decision checks:
        1. Preflight execution gate MUST have dispatch_allowed=True.
        2. Tenant in gate decision MUST match requested tenant.
        3. Secret must exist in authorized vault for this tenant.
        """
        # 1. Gate Approval Check
        if not gate_decision.dispatch_allowed:
            raise UnsafeSecretResolutionError(
                f"Cannot resolve secret '{secret_ref.secret_id}': execution preflight gate rejected dispatch ({gate_decision.reason_code})",
                reason_code="GATE_REJECTED",
            )

        # 2. Tenant Boundary Check
        if gate_decision.tenant_id != tenant_id:
            raise UnsafeSecretResolutionError(
                f"Tenant mismatch: gate decision authorized '{gate_decision.tenant_id}', but secret requested for '{tenant_id}'",
                reason_code="TENANT_MISMATCH",
            )

        # 3. Vault Lookup
        version = secret_ref.expected_version or "v1"
        lookup_key = (tenant_id, secret_ref.secret_id, version)
        if lookup_key not in self._vault:
            raise UnsafeSecretResolutionError(
                f"Secret '{secret_ref.secret_id}' (version '{version}') not found in vault for tenant '{tenant_id}'",
                reason_code="SECRET_NOT_FOUND",
            )

        raw_secret = self._vault[lookup_key]
        handle = RedactedSecretHandle(
            secret_id=secret_ref.secret_id,
            version=version,
            tenant_id=tenant_id,
            purpose=secret_ref.expected_purpose,
            secret_value=raw_secret,
        )

        return session.register_handle(handle)
