"""
agent/etl_pipeline/cross_engine/capability_models.py

Authoritative capability models, decision types, and failure taxonomy
for cross-engine ETL compilation and semantic parity governance.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Tuple

from pydantic import BaseModel, Field

from agent.etl_pipeline.transformation_ir import ActionFamily


class CapabilityStatus(str, Enum):
    """
    Exhaustive lifecycle status of an engine capability.
    Default for all unregistered capabilities is NOT_TESTED.
    """
    VERIFIED = "verified"                    # Fully certified and verified by parity tests
    PARTIAL = "partial"                      # Partially supported; blocked by default without waiver
    NOT_TESTED = "not_tested"                # Support unestablished; strictly blocked
    UNSUPPORTED = "unsupported"              # Known unachievable or prohibited semantics; strictly blocked
    ORCHESTRATION_ONLY = "orchestration_only"  # Supports external activity dispatch, no transform execution


class EngineCapabilityKey(BaseModel):
    """
    Authoritative 11-field preflight and certification capability identity.
    Binds the exact semantic context required for execution eligibility.
    """
    family: ActionFamily
    expression_subtype: str
    policy_profile_hash: str
    ir_version: str = "v1.0"
    catalogue_version: str = "v1.0"
    compiler_name: str
    compiler_version: str
    engine_name: str
    engine_version: str
    dialect: Optional[str] = None
    runtime_profile_hash: str

    def to_compact_key(self) -> str:
        """Deterministic string key representation for indexing and logging."""
        dialect_part = f":{self.dialect}" if self.dialect else ""
        return (
            f"{self.compiler_name}:{self.compiler_version}/"
            f"{self.engine_name}:{self.engine_version}{dialect_part}/"
            f"{self.family.value}:{self.expression_subtype}/"
            f"ir={self.ir_version}:pol={self.policy_profile_hash[:8]}:rt={self.runtime_profile_hash[:8]}"
        )


class CapabilityDecision(BaseModel):
    """
    Authoritative capability determination produced by preflight evaluation.
    Execution eligibility is explicitly computed and validated, never inferred informally.
    """
    key: EngineCapabilityKey
    status: CapabilityStatus
    reason_code: str
    limitations: Tuple[str, ...] = Field(default_factory=tuple)
    verified_test_ids: Tuple[str, ...] = Field(default_factory=tuple)
    certification_commit: Optional[str] = None
    waiver_required: bool = False
    execution_eligible: bool = False

    @classmethod
    def create(
        cls,
        key: EngineCapabilityKey,
        status: CapabilityStatus,
        reason_code: str,
        limitations: Tuple[str, ...] = (),
        verified_test_ids: Tuple[str, ...] = (),
        certification_commit: Optional[str] = None,
        waiver_granted: bool = False,
    ) -> CapabilityDecision:
        """
        Factory enforcing strict execution eligibility rules:
        - VERIFIED: eligible for execution.
        - PARTIAL: eligible ONLY IF an explicit waiver is granted; otherwise blocked.
        - NOT_TESTED, UNSUPPORTED, ORCHESTRATION_ONLY: strictly NOT execution eligible.
        """
        waiver_req = (status == CapabilityStatus.PARTIAL)
        eligible = False
        if status == CapabilityStatus.VERIFIED:
            eligible = True
        elif status == CapabilityStatus.PARTIAL and waiver_granted:
            eligible = True

        return cls(
            key=key,
            status=status,
            reason_code=reason_code,
            limitations=limitations,
            verified_test_ids=verified_test_ids,
            certification_commit=certification_commit,
            waiver_required=waiver_req,
            execution_eligible=eligible,
        )


class FailureCategory(str, Enum):
    """Canonical taxonomy of failure modes across all ETL engines."""
    SCHEMA_BINDING = "schema_binding"
    NULL_POLICY = "null_policy"
    PARSE_FAILURE = "parse_failure"
    OVERFLOW = "overflow"
    DIVIDE_BY_ZERO = "divide_by_zero"
    DUPLICATE_KEY = "duplicate_key"
    JOIN_CARDINALITY = "join_cardinality"
    EXPANSION_LIMIT = "expansion_limit"
    AUTHORIZATION = "authorization"
    UNSUPPORTED_CAPABILITY = "unsupported_capability"


class NormalizedFailure(BaseModel):
    """
    Standardized cross-engine failure envelope.
    Engines must agree on failure categories and typed reason codes,
    not raw engine-specific exception text.
    """
    category: FailureCategory
    reason_code: str
    step_id: str
    family: ActionFamily
    retryable: bool = False
    details: Dict[str, Any] = Field(default_factory=dict)


class ExpectedEngineDeviation(BaseModel):
    """
    Versioned, approved, and expiring deviation registry entry.
    Documented differences (such as floating-point precision differences)
    must be declared explicitly and never silently weaken security contracts.
    """
    deviation_id: str
    family: ActionFamily
    expression_subtype: str
    engine_name: str
    engine_version_range: str
    runtime_profile_hash: str
    affected_field: str
    comparator_policy: str
    rationale: str
    approved_by: str
    expires_at: Optional[datetime] = None
    test_ids: Tuple[str, ...] = Field(default_factory=tuple)

    def is_active(self, current_time: Optional[datetime] = None) -> bool:
        """Verify that the deviation has not expired."""
        if self.expires_at is None:
            return True
        now = current_time or datetime.now(self.expires_at.tzinfo or datetime.now().astimezone().tzinfo)
        return now <= self.expires_at
