"""
agent/etl_pipeline/cross_engine/capability_registry.py

Authoritative registry of engine capabilities, preflight evaluation,
and versioned expected engine deviations.
"""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

from agent.etl_pipeline.cross_engine.capability_models import (
    CapabilityDecision,
    CapabilityStatus,
    EngineCapabilityKey,
    ExpectedEngineDeviation,
)


class EngineCapabilityRegistry:
    """
    Centralized capability registry enforcing default-to-blocked governance:
    Any capability not explicitly verified and registered defaults to NOT_TESTED
    and execution_eligible = False.
    """

    def __init__(self) -> None:
        self._decisions: Dict[str, CapabilityDecision] = {}
        self._deviations: Dict[str, ExpectedEngineDeviation] = {}

    def register_capability(
        self,
        key: EngineCapabilityKey,
        status: CapabilityStatus,
        reason_code: str,
        limitations: Tuple[str, ...] = (),
        verified_test_ids: Tuple[str, ...] = (),
        certification_commit: Optional[str] = None,
        waiver_granted: bool = False,
    ) -> CapabilityDecision:
        """Register an evaluated capability decision for a specific capability key."""
        decision = CapabilityDecision.create(
            key=key,
            status=status,
            reason_code=reason_code,
            limitations=limitations,
            verified_test_ids=verified_test_ids,
            certification_commit=certification_commit,
            waiver_granted=waiver_granted,
        )
        compact = key.to_compact_key()
        self._decisions[compact] = decision
        return decision

    def evaluate_capability(
        self,
        key: EngineCapabilityKey,
        waiver_granted: bool = False,
    ) -> CapabilityDecision:
        """
        Preflight capability lookup. If the key has not been explicitly registered,
        strictly returns NOT_TESTED with execution_eligible = False.
        """
        compact = key.to_compact_key()
        if compact in self._decisions:
            decision = self._decisions[compact]
            if waiver_granted and decision.status == CapabilityStatus.PARTIAL and not decision.execution_eligible:
                return CapabilityDecision.create(
                    key=key,
                    status=decision.status,
                    reason_code=decision.reason_code,
                    limitations=decision.limitations,
                    verified_test_ids=decision.verified_test_ids,
                    certification_commit=decision.certification_commit,
                    waiver_granted=True,
                )
            return decision

        # Check wildcard policy profile if registered for this (compiler, engine, family, subtype, runtime)
        wildcard_key = key.model_copy(update={"policy_profile_hash": "*"})
        wildcard_compact = wildcard_key.to_compact_key()
        if wildcard_compact in self._decisions:
            decision = self._decisions[wildcard_compact]
            if waiver_granted and decision.status == CapabilityStatus.PARTIAL and not decision.execution_eligible:
                return CapabilityDecision.create(
                    key=key,
                    status=decision.status,
                    reason_code=decision.reason_code,
                    limitations=decision.limitations,
                    verified_test_ids=decision.verified_test_ids,
                    certification_commit=decision.certification_commit,
                    waiver_granted=True,
                )
            return decision

        # Default-to-blocked invariant
        return CapabilityDecision.create(
            key=key,
            status=CapabilityStatus.NOT_TESTED,
            reason_code="CAPABILITY_NOT_REGISTERED",
            limitations=("Unestablished engine capability; compilation blocked by default.",),
        )


    def register_deviation(self, deviation: ExpectedEngineDeviation) -> None:
        """Register a versioned expected engine deviation."""
        self._deviations[deviation.deviation_id] = deviation

    def get_deviation(self, deviation_id: str) -> Optional[ExpectedEngineDeviation]:
        """Retrieve a registered deviation by ID."""
        return self._deviations.get(deviation_id)

    def find_active_deviations(
        self,
        key: EngineCapabilityKey,
        affected_field: Optional[str] = None,
        current_time: Optional[datetime] = None,
    ) -> List[ExpectedEngineDeviation]:
        """
        Find matching, currently non-expired deviations for the given capability key.
        Expired deviations are excluded and must fail CI.
        """
        active: List[ExpectedEngineDeviation] = []
        for dev in self._deviations.values():
            if dev.family == key.family and dev.expression_subtype == key.expression_subtype:
                if dev.engine_name == key.engine_name and dev.runtime_profile_hash == key.runtime_profile_hash:
                    if affected_field is None or dev.affected_field == affected_field:
                        if dev.is_active(current_time):
                            active.append(dev)
        return active


# Global registry instance
_GLOBAL_CAPABILITY_REGISTRY = EngineCapabilityRegistry()


def get_global_capability_registry() -> EngineCapabilityRegistry:
    """Obtain global singleton instance of the EngineCapabilityRegistry."""
    return _GLOBAL_CAPABILITY_REGISTRY
