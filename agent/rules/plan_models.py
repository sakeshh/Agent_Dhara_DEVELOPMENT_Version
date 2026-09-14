"""
Plan Models, Plan Steps, Effect Specifications, and Proposed DAG Models.

Implements the frozen v9.3 specification:
- Pure semantic DataEffect model with complete effect identity and parameter hashes.
- PlanStep and PlanStepDependency models with DAG graph builder and cycle detection.
- ProposedPlan container tracking READY_FOR_AUTHORIZATION status.
"""
from __future__ import annotations

import hashlib
from enum import Enum
from typing import Any, Dict, FrozenSet, List, Literal, Optional, Set, Tuple

from pydantic import BaseModel, Field, field_validator, model_validator

from agent.rules.canonical import (
    CANONICALIZATION_SCHEME,
    CANONICALIZATION_VERSION,
    canonical_json,
    compute_canonical_hash,
)


class PlannerInvariantError(RuntimeError):
    """Raised when dependency graph integrity or plan invariants are violated."""
    pass


class DataEffectAccess(str, Enum):
    READ = "read"
    WRITE = "write"
    APPEND = "append"
    CREATE = "create"
    DELETE = "delete"


class OrderingDirection(str, Enum):
    A_BEFORE_B = "a_before_b"
    B_BEFORE_A = "b_before_a"
    NONE = "none"


class CompatibilityOutcome(str, Enum):
    COMPATIBLE_PARALLEL = "compatible_parallel"
    ORDERED_COMPOSITION = "ordered_composition"
    IDEMPOTENT_DUPLICATE = "idempotent_duplicate"
    MUTUALLY_EXCLUSIVE = "mutually_exclusive"
    UNRESOLVED = "unresolved"


class DataEffect(BaseModel):
    """Concrete data operation effect produced by a transformation step or rule assertion."""
    access: DataEffectAccess
    resource_ref: str
    column_scope: FrozenSet[str] = frozenset()

    action: str
    effect_semantic_hash: str          # Hash of action + normalized parameters + target schema
    normalized_parameters_hash: str    # Canonical hash of effect parameters

    reads_state: Optional[Literal["pre_effect", "post_effect", "either"]] = None
    commutative: bool = False
    idempotent: bool = False
    is_explicit_recreate: bool = False

    @field_validator("column_scope", mode="before")
    @classmethod
    def ensure_frozenset(cls, v: Any) -> FrozenSet[str]:
        if isinstance(v, (list, set, tuple)):
            return frozenset(str(c) for c in v)
        if isinstance(v, frozenset):
            return v
        return frozenset()


class CompatibilityResult(BaseModel):
    """Outcome of evaluating effect compatibility with directional execution ordering."""
    outcome: CompatibilityOutcome
    ordering: OrderingDirection = OrderingDirection.NONE
    reason_code: str
    interacting_effects: Tuple[DataEffect, ...] = ()

    @model_validator(mode="after")
    def validate_ordering_invariant(self) -> "CompatibilityResult":
        if self.outcome == CompatibilityOutcome.ORDERED_COMPOSITION and self.ordering == OrderingDirection.NONE:
            raise PlannerInvariantError("Ordered composition requires an explicit ordering direction (A_BEFORE_B or B_BEFORE_A)")
        if self.outcome != CompatibilityOutcome.ORDERED_COMPOSITION and self.ordering != OrderingDirection.NONE:
            raise PlannerInvariantError(f"Ordering direction '{self.ordering}' is only valid for ordered composition (got {self.outcome})")
        return self


class StepAuthorizationStatus(str, Enum):
    AUTHORIZED = "authorized"
    PENDING = "pending"
    REJECTED = "rejected"


class PlanStep(BaseModel):
    """Individual atomic execution step in a proposed or authorized ETL plan."""
    step_id: str
    action: str
    action_version: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    source_rule_ids: Tuple[str, ...] = ()
    effects: Tuple[DataEffect, ...] = ()

    authorization_status: StepAuthorizationStatus = StepAuthorizationStatus.PENDING
    policy_decision_id: Optional[str] = None

    def compute_hash(self) -> str:
        """Computes deterministic hash of step content for authorization verification."""
        payload = {
            "step_id": self.step_id,
            "action": self.action,
            "action_version": self.action_version,
            "parameters": self.parameters,
            "source_rule_ids": sorted(self.source_rule_ids),
            "effects": [e.model_dump(mode="python") for e in self.effects],
        }
        return compute_canonical_hash(payload)


class PlanStepDependency(BaseModel):
    """Directed dependency edge requiring before_step_id to complete before after_step_id."""
    before_step_id: str
    after_step_id: str
    dependency_type: str = "effect_ordering"


class ProposedPlanStatus(str, Enum):
    DRAFT = "draft"
    ANALYSIS_COMPLETE = "analysis_complete"
    READY_FOR_AUTHORIZATION = "ready_for_authorization"
    REJECTED = "rejected"


class ProposedPlan(BaseModel):
    """Proposed DAG of transformation steps ready for review and authorization."""
    plan_id: str
    status: ProposedPlanStatus = ProposedPlanStatus.DRAFT
    steps: Tuple[PlanStep, ...] = ()
    dependencies: Tuple[PlanStepDependency, ...] = ()

    canonicalization_scheme: str = CANONICALIZATION_SCHEME
    canonicalization_version: str = CANONICALIZATION_VERSION

    def compute_hash(self) -> str:
        """Computes content hash over sorted steps and dependencies."""
        sorted_steps = [s.model_dump(mode="python") for s in sorted(self.steps, key=lambda s: s.step_id)]
        sorted_deps = [
            d.model_dump(mode="python")
            for d in sorted(self.dependencies, key=lambda d: (d.before_step_id, d.after_step_id, d.dependency_type))
        ]
        payload = {
            "plan_id": self.plan_id,
            "status": self.status.value,
            "steps": sorted_steps,
            "dependencies": sorted_deps,
            "scheme": self.canonicalization_scheme,
            "version": self.canonicalization_version,
        }
        return compute_canonical_hash(payload)

    def validate_dag_integrity(self) -> None:
        """Validates that all dependency references are known, no self dependencies, no cycles."""
        from agent.rules.conflict_resolver import build_dependency_graph, topological_sort_steps
        build_dependency_graph(self.steps, self.dependencies)
        topological_sort_steps(self.steps, self.dependencies)
