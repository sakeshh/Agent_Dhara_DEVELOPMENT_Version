"""
Deterministic Conflict Resolution Engine, 25-Pair Effect Matrix, and DAG Planner.

Implements the frozen v9.3 specification with Errata E4:
- Complete 25-pair base access matrix with startup validation.
- Semantic effect resolver with wildcard protection and semantic duplicate deduplication.
- Multi-effect compatibility aggregator with strict deterministic priority hierarchy.
- Single unified dependency graph builder with cycle and self-dependency detection.
- Deterministic Kahn topological sorting with semantic tie-breaking.
- 10-step conflict resolution pipeline prioritizing effect compatibility before authority.
"""
from __future__ import annotations

from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Set, Tuple

from agent.rules.plan_models import (
    CompatibilityOutcome,
    CompatibilityResult,
    DataEffect,
    DataEffectAccess,
    OrderingDirection,
    PlanStep,
    PlanStepDependency,
    PlannerInvariantError,
)
from agent.rules.rule_models import RuleOccurrence, RuleScope


class InvalidEffectCompatibilityMatrix(RuntimeError):
    """Raised when the base access compatibility matrix is incomplete or corrupted."""
    pass


# ==============================================================================
# 1. 25-Pair Base Access Compatibility Matrix
# ==============================================================================

_BASE_ACCESS_MATRIX: Dict[
    Tuple[DataEffectAccess, DataEffectAccess],
    Tuple[CompatibilityOutcome, OrderingDirection, str]
] = {
    # READ pairs
    (DataEffectAccess.READ, DataEffectAccess.READ): (CompatibilityOutcome.COMPATIBLE_PARALLEL, OrderingDirection.NONE, "read_read_parallel"),
    (DataEffectAccess.READ, DataEffectAccess.WRITE): (CompatibilityOutcome.UNRESOLVED, OrderingDirection.NONE, "read_write_timing_dependent"),
    (DataEffectAccess.READ, DataEffectAccess.APPEND): (CompatibilityOutcome.UNRESOLVED, OrderingDirection.NONE, "read_append_timing_dependent"),
    (DataEffectAccess.READ, DataEffectAccess.CREATE): (CompatibilityOutcome.ORDERED_COMPOSITION, OrderingDirection.B_BEFORE_A, "create_before_read"),
    (DataEffectAccess.READ, DataEffectAccess.DELETE): (CompatibilityOutcome.ORDERED_COMPOSITION, OrderingDirection.A_BEFORE_B, "read_before_delete"),

    # WRITE pairs
    (DataEffectAccess.WRITE, DataEffectAccess.READ): (CompatibilityOutcome.UNRESOLVED, OrderingDirection.NONE, "write_read_timing_dependent"),
    (DataEffectAccess.WRITE, DataEffectAccess.WRITE): (CompatibilityOutcome.MUTUALLY_EXCLUSIVE, OrderingDirection.NONE, "conflicting_concurrent_writes"),
    (DataEffectAccess.WRITE, DataEffectAccess.APPEND): (CompatibilityOutcome.ORDERED_COMPOSITION, OrderingDirection.A_BEFORE_B, "write_before_append"),
    (DataEffectAccess.WRITE, DataEffectAccess.CREATE): (CompatibilityOutcome.MUTUALLY_EXCLUSIVE, OrderingDirection.NONE, "create_write_collision"),
    (DataEffectAccess.WRITE, DataEffectAccess.DELETE): (CompatibilityOutcome.MUTUALLY_EXCLUSIVE, OrderingDirection.NONE, "write_delete_collision"),

    # APPEND pairs
    (DataEffectAccess.APPEND, DataEffectAccess.READ): (CompatibilityOutcome.UNRESOLVED, OrderingDirection.NONE, "append_read_timing_dependent"),
    (DataEffectAccess.APPEND, DataEffectAccess.WRITE): (CompatibilityOutcome.ORDERED_COMPOSITION, OrderingDirection.B_BEFORE_A, "write_before_append"),
    (DataEffectAccess.APPEND, DataEffectAccess.APPEND): (CompatibilityOutcome.UNRESOLVED, OrderingDirection.NONE, "append_commutativity_dependent"),
    (DataEffectAccess.APPEND, DataEffectAccess.CREATE): (CompatibilityOutcome.ORDERED_COMPOSITION, OrderingDirection.B_BEFORE_A, "create_before_append"),
    (DataEffectAccess.APPEND, DataEffectAccess.DELETE): (CompatibilityOutcome.MUTUALLY_EXCLUSIVE, OrderingDirection.NONE, "append_delete_collision"),

    # CREATE pairs
    (DataEffectAccess.CREATE, DataEffectAccess.READ): (CompatibilityOutcome.ORDERED_COMPOSITION, OrderingDirection.A_BEFORE_B, "create_before_read"),
    (DataEffectAccess.CREATE, DataEffectAccess.WRITE): (CompatibilityOutcome.MUTUALLY_EXCLUSIVE, OrderingDirection.NONE, "create_write_collision"),
    (DataEffectAccess.CREATE, DataEffectAccess.APPEND): (CompatibilityOutcome.ORDERED_COMPOSITION, OrderingDirection.A_BEFORE_B, "create_before_append"),
    (DataEffectAccess.CREATE, DataEffectAccess.CREATE): (CompatibilityOutcome.MUTUALLY_EXCLUSIVE, OrderingDirection.NONE, "duplicate_target_creation"),
    (DataEffectAccess.CREATE, DataEffectAccess.DELETE): (CompatibilityOutcome.UNRESOLVED, OrderingDirection.NONE, "create_delete_intent_dependent"),

    # DELETE pairs
    (DataEffectAccess.DELETE, DataEffectAccess.READ): (CompatibilityOutcome.ORDERED_COMPOSITION, OrderingDirection.B_BEFORE_A, "read_before_delete"),
    (DataEffectAccess.DELETE, DataEffectAccess.WRITE): (CompatibilityOutcome.MUTUALLY_EXCLUSIVE, OrderingDirection.NONE, "write_delete_collision"),
    (DataEffectAccess.DELETE, DataEffectAccess.APPEND): (CompatibilityOutcome.MUTUALLY_EXCLUSIVE, OrderingDirection.NONE, "append_delete_collision"),
    (DataEffectAccess.DELETE, DataEffectAccess.CREATE): (CompatibilityOutcome.UNRESOLVED, OrderingDirection.NONE, "delete_create_intent_dependent"),
    (DataEffectAccess.DELETE, DataEffectAccess.DELETE): (CompatibilityOutcome.MUTUALLY_EXCLUSIVE, OrderingDirection.NONE, "redundant_delete_conflict"),
}


def validate_effect_matrix_completeness() -> None:
    """Validates that all 25 ordered access pairs are defined and conform to ordering invariants."""
    expected_pairs = {
        (left, right)
        for left in DataEffectAccess
        for right in DataEffectAccess
    }
    missing_pairs = expected_pairs - set(_BASE_ACCESS_MATRIX.keys())
    if missing_pairs:
        raise InvalidEffectCompatibilityMatrix(f"Incomplete base access matrix. Missing pairs: {sorted(missing_pairs)}")

    if len(_BASE_ACCESS_MATRIX) != 25:
        raise InvalidEffectCompatibilityMatrix(f"Base access matrix must contain exactly 25 entries, found {len(_BASE_ACCESS_MATRIX)}")

    for pair, (outcome, ordering, reason) in _BASE_ACCESS_MATRIX.items():
        if outcome == CompatibilityOutcome.ORDERED_COMPOSITION and ordering == OrderingDirection.NONE:
            raise InvalidEffectCompatibilityMatrix(f"Matrix entry {pair} has ORDERED_COMPOSITION but ordering is NONE")
        if outcome != CompatibilityOutcome.ORDERED_COMPOSITION and ordering != OrderingDirection.NONE:
            raise InvalidEffectCompatibilityMatrix(f"Matrix entry {pair} has outcome {outcome} but ordering is {ordering}")
        if not reason:
            raise InvalidEffectCompatibilityMatrix(f"Matrix entry {pair} has empty reason code")


# Execute startup matrix check
validate_effect_matrix_completeness()


# ==============================================================================
# 2. Semantic Effect Compatibility Resolver
# ==============================================================================

def _is_unresolved_resource(resource: str) -> bool:
    return resource in {"*", "__any__", "__unknown__", ""}


def _effects_are_semantically_identical(ea: DataEffect, eb: DataEffect) -> bool:
    """Idempotent does not mean duplicate. Deduplication requires complete semantic identity."""
    return (
        ea.access == eb.access
        and ea.resource_ref == eb.resource_ref
        and ea.column_scope == eb.column_scope
        and ea.action == eb.action
        and ea.effect_semantic_hash == eb.effect_semantic_hash
        and ea.normalized_parameters_hash == eb.normalized_parameters_hash
    )


def _resolve_context_dependent_pair(ea: DataEffect, eb: DataEffect) -> CompatibilityResult:
    """Refines UNRESOLVED access pairs using context attributes."""
    # 1. READ / WRITE timing semantics
    if ea.access == DataEffectAccess.READ and eb.access == DataEffectAccess.WRITE:
        if ea.reads_state == "pre_effect":
            return CompatibilityResult(outcome=CompatibilityOutcome.ORDERED_COMPOSITION, ordering=OrderingDirection.A_BEFORE_B, reason_code="read_pre_write_state", interacting_effects=(ea, eb))
        elif ea.reads_state == "post_effect":
            return CompatibilityResult(outcome=CompatibilityOutcome.ORDERED_COMPOSITION, ordering=OrderingDirection.B_BEFORE_A, reason_code="read_post_write_state", interacting_effects=(ea, eb))
        return CompatibilityResult(outcome=CompatibilityOutcome.UNRESOLVED, reason_code="unspecified_read_timing", interacting_effects=(ea, eb))

    if ea.access == DataEffectAccess.WRITE and eb.access == DataEffectAccess.READ:
        if eb.reads_state == "pre_effect":
            return CompatibilityResult(outcome=CompatibilityOutcome.ORDERED_COMPOSITION, ordering=OrderingDirection.B_BEFORE_A, reason_code="read_pre_write_state", interacting_effects=(ea, eb))
        elif eb.reads_state == "post_effect":
            return CompatibilityResult(outcome=CompatibilityOutcome.ORDERED_COMPOSITION, ordering=OrderingDirection.A_BEFORE_B, reason_code="read_post_write_state", interacting_effects=(ea, eb))
        return CompatibilityResult(outcome=CompatibilityOutcome.UNRESOLVED, reason_code="unspecified_read_timing", interacting_effects=(ea, eb))

    # 2. APPEND / APPEND commutativity
    if ea.access == DataEffectAccess.APPEND and eb.access == DataEffectAccess.APPEND:
        if ea.commutative and eb.commutative:
            return CompatibilityResult(outcome=CompatibilityOutcome.COMPATIBLE_PARALLEL, reason_code="commutative_appends", interacting_effects=(ea, eb))
        return CompatibilityResult(outcome=CompatibilityOutcome.UNRESOLVED, reason_code="non_commutative_appends", interacting_effects=(ea, eb))

    # 3. DELETE / CREATE explicit recreate
    if {ea.access, eb.access} == {DataEffectAccess.DELETE, DataEffectAccess.CREATE}:
        if ea.is_explicit_recreate or eb.is_explicit_recreate:
            first = OrderingDirection.A_BEFORE_B if ea.access == DataEffectAccess.DELETE else OrderingDirection.B_BEFORE_A
            return CompatibilityResult(outcome=CompatibilityOutcome.ORDERED_COMPOSITION, ordering=first, reason_code="explicit_recreate", interacting_effects=(ea, eb))
        return CompatibilityResult(outcome=CompatibilityOutcome.UNRESOLVED, reason_code="uncoordinated_delete_create", interacting_effects=(ea, eb))

    return CompatibilityResult(outcome=CompatibilityOutcome.UNRESOLVED, reason_code="unresolved_effect_pair", interacting_effects=(ea, eb))


def resolve_semantic_effect_compatibility(ea: DataEffect, eb: DataEffect) -> CompatibilityResult:
    """Evaluates compatibility between two concrete data effects."""
    # 1. Wildcard resource check
    if _is_unresolved_resource(ea.resource_ref) or _is_unresolved_resource(eb.resource_ref):
        return CompatibilityResult(
            outcome=CompatibilityOutcome.UNRESOLVED,
            reason_code="unresolved_resource_wildcard",
            interacting_effects=(ea, eb),
        )

    # 2. Disjoint resources
    if ea.resource_ref != eb.resource_ref:
        return CompatibilityResult(
            outcome=CompatibilityOutcome.COMPATIBLE_PARALLEL,
            reason_code="disjoint_resources",
            interacting_effects=(ea, eb),
        )

    # 3. Disjoint column scopes (if non-wildcard)
    if ea.column_scope and eb.column_scope:
        if "*" not in ea.column_scope and "*" not in eb.column_scope:
            if ea.column_scope.isdisjoint(eb.column_scope):
                return CompatibilityResult(
                    outcome=CompatibilityOutcome.COMPATIBLE_PARALLEL,
                    reason_code="disjoint_columns",
                    interacting_effects=(ea, eb),
                )

    # 4. Semantic duplicate / idempotency refinement (runs BEFORE restrictive base outcomes)
    if _effects_are_semantically_identical(ea, eb) and ea.idempotent and eb.idempotent:
        return CompatibilityResult(
            outcome=CompatibilityOutcome.IDEMPOTENT_DUPLICATE,
            ordering=OrderingDirection.NONE,
            reason_code="identical_idempotent_effects",
            interacting_effects=(ea, eb),
        )

    # 5. Base access matrix lookup
    base_outcome, base_order, base_reason = _BASE_ACCESS_MATRIX[(ea.access, eb.access)]
    if base_outcome != CompatibilityOutcome.UNRESOLVED:
        return CompatibilityResult(
            outcome=base_outcome,
            ordering=base_order,
            reason_code=base_reason,
            interacting_effects=(ea, eb),
        )

    # 6. Context-dependent pair refinement
    return _resolve_context_dependent_pair(ea, eb)


def aggregate_compatibility_results(results: List[CompatibilityResult]) -> CompatibilityResult:
    """
    Aggregates multiple effect interaction outcomes according to strict precedence:
    MUTUALLY_EXCLUSIVE > UNRESOLVED > Contradictory Directions > ORDERED_COMPOSITION > IDEMPOTENT_DUPLICATE > COMPATIBLE_PARALLEL
    """
    if not results:
        return CompatibilityResult(
            outcome=CompatibilityOutcome.COMPATIBLE_PARALLEL,
            ordering=OrderingDirection.NONE,
            reason_code="empty_effect_list",
        )

    # Priority 1: Any mutual exclusion dominates
    if any(r.outcome == CompatibilityOutcome.MUTUALLY_EXCLUSIVE for r in results):
        exclusive_effects = tuple(e for r in results if r.outcome == CompatibilityOutcome.MUTUALLY_EXCLUSIVE for e in r.interacting_effects)
        return CompatibilityResult(
            outcome=CompatibilityOutcome.MUTUALLY_EXCLUSIVE,
            ordering=OrderingDirection.NONE,
            reason_code="at_least_one_effect_pair_is_exclusive",
            interacting_effects=exclusive_effects,
        )

    # Priority 2: Any unresolved outcome propagates
    if any(r.outcome == CompatibilityOutcome.UNRESOLVED for r in results):
        unresolved_effects = tuple(e for r in results if r.outcome == CompatibilityOutcome.UNRESOLVED for e in r.interacting_effects)
        return CompatibilityResult(
            outcome=CompatibilityOutcome.UNRESOLVED,
            ordering=OrderingDirection.NONE,
            reason_code="at_least_one_effect_pair_is_unresolved",
            interacting_effects=unresolved_effects,
        )

    # Priority 3: Check for contradictory ordering directions
    directions = {r.ordering for r in results if r.outcome == CompatibilityOutcome.ORDERED_COMPOSITION}
    if {OrderingDirection.A_BEFORE_B, OrderingDirection.B_BEFORE_A}.issubset(directions):
        return CompatibilityResult(
            outcome=CompatibilityOutcome.UNRESOLVED,
            ordering=OrderingDirection.NONE,
            reason_code="contradictory_pairwise_ordering",
            interacting_effects=tuple(e for r in results for e in r.interacting_effects),
        )

    # Priority 4: Ordered composition
    if OrderingDirection.A_BEFORE_B in directions:
        return CompatibilityResult(
            outcome=CompatibilityOutcome.ORDERED_COMPOSITION,
            ordering=OrderingDirection.A_BEFORE_B,
            reason_code="aggregated_ordered_composition",
            interacting_effects=tuple(e for r in results for e in r.interacting_effects),
        )
    if OrderingDirection.B_BEFORE_A in directions:
        return CompatibilityResult(
            outcome=CompatibilityOutcome.ORDERED_COMPOSITION,
            ordering=OrderingDirection.B_BEFORE_A,
            reason_code="aggregated_ordered_composition",
            interacting_effects=tuple(e for r in results for e in r.interacting_effects),
        )

    # Priority 5: Idempotent duplicate
    if any(r.outcome == CompatibilityOutcome.IDEMPOTENT_DUPLICATE for r in results):
        return CompatibilityResult(
            outcome=CompatibilityOutcome.IDEMPOTENT_DUPLICATE,
            ordering=OrderingDirection.NONE,
            reason_code="aggregated_idempotent_duplicate",
            interacting_effects=tuple(e for r in results for e in r.interacting_effects),
        )

    # Priority 6: All parallel
    return CompatibilityResult(
        outcome=CompatibilityOutcome.COMPATIBLE_PARALLEL,
        ordering=OrderingDirection.NONE,
        reason_code="all_effect_pairs_parallel",
        interacting_effects=tuple(e for r in results for e in r.interacting_effects),
    )


# ==============================================================================
# 3. Single-Function Dependency Graph Builder & Deterministic Kahn DAG Sorter
# ==============================================================================

def build_dependency_graph(
    steps: Tuple[PlanStep, ...],
    dependencies: Tuple[PlanStepDependency, ...],
) -> Dict[str, Set[str]]:
    """
    Single unified dependency graph builder used across planning, sorting, 
    authorization validation, and execution gating (Errata E4).
    """
    step_ids = {s.step_id for s in steps}
    adj: Dict[str, Set[str]] = {sid: set() for sid in step_ids}
    seen_edges: Set[Tuple[str, str]] = set()

    for dep in dependencies:
        before = dep.before_step_id
        after = dep.after_step_id

        if before not in step_ids:
            raise PlannerInvariantError(f"Dependency references unknown before_step_id: {before}")
        if after not in step_ids:
            raise PlannerInvariantError(f"Dependency references unknown after_step_id: {after}")
        if before == after:
            raise PlannerInvariantError(f"Self-dependency detected on step: {before}")

        edge = (before, after)
        if edge in seen_edges:
            raise PlannerInvariantError(f"Duplicate dependency edge detected: {edge}")
        seen_edges.add(edge)
        adj[before].add(after)

    return adj


def topological_sort_steps(
    steps: Tuple[PlanStep, ...],
    dependencies: Tuple[PlanStepDependency, ...],
) -> List[PlanStep]:
    """
    Deterministic Kahn topological sort using stable alphabetical tie-breaking on zero-in-degree queue.
    """
    adj = build_dependency_graph(steps, dependencies)
    in_degree: Dict[str, int] = {s.step_id: 0 for s in steps}
    step_lookup: Dict[str, PlanStep] = {s.step_id: s for s in steps}

    for before, after_set in adj.items():
        for after in after_set:
            in_degree[after] += 1

    # Deterministic alphabetical tie-breaking queue
    zero_in_degree = sorted([sid for sid, deg in in_degree.items() if deg == 0])
    ordered_steps: List[PlanStep] = []

    while zero_in_degree:
        curr_id = zero_in_degree.pop(0)
        ordered_steps.append(step_lookup[curr_id])

        for neighbor in sorted(adj[curr_id]):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                zero_in_degree.append(neighbor)
                zero_in_degree.sort()

    if len(ordered_steps) != len(steps):
        raise PlannerInvariantError("Cycle detected in generated plan DAG across ordered dependencies")

    return ordered_steps
