"""
Tests for agent.rules.conflict_resolver: 25-pair access matrix, semantic effect resolver,
multi-effect aggregation, and deterministic DAG topological sorting.
"""
import pytest

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
from agent.rules.conflict_resolver import (
    _BASE_ACCESS_MATRIX,
    validate_effect_matrix_completeness,
    resolve_semantic_effect_compatibility,
    aggregate_compatibility_results,
    build_dependency_graph,
    topological_sort_steps,
)


def test_matrix_completeness_and_invariants():
    # Must have exactly 25 entries
    assert len(_BASE_ACCESS_MATRIX) == 25
    validate_effect_matrix_completeness()


def test_wildcard_resource_unresolved():
    e1 = DataEffect(
        access=DataEffectAccess.READ,
        resource_ref="*",
        action="read_all",
        effect_semantic_hash="h1",
        normalized_parameters_hash="p1",
    )
    e2 = DataEffect(
        access=DataEffectAccess.WRITE,
        resource_ref="customers",
        action="write_table",
        effect_semantic_hash="h2",
        normalized_parameters_hash="p2",
    )
    res = resolve_semantic_effect_compatibility(e1, e2)
    assert res.outcome == CompatibilityOutcome.UNRESOLVED
    assert res.reason_code == "unresolved_resource_wildcard"


def test_disjoint_resources_parallel():
    e1 = DataEffect(
        access=DataEffectAccess.WRITE,
        resource_ref="orders",
        action="update",
        effect_semantic_hash="h1",
        normalized_parameters_hash="p1",
    )
    e2 = DataEffect(
        access=DataEffectAccess.WRITE,
        resource_ref="customers",
        action="update",
        effect_semantic_hash="h2",
        normalized_parameters_hash="p2",
    )
    res = resolve_semantic_effect_compatibility(e1, e2)
    assert res.outcome == CompatibilityOutcome.COMPATIBLE_PARALLEL
    assert res.reason_code == "disjoint_resources"


def test_identical_idempotent_duplicate():
    e1 = DataEffect(
        access=DataEffectAccess.WRITE,
        resource_ref="customers",
        column_scope=frozenset({"status"}),
        action="set_active",
        effect_semantic_hash="same_hash",
        normalized_parameters_hash="same_params",
        idempotent=True,
    )
    e2 = DataEffect(
        access=DataEffectAccess.WRITE,
        resource_ref="customers",
        column_scope=frozenset({"status"}),
        action="set_active",
        effect_semantic_hash="same_hash",
        normalized_parameters_hash="same_params",
        idempotent=True,
    )
    res = resolve_semantic_effect_compatibility(e1, e2)
    assert res.outcome == CompatibilityOutcome.IDEMPOTENT_DUPLICATE
    assert res.reason_code == "identical_idempotent_effects"


def test_idempotent_non_identical_writes_exclusive():
    # Two idempotent writes with DIFFERENT actions/parameters are mutually exclusive!
    e1 = DataEffect(
        access=DataEffectAccess.WRITE,
        resource_ref="customers",
        column_scope=frozenset({"status"}),
        action="set_active",
        effect_semantic_hash="hash_active",
        normalized_parameters_hash="p1",
        idempotent=True,
    )
    e2 = DataEffect(
        access=DataEffectAccess.WRITE,
        resource_ref="customers",
        column_scope=frozenset({"status"}),
        action="set_inactive",
        effect_semantic_hash="hash_inactive",
        normalized_parameters_hash="p2",
        idempotent=True,
    )
    res = resolve_semantic_effect_compatibility(e1, e2)
    assert res.outcome == CompatibilityOutcome.MUTUALLY_EXCLUSIVE


def test_read_write_timing_semantics():
    e_read = DataEffect(
        access=DataEffectAccess.READ,
        resource_ref="customers",
        column_scope=frozenset({"status"}),
        action="read_state",
        effect_semantic_hash="h_r",
        normalized_parameters_hash="p_r",
        reads_state="pre_effect",
    )
    e_write = DataEffect(
        access=DataEffectAccess.WRITE,
        resource_ref="customers",
        column_scope=frozenset({"status"}),
        action="update_status",
        effect_semantic_hash="h_w",
        normalized_parameters_hash="p_w",
    )
    res = resolve_semantic_effect_compatibility(e_read, e_write)
    assert res.outcome == CompatibilityOutcome.ORDERED_COMPOSITION
    assert res.ordering == OrderingDirection.A_BEFORE_B
    assert res.reason_code == "read_pre_write_state"


def test_aggregate_compatibility_precedence():
    r_parallel = CompatibilityResult(outcome=CompatibilityOutcome.COMPATIBLE_PARALLEL, reason_code="p")
    r_ordered = CompatibilityResult(outcome=CompatibilityOutcome.ORDERED_COMPOSITION, ordering=OrderingDirection.A_BEFORE_B, reason_code="o")
    r_unresolved = CompatibilityResult(outcome=CompatibilityOutcome.UNRESOLVED, reason_code="u")
    r_exclusive = CompatibilityResult(outcome=CompatibilityOutcome.MUTUALLY_EXCLUSIVE, reason_code="e")

    # Mutual exclusion dominates unresolved, ordered, parallel
    assert aggregate_compatibility_results([r_parallel, r_ordered, r_exclusive]).outcome == CompatibilityOutcome.MUTUALLY_EXCLUSIVE
    assert aggregate_compatibility_results([r_parallel, r_unresolved, r_ordered]).outcome == CompatibilityOutcome.UNRESOLVED
    assert aggregate_compatibility_results([r_parallel, r_ordered]).outcome == CompatibilityOutcome.ORDERED_COMPOSITION
    assert aggregate_compatibility_results([r_parallel, r_parallel]).outcome == CompatibilityOutcome.COMPATIBLE_PARALLEL


def test_aggregate_contradictory_ordering():
    r1 = CompatibilityResult(outcome=CompatibilityOutcome.ORDERED_COMPOSITION, ordering=OrderingDirection.A_BEFORE_B, reason_code="o1")
    r2 = CompatibilityResult(outcome=CompatibilityOutcome.ORDERED_COMPOSITION, ordering=OrderingDirection.B_BEFORE_A, reason_code="o2")
    res = aggregate_compatibility_results([r1, r2])
    assert res.outcome == CompatibilityOutcome.UNRESOLVED
    assert res.reason_code == "contradictory_pairwise_ordering"


def test_dependency_graph_validation():
    s1 = PlanStep(step_id="step-1", action="a1", action_version="1.0")
    s2 = PlanStep(step_id="step-2", action="a2", action_version="1.0")
    s3 = PlanStep(step_id="step-3", action="a3", action_version="1.0")

    # Unknown step reference
    with pytest.raises(PlannerInvariantError, match="unknown before_step_id"):
        build_dependency_graph((s1, s2), (PlanStepDependency(before_step_id="step-99", after_step_id="step-1"),))

    # Self dependency
    with pytest.raises(PlannerInvariantError, match="Self-dependency"):
        build_dependency_graph((s1,), (PlanStepDependency(before_step_id="step-1", after_step_id="step-1"),))

    # Duplicate edge
    with pytest.raises(PlannerInvariantError, match="Duplicate dependency edge"):
        dep = PlanStepDependency(before_step_id="step-1", after_step_id="step-2")
        build_dependency_graph((s1, s2), (dep, dep))


def test_topological_sort_and_cycle_detection():
    s1 = PlanStep(step_id="step-1", action="a1", action_version="1.0")
    s2 = PlanStep(step_id="step-2", action="a2", action_version="1.0")
    s3 = PlanStep(step_id="step-3", action="a3", action_version="1.0")

    # Linear DAG: step-1 -> step-2 -> step-3
    deps = (
        PlanStepDependency(before_step_id="step-1", after_step_id="step-2"),
        PlanStepDependency(before_step_id="step-2", after_step_id="step-3"),
    )
    sorted_steps = topological_sort_steps((s3, s1, s2), deps)
    assert [s.step_id for s in sorted_steps] == ["step-1", "step-2", "step-3"]

    # Cycle: step-1 -> step-2 -> step-3 -> step-1
    cycle_deps = deps + (PlanStepDependency(before_step_id="step-3", after_step_id="step-1"),)
    with pytest.raises(PlannerInvariantError, match="Cycle detected"):
        topological_sort_steps((s1, s2, s3), cycle_deps)
