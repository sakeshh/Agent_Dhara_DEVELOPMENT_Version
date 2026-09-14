"""
Canonical Rule Models, AST Expressions, Lifecycle Dimensions, and Intake Registry.

Implements the frozen v9.3 specification with Errata:
- Strict bi-directional intake validation with no-drop guarantee.
- Parallel independent lifecycle dimensions (Assessment, Planning, Execution) and DataRuleOutcome.
- Entity- and correlation-scoped lifecycle transitions with monotonic sequence verification.
- Pure semantic rule identity (excluding governance metadata logical_rule_id and rule_version).
- Recursive CanonicalExpression AST with operator arity and commutative operand sorting.
- Separation of CanonicalRule, RuleOccurrence, and EffectiveRuleClaim in CanonicalRuleRegistry.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, FrozenSet, List, Literal, Mapping, Optional, Tuple, Union

from pydantic import BaseModel, Field, field_validator, model_validator

from agent.rules.canonical import (
    CANONICALIZATION_SCHEME,
    CANONICALIZATION_VERSION,
    canonical_json,
    compute_canonical_hash,
)

UUID_NAMESPACE_AGENT_DHARA = uuid.uuid5(uuid.NAMESPACE_DNS, "agent-dhara.capgemini.com")


# ==============================================================================
# 1. Exceptions
# ==============================================================================

class RuleDispositionInvariantError(RuntimeError):
    """Raised when an intake record or rule disposition invariant is violated."""
    pass


class LifecycleTransitionInvariantError(RuntimeError):
    """Raised when lifecycle history is discontinuous, non-monotonic, or corrupted."""
    pass


class ExpressionArityError(ValueError):
    """Raised when a CanonicalExpression violates operator arity constraints."""
    pass


# ==============================================================================
# 2. Intake Dispositions
# ==============================================================================

class IntakeDisposition(str, Enum):
    """Terminal intake outcome of an atomic input rule during registry construction."""
    INTERPRETED = "interpreted"
    PRESERVED_UNMAPPED = "preserved_unmapped"
    INVALID = "invalid"
    REJECTED = "rejected"
    MANUAL_REVIEW = "manual_review"


INTAKE_TERMINAL_STATES: FrozenSet[IntakeDisposition] = frozenset({
    IntakeDisposition.INTERPRETED,
    IntakeDisposition.PRESERVED_UNMAPPED,
    IntakeDisposition.INVALID,
    IntakeDisposition.REJECTED,
    IntakeDisposition.MANUAL_REVIEW,
})


class RuleIntakeRecord(BaseModel):
    """Record of an input rule's intake disposition."""
    input_rule_id: str
    disposition: IntakeDisposition
    reason_code: str
    canonical_rule_id: Optional[str] = None
    candidate_rule_id: Optional[str] = None


# ==============================================================================
# 3. Lifecycle Dimensions & Transition Audit History
# ==============================================================================

class RuleProcessingStage(str, Enum):
    """Progression of an interpreted rule through the execution pipeline."""
    REGISTERED = "registered"
    ASSESSABLE = "assessable"
    ACTIONABLE = "actionable"
    EVALUATED = "evaluated"
    PLANNED = "planned"
    AUTHORIZED = "authorized"
    GENERATED = "generated"
    EXECUTED = "executed"
    POST_VALIDATED = "post_validated"


class AssessmentLifecycleStatus(str, Enum):
    """Operational status of the assessment pipeline process."""
    NOT_APPLICABLE = "not_applicable"
    PENDING = "pending"
    EVALUATED = "evaluated"
    NOT_EVALUATED = "not_evaluated"
    ERROR = "error"


class DataRuleOutcome(str, Enum):
    """Semantic data evaluation outcome (orthogonal to process health)."""
    PASS = "pass"
    FAIL = "fail"
    UNKNOWN = "unknown"
    NOT_APPLICABLE = "not_applicable"


class PlanningLifecycleStatus(str, Enum):
    """Operational status of rule transformation planning."""
    NOT_APPLICABLE = "not_applicable"
    PENDING = "pending"
    PLANNED = "planned"
    REJECTED = "rejected"
    MANUAL_REVIEW = "manual_review"


class ExecutionLifecycleStatus(str, Enum):
    """Operational status of generated artifact transformation execution."""
    NOT_APPLICABLE = "not_applicable"
    PENDING = "pending"
    AUTHORIZED = "authorized"
    GENERATED = "generated"
    EXECUTED = "executed"
    POST_VALIDATED = "post_validated"
    FAILED = "failed"


class LifecycleEntityType(str, Enum):
    RULE = "rule"
    PLAN = "plan"
    STEP = "step"
    EXECUTION = "execution"


class LifecycleTransition(BaseModel):
    """Typed, tamper-evident record of an entity's lifecycle state change."""
    transition_id: str
    correlation_id: str

    entity_type: LifecycleEntityType
    entity_id: str

    dimension: Literal["assessment", "planning", "execution"]
    sequence_number: int = Field(ge=1)

    from_state: str
    to_state: str
    transitioned_at: datetime

    actor_type: Literal["system", "specialist_llm", "engineer", "policy_engine"]
    actor_id: Optional[str] = None
    reason_code: str
    audit_event_id: str

    @field_validator("transitioned_at")
    @classmethod
    def require_timezone(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("transitioned_at must be timezone-aware")
        return v.astimezone(timezone.utc)


def validate_transition_chain(transitions: Tuple[LifecycleTransition, ...]) -> None:
    """
    Validates transition history per entity, correlation, and dimension:
    - Global uniqueness of transition_id and audit_event_id
    - Monotonic contiguous sequence numbers [1, ..., N]
    - State continuity (curr.from_state == prev.to_state)
    - Monotonic timestamps
    """
    transition_ids = [t.transition_id for t in transitions]
    if len(transition_ids) != len(set(transition_ids)):
        raise LifecycleTransitionInvariantError("Duplicate transition_id detected in lifecycle history")

    audit_ids = [t.audit_event_id for t in transitions]
    if len(audit_ids) != len(set(audit_ids)):
        raise LifecycleTransitionInvariantError("Duplicate audit_event_id detected in lifecycle history")

    # Group by (correlation_id, entity_type, entity_id, dimension)
    by_chain: Dict[Tuple[str, LifecycleEntityType, str, str], List[LifecycleTransition]] = {}
    for t in transitions:
        chain_key = (t.correlation_id, t.entity_type, t.entity_id, t.dimension)
        by_chain.setdefault(chain_key, []).append(t)

    for (corr_id, ent_type, ent_id, dim), chain in by_chain.items():
        chain.sort(key=lambda item: item.sequence_number)

        expected_sequence = list(range(1, len(chain) + 1))
        actual_sequence = [item.sequence_number for item in chain]
        if actual_sequence != expected_sequence:
            raise LifecycleTransitionInvariantError(
                f"Non-contiguous sequence numbers for entity {ent_type.value}:{ent_id} [{dim}] (correlation {corr_id}): "
                f"{actual_sequence} (expected {expected_sequence})"
            )

        for prev, curr in zip(chain, chain[1:]):
            if curr.from_state != prev.to_state:
                raise LifecycleTransitionInvariantError(
                    f"Discontinuous state transition for {ent_type.value}:{ent_id} [{dim}]: "
                    f"'{prev.to_state}' -> '{curr.from_state}' (expected '{prev.to_state}')"
                )
            if curr.transitioned_at < prev.transitioned_at:
                raise LifecycleTransitionInvariantError(
                    f"Non-monotonic timestamp for {ent_type.value}:{ent_id} [{dim}]: "
                    f"{curr.transitioned_at} occurred before {prev.transitioned_at}"
                )


class RuleLifecycleState(BaseModel):
    """Parallel multidimensional status container for a canonical rule."""
    rule_id: str
    assessment_status: AssessmentLifecycleStatus = AssessmentLifecycleStatus.NOT_APPLICABLE
    data_rule_outcome: DataRuleOutcome = DataRuleOutcome.NOT_APPLICABLE
    planning_status: PlanningLifecycleStatus = PlanningLifecycleStatus.NOT_APPLICABLE
    execution_status: ExecutionLifecycleStatus = ExecutionLifecycleStatus.NOT_APPLICABLE
    transitions: Tuple[LifecycleTransition, ...] = ()


# ==============================================================================
# 4. Recursive CanonicalExpression AST
# ==============================================================================

OPERATOR_SPECS: Dict[str, Dict[str, Any]] = {
    "==": {"min_args": 2, "max_args": 2, "commutative": True},
    "!=": {"min_args": 2, "max_args": 2, "commutative": True},
    ">": {"min_args": 2, "max_args": 2, "commutative": False},
    ">=": {"min_args": 2, "max_args": 2, "commutative": False},
    "<": {"min_args": 2, "max_args": 2, "commutative": False},
    "<=": {"min_args": 2, "max_args": 2, "commutative": False},
    "AND": {"min_args": 2, "max_args": None, "commutative": True},
    "OR": {"min_args": 2, "max_args": None, "commutative": True},
    "NOT": {"min_args": 1, "max_args": 1, "commutative": False},
    "IS_NULL": {"min_args": 1, "max_args": 1, "commutative": False},
    "IS_NOT_NULL": {"min_args": 1, "max_args": 1, "commutative": False},
    "IN": {"min_args": 2, "max_args": 2, "commutative": False},
}

OPERATOR_ALIASES: Dict[str, str] = {
    "EQ": "==",
    "EQUALS": "==",
    "NE": "!=",
    "NOT_EQUALS": "!=",
    "GT": ">",
    "GTE": ">=",
    "LT": "<",
    "LTE": "<=",
}


class ColumnOperand(BaseModel):
    operand_type: Literal["column"] = "column"
    dataset: Optional[str] = None
    column: str
    data_type: Optional[str] = None


class LiteralOperand(BaseModel):
    operand_type: Literal["literal"] = "literal"
    value: Any
    data_type: str


OperandNode = Union[ColumnOperand, LiteralOperand, "CanonicalExpression"]


class CanonicalExpression(BaseModel):
    operand_type: Literal["expression"] = "expression"
    operator: str
    operands: Tuple[OperandNode, ...]

    def normalize(self) -> "CanonicalExpression":
        raw_op = self.operator.strip().upper()
        norm_op = OPERATOR_ALIASES.get(raw_op, self.operator.strip())

        spec = OPERATOR_SPECS.get(norm_op)
        if spec:
            arg_count = len(self.operands)
            if arg_count < spec["min_args"]:
                raise ExpressionArityError(f"Operator '{norm_op}' requires at least {spec['min_args']} operands, got {arg_count}")
            if spec["max_args"] is not None and arg_count > spec["max_args"]:
                raise ExpressionArityError(f"Operator '{norm_op}' permits at most {spec['max_args']} operands, got {arg_count}")

        # Recursively normalize nested expressions
        norm_operands: List[OperandNode] = []
        for op in self.operands:
            if isinstance(op, CanonicalExpression):
                norm_operands.append(op.normalize())
            else:
                norm_operands.append(op)

        # Commutative sorting strictly for commutative operators
        if spec and spec.get("commutative", False):
            sorted_operands = tuple(sorted(norm_operands, key=lambda x: canonical_json(x)))
        else:
            sorted_operands = tuple(norm_operands)

        return CanonicalExpression(operator=norm_op, operands=sorted_operands)


# Enable recursive model definition in Pydantic v2
CanonicalExpression.model_rebuild()


# ==============================================================================
# 5. Core Rule Taxonomy & Semantic Identity
# ==============================================================================

class RuleType(str, Enum):
    VALIDATION_ASSERTION = "validation_assertion"
    TRANSFORMATION = "transformation"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    SCHEMA_CONSTRAINT = "schema_constraint"
    ANOMALY_DETECTION = "anomaly_detection"


class RuleAction(str, Enum):
    ASSERT = "assert"
    FLAG = "flag"
    REPLACE = "replace"
    IMPUTE = "impute"
    QUARANTINE = "quarantine"
    STANDARDIZE = "standardize"
    DROP = "drop"


class NullPolicy(str, Enum):
    IGNORE = "ignore"
    FAIL = "fail"
    SEPARATE_VIOLATION = "separate_violation"
    COALESCE = "coalesce"


class RuleScope(BaseModel):
    dataset: str
    columns: Tuple[str, ...] = ()

    @field_validator("columns", mode="before")
    @classmethod
    def ensure_tuple(cls, v: Any) -> Tuple[str, ...]:
        if isinstance(v, (list, set, frozenset)):
            return tuple(sorted(str(c) for c in v))
        if isinstance(v, tuple):
            return tuple(sorted(str(c) for c in v))
        return ()


class CanonicalRuleMaterial(BaseModel):
    """
    Pure semantic material defining rule logic.
    Excludes governance IDs (logical_rule_id, rule_version), timestamps, and provenance.
    """
    rule_type: RuleType
    expression: CanonicalExpression
    scope: RuleScope
    action: RuleAction
    normalized_parameters: Dict[str, Any] = Field(default_factory=dict)
    null_policy: NullPolicy = NullPolicy.SEPARATE_VIOLATION


def build_rule_id(material: CanonicalRuleMaterial) -> str:
    """Computes stable, deterministic semantic rule_id from canonical material hash."""
    norm_material = material.model_copy(update={"expression": material.expression.normalize()})
    canonical_repr = canonical_json(norm_material)
    return str(uuid.uuid5(UUID_NAMESPACE_AGENT_DHARA, canonical_repr))


# ==============================================================================
# 6. Occurrence, Claim, and Registry Models
# ==============================================================================

class RuleOccurrence(BaseModel):
    """Single observed assertion or instruction from a specific input source."""
    occurrence_id: str
    canonical_rule_id: str
    source_type: Literal["notes", "yaml_config", "profiler", "ui_override"]
    source_reference: str
    authority_rank: int = Field(ge=0)
    is_explicit: bool
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def require_tz(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        return v.astimezone(timezone.utc)


class EffectiveRuleClaim(BaseModel):
    """Winning policy claim for a canonical rule following source consolidation."""
    rule_id: str
    winning_occurrence_id: str
    consolidation_reason: str


class CanonicalRule(BaseModel):
    """Governed container binding semantic rule logic to governance metadata and occurrences."""
    rule_id: str  # Pure semantic hash
    logical_rule_id: str  # Governance identifier
    rule_version: int  # Governance revision
    material: CanonicalRuleMaterial
    source_occurrences: Tuple[str, ...] = ()


class CanonicalRuleRegistry(BaseModel):
    """Central immutable registry holding rules, occurrences, claims, and intake records."""
    rules: Dict[str, CanonicalRule] = Field(default_factory=dict)
    occurrences: Dict[str, RuleOccurrence] = Field(default_factory=dict)
    claims: Dict[str, EffectiveRuleClaim] = Field(default_factory=dict)
    intake_records: Dict[str, RuleIntakeRecord] = Field(default_factory=dict)

    canonicalization_scheme: str = CANONICALIZATION_SCHEME
    canonicalization_version: str = CANONICALIZATION_VERSION

    def compute_hash(self) -> str:
        """Computes deterministic hash of all contained rules, claims, and intake records."""
        payload = {
            "rules": {rid: r.model_dump(mode="python") for rid, r in sorted(self.rules.items())},
            "claims": {cid: c.model_dump(mode="python") for cid, c in sorted(self.claims.items())},
            "intake_records": {iid: rec.model_dump(mode="python") for iid, rec in sorted(self.intake_records.items())},
            "scheme": self.canonicalization_scheme,
            "version": self.canonicalization_version,
        }
        return compute_canonical_hash(payload)


def validate_intake_records(
    input_rule_ids: set[str],
    intake_records: Mapping[str, RuleIntakeRecord],
    registry: CanonicalRuleRegistry,
) -> None:
    """
    Validates bi-directional intake completeness and canonical rule linkage:
    - set(intake_records.keys()) == input_rule_ids (no-drop guarantee)
    - key == record.input_rule_id
    - INTERPRETED rules have canonical_rule_id present in registry.rules
    - Non-interpreted rules have canonical_rule_id is None
    """
    record_keys = set(intake_records.keys())

    unexpected = record_keys - input_rule_ids
    if unexpected:
        raise RuleDispositionInvariantError(f"Intake records reference unknown input rules: {sorted(unexpected)}")

    missing = input_rule_ids - record_keys
    if missing:
        raise RuleDispositionInvariantError(f"Input rules missing intake records: {sorted(missing)}")

    mismatches = [k for k, r in intake_records.items() if k != r.input_rule_id]
    if mismatches:
        raise RuleDispositionInvariantError(f"Intake record key and input_rule_id mismatch: {sorted(mismatches)}")

    for rec in intake_records.values():
        if rec.disposition == IntakeDisposition.INTERPRETED:
            if not rec.canonical_rule_id:
                raise RuleDispositionInvariantError(f"INTERPRETED rule {rec.input_rule_id} missing canonical_rule_id")
            if rec.canonical_rule_id not in registry.rules:
                raise RuleDispositionInvariantError(
                    f"INTERPRETED rule {rec.input_rule_id} references non-existent canonical_rule_id {rec.canonical_rule_id}"
                )
        else:
            if rec.canonical_rule_id is not None:
                raise RuleDispositionInvariantError(
                    f"Non-interpreted rule {rec.input_rule_id} ({rec.disposition.value}) must have null canonical_rule_id"
                )
