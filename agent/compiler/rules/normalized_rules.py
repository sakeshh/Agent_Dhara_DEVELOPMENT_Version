from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union


class ConstraintType(str, Enum):
    RANGE = "RANGE"
    NOT_NULL = "NOT_NULL"
    ALLOWED_VALUES = "ALLOWED_VALUES"
    ASSERTION = "ASSERTION"
    TRANSFORMATION = "TRANSFORMATION"
    POLICY = "POLICY"
    REQUIRED_COLUMN = "REQUIRED_COLUMN"


class ViolationDisposition(str, Enum):
    FLAG = "FLAG"
    REJECT = "REJECT"
    QUARANTINE = "QUARANTINE"
    CLIP = "CLIP"
    FILL = "FILL"
    MAP = "MAP"


class RuleAuthority(str, Enum):
    MANUAL_OVERRIDE = "MANUAL_OVERRIDE"
    APPROVED_BUSINESS_RULE = "APPROVED_BUSINESS_RULE"
    SCHEMA_CONTRACT = "SCHEMA_CONTRACT"
    SEMANTIC_RULE = "SEMANTIC_RULE"
    TIER3_PROPOSAL = "TIER3_PROPOSAL"
    HEURISTIC = "HEURISTIC"


AUTHORITY_WEIGHTS: Dict[RuleAuthority, int] = {
    RuleAuthority.MANUAL_OVERRIDE: 1000,
    RuleAuthority.APPROVED_BUSINESS_RULE: 800,
    RuleAuthority.SCHEMA_CONTRACT: 600,
    RuleAuthority.SEMANTIC_RULE: 400,
    RuleAuthority.TIER3_PROPOSAL: 200,
    RuleAuthority.HEURISTIC: 100,
}


@dataclass(frozen=True)
class RuleSourceReference:
    source_type: str
    location: Optional[str] = None
    rule_id: Optional[str] = None


@dataclass(frozen=True)
class ColumnBusinessRule:
    """Represents a column-level business constraint or remediation rule."""
    rule_id: str
    dataset: str
    column: str
    constraint_type: ConstraintType
    parameters: Dict[str, Any] = field(default_factory=dict)
    on_violation_disposition: ViolationDisposition = ViolationDisposition.FLAG
    explicit_remediation_action: Optional[str] = None
    authority: RuleAuthority = RuleAuthority.APPROVED_BUSINESS_RULE
    source: str = "business_rules"
    priority: int = 100
    approval_required: bool = False
    enabled: bool = True
    description: str = ""

    @property
    def is_constraint_only(self) -> bool:
        """True if this rule only specifies what must be true, without authorizing data mutation."""
        return self.explicit_remediation_action is None

    @property
    def defines_explicit_remediation(self) -> bool:
        """True if this rule explicitly authorizes how invalid data should be transformed."""
        return self.explicit_remediation_action is not None


@dataclass(frozen=True)
class CrossFieldBusinessRule:
    """Represents a business rule spanning multiple columns (e.g. StartDate <= EndDate)."""
    rule_id: str
    dataset: str
    columns: Tuple[str, ...]
    expression: str
    on_violation_disposition: ViolationDisposition = ViolationDisposition.FLAG
    explicit_remediation_action: Optional[str] = None
    authority: RuleAuthority = RuleAuthority.APPROVED_BUSINESS_RULE
    source: str = "business_rules"
    priority: int = 100
    approval_required: bool = False
    enabled: bool = True
    description: str = ""

    @property
    def is_constraint_only(self) -> bool:
        return self.explicit_remediation_action is None


@dataclass(frozen=True)
class TransformationBusinessRule:
    """Represents an explicit data transformation (prefix, suffix, pad, format)."""
    rule_id: str
    dataset: str
    column: str
    action_type: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    authority: RuleAuthority = RuleAuthority.APPROVED_BUSINESS_RULE
    source: str = "business_rules"
    priority: int = 100
    enabled: bool = True
    description: str = ""


@dataclass(frozen=True)
class PolicyRule:
    """Represents a global or dataset-level governance policy (e.g. never_drop_rows)."""
    rule_id: str
    name: str
    value: Any
    dataset: Optional[str] = None
    authority: RuleAuthority = RuleAuthority.APPROVED_BUSINESS_RULE
    source: str = "business_rules"
    enabled: bool = True
    description: str = ""


@dataclass(frozen=True)
class NormalizedBusinessRules:
    """
    Canonical, unified business rules representation.
    All input paths (YAML, requirements documents, UI, chat, legacy domain_rules,
    assessment business_rules) must converge into this model before planning.
    """
    never_drop_rows: bool = False
    required_columns: Tuple[str, ...] = ()
    column_rules: Tuple[ColumnBusinessRule, ...] = ()
    cross_field_rules: Tuple[CrossFieldBusinessRule, ...] = ()
    transformation_rules: Tuple[TransformationBusinessRule, ...] = ()
    policy_rules: Tuple[PolicyRule, ...] = ()
    source_references: Tuple[RuleSourceReference, ...] = ()
    metadata: Dict[str, Any] = field(default_factory=dict)

    def get_rules_for_column(self, dataset: str, column: str) -> List[ColumnBusinessRule]:
        return [
            r for r in self.column_rules
            if r.enabled and r.dataset.lower() == dataset.lower() and r.column.lower() == column.lower()
        ]

    def get_transformations_for_column(self, dataset: str, column: str) -> List[TransformationBusinessRule]:
        return [
            r for r in self.transformation_rules
            if r.enabled and r.dataset.lower() == dataset.lower() and r.column.lower() == column.lower()
        ]

    def get_rules_for_dataset(self, dataset: str) -> List[ColumnBusinessRule]:
        return [
            r for r in self.column_rules
            if r.enabled and r.dataset.lower() == dataset.lower()
        ]
