from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# ==============================================================================
# Lifecycle States and Enumerations
# ==============================================================================

class ResolutionStatus(str, Enum):
    PROPOSED = "PROPOSED"
    VALIDATED = "VALIDATED"
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
    APPROVED = "APPROVED"
    COMPILED = "COMPILED"

    MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
    WAIVED = "WAIVED"
    UNSUPPORTED = "UNSUPPORTED"

    INVALID_RESPONSE = "INVALID_RESPONSE"
    SCHEMA_VALIDATION_FAILED = "SCHEMA_VALIDATION_FAILED"
    TYPE_VALIDATION_FAILED = "TYPE_VALIDATION_FAILED"
    SAFETY_VALIDATION_FAILED = "SAFETY_VALIDATION_FAILED"
    CONFLICTED = "CONFLICTED"
    MODEL_UNAVAILABLE = "MODEL_UNAVAILABLE"


class ResolutionDisposition(str, Enum):
    CORRECT = "CORRECT"
    NULLIFY = "NULLIFY"
    REJECT = "REJECT"
    QUARANTINE = "QUARANTINE"
    FLAG = "FLAG"
    RETAIN = "RETAIN"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    WAIVE = "WAIVE"


class EvidenceAuthority(str, Enum):
    DECLARED_SCHEMA = "DECLARED_SCHEMA"
    APPROVED_BUSINESS_RULE = "APPROVED_BUSINESS_RULE"
    APPROVED_CATALOG = "APPROVED_CATALOG"
    USER_INSTRUCTION = "USER_INSTRUCTION"
    ASSESSMENT_EVIDENCE = "ASSESSMENT_EVIDENCE"
    PROFILE_INFERENCE = "PROFILE_INFERENCE"
    MODEL_INFERENCE = "MODEL_INFERENCE"


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    PROHIBITED = "PROHIBITED"


# ==============================================================================
# Condition AST Nodes
# ==============================================================================

class AstNodeType(str, Enum):
    COLUMN = "COLUMN"
    LITERAL = "LITERAL"
    COMPARISON = "COMPARISON"
    BOOLEAN = "BOOLEAN"
    NULL_CHECK = "NULL_CHECK"
    IN_LIST = "IN_LIST"
    BETWEEN = "BETWEEN"
    LIKE = "LIKE"


class ComparisonOperator(str, Enum):
    EQ = "EQ"
    NE = "NE"
    LT = "LT"
    LTE = "LTE"
    GT = "GT"
    GTE = "GTE"


class BooleanOperator(str, Enum):
    AND = "AND"
    OR = "OR"
    NOT = "NOT"


class AstDict(dict):
    def to_dict(self) -> Dict[str, Any]:
        return dict(self)


class _AstBase(BaseModel):
    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(mode="json")


class ColumnRef(_AstBase):
    model_config = ConfigDict(frozen=True, extra="forbid")
    node_type: Literal[AstNodeType.COLUMN] = AstNodeType.COLUMN
    dataset: str
    column: str


class LiteralValue(_AstBase):
    model_config = ConfigDict(frozen=True, extra="forbid")
    node_type: Literal[AstNodeType.LITERAL] = AstNodeType.LITERAL
    data_type: str
    value: Any


class ComparisonExpression(_AstBase):
    model_config = ConfigDict(frozen=True, extra="forbid")
    node_type: Literal[AstNodeType.COMPARISON] = AstNodeType.COMPARISON
    operator: ComparisonOperator
    left: ColumnRef
    right: Union[ColumnRef, LiteralValue]


class NullCheckExpression(_AstBase):
    model_config = ConfigDict(frozen=True, extra="forbid")
    node_type: Literal[AstNodeType.NULL_CHECK] = AstNodeType.NULL_CHECK
    column: ColumnRef
    is_null: bool = True


class InListExpression(_AstBase):
    model_config = ConfigDict(frozen=True, extra="forbid")
    node_type: Literal[AstNodeType.IN_LIST] = AstNodeType.IN_LIST
    column: ColumnRef
    values: List[LiteralValue]
    negated: bool = False


class BetweenExpression(_AstBase):
    model_config = ConfigDict(frozen=True, extra="forbid")
    node_type: Literal[AstNodeType.BETWEEN] = AstNodeType.BETWEEN
    column: ColumnRef
    lower: LiteralValue
    upper: LiteralValue
    negated: bool = False


class LikeExpression(_AstBase):
    model_config = ConfigDict(frozen=True, extra="forbid")
    node_type: Literal[AstNodeType.LIKE] = AstNodeType.LIKE
    column: ColumnRef
    pattern: str
    negated: bool = False


class BooleanExpression(_AstBase):
    model_config = ConfigDict(frozen=True, extra="forbid")
    node_type: Literal[AstNodeType.BOOLEAN] = AstNodeType.BOOLEAN
    operator: BooleanOperator
    operands: List[ConditionAstNode]


ConditionAstNode = Union[
    ComparisonExpression,
    BooleanExpression,
    NullCheckExpression,
    InListExpression,
    BetweenExpression,
    LikeExpression,
]

# Update forward reference for recursive BooleanExpression
BooleanExpression.model_rebuild()


# ==============================================================================
# Tier 3 Action Allowlist
# ==============================================================================

ALLOWED_TIER3_ACTION_TYPES: set[str] = {
    "TRIM",
    "NORMALIZE_CASE",
    "REPLACE_SENTINELS",
    "VALUE_MAP",
    "CONDITIONAL_UPDATE",
    "VALIDATE_RANGE",
    "VALIDATE_ALLOWED_VALUES",
    "VALIDATE_REFERENTIAL_INTEGRITY",
    "FLAG_ROW",
    "REJECT_ROW",
    "MANUAL_REVIEW",
    "DEDUPLICATE",
    "PARSE_DATES",
    "CAST_TYPE",
    "FILL_NULLS",
}


# ==============================================================================
# Tier 3 Resolution Proposal Model
# ==============================================================================

class Tier3ResolutionProposal(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    contract_version: str = Field(default="1.0", description="Tier 3 Contract Schema Version")
    proposal_id: str = Field(..., description="Unique deterministic proposal identifier")
    issue_id: str = Field(..., description="Target canonical issue identifier")
    dataset_name: str = Field(..., description="Target dataset name")

    proposed_action_type: str = Field(..., description="Allowlisted ActionType string")
    target_columns: List[str] = Field(default_factory=list, description="Target dataset columns")
    disposition: ResolutionDisposition = Field(..., description="Target remediation disposition")

    status: ResolutionStatus = Field(default=ResolutionStatus.PROPOSED, description="Current lifecycle status")
    condition_ast: Optional[Union[AstDict, Dict[str, Any], ConditionAstNode]] = Field(default=None, description="Condition AST dict conforming to ConditionAstNode")
    action_parameters: Dict[str, Any] = Field(default_factory=dict, description="Typed action parameter dict")

    evidence_authorities: List[EvidenceAuthority] = Field(default_factory=list, description="Provenance authorities supporting this action")
    evidence_references: List[str] = Field(default_factory=list, description="Rule IDs, sample citations, or evidence IDs")

    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score bounded in [0.0, 1.0]")
    risk_level: RiskLevel = Field(..., description="Classified risk level of the transformation")
    approval_required: bool = Field(default=True, description="Whether human steward approval is mandatory before compilation")

    rationale: str = Field(default="", description="Audit rationale explaining why this action was proposed")
    assumptions: List[str] = Field(default_factory=list, description="Explicit assumptions made if parameters were inferred")
    unresolved_fields: List[str] = Field(default_factory=list, description="Parameters or fields that could not be determined")

    model_provider: Optional[str] = Field(default=None, description="LLM provider name, e.g. azure_openai")
    model_deployment: Optional[str] = Field(default=None, description="Deployment or model name")
    model_version: Optional[str] = Field(default=None, description="Model version tag")
    prompt_contract_version: Optional[str] = Field(default=None, description="Prompt template version tag")

    @field_validator("proposed_action_type")
    @classmethod
    def validate_action_allowlist(cls, v: str) -> str:
        clean_v = str(v).strip().upper()
        if clean_v not in ALLOWED_TIER3_ACTION_TYPES:
            raise ValueError(f"Action '{v}' is not in the Tier 3 allowlist: {sorted(list(ALLOWED_TIER3_ACTION_TYPES))}")
        return clean_v

    @field_validator("target_columns")
    @classmethod
    def validate_target_columns(cls, v: List[str]) -> List[str]:
        cleaned = []
        for c in v:
            c_str = str(c).strip().strip("[]")
            if not c_str:
                raise ValueError("Target column names cannot be empty")
            cleaned.append(c_str)
        return cleaned

    @model_validator(mode="after")
    def validate_authority_and_policy_sanity(self) -> Tier3ResolutionProposal:
        # 1. Evidence authority is mandatory
        if not self.evidence_authorities:
            raise ValueError("Proposal must specify at least one EvidenceAuthority")

        # 2. Destructive dispositions with only MODEL_INFERENCE must require approval
        destructive_dispositions = {
            ResolutionDisposition.REJECT,
            ResolutionDisposition.QUARANTINE,
            ResolutionDisposition.NULLIFY,
        }
        authoritative_authorities = {
            EvidenceAuthority.DECLARED_SCHEMA,
            EvidenceAuthority.APPROVED_BUSINESS_RULE,
            EvidenceAuthority.APPROVED_CATALOG,
        }
        has_authoritative = any(a in authoritative_authorities for a in self.evidence_authorities)

        if self.disposition in destructive_dispositions and not has_authoritative:
            if not self.approval_required:
                raise ValueError(
                    f"Destructive disposition '{self.disposition.value}' without authoritative evidence requires approval_required=True"
                )
            if self.risk_level == RiskLevel.LOW:
                raise ValueError(
                    f"Destructive disposition '{self.disposition.value}' cannot be classified as LOW risk"
                )

        # 3. PROHIBITED risk level can never be auto-approved
        if self.risk_level == RiskLevel.PROHIBITED and not self.approval_required:
            raise ValueError("PROHIBITED risk level must require approval_required=True")

        return self

    @field_validator("condition_ast", mode="before")
    @classmethod
    def _wrap_condition_ast(cls, v: Any) -> Any:
        if isinstance(v, dict) and not hasattr(v, "to_dict"):
            return AstDict(v)
        return v

    @property
    def fingerprint(self) -> str:
        from agent.compiler.tier3.fingerprint import compute_proposal_fingerprint
        return compute_proposal_fingerprint(self)

