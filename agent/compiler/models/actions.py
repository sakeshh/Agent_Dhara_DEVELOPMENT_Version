from __future__ import annotations

from enum import Enum
import re
from typing import Any, Dict, List, Literal, Optional, Type, Union
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from agent.compiler.models.logical import ActionType


class GenericActionParameters(BaseModel):
    model_config = ConfigDict(extra="allow")


class ActionSource(str, Enum):
    """Business provenance of why a transformation action exists."""
    ASSESSMENT = "assessment"
    BUSINESS_RULE = "business_rule"
    CATALOG = "catalog"
    PROMOTED_RULE = "promoted_rule"
    USER_OVERRIDE = "user_override"
    TIER3 = "tier3"


# ==============================================================================
# Typed Parameter Contracts
# ==============================================================================

# --- Level 2 Composable Operators ---

class PrefixAddParameters(BaseModel):
    prefix: str = Field(..., description="String prefix to prepend to the column value")
    only_if_missing: bool = Field(default=True, description="Only add prefix if value does not already start with it")

    @field_validator("prefix")
    @classmethod
    def validate_prefix(cls, v: str) -> str:
        if not v:
            raise ValueError("prefix cannot be empty")
        return v


class SuffixAddParameters(BaseModel):
    suffix: str = Field(..., description="String suffix to append to the column value")
    only_if_missing: bool = Field(default=True, description="Only add suffix if value does not already end with it")

    @field_validator("suffix")
    @classmethod
    def validate_suffix(cls, v: str) -> str:
        if not v:
            raise ValueError("suffix cannot be empty")
        return v


class PatternReplaceParameters(BaseModel):
    pattern: str = Field(..., description="Pattern or regular expression to search for")
    replacement: str = Field(default="", description="Replacement string")
    regex: bool = Field(default=True, description="Whether pattern is a regular expression")

    @field_validator("pattern")
    @classmethod
    def validate_pattern(cls, v: str, info: Any) -> str:
        if not v:
            raise ValueError("pattern cannot be empty")
        return v

    @model_validator(mode="after")
    def validate_regex_compilation(self) -> PatternReplaceParameters:
        if self.regex:
            try:
                re.compile(self.pattern)
            except re.error as e:
                raise ValueError(f"Invalid regular expression pattern '{self.pattern}': {e}")
        return self


class ValueMapParameters(BaseModel):
    mappings: Dict[str, str] = Field(..., description="Dictionary of source value -> replacement value")
    preserve_unmapped: bool = Field(default=True, description="If True, retain unmapped values; if False, set to NULL")

    @field_validator("mappings")
    @classmethod
    def validate_mappings(cls, v: Dict[str, str]) -> Dict[str, str]:
        if not v:
            raise ValueError("mappings cannot be empty dictionary")
        return v


class PadParameters(BaseModel):
    length: int = Field(..., description="Target string length after padding")
    fill_character: str = Field(default="0", description="Single character used for padding")
    side: Literal["left", "right"] = Field(default="left", description="Side to apply padding")

    @field_validator("length")
    @classmethod
    def validate_length(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("length must be a positive integer greater than 0")
        return v

    @field_validator("fill_character")
    @classmethod
    def validate_fill_character(cls, v: str) -> str:
        if len(v) != 1:
            raise ValueError(f"fill_character must be exactly 1 character, got {len(v)}")
        return v


class ComparisonOperator(str, Enum):
    EQ = "EQ"
    NEQ = "NEQ"
    GT = "GT"
    LT = "LT"
    GTE = "GTE"
    LTE = "LTE"
    IS_NULL = "IS_NULL"
    IS_NOT_NULL = "IS_NOT_NULL"
    LIKE = "LIKE"
    IN = "IN"


class StructuredCondition(BaseModel):
    column: str = Field(..., description="Column identifier for condition")
    operator: ComparisonOperator = Field(default=ComparisonOperator.EQ, description="Comparison operator")
    value: Optional[Any] = Field(default=None, description="Value to compare against (None for null checks)")


class ConditionalUpdateParameters(BaseModel):
    condition: Optional[Union[StructuredCondition, Dict[str, Any], str]] = Field(default=None, description="Structured condition or safe comparison predicate")
    condition_ast: Optional[Dict[str, Any]] = Field(default=None, description="Condition AST dict conforming to ConditionAstNode")
    new_value: Optional[Any] = Field(default=None, description="Value or expression when condition is true")
    else_value: Optional[Any] = Field(default=None, description="Value or expression when condition is false (None retains existing column)")
    disposition: Optional[str] = Field(default=None, description="Remediation disposition")

    @field_validator("condition")
    @classmethod
    def validate_condition(cls, v: Optional[Union[StructuredCondition, Dict[str, Any], str]]) -> Optional[Union[StructuredCondition, Dict[str, Any], str]]:
        if v is None:
            return v
        if isinstance(v, str):
            if not v or not v.strip():
                raise ValueError("condition cannot be empty")
            return v.strip()
        elif isinstance(v, dict):
            if "column" not in v:
                raise ValueError("Structured condition dict must specify 'column'")
            return v
        return v

    @model_validator(mode="after")
    def validate_condition_presence(self) -> ConditionalUpdateParameters:
        if self.condition is None and self.condition_ast is None:
            raise ValueError("ConditionalUpdateParameters requires either 'condition' or 'condition_ast'.")
        return self


# --- Level 1 Deterministic Operators ---

class TrimParameters(BaseModel):
    side: Literal["both", "left", "right"] = Field(default="both", description="Side to strip whitespace")


class CastTypeParameters(BaseModel):
    target_type: str = Field(..., description="Target SQL/Python data type (e.g. NVARCHAR(100), INT, DECIMAL(18,2))")

    @field_validator("target_type")
    @classmethod
    def validate_target_type(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("target_type cannot be empty")
        return v.strip()


FILL_STRATEGY_ALIASES: Dict[str, str] = {
    "value": "constant",
    "fixed": "constant",
    "literal": "constant",
    "ffill": "forward_fill",
    "bfill": "backward_fill",
}


class FillNullsParameters(BaseModel):
    strategy: Literal["constant", "mean", "median", "mode", "forward_fill", "backward_fill"] = Field(
        default="constant", description="Imputation strategy"
    )
    fill_value: Optional[Any] = Field(default=None, description="Constant value if strategy is 'constant'")

    @model_validator(mode="before")
    @classmethod
    def normalize_strategy_and_values(cls, data: Any) -> Any:
        if isinstance(data, dict):
            strat = data.get("strategy")
            if strat and str(strat).lower() in FILL_STRATEGY_ALIASES:
                data["strategy"] = FILL_STRATEGY_ALIASES[str(strat).lower()]
            if "fill_value" not in data and "value" in data:
                data["fill_value"] = data["value"]
        return data


class RangeClipParameters(BaseModel):
    min_value: Optional[float] = Field(default=None, description="Lower threshold")
    max_value: Optional[float] = Field(default=None, description="Upper threshold")

    @model_validator(mode="after")
    def validate_range(self) -> RangeClipParameters:
        if self.min_value is None and self.max_value is None:
            raise ValueError("At least one of min_value or max_value must be provided")
        if self.min_value is not None and self.max_value is not None and self.min_value > self.max_value:
            raise ValueError(f"min_value ({self.min_value}) cannot exceed max_value ({self.max_value})")
        return self


class DeduplicateParameters(BaseModel):
    subset_columns: List[str] = Field(default_factory=list, description="Columns to define row uniqueness; empty uses all")
    keep: Literal["first", "last"] = Field(default="first", description="Which duplicate record to retain")


class ParseDatesParameters(BaseModel):
    date_format: Optional[str] = Field(default=None, description="Expected date/time string format, or None for auto-infer")


class ZeroToNullParameters(BaseModel):
    pass


class ReplaceSentinelsParameters(BaseModel):
    sentinels: List[Any] = Field(default_factory=lambda: ["-999", "-9999", "N/A", "NA", "NULL", "none", "UNKNOWN"])


class CustomExpressionParameters(BaseModel):
    expression: str = Field(..., description="Legacy/governed custom expression string")


# ==============================================================================
# Parameter Model Registry
# ==============================================================================

ACTION_PARAMETER_REGISTRY: Dict[ActionType, Type[BaseModel]] = {
    # Level 2 Composable Operators
    ActionType.PREFIX_ADD: PrefixAddParameters,
    ActionType.SUFFIX_ADD: SuffixAddParameters,
    ActionType.PATTERN_REPLACE: PatternReplaceParameters,
    ActionType.VALUE_MAP: ValueMapParameters,
    ActionType.PAD: PadParameters,
    ActionType.CONDITIONAL_UPDATE: ConditionalUpdateParameters,
    # Level 1 Operators
    ActionType.TRIM: TrimParameters,
    ActionType.CAST_TYPE: CastTypeParameters,
    ActionType.FILL_NULLS: FillNullsParameters,
    ActionType.RANGE_CLIP: RangeClipParameters,
    ActionType.CAP_OUTLIERS: RangeClipParameters,
    ActionType.DEDUPLICATE: DeduplicateParameters,
    ActionType.PARSE_DATES: ParseDatesParameters,
    ActionType.ZERO_TO_NULL: ZeroToNullParameters,
    ActionType.REPLACE_SENTINELS: ReplaceSentinelsParameters,
    ActionType.CUSTOM_EXPRESSION: CustomExpressionParameters,
}


# ==============================================================================
# StructuredAction Universal Contract
# ==============================================================================

class StructuredAction(BaseModel):
    """
    Universal boundary contract for all transformation intent.
    Every source (assessment findings, business rules, catalog rules, LLM extraction)
    must produce a validated StructuredAction before logical/physical planning.
    """
    action_type: ActionType
    dataset_name: str
    target_columns: List[str] = Field(default_factory=list)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    action_id: Optional[str] = Field(default=None, description="Unique action identifier for lineage tracking")
    source_issue_ids: List[str] = Field(default_factory=list, description="IDs of source assessment issues remediated by this action")
    source: ActionSource = ActionSource.ASSESSMENT
    source_reference: Optional[str] = Field(default=None, description="Original rule ID or issue ID")
    extraction_method: Optional[str] = Field(default=None, description="E.g. deterministic, llm, catalog")
    confidence: Optional[float] = Field(default=1.0, ge=0.0, le=1.0)
    requires_approval: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _ensure_action_lineage(self) -> StructuredAction:
        if not self.action_id:
            cols_str = "_".join(self.target_columns) if self.target_columns else "global"
            self.action_id = f"action_{self.dataset_name}_{self.action_type.value}_{cols_str}"
        if not self.source_issue_ids and self.source_reference:
            self.source_issue_ids = [self.source_reference]
        return self

    @field_validator("target_columns")
    @classmethod
    def validate_target_columns(cls, v: List[str]) -> List[str]:
        # Strip leading/trailing whitespaces from column names
        return [c.strip() for c in v if c and c.strip()]

    def get_typed_parameters(self) -> BaseModel:
        """
        Validates and returns the strongly-typed parameter instance
        corresponding to this action's ActionType.
        """
        param_cls = ACTION_PARAMETER_REGISTRY.get(self.action_type)
        if not param_cls:
            # For actions without a specialized parameter model, return a generic wrapper
            return GenericActionParameters(**self.parameters)
        return param_cls.model_validate(self.parameters)
