"""
transformation_ir.py — Typed Transformation Intermediate Representation (IR) v1.0.0 (Phase 2).

Defines an engine-neutral, strictly typed AST for ETL transformations:
- Common expression primitives (ColumnRef, LiteralValue, BinaryOp, NullCheck, InSetPredicate, etc.)
- Value-version semantics (ORIGINAL / CURRENT / AFTER_STEP)
- Pre-image requirements (purposes, retention, encryption)
- Engine capabilities (TargetEngine, CapabilityMaturity, dialect, limitations)
- Concrete Data Effects (reads, writes, creates, drops, filters_rows, partitions_rows)
- Action Families (18 canonical families + PASS_THROUGH control)
- Discriminated union of typed transformation expressions
- Deterministic Pandas reference compiler for Slice 1
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
from typing import Annotated, Any, Dict, List, Literal, Optional, Set, Tuple, Union
from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator


TRANSFORMATION_IR_VERSION = "1.0.0"


class TransformationIRInvariantError(ValueError):
    """Raised when an immutable IR invariant or effect semantic constraint is violated."""
    pass


# ============================================================================
# 1. Enums & Governance Metadata Models
# ============================================================================

class MigrationDisposition(str, Enum):
    MAPPED_TO_IR = "mapped_to_ir"
    COMPOSITE_IR = "composite_ir"
    PASS_THROUGH_DECISION = "pass_through_decision"
    UNSUPPORTED = "unsupported"
    DEPRECATED = "deprecated"
    MANUAL_REVIEW = "manual_review"


class ClipNullPolicy(str, Enum):
    PRESERVE = "preserve"
    FAIL = "fail"
    IMPUTE_BEFORE_CLIP = "impute_before_clip"


class UnmappedValuePolicy(str, Enum):
    KEEP_ORIGINAL = "keep_original"
    SET_NULL = "set_null"
    FLAG = "flag"
    FAIL_STEP = "fail_step"
    QUARANTINE = "quarantine"


class RowIdentityStrategy(str, Enum):
    EXISTING_KEY = "existing_key"
    COMPOSITE_KEY = "composite_key"
    GENERATED_OCCURRENCE_ID = "generated_occurrence_id"


class ValueVersion(str, Enum):
    ORIGINAL = "original"
    CURRENT = "current"
    AFTER_STEP = "after_step"


class DataType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    DECIMAL = "decimal"
    ANY = "any"


class TargetEngine(str, Enum):
    PANDAS = "pandas"
    PYSPARK = "pyspark"
    SQL_ANSI = "sql_ansi"
    SQL_TSQL = "sql_tsql"
    SQL_SPARK = "sql_spark"
    SQL_DUCKDB = "sql_duckdb"
    ADF = "adf"


class CapabilityMaturity(str, Enum):
    VERIFIED = "verified"
    PARTIAL = "partial"
    UNSUPPORTED = "unsupported"
    NOT_TESTED = "not_tested"


class EngineCapability(BaseModel):
    engine: TargetEngine
    dialect: Optional[str] = None
    maturity: CapabilityMaturity = CapabilityMaturity.NOT_TESTED
    compiler_version: Optional[str] = None
    limitations: Tuple[str, ...] = ()
    verified_test_ids: Tuple[str, ...] = ()


class ActionRisk(str, Enum):
    SAFE = "safe"
    AUDITABLE = "auditable"
    DESTRUCTIVE = "destructive"
    PRIVACY_SENSITIVE = "privacy_sensitive"
    SCHEMA_ALTERING = "schema_altering"


class PreimagePurpose(str, Enum):
    EXECUTION = "execution"
    ROLLBACK = "rollback"
    AUDIT = "audit"
    POSTCONDITION = "postcondition"


class PreimageRetention(str, Enum):
    NONE = "none"
    TEMPORARY = "temporary"
    AUDIT_PERSISTED = "audit_persisted"
    ROLLBACK_PERSISTED = "rollback_persisted"


class PreimageRequirement(BaseModel):
    purposes: Tuple[PreimagePurpose, ...] = ()
    retention: PreimageRetention = PreimageRetention.NONE
    required_columns: Tuple[str, ...] = ()
    retention_policy_id: Optional[str] = None
    encryption_required: bool = False

    @property
    def required_for_execution(self) -> bool:
        return PreimagePurpose.EXECUTION in self.purposes

    @property
    def required_for_rollback(self) -> bool:
        return PreimagePurpose.ROLLBACK in self.purposes

    @property
    def required_for_audit(self) -> bool:
        return PreimagePurpose.AUDIT in self.purposes

    @property
    def required_for_postcondition(self) -> bool:
        return PreimagePurpose.POSTCONDITION in self.purposes


# Backwards compatibility alias
PreImageRequirement = PreimageRequirement


def authorize_engine_capability(
    capability: EngineCapability,
    allow_partial: bool = False,
) -> bool:
    """
    Evaluates whether an engine capability is authorized for production execution.
    Only VERIFIED capabilities are authorized by default.
    PARTIAL is allowed only when explicitly permitted by policy.
    NOT_TESTED and UNSUPPORTED always fail preflight.
    """
    if capability.maturity == CapabilityMaturity.VERIFIED:
        return True
    if allow_partial and capability.maturity == CapabilityMaturity.PARTIAL:
        return True
    return False


class ParseFailurePolicy(str, Enum):
    KEEP_ORIGINAL = "keep_original"
    SET_NULL = "set_null"
    FLAG = "flag"
    QUARANTINE = "quarantine"
    FAIL_STEP = "fail_step"


class PredicateUnknownPolicy(str, Enum):
    PRESERVE_UNKNOWN = "preserve_unknown"
    TREAT_AS_FALSE = "treat_as_false"
    TREAT_AS_TRUE = "treat_as_true"
    FAIL_STEP = "fail_step"
    FLAG_UNKNOWN_SEPARATELY = "flag_unknown_separately"


class InfinityPolicy(str, Enum):
    PRESERVE = "preserve"
    TREAT_AS_NULL = "treat_as_null"
    CLIP_TO_BOUND = "clip_to_bound"
    FAIL_STEP = "fail_step"
    FLAG = "flag"


class UnboundedInfinityPolicy(str, Enum):
    PRESERVE = "preserve"
    TREAT_AS_NULL = "treat_as_null"
    FAIL_STEP = "fail_step"


class ExistingColumnPolicy(str, Enum):
    ERROR = "error"
    OVERWRITE = "overwrite"
    BOOLEAN_OR = "boolean_or"
    MERGE_BOOLEAN_OR = "merge_boolean_or"


class StatisticalImputationStrategy(str, Enum):
    MEAN = "mean"
    MEDIAN = "median"
    MODE = "mode"


class GroupedImputationStrategy(str, Enum):
    GROUPED_MEAN = "grouped_mean"
    GROUPED_MEDIAN = "grouped_median"


class ModeTieBreaker(str, Enum):
    LOWEST_CANONICAL_VALUE = "lowest_canonical_value"
    HIGHEST_CANONICAL_VALUE = "highest_canonical_value"
    FIRST_BY_AUTHORIZED_ORDER = "first_by_authorized_order"
    FAIL_FIT = "fail_fit"


class MissingGroupPolicy(str, Enum):
    USE_GLOBAL_FALLBACK = "use_global_fallback"
    KEEP_NULL = "keep_null"
    FLAG = "flag"
    QUARANTINE = "quarantine"
    FAIL_STEP = "fail_step"


class ParseOutputMode(str, Enum):
    REPLACE_SOURCE_COLUMN = "replace_source_column"
    CREATE_PARSED_COLUMN = "create_parsed_column"
    CREATE_PARSED_AND_RAW_COLUMNS = "create_parsed_and_raw_columns"


class AmbiguousTimePolicy(str, Enum):
    RAISE = "raise"
    EARLIEST = "earliest"
    LATEST = "latest"
    SET_NULL = "set_null"


class NonexistentTimePolicy(str, Enum):
    RAISE = "raise"
    SHIFT_FORWARD = "shift_forward"
    SHIFT_BACKWARD = "shift_backward"
    SET_NULL = "set_null"


class TemporalParseTarget(str, Enum):
    CALENDAR_DATE = "calendar_date"
    TIMESTAMP_WITHOUT_TZ = "timestamp_without_tz"
    TIMESTAMP_WITH_TZ = "timestamp_with_tz"


class NoTransformationReason(str, Enum):
    INTENTIONAL_NULL = "intentional_null"
    INTENTIONAL_SKIP = "intentional_skip"
    SOURCE_SYSTEM_OWNED = "source_system_owned"
    ACCEPTED_EXCEPTION = "accepted_exception"


class PassThroughDecision(BaseModel):
    reason: NoTransformationReason
    supporting_rule_ids: Tuple[str, ...] = ()
    supporting_evidence_ids: Tuple[str, ...] = ()
    approved_by: Optional[str] = None
    policy_decision_id: Optional[str] = None


class TransformationEffects(BaseModel):
    reads: Tuple[str, ...] = ()
    writes: Tuple[str, ...] = ()
    creates: Tuple[str, ...] = ()
    drops: Tuple[str, ...] = ()
    filters_rows: bool = False
    partitions_rows: bool = False


class ActionFamily(str, Enum):
    # The 18 Canonical Action Families
    VALIDATE = "validate"
    QUARANTINE = "quarantine"
    FLAG = "flag"
    MAP_VALUE = "map_value"
    CONDITIONAL_REPLACE = "conditional_replace"
    CLIP = "clip"
    IMPUTE = "impute"
    STANDARDIZE = "standardize"
    PARSE = "parse"
    EXTRACT = "extract"
    DERIVE = "derive"
    FILTER = "filter"
    JOIN = "join"
    AGGREGATE = "aggregate"
    DEDUPLICATE = "deduplicate"
    HASH = "hash"
    MASK = "mask"
    SCHEMA_MUTATION = "schema_mutation"
    # Planning Control Construct
    PASS_THROUGH = "pass_through"


class StepKind(str, Enum):
    ROW_TRANSFORMATION = "row_transformation"
    COLUMN_DERIVATION = "column_derivation"
    DATASET_FILTER = "dataset_filter"
    ASSERTION = "assertion"
    AUDIT_FLAG = "audit_flag"
    CONTROL = "control"
    QUARANTINE_ROUTING = "quarantine_routing"
    SCHEMA_MUTATION = "schema_mutation"


# ============================================================================
# 2. Shared Expression Primitives
# ============================================================================

class Expression(BaseModel):
    """Base model for all typed IR AST expressions."""
    def extract_reads(self) -> List[str]:
        return []

    def to_pandas_code(self, df_var: str = "df", pre_image_var: Optional[str] = None) -> str:
        raise NotImplementedError(f"to_pandas_code not implemented for {self.__class__.__name__}")


class ColumnRef(Expression):
    column_name: str
    dataset_name: Optional[str] = None
    version: ValueVersion = ValueVersion.CURRENT
    referenced_step_id: Optional[str] = None

    @model_validator(mode="after")
    def validate_step_ref(self) -> "ColumnRef":
        if self.version == ValueVersion.AFTER_STEP and not self.referenced_step_id:
            raise ValueError("ColumnRef with ValueVersion.AFTER_STEP must declare referenced_step_id")
        return self

    def extract_reads(self) -> List[str]:
        return [self.column_name] if self.column_name else []

    def to_pandas_code(self, df_var: str = "df", pre_image_var: Optional[str] = None) -> str:
        if self.version == ValueVersion.ORIGINAL and pre_image_var:
            return f"{pre_image_var}['{self.column_name}']"
        return f"{df_var}['{self.column_name}']"


class LiteralValue(Expression):
    value: Any
    data_type: DataType = DataType.ANY

    def to_pandas_code(self, df_var: str = "df", pre_image_var: Optional[str] = None) -> str:
        if self.value is None:
            return "np.nan"
        if isinstance(self.value, str):
            escaped = self.value.replace("'", "\\'")
            return f"'{escaped}'"
        if isinstance(self.value, bool):
            return str(self.value)
        return str(self.value)


class BinaryOperator(str, Enum):
    EQ = "=="
    NEQ = "!="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="
    AND = "and"
    OR = "or"
    ADD = "+"
    SUB = "-"
    MUL = "*"
    DIV = "/"


class BinaryOp(Expression):
    left: Expression
    op: BinaryOperator
    right: Expression

    def extract_reads(self) -> List[str]:
        return self.left.extract_reads() + self.right.extract_reads()

    def to_pandas_code(self, df_var: str = "df", pre_image_var: Optional[str] = None) -> str:
        l_code = self.left.to_pandas_code(df_var, pre_image_var)
        r_code = self.right.to_pandas_code(df_var, pre_image_var)
        op_map = {
            BinaryOperator.AND: "&",
            BinaryOperator.OR: "|",
            BinaryOperator.EQ: "==",
            BinaryOperator.NEQ: "!=",
            BinaryOperator.LT: "<",
            BinaryOperator.LTE: "<=",
            BinaryOperator.GT: ">",
            BinaryOperator.GTE: ">=",
            BinaryOperator.ADD: "+",
            BinaryOperator.SUB: "-",
            BinaryOperator.MUL: "*",
            BinaryOperator.DIV: "/",
        }
        py_op = op_map.get(self.op, str(self.op.value))
        return f"({l_code} {py_op} {r_code})"


class UnaryOperator(str, Enum):
    NOT = "not"
    NEG = "-"


class UnaryOp(Expression):
    op: UnaryOperator
    operand: Expression

    def extract_reads(self) -> List[str]:
        return self.operand.extract_reads()

    def to_pandas_code(self, df_var: str = "df", pre_image_var: Optional[str] = None) -> str:
        op_code = self.operand.to_pandas_code(df_var, pre_image_var)
        if self.op == UnaryOperator.NOT:
            return f"(~{op_code})"
        return f"(-{op_code})"


class NullCheck(Expression):
    operand: Expression
    is_null: bool = True  # True: IS NULL, False: IS NOT NULL

    def extract_reads(self) -> List[str]:
        return self.operand.extract_reads()

    def to_pandas_code(self, df_var: str = "df", pre_image_var: Optional[str] = None) -> str:
        op_code = self.operand.to_pandas_code(df_var, pre_image_var)
        if self.is_null:
            return f"{op_code}.isna()"
        return f"{op_code}.notna()"


class InSetPredicate(Expression):
    operand: Expression
    values: Tuple[LiteralValue, ...]
    negated: bool = False

    def extract_reads(self) -> List[str]:
        return self.operand.extract_reads()

    def to_pandas_code(self, df_var: str = "df", pre_image_var: Optional[str] = None) -> str:
        op_code = self.operand.to_pandas_code(df_var, pre_image_var)
        vals_list = [v.value for v in self.values]
        repr_list = repr(vals_list)
        if self.negated:
            return f"(~{op_code}.isin({repr_list}))"
        return f"{op_code}.isin({repr_list})"


class RegexMatchPredicate(Expression):
    operand: Expression
    pattern: str
    case_sensitive: bool = True

    def extract_reads(self) -> List[str]:
        return self.operand.extract_reads()

    def to_pandas_code(self, df_var: str = "df", pre_image_var: Optional[str] = None) -> str:
        op_code = self.operand.to_pandas_code(df_var, pre_image_var)
        case_flag = "True" if self.case_sensitive else "False"
        escaped_pat = self.pattern.replace("'", "\\'")
        return f"{op_code}.astype('string').str.contains(r'{escaped_pat}', case={case_flag}, na=False, regex=True)"


class ConditionalExpression(Expression):
    condition: Expression
    then_branch: Expression
    else_branch: Expression

    def extract_reads(self) -> List[str]:
        return (
            self.condition.extract_reads()
            + self.then_branch.extract_reads()
            + self.else_branch.extract_reads()
        )

    def to_pandas_code(self, df_var: str = "df", pre_image_var: Optional[str] = None) -> str:
        c = self.condition.to_pandas_code(df_var, pre_image_var)
        t = self.then_branch.to_pandas_code(df_var, pre_image_var)
        e = self.else_branch.to_pandas_code(df_var, pre_image_var)
        return f"np.where({c}, {t}, {e})"


class RuntimeDateReference(BaseModel):
    source: Literal["pipeline_execution_date", "assessment_date", "tenant_business_date"] = "pipeline_execution_date"
    timezone: str = "UTC"


# ============================================================================
# 3. Action Family Expression Models (Discriminated Union)
# ============================================================================

class ValidateExpression(BaseModel):
    """Pure validation/assertion without row partitioning or row drops."""
    expression_type: Literal["validate"] = "validate"
    predicate: Expression
    severity: str = "error"
    rejection_reason_code: Optional[str] = None

    def derive_effects(self, dataset: str) -> TransformationEffects:
        reads = tuple(sorted(set(self.predicate.extract_reads())))
        return TransformationEffects(reads=reads, filters_rows=False, partitions_rows=False)


class QuarantineExpression(BaseModel):
    """Row routing and partitioning into distinct output destinations."""
    expression_type: Literal["quarantine"] = "quarantine"
    predicate: Expression
    routing_mode: Literal["partition", "copy"] = "partition"
    curated_output_ref: str
    quarantine_output_ref: str
    identity_strategy: RowIdentityStrategy = RowIdentityStrategy.EXISTING_KEY
    identity_columns: Tuple[str, ...] = ()
    generated_identity_column: Optional[str] = None
    reason_column: Optional[str] = None
    policy_id: str = "DEFAULT_QUARANTINE_POLICY"
    retention_policy_id: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def handle_aliases(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "accepted_output_ref" in data and "curated_output_ref" not in data:
                data["curated_output_ref"] = data.pop("accepted_output_ref")
            if "rejected_output_ref" in data and "quarantine_output_ref" not in data:
                data["quarantine_output_ref"] = data.pop("rejected_output_ref")
        return data

    @model_validator(mode="after")
    def validate_identity(self) -> "QuarantineExpression":
        if self.identity_strategy == RowIdentityStrategy.GENERATED_OCCURRENCE_ID and not self.generated_identity_column:
            self.generated_identity_column = "_source_row_id"
        return self

    def derive_effects(self, dataset: str) -> TransformationEffects:
        id_cols = list(self.identity_columns)
        if self.generated_identity_column:
            id_cols.append(self.generated_identity_column)
        reads = tuple(sorted(set(self.predicate.extract_reads() + id_cols)))
        creates = (self.reason_column,) if self.reason_column else ()
        is_partition = (self.routing_mode == "partition")
        return TransformationEffects(
            reads=reads,
            creates=creates,
            filters_rows=is_partition,
            partitions_rows=is_partition,
        )


class FlagExpression(BaseModel):
    """Explicit companion audit/indicator column generation without mutating source values."""
    expression_type: Literal["flag"] = "flag"
    predicate: Expression
    output_column: str
    true_value: Any = True
    false_value: Any = False
    unknown_value: Any = None
    unknown_policy: PredicateUnknownPolicy = PredicateUnknownPolicy.PRESERVE_UNKNOWN
    existing_column_policy: ExistingColumnPolicy = ExistingColumnPolicy.ERROR

    @model_validator(mode="after")
    def validate_flag(self) -> "FlagExpression":
        if not self.output_column or not self.output_column.strip():
            raise ValueError("FlagExpression requires a non-empty output_column")
        return self

    def derive_effects(self, dataset: str) -> TransformationEffects:
        reads = tuple(sorted(set(self.predicate.extract_reads())))
        return TransformationEffects(
            reads=reads,
            creates=(self.output_column,),
            filters_rows=False,
            partitions_rows=False,
        )


class ClipExpression(BaseModel):
    """Numerical clamping between lower and/or upper bounds."""
    expression_type: Literal["clip"] = "clip"
    target_column: ColumnRef
    lower_bound: Optional[LiteralValue] = None
    upper_bound: Optional[LiteralValue] = None
    iqr_multiplier: Optional[float] = None
    null_policy: ClipNullPolicy = ClipNullPolicy.PRESERVE
    infinity_policy: InfinityPolicy = InfinityPolicy.CLIP_TO_BOUND
    unbounded_infinity_policy: UnboundedInfinityPolicy = UnboundedInfinityPolicy.PRESERVE

    @model_validator(mode="after")
    def validate_bounds(self) -> "ClipExpression":
        if self.lower_bound is None and self.upper_bound is None and self.iqr_multiplier is None:
            raise ValueError("ClipExpression must declare lower_bound, upper_bound, or iqr_multiplier")
        if self.lower_bound is not None and self.upper_bound is not None:
            l_val = self.lower_bound.value
            u_val = self.upper_bound.value
            if isinstance(l_val, (int, float)) and isinstance(u_val, (int, float)):
                if l_val > u_val:
                    raise ValueError(f"lower_bound ({l_val}) cannot exceed upper_bound ({u_val})")
        return self

    def derive_effects(self, dataset: str) -> TransformationEffects:
        col = self.target_column.column_name
        return TransformationEffects(
            reads=(col,) if col else (),
            writes=(col,) if col else (),
            filters_rows=False,
            partitions_rows=False,
        )


class ScalarMapOperation(str, Enum):
    TRIM = "trim"
    LOWER = "lower"
    UPPER = "upper"
    CAST = "cast"
    PARSE_NUMBER = "parse_number"
    PARSE_DATE = "parse_date"
    STANDARDIZE_BOOLEAN = "standardize_boolean"
    REGEX_SUBSTITUTE = "regex_substitute"
    LOOKUP_MAP = "lookup_map"
    NORMALIZE_PHONE = "normalize_phone"
    SANITIZE_EMAIL = "sanitize_email"


class ScalarMapExpression(BaseModel):
    """Typed 1-to-1 deterministic scalar mapping with typed operation schemas."""
    expression_type: Literal["map_value"] = "map_value"
    source_column: ColumnRef
    output_column: str
    operation: ScalarMapOperation
    parameters: Dict[str, Any] = Field(default_factory=dict)
    failure_policy: Optional[ParseFailurePolicy] = None
    unmapped_policy: UnmappedValuePolicy = UnmappedValuePolicy.KEEP_ORIGINAL
    preserve_input_type: bool = False

    @model_validator(mode="after")
    def validate_operation_parameters(self) -> "ScalarMapExpression":
        if self.operation == ScalarMapOperation.REGEX_SUBSTITUTE:
            if "pattern" not in self.parameters or "replacement" not in self.parameters:
                raise ValueError("REGEX_SUBSTITUTE requires 'pattern' and 'replacement' parameters")
        elif self.operation == ScalarMapOperation.LOOKUP_MAP:
            if "mapping" not in self.parameters:
                raise ValueError("LOOKUP_MAP requires 'mapping' parameter")
        elif self.operation == ScalarMapOperation.CAST:
            if "target_type" not in self.parameters:
                raise ValueError("CAST requires 'target_type' parameter")
        return self

    def derive_effects(self, dataset: str) -> TransformationEffects:
        in_col = self.source_column.column_name
        out_col = self.output_column
        writes = (out_col,) if out_col == in_col and out_col else ()
        creates = (out_col,) if out_col != in_col and out_col else ()
        reads = (in_col,) if in_col else ()
        return TransformationEffects(
            reads=reads,
            writes=writes,
            creates=creates,
            filters_rows=False,
            partitions_rows=False,
        )


class ConditionalReplaceExpression(BaseModel):
    """Condition-driven value substitution."""
    expression_type: Literal["conditional_replace"] = "conditional_replace"
    target_column: ColumnRef
    condition: Expression
    replace_value: Expression
    output_column: Optional[str] = None
    unknown_policy: PredicateUnknownPolicy = PredicateUnknownPolicy.TREAT_AS_FALSE
    unknown_value: Optional[Expression] = None

    def derive_effects(self, dataset: str) -> TransformationEffects:
        in_col = self.target_column.column_name
        out_col = self.output_column or in_col
        writes = (out_col,) if out_col == in_col and out_col else ()
        creates = (out_col,) if out_col != in_col and out_col else ()
        reads = tuple(sorted(set([in_col] + self.condition.extract_reads() + self.replace_value.extract_reads())))
        return TransformationEffects(
            reads=reads,
            writes=writes,
            creates=creates,
            filters_rows=False,
            partitions_rows=False,
        )


class GroupKeyValue(BaseModel):
    model_config = ConfigDict(frozen=True)
    column: str
    data_type: str
    value: Any


class GroupedImputationValue(BaseModel):
    model_config = ConfigDict(frozen=True)
    group_key: Tuple[GroupKeyValue, ...]
    learned_value: Any


def compute_model_snapshot_hash(
    strategy: str,
    learned_value: Any,
    group_values: Tuple[GroupedImputationValue, ...],
    fit_population_hash: str,
    fit_schema_hash: str,
    target_column_type: str = "",
    group_columns: Tuple[str, ...] = (),
    tie_breaker: str = "",
    global_fallback_value: Any = None,
    missing_group_policy: str = "",
    null_exclusion_policy: str = "exclude_nulls",
    model_version: str = "1.0.0",
    canonicalization_version: str = "1.0.0",
) -> str:
    """Computes a deterministic cryptographic fingerprint of a learned imputation model."""
    serialized_groups = []
    for gv in group_values:
        serialized_key = sorted(
            [{"column": k.column, "type": k.data_type, "val": str(k.value)} for k in gv.group_key],
            key=lambda x: x["column"]
        )
        serialized_groups.append({
            "key": serialized_key,
            "val": str(gv.learned_value)
        })
    serialized_groups.sort(key=lambda g: json.dumps(g["key"], sort_keys=True))

    payload = {
        "strategy": strategy,
        "learned_value": str(learned_value) if learned_value is not None else None,
        "group_values": serialized_groups,
        "fit_population_hash": fit_population_hash,
        "fit_schema_hash": fit_schema_hash,
        "target_column_type": target_column_type,
        "group_columns": sorted(list(group_columns)),
        "tie_breaker": tie_breaker,
        "global_fallback_value": str(global_fallback_value) if global_fallback_value is not None else None,
        "missing_group_policy": str(missing_group_policy),
        "null_exclusion_policy": str(null_exclusion_policy),
        "model_version": model_version,
        "canonicalization_version": canonicalization_version,
    }
    raw = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class ImputationModelIntegrityError(ValueError):
    """Raised when an imputation model's snapshot hash does not match its computed hash or is tampered with."""
    pass


def verify_imputation_model(model: "LearnedImputationModel") -> None:
    """Use-time verification that an imputation model has not been tampered with."""
    expected_hash = compute_model_snapshot_hash(
        strategy=model.strategy,
        learned_value=model.learned_value,
        group_values=model.group_values,
        fit_population_hash=model.fit_population_hash,
        fit_schema_hash=model.fit_schema_hash,
        target_column_type=model.target_column_type,
        group_columns=model.group_columns,
        tie_breaker=model.tie_breaker.value if hasattr(model.tie_breaker, "value") else str(model.tie_breaker),
        global_fallback_value=model.global_fallback_value,
        missing_group_policy=model.missing_group_policy.value if hasattr(model.missing_group_policy, "value") else str(model.missing_group_policy),
        null_exclusion_policy=model.null_exclusion_policy,
        model_version=model.model_version,
        canonicalization_version=model.canonicalization_version,
    )
    if model.model_snapshot_hash and model.model_snapshot_hash != expected_hash:
        raise ImputationModelIntegrityError(
            f"Imputation model snapshot mismatch for {model.model_id}: expected {expected_hash}, got {model.model_snapshot_hash}"
        )


class LearnedImputationModel(BaseModel):
    """Immutable, versioned, cryptographic snapshot of learned statistics from authorized population."""
    model_config = ConfigDict(frozen=True)

    model_id: str
    model_version: str = "1.0.0"
    strategy: str  # StatisticalImputationStrategy or GroupedImputationStrategy or "constant"

    learned_value: Optional[Any] = None
    group_values: Tuple[GroupedImputationValue, ...] = ()
    group_columns: Tuple[str, ...] = ()

    missing_group_policy: MissingGroupPolicy = MissingGroupPolicy.KEEP_NULL
    global_fallback_value: Optional[Any] = None

    fit_dataset_ref: str = ""
    fit_population_hash: str = ""
    fit_schema_hash: str = ""
    fit_rule_id: Optional[str] = None
    fit_policy_decision_id: str = ""

    fitted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    fitted_by: str = "agent_dhara_fit_engine"

    tie_breaker: ModeTieBreaker = ModeTieBreaker.LOWEST_CANONICAL_VALUE

    target_column_type: str = ""
    null_exclusion_policy: str = "exclude_nulls"
    group_key_normalization_rules: str = "v1"

    canonicalization_scheme: str = "v1"
    canonicalization_version: str = "1.0.0"
    model_snapshot_hash: str = ""

    @model_validator(mode="after")
    def validate_model(self) -> "LearnedImputationModel":
        if self.missing_group_policy == MissingGroupPolicy.USE_GLOBAL_FALLBACK and self.global_fallback_value is None:
            raise ValueError("USE_GLOBAL_FALLBACK requires global_fallback_value to be provided")

        seen_keys = set()
        for gv in self.group_values:
            key_repr = tuple(sorted((k.column, str(k.value)) for k in gv.group_key))
            if key_repr in seen_keys:
                raise ValueError(f"Duplicate group key detected in LearnedImputationModel: {key_repr}")
            seen_keys.add(key_repr)
            if self.group_columns:
                key_cols = set(k.column for k in gv.group_key)
                if key_cols != set(self.group_columns):
                    raise ValueError(f"Group key columns {key_cols} do not match model group_columns {set(self.group_columns)}")

        expected_hash = compute_model_snapshot_hash(
            strategy=self.strategy,
            learned_value=self.learned_value,
            group_values=self.group_values,
            fit_population_hash=self.fit_population_hash,
            fit_schema_hash=self.fit_schema_hash,
            target_column_type=self.target_column_type,
            group_columns=self.group_columns,
            tie_breaker=self.tie_breaker.value if hasattr(self.tie_breaker, "value") else str(self.tie_breaker),
            global_fallback_value=self.global_fallback_value,
            missing_group_policy=self.missing_group_policy.value if hasattr(self.missing_group_policy, "value") else str(self.missing_group_policy),
            null_exclusion_policy=self.null_exclusion_policy,
            model_version=self.model_version,
            canonicalization_version=self.canonicalization_version,
        )
        if self.model_snapshot_hash and self.model_snapshot_hash != expected_hash:
            raise ImputationModelIntegrityError(f"Model snapshot hash mismatch: provided {self.model_snapshot_hash} != expected {expected_hash}")
        if not self.model_snapshot_hash:
            object.__setattr__(self, "model_snapshot_hash", expected_hash)

        return self


class ImputeExpression(BaseModel):
    """Statistical/policy value imputation with fit/apply separation and pre-fill indicators."""
    expression_type: Literal["impute"] = "impute"
    target_column: ColumnRef
    strategy: str  # "constant", "mean", "median", "mode", "grouped_mean", "grouped_median"
    constant_value: Optional[Any] = None
    learned_model: Optional[LearnedImputationModel] = None
    group_by_columns: Tuple[str, ...] = ()
    output_column: Optional[str] = None
    emit_indicator: bool = False
    indicator_column: Optional[str] = None
    existing_indicator_policy: ExistingColumnPolicy = ExistingColumnPolicy.ERROR
    policy_id: Optional[str] = None

    @model_validator(mode="after")
    def validate_impute(self) -> "ImputeExpression":
        stat_strategies = {"mean", "median", "mode", "grouped_mean", "grouped_median"}
        if self.strategy in stat_strategies:
            if self.learned_model is None:
                raise ValueError(f"Statistical imputation strategy '{self.strategy}' requires an authorized learned_model")
        elif self.strategy == "constant":
            if self.constant_value is None and (self.learned_model is None or self.learned_model.learned_value is None):
                raise ValueError("Constant imputation requires constant_value or learned_model with learned_value")
        if self.emit_indicator and not self.indicator_column:
            self.indicator_column = f"{self.target_column.column_name}_was_imputed"
        return self

    def derive_effects(self, dataset: str) -> TransformationEffects:
        in_col = self.target_column.column_name
        out_col = self.output_column or in_col
        writes = (out_col,) if out_col == in_col and out_col else ()
        creates_list = []
        if out_col != in_col and out_col:
            creates_list.append(out_col)
        if self.emit_indicator and self.indicator_column:
            creates_list.append(self.indicator_column)
        reads = tuple(sorted(set([in_col] + list(self.group_by_columns))))
        return TransformationEffects(
            reads=reads,
            writes=writes,
            creates=tuple(creates_list),
            filters_rows=False,
            partitions_rows=False,
        )


class NumericFormatSpec(BaseModel):
    decimal_separator: Literal[".", ","] = "."
    thousands_separator: Optional[str] = None
    currency_symbols: Tuple[str, ...] = ()
    allow_parentheses_negative: bool = False
    allow_leading_sign: bool = True
    allow_scientific_notation: bool = False
    allow_percent: bool = False
    percent_scale: Optional[Literal["fraction", "whole"]] = None
    trim_whitespace: bool = True
    target_type: Literal["Int64", "Float64", "decimal"] = "Float64"
    decimal_precision: Optional[int] = None
    decimal_scale: Optional[int] = None

    @model_validator(mode="after")
    def validate_spec(self) -> "NumericFormatSpec":
        if self.thousands_separator is not None and self.thousands_separator == self.decimal_separator:
            raise ValueError("Decimal and thousands separators must differ")
        if self.allow_percent and self.percent_scale is None:
            raise ValueError("allow_percent=True requires percent_scale to be 'fraction' or 'whole'")
        return self


class DateFormatSpec(BaseModel):
    accepted_formats: Tuple[str, ...] = ("%Y-%m-%d",)
    input_timezone: Optional[str] = None
    output_timezone: str = "UTC"
    dayfirst: bool = False
    yearfirst: bool = False
    ambiguous_time_policy: AmbiguousTimePolicy = AmbiguousTimePolicy.RAISE
    nonexistent_time_policy: NonexistentTimePolicy = NonexistentTimePolicy.RAISE
    output_type: Literal["date", "timestamp"] = "date"
    temporal_target: TemporalParseTarget = TemporalParseTarget.CALENDAR_DATE


class BooleanFormatSpec(BaseModel):
    true_tokens: frozenset[str] = frozenset({"true", "1", "yes", "y", "t", "si"})
    false_tokens: frozenset[str] = frozenset({"false", "0", "no", "n", "f"})
    case_sensitive: bool = False
    trim_whitespace: bool = True
    output_dtype: Literal["boolean"] = "boolean"

    @model_validator(mode="after")
    def validate_tokens(self) -> "BooleanFormatSpec":
        def norm(t: str) -> str:
            val = t.strip() if self.trim_whitespace else t
            return val.lower() if not self.case_sensitive else val

        norm_true = {norm(t) for t in self.true_tokens}
        norm_false = {norm(t) for t in self.false_tokens}
        overlap = norm_true & norm_false
        if overlap:
            raise ValueError(f"Normalized true tokens and false tokens must be disjoint, found collision: {overlap}")
        return self


class ParseExpression(BaseModel):
    """Typed parsing from string/raw representations into typed primitives."""
    expression_type: Literal["parse"] = "parse"
    source_column: ColumnRef
    target_column: Optional[str] = None
    output_mode: ParseOutputMode = ParseOutputMode.REPLACE_SOURCE_COLUMN
    failure_policy: ParseFailurePolicy = ParseFailurePolicy.SET_NULL
    target_data_type: DataType = DataType.FLOAT
    numeric_spec: Optional[NumericFormatSpec] = None
    date_spec: Optional[DateFormatSpec] = None
    boolean_spec: Optional[BooleanFormatSpec] = None

    @model_validator(mode="after")
    def validate_parse_expression(self) -> "ParseExpression":
        out_col = self.target_column or self.source_column.column_name
        if self.failure_policy == ParseFailurePolicy.KEEP_ORIGINAL:
            if self.output_mode == ParseOutputMode.REPLACE_SOURCE_COLUMN and self.target_data_type not in (DataType.STRING, DataType.ANY):
                raise ValueError(
                    "KEEP_ORIGINAL is prohibited for in-place replacement of typed non-string columns (would create mixed/object type). "
                    "Use ParseOutputMode.CREATE_PARSED_COLUMN or SET_NULL/FLAG."
                )
        return self

    def derive_effects(self, dataset: str) -> TransformationEffects:
        in_col = self.source_column.column_name
        out_col = self.target_column or in_col
        writes = (out_col,) if out_col == in_col and out_col else ()
        creates_list = []
        if out_col != in_col and out_col:
            creates_list.append(out_col)
        if self.failure_policy == ParseFailurePolicy.FLAG:
            creates_list.append(f"{out_col}_is_parse_error")
        return TransformationEffects(
            reads=(in_col,) if in_col else (),
            writes=writes,
            creates=tuple(creates_list),
            filters_rows=False,
            partitions_rows=False,
        )


class PassThroughExpression(BaseModel):
    """Explicit intentional non-mutation record (classified as StepKind.CONTROL)."""
    expression_type: Literal["pass_through"] = "pass_through"
    target_column: Optional[ColumnRef] = None
    reason: NoTransformationReason = NoTransformationReason.INTENTIONAL_SKIP
    reason_code: str = "intentional_non_mutation"
    preserve_original: Literal[True] = True
    residual_risk: Optional[str] = None

    def derive_effects(self, dataset: str) -> TransformationEffects:
        col = self.target_column.column_name if self.target_column else None
        reads = (col,) if col else ()
        return TransformationEffects(reads=reads, writes=(), creates=(), drops=(), filters_rows=False, partitions_rows=False)


class HashExpression(BaseModel):
    """Cryptographic one-way hash transformation."""
    expression_type: Literal["hash"] = "hash"
    input_column: ColumnRef
    output_column: str
    algorithm: str = "sha256"
    salt_reference: Optional[str] = None
    preserve_original: bool = False
    reversible: Literal[False] = False
    security_class: str = "one_way_pseudonymization"

    def derive_effects(self, dataset: str) -> TransformationEffects:
        in_col = self.input_column.column_name
        out_col = self.output_column
        writes = (out_col,) if out_col == in_col else ()
        creates = (out_col,) if out_col != in_col else ()
        return TransformationEffects(
            reads=(in_col,) if in_col else (),
            writes=writes,
            creates=creates,
            filters_rows=False,
            partitions_rows=False,
        )


class MaskStrategy(str, Enum):
    REDACT_ALL = "redact_all"
    PRESERVE_LAST_N = "preserve_last_n"
    PARTIAL_REDACTION = "partial_redaction"
    TOKENIZE = "tokenize"


class MaskExpression(BaseModel):
    """Data masking/redaction transformation."""
    expression_type: Literal["mask"] = "mask"
    input_column: ColumnRef
    output_column: str
    strategy: MaskStrategy = MaskStrategy.PRESERVE_LAST_N
    preserve_last_n: Optional[int] = 4
    mask_character: str = "*"
    token_vault_policy_id: Optional[str] = None
    reversible: bool = False
    security_class: str = "redaction"

    @model_validator(mode="before")
    @classmethod
    def handle_legacy_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "masking_strategy" in data and "strategy" not in data:
                strat = str(data.pop("masking_strategy"))
                if "last" in strat:
                    data["strategy"] = MaskStrategy.PRESERVE_LAST_N
                elif "all" in strat:
                    data["strategy"] = MaskStrategy.REDACT_ALL
                elif "token" in strat:
                    data["strategy"] = MaskStrategy.TOKENIZE
                else:
                    data["strategy"] = MaskStrategy.PARTIAL_REDACTION
            if "visible_suffix_length" in data and "preserve_last_n" not in data:
                data["preserve_last_n"] = data.pop("visible_suffix_length")
        return data

    def derive_effects(self, dataset: str) -> TransformationEffects:
        in_col = self.input_column.column_name
        out_col = self.output_column
        writes = (out_col,) if out_col == in_col else ()
        creates = (out_col,) if out_col != in_col else ()
        return TransformationEffects(
            reads=(in_col,) if in_col else (),
            writes=writes,
            creates=creates,
            filters_rows=False,
            partitions_rows=False,
        )


class SchemaMutationExpression(BaseModel):
    """Schema mutation: dropping or project-excluding columns."""
    expression_type: Literal["schema_mutation"] = "schema_mutation"
    mutation_kind: Literal["drop_column", "project_exclude"]
    target_column: str
    dataset_name: Optional[str] = None
    reversible: Literal[False] = False

    def derive_effects(self, dataset: str) -> TransformationEffects:
        drops = (self.target_column,) if self.mutation_kind == "drop_column" else ()
        return TransformationEffects(
            reads=(),
            writes=(),
            creates=(),
            drops=drops,
            filters_rows=False,
            partitions_rows=False,
        )


TransformationExpression = Annotated[
    Union[
        ValidateExpression,
        QuarantineExpression,
        FlagExpression,
        ClipExpression,
        ScalarMapExpression,
        ConditionalReplaceExpression,
        ImputeExpression,
        ParseExpression,
        PassThroughExpression,
        HashExpression,
        MaskExpression,
        SchemaMutationExpression,
    ],
    Field(discriminator="expression_type"),
]


# ============================================================================
# 4. Effect Derivation & Schema Binding (Layer 2 Validation)
# ============================================================================

def canonical_effects_dump(eff: TransformationEffects) -> str:
    return json.dumps({
        "reads": sorted(list(eff.reads)),
        "writes": sorted(list(eff.writes)),
        "creates": sorted(list(eff.creates)),
        "drops": sorted(list(eff.drops)),
        "filters_rows": eff.filters_rows,
        "partitions_rows": eff.partitions_rows,
    }, sort_keys=True)


class EffectDerivationContext(BaseModel):
    input_schemas: Dict[str, Any] = Field(default_factory=dict)
    output_naming_policy: str = "standard"
    target_engine: Optional[TargetEngine] = None
    catalogue_version: str = "1.0.0"


def derive_effects(
    expression: TransformationExpression,
    dataset: str,
    target_column: Optional[str] = None,
    context: Optional[EffectDerivationContext] = None,
) -> TransformationEffects:
    """Deterministically derives data effects from expression semantics."""
    if hasattr(expression, "derive_effects"):
        return expression.derive_effects(dataset)
    return TransformationEffects()


class DatasetColumnSchema(BaseModel):
    name: str
    data_type: DataType = DataType.STRING
    nullable: bool = True


class DatasetSchema(BaseModel):
    dataset_name: str
    columns: Dict[str, DatasetColumnSchema] = Field(default_factory=dict)


class BoundTransformationExpression(BaseModel):
    expression: TransformationExpression
    dataset_name: str
    read_columns: Tuple[str, ...] = ()
    written_columns: Tuple[str, ...] = ()
    created_columns: Tuple[str, ...] = ()
    dropped_columns: Tuple[str, ...] = ()


def bind_expression_to_schema(
    expression: TransformationExpression,
    dataset_name: str,
    schemas: Mapping[str, DatasetSchema],
    ancestor_step_ids: Tuple[str, ...] = (),
) -> BoundTransformationExpression:
    """
    Layer 2 schema binding: validates expression against runtime dataset schema.
    Checks column existence, type compatibility, name collisions, and AFTER_STEP lineage.
    """
    if dataset_name not in schemas:
        raise TransformationIRInvariantError(f"Dataset '{dataset_name}' not found in schemas")
    ds_schema = schemas[dataset_name]

    eff = derive_effects(expression, dataset_name)

    # Check column existence for reads
    for r in eff.reads:
        if r not in ds_schema.columns:
            raise TransformationIRInvariantError(f"Column '{r}' does not exist in dataset '{dataset_name}'")

    # For ClipExpression: target column must be numeric
    if isinstance(expression, ClipExpression):
        col_name = expression.target_column.column_name
        if col_name in ds_schema.columns:
            col_type = ds_schema.columns[col_name].data_type
            if col_type not in (DataType.INTEGER, DataType.FLOAT, DataType.DECIMAL, DataType.ANY):
                raise TransformationIRInvariantError(
                    f"Target column '{col_name}' in ClipExpression must be numeric, got {col_type.value}"
                )

    # For FlagExpression: check collision with ExistingColumnPolicy.ERROR
    if isinstance(expression, FlagExpression):
        out_col = expression.output_column
        if out_col in ds_schema.columns and expression.existing_column_policy == ExistingColumnPolicy.ERROR:
            raise TransformationIRInvariantError(
                f"Flag output column '{out_col}' already exists in schema (policy=ERROR)"
            )

    # For ImputeExpression: target column numeric check for mean/median, indicator collision check
    if isinstance(expression, ImputeExpression):
        t_col = expression.target_column.column_name
        if t_col in ds_schema.columns:
            t_type = ds_schema.columns[t_col].data_type
            if expression.strategy in ("mean", "median", "grouped_mean", "grouped_median"):
                if t_type not in (DataType.INTEGER, DataType.FLOAT, DataType.DECIMAL, DataType.ANY):
                    raise TransformationIRInvariantError(
                        f"Statistical imputation target '{t_col}' must be numeric, got {t_type.value}"
                    )
        if expression.emit_indicator and expression.indicator_column:
            ind_col = expression.indicator_column
            if ind_col in ds_schema.columns and expression.existing_indicator_policy == ExistingColumnPolicy.ERROR:
                raise TransformationIRInvariantError(
                    f"Imputation indicator column '{ind_col}' already exists in schema (policy=ERROR)"
                )

    # For ParseExpression: source column check and collision check
    if isinstance(expression, ParseExpression):
        out_col = expression.target_column or expression.source_column.column_name
        if expression.output_mode == ParseOutputMode.CREATE_PARSED_COLUMN:
            if out_col in ds_schema.columns:
                raise TransformationIRInvariantError(
                    f"Parsed output column '{out_col}' already exists in schema (output_mode=CREATE_PARSED_COLUMN)"
                )

    # Check AFTER_STEP references
    def _check_after_step(obj: Any):
        if isinstance(obj, ColumnRef) and obj.version == ValueVersion.AFTER_STEP:
            if obj.referenced_step_id not in ancestor_step_ids:
                raise TransformationIRInvariantError(
                    f"Referenced step '{obj.referenced_step_id}' is not in ancestor steps {ancestor_step_ids}"
                )
        elif isinstance(obj, BaseModel):
            for v in obj.__dict__.values():
                _check_after_step(v)
        elif isinstance(obj, (list, tuple)):
            for it in obj:
                _check_after_step(it)

    _check_after_step(expression)

    return BoundTransformationExpression(
        expression=expression,
        dataset_name=dataset_name,
        read_columns=eff.reads,
        written_columns=eff.writes,
        created_columns=eff.creates,
        dropped_columns=eff.drops,
    )


# ============================================================================
# 5. Canonical Transformation Step IR Model & Graph Envelope
# ============================================================================

class TransformationStepIR(BaseModel):
    step_id: str
    dataset_name: str
    family: ActionFamily
    kind: StepKind
    expression: TransformationExpression
    target_column: Optional[str] = None
    persisted_effects: Optional[TransformationEffects] = None
    pre_image: PreImageRequirement = Field(default_factory=PreImageRequirement)
    prerequisites: Tuple[str, ...] = ()
    required_postconditions: Tuple[str, ...] = ()
    supporting_rule_ids: Tuple[str, ...] = ()
    supporting_evidence_ids: Tuple[str, ...] = ()
    legacy_action_name: Optional[str] = None
    risk_class: ActionRisk = ActionRisk.SAFE
    idempotent: bool = True
    deterministic: bool = True
    reversible: bool = False

    @model_validator(mode="before")
    @classmethod
    def handle_caller_effects(cls, data: Any) -> Any:
        if isinstance(data, dict) and "effects" in data:
            data["persisted_effects"] = data.pop("effects")
        return data

    @model_validator(mode="after")
    def validate_effects_invariant(self) -> "TransformationStepIR":
        _ = self.effects
        return self

    @computed_field
    @property
    def effects(self) -> TransformationEffects:
        derived = derive_effects(self.expression, self.dataset_name, self.target_column)
        if self.persisted_effects is not None:
            if canonical_effects_dump(self.persisted_effects) != canonical_effects_dump(derived):
                raise TransformationIRInvariantError(
                    f"Declared effects do not match expression semantics for step {self.step_id}"
                )
        return derived

    @property
    def reads(self) -> Tuple[str, ...]:
        return self.effects.reads

    @property
    def writes(self) -> Tuple[str, ...]:
        return tuple(sorted(set(self.effects.writes + self.effects.creates)))

    @property
    def expression_hash(self) -> str:
        """Deterministic semantic fingerprint of engine-neutral transformation behavior (independent of dataset)."""
        data = {
            "ir_version": TRANSFORMATION_IR_VERSION,
            "family": self.family.value if hasattr(self.family, "value") else str(self.family),
            "expression": self.expression.model_dump(mode="json"),
        }
        dumped = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(dumped.encode("utf-8")).hexdigest()

    @property
    def bound_transformation_hash(self) -> str:
        """Deterministic fingerprint of transformation bound to dataset and target column."""
        data = {
            "expression_hash": self.expression_hash,
            "dataset": self.dataset_name,
            "target_column": self.target_column,
        }
        dumped = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(dumped.encode("utf-8")).hexdigest()

    @property
    def transformation_hash(self) -> str:
        """Deterministic semantic fingerprint of the transformation itself (excluding graph context)."""
        return self.bound_transformation_hash

    @property
    def plan_node_hash(self) -> str:
        """Deterministic fingerprint of the plan node (including prerequisites and postconditions)."""
        data = {
            "transformation_hash": self.transformation_hash,
            "prerequisites": sorted(list(self.prerequisites)),
            "postconditions": sorted(list(self.required_postconditions)),
        }
        dumped = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(dumped.encode("utf-8")).hexdigest()

    @property
    def semantic_hash(self) -> str:
        """Backwards compatibility alias for transformation_hash."""
        return self.transformation_hash


class IRDependency(BaseModel):
    source_step_id: str
    target_step_id: str
    kind: Literal["prerequisite", "pre_image", "schema_parent"] = "prerequisite"


class TransformationIRGraph(BaseModel):
    graph_id: str
    ir_version: str = TRANSFORMATION_IR_VERSION
    action_catalogue_version: str = "1.0.0"
    schema_snapshot_hash: str = ""
    policy_version: str = "1.0.0"
    steps: Tuple[TransformationStepIR, ...]
    dependencies: Tuple[IRDependency, ...] = ()

    @model_validator(mode="after")
    def validate_graph(self) -> "TransformationIRGraph":
        step_dict = {s.step_id: s for s in self.steps}
        step_ids = set(step_dict.keys())
        if len(step_ids) != len(self.steps):
            raise TransformationIRInvariantError("Duplicate step_id found in TransformationIRGraph")
        for dep in self.dependencies:
            if dep.source_step_id not in step_ids:
                raise TransformationIRInvariantError(f"Dependency source '{dep.source_step_id}' not found in graph")
            if dep.target_step_id not in step_ids:
                raise TransformationIRInvariantError(f"Dependency target '{dep.target_step_id}' not found in graph")
            if dep.source_step_id == dep.target_step_id:
                raise TransformationIRInvariantError(f"Self-dependency detected on step '{dep.source_step_id}'")
        # Cycle check
        adj: Dict[str, List[str]] = {sid: [] for sid in step_ids}
        for dep in self.dependencies:
            adj[dep.source_step_id].append(dep.target_step_id)
        visited: Dict[str, int] = {}
        def dfs(node: str):
            visited[node] = 1
            for neighbor in adj[node]:
                if visited.get(neighbor) == 1:
                    raise TransformationIRInvariantError(f"Cycle detected in TransformationIRGraph: {node} -> {neighbor}")
                if neighbor not in visited:
                    dfs(neighbor)
            visited[node] = 2
        for sid in step_ids:
            if sid not in visited:
                dfs(sid)

        # Transitive ancestor calculation for strict AFTER_STEP verification
        ancestor_map: Dict[str, Set[str]] = {sid: set() for sid in step_ids}
        # Invert edges: who is upstream of target?
        upstream_adj: Dict[str, List[str]] = {sid: [] for sid in step_ids}
        for dep in self.dependencies:
            upstream_adj[dep.target_step_id].append(dep.source_step_id)

        def get_ancestors(node: str, memo: Dict[str, Set[str]]) -> Set[str]:
            if node in memo:
                return memo[node]
            res = set()
            for parent in upstream_adj[node]:
                res.add(parent)
                res.update(get_ancestors(parent, memo))
            memo[node] = res
            return res

        memo_ancestors: Dict[str, Set[str]] = {}
        for sid in step_ids:
            ancestor_map[sid] = get_ancestors(sid, memo_ancestors)

        # Strict AFTER_STEP DAG enforcement
        def check_after_step(obj: Any, consumer_step_id: str):
            if isinstance(obj, ColumnRef) and obj.version == ValueVersion.AFTER_STEP:
                ref_step_id = obj.referenced_step_id
                if not ref_step_id:
                    raise TransformationIRInvariantError(f"AFTER_STEP ColumnRef on step '{consumer_step_id}' missing referenced_step_id")
                if ref_step_id not in step_dict:
                    raise TransformationIRInvariantError(f"AFTER_STEP references step '{ref_step_id}' which does not exist in graph")
                if ref_step_id not in ancestor_map[consumer_step_id]:
                    raise TransformationIRInvariantError(
                        f"AFTER_STEP references producer '{ref_step_id}' which is not a DAG ancestor of consumer '{consumer_step_id}'"
                    )
                producer = step_dict[ref_step_id]
                col_name = obj.column_name
                if col_name not in producer.writes:
                    raise TransformationIRInvariantError(
                        f"Producer '{ref_step_id}' does not write/create referenced column '{col_name}' for AFTER_STEP consumer '{consumer_step_id}'"
                    )
            elif isinstance(obj, BaseModel):
                for v in obj.__dict__.values():
                    check_after_step(v, consumer_step_id)
            elif isinstance(obj, (list, tuple)):
                for it in obj:
                    check_after_step(it, consumer_step_id)

        for step in self.steps:
            check_after_step(step.expression, step.step_id)

        return self

    @property
    def graph_hash(self) -> str:
        data = {
            "ir_version": self.ir_version,
            "action_catalogue_version": self.action_catalogue_version,
            "schema_snapshot_hash": self.schema_snapshot_hash,
            "policy_version": self.policy_version,
            "steps": [s.plan_node_hash for s in self.steps],
            "dependencies": [dep.model_dump() for dep in self.dependencies],
        }
        dumped = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


# ============================================================================
# 6. Legacy Action Migration Ledger & Inventory Records
# ============================================================================

class LegacyActionMigrationRecord(BaseModel):
    legacy_action_name: str
    disposition: MigrationDisposition

    canonical_family: Optional[ActionFamily] = None
    expression_model: Optional[str] = None
    canonical_action_name: Optional[str] = None
    alias_of: Optional[str] = None

    adapter_version: Optional[str] = None
    limitations: Tuple[str, ...] = ()
    deprecation_target: Optional[str] = None

    reason_code: str = "standard_catalog_mapping"
    replacement_action: Optional[str] = None
    removal_target: Optional[str] = None
    manual_review_owner: Optional[str] = None

    @model_validator(mode="after")
    def validate_disposition(self) -> "LegacyActionMigrationRecord":
        if self.disposition == MigrationDisposition.MAPPED_TO_IR:
            if not self.canonical_family or not self.expression_model:
                raise ValueError("MAPPED_TO_IR requires family and expression model")
        elif self.disposition == MigrationDisposition.COMPOSITE_IR:
            if not self.expression_model:
                raise ValueError("COMPOSITE_IR requires composite graph definition")
        elif self.disposition == MigrationDisposition.PASS_THROUGH_DECISION:
            if self.canonical_family is not None:
                raise ValueError("PASS_THROUGH_DECISION is a planning control construct, not an executable family")
        elif self.disposition == MigrationDisposition.DEPRECATED:
            if not self.replacement_action or not self.removal_target:
                raise ValueError("DEPRECATED requires replacement and removal target")
        elif self.disposition == MigrationDisposition.MANUAL_REVIEW:
            if not self.manual_review_owner:
                raise ValueError("MANUAL_REVIEW requires an owner")
        elif self.disposition == MigrationDisposition.UNSUPPORTED:
            if not self.reason_code:
                raise ValueError("UNSUPPORTED requires a visible reason_code")
        return self


def build_legacy_action_migration_registry() -> Dict[str, LegacyActionMigrationRecord]:
    from agent.etl_pipeline.action_registry import ACTION_REGISTRY

    ledger: Dict[str, LegacyActionMigrationRecord] = {}

    def map_ir(name: str, family: ActionFamily, model: str, canonical_name: Optional[str] = None, alias: Optional[str] = None, lim: Tuple[str, ...] = ()):
        ledger[name] = LegacyActionMigrationRecord(
            legacy_action_name=name,
            disposition=MigrationDisposition.MAPPED_TO_IR,
            canonical_family=family,
            expression_model=model,
            canonical_action_name=canonical_name or name,
            alias_of=alias,
            adapter_version="1.0.0",
            limitations=lim,
        )

    def composite_ir(name: str, model: str, alias: Optional[str] = None, lim: Tuple[str, ...] = ()):
        ledger[name] = LegacyActionMigrationRecord(
            legacy_action_name=name,
            disposition=MigrationDisposition.COMPOSITE_IR,
            expression_model=model,
            alias_of=alias,
            adapter_version="1.0.0",
            limitations=lim,
        )

    def pass_through(name: str, reason: str):
        ledger[name] = LegacyActionMigrationRecord(
            legacy_action_name=name,
            disposition=MigrationDisposition.PASS_THROUGH_DECISION,
            reason_code=reason,
            adapter_version="1.0.0",
        )

    def manual_review(name: str, owner: str, reason: str):
        ledger[name] = LegacyActionMigrationRecord(
            legacy_action_name=name,
            disposition=MigrationDisposition.MANUAL_REVIEW,
            manual_review_owner=owner,
            reason_code=reason,
        )

    def deprecated(name: str, replacement: str, removal: str):
        ledger[name] = LegacyActionMigrationRecord(
            legacy_action_name=name,
            disposition=MigrationDisposition.DEPRECATED,
            replacement_action=replacement,
            removal_target=removal,
            reason_code="superseded_by_canonical_action",
        )

    # MAP_VALUE
    map_ir("trim", ActionFamily.MAP_VALUE, "ScalarMapExpression(TRIM)")
    map_ir("trim_preserve_case", ActionFamily.MAP_VALUE, "ScalarMapExpression(TRIM)", alias="trim")
    map_ir("lowercase", ActionFamily.MAP_VALUE, "ScalarMapExpression(LOWER)")
    map_ir("uppercase", ActionFamily.MAP_VALUE, "ScalarMapExpression(UPPER)")
    map_ir("cast_type", ActionFamily.MAP_VALUE, "ScalarMapExpression(CAST)")
    map_ir("coerce_numeric", ActionFamily.MAP_VALUE, "ScalarMapExpression(PARSE_NUMBER)")
    map_ir("standardize_boolean", ActionFamily.MAP_VALUE, "ScalarMapExpression(STANDARDIZE_BOOLEAN)")
    map_ir("parse_dates", ActionFamily.MAP_VALUE, "ScalarMapExpression(PARSE_DATE)")
    map_ir("parse_dates_safe", ActionFamily.MAP_VALUE, "ScalarMapExpression(PARSE_DATE)")
    map_ir("sanitize_email", ActionFamily.MAP_VALUE, "ScalarMapExpression(SANITIZE_EMAIL)")
    map_ir("normalize_phone", ActionFamily.MAP_VALUE, "ScalarMapExpression(NORMALIZE_PHONE)")
    map_ir("regex_replace", ActionFamily.MAP_VALUE, "ScalarMapExpression(REGEX_SUBSTITUTE)")
    map_ir("replace_values", ActionFamily.MAP_VALUE, "ScalarMapExpression(LOOKUP_MAP)")
    map_ir("create_normalized_match_key", ActionFamily.MAP_VALUE, "ScalarMapExpression(TRIM)")

    # CLIP
    map_ir("range_clip", ActionFamily.CLIP, "ClipExpression")
    map_ir("cap_outliers", ActionFamily.CLIP, "ClipExpression")

    # CONDITIONAL_REPLACE
    map_ir("replace_sentinel_values", ActionFamily.CONDITIONAL_REPLACE, "ConditionalReplaceExpression")
    map_ir("normalize_registered_placeholder", ActionFamily.CONDITIONAL_REPLACE, "ConditionalReplaceExpression")
    map_ir("zero_to_null", ActionFamily.CONDITIONAL_REPLACE, "ConditionalReplaceExpression")
    map_ir("nullify_punctuation", ActionFamily.CONDITIONAL_REPLACE, "ConditionalReplaceExpression")
    map_ir("nullify_date", ActionFamily.CONDITIONAL_REPLACE, "ConditionalReplaceExpression")
    map_ir("nullify_dummy_dates", ActionFamily.CONDITIONAL_REPLACE, "ConditionalReplaceExpression")
    map_ir("nullify_future_dates", ActionFamily.CONDITIONAL_REPLACE, "ConditionalReplaceExpression")
    map_ir("fill_constant", ActionFamily.CONDITIONAL_REPLACE, "ConditionalReplaceExpression")
    map_ir("fill_null_with_constant", ActionFamily.CONDITIONAL_REPLACE, "ConditionalReplaceExpression", alias="fill_constant")
    map_ir("nullify_clean_value", ActionFamily.CONDITIONAL_REPLACE, "ConditionalReplaceExpression")

    # FLAG
    map_ir("flag_only", ActionFamily.FLAG, "FlagExpression")
    map_ir("flag_outliers", ActionFamily.FLAG, "FlagExpression")
    map_ir("flag_domain_violation", ActionFamily.FLAG, "FlagExpression")
    map_ir("fill_nulls_flag", ActionFamily.FLAG, "FlagExpression")
    map_ir("flag_missing_key", ActionFamily.FLAG, "FlagExpression")
    map_ir("flag_missing_declared_key", ActionFamily.FLAG, "FlagExpression")
    map_ir("flag_high_null_key", ActionFamily.FLAG, "FlagExpression")
    map_ir("flag_duplicate_candidates", ActionFamily.FLAG, "FlagExpression")
    map_ir("flag_near_duplicate_cluster", ActionFamily.FLAG, "FlagExpression")
    map_ir("flag_business_key_duplicate", ActionFamily.FLAG, "FlagExpression")
    map_ir("flag_range_violation", ActionFamily.FLAG, "FlagExpression")
    map_ir("flag_cross_field_violation", ActionFamily.FLAG, "FlagExpression")

    # QUARANTINE
    map_ir("quarantine_key_null", ActionFamily.QUARANTINE, "QuarantineExpression")
    map_ir("quarantine_orphan_fk", ActionFamily.QUARANTINE, "QuarantineExpression")
    map_ir("quarantine_missing_key", ActionFamily.QUARANTINE, "QuarantineExpression")
    map_ir("quarantine_row", ActionFamily.QUARANTINE, "QuarantineExpression")
    map_ir("quarantine_null_rows", ActionFamily.QUARANTINE, "QuarantineExpression")

    # HASH, MASK, IMPUTE, SCHEMA_MUTATION, OTHER
    map_ir("hash_phone", ActionFamily.HASH, "HashExpression")
    map_ir("mask_phone", ActionFamily.MASK, "MaskExpression")
    map_ir("impute_numeric", ActionFamily.IMPUTE, "ImputeExpression")
    map_ir("impute_date", ActionFamily.IMPUTE, "ImputeExpression")
    map_ir("drop_column", ActionFamily.SCHEMA_MUTATION, "SchemaMutationExpression")
    map_ir("exclude_column", ActionFamily.SCHEMA_MUTATION, "SchemaMutationExpression")
    map_ir("drop_rows", ActionFamily.FILTER, "FilterExpression")
    map_ir("deduplicate", ActionFamily.DEDUPLICATE, "DeduplicateExpression")
    map_ir("join_datasets", ActionFamily.JOIN, "JoinExpression")

    # COMPOSITE_IR
    composite_ir("validate_and_partition_relationship", "ValidateExpression + QuarantineExpression")
    composite_ir("validate_referential_integrity_or_stage", "ValidateExpression + QuarantineExpression", alias="validate_and_partition_relationship")
    composite_ir("clip_outliers", "AggregateExpression + DeriveExpression + ClipExpression")
    composite_ir("clip_or_flag", "FlagExpression + ClipExpression")
    composite_ir("preserve_null_and_flag", "PassThroughExpression + FlagExpression")

    # PASS_THROUGH_DECISION
    pass_through("noop", "intentional_noop")
    pass_through("keep_as_is", "intentional_keep_as_is")
    pass_through("skip_etl", "intentional_skip_etl")
    pass_through("preserve_null", "intentional_preserve_null")
    pass_through("preserve_missing_suspected_key", "intentional_preserve_missing_key")
    pass_through("accept_null_as_optional", "policy_accepted_null")
    pass_through("skip_requirement", "policy_skipped_requirement")
    pass_through("force_unlock", "steward_force_unlock")

    # MANUAL_REVIEW
    manual_review("review_manually", "data_steward", "explicit_manual_review_required")
    manual_review("fill_or_drop", "policy_steward", "ambiguous_intent_fill_or_drop")

    # DEPRECATED
    deprecated("fill_nulls_simple", "impute_numeric", "Phase 3")

    # Dynamic set equality reconciliation
    registry_actions = set(ACTION_REGISTRY.keys())
    migration_actions = set(ledger.keys())
    missing = registry_actions - migration_actions
    unexpected = migration_actions - registry_actions
    if missing or unexpected:
        raise TransformationIRInvariantError(
            f"Legacy migration registry mismatch: missing={sorted(missing)}, unexpected={sorted(unexpected)}"
        )

    return ledger


LEGACY_ACTION_MIGRATION_REGISTRY: Dict[str, LegacyActionMigrationRecord] = build_legacy_action_migration_registry()


class LegacyActionInventoryRecord(BaseModel):
    """Authoritative metadata record for a migrated legacy action."""
    legacy_action_name: str
    canonical_family: ActionFamily
    expression_model: str

    step_kind: StepKind
    target_scope: str = "column"

    changes_existing_values: bool = False
    changes_row_count: bool = False
    changes_schema: bool = False
    creates_columns: bool = False
    deletes_columns: bool = False
    creates_datasets: bool = False

    requires_preimage: bool = False
    preimage_requirement: PreimageRequirement = Field(default_factory=PreimageRequirement)

    risk_class: ActionRisk = ActionRisk.SAFE
    idempotent: bool = True
    deterministic: bool = True
    reversible: bool = False

    required_parameters: Tuple[str, ...] = ()
    null_semantics: str = "preserve"
    failure_policy: str = "ignore"

    engine_capabilities: Tuple[EngineCapability, ...] = ()
    required_postconditions: Tuple[str, ...] = ()


# ============================================================================
# 5. Legacy Step Adapter -> IR Steps
# ============================================================================

def plan_step_to_ir_steps(step: Dict[str, Any], dataset_name: Optional[str] = None) -> List[TransformationStepIR]:
    """
    Adapter converting a legacy dictionary plan step to one or more typed TransformationStepIR steps.
    Decomposes composite actions (e.g. validate_and_partition) into sequenced atomic steps.
    """
    sid = str(step.get("step_id") or step.get("id") or "step_unknown")
    ds = str(step.get("dataset") or dataset_name or "_global")
    col = str(step.get("column") or "")
    action = str(step.get("action") or "noop").strip().lower()
    params = step.get("params") or {}
    rule_id = step.get("rule_id")
    rule_ids = (str(rule_id),) if rule_id else ()
    ev_id = step.get("evidence_id")
    ev_ids = (str(ev_id),) if ev_id else ()

    col_ref = ColumnRef(column_name=col, dataset_name=ds)

    # 1. Composite: validate_and_partition_relationship -> VALIDATE then QUARANTINE
    if action in ("validate_and_partition_relationship", "validate_referential_integrity_or_stage"):
        val_id = f"{sid}_val"
        val_cond = BinaryOp(left=col_ref, op=BinaryOperator.NEQ, right=LiteralValue(value=None))
        val_expr = ValidateExpression(predicate=val_cond, rejection_reason_code="orphan_foreign_key")
        val_step = TransformationStepIR(
            step_id=val_id, dataset_name=ds, target_column=col,
            family=ActionFamily.VALIDATE, kind=StepKind.ASSERTION,
            expression=val_expr, effects=val_expr.derive_effects(ds),
            supporting_rule_ids=rule_ids, supporting_evidence_ids=ev_ids,
            legacy_action_name=action, risk_class=ActionRisk.SAFE,
        )

        quar_id = f"{sid}_quar"
        quar_expr = QuarantineExpression(
            predicate=NullCheck(operand=col_ref, is_null=True),
            curated_output_ref=f"{ds}_valid",
            quarantine_output_ref=f"{ds}_orphans",
            record_identity_columns=(col,),
            reason_column="relationship_validation_status",
        )
        quar_step = TransformationStepIR(
            step_id=quar_id, dataset_name=ds, target_column=col,
            family=ActionFamily.QUARANTINE, kind=StepKind.QUARANTINE_ROUTING,
            expression=quar_expr, effects=quar_expr.derive_effects(ds),
            prerequisites=(val_id,),
            supporting_rule_ids=rule_ids, supporting_evidence_ids=ev_ids,
            legacy_action_name=action, risk_class=ActionRisk.DESTRUCTIVE,
        )
        return [val_step, quar_step]

    # 2. QUARANTINE
    if action in ("quarantine_key_null", "quarantine_orphan_fk", "quarantine_missing_key", "quarantine_row", "quarantine_null_rows"):
        quar_expr = QuarantineExpression(
            predicate=NullCheck(operand=col_ref, is_null=True),
            curated_output_ref=ds,
            quarantine_output_ref=f"{ds}_quarantine",
            record_identity_columns=(col,) if col else (),
            reason_column="quarantine_reason",
        )
        return [TransformationStepIR(
            step_id=sid, dataset_name=ds, target_column=col,
            family=ActionFamily.QUARANTINE, kind=StepKind.QUARANTINE_ROUTING,
            expression=quar_expr, effects=quar_expr.derive_effects(ds),
            supporting_rule_ids=rule_ids, supporting_evidence_ids=ev_ids,
            legacy_action_name=action, risk_class=ActionRisk.DESTRUCTIVE,
        )]

    # 3. PASS_THROUGH
    if action in ("noop", "keep_as_is", "skip_etl", "preserve_null", "preserve_missing_suspected_key", "force_unlock", "skip_requirement", "accept_null_as_optional"):
        reason = NoTransformationReason.INTENTIONAL_NULL if "null" in action else NoTransformationReason.INTENTIONAL_SKIP
        pt_expr = PassThroughExpression(
            target_column=col_ref if col else None,
            reason=reason,
            reason_code=f"intentional_{action}",
        )
        return [TransformationStepIR(
            step_id=sid, dataset_name=ds, target_column=col,
            family=ActionFamily.PASS_THROUGH, kind=StepKind.CONTROL,
            expression=pt_expr, effects=pt_expr.derive_effects(ds),
            supporting_rule_ids=rule_ids, supporting_evidence_ids=ev_ids,
            legacy_action_name=action, risk_class=ActionRisk.SAFE,
        )]

    # 4. HASH & MASK
    if action == "hash_phone":
        h_expr = HashExpression(
            input_column=col_ref,
            output_column=col,
            algorithm=params.get("algorithm") or "sha256",
        )
        return [TransformationStepIR(
            step_id=sid, dataset_name=ds, target_column=col,
            family=ActionFamily.HASH, kind=StepKind.ROW_TRANSFORMATION,
            expression=h_expr, effects=h_expr.derive_effects(ds),
            pre_image=PreimageRequirement(purposes=(PreimagePurpose.EXECUTION,), retention=PreimageRetention.TEMPORARY, required_columns=(col,)),
            supporting_rule_ids=rule_ids, supporting_evidence_ids=ev_ids,
            legacy_action_name=action, risk_class=ActionRisk.PRIVACY_SENSITIVE,
        )]
    if action == "mask_phone":
        m_expr = MaskExpression(
            input_column=col_ref,
            output_column=col,
            strategy=MaskStrategy.PRESERVE_LAST_N,
            preserve_last_n=4,
        )
        return [TransformationStepIR(
            step_id=sid, dataset_name=ds, target_column=col,
            family=ActionFamily.MASK, kind=StepKind.ROW_TRANSFORMATION,
            expression=m_expr, effects=m_expr.derive_effects(ds),
            pre_image=PreimageRequirement(purposes=(PreimagePurpose.ROLLBACK,), retention=PreimageRetention.ROLLBACK_PERSISTED, required_columns=(col,)),
            supporting_rule_ids=rule_ids, supporting_evidence_ids=ev_ids,
            legacy_action_name=action, risk_class=ActionRisk.PRIVACY_SENSITIVE,
        )]

    # 5. IMPUTE
    if action in ("impute_numeric", "impute_date"):
        strat = params.get("strategy") or "median"
        group_cols = tuple(params.get("group_by") or ())
        l_model = params.get("learned_model")
        if isinstance(l_model, dict):
            l_model = LearnedImputationModel.model_validate(l_model)
        elif l_model is None and strat in ("mean", "median", "mode", "grouped_mean", "grouped_median"):
            val = params.get("value")
            if val is None:
                val = 0.0
            l_model = LearnedImputationModel(
                model_id=f"legacy_synth_{sid}",
                target_dataset=ds,
                target_column=col,
                strategy=strat,
                group_columns=group_cols,
                fit_population_hash="legacy_untracked_fit_hash",
                fit_schema_hash="legacy_untracked_schema_hash",
                learned_value=val if not group_cols else None,
                group_values=(),
                missing_group_policy=MissingGroupPolicy.USE_GLOBAL_FALLBACK if group_cols else MissingGroupPolicy.KEEP_NULL,
                global_fallback_value=val if group_cols else None,
            )
        imp_expr = ImputeExpression(
            target_column=col_ref,
            strategy=strat,
            constant_value=params.get("value") if strat == "constant" else None,
            learned_model=l_model,
            group_by_columns=group_cols,
            output_column=params.get("output_column") or col,
            policy_id=params.get("policy_id"),
        )
        return [TransformationStepIR(
            step_id=sid, dataset_name=ds, target_column=col,
            family=ActionFamily.IMPUTE, kind=StepKind.ROW_TRANSFORMATION,
            expression=imp_expr, effects=imp_expr.derive_effects(ds),
            pre_image=PreimageRequirement(purposes=(PreimagePurpose.ROLLBACK, PreimagePurpose.AUDIT), retention=PreimageRetention.AUDIT_PERSISTED, required_columns=(col,)),
            supporting_rule_ids=rule_ids, supporting_evidence_ids=ev_ids,
            legacy_action_name=action, risk_class=ActionRisk.AUDITABLE,
        )]

    # 6. SCHEMA_MUTATION
    if action in ("drop_column", "exclude_column"):
        kind = "drop_column" if action == "drop_column" else "project_exclude"
        mut_expr = SchemaMutationExpression(mutation_kind=kind, target_column=col, dataset_name=ds)
        return [TransformationStepIR(
            step_id=sid, dataset_name=ds, target_column=col,
            family=ActionFamily.SCHEMA_MUTATION, kind=StepKind.SCHEMA_MUTATION,
            expression=mut_expr, effects=mut_expr.derive_effects(ds),
            pre_image=PreimageRequirement(purposes=(PreimagePurpose.ROLLBACK,), retention=PreimageRetention.ROLLBACK_PERSISTED, required_columns=(col,)),
            supporting_rule_ids=rule_ids, supporting_evidence_ids=ev_ids,
            legacy_action_name=action, risk_class=ActionRisk.SCHEMA_ALTERING,
        )]

    # 7. CLIP
    if action in ("range_clip", "clip_outliers", "cap_outliers"):
        min_v = params.get("min_value") or params.get("lower_bound")
        max_v = params.get("max_value") or params.get("upper_bound")
        clip_expr = ClipExpression(
            target_column=col_ref,
            lower_bound=LiteralValue(value=min_v, data_type=DataType.FLOAT) if min_v is not None else None,
            upper_bound=LiteralValue(value=max_v, data_type=DataType.FLOAT) if max_v is not None else None,
        )
        return [TransformationStepIR(
            step_id=sid, dataset_name=ds, target_column=col,
            family=ActionFamily.CLIP, kind=StepKind.ROW_TRANSFORMATION,
            expression=clip_expr, effects=clip_expr.derive_effects(ds),
            pre_image=PreimageRequirement(purposes=(PreimagePurpose.ROLLBACK,), retention=PreimageRetention.TEMPORARY, required_columns=(col,)),
            supporting_rule_ids=rule_ids, supporting_evidence_ids=ev_ids,
            legacy_action_name=action, risk_class=ActionRisk.AUDITABLE,
        )]

    # 8. FLAG
    if action in ("flag_only", "flag_outliers", "flag_missing_key", "flag_domain_violation", "flag_range_violation", "preserve_null_and_flag", "fill_nulls_flag", "flag_duplicate_candidates", "flag_near_duplicate_cluster", "flag_business_key_duplicate", "flag_cross_field_violation"):
        if action == "flag_range_violation":
            min_v = params.get("min_value") or params.get("lower_bound")
            max_v = params.get("max_value") or params.get("upper_bound")
            flag_col = params.get("flag_column") or (f"is_out_of_range_{col}" if col else f"flag_range_{sid}")
            preds = []
            if min_v is not None:
                preds.append(BinaryOp(left=col_ref, op=BinaryOperator.LT, right=LiteralValue(value=min_v, data_type=DataType.FLOAT)))
            if max_v is not None:
                preds.append(BinaryOp(left=col_ref, op=BinaryOperator.GT, right=LiteralValue(value=max_v, data_type=DataType.FLOAT)))
            if len(preds) == 2:
                cond = BinaryOp(left=preds[0], op=BinaryOperator.OR, right=preds[1])
            elif len(preds) == 1:
                cond = preds[0]
            else:
                cond = BinaryOp(left=col_ref, op=BinaryOperator.NEQ, right=LiteralValue(value=None))
        else:
            flag_col = params.get("flag_column") or (f"is_{action}_{col}" if col else f"flag_{sid}")
            cond = NullCheck(operand=col_ref, is_null=True) if ("null" in action or "missing" in action) else BinaryOp(
                left=col_ref, op=BinaryOperator.NEQ, right=LiteralValue(value=None)
            )

        flag_expr = FlagExpression(predicate=cond, output_column=flag_col)
        return [TransformationStepIR(
            step_id=sid, dataset_name=ds, target_column=flag_col,
            family=ActionFamily.FLAG, kind=StepKind.AUDIT_FLAG,
            expression=flag_expr,
            supporting_rule_ids=rule_ids, supporting_evidence_ids=ev_ids,
            legacy_action_name=action, risk_class=ActionRisk.SAFE,
        )]

    # 9. CONDITIONAL_REPLACE
    if action in ("replace_sentinel_values", "zero_to_null", "nullify_punctuation", "nullify_date", "nullify_dummy_dates", "nullify_future_dates", "fill_constant", "fill_null_with_constant"):
        if action == "zero_to_null":
            cond = BinaryOp(left=col_ref, op=BinaryOperator.EQ, right=LiteralValue(value=0, data_type=DataType.INTEGER))
            rep = LiteralValue(value=None)
        elif action in ("fill_constant", "fill_null_with_constant"):
            cond = NullCheck(operand=col_ref, is_null=True)
            rep = LiteralValue(value=params.get("value"))
        elif action == "replace_sentinel_values":
            sentinels = params.get("sentinel_values") or params.get("sentinels") or [-999, -99, 999]
            vals = tuple(LiteralValue(value=s) for s in sentinels)
            cond = InSetPredicate(operand=col_ref, values=vals)
            rep = LiteralValue(value=None)
        else:
            cond = NullCheck(operand=col_ref, is_null=True)
            rep = LiteralValue(value=None)

        cond_rep = ConditionalReplaceExpression(
            target_column=col_ref,
            condition=cond,
            replace_value=rep,
            output_column=params.get("output_column") or col,
        )
        return [TransformationStepIR(
            step_id=sid, dataset_name=ds, target_column=col,
            family=ActionFamily.CONDITIONAL_REPLACE, kind=StepKind.ROW_TRANSFORMATION,
            expression=cond_rep,
            pre_image=PreimageRequirement(purposes=(PreimagePurpose.ROLLBACK,), retention=PreimageRetention.TEMPORARY, required_columns=(col,)),
            supporting_rule_ids=rule_ids, supporting_evidence_ids=ev_ids,
            legacy_action_name=action, risk_class=ActionRisk.AUDITABLE,
        )]

    # 10. MAP_VALUE
    out_col = params.get("output_column") or col
    op_mapping = {
        "trim": ScalarMapOperation.TRIM,
        "trim_preserve_case": ScalarMapOperation.TRIM,
        "lowercase": ScalarMapOperation.LOWER,
        "uppercase": ScalarMapOperation.UPPER,
        "cast_type": ScalarMapOperation.CAST,
        "coerce_numeric": ScalarMapOperation.PARSE_NUMBER,
        "parse_dates": ScalarMapOperation.PARSE_DATE,
        "parse_dates_safe": ScalarMapOperation.PARSE_DATE,
        "standardize_boolean": ScalarMapOperation.STANDARDIZE_BOOLEAN,
        "regex_replace": ScalarMapOperation.REGEX_SUBSTITUTE,
        "replace_values": ScalarMapOperation.LOOKUP_MAP,
        "normalize_phone": ScalarMapOperation.NORMALIZE_PHONE,
        "sanitize_email": ScalarMapOperation.SANITIZE_EMAIL,
    }
    op = op_mapping.get(action, ScalarMapOperation.TRIM)
    op_params = dict(params)
    if op == ScalarMapOperation.LOOKUP_MAP:
        mapping = params.get("replace_values") or params.get("mapping") or {}
        op_params["mapping"] = mapping
    elif op == ScalarMapOperation.CAST and "target_type" not in op_params:
        op_params["target_type"] = "string"

    s_expr = ScalarMapExpression(
        source_column=col_ref,
        output_column=out_col,
        operation=op,
        parameters=op_params,
        unmapped_policy=params.get("unmapped_policy") or UnmappedValuePolicy.KEEP_ORIGINAL,
    )
    return [TransformationStepIR(
        step_id=sid, dataset_name=ds, target_column=out_col,
        family=ActionFamily.MAP_VALUE,
        kind=StepKind.ROW_TRANSFORMATION if out_col == col else StepKind.COLUMN_DERIVATION,
        expression=s_expr,
        pre_image=PreimageRequirement(purposes=(PreimagePurpose.ROLLBACK,), retention=PreimageRetention.TEMPORARY, required_columns=(col,)),
        supporting_rule_ids=rule_ids, supporting_evidence_ids=ev_ids,
        legacy_action_name=action, risk_class=ActionRisk.SAFE,
    )]


def plan_step_to_ir(step: Dict[str, Any], dataset_name: Optional[str] = None) -> TransformationStepIR:
    """Convenience adapter returning the first or primary TransformationStepIR for a plan step."""
    return plan_step_to_ir_steps(step, dataset_name)[0]


# Backwards compatibility and capability-migration alias
adapt_legacy_action_to_ir = plan_step_to_ir


def plan_step_to_ir_graph(step: Dict[str, Any], dataset_name: Optional[str] = None) -> TransformationIRGraph:
    """
    Converts a plan step dictionary into a validated TransformationIRGraph.
    Consistently returns a graph envelope for both single and composite steps.
    """
    steps = plan_step_to_ir_steps(step, dataset_name)
    step_id = str(step.get("step_id") or step.get("id") or "step_graph")
    dependencies = []
    step_id_set = {s.step_id for s in steps}
    for s in steps:
        for prereq in s.prerequisites:
            if prereq in step_id_set:
                dependencies.append(IRDependency(source_step_id=prereq, target_step_id=s.step_id))

    return TransformationIRGraph(
        graph_id=f"graph_{step_id}",
        steps=tuple(steps),
        dependencies=tuple(dependencies),
    )


# ============================================================================
# 6. Slice 1: Pandas Reference Compiler
# ============================================================================

def compile_pandas_step(step: TransformationStepIR, df_var: str = "df", pre_image_var: Optional[str] = "pre_df") -> str:
    """
    Deterministic Pandas code compiler for Slice 1 families:
    FLAG, CLIP, MAP_VALUE, CONDITIONAL_REPLACE, VALIDATE.
    """
    expr = step.expression
    t_col = step.target_column or ""

    # 1. FLAG
    if isinstance(expr, FlagExpression):
        cond_code = expr.predicate.to_pandas_code(df_var, pre_image_var)
        out_col = expr.output_column
        return f"{df_var}['{out_col}'] = np.where({cond_code}, {repr(expr.true_value)}, {repr(expr.false_value)})"

    # 2. CLIP
    if isinstance(expr, ClipExpression):
        col_name = expr.target_column.column_name
        lines = []
        if expr.lower_bound is not None and expr.upper_bound is not None:
            l_val = expr.lower_bound.to_pandas_code(df_var, pre_image_var)
            u_val = expr.upper_bound.to_pandas_code(df_var, pre_image_var)
            return f"{df_var}['{col_name}'] = {df_var}['{col_name}'].clip(lower={l_val}, upper={u_val})"
        elif expr.lower_bound is not None:
            l_val = expr.lower_bound.to_pandas_code(df_var, pre_image_var)
            return f"{df_var}['{col_name}'] = {df_var}['{col_name}'].clip(lower={l_val})"
        elif expr.upper_bound is not None:
            u_val = expr.upper_bound.to_pandas_code(df_var, pre_image_var)
            return f"{df_var}['{col_name}'] = {df_var}['{col_name}'].clip(upper={u_val})"

    # 3. MAP_VALUE
    if isinstance(expr, ScalarMapExpression):
        in_col = expr.source_column.column_name
        out_col = expr.output_column
        if expr.operation == ScalarMapOperation.TRIM:
            return f"{df_var}['{out_col}'] = {df_var}['{in_col}'].astype('string').str.strip()"
        elif expr.operation == ScalarMapOperation.LOWER:
            return f"{df_var}['{out_col}'] = {df_var}['{in_col}'].astype('string').str.lower()"
        elif expr.operation == ScalarMapOperation.UPPER:
            return f"{df_var}['{out_col}'] = {df_var}['{in_col}'].astype('string').str.upper()"
        elif expr.operation == ScalarMapOperation.LOOKUP_MAP:
            mapping = expr.parameters.get("mapping", {})
            return f"{df_var}['{out_col}'] = {df_var}['{in_col}'].map({repr(mapping)}).fillna({df_var}['{in_col}'])"

    # 4. CONDITIONAL_REPLACE
    if isinstance(expr, ConditionalReplaceExpression):
        in_col = expr.target_column.column_name
        out_col = expr.output_column or in_col
        cond_code = expr.condition.to_pandas_code(df_var, pre_image_var)
        rep_code = expr.replace_value.to_pandas_code(df_var, pre_image_var)
        return f"{df_var}['{out_col}'] = np.where({cond_code}, {rep_code}, {df_var}['{in_col}'])"

    # 5. VALIDATE
    if isinstance(expr, ValidateExpression):
        cond_code = expr.predicate.to_pandas_code(df_var, pre_image_var)
        return f"# VALIDATE {step.step_id}: assert ({cond_code}).all(), '{expr.rejection_reason_code or 'Validation failed'}'"

    # 6. PASS_THROUGH
    if isinstance(expr, PassThroughExpression):
        return f"# PASS_THROUGH {step.step_id}: intentional non-mutation ({expr.reason_code})"

    return f"# UNHANDLED {step.step_id} ({expr.expression_type})"


def compile_pandas_plan(ir_steps: List[TransformationStepIR], df_var: str = "df") -> str:
    """
    Compiles an ordered list of TransformationStepIR steps into an executable Pandas script block.
    Supports pre-image snapshots when required by downstream steps.
    """
    def _reads_original(expr_obj: Any) -> bool:
        if isinstance(expr_obj, ColumnRef):
            return expr_obj.version == ValueVersion.ORIGINAL
        if isinstance(expr_obj, BaseModel):
            for v in expr_obj.__dict__.values():
                if _reads_original(v):
                    return True
        elif isinstance(expr_obj, (list, tuple)):
            for item in expr_obj:
                if _reads_original(item):
                    return True
        return False

    needs_pre = any(
        s.pre_image.required_for_execution or _reads_original(s.expression)
        for s in ir_steps
    )
    lines = ["import numpy as np", "import pandas as pd", ""]
    pre_var = "pre_df" if needs_pre else None
    if needs_pre:
        lines.append(f"{pre_var} = {df_var}.copy()")

    for st in ir_steps:
        code = compile_pandas_step(st, df_var=df_var, pre_image_var=pre_var)
        lines.append(code)

    return "\n".join(lines)
