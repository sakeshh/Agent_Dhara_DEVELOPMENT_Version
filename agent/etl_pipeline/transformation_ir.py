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
from decimal import Decimal
from enum import Enum
import hashlib
import json
from typing import Annotated, Any, Callable, Dict, List, Literal, Optional, Protocol, Sequence, Set, Tuple, Union
from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator


TRANSFORMATION_IR_VERSION = "1.0.0"


class TransformationIRInvariantError(ValueError):
    """Raised when an immutable IR invariant or effect semantic constraint is violated."""
    pass


class SchemaBindingError(TransformationIRInvariantError):
    """Raised when an expression fails validation against a schema."""
    pass


class CanonicalSerializationError(TransformationIRInvariantError):
    """Raised when a scalar value or type fails canonical scalar serialization."""
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


class RoutingMode(str, Enum):
    PARTITION = "partition"
    COPY = "copy"


class QuarantineUnknownPolicy(str, Enum):
    ROUTE_TO_QUARANTINE = "route_to_quarantine"
    RETAIN_IN_CURATED = "retain_in_curated"
    SEPARATE_OUTPUT = "separate_output"
    FAIL_STEP = "fail_step"


class FilterUnknownPolicy(str, Enum):
    RETAIN = "retain"
    FILTER_OUT = "filter_out"
    ROUTE_TO_DROPPED_OUTPUT = "route_to_dropped_output"
    SEPARATE_OUTPUT = "separate_output"
    FAIL_STEP = "fail_step"


class ValidationUnknownPolicy(str, Enum):
    COUNT_AS_VIOLATION = "count_as_violation"
    COUNT_AS_PASS = "count_as_pass"
    REPORT_SEPARATELY = "report_separately"
    FAIL_STEP = "fail_step"


class FilterDropPolicy(str, Enum):
    DROP = "drop"
    EXPORT_DROPPED = "export_dropped"


class ValidateSeverity(str, Enum):
    ERROR = "error"
    WARNING = "warning"


class OrderDirection(str, Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"


class DeduplicationTieBreaker(str, Enum):
    PRESERVE_FIRST = "preserve_first"
    PRESERVE_LAST = "preserve_last"
    EXPLICIT_ORDER = "explicit_order"


class DuplicateExportMode(str, Enum):
    REMOVED_OCCURRENCES_ONLY = "removed_occurrences_only"
    ALL_GROUP_MEMBERS = "all_group_members"


class NullKeyPolicy(str, Enum):
    GROUP_NULLS_TOGETHER = "group_nulls_together"
    TREAT_NULLS_AS_EQUAL = "group_nulls_together"  # backward-compatible alias
    TREAT_EACH_NULL_AS_DISTINCT = "treat_each_null_as_distinct"
    FAIL_STEP = "fail_step"
    EXCLUDE_NULL_KEY_ROWS = "exclude_null_key_rows"


class OutputPurpose(str, Enum):
    PRIMARY = "primary"
    QUARANTINE = "quarantine"
    UNKNOWN = "unknown"
    DROPPED = "dropped"
    DUPLICATES = "duplicates"
    DIAGNOSTIC = "diagnostic"


class OutputProducerKind(str, Enum):
    STEP = "step"
    GRAPH_FINAL = "graph_final"


class OccurrenceIdentityScope(str, Enum):
    EXECUTION_LOCAL = "execution_local"
    SOURCE_STABLE = "source_stable"


class RowIdentityStrategy(str, Enum):
    EXISTING_KEY = "existing_key"
    COMPOSITE_KEY = "composite_key"
    GENERATED_OCCURRENCE_ID = "generated_occurrence_id"
    INDEX_BASED = "index_based"
    PRESERVE_EXISTING_ID = "preserve_existing_id"
    SOURCE_STABLE_COORDINATES = "source_stable_coordinates"


class SortColumnSpec(BaseModel):
    column: str
    direction: OrderDirection = OrderDirection.ASCENDING
    nulls_last: bool = True


class SourceStableIdentitySpec(BaseModel):
    snapshot_id: str
    partition_column: str
    row_offset_column: str
    source_file_column: Optional[str] = None
    ingestion_run_column: Optional[str] = None


class RoutingIdentityPolicy(BaseModel):
    strategy: RowIdentityStrategy = RowIdentityStrategy.GENERATED_OCCURRENCE_ID
    internal_column: str = "__dhara_row_occurrence_0"
    published_column: Optional[str] = None
    scope: OccurrenceIdentityScope = OccurrenceIdentityScope.EXECUTION_LOCAL
    source_stable_spec: Optional[SourceStableIdentitySpec] = None
    retain_in_primary_output: bool = False
    retain_in_secondary_outputs: bool = True

    @model_validator(mode="after")
    def validate_scope(self) -> "RoutingIdentityPolicy":
        if self.scope == OccurrenceIdentityScope.EXECUTION_LOCAL and self.source_stable_spec is not None:
            raise ValueError("EXECUTION_LOCAL scope forbids source_stable_spec")
        if self.scope == OccurrenceIdentityScope.SOURCE_STABLE and self.source_stable_spec is None:
            raise ValueError("SOURCE_STABLE scope requires source_stable_spec")
        return self


class DatasetOutputSpec(BaseModel):
    output_ref: str
    purpose: OutputPurpose
    producer_kind: OutputProducerKind
    produced_by_step_id: Optional[str] = None
    is_primary: bool = False

    @model_validator(mode="after")
    def validate_producer(self) -> "DatasetOutputSpec":
        if self.producer_kind == OutputProducerKind.STEP and not self.produced_by_step_id:
            raise ValueError("STEP producer requires produced_by_step_id")
        if self.producer_kind == OutputProducerKind.GRAPH_FINAL and self.produced_by_step_id:
            raise ValueError("GRAPH_FINAL producer forbids produced_by_step_id")
        return self


class RoutingReconciliationEvidence(BaseModel):
    step_id: str
    mode: RoutingMode
    input_occurrences: int
    curated_occurrences: int
    quarantine_occurrences: int
    unknown_occurrences: int = 0
    reconciled: bool
    overlap_count: int = 0


class ValidationExecutionSummary(BaseModel):
    step_id: str
    candidate_count: int
    pass_count: int
    violation_count: int
    raw_unknown_count: int
    effective_violation_count: int
    severity: ValidateSeverity
    gate_outcome: str  # "PASS" | "FAIL" | "WARN"
    reason_code: str


class OutputReferenceCollisionError(ValueError):
    """Raised when an output reference collides with another output producer or reserved reference."""
    pass


class MultiOutputArtifactError(ValueError):
    """Raised when single-output access is attempted on a pipeline producing multiple datasets without a singular primary output."""
    pass


class OutputColumnCollisionError(ValueError):
    """Raised when a technical column collides with existing schema columns."""
    pass


class RoutingReconciliationError(ValueError):
    """Raised when routed partition/copy outputs fail multiset conservation invariants."""
    pass


class RoutingPredicateUnknownError(ValueError):
    """Raised when a routing predicate evaluates to unknown/NA under a FAIL_STEP policy."""
    pass


class ValidationGateError(ValueError):
    """Raised when a validation step fails under ERROR severity."""
    pass


class KeyProviderRequiredError(ValueError):
    """Raised when an operation requires a key provider but none is provided."""
    pass


class SecretResolutionError(ValueError):
    """Raised when a secret cannot be resolved, is expired, revoked, or has wrong tenant/purpose."""
    pass


class SchemaSnapshotMismatchError(ValueError):
    """Raised when the actual schema at execution does not match the expected/predicted schema snapshot hash."""
    pass


class UnauthorizedDestructiveActionError(ValueError):
    """Raised when a destructive action (such as drop_column) lacks valid StepAuthorization."""
    pass


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
    MULTI_DATASET_JOIN = "multi_dataset_join"
    DATASET_AGGREGATION = "dataset_aggregation"


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
    severity: ValidateSeverity = ValidateSeverity.ERROR
    unknown_policy: ValidationUnknownPolicy = ValidationUnknownPolicy.FAIL_STEP
    rejection_reason_code: Optional[str] = None
    reason_code: str = "VALIDATION_RULE_VIOLATED"

    @model_validator(mode="before")
    @classmethod
    def handle_legacy_severity(cls, data: Any) -> Any:
        if isinstance(data, dict) and "severity" in data:
            s = data["severity"]
            if isinstance(s, str):
                s_lower = s.lower()
                if s_lower == "warning":
                    data["severity"] = ValidateSeverity.WARNING
                else:
                    data["severity"] = ValidateSeverity.ERROR
        return data

    def derive_effects(self, dataset: str) -> TransformationEffects:
        reads = tuple(sorted(set(self.predicate.extract_reads())))
        return TransformationEffects(reads=reads, filters_rows=False, partitions_rows=False)


class QuarantineExpression(BaseModel):
    """Row routing and partitioning into distinct output destinations."""
    expression_type: Literal["quarantine"] = "quarantine"
    predicate: Expression
    routing_mode: RoutingMode = RoutingMode.PARTITION
    unknown_policy: QuarantineUnknownPolicy = QuarantineUnknownPolicy.FAIL_STEP
    curated_output_ref: str
    quarantine_output_ref: str
    unknown_output_ref: Optional[str] = None
    reason_column: Optional[str] = None
    reason_code: str = "QUARANTINE_POLICY_VIOLATION"
    policy_id: str = "DEFAULT_QUARANTINE_POLICY"
    retention_policy_id: Optional[str] = None
    # Compatibility attributes
    identity_strategy: Optional[RowIdentityStrategy] = None
    identity_columns: Tuple[str, ...] = ()
    generated_identity_column: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def handle_aliases(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "accepted_output_ref" in data and "curated_output_ref" not in data:
                data["curated_output_ref"] = data.pop("accepted_output_ref")
            if "rejected_output_ref" in data and "quarantine_output_ref" not in data:
                data["quarantine_output_ref"] = data.pop("rejected_output_ref")
            if "routing_mode" in data and isinstance(data["routing_mode"], str):
                mode_str = data["routing_mode"].lower()
                if mode_str == "copy":
                    data["routing_mode"] = RoutingMode.COPY
                else:
                    data["routing_mode"] = RoutingMode.PARTITION
            if "record_identity_columns" in data and "identity_columns" not in data:
                data["identity_columns"] = data.pop("record_identity_columns")
        return data

    @model_validator(mode="after")
    def validate_unknown_output(self) -> "QuarantineExpression":
        if self.unknown_policy == QuarantineUnknownPolicy.SEPARATE_OUTPUT and not self.unknown_output_ref:
            raise ValueError("SEPARATE_OUTPUT requires unknown_output_ref")
        if self.unknown_policy != QuarantineUnknownPolicy.SEPARATE_OUTPUT and self.unknown_output_ref is not None:
            raise ValueError("unknown_output_ref is valid only with SEPARATE_OUTPUT")
        if self.identity_strategy == RowIdentityStrategy.GENERATED_OCCURRENCE_ID and not self.generated_identity_column:
            self.generated_identity_column = "_source_row_id"
        return self

    def derive_effects(self, dataset: str) -> TransformationEffects:
        id_cols = list(self.identity_columns)
        if self.generated_identity_column:
            id_cols.append(self.generated_identity_column)
        reads = tuple(sorted(set(self.predicate.extract_reads() + id_cols)))
        creates = (self.reason_column,) if self.reason_column else ()
        is_partition = (self.routing_mode == RoutingMode.PARTITION)
        return TransformationEffects(
            reads=reads,
            creates=creates,
            filters_rows=is_partition,
            partitions_rows=is_partition,
        )


class FilterExpression(BaseModel):
    """Row filtering: retaining or removing rows from primary output."""
    expression_type: Literal["filter"] = "filter"
    predicate: Expression
    retain_matching: bool = True
    drop_policy: FilterDropPolicy = FilterDropPolicy.DROP
    unknown_policy: FilterUnknownPolicy = FilterUnknownPolicy.FAIL_STEP
    unknown_output_ref: Optional[str] = None
    dropped_output_ref: Optional[str] = None
    rejection_reason_code: Optional[str] = None

    @model_validator(mode="after")
    def validate_filter_contract(self) -> "FilterExpression":
        if self.drop_policy == FilterDropPolicy.EXPORT_DROPPED:
            if not self.dropped_output_ref:
                raise ValueError("EXPORT_DROPPED requires dropped_output_ref")
            if not self.rejection_reason_code:
                raise ValueError("EXPORT_DROPPED requires rejection_reason_code")
        elif self.drop_policy == FilterDropPolicy.DROP:
            if self.dropped_output_ref is not None:
                raise ValueError("dropped_output_ref is valid only with EXPORT_DROPPED")
        if self.unknown_policy == FilterUnknownPolicy.SEPARATE_OUTPUT and not self.unknown_output_ref:
            raise ValueError("SEPARATE_OUTPUT requires unknown_output_ref")
        if self.unknown_policy != FilterUnknownPolicy.SEPARATE_OUTPUT and self.unknown_output_ref is not None:
            raise ValueError("unknown_output_ref is valid only with SEPARATE_OUTPUT")
        return self

    def derive_effects(self, dataset: str) -> TransformationEffects:
        reads = tuple(sorted(set(self.predicate.extract_reads())))
        return TransformationEffects(
            reads=reads,
            filters_rows=True,
            partitions_rows=(self.drop_policy == FilterDropPolicy.EXPORT_DROPPED),
        )


class DeduplicateExpression(BaseModel):
    """Deduplication: selecting survivors among duplicate occurrences."""
    expression_type: Literal["deduplicate"] = "deduplicate"
    key_columns: Tuple[str, ...] = ()
    resolved_key_columns: Tuple[str, ...] = ()
    tie_breaker: DeduplicationTieBreaker = DeduplicationTieBreaker.PRESERVE_FIRST
    order_by: Tuple[SortColumnSpec, ...] = ()
    null_key_policy: NullKeyPolicy = NullKeyPolicy.GROUP_NULLS_TOGETHER
    use_occurrence_id_as_final_tie_breaker: bool = True
    duplicates_output_ref: Optional[str] = None
    duplicate_export_mode: DuplicateExportMode = DuplicateExportMode.REMOVED_OCCURRENCES_ONLY

    @model_validator(mode="after")
    def validate_dedup(self) -> "DeduplicateExpression":
        if self.tie_breaker == DeduplicationTieBreaker.EXPLICIT_ORDER and not self.order_by:
            raise ValueError("EXPLICIT_ORDER tie_breaker requires non-empty order_by specs")
        return self

    def derive_effects(self, dataset: str) -> TransformationEffects:
        keys = self.resolved_key_columns or self.key_columns
        order_cols = tuple(s.column for s in self.order_by)
        reads = tuple(sorted(set(list(keys) + list(order_cols))))
        return TransformationEffects(
            reads=reads,
            filters_rows=True,
            partitions_rows=bool(self.duplicates_output_ref),
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


# ============================================================================
# Security Action Enums & Models (Increment 3D)
# ============================================================================

class HashMode(str, Enum):
    DIGEST = "digest"                  # Unkeyed cryptographic digest
    SALTED_DIGEST = "salted_digest"    # Salted digest with public/non-secret salt
    HMAC = "hmac"                      # Secret-key keyed HMAC construction


class HashAlgorithm(str, Enum):
    SHA256 = "sha256"
    SHA512 = "sha512"
    SHA3_256 = "sha3_256"
    BLAKE2B = "blake2b"


class HashInputEncoding(str, Enum):
    CANONICAL_SCALAR_V1 = "canonical_scalar_v1"
    UTF8_STRING = "utf8_string"
    RAW_BYTES = "raw_bytes"


class DecimalCanonicalizationMode(str, Enum):
    NUMERIC_VALUE = "numeric_value"
    PRESERVE_DECLARED_SCALE = "preserve_declared_scale"


class DigestEncoding(str, Enum):
    HEX = "hex"
    BASE64URL = "base64url"


class HashNullPolicy(str, Enum):
    PRESERVE_NULL = "preserve_null"
    HASH_NULL = "hash_null"
    FAIL_STEP = "fail_step"


class CollisionPolicy(str, Enum):
    FAIL_ON_COLLISION = "fail_on_collision"
    ALLOW_COLLISION = "allow_collision"


class MaskStrategy(str, Enum):
    PRESERVE_LAST_N = "preserve_last_n"
    PRESERVE_FIRST_N = "preserve_first_n"
    MASK_ALL = "mask_all"
    EMAIL_MASK = "email_mask"
    # Legacy aliases
    REDACT_ALL = "mask_all"
    PARTIAL_REDACTION = "preserve_last_n"
    TOKENIZE = "tokenize"


class ShortValuePolicy(str, Enum):
    MASK_ALL = "mask_all"
    SET_NULL = "set_null"
    FAIL_STEP = "fail_step"
    KEEP_ORIGINAL = "keep_original"   # Requires explicit policy approval


class MaskNullPolicy(str, Enum):
    PRESERVE_NULL = "preserve_null"
    SET_EMPTY = "set_empty"
    FAIL_STEP = "fail_step"


class EmailMaskDomainPolicy(str, Enum):
    PRESERVE = "preserve"
    MASK = "mask"
    HASH = "hash"


class SecretReference(BaseModel):
    secret_id: str
    expected_purpose: Literal["hmac_key", "hash_pepper", "tokenization_key"] = "hmac_key"
    expected_version: Optional[str] = None


class SecretHandle(BaseModel):
    secret_id: str
    secret_bytes: bytes
    version: str = "v1"
    tenant_id: str = "default"
    purpose: str = "etl_hash"

    def __repr__(self) -> str:
        return f"<SecretHandle secret_id={self.secret_id!r} version={self.version!r} [REDACTED]>"


class ExecutionKeyProvider(Protocol):
    def resolve_secret(
        self,
        *,
        secret_id: str,
        tenant_id: str,
        purpose: str,
        expected_version: Optional[str] = None,
    ) -> SecretHandle:
        ...


class HashExpression(BaseModel):
    """Cryptographic one-way hash transformation."""
    expression_type: Literal["hash"] = "hash"
    source_column: str = ""
    target_column: Optional[str] = None
    input_column: Optional[ColumnRef] = None
    output_column: Optional[str] = None
    mode: HashMode = HashMode.DIGEST
    algorithm: HashAlgorithm = HashAlgorithm.SHA256
    secret_ref: Optional[SecretReference] = None
    salt_value: Optional[str] = None
    salt_reference: Optional[str] = None
    input_encoding: HashInputEncoding = HashInputEncoding.CANONICAL_SCALAR_V1
    input_data_type: Optional[DataType] = None
    unicode_normalization: Literal["NFC", "NFKC"] = "NFC"
    output_encoding: DigestEncoding = DigestEncoding.HEX
    null_policy: HashNullPolicy = HashNullPolicy.PRESERVE_NULL
    truncate_length: Optional[int] = None
    collision_policy: CollisionPolicy = CollisionPolicy.ALLOW_COLLISION
    existing_column_policy: ExistingColumnPolicy = ExistingColumnPolicy.ERROR
    preserve_original: bool = False
    reversible: Literal[False] = False
    inverse_operation_supported: Literal[False] = False
    security_class: str = "one_way_pseudonymization"
    remove_source_column_at_publication: bool = True

    @model_validator(mode="before")
    @classmethod
    def handle_legacy_hash(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "input_column" in data and not data.get("source_column"):
                ic = data["input_column"]
                data["source_column"] = ic.column_name if hasattr(ic, "column_name") else str(ic)
            if "output_column" in data and not data.get("target_column"):
                data["target_column"] = data["output_column"]
            if "salt_reference" in data and not data.get("secret_ref") and not data.get("salt_value"):
                sref = data["salt_reference"]
                if sref:
                    data["secret_ref"] = {"secret_id": str(sref), "expected_purpose": "hmac_key"}
                    data["mode"] = HashMode.HMAC
            if isinstance(data.get("algorithm"), str):
                alg_str = data["algorithm"].lower().replace("-", "")
                if alg_str in ("sha256", "sha512", "sha3256", "sha3_256", "blake2b"):
                    data["algorithm"] = HashAlgorithm.SHA3_256 if alg_str in ("sha3256", "sha3_256") else HashAlgorithm(alg_str)
        return data

    @model_validator(mode="after")
    def validate_hash_contract(self) -> "HashExpression":
        if not self.source_column and self.input_column:
            self.source_column = self.input_column.column_name
        if not self.target_column and self.output_column:
            self.target_column = self.output_column
        if not self.source_column:
            raise ValueError("HashExpression requires source_column or input_column")
        if self.mode == HashMode.HMAC and not self.secret_ref:
            raise ValueError("HMAC mode strictly requires secret_ref")
        if self.mode == HashMode.DIGEST and (self.secret_ref or self.salt_value):
            raise ValueError("DIGEST mode prohibits secret_ref and salt_value")
        if self.mode == HashMode.SALTED_DIGEST and not self.salt_value and not self.secret_ref:
            raise ValueError("SALTED_DIGEST requires salt_value or secret_ref")
        if self.truncate_length is not None and self.truncate_length < 16:
            raise ValueError("Hash truncate_length must be at least 16 characters")
        return self

    def derive_effects(self, dataset: str) -> TransformationEffects:
        in_col = self.source_column
        out_col = self.target_column or in_col
        writes = (out_col,) if out_col == in_col else ()
        creates = (out_col,) if out_col != in_col else ()
        return TransformationEffects(
            reads=(in_col,) if in_col else (),
            writes=writes,
            creates=creates,
            filters_rows=False,
            partitions_rows=False,
        )


class MaskExpression(BaseModel):
    """Data masking/redaction transformation."""
    expression_type: Literal["mask"] = "mask"
    source_column: str = ""
    target_column: Optional[str] = None
    input_column: Optional[ColumnRef] = None
    output_column: Optional[str] = None
    strategy: MaskStrategy = MaskStrategy.PRESERVE_LAST_N
    preserved_count: int = 4
    preserve_last_n: Optional[int] = None
    mask_character: str = "*"
    short_value_policy: ShortValuePolicy = ShortValuePolicy.MASK_ALL
    null_policy: MaskNullPolicy = MaskNullPolicy.PRESERVE_NULL
    email_domain_policy: EmailMaskDomainPolicy = EmailMaskDomainPolicy.PRESERVE
    token_vault_policy_id: Optional[str] = None
    reversible: bool = False
    inverse_operation_supported: Literal[False] = False
    security_class: str = "redaction"
    existing_column_policy: ExistingColumnPolicy = ExistingColumnPolicy.ERROR
    remove_source_column_at_publication: bool = True

    @model_validator(mode="before")
    @classmethod
    def handle_legacy_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "input_column" in data and not data.get("source_column"):
                ic = data["input_column"]
                data["source_column"] = ic.column_name if hasattr(ic, "column_name") else str(ic)
            if "output_column" in data and not data.get("target_column"):
                data["target_column"] = data["output_column"]
            if "masking_strategy" in data and "strategy" not in data:
                strat = str(data.pop("masking_strategy"))
                if "last" in strat:
                    data["strategy"] = MaskStrategy.PRESERVE_LAST_N
                elif "first" in strat:
                    data["strategy"] = MaskStrategy.PRESERVE_FIRST_N
                elif "email" in strat:
                    data["strategy"] = MaskStrategy.EMAIL_MASK
                else:
                    data["strategy"] = MaskStrategy.MASK_ALL
            if "visible_suffix_length" in data and "preserved_count" not in data:
                data["preserved_count"] = data.pop("visible_suffix_length")
            if "preserve_last_n" in data and "preserved_count" not in data:
                data["preserved_count"] = data["preserve_last_n"]
        return data

    @model_validator(mode="after")
    def validate_mask(self) -> "MaskExpression":
        if not self.source_column and self.input_column:
            self.source_column = self.input_column.column_name
        if not self.target_column and self.output_column:
            self.target_column = self.output_column
        if not self.source_column:
            raise ValueError("MaskExpression requires source_column or input_column")
        if len(self.mask_character) != 1 or ord(self.mask_character) < 32:
            raise ValueError("mask_character must be a single non-control Unicode character")
        if self.preserved_count < 0:
            raise ValueError("preserved_count cannot be negative")
        return self

    def derive_effects(self, dataset: str) -> TransformationEffects:
        in_col = self.source_column
        out_col = self.target_column or in_col
        writes = (out_col,) if out_col == in_col else ()
        creates = (out_col,) if out_col != in_col else ()
        return TransformationEffects(
            reads=(in_col,) if in_col else (),
            writes=writes,
            creates=creates,
            filters_rows=False,
            partitions_rows=False,
        )


# ============================================================================
# Schema Mutation Enums & Models (Increment 3D)
# ============================================================================

class ColumnRole(str, Enum):
    BUSINESS = "business"
    LINEAGE = "lineage"
    TEMPORARY = "temporary"
    VALIDITY_INDICATOR = "validity_indicator"
    IMPUTATION_INDICATOR = "imputation_indicator"
    PARSE_ERROR = "parse_error"
    ROUTING_IDENTITY = "routing_identity"
    SECURITY_OUTPUT = "security_output"


class ColumnProtectionClass(str, Enum):
    PRIMARY_KEY = "primary_key"
    BUSINESS_KEY = "business_key"
    SECURITY_INTEGRITY = "security_integrity"
    LINEAGE = "lineage"
    ROUTING_IDENTITY = "routing_identity"


INTRINSICALLY_PROTECTED_ROLES: Set[ColumnRole] = {
    ColumnRole.ROUTING_IDENTITY,
    ColumnRole.LINEAGE,
    ColumnRole.VALIDITY_INDICATOR,
    ColumnRole.IMPUTATION_INDICATOR,
    ColumnRole.PARSE_ERROR,
}


def is_column_effectively_protected(col: Any) -> bool:
    """Evaluates whether a column is protected under ColumnProtectionClass, role, or flag."""
    if getattr(col, "is_protected", False):
        return True
    if getattr(col, "protection_classes", ()):
        return True
    if getattr(col, "role", None) in INTRINSICALLY_PROTECTED_ROLES:
        return True
    return False


class ColumnMetadata(BaseModel):
    name: str
    logical_type: DataType = DataType.STRING
    role: ColumnRole = ColumnRole.BUSINESS
    nullable: bool = True
    is_protected: bool = False
    protection_classes: Tuple[ColumnProtectionClass, ...] = ()


class SchemaMutationOp(str, Enum):
    CREATE_COLUMN = "create_column"
    RENAME_COLUMN = "rename_column"
    DROP_COLUMN = "drop_column"
    PROJECT = "project"
    REORDER_COLUMNS = "reorder_columns"
    CAST_COLUMN = "cast_column"


class ColumnRenameSpec(BaseModel):
    old_name: str
    new_name: str


class ColumnDropSpec(BaseModel):
    column: str
    justification: str = "Authorized schema reduction"


class ColumnProjectSpec(BaseModel):
    projected_columns: Tuple[str, ...]
    reorder: bool = True
    allow_missing: bool = False


class CastFailurePolicy(str, Enum):
    FAIL_STEP = "fail_step"
    SET_NULL = "set_null"
    FLAG = "flag"
    QUARANTINE = "quarantine"


class NumericOverflowPolicy(str, Enum):
    FAIL_STEP = "fail_step"
    CLIP = "clip"
    CLIP_TO_RANGE = "clip"
    SET_NULL = "set_null"
    WRAP = "wrap"


class StepAuthorization(BaseModel):
    step_id: str
    policy_decision_id: str
    approved_by_subject_id: str
    approved_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: datetime
    authorized_step_hash: str
    tenant_id: str = "default"


def compute_canonical_schema_hash(columns: Sequence[Tuple[str, Union[DataType, str], bool]], order_sensitive: bool = True) -> str:
    norm = []
    for col in columns:
        dt = col[1].value if isinstance(col[1], DataType) else str(col[1])
        norm.append((col[0], dt, bool(col[2])))
    if not order_sensitive:
        norm = sorted(norm, key=lambda x: x[0])
    payload = json.dumps(norm, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class BoundSchemaMutation(BaseModel):
    step_id: str
    source_schema_hash: str
    predicted_output_schema_hash: str


class SchemaMutationEvidence(BaseModel):
    step_id: str
    actual_before_schema_hash: str
    actual_after_schema_hash: str
    predicted_after_schema_hash: str
    matched_prediction: bool


class SchemaMutationExpression(BaseModel):
    """Schema mutation: dropping, renaming, projecting, reordering, creating, or casting columns."""
    expression_type: Literal["schema_mutation"] = "schema_mutation"
    op: SchemaMutationOp = SchemaMutationOp.DROP_COLUMN
    mutation_kind: Optional[str] = None
    target_column: Optional[str] = None
    source_column: Optional[str] = None
    dataset_name: Optional[str] = None
    reversible: Literal[False] = False
    renames: Tuple[ColumnRenameSpec, ...] = ()
    drops: Tuple[ColumnDropSpec, ...] = ()
    projection: Optional[ColumnProjectSpec] = None
    ordered_columns: Tuple[str, ...] = ()
    create_column_name: Optional[str] = None
    create_target_type: Optional[DataType] = None
    create_expression: Optional[Expression] = None
    cast_target_type: Optional[DataType] = None
    cast_failure_policy: CastFailurePolicy = CastFailurePolicy.FAIL_STEP
    cast_overflow_policy: NumericOverflowPolicy = NumericOverflowPolicy.FAIL_STEP
    in_place: bool = False
    expected_before_schema_hash: Optional[str] = None
    expected_after_schema_hash: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def handle_legacy_mutation(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "mutation_kind" in data and "op" not in data:
                mkind = data["mutation_kind"]
                if mkind == "drop_column":
                    data["op"] = SchemaMutationOp.DROP_COLUMN
                    if "target_column" in data and not data.get("drops"):
                        data["drops"] = [{"column": data["target_column"], "justification": "legacy_drop"}]
                elif mkind == "project_exclude":
                    data["op"] = SchemaMutationOp.PROJECT
        return data

    def derive_effects(self, dataset: str) -> TransformationEffects:
        if self.op == SchemaMutationOp.DROP_COLUMN:
            drop_cols = tuple(d.column for d in self.drops) if self.drops else ((self.target_column,) if self.target_column else ())
            return TransformationEffects(drops=drop_cols, filters_rows=False, partitions_rows=False)
        elif self.op == SchemaMutationOp.RENAME_COLUMN:
            reads = tuple(r.old_name for r in self.renames)
            creates = tuple(r.new_name for r in self.renames)
            drops = tuple(r.old_name for r in self.renames)
            return TransformationEffects(reads=reads, creates=creates, drops=drops, filters_rows=False, partitions_rows=False)
        elif self.op == SchemaMutationOp.PROJECT:
            reads = self.projection.projected_columns if self.projection else ()
            return TransformationEffects(reads=reads, filters_rows=False, partitions_rows=False)
        elif self.op == SchemaMutationOp.REORDER_COLUMNS:
            return TransformationEffects(reads=self.ordered_columns, filters_rows=False, partitions_rows=False)
        elif self.op == SchemaMutationOp.CREATE_COLUMN:
            col = self.create_column_name or self.target_column or "new_col"
            reads = tuple(self.create_expression.extract_reads()) if self.create_expression else ()
            return TransformationEffects(reads=reads, creates=(col,), filters_rows=False, partitions_rows=False)
        elif self.op == SchemaMutationOp.CAST_COLUMN:
            src = self.source_column or self.target_column or "col"
            dst = self.target_column or src
            writes = (dst,) if dst == src else ()
            creates = (dst,) if dst != src else ()
            return TransformationEffects(reads=(src,), writes=writes, creates=creates, filters_rows=False, partitions_rows=False)
        drops = (self.target_column,) if self.target_column and self.mutation_kind == "drop_column" else ()
        return TransformationEffects(drops=drops, filters_rows=False, partitions_rows=False)


class CreateColumnExpression(SchemaMutationExpression):
    op: SchemaMutationOp = SchemaMutationOp.CREATE_COLUMN
    target_column: str = ""
    target_type: DataType = DataType.STRING
    value_expression: Optional[Expression] = None
    existing_column_policy: ExistingColumnPolicy = ExistingColumnPolicy.ERROR

    @model_validator(mode="before")
    @classmethod
    def set_create_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data["op"] = SchemaMutationOp.CREATE_COLUMN
            if "target_column" in data and not data.get("create_column_name"):
                data["create_column_name"] = data["target_column"]
            if "value_expression" in data and not data.get("create_expression"):
                data["create_expression"] = data["value_expression"]
            if "target_type" in data and not data.get("create_target_type"):
                data["create_target_type"] = data["target_type"]
        return data


class RenameColumnsExpression(SchemaMutationExpression):
    op: SchemaMutationOp = SchemaMutationOp.RENAME_COLUMN
    renames: Tuple[ColumnRenameSpec, ...] = ()

    @model_validator(mode="before")
    @classmethod
    def set_rename_op(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data["op"] = SchemaMutationOp.RENAME_COLUMN
        return data


class DropColumnsExpression(SchemaMutationExpression):
    op: SchemaMutationOp = SchemaMutationOp.DROP_COLUMN
    drops: Tuple[ColumnDropSpec, ...] = ()

    @model_validator(mode="before")
    @classmethod
    def set_drop_op(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data["op"] = SchemaMutationOp.DROP_COLUMN
        return data


class ProjectColumnsExpression(SchemaMutationExpression):
    op: SchemaMutationOp = SchemaMutationOp.PROJECT
    projection: Optional[ColumnProjectSpec] = None

    @model_validator(mode="before")
    @classmethod
    def set_proj_op(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data["op"] = SchemaMutationOp.PROJECT
        return data


class ReorderColumnsExpression(SchemaMutationExpression):
    op: SchemaMutationOp = SchemaMutationOp.REORDER_COLUMNS
    ordered_columns: Tuple[str, ...] = ()

    @model_validator(mode="before")
    @classmethod
    def set_reorder_op(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data["op"] = SchemaMutationOp.REORDER_COLUMNS
        return data


class CastColumnExpression(SchemaMutationExpression):
    op: SchemaMutationOp = SchemaMutationOp.CAST_COLUMN
    source_column: str = ""
    target_column: Optional[str] = None
    target_type: DataType = DataType.STRING
    failure_policy: CastFailurePolicy = CastFailurePolicy.FAIL_STEP
    overflow_policy: NumericOverflowPolicy = NumericOverflowPolicy.FAIL_STEP
    lossless_required: bool = False
    in_place: bool = False

    @model_validator(mode="before")
    @classmethod
    def set_cast_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data["op"] = SchemaMutationOp.CAST_COLUMN
            if "target_type" in data and not data.get("cast_target_type"):
                data["cast_target_type"] = data["target_type"]
            if "failure_policy" in data and not data.get("cast_failure_policy"):
                data["cast_failure_policy"] = data["failure_policy"]
            if "overflow_policy" in data and not data.get("cast_overflow_policy"):
                data["cast_overflow_policy"] = data["overflow_policy"]
        return data



# ============================================================================
# Derivation & Extraction Enums & Models (Increment 3E)
# ============================================================================

class ArithmeticDomain(str, Enum):
    INT64 = "int64"
    FLOAT64 = "float64"
    DECIMAL = "decimal"


class ArithmeticOperator(str, Enum):
    ADD = "add"
    SUBTRACT = "subtract"
    MULTIPLY = "multiply"
    DIVIDE = "divide"
    FLOOR_DIVIDE = "floor_divide"
    MODULO = "modulo"
    POWER = "power"


class DivisionByZeroPolicy(str, Enum):
    SET_NULL = "set_null"
    FAIL_STEP = "fail_step"
    FAIL = "fail_step"
    RETURN_INFINITY = "return_infinity"
    USE_FALLBACK = "use_fallback"


class ConditionalUnknownPolicy(str, Enum):
    SET_NULL = "set_null"
    USE_THEN = "use_then"
    USE_ELSE = "use_else"
    USE_FALLBACK = "use_fallback"
    FAIL_STEP = "fail_step"


class ConcatNullPolicy(str, Enum):
    IGNORE_NULLS = "ignore_nulls"
    IGNORE = "ignore_nulls"
    PRESERVE_AS_EMPTY = "preserve_as_empty"
    COALESCE_EMPTY = "coalesce_empty"
    SET_RESULT_NULL = "set_result_null"
    PROPAGATE_NULL = "propagate_null"


class DateArithmeticUnit(str, Enum):
    DAY = "day"
    MONTH = "month"
    YEAR = "year"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"


class DateDiffUnit(str, Enum):
    DAY = "day"
    MONTH = "month"
    YEAR = "year"
    HOUR = "hour"
    SECOND = "second"


class NumericOperand(BaseModel):
    column_name: Optional[str] = None
    literal_value: Optional[Union[int, float, Decimal]] = None
    value: Optional[Union[int, float, Decimal]] = None

    @model_validator(mode="after")
    def validate_operand(self) -> "NumericOperand":
        if self.literal_value is None and self.value is not None:
            self.literal_value = self.value
        elif self.value is None and self.literal_value is not None:
            self.value = self.literal_value
        if self.column_name is None and self.literal_value is None:
            raise ValueError("NumericOperand must specify either column_name or literal_value")
        return self


class ArithmeticDerivation(BaseModel):
    expression_type: Literal["derive"] = "derive"
    derivation_type: Literal["arithmetic"] = "arithmetic"
    operator: ArithmeticOperator
    left: NumericOperand
    right: Optional[NumericOperand] = None
    domain: ArithmeticDomain = ArithmeticDomain.FLOAT64
    output_column: str
    output_type: DataType = DataType.FLOAT
    division_by_zero_policy: DivisionByZeroPolicy = DivisionByZeroPolicy.SET_NULL
    fallback_value: Optional[Union[int, float, Decimal]] = None
    overflow_policy: NumericOverflowPolicy = NumericOverflowPolicy.FAIL_STEP
    existing_column_policy: ExistingColumnPolicy = ExistingColumnPolicy.ERROR

    def derive_effects(self, dataset: str) -> TransformationEffects:
        reads = []
        if self.left.column_name:
            reads.append(self.left.column_name)
        if self.right and self.right.column_name:
            reads.append(self.right.column_name)
        return TransformationEffects(reads=tuple(reads), creates=(self.output_column,), filters_rows=False, partitions_rows=False)


class ConditionalDerivation(BaseModel):
    expression_type: Literal["derive"] = "derive"
    derivation_type: Literal["conditional"] = "conditional"
    condition: Union[BinaryOp, InSetPredicate, NullCheck, ColumnRef, Expression]
    then_column: Optional[str] = None
    then_value: Optional[Any] = None
    else_column: Optional[str] = None
    else_value: Optional[Any] = None
    fallback_value: Optional[Any] = None
    unknown_policy: ConditionalUnknownPolicy = ConditionalUnknownPolicy.SET_NULL
    output_column: str
    output_type: Optional[DataType] = None
    existing_column_policy: ExistingColumnPolicy = ExistingColumnPolicy.ERROR

    def derive_effects(self, dataset: str) -> TransformationEffects:
        reads = []
        if hasattr(self.condition, "extract_reads"):
            reads.extend(self.condition.extract_reads())
        elif hasattr(self.condition, "column"):
            col_val = self.condition.column
            reads.append(col_val.column_name if hasattr(col_val, "column_name") else str(col_val))
        if self.then_column:
            reads.append(self.then_column)
        if self.else_column:
            reads.append(self.else_column)
        return TransformationEffects(reads=tuple(reads), creates=(self.output_column,), filters_rows=False, partitions_rows=False)


class CoalesceDerivation(BaseModel):
    expression_type: Literal["derive"] = "derive"
    derivation_type: Literal["coalesce"] = "coalesce"
    columns: List[str]
    fallback_value: Optional[Any] = None
    output_column: str
    output_type: Optional[DataType] = None
    existing_column_policy: ExistingColumnPolicy = ExistingColumnPolicy.ERROR

    def derive_effects(self, dataset: str) -> TransformationEffects:
        return TransformationEffects(reads=tuple(self.columns), creates=(self.output_column,), filters_rows=False, partitions_rows=False)


class ConcatDerivation(BaseModel):
    expression_type: Literal["derive"] = "derive"
    derivation_type: Literal["concat"] = "concat"
    columns: List[str]
    delimiter: str = ""
    null_policy: ConcatNullPolicy = ConcatNullPolicy.IGNORE_NULLS
    output_column: str
    existing_column_policy: ExistingColumnPolicy = ExistingColumnPolicy.ERROR

    def derive_effects(self, dataset: str) -> TransformationEffects:
        return TransformationEffects(reads=tuple(self.columns), creates=(self.output_column,), filters_rows=False, partitions_rows=False)


class DateArithmeticDerivation(BaseModel):
    expression_type: Literal["derive"] = "derive"
    derivation_type: Literal["date_arithmetic"] = "date_arithmetic"
    source_column: str
    operation: Literal["add", "subtract", "diff"] = "diff"
    target_column: Optional[str] = None
    amount: Optional[int] = None
    unit: DateArithmeticUnit = DateArithmeticUnit.DAY
    output_column: str
    existing_column_policy: ExistingColumnPolicy = ExistingColumnPolicy.ERROR

    def derive_effects(self, dataset: str) -> TransformationEffects:
        reads = [self.source_column]
        if self.target_column:
            reads.append(self.target_column)
        return TransformationEffects(reads=tuple(reads), creates=(self.output_column,), filters_rows=False, partitions_rows=False)


DeriveExpression = Annotated[
    Union[ArithmeticDerivation, ConditionalDerivation, CoalesceDerivation, ConcatDerivation, DateArithmeticDerivation],
    Field(discriminator="derivation_type"),
]


# Extraction models
class ExtractFailurePolicy(str, Enum):
    SET_NULL = "set_null"
    FAIL_STEP = "fail_step"
    USE_FALLBACK = "use_fallback"


class MultipleMatchPolicy(str, Enum):
    FIRST = "first"
    LAST = "last"
    FAIL_STEP = "fail_step"


class DatePart(str, Enum):
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"
    DAY_OF_WEEK = "day_of_week"
    QUARTER = "quarter"


class BoundedJsonPath(BaseModel):
    path: str
    max_depth: int = 10
    max_array_index: int = 1000

    @model_validator(mode="after")
    def validate_path_syntax(self) -> "BoundedJsonPath":
        p = self.path.strip()
        if not p:
            raise ValueError("BoundedJsonPath path cannot be empty")
        if ".." in p or "*" in p or "?" in p or "(" in p:
            raise ValueError(f"Prohibited recursive, wildcard, or filter expression in BoundedJsonPath: '{self.path}'")
        parts = p.replace("[", ".").replace("]", "").split(".")
        parts = [pt for pt in parts if pt]
        if len(parts) > self.max_depth:
            raise ValueError(f"JSON path depth {len(parts)} exceeds maximum permitted depth {self.max_depth}")
        for pt in parts:
            if pt.isdigit():
                idx = int(pt)
                if idx > self.max_array_index:
                    raise ValueError(f"JSON array index {idx} exceeds maximum permitted index {self.max_array_index}")
        return self


class RegexExtract(BaseModel):
    expression_type: Literal["extract"] = "extract"
    extraction_type: Literal["regex"] = "regex"
    source_column: str
    pattern: str
    group_index: int = 1
    group_name: Optional[str] = None
    multiple_match_policy: MultipleMatchPolicy = MultipleMatchPolicy.FIRST
    failure_policy: ExtractFailurePolicy = ExtractFailurePolicy.SET_NULL
    fallback_value: Optional[str] = None
    output_column: str
    existing_column_policy: ExistingColumnPolicy = ExistingColumnPolicy.ERROR

    def derive_effects(self, dataset: str) -> TransformationEffects:
        return TransformationEffects(reads=(self.source_column,), creates=(self.output_column,), filters_rows=False, partitions_rows=False)


class JsonPathExtract(BaseModel):
    expression_type: Literal["extract"] = "extract"
    extraction_type: Literal["json_path"] = "json_path"
    source_column: str
    path: Union[BoundedJsonPath, str]
    failure_policy: ExtractFailurePolicy = ExtractFailurePolicy.SET_NULL
    fallback_value: Optional[Any] = None
    output_column: str
    existing_column_policy: ExistingColumnPolicy = ExistingColumnPolicy.ERROR

    @model_validator(mode="before")
    @classmethod
    def coerce_path(cls, data: Any) -> Any:
        if isinstance(data, dict) and isinstance(data.get("path"), str):
            data["path"] = {"path": data["path"]}
        return data

    def derive_effects(self, dataset: str) -> TransformationEffects:
        return TransformationEffects(reads=(self.source_column,), creates=(self.output_column,), filters_rows=False, partitions_rows=False)


class DelimiterExtract(BaseModel):
    expression_type: Literal["extract"] = "extract"
    extraction_type: Literal["delimiter"] = "delimiter"
    source_column: str
    delimiter: str
    index: int = 0
    failure_policy: ExtractFailurePolicy = ExtractFailurePolicy.SET_NULL
    fallback_value: Optional[str] = None
    output_column: str
    existing_column_policy: ExistingColumnPolicy = ExistingColumnPolicy.ERROR

    def derive_effects(self, dataset: str) -> TransformationEffects:
        return TransformationEffects(reads=(self.source_column,), creates=(self.output_column,), filters_rows=False, partitions_rows=False)


class DatePartExtract(BaseModel):
    expression_type: Literal["extract"] = "extract"
    extraction_type: Literal["date_part"] = "date_part"
    source_column: str
    part: DatePart
    failure_policy: ExtractFailurePolicy = ExtractFailurePolicy.SET_NULL
    output_column: str
    existing_column_policy: ExistingColumnPolicy = ExistingColumnPolicy.ERROR

    def derive_effects(self, dataset: str) -> TransformationEffects:
        return TransformationEffects(reads=(self.source_column,), creates=(self.output_column,), filters_rows=False, partitions_rows=False)


ExtractExpression = Annotated[
    Union[RegexExtract, JsonPathExtract, DelimiterExtract, DatePartExtract],
    Field(discriminator="extraction_type"),
]


# Multi-Dataset Join Models
class JoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL_OUTER = "full_outer"
    CROSS = "cross"
    SEMI = "semi"
    ANTI = "anti"


class JoinCardinality(str, Enum):
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class JoinNullPolicy(str, Enum):
    DROP_NULL_MATCHES = "drop_null_matches"
    MATCH_NULLS_TOGETHER = "match_nulls_together"
    FAIL_STEP_ON_NULL_KEY = "fail_step_on_null_key"


class UnmatchedRowDisposition(str, Enum):
    DROP = "drop"
    RETAIN = "retain"
    ROUTE_TO_UNMATCHED_OUTPUT = "route_to_unmatched_output"


class JoinCollisionPolicy(str, Enum):
    RENAME_WITH_SUFFIX = "rename_with_suffix"
    FAIL_ON_COLLISION = "fail_on_collision"
    SELECT_LEFT = "select_left"
    SELECT_RIGHT = "select_right"


class JoinKeyPair(BaseModel):
    left: Union[ColumnRef, str]
    right: Union[ColumnRef, str]
    comparison_type: str = "eq"
    normalization_ref: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def coerce_keys(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if isinstance(data.get("left"), str):
                data["left"] = {"column_name": data["left"]}
            if isinstance(data.get("right"), str):
                data["right"] = {"column_name": data["right"]}
        return data


class JoinUnmatchedPolicy(BaseModel):
    left_unmatched: UnmatchedRowDisposition = UnmatchedRowDisposition.DROP
    right_unmatched: UnmatchedRowDisposition = UnmatchedRowDisposition.DROP
    left_output_ref: Optional[str] = None
    right_output_ref: Optional[str] = None


class JoinExecutionAccounting(BaseModel):
    left_input_count: int
    right_input_count: int
    left_matched_count: int
    right_matched_count: int
    left_unmatched_count: int
    right_unmatched_count: int
    output_count: int
    expansion_ratio: float


class JoinExpression(BaseModel):
    expression_type: Literal["join"] = "join"
    left_dataset: str
    right_dataset: str
    join_type: JoinType
    key_pairs: List[JoinKeyPair] = Field(default_factory=list)
    cardinality: JoinCardinality = JoinCardinality.MANY_TO_ONE
    null_policy: JoinNullPolicy = JoinNullPolicy.DROP_NULL_MATCHES
    unmatched_policy: JoinUnmatchedPolicy = Field(default_factory=JoinUnmatchedPolicy)
    collision_policy: JoinCollisionPolicy = JoinCollisionPolicy.RENAME_WITH_SUFFIX
    left_suffix: str = "_left"
    right_suffix: str = "_right"
    max_output_row_count: Optional[int] = None
    max_row_expansion_ratio: Optional[float] = None
    output_dataset: Optional[str] = None

    @model_validator(mode="after")
    def validate_join(self) -> "JoinExpression":
        if self.join_type == JoinType.CROSS:
            if self.key_pairs:
                raise ValueError("CROSS join cannot declare key_pairs")
        else:
            if not self.key_pairs:
                raise ValueError(f"Join type {self.join_type.value} requires at least one key_pair")
        return self

    def derive_effects(self, dataset: str) -> TransformationEffects:
        left_reads = tuple(k.left.column_name if hasattr(k.left, "column_name") else str(k.left) for k in self.key_pairs)
        filters = self.join_type in (JoinType.INNER, JoinType.SEMI, JoinType.ANTI)
        return TransformationEffects(reads=left_reads, filters_rows=filters, partitions_rows=False)


# Aggregation Models
class GroupKeyNullPolicy(str, Enum):
    INCLUDE_NULL_GROUP = "include_null_group"
    DROP_NULL_GROUP = "drop_null_group"
    FAIL_STEP = "fail_step"


class AggregateOp(str, Enum):
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    STDDEV_SAMPLE = "stddev_sample"
    STDDEV_POPULATION = "stddev_population"
    MEDIAN = "median"


class AggregateValueNullPolicy(str, Enum):
    EXCLUDE_NULLS = "exclude_nulls"
    PROPAGATE_NULL = "propagate_null"
    TREAT_AS_ZERO = "treat_as_zero"
    FAIL_STEP = "fail_step"


class EmptyInputPolicy(str, Enum):
    EMIT_EMPTY_DATASET = "emit_empty_dataset"
    EMIT_SINGLE_NULL_ROW = "emit_single_null_row"
    FAIL_STEP = "fail_step"


class AggregateColumnSpec(BaseModel):
    source_column: Optional[str] = None  # None for COUNT(*)
    operation: AggregateOp
    output_column: str
    output_type: Optional[DataType] = None
    null_policy: AggregateValueNullPolicy = AggregateValueNullPolicy.EXCLUDE_NULLS
    precision: Optional[int] = None
    scale: Optional[int] = None


class AggregateExpression(BaseModel):
    expression_type: Literal["aggregate"] = "aggregate"
    group_by: List[str] = Field(default_factory=list)
    aggregates: List[AggregateColumnSpec]
    group_key_null_policy: GroupKeyNullPolicy = GroupKeyNullPolicy.INCLUDE_NULL_GROUP
    empty_input_policy: EmptyInputPolicy = EmptyInputPolicy.EMIT_EMPTY_DATASET
    order_by: List[SortColumnSpec] = Field(default_factory=list)
    output_dataset: Optional[str] = None

    def derive_effects(self, dataset: str) -> TransformationEffects:
        reads = list(self.group_by)
        for a in self.aggregates:
            if a.source_column:
                reads.append(a.source_column)
        creates = tuple(a.output_column for a in self.aggregates)
        return TransformationEffects(reads=tuple(reads), creates=creates, partitions_rows=True)


# Publication Models
class PublicationStageHandle(BaseModel):
    staging_id: str
    output_refs: List[str]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PublicationValidationResult(BaseModel):
    is_valid: bool
    passed_postconditions: int = 0
    failed_postconditions: int = 0
    errors: List[str] = Field(default_factory=list)


class PublicationCommitResult(BaseModel):
    is_committed: bool
    published_output_count: int
    manifest_version: str
    diagnostics: Dict[str, Any] = Field(default_factory=dict)


class StagedExecutionResult(BaseModel):
    staging_id: str
    outputs: Dict[str, Any]
    is_ready_for_publication: bool
    postcondition_errors: List[str] = Field(default_factory=list)


class PublicationAdapter(Protocol):
    def stage(self, staging_id: str, outputs: Mapping[str, Any]) -> PublicationStageHandle:
        ...

    def validate(self, handle: PublicationStageHandle) -> PublicationValidationResult:
        ...

    def commit(self, handle: PublicationStageHandle) -> PublicationCommitResult:
        ...

    def abort(self, handle: PublicationStageHandle) -> None:
        ...


TransformationExpression = Annotated[
    Union[
        ValidateExpression,
        QuarantineExpression,
        FilterExpression,
        DeduplicateExpression,
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
        DeriveExpression,
        ExtractExpression,
        JoinExpression,
        AggregateExpression,
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
    role: ColumnRole = ColumnRole.BUSINESS
    is_protected: bool = False
    protection_classes: Tuple[ColumnProtectionClass, ...] = ()

    @model_validator(mode="before")
    @classmethod
    def _map_logical_type(cls, data: Any) -> Any:
        if isinstance(data, dict) and "logical_type" in data and "data_type" not in data:
            data["data_type"] = data.pop("logical_type")
        return data

    @property
    def logical_type(self) -> DataType:
        return self.data_type


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

    # For DeduplicateExpression: resolve empty keys to business columns and validate sort specs
    if isinstance(expression, DeduplicateExpression):
        if not expression.key_columns:
            # Resolve to all schema columns excluding internal technical columns
            resolved = tuple(k for k in ds_schema.columns.keys() if not k.startswith("__"))
            expression = expression.model_copy(update={"resolved_key_columns": resolved})
        else:
            for k in expression.key_columns:
                if k not in ds_schema.columns:
                    raise TransformationIRInvariantError(f"Deduplicate key column '{k}' does not exist in schema")
            expression = expression.model_copy(update={"resolved_key_columns": expression.key_columns})

        for spec in expression.order_by:
            if spec.column not in ds_schema.columns:
                raise TransformationIRInvariantError(f"Deduplicate sort column '{spec.column}' does not exist in schema")
        eff = derive_effects(expression, dataset_name)

    # For HashExpression: source column exists, target column collision check
    if isinstance(expression, HashExpression):
        s_col = expression.source_column or (expression.input_column.column_name if expression.input_column else "")
        t_col = expression.target_column or expression.output_column or s_col
        if s_col and s_col not in ds_schema.columns:
            raise TransformationIRInvariantError(f"Hash source column '{s_col}' does not exist in schema")
        if t_col != s_col and t_col in ds_schema.columns and expression.existing_column_policy == ExistingColumnPolicy.ERROR:
            raise TransformationIRInvariantError(f"Hash target column '{t_col}' already exists in schema (policy=ERROR)")

    # For MaskExpression: source column exists, target column collision check
    if isinstance(expression, MaskExpression):
        s_col = expression.source_column or (expression.input_column.column_name if expression.input_column else "")
        t_col = expression.target_column or expression.output_column or s_col
        if s_col and s_col not in ds_schema.columns:
            raise TransformationIRInvariantError(f"Mask source column '{s_col}' does not exist in schema")
        if t_col != s_col and t_col in ds_schema.columns and expression.existing_column_policy == ExistingColumnPolicy.ERROR:
            raise TransformationIRInvariantError(f"Mask target column '{t_col}' already exists in schema (policy=ERROR)")

    # For SchemaMutationExpression: role checks, existence, collision
    if isinstance(expression, SchemaMutationExpression):
        if expression.op == SchemaMutationOp.DROP_COLUMN:
            drop_cols = [d.column for d in expression.drops] if expression.drops else ([expression.target_column] if expression.target_column else [])
            for dc in drop_cols:
                if dc not in ds_schema.columns:
                    raise TransformationIRInvariantError(f"Cannot drop non-existent column '{dc}'")
                # Role-based protection: cannot drop internal routing, lineage, or indicator columns
                col_schema = ds_schema.columns[dc]
                if (
                    dc.startswith("__dhara_")
                    or dc.endswith("_was_imputed")
                    or dc.endswith("_parse_error")
                    or is_column_effectively_protected(col_schema)
                ):
                    raise SchemaBindingError(f"Cannot drop protected column with role '{col_schema.role.value}': '{dc}'")
        elif expression.op == SchemaMutationOp.RENAME_COLUMN:
            for r in expression.renames:
                if r.old_name not in ds_schema.columns:
                    raise TransformationIRInvariantError(f"Cannot rename non-existent column '{r.old_name}'")
                if r.new_name in ds_schema.columns and r.new_name != r.old_name:
                    raise TransformationIRInvariantError(f"Rename target column '{r.new_name}' already exists in schema")
        elif expression.op == SchemaMutationOp.PROJECT:
            if expression.projection:
                proj_set = set(expression.projection.projected_columns)
                if not expression.projection.allow_missing:
                    for pc in expression.projection.projected_columns:
                        if pc not in ds_schema.columns:
                            raise TransformationIRInvariantError(f"Projected column '{pc}' does not exist in schema")
                for c_name, col_schema in ds_schema.columns.items():
                    if c_name not in proj_set and is_column_effectively_protected(col_schema):
                        raise SchemaBindingError(
                            f"Cannot drop protected column with role '{col_schema.role.value}' via projection exclusion: '{c_name}'"
                        )
        elif expression.op == SchemaMutationOp.REORDER_COLUMNS:
            for rc in expression.ordered_columns:
                if rc not in ds_schema.columns:
                    raise TransformationIRInvariantError(f"Reorder column '{rc}' does not exist in schema")
        elif expression.op == SchemaMutationOp.CREATE_COLUMN:
            c_name = expression.create_column_name or expression.target_column
            if c_name and c_name in ds_schema.columns and expression.existing_column_policy == ExistingColumnPolicy.ERROR:
                raise TransformationIRInvariantError(f"Created column '{c_name}' already exists in schema (policy=ERROR)")
        elif expression.op == SchemaMutationOp.CAST_COLUMN:
            src = expression.source_column or expression.target_column
            if src and src not in ds_schema.columns:
                raise TransformationIRInvariantError(f"Cast source column '{src}' does not exist in schema")

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
    authorization: Optional[StepAuthorization] = None

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

    routing_identity_policy: Optional[RoutingIdentityPolicy] = None
    primary_output_ref: Optional[str] = None
    output_specs: Tuple[DatasetOutputSpec, ...] = ()

    @property
    def graph_hash(self) -> str:
        data = {
            "ir_version": self.ir_version,
            "action_catalogue_version": self.action_catalogue_version,
            "schema_snapshot_hash": self.schema_snapshot_hash,
            "policy_version": self.policy_version,
            "steps": [s.plan_node_hash for s in self.steps],
            "dependencies": [dep.model_dump() for dep in self.dependencies],
            "primary_output_ref": self.primary_output_ref,
            "output_specs": [o.model_dump() for o in self.output_specs],
        }
        dumped = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


def validate_output_registry(graph: TransformationIRGraph, schema: Optional[DatasetSchema] = None) -> None:
    """
    Validates graph-wide output ownership, collision safety, and source-stable binding.
    """
    seen_refs: Dict[str, str] = {}
    normalized_refs: Dict[str, str] = {}
    primary_count = 0

    for spec in graph.output_specs:
        ref = spec.output_ref
        norm_ref = ref.lower()

        # Case normalization collision check
        if norm_ref in normalized_refs and normalized_refs[norm_ref] != ref:
            raise OutputReferenceCollisionError(
                f"Output reference '{ref}' collides with existing reference '{normalized_refs[norm_ref]}' under case-normalization"
            )
        normalized_refs[norm_ref] = ref

        # Unique producer check
        producer_key = spec.produced_by_step_id or "__graph_final__"
        if ref in seen_refs:
            raise OutputReferenceCollisionError(
                f"Output reference '{ref}' has multiple producers: '{seen_refs[ref]}' and '{producer_key}'"
            )
        seen_refs[ref] = producer_key

        if spec.is_primary:
            primary_count += 1

    if graph.primary_output_ref:
        if primary_count > 1:
            raise OutputReferenceCollisionError(f"Multiple primary outputs declared in graph: {primary_count}")
        # Verify no secondary step overwrites primary_output_ref
        for step in graph.steps:
            expr = step.expression
            if isinstance(expr, QuarantineExpression):
                if expr.quarantine_output_ref == graph.primary_output_ref:
                    raise OutputReferenceCollisionError(f"Secondary output '{expr.quarantine_output_ref}' cannot overwrite primary output")
                if expr.unknown_output_ref == graph.primary_output_ref:
                    raise OutputReferenceCollisionError(f"Secondary unknown output '{expr.unknown_output_ref}' cannot overwrite primary output")
            elif isinstance(expr, FilterExpression):
                if expr.dropped_output_ref == graph.primary_output_ref:
                    raise OutputReferenceCollisionError(f"Secondary dropped output '{expr.dropped_output_ref}' cannot overwrite primary output")
                if expr.unknown_output_ref == graph.primary_output_ref:
                    raise OutputReferenceCollisionError(f"Secondary unknown output '{expr.unknown_output_ref}' cannot overwrite primary output")
            elif isinstance(expr, DeduplicateExpression):
                if expr.duplicates_output_ref == graph.primary_output_ref:
                    raise OutputReferenceCollisionError(f"Secondary duplicates output '{expr.duplicates_output_ref}' cannot overwrite primary output")

    # Source-stable coordinates schema check
    if graph.routing_identity_policy and graph.routing_identity_policy.scope == OccurrenceIdentityScope.SOURCE_STABLE:
        spec = graph.routing_identity_policy.source_stable_spec
        if not spec:
            raise ValueError("SOURCE_STABLE policy requires source_stable_spec")
        if schema:
            for c in [spec.partition_column, spec.row_offset_column]:
                if c not in schema.columns:
                    raise ValueError(f"Source-stable coordinate column '{c}' not found in schema")
            if spec.source_file_column and spec.source_file_column not in schema.columns:
                raise ValueError(f"Source-stable coordinate column '{spec.source_file_column}' not found in schema")
            if spec.ingestion_run_column and spec.ingestion_run_column not in schema.columns:
                raise ValueError(f"Source-stable coordinate column '{spec.ingestion_run_column}' not found in schema")


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
            identity_columns=(col,) if col else (),
            reason_column="quarantine_reason",
        )
        return [TransformationStepIR(
            step_id=sid, dataset_name=ds, target_column=col,
            family=ActionFamily.QUARANTINE, kind=StepKind.QUARANTINE_ROUTING,
            expression=quar_expr, effects=quar_expr.derive_effects(ds),
            supporting_rule_ids=rule_ids, supporting_evidence_ids=ev_ids,
            legacy_action_name=action, risk_class=ActionRisk.DESTRUCTIVE,
        )]

    # 2b. FILTER
    if action in ("drop_rows", "filter_rows", "filter_null_rows"):
        cond = NullCheck(operand=col_ref, is_null=True) if "null" in action else BinaryOp(left=col_ref, op=BinaryOperator.NEQ, right=LiteralValue(value=None))
        filt_expr = FilterExpression(
            predicate=cond,
            retain_matching=False,
            drop_policy=FilterDropPolicy.DROP,
        )
        return [TransformationStepIR(
            step_id=sid, dataset_name=ds, target_column=col,
            family=ActionFamily.FILTER, kind=StepKind.DATASET_FILTER,
            expression=filt_expr, effects=filt_expr.derive_effects(ds),
            supporting_rule_ids=rule_ids, supporting_evidence_ids=ev_ids,
            legacy_action_name=action, risk_class=ActionRisk.DESTRUCTIVE,
        )]

    # 2c. DEDUPLICATE
    if action in ("deduplicate", "drop_duplicates", "deduplicate_rows"):
        keys = tuple(params.get("key_columns") or ([col] if col else []))
        dedup_expr = DeduplicateExpression(
            key_columns=keys,
            resolved_key_columns=keys,
            tie_breaker=DeduplicationTieBreaker.PRESERVE_FIRST,
            null_key_policy=NullKeyPolicy.GROUP_NULLS_TOGETHER,
        )
        return [TransformationStepIR(
            step_id=sid, dataset_name=ds, target_column=col,
            family=ActionFamily.DEDUPLICATE, kind=StepKind.DATASET_FILTER,
            expression=dedup_expr, effects=dedup_expr.derive_effects(ds),
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
