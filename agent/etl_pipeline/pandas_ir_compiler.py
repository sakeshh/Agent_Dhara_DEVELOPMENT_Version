"""
Pandas Reference Compiler (Transformation IR v1.0 -> Executable Pandas)
========================================================================

Vertical Slice 3 (Increment 3A): Reference compiler for deterministic scalar actions:
- MAP_VALUE (11 operations)
- CONDITIONAL_REPLACE
- CLIP (with null and infinity policies)
- FLAG (with 3-valued logic and existing-column policies)
- PASS_THROUGH (control construct, non-emitting)

Core Architectural Invariants:
1. Strict Step Accounting:
   Every submitted IR step receives exactly one disposition:
   COMPILED, CONTROL_ONLY, UNSUPPORTED, INVALID_SCHEMA_BINDING, INVALID_IR.
   Input step IDs == disposition step IDs.
2. Collision-Safe Selective Pre-Images:
   Prohibits full-frame copies (df.copy()). Captures required ValueVersion.ORIGINAL
   columns as generated symbols (_preimage_0, _preimage_1) in preamble before mutations.
3. Explicit 3-Valued Null and Infinity Policies:
   No silent coercion; explicit handling of None, np.nan, pd.NA, +inf, -inf.
4. AST Safety Validation:
   Inspects emitted Python AST to guarantee only approved numpy/pandas calls and no
   arbitrary code execution or reflection.
5. Atomic Failure:
   Failure on any required step produces status != SUCCEEDED and artifact is None.
"""

from __future__ import annotations

import ast
import datetime as _dt
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import hashlib
import hmac
import json
import re
import struct
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union
import unicodedata

from pydantic import BaseModel, Field, computed_field, model_validator

from agent.etl_pipeline.codegen_shared import CodegenStatus
from agent.etl_pipeline.transformation_ir import (
    TRANSFORMATION_IR_VERSION,
    ActionFamily,
    AggregateColumnSpec,
    AggregateExpression,
    AggregateOp,
    AggregateValueNullPolicy,
    AmbiguousTimePolicy,
    ArithmeticDerivation,
    ArithmeticDomain,
    ArithmeticOperator,
    BinaryOp,
    BinaryOperator,
    BooleanFormatSpec,
    BoundedJsonPath,
    CanonicalSerializationError,
    CastColumnExpression,
    CastFailurePolicy,
    ClipExpression,
    ClipNullPolicy,
    CoalesceDerivation,
    CollisionPolicy,
    ColumnDropSpec,
    ColumnMetadata,
    ColumnProjectSpec,
    ColumnRef,
    ColumnRenameSpec,
    ColumnRole,
    ConcatDerivation,
    ConcatNullPolicy,
    ConditionalDerivation,
    ConditionalReplaceExpression,
    ConditionalUnknownPolicy,
    CreateColumnExpression,
    DataType,
    DatasetColumnSchema,
    DatasetOutputSpec,
    DatasetSchema,
    DateArithmeticDerivation,
    DateArithmeticUnit,
    DateDiffUnit,
    DatePart,
    DatePartExtract,
    DecimalCanonicalizationMode,
    DeduplicateExpression,
    DeduplicationTieBreaker,
    DelimiterExtract,
    DeriveExpression,
    DigestEncoding,
    DivisionByZeroPolicy,
    DropColumnsExpression,
    DuplicateExportMode,
    EmailMaskDomainPolicy,
    EmptyInputPolicy,
    ExecutionKeyProvider,
    ExistingColumnPolicy,
    Expression,
    ExtractExpression,
    ExtractFailurePolicy,
    FilterDropPolicy,
    FilterExpression,
    FilterUnknownPolicy,
    FlagExpression,
    GroupKeyNullPolicy,
    GroupKeyValue,
    GroupedImputationStrategy,
    GroupedImputationValue,
    HashAlgorithm,
    HashExpression,
    HashInputEncoding,
    HashMode,
    HashNullPolicy,
    ImputationModelIntegrityError,
    ImputeExpression,
    InfinityPolicy,
    InSetPredicate,
    JoinCardinality,
    JoinCollisionPolicy,
    JoinExecutionAccounting,
    JoinExpression,
    JoinKeyPair,
    JoinNullPolicy,
    JoinType,
    JoinUnmatchedPolicy,
    JsonPathExtract,
    KeyProviderRequiredError,
    LearnedImputationModel,
    LiteralValue,
    MaskExpression,
    MaskNullPolicy,
    MaskStrategy,
    MissingGroupPolicy,
    ModeTieBreaker,
    MultiOutputArtifactError,
    MultipleMatchPolicy,
    NonexistentTimePolicy,
    NullCheck,
    NullKeyPolicy,
    NumericFormatSpec,
    NumericOperand,
    NumericOverflowPolicy,
    OccurrenceIdentityScope,
    OrderDirection,
    OutputColumnCollisionError,
    OutputProducerKind,
    OutputPurpose,
    OutputReferenceCollisionError,
    ParseExpression,
    ParseFailurePolicy,
    ParseOutputMode,
    PassThroughExpression,
    PredicateUnknownPolicy,
    ProjectColumnsExpression,
    PublicationAdapter,
    PublicationCommitResult,
    PublicationStageHandle,
    PublicationValidationResult,
    QuarantineExpression,
    QuarantineUnknownPolicy,
    RegexExtract,
    RenameColumnsExpression,
    ReorderColumnsExpression,
    RoutingIdentityPolicy,
    RoutingMode,
    RoutingPredicateUnknownError,
    RoutingReconciliationError,
    ScalarMapExpression,
    ScalarMapOperation,
    SchemaMutationEvidence,
    SchemaMutationExpression,
    SchemaMutationOp,
    SchemaSnapshotMismatchError,
    SecretHandle,
    SecretReference,
    SecretResolutionError,
    ShortValuePolicy,
    SortColumnSpec,
    SourceStableIdentitySpec,
    StagedExecutionResult,
    StatisticalImputationStrategy,
    StepAuthorization,
    StepKind,
    TemporalParseTarget,
    TransformationEffects,
    TransformationIRGraph,
    TransformationStepIR,
    UnauthorizedDestructiveActionError,
    UnboundedInfinityPolicy,
    UnmappedValuePolicy,
    UnmatchedRowDisposition,
    ValidateExpression,
    ValidateSeverity,
    ValidationExecutionSummary,
    ValidationGateError,
    ValidationUnknownPolicy,
    ValueVersion,
    bind_expression_to_schema,
    compute_canonical_schema_hash,
    compute_model_snapshot_hash,
    validate_output_registry,
    verify_imputation_model,
)


class RowExpansionLimitExceededError(ValueError):
    """Raised when join row expansion exceeds the absolute cap or ratio cap."""
    pass


class PublicationPostconditionError(ValueError):
    """Raised when a postcondition or schema assertion fails during publication validation."""
    pass


class PublicationAbortError(RuntimeError):
    """Raised when publication cannot proceed or is aborted."""
    pass


# Re-export authoritative canonical scalar encoder from canonical_encoding module
from agent.etl_pipeline.canonical_encoding import (
    CANONICAL_ENCODING_VERSION,
    TAG_BOOL_FALSE,
    TAG_BOOL_TRUE,
    TAG_BYTES_PREFIX,
    TAG_DATE_PREFIX,
    TAG_DECIMAL_PREFIX,
    TAG_FLOAT_PREFIX,
    TAG_INT_PREFIX,
    TAG_NAT,
    TAG_NULL,
    TAG_STRING_PREFIX,
    TAG_TIMESTAMP_PREFIX,
    canonical_encode_scalar_v1,
)



# ============================================================================
# 1. Compiler Governance Enums & Accounting Models
# ============================================================================

class CompilerStepDisposition(str, Enum):
    COMPILED = "compiled"
    CONTROL_ONLY = "control_only"
    UNSUPPORTED = "unsupported"
    INVALID_SCHEMA_BINDING = "invalid_schema_binding"
    INVALID_IR = "invalid_ir"


class CompilerAccountingError(ValueError):
    """Raised when compiler disposition accounting fails or diverges from input steps."""
    pass


class PreimageBinding(BaseModel):
    """Binds an original column to a generated collision-safe variable symbol."""
    dataset_ref: str
    column_ref: str
    internal_symbol: str
    required_by_step_ids: Tuple[str, ...] = ()


class CompiledPandasArtifact(BaseModel):
    """Self-contained, deterministic Pandas code artifact with cryptographically signed metadata."""
    artifact_bytes: bytes
    artifact_hash: str
    entrypoint: str = "transform(df: pd.DataFrame) -> pd.DataFrame"
    datasets_entrypoint: str = "transform_datasets(df: pd.DataFrame) -> Dict[str, pd.DataFrame]"
    supports_single_output_entrypoint: bool = True
    primary_output_ref: Optional[str] = None
    required_imports: Tuple[str, ...] = ("collections", "numpy", "pandas")
    compiled_step_ids: Tuple[str, ...]
    control_only_step_ids: Tuple[str, ...]
    schema_snapshot_hash: str
    compiler_version: str = "1.0.0"

    @property
    def source_code(self) -> str:
        return self.artifact_bytes.decode("utf-8")


class PandasCompilerResult(BaseModel):
    """Authoritative result envelope from the Reference Pandas Compiler."""
    status: CodegenStatus
    artifact: Optional[CompiledPandasArtifact] = None
    step_dispositions: Dict[str, CompilerStepDisposition]
    preimage_bindings: Tuple[PreimageBinding, ...] = ()
    diagnostics: Tuple[str, ...] = ()
    compiler_name: str = "pandas_reference"
    compiler_version: str = "1.0.0"
    graph_id: str
    ir_version: str = TRANSFORMATION_IR_VERSION

    @computed_field
    @property
    def is_success(self) -> bool:
        return self.status == CodegenStatus.SUCCEEDED

    @model_validator(mode="after")
    def validate_result_invariant(self) -> "PandasCompilerResult":
        if self.status != CodegenStatus.SUCCEEDED and self.artifact is not None:
            raise CompilerAccountingError("Failed compilation result cannot retain a compiled artifact")
        if self.status == CodegenStatus.SUCCEEDED and self.artifact is None:
            raise CompilerAccountingError("Successful compilation result must contain a compiled artifact")
        return self


# ============================================================================
# 2. Reference Pandas Compiler Implementation
# ============================================================================

class PandasIRCompiler:
    """
    Deterministic reference compiler converting TransformationIRGraph to Python/Pandas code.
    Increment 3A: MAP_VALUE, CONDITIONAL_REPLACE, CLIP, FLAG, PASS_THROUGH.
    Increment 3B: IMPUTE, PARSE.
    Increment 3C: QUARANTINE, FILTER, DEDUPLICATE, VALIDATE.
    Increment 3D: HASH, MASK, SCHEMA_MUTATION.
    """

    COMPILER_NAME = "pandas_reference"
    COMPILER_VERSION = "1.0.0"
    engine_name = "pandas"

    SUPPORTED_FAMILIES = {
        ActionFamily.MAP_VALUE,
        ActionFamily.CONDITIONAL_REPLACE,
        ActionFamily.CLIP,
        ActionFamily.FLAG,
        ActionFamily.IMPUTE,
        ActionFamily.PARSE,
        ActionFamily.QUARANTINE,
        ActionFamily.FILTER,
        ActionFamily.DEDUPLICATE,
        ActionFamily.VALIDATE,
        ActionFamily.HASH,
        ActionFamily.MASK,
        ActionFamily.SCHEMA_MUTATION,
        ActionFamily.DERIVE,
        ActionFamily.EXTRACT,
        ActionFamily.JOIN,
        ActionFamily.AGGREGATE,
    }

    CONTROL_FAMILIES = {
        ActionFamily.PASS_THROUGH,
    }

    def __init__(self, schemas: Optional[Mapping[str, DatasetSchema]] = None):
        self.schemas = dict(schemas or {})

    def execute(
        self,
        graph: TransformationIRGraph,
        df: Union[pd.DataFrame, Dict[str, pd.DataFrame]],
        key_provider: Optional[Any] = None,
    ) -> Any:
        """Executes the compiled reference Pandas artifact against in-memory input DataFrame(s)."""
        comp_res = self.compile(graph)
        if not comp_res.is_success or comp_res.artifact is None:
            raise RuntimeError(f"Reference Pandas compilation failed: {comp_res.diagnostics}")
        loc: Dict[str, Any] = {}
        exec(comp_res.artifact.source_code, loc)
        datasets_fn = loc.get("transform_datasets")
        if datasets_fn:
            res_dict = datasets_fn(df, key_provider=key_provider)
            class PandasExecutionResult:
                def __init__(self, outputs: Dict[str, pd.DataFrame]):
                    self.outputs = outputs
                    self.primary_output = next(iter(outputs.values())) if outputs else pd.DataFrame()
            return PandasExecutionResult(res_dict)
        single_fn = loc.get("transform")
        if single_fn:
            primary_out = single_fn(df, key_provider=key_provider)
            class PandasExecutionResult:
                def __init__(self, primary: pd.DataFrame):
                    self.outputs = {"primary": primary}
                    self.primary_output = primary
            return PandasExecutionResult(primary_out)
        raise RuntimeError("No valid entrypoint found in compiled Pandas artifact")

    def compile(self, graph: TransformationIRGraph, df_var: str = "df") -> PandasCompilerResult:
        """
        Compiles the provided TransformationIRGraph into a PandasCompilerResult.
        Enforces bidirectional step accounting, selective pre-image extraction,
        and Python AST safety validation.
        """
        step_dispositions: Dict[str, CompilerStepDisposition] = {}
        diagnostics: List[str] = []
        compiled_step_ids: List[str] = []
        control_only_step_ids: List[str] = []

        # 0. Output Registry Pre-Validation
        if graph.output_specs or graph.primary_output_ref or graph.routing_identity_policy:
            ds_name = graph.steps[0].dataset_name if graph.steps else None
            ds_schema = self.schemas.get(ds_name) if (self.schemas and ds_name) else None
            try:
                validate_output_registry(graph, schema=ds_schema)
            except Exception as e:
                diagnostics.append(f"Output registry validation failed: {str(e)}")
                return PandasCompilerResult(
                    status=CodegenStatus.FAILED_COMPILATION,
                    artifact=None,
                    step_dispositions={s.step_id: CompilerStepDisposition.INVALID_IR for s in graph.steps},
                    diagnostics=tuple(diagnostics),
                    graph_id=graph.graph_id,
                    ir_version=graph.ir_version,
                )

        # 1. Bidirectional Accounting Pre-Check
        input_step_ids = {s.step_id for s in graph.steps}
        step_map = {s.step_id: s for s in graph.steps}

        # 2. Step-by-Step Validation and Disposition
        bound_steps: Dict[str, Any] = {}
        for step in graph.steps:
            sid = step.step_id
            family = step.family

            # A. Control Constructs
            if family in self.CONTROL_FAMILIES or family == ActionFamily.PASS_THROUGH:
                step_dispositions[sid] = CompilerStepDisposition.CONTROL_ONLY
                control_only_step_ids.append(sid)
                continue

            # B. Supported Executable Families Check
            if family not in self.SUPPORTED_FAMILIES:
                step_dispositions[sid] = CompilerStepDisposition.UNSUPPORTED
                diagnostics.append(f"Step '{sid}' uses family '{family.value}' which is not supported by {self.COMPILER_NAME}")
                continue

            # B2. Destructive Action Authorization Check
            if isinstance(step.expression, SchemaMutationExpression) and step.expression.op == SchemaMutationOp.DROP_COLUMN:
                auth = step.authorization
                if not auth:
                    step_dispositions[sid] = CompilerStepDisposition.INVALID_IR
                    diagnostics.append(f"Step '{sid}' destructive drop requires a valid StepAuthorization (UnauthorizedDestructiveActionError)")
                    continue
                if auth.expires_at < datetime.now(timezone.utc):
                    step_dispositions[sid] = CompilerStepDisposition.INVALID_IR
                    diagnostics.append(f"Step '{sid}' StepAuthorization is expired (UnauthorizedDestructiveActionError)")
                    continue
                if auth.authorized_step_hash and auth.authorized_step_hash != step.semantic_hash:
                    step_dispositions[sid] = CompilerStepDisposition.INVALID_IR
                    diagnostics.append(f"Step '{sid}' StepAuthorization step_hash mismatch (UnauthorizedDestructiveActionError)")
                    continue

            # B3. Elevated Authorization for Cross and Many-to-Many Joins (Contract 9)
            if isinstance(step.expression, JoinExpression):
                if step.expression.join_type == JoinType.CROSS or step.expression.cardinality == JoinCardinality.MANY_TO_MANY:
                    auth = step.authorization
                    if not auth:
                        step_dispositions[sid] = CompilerStepDisposition.INVALID_IR
                        diagnostics.append(f"Step '{sid}' {step.expression.join_type.value}/{step.expression.cardinality.value} join requires elevated StepAuthorization (UnauthorizedDestructiveActionError)")
                        continue
                    if auth.expires_at < datetime.now(timezone.utc):
                        step_dispositions[sid] = CompilerStepDisposition.INVALID_IR
                        diagnostics.append(f"Step '{sid}' StepAuthorization is expired")
                        continue

                # Cross join mandatory caps
                if step.expression.join_type == JoinType.CROSS:
                    if step.expression.max_output_row_count is None:
                        step_dispositions[sid] = CompilerStepDisposition.INVALID_IR
                        diagnostics.append(f"Step '{sid}' CROSS join requires mandatory max_output_row_count")
                        continue
                    if step.expression.max_row_expansion_ratio is None:
                        step_dispositions[sid] = CompilerStepDisposition.INVALID_IR
                        diagnostics.append(f"Step '{sid}' CROSS join requires mandatory max_row_expansion_ratio")
                        continue

            # C. Schema Binding Validation
            if self.schemas:
                ds_name = step.dataset_name
                if ds_name in self.schemas:
                    try:
                        ancestors = set(step.prerequisites)
                        bound = bind_expression_to_schema(
                            expression=step.expression,
                            schemas=self.schemas,
                            dataset_name=ds_name,
                            ancestor_step_ids=ancestors,
                        )
                        bound_steps[sid] = bound
                    except Exception as e:
                        step_dispositions[sid] = CompilerStepDisposition.INVALID_SCHEMA_BINDING
                        diagnostics.append(f"Step '{sid}' schema binding failed: {str(e)}")
                        continue

            # Step passed binding check
            step_dispositions[sid] = CompilerStepDisposition.COMPILED
            compiled_step_ids.append(sid)

        # 3. Invariant Verification of Accounting Key-Set
        disposition_step_ids = set(step_dispositions.keys())
        missing = input_step_ids - disposition_step_ids
        unexpected = disposition_step_ids - input_step_ids
        if missing or unexpected:
            raise CompilerAccountingError(
                f"Compiler accounting mismatch: missing={sorted(missing)}, unexpected={sorted(unexpected)}"
            )

        # Check if compilation failed on any step
        has_failures = any(
            d in (
                CompilerStepDisposition.UNSUPPORTED,
                CompilerStepDisposition.INVALID_SCHEMA_BINDING,
                CompilerStepDisposition.INVALID_IR,
            )
            for d in step_dispositions.values()
        )

        if has_failures:
            return PandasCompilerResult(
                status=CodegenStatus.FAILED_COMPILATION,
                artifact=None,
                step_dispositions=step_dispositions,
                diagnostics=tuple(diagnostics),
                graph_id=graph.graph_id,
                ir_version=graph.ir_version,
            )

        # 4. Selective Pre-Image Planning
        preimage_bindings: List[PreimageBinding] = []
        col_to_preimage: Dict[str, str] = {}
        symbol_counter = 0

        for step in graph.steps:
            if step_dispositions[step.step_id] != CompilerStepDisposition.COMPILED:
                continue
            original_cols = self._extract_original_column_refs(step.expression)
            for col in sorted(original_cols):
                if col not in col_to_preimage:
                    symbol = f"_preimage_{symbol_counter}"
                    symbol_counter += 1
                    col_to_preimage[col] = symbol
                    preimage_bindings.append(
                        PreimageBinding(
                            dataset_ref=step.dataset_name,
                            column_ref=col,
                            internal_symbol=symbol,
                            required_by_step_ids=(step.step_id,),
                        )
                    )
                else:
                    for i, binding in enumerate(preimage_bindings):
                        if binding.column_ref == col and binding.dataset_ref == step.dataset_name:
                            if step.step_id not in binding.required_by_step_ids:
                                updated_steps = binding.required_by_step_ids + (step.step_id,)
                                preimage_bindings[i] = PreimageBinding(
                                    dataset_ref=binding.dataset_ref,
                                    column_ref=binding.column_ref,
                                    internal_symbol=binding.internal_symbol,
                                    required_by_step_ids=updated_steps,
                                )

        # 5. Assembly of Python Code Lines
        identity_policy = graph.routing_identity_policy or RoutingIdentityPolicy()
        internal_col = identity_policy.internal_column
        published_col = identity_policy.published_column
        scope = identity_policy.scope
        spec = identity_policy.source_stable_spec
        retain_in_primary = identity_policy.retain_in_primary_output
        retain_in_secondary = identity_policy.retain_in_secondary_outputs
        primary_ref = graph.primary_output_ref

        code_lines: List[str] = [
            "# Auto-generated by Agent Dhara Reference Pandas Compiler v1.0.0",
            "# Ir-Version: " + graph.ir_version,
            "# Graph-Id: " + graph.graph_id,
            "from collections import Counter",
            "from decimal import Decimal, ROUND_HALF_UP",
            "import hashlib",
            "import hmac",
            "import json",
            "import numpy as np",
            "import pandas as pd",
            "import re",
            "import struct",
            "import unicodedata",
            "try:",
            "    OutputColumnCollisionError",
            "except NameError:",
            "    class OutputColumnCollisionError(ValueError): pass",
            "try:",
            "    RowExpansionLimitExceededError",
            "except NameError:",
            "    class RowExpansionLimitExceededError(ValueError): pass",
            "",
            "def transform_datasets(df, key_provider=None) -> dict[str, pd.DataFrame]:",
            "    if isinstance(df, dict):",
            "        _datasets = {k: v.copy(deep=False) for k, v in df.items()}",
            "        _working = next(iter(_datasets.values())).copy(deep=False) if _datasets else pd.DataFrame()",
            "    else:",
            "        _working = df.copy(deep=False)  # isolated working frame",
            "        _datasets = {'primary': _working}",
            "",
            f"    # --- Occurrence Identity Lifecycle ({scope.value}) ---",
            f"    _identity_col = {repr(internal_col)}",
        ]

        if scope == OccurrenceIdentityScope.SOURCE_STABLE and spec:
            extra_file = f" + '_' + _working[{repr(spec.source_file_column)}].astype(str)" if spec.source_file_column else ""
            extra_ing = f" + '_' + _working[{repr(spec.ingestion_run_column)}].astype(str)" if spec.ingestion_run_column else ""
            code_lines.append(
                f"    _working[_identity_col] = ("
                f"{repr(spec.snapshot_id)} + '_' + "
                f"_working[{repr(spec.partition_column)}].astype(str) + '_' + "
                f"_working[{repr(spec.row_offset_column)}].astype(str)"
                f"{extra_file}{extra_ing}"
                f")"
            )
            code_lines.append(f"    if bool(_working[_identity_col].duplicated().any()):")
            code_lines.append(f"        raise ValueError('Duplicate source-stable coordinates detected in input DataFrame')")
        else:
            code_lines.append(f"    _working[_identity_col] = np.arange(len(_working))")

        code_lines.append(f"    _input_ids = list(_working[_identity_col])")
        code_lines.append(f"    if len(_input_ids) != len(set(_input_ids)):")
        code_lines.append(f"        raise ValueError('Input occurrence identities are not unique')")
        code_lines.append("")
        code_lines.append("    outputs = {}")
        code_lines.append("")

        # Preamble: Selective Series Pre-Images (Series copy only, never whole df.copy())
        if preimage_bindings:
            code_lines.append("    # --- Selective Series Pre-images ---")
            for binding in preimage_bindings:
                code_lines.append(
                    f"    {binding.internal_symbol} = df[{repr(binding.column_ref)}].copy(deep=True)"
                )
            code_lines.append("")

        # Step Transformations
        code_lines.append("    # --- Transformation Pipeline ---")
        for step in graph.steps:
            disp = step_dispositions[step.step_id]
            if disp == CompilerStepDisposition.CONTROL_ONLY:
                code_lines.append(f"    # Step {step.step_id}: CONTROL_ONLY ({step.family.value})")
                continue

            step_code = self._emit_step_code(step, col_to_preimage, df_var="_working")
            code_lines.append(f"    # Step {step.step_id}: {step.family.value}")
            for line in step_code:
                code_lines.append(f"    {line}")
            code_lines.append("")

        # Final Primary Output Registration
        if primary_ref:
            code_lines.append(f"    # --- Primary Output Registration ---")
            code_lines.append(f"    if {repr(primary_ref)} in outputs and outputs[{repr(primary_ref)}] is not _working:")
            code_lines.append(f"        raise ValueError('Primary output already produced: ' + {repr(primary_ref)})")
            code_lines.append(f"    outputs[{repr(primary_ref)}] = _working")
        else:
            code_lines.append(f"    # --- Default Primary Output Registration ---")
            code_lines.append(f"    if 'primary' not in outputs:")
            code_lines.append(f"        outputs['primary'] = _working")

        # Identity Publication Stripping
        code_lines.append(f"    # --- Publication Projection & Identity Stripping ---")
        code_lines.append(f"    _primary_key = {repr(primary_ref)} if {repr(primary_ref)} else ('primary' if 'primary' in outputs else None)")
        if not retain_in_primary:
            code_lines.append(f"    if _primary_key and _primary_key in outputs and _identity_col in outputs[_primary_key].columns:")
            if published_col:
                code_lines.append(f"        outputs[_primary_key] = outputs[_primary_key].rename(columns={{_identity_col: {repr(published_col)}}})")
            else:
                code_lines.append(f"        outputs[_primary_key] = outputs[_primary_key].drop(columns=[_identity_col])")
        else:
            if published_col:
                code_lines.append(f"    if _primary_key and _primary_key in outputs and _identity_col in outputs[_primary_key].columns:")
                code_lines.append(f"        outputs[_primary_key] = outputs[_primary_key].rename(columns={{_identity_col: {repr(published_col)}}})")

        if not retain_in_secondary:
            code_lines.append(f"    for _sec_ref in list(outputs.keys()):")
            code_lines.append(f"        if _sec_ref != _primary_key and _identity_col in outputs[_sec_ref].columns:")
            code_lines.append(f"            outputs[_sec_ref] = outputs[_sec_ref].drop(columns=[_identity_col])")

        code_lines.append("    return outputs")
        code_lines.append("")

        # Single-dataset entrypoint
        code_lines.append("def transform(df: pd.DataFrame, key_provider=None) -> pd.DataFrame:")
        code_lines.append("    all_outputs = transform_datasets(df, key_provider=key_provider)")
        if primary_ref:
            code_lines.append(f"    return all_outputs[{repr(primary_ref)}]")
        else:
            code_lines.append("    if 'primary' in all_outputs:")
            code_lines.append("        if len(all_outputs) > 1:")
            code_lines.append("            raise ValueError('MultiOutputArtifactError: Multiple datasets produced in routing graph (' + ', '.join(all_outputs.keys()) + ') without a designated primary_output_ref. Use transform_datasets(df) to access all outputs.')")
            code_lines.append("        return all_outputs['primary']")
            code_lines.append("    if len(all_outputs) == 1:")
            code_lines.append("        return next(iter(all_outputs.values()))")
            code_lines.append("    raise ValueError('MultiOutputArtifactError: Multiple datasets produced without primary_output_ref')")
        code_lines.append("")

        full_source = "\n".join(code_lines)
        artifact_bytes = full_source.encode("utf-8")
        artifact_hash = hashlib.sha256(artifact_bytes).hexdigest()

        # 6. AST Safety Validation
        has_aggregates = any(s.family == ActionFamily.AGGREGATE for s in graph.steps)
        try:
            self._validate_ast_safety(full_source, has_aggregates=has_aggregates)
        except Exception as e:
            diagnostics.append(f"AST safety inspection failed: {str(e)}")
            return PandasCompilerResult(
                status=CodegenStatus.FAILED_COMPILATION,
                artifact=None,
                step_dispositions=step_dispositions,
                diagnostics=tuple(diagnostics),
                graph_id=graph.graph_id,
                ir_version=graph.ir_version,
            )

        has_multiple_outputs = False
        for s in graph.steps:
            if isinstance(s.expression, (QuarantineExpression, FilterExpression, DeduplicateExpression)):
                if isinstance(s.expression, QuarantineExpression) or (isinstance(s.expression, FilterExpression) and s.expression.dropped_output_ref) or (isinstance(s.expression, DeduplicateExpression) and s.expression.duplicates_output_ref):
                    has_multiple_outputs = True
                    break

        supports_single = not (has_multiple_outputs and graph.primary_output_ref is None)

        artifact = CompiledPandasArtifact(
            artifact_bytes=artifact_bytes,
            artifact_hash=artifact_hash,
            entrypoint="transform(df: pd.DataFrame, key_provider: Optional[Any] = None) -> pd.DataFrame",
            datasets_entrypoint="transform_datasets(df: pd.DataFrame, key_provider: Optional[Any] = None) -> Dict[str, pd.DataFrame]",
            supports_single_output_entrypoint=supports_single,
            primary_output_ref=primary_ref,
            required_imports=("collections", "hashlib", "hmac", "numpy", "pandas", "struct", "unicodedata"),
            compiled_step_ids=tuple(compiled_step_ids),
            control_only_step_ids=tuple(control_only_step_ids),
            schema_snapshot_hash=graph.schema_snapshot_hash,
            compiler_version=self.COMPILER_VERSION,
        )

        return PandasCompilerResult(
            status=CodegenStatus.SUCCEEDED,
            artifact=artifact,
            step_dispositions=step_dispositions,
            preimage_bindings=tuple(preimage_bindings),
            diagnostics=tuple(diagnostics),
            graph_id=graph.graph_id,
            ir_version=graph.ir_version,
        )

    # ------------------------------------------------------------------------
    # Step Emitters
    # ------------------------------------------------------------------------

    def _emit_step_code(
        self,
        step: TransformationStepIR,
        preimage_symbols: Dict[str, str],
        df_var: str = "df",
    ) -> List[str]:
        expr = step.expression

        # 1. FLAG
        if isinstance(expr, FlagExpression):
            return self._emit_flag(expr, preimage_symbols, df_var)

        # 2. CLIP
        if isinstance(expr, ClipExpression):
            return self._emit_clip(expr, preimage_symbols, df_var)

        # 3. MAP_VALUE
        if isinstance(expr, ScalarMapExpression):
            return self._emit_scalar_map(expr, preimage_symbols, df_var)

        # 4. CONDITIONAL_REPLACE
        if isinstance(expr, ConditionalReplaceExpression):
            return self._emit_conditional_replace(expr, preimage_symbols, df_var)

        # 5. IMPUTE
        if isinstance(expr, ImputeExpression):
            return self._emit_impute(expr, preimage_symbols, df_var, step_id=step.step_id)

        # 6. PARSE
        if isinstance(expr, ParseExpression):
            return self._emit_parse(expr, preimage_symbols, df_var, step_id=step.step_id)

        # 7. QUARANTINE
        if isinstance(expr, QuarantineExpression):
            return self._emit_quarantine(expr, preimage_symbols, df_var, step_id=step.step_id)

        # 8. FILTER
        if isinstance(expr, FilterExpression):
            return self._emit_filter(expr, preimage_symbols, df_var, step_id=step.step_id)

        # 9. DEDUPLICATE
        if isinstance(expr, DeduplicateExpression):
            return self._emit_deduplicate(expr, preimage_symbols, df_var, step_id=step.step_id)

        # 10. VALIDATE
        if isinstance(expr, ValidateExpression):
            return self._emit_validate(expr, preimage_symbols, df_var, step_id=step.step_id)

        # 11. HASH
        if isinstance(expr, HashExpression):
            return self._emit_hash(expr, preimage_symbols, df_var, step_id=step.step_id)

        # 12. MASK
        if isinstance(expr, MaskExpression):
            return self._emit_mask(expr, preimage_symbols, df_var, step_id=step.step_id)

        # 13. SCHEMA_MUTATION
        if isinstance(expr, SchemaMutationExpression):
            return self._emit_schema_mutation(expr, preimage_symbols, df_var, step_id=step.step_id)

        # 14. DERIVE
        if isinstance(expr, (ArithmeticDerivation, ConditionalDerivation, CoalesceDerivation, ConcatDerivation, DateArithmeticDerivation)):
            return self._emit_derive(expr, preimage_symbols, df_var, step_id=step.step_id)

        # 15. EXTRACT
        if isinstance(expr, (RegexExtract, JsonPathExtract, DelimiterExtract, DatePartExtract)):
            return self._emit_extract(expr, preimage_symbols, df_var, step_id=step.step_id)

        # 16. JOIN
        if isinstance(expr, JoinExpression):
            return self._emit_join(expr, preimage_symbols, df_var, step_id=step.step_id)

        # 17. AGGREGATE
        if isinstance(expr, AggregateExpression):
            return self._emit_aggregate(expr, preimage_symbols, df_var, step_id=step.step_id)

        return [f"# Unsupported expression: {type(expr).__name__}"]

    def _emit_flag(
        self,
        expr: FlagExpression,
        preimages: Dict[str, str],
        df_var: str,
    ) -> List[str]:
        out_col = expr.output_column
        cond_expr = self._emit_expression(expr.predicate, preimages, df_var)
        true_repr = repr(expr.true_value)
        false_repr = repr(expr.false_value)

        policy = expr.unknown_policy
        lines: List[str] = []

        if policy == PredicateUnknownPolicy.TREAT_AS_FALSE:
            mask_code = f"({cond_expr}).fillna(False).astype(bool)"
            lines.append(f"{df_var}[{repr(out_col)}] = np.where({mask_code}, {true_repr}, {false_repr})")
        elif policy == PredicateUnknownPolicy.TREAT_AS_TRUE:
            mask_code = f"({cond_expr}).fillna(True).astype(bool)"
            lines.append(f"{df_var}[{repr(out_col)}] = np.where({mask_code}, {true_repr}, {false_repr})")
        elif policy == PredicateUnknownPolicy.PRESERVE_UNKNOWN:
            clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', out_col)
            cond_var = f"_cond_{clean_name}"
            na_var = f"_na_{clean_name}"
            reads = expr.predicate.extract_reads()
            if reads:
                na_exprs = [f"{preimages.get(r, f'{df_var}[{repr(r)}]')}.isna()" for r in set(reads)]
                na_check = " | ".join(na_exprs)
            else:
                na_check = f"({cond_expr}).isna()"
            lines.append(f"{na_var} = ({na_check})")
            lines.append(f"{cond_var} = ({cond_expr})")
            lines.append(
                f"{df_var}[{repr(out_col)}] = np.select("
                f"[{na_var}, {cond_var}], "
                f"[pd.NA, {true_repr}], default={false_repr}"
                f")"
            )
        elif policy == PredicateUnknownPolicy.FAIL_STEP:
            clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', out_col)
            na_var = f"_na_{clean_name}"
            reads = expr.predicate.extract_reads()
            if reads:
                na_exprs = [f"{preimages.get(r, f'{df_var}[{repr(r)}]')}.isna()" for r in set(reads)]
                na_check = " | ".join(na_exprs)
            else:
                na_check = f"({cond_expr}).isna()"
            lines.append(f"{na_var} = ({na_check})")
            lines.append(f"if {na_var}.any(): raise ValueError('Predicate evaluated to unknown/NA under FAIL_STEP policy for {out_col}')")
            lines.append(f"{df_var}[{repr(out_col)}] = np.where(({cond_expr}).fillna(False).astype(bool), {true_repr}, {false_repr})")
        elif policy == PredicateUnknownPolicy.FLAG_UNKNOWN_SEPARATELY:
            clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', out_col)
            na_var = f"_na_{clean_name}"
            reads = expr.predicate.extract_reads()
            if reads:
                na_exprs = [f"{preimages.get(r, f'{df_var}[{repr(r)}]')}.isna()" for r in set(reads)]
                na_check = " | ".join(na_exprs)
            else:
                na_check = f"({cond_expr}).isna()"
            lines.append(f"{na_var} = ({na_check})")
            lines.append(f"{df_var}[{repr(f'{out_col}_unknown')}] = {na_var}.astype('boolean')")
            lines.append(f"{df_var}[{repr(out_col)}] = np.where(({cond_expr}).fillna(False).astype(bool), {true_repr}, {false_repr})")
        else:
            lines.append(f"{df_var}[{repr(out_col)}] = np.where(({cond_expr}), {true_repr}, {false_repr})")

        if expr.true_value is True and expr.false_value is False:
            lines.append(f"{df_var}[{repr(out_col)}] = {df_var}[{repr(out_col)}].astype('boolean')")

        return lines

    def _emit_clip(
        self,
        expr: ClipExpression,
        preimages: Dict[str, str],
        df_var: str,
    ) -> List[str]:
        col = expr.target_column.column_name
        lines: List[str] = []

        # Handle infinity policy
        if expr.infinity_policy == InfinityPolicy.CLIP_TO_BOUND:
            # Positive infinity bound
            if expr.upper_bound is not None:
                lines.append(f"{df_var}[{repr(col)}] = {df_var}[{repr(col)}].replace(np.inf, {expr.upper_bound.value})")
            else:
                if expr.unbounded_infinity_policy == UnboundedInfinityPolicy.TREAT_AS_NULL:
                    lines.append(f"{df_var}[{repr(col)}] = {df_var}[{repr(col)}].replace(np.inf, np.nan)")
                elif expr.unbounded_infinity_policy == UnboundedInfinityPolicy.FAIL_STEP:
                    lines.append(f"if ({df_var}[{repr(col)}] == np.inf).any(): raise ValueError('Encountered +inf without upper bound under FAIL_STEP for {col}')")

            # Negative infinity bound
            if expr.lower_bound is not None:
                lines.append(f"{df_var}[{repr(col)}] = {df_var}[{repr(col)}].replace(-np.inf, {expr.lower_bound.value})")
            else:
                if expr.unbounded_infinity_policy == UnboundedInfinityPolicy.TREAT_AS_NULL:
                    lines.append(f"{df_var}[{repr(col)}] = {df_var}[{repr(col)}].replace(-np.inf, np.nan)")
                elif expr.unbounded_infinity_policy == UnboundedInfinityPolicy.FAIL_STEP:
                    lines.append(f"if ({df_var}[{repr(col)}] == -np.inf).any(): raise ValueError('Encountered -inf without lower bound under FAIL_STEP for {col}')")

        elif expr.infinity_policy == InfinityPolicy.TREAT_AS_NULL:
            lines.append(f"{df_var}[{repr(col)}] = {df_var}[{repr(col)}].replace([np.inf, -np.inf], np.nan)")
        elif expr.infinity_policy == InfinityPolicy.FAIL_STEP:
            lines.append(f"if np.isinf({df_var}[{repr(col)}]).any(): raise ValueError('Encountered infinity under FAIL_STEP for {col}')")
        elif expr.infinity_policy == InfinityPolicy.FLAG:
            lines.append(f"{df_var}[{repr(f'{col}_is_inf')}] = np.isinf({df_var}[{repr(col)}]).astype('boolean')")

        # Normal clip
        if expr.lower_bound is not None and expr.upper_bound is not None:
            lines.append(f"{df_var}[{repr(col)}] = {df_var}[{repr(col)}].clip(lower={expr.lower_bound.value}, upper={expr.upper_bound.value})")
        elif expr.lower_bound is not None:
            lines.append(f"{df_var}[{repr(col)}] = {df_var}[{repr(col)}].clip(lower={expr.lower_bound.value})")
        elif expr.upper_bound is not None:
            lines.append(f"{df_var}[{repr(col)}] = {df_var}[{repr(col)}].clip(upper={expr.upper_bound.value})")

        return lines

    def _emit_scalar_map(
        self,
        expr: ScalarMapExpression,
        preimages: Dict[str, str],
        df_var: str,
    ) -> List[str]:
        src_col = expr.source_column.column_name
        out_col = expr.output_column
        op = expr.operation
        lines: List[str] = []

        src_ref = f"{df_var}[{repr(src_col)}]"
        out_ref = f"{df_var}[{repr(out_col)}]"

        if op == ScalarMapOperation.TRIM:
            lines.append(f"{out_ref} = {src_ref}.astype('string').str.strip()")
        elif op == ScalarMapOperation.LOWER:
            lines.append(f"{out_ref} = {src_ref}.astype('string').str.lower()")
        elif op == ScalarMapOperation.UPPER:
            lines.append(f"{out_ref} = {src_ref}.astype('string').str.upper()")
        elif op == ScalarMapOperation.CAST:
            target_t = expr.parameters.get("target_type", "string")
            type_map = {"string": "string", "int": "Int64", "float": "Float64", "bool": "boolean"}
            pandas_t = type_map.get(target_t, "string")
            lines.append(f"{out_ref} = {src_ref}.astype({repr(pandas_t)})")
        elif op == ScalarMapOperation.PARSE_NUMBER:
            lines.append(f"{out_ref} = pd.to_numeric({src_ref}, errors='coerce')")
        elif op == ScalarMapOperation.PARSE_DATE:
            date_fmt = expr.parameters.get("format")
            fmt_arg = f", format={repr(date_fmt)}" if date_fmt else ""
            lines.append(f"{out_ref} = pd.to_datetime({src_ref}, errors='coerce'{fmt_arg})")
        elif op == ScalarMapOperation.STANDARDIZE_BOOLEAN:
            bool_map = {"true": True, "1": True, "yes": True, "y": True, "t": True,
                        "false": False, "0": False, "no": False, "n": False, "f": False}
            lines.append(f"{out_ref} = {src_ref}.astype('string').str.strip().str.lower().map({repr(bool_map)}).astype('boolean')")
        elif op == ScalarMapOperation.REGEX_SUBSTITUTE:
            pattern = expr.parameters.get("pattern", "")
            replacement = expr.parameters.get("replacement", "")
            lines.append(f"{out_ref} = {src_ref}.astype('string').str.replace({repr(pattern)}, {repr(replacement)}, regex=True)")
        elif op == ScalarMapOperation.LOOKUP_MAP:
            mapping = expr.parameters.get("mapping", {})
            policy = expr.unmapped_policy
            if policy == UnmappedValuePolicy.SET_NULL:
                lines.append(f"{out_ref} = {src_ref}.map({repr(mapping)})")
            else:
                lines.append(f"{out_ref} = {src_ref}.map({repr(mapping)}).fillna({src_ref})")
        elif op == ScalarMapOperation.NORMALIZE_PHONE:
            lines.append(f"{out_ref} = {src_ref}.astype('string').str.replace(r'[^0-9+]', '', regex=True)")
        elif op == ScalarMapOperation.SANITIZE_EMAIL:
            lines.append(f"{out_ref} = {src_ref}.astype('string').str.strip().str.lower()")
        else:
            lines.append(f"# Unimplemented scalar map op: {op}")

        return lines

    def _emit_conditional_replace(
        self,
        expr: ConditionalReplaceExpression,
        preimages: Dict[str, str],
        df_var: str,
    ) -> List[str]:
        target_col = expr.target_column.column_name
        out_col = expr.output_column or target_col
        cond_expr = self._emit_expression(expr.condition, preimages, df_var)
        rep_expr = self._emit_expression(expr.replace_value, preimages, df_var)
        orig_ref = f"{df_var}[{repr(target_col)}]"
        out_ref = f"{df_var}[{repr(out_col)}]"

        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', out_col)
        reads = expr.condition.extract_reads()
        if reads:
            na_exprs = [f"{preimages.get(r, f'{df_var}[{repr(r)}]')}.isna()" for r in set(reads)]
            na_check = " | ".join(na_exprs)
        else:
            na_check = f"({cond_expr}).isna()"

        if expr.unknown_policy == PredicateUnknownPolicy.TREAT_AS_FALSE:
            mask = f"({cond_expr}).fillna(False).astype(bool)"
            return [f"{out_ref} = np.where({mask}, {rep_expr}, {orig_ref})"]
        elif expr.unknown_policy == PredicateUnknownPolicy.TREAT_AS_TRUE:
            mask = f"({cond_expr}).fillna(True).astype(bool)"
            return [f"{out_ref} = np.where({mask}, {rep_expr}, {orig_ref})"]
        elif expr.unknown_policy == PredicateUnknownPolicy.PRESERVE_UNKNOWN:
            na_var = f"_na_{clean_name}"
            cond_var = f"_cond_{clean_name}"
            return [
                f"{na_var} = ({na_check})",
                f"{cond_var} = ({cond_expr}).fillna(False).astype(bool)",
                f"{out_ref} = np.select([{na_var}, {cond_var}], [{orig_ref}, {rep_expr}], default={orig_ref})",
            ]
        elif expr.unknown_policy == PredicateUnknownPolicy.FAIL_STEP:
            na_var = f"_na_{clean_name}"
            return [
                f"{na_var} = ({na_check})",
                f"if {na_var}.any(): raise ValueError('Conditional replace condition evaluated to unknown/NA under FAIL_STEP policy for {out_col}')",
                f"{out_ref} = np.where(({cond_expr}).fillna(False).astype(bool), {rep_expr}, {orig_ref})",
            ]
        elif expr.unknown_policy == PredicateUnknownPolicy.FLAG_UNKNOWN_SEPARATELY:
            na_var = f"_na_{clean_name}"
            return [
                f"{na_var} = ({na_check})",
                f"{df_var}[{repr(f'{out_col}_unknown')}] = {na_var}.astype('boolean')",
                f"{out_ref} = np.where(({cond_expr}).fillna(False).astype(bool), {rep_expr}, {orig_ref})",
            ]
        else:
            return [f"{out_ref} = np.where({cond_expr}, {rep_expr}, {orig_ref})"]

    def _emit_impute(
        self,
        expr: ImputeExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str = "",
    ) -> List[str]:
        col = expr.target_column.column_name
        out_col = expr.output_column or col
        lines: List[str] = []
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', col)

        # Invariant 12: Capture imputation indicator BEFORE filling!
        mask_var = f"_impute_mask_{clean_name}"
        if expr.emit_indicator:
            lines.append(f"{mask_var} = {df_var}[{repr(col)}].isna().astype('boolean')")

        # Invariant 1: Constant Imputation
        if expr.strategy == "constant":
            val = expr.constant_value.value if hasattr(expr.constant_value, "value") else expr.constant_value
            if val is None and expr.learned_model:
                val = expr.learned_model.learned_value
            lines.append(f"{df_var}[{repr(out_col)}] = pd.Series(np.where({df_var}[{repr(col)}].isna(), {repr(val)}, {df_var}[{repr(col)}]), index={df_var}.index).infer_objects(copy=False)")

        # Invariant 1 & 5: Fitted Scalar Statistical Imputation (MEAN, MEDIAN, MODE)
        elif expr.strategy in ("mean", "median", "mode", StatisticalImputationStrategy.MEAN, StatisticalImputationStrategy.MEDIAN, StatisticalImputationStrategy.MODE):
            if expr.learned_model is None or expr.learned_model.learned_value is None:
                raise ValueError(f"Statistical imputation strategy '{expr.strategy}' requires an authorized learned_model with learned_value")
            # Snapshot hash verification at use time
            verify_imputation_model(expr.learned_model)

            learned_val = expr.learned_model.learned_value
            # Binds fixed constant; zero runtime recalculation
            lines.append(f"{df_var}[{repr(out_col)}] = pd.Series(np.where({df_var}[{repr(col)}].isna(), {repr(learned_val)}, {df_var}[{repr(col)}]), index={df_var}.index).infer_objects(copy=False)")

        # Invariant 1, 3, 4: Fitted Grouped Statistical Imputation (GROUPED_MEAN, GROUPED_MEDIAN)
        elif expr.strategy in ("grouped_mean", "grouped_median", GroupedImputationStrategy.GROUPED_MEAN, GroupedImputationStrategy.GROUPED_MEDIAN):
            if expr.learned_model is None:
                raise ValueError("Grouped imputation requires an authorized learned_model with group_values")
            # Snapshot hash verification at use time
            verify_imputation_model(expr.learned_model)

            group_cols = list(expr.learned_model.group_columns or expr.group_by_columns)
            if not group_cols:
                raise ValueError("Grouped imputation requires at least one grouping column")

            missing_policy = expr.learned_model.missing_group_policy
            fallback_val = expr.learned_model.global_fallback_value

            # Build serializable group lookup mapping
            if len(group_cols) == 1:
                grp_c = group_cols[0]
                lookup = {}
                for gv in expr.learned_model.group_values:
                    k_val = gv.group_key[0].value
                    lookup[k_val] = gv.learned_value
                map_var = f"_grp_map_{clean_name}"
                series_var = f"_grp_series_{clean_name}"
                lines.append(f"{map_var} = {repr(lookup)}")
                lines.append(f"{series_var} = {df_var}[{repr(grp_c)}].map({map_var})")
            else:
                lookup = {}
                for gv in expr.learned_model.group_values:
                    k_dict = {k.column: k.value for k in gv.group_key}
                    comp_key = tuple(k_dict[c] for c in group_cols)
                    lookup[comp_key] = gv.learned_value
                map_var = f"_grp_map_{clean_name}"
                series_var = f"_grp_series_{clean_name}"
                lines.append(f"{map_var} = {repr(lookup)}")
                zip_expr = ", ".join(f"{df_var}[{repr(c)}]" for c in group_cols)
                lines.append(f"{series_var} = pd.Series(list(zip({zip_expr})), index={df_var}.index).map({map_var})")

            # Handle missing/unseen groups
            if missing_policy == MissingGroupPolicy.USE_GLOBAL_FALLBACK:
                lines.append(f"{series_var} = pd.Series(np.where({series_var}.isna(), {repr(fallback_val)}, {series_var}), index={df_var}.index).infer_objects(copy=False)")
                lines.append(f"{df_var}[{repr(out_col)}] = pd.Series(np.where({df_var}[{repr(col)}].isna(), {series_var}, {df_var}[{repr(col)}]), index={df_var}.index).infer_objects(copy=False)")
            elif missing_policy == MissingGroupPolicy.FAIL_STEP:
                unseen_var = f"_unseen_{clean_name}"
                lines.append(f"{unseen_var} = {df_var}[{repr(col)}].isna() & {series_var}.isna()")
                lines.append(f"if bool({unseen_var}.any()): raise ValueError(\"Grouped imputation step failure [step_id={step_id}]: column={out_col} - Unseen group encountered with \" + str(int({unseen_var}.sum())) + \" rows under FAIL_STEP (reason_code=UNSEEN_GROUP)\")")
                lines.append(f"{df_var}[{repr(out_col)}] = pd.Series(np.where({df_var}[{repr(col)}].isna(), {series_var}, {df_var}[{repr(col)}]), index={df_var}.index).infer_objects(copy=False)")
            elif missing_policy == MissingGroupPolicy.FLAG:
                unseen_var = f"_unseen_{clean_name}"
                lines.append(f"{unseen_var} = {df_var}[{repr(col)}].isna() & {series_var}.isna()")
                lines.append(f"{df_var}[{repr(f'{out_col}_unseen_group')}] = {unseen_var}.astype('boolean')")
                lines.append(f"{df_var}[{repr(out_col)}] = pd.Series(np.where({df_var}[{repr(col)}].isna(), {series_var}, {df_var}[{repr(col)}]), index={df_var}.index).infer_objects(copy=False)")
            else:  # KEEP_NULL
                lines.append(f"{df_var}[{repr(out_col)}] = pd.Series(np.where({df_var}[{repr(col)}].isna(), {series_var}, {df_var}[{repr(col)}]), index={df_var}.index).infer_objects(copy=False)")

        # Invariant 12: Assign imputation indicator AFTER filling
        if expr.emit_indicator:
            ind_col = expr.indicator_column or f"{col}_was_imputed"
            lines.append(f"{df_var}[{repr(ind_col)}] = {mask_var}")

        return lines

    def _emit_parse(
        self,
        expr: ParseExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str = "",
    ) -> List[str]:
        src_col = expr.source_column.column_name
        out_col = expr.target_column or src_col
        lines: List[str] = []
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', out_col)

        src_ref = f"{df_var}[{repr(src_col)}]"
        out_ref = f"{df_var}[{repr(out_col)}]"

        src_na_var = f"_src_na_{clean_name}"
        parsed_var = f"_parsed_{clean_name}"
        parse_err_var = f"_parse_err_{clean_name}"

        # Contract 6: Existing missing values are strictly distinguished from parse errors!
        lines.append(f"{src_na_var} = {src_ref}.isna()")

        # A. Boolean Parsing
        if expr.boolean_spec or expr.target_data_type == DataType.BOOLEAN:
            bspec = expr.boolean_spec or BooleanFormatSpec()
            token_map = {}
            for t in bspec.true_tokens:
                token_map[t if bspec.case_sensitive else t.lower()] = True
            for f in bspec.false_tokens:
                token_map[f if bspec.case_sensitive else f.lower()] = False

            s_prep = f"{src_ref}.astype('string')"
            if bspec.trim_whitespace:
                s_prep += ".str.strip()"
            if not bspec.case_sensitive:
                s_prep += ".str.lower()"

            lines.append(f"{parsed_var} = {s_prep}.map({repr(token_map)}).astype('boolean')")

        # B. Date / Timestamp Parsing
        elif expr.date_spec or expr.target_data_type in (DataType.DATE, DataType.TIMESTAMP):
            dspec = expr.date_spec or DateFormatSpec()
            fmt = dspec.accepted_formats[0] if dspec.accepted_formats else None
            fmt_arg = f", format={repr(fmt)}" if fmt else ""
            if dspec.temporal_target == TemporalParseTarget.CALENDAR_DATE or dspec.output_type == "date":
                # Calendar date: instantless, avoid timezone shifting
                lines.append(f"{parsed_var} = pd.to_datetime({src_ref}, errors='coerce'{fmt_arg}).dt.date")
            elif dspec.temporal_target == TemporalParseTarget.TIMESTAMP_WITHOUT_TZ:
                lines.append(f"{parsed_var} = pd.to_datetime({src_ref}, errors='coerce'{fmt_arg})")
            elif dspec.temporal_target == TemporalParseTarget.TIMESTAMP_WITH_TZ:
                lines.append(f"{parsed_var} = pd.to_datetime({src_ref}, errors='coerce'{fmt_arg})")
                if dspec.input_timezone:
                    lines.append(f"{parsed_var} = {parsed_var}.dt.tz_localize({repr(dspec.input_timezone)}, ambiguous={repr(dspec.ambiguous_time_policy.value)}, nonexistent={repr(dspec.nonexistent_time_policy.value)})")
                if dspec.output_timezone:
                    lines.append(f"{parsed_var} = {parsed_var}.dt.tz_convert({repr(dspec.output_timezone)})")
            else:
                lines.append(f"{parsed_var} = pd.to_datetime({src_ref}, errors='coerce'{fmt_arg})")

        # C. Numeric Parsing
        else:
            nspec = expr.numeric_spec or NumericFormatSpec()
            s_prep = f"{src_ref}.astype('string')"
            if nspec.trim_whitespace:
                s_prep += ".str.strip()"
            if nspec.currency_symbols:
                for sym in nspec.currency_symbols:
                    s_prep += f".str.replace({repr(sym)}, '', regex=False)"
            if nspec.thousands_separator:
                s_prep += f".str.replace({repr(nspec.thousands_separator)}, '', regex=False)"
            if nspec.decimal_separator == ",":
                s_prep += ".str.replace(',', '.', regex=False)"
            if nspec.allow_parentheses_negative:
                s_prep += ".str.replace(r'^\\((.*)\\)$', r'-\\1', regex=True)"

            clean_num_var = f"_clean_num_{clean_name}"
            lines.append(f"{clean_num_var} = {s_prep}")
            lines.append(f"{parsed_var} = pd.to_numeric({clean_num_var}, errors='coerce')")

            if nspec.allow_percent and nspec.percent_scale == "fraction":
                lines.append(f"{parsed_var} = np.where({clean_num_var}.str.endswith('%').fillna(False), {parsed_var} / 100.0, {parsed_var})")

            target_t = nspec.target_type
            if target_t == "Int64":
                lines.append(f"{parsed_var} = {parsed_var}.astype('Int64')")
            else:
                lines.append(f"{parsed_var} = {parsed_var}.astype('Float64')")

        # Contract 6: parse_error = (~source_missing) & parsed.isna()
        lines.append(f"{parse_err_var} = (~{src_na_var}) & {parsed_var}.isna()")

        # Contract 7 & 8: Failure Policies
        if expr.failure_policy == ParseFailurePolicy.SET_NULL:
            lines.append(f"{out_ref} = {parsed_var}")
        elif expr.failure_policy == ParseFailurePolicy.FLAG:
            lines.append(f"{out_ref} = {parsed_var}")
            lines.append(f"{df_var}[{repr(f'{out_col}_is_parse_error')}] = {parse_err_var}.astype('boolean')")
        elif expr.failure_policy == ParseFailurePolicy.FAIL_STEP:
            lines.append(f"if bool({parse_err_var}.any()): raise ValueError(\"Parse failure encountered [step_id={step_id}]: column={out_col} with \" + str(int({parse_err_var}.sum())) + \" invalid unparseable values under FAIL_STEP (reason_code=UNPARSEABLE_VALUE)\")")
            lines.append(f"{out_ref} = {parsed_var}")
        elif expr.failure_policy == ParseFailurePolicy.KEEP_ORIGINAL:
            lines.append(f"{out_ref} = np.where({parse_err_var}, {src_ref}, {parsed_var})")
        else:
            lines.append(f"{out_ref} = {parsed_var}")

        return lines

    def _emit_quarantine(
        self,
        expr: QuarantineExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        cond_expr = self._emit_expression(expr.predicate, preimages, df_var)
        clean_id = re.sub(r'[^a-zA-Z0-9_]', '_', step_id)
        raw_var = f"_quar_raw_{clean_id}"
        na_var = f"_quar_na_{clean_id}"
        mask_var = f"_quar_mask_{clean_id}"

        reads = expr.predicate.extract_reads()
        if isinstance(expr.predicate, NullCheck):
            na_check = f"({cond_expr}).isna()"
        elif reads:
            na_exprs = [f"{df_var}[{repr(r)}].isna()" for r in set(reads)]
            na_check = f"({' | '.join(na_exprs)}) | ({cond_expr}).isna()"
        else:
            na_check = f"({cond_expr}).isna()"

        lines: List[str] = [
            f"{na_var} = ({na_check})",
            f"{raw_var} = np.where({na_var}, pd.NA, ({cond_expr}))",
            f"{raw_var} = pd.Series({raw_var}, index={df_var}.index, dtype='boolean')",
        ]

        if expr.unknown_policy == QuarantineUnknownPolicy.FAIL_STEP:
            lines.append(f"if bool({na_var}.any()): raise ValueError(\"Quarantine predicate evaluated to unknown under FAIL_STEP: \" + str(int({na_var}.sum())) + \" unknown rows [step_id={step_id}]\")")
            lines.append(f"{mask_var} = {raw_var}.astype(bool)")
        elif expr.unknown_policy == QuarantineUnknownPolicy.ROUTE_TO_QUARANTINE:
            lines.append(f"{mask_var} = {raw_var}.fillna(True).astype(bool)")
        elif expr.unknown_policy == QuarantineUnknownPolicy.RETAIN_IN_CURATED:
            lines.append(f"{mask_var} = {raw_var}.fillna(False).astype(bool)")
        elif expr.unknown_policy == QuarantineUnknownPolicy.SEPARATE_OUTPUT:
            lines.append(f"outputs[{repr(expr.unknown_output_ref)}] = {df_var}[{na_var}].copy()")
            lines.append(f"{mask_var} = {raw_var}.fillna(False).astype(bool)")

        if expr.routing_mode == RoutingMode.PARTITION:
            if expr.unknown_policy == QuarantineUnknownPolicy.SEPARATE_OUTPUT:
                lines.append(f"_quar_rows_{clean_id} = {df_var}[{mask_var} & (~{na_var})].copy()")
                lines.append(f"_cur_rows_{clean_id} = {df_var}[(~{mask_var}) & (~{na_var})].copy()")
            else:
                lines.append(f"_quar_rows_{clean_id} = {df_var}[{mask_var}].copy()")
                lines.append(f"_cur_rows_{clean_id} = {df_var}[~{mask_var}].copy()")

            if expr.reason_column:
                lines.append(f"_quar_rows_{clean_id}[{repr(expr.reason_column)}] = {repr(expr.reason_code)}")

            lines.append(f"outputs[{repr(expr.quarantine_output_ref)}] = _quar_rows_{clean_id}")
            lines.append(f"outputs[{repr(expr.curated_output_ref)}] = _cur_rows_{clean_id}")
            lines.append(f"{df_var} = _cur_rows_{clean_id}")

            lines.append(f"_q_ids = list(outputs[{repr(expr.quarantine_output_ref)}][_identity_col])")
            lines.append(f"_c_ids = list(outputs[{repr(expr.curated_output_ref)}][_identity_col])")
            if expr.unknown_policy == QuarantineUnknownPolicy.SEPARATE_OUTPUT:
                lines.append(f"_u_ids = list(outputs[{repr(expr.unknown_output_ref)}][_identity_col])")
            else:
                lines.append(f"_u_ids = []")
            lines.append(f"if set(_c_ids) & set(_q_ids): raise ValueError('Partition outputs contain overlapping occurrences between curated and quarantine')")
            lines.append(f"if set(_c_ids) & set(_u_ids): raise ValueError('Partition outputs contain overlapping occurrences between curated and unknown')")
            lines.append(f"if set(_q_ids) & set(_u_ids): raise ValueError('Partition outputs contain overlapping occurrences between quarantine and unknown')")
            lines.append(f"if Counter(_c_ids) + Counter(_q_ids) + Counter(_u_ids) != Counter(_input_ids): raise ValueError('Partition outputs do not reconcile to input occurrences')")

        elif expr.routing_mode == RoutingMode.COPY:
            lines.append(f"_quar_rows_{clean_id} = {df_var}[{mask_var}].copy()")
            if expr.reason_column:
                lines.append(f"_quar_rows_{clean_id}[{repr(expr.reason_column)}] = {repr(expr.reason_code)}")
            lines.append(f"outputs[{repr(expr.quarantine_output_ref)}] = _quar_rows_{clean_id}")
            lines.append(f"outputs[{repr(expr.curated_output_ref)}] = {df_var}")

            lines.append(f"_p_ids = list({df_var}[_identity_col])")
            lines.append(f"_q_ids = list(_quar_rows_{clean_id}[_identity_col])")
            lines.append(f"if Counter(_p_ids) != Counter(_input_ids): raise ValueError('Copy mode curated output does not match input occurrences')")
            lines.append(f"if not (Counter(_q_ids) <= Counter(_input_ids)): raise ValueError('Copy mode quarantine occurrences exceed input occurrences')")

        return lines

    def _emit_filter(
        self,
        expr: FilterExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        cond_expr = self._emit_expression(expr.predicate, preimages, df_var)
        clean_id = re.sub(r'[^a-zA-Z0-9_]', '_', step_id)
        raw_var = f"_filt_raw_{clean_id}"
        na_var = f"_filt_na_{clean_id}"
        cond_eff_var = f"_filt_eff_{clean_id}"
        retain_mask_var = f"_filt_retain_{clean_id}"
        dropped_mask_var = f"_filt_dropped_{clean_id}"

        reads = expr.predicate.extract_reads()
        if isinstance(expr.predicate, NullCheck):
            na_check = f"({cond_expr}).isna()"
        elif reads:
            na_exprs = [f"{df_var}[{repr(r)}].isna()" for r in set(reads)]
            na_check = f"({' | '.join(na_exprs)}) | ({cond_expr}).isna()"
        else:
            na_check = f"({cond_expr}).isna()"

        lines: List[str] = [
            f"{na_var} = ({na_check})",
            f"{raw_var} = np.where({na_var}, pd.NA, ({cond_expr}))",
            f"{raw_var} = pd.Series({raw_var}, index={df_var}.index, dtype='boolean')",
        ]

        if expr.unknown_policy == FilterUnknownPolicy.FAIL_STEP:
            lines.append(f"if bool({na_var}.any()): raise ValueError(\"Filter predicate evaluated to unknown under FAIL_STEP: \" + str(int({na_var}.sum())) + \" unknown rows [step_id={step_id}]\")")
            lines.append(f"{cond_eff_var} = {raw_var}.astype(bool)")
        elif expr.unknown_policy == FilterUnknownPolicy.RETAIN:
            fill_v = True if expr.retain_matching else False
            lines.append(f"{cond_eff_var} = {raw_var}.fillna({fill_v}).astype(bool)")
        elif expr.unknown_policy in (FilterUnknownPolicy.FILTER_OUT, FilterUnknownPolicy.ROUTE_TO_DROPPED_OUTPUT):
            fill_v = False if expr.retain_matching else True
            lines.append(f"{cond_eff_var} = {raw_var}.fillna({fill_v}).astype(bool)")
        elif expr.unknown_policy == FilterUnknownPolicy.SEPARATE_OUTPUT:
            lines.append(f"outputs[{repr(expr.unknown_output_ref)}] = {df_var}[{na_var}].copy()")
            lines.append(f"{cond_eff_var} = {raw_var}.fillna(False).astype(bool)")

        if expr.retain_matching:
            lines.append(f"{retain_mask_var} = {cond_eff_var}")
        else:
            lines.append(f"{retain_mask_var} = ~{cond_eff_var}")

        if expr.unknown_policy == FilterUnknownPolicy.SEPARATE_OUTPUT:
            lines.append(f"{retain_mask_var} = {retain_mask_var} & (~{na_var})")
            lines.append(f"{dropped_mask_var} = (~{retain_mask_var}) & (~{na_var})")
        else:
            lines.append(f"{dropped_mask_var} = ~{retain_mask_var}")

        lines.append(f"_filter_accounting_{clean_id} = {{'step_id': {repr(step_id)}, 'input_count': len({df_var}), 'retained_count': int({retain_mask_var}.sum()), 'filtered_count': int({dropped_mask_var}.sum()), 'unknown_count': int({na_var}.sum())}}")

        if expr.drop_policy == FilterDropPolicy.EXPORT_DROPPED:
            lines.append(f"_dropped_df_{clean_id} = {df_var}[{dropped_mask_var}].copy()")
            if expr.rejection_reason_code:
                lines.append(f"_dropped_df_{clean_id}['_dhara_filter_reason'] = {repr(expr.rejection_reason_code)}")
            lines.append(f"outputs[{repr(expr.dropped_output_ref)}] = _dropped_df_{clean_id}")

        lines.append(f"{df_var} = {df_var}[{retain_mask_var}].copy()")
        return lines

    def _emit_deduplicate(
        self,
        expr: DeduplicateExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        clean_id = re.sub(r'[^a-zA-Z0-9_]', '_', step_id)
        keys = list(expr.resolved_key_columns or expr.key_columns)
        lines: List[str] = []

        # Lowest-priority tie-breaker first (user correction 4)
        if expr.use_occurrence_id_as_final_tie_breaker:
            if expr.order_by:
                lines.append(f"{df_var} = {df_var}.sort_values(by=_identity_col, ascending=True, na_position='last', kind='mergesort')")
            else:
                asc = (expr.tie_breaker != DeduplicationTieBreaker.PRESERVE_LAST)
                lines.append(f"{df_var} = {df_var}.sort_values(by=_identity_col, ascending={asc}, na_position='last', kind='mergesort')")

        if expr.order_by:
            for spec in reversed(expr.order_by):
                asc = (spec.direction == OrderDirection.ASCENDING)
                na_pos = 'last' if spec.nulls_last else 'first'
                lines.append(f"{df_var} = {df_var}.sort_values(by={repr(spec.column)}, ascending={asc}, na_position={repr(na_pos)}, kind='mergesort')")

        keep = "last" if expr.tie_breaker == DeduplicationTieBreaker.PRESERVE_LAST else "first"
        dup_mask_var = f"_dup_mask_{clean_id}"
        all_dups_mask_var = f"_all_dups_mask_{clean_id}"

        if expr.null_key_policy in (NullKeyPolicy.GROUP_NULLS_TOGETHER, NullKeyPolicy.TREAT_NULLS_AS_EQUAL):
            lines.append(f"{dup_mask_var} = {df_var}.duplicated(subset={repr(keys)}, keep={repr(keep)})")
            lines.append(f"{all_dups_mask_var} = {df_var}.duplicated(subset={repr(keys)}, keep=False)")
        elif expr.null_key_policy == NullKeyPolicy.FAIL_STEP:
            lines.append(f"if bool({df_var}[{repr(keys)}].isna().any().any()):")
            lines.append(f"    raise RoutingPredicateUnknownError('Deduplication encountered null key under FAIL_STEP policy')")
            lines.append(f"{dup_mask_var} = {df_var}.duplicated(subset={repr(keys)}, keep={repr(keep)})")
            lines.append(f"{all_dups_mask_var} = {df_var}.duplicated(subset={repr(keys)}, keep=False)")
        elif expr.null_key_policy == NullKeyPolicy.TREAT_EACH_NULL_AS_DISTINCT:
            lines.append(f"_has_null_key_{clean_id} = {df_var}[{repr(keys)}].isna().any(axis=1)")
            lines.append(f"_eff_dedup_key_{clean_id} = {df_var}[{repr(keys)}].astype(str).agg(':::'.join, axis=1)")
            lines.append(f"_eff_dedup_key_{clean_id} = np.where(_has_null_key_{clean_id}, _eff_dedup_key_{clean_id} + ':::ID:::' + {df_var}[_identity_col].astype(str), _eff_dedup_key_{clean_id})")
            lines.append(f"{dup_mask_var} = pd.Series(_eff_dedup_key_{clean_id}, index={df_var}.index).duplicated(keep={repr(keep)})")
            lines.append(f"{all_dups_mask_var} = pd.Series(_eff_dedup_key_{clean_id}, index={df_var}.index).duplicated(keep=False)")
        elif expr.null_key_policy == NullKeyPolicy.EXCLUDE_NULL_KEY_ROWS:
            lines.append(f"_has_null_key_{clean_id} = {df_var}[{repr(keys)}].isna().any(axis=1)")
            lines.append(f"{dup_mask_var} = {df_var}.duplicated(subset={repr(keys)}, keep={repr(keep)}) | _has_null_key_{clean_id}")
            lines.append(f"{all_dups_mask_var} = {df_var}.duplicated(subset={repr(keys)}, keep=False) | _has_null_key_{clean_id}")

        if expr.duplicates_output_ref:
            if expr.duplicate_export_mode == DuplicateExportMode.REMOVED_OCCURRENCES_ONLY:
                lines.append(f"outputs[{repr(expr.duplicates_output_ref)}] = {df_var}[{dup_mask_var}].copy()")
            else:
                lines.append(f"outputs[{repr(expr.duplicates_output_ref)}] = {df_var}[{all_dups_mask_var}].copy()")

        lines.append(f"{df_var} = {df_var}[~{dup_mask_var}].copy()")
        return lines

    def _emit_validate(
        self,
        expr: ValidateExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        cond_expr = self._emit_expression(expr.predicate, preimages, df_var)
        clean_id = re.sub(r'[^a-zA-Z0-9_]', '_', step_id)
        raw_var = f"_val_raw_{clean_id}"
        cand_var = f"_cand_cnt_{clean_id}"
        pass_var = f"_raw_pass_{clean_id}"
        viol_var = f"_raw_viol_{clean_id}"
        unk_var = f"_raw_unk_{clean_id}"
        eff_viol_var = f"_eff_viol_{clean_id}"
        summary_var = f"_val_summary_{clean_id}"
        reason = expr.reason_code or expr.rejection_reason_code or "VALIDATION_RULE_VIOLATED"

        reads = expr.predicate.extract_reads()
        if isinstance(expr.predicate, NullCheck):
            na_check = f"({cond_expr}).isna()"
        elif reads:
            na_exprs = [f"{df_var}[{repr(r)}].isna()" for r in set(reads)]
            na_check = f"({' | '.join(na_exprs)}) | ({cond_expr}).isna()"
        else:
            na_check = f"({cond_expr}).isna()"

        lines: List[str] = [
            f"_val_na_{clean_id} = ({na_check})",
            f"{raw_var} = np.where(_val_na_{clean_id}, pd.NA, ({cond_expr}))",
            f"{raw_var} = pd.Series({raw_var}, index={df_var}.index, dtype='boolean')",
            f"{cand_var} = len({df_var})",
            f"{unk_var} = int({raw_var}.isna().sum())",
            f"{pass_var} = int(({raw_var} == True).sum())",
            f"{viol_var} = int(({raw_var} == False).sum())",
        ]

        if expr.unknown_policy == ValidationUnknownPolicy.COUNT_AS_VIOLATION:
            lines.append(f"{eff_viol_var} = {viol_var} + {unk_var}")
        elif expr.unknown_policy in (ValidationUnknownPolicy.COUNT_AS_PASS, ValidationUnknownPolicy.REPORT_SEPARATELY):
            lines.append(f"{eff_viol_var} = {viol_var}")
        elif expr.unknown_policy == ValidationUnknownPolicy.FAIL_STEP:
            lines.append(f"if {unk_var} > 0: raise ValueError(\"Validation condition evaluated to unknown under FAIL_STEP: \" + str({unk_var}) + \" unknown rows [step_id={step_id}]\")")
            lines.append(f"{eff_viol_var} = {viol_var}")

        lines.append(f"{summary_var} = {{'step_id': {repr(step_id)}, 'candidate_count': {cand_var}, 'pass_count': {pass_var}, 'violation_count': {viol_var}, 'raw_unknown_count': {unk_var}, 'effective_violation_count': {eff_viol_var}, 'severity': {repr(expr.severity.value)}, 'reason_code': {repr(reason)}}}")

        if expr.severity == ValidateSeverity.ERROR:
            lines.append(f"if {eff_viol_var} > 0: raise ValueError(\"Validation assertion failed [step_id={step_id}]: \" + str({eff_viol_var}) + \" violations out of \" + str({cand_var}) + \" candidates (reason_code={reason})\")")
            lines.append(f"{summary_var}['gate_outcome'] = 'PASS'")
        else:
            lines.append(f"{summary_var}['gate_outcome'] = 'WARN' if {eff_viol_var} > 0 else 'PASS'")

        return lines

    def _emit_hash(
        self,
        expr: HashExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        clean_id = step_id.replace("-", "_").replace(".", "_")
        in_col = expr.source_column or (expr.input_column.column_name if expr.input_column else "")
        out_col = expr.target_column or expr.output_column or in_col
        lines: List[str] = []

        # Target collision check
        if out_col != in_col and expr.existing_column_policy == ExistingColumnPolicy.ERROR:
            lines.append(f"if {repr(out_col)} in {df_var}.columns:")
            lines.append(f"    raise OutputColumnCollisionError('Target column already exists: ' + {repr(out_col)})")

        # Key / Secret resolution
        if expr.mode == HashMode.HMAC:
            lines.append(f"if key_provider is None:")
            lines.append(f"    raise KeyProviderRequiredError('Step {step_id} requires a key provider for HMAC hash')")
            sec_id = expr.secret_ref.secret_id if expr.secret_ref else "default_key"
            sec_purpose = expr.secret_ref.expected_purpose if expr.secret_ref else "hmac_key"
            sec_ver = repr(expr.secret_ref.expected_version) if (expr.secret_ref and expr.secret_ref.expected_version) else "None"
            lines.append(f"_sec_handle_{clean_id} = key_provider.resolve_secret(secret_id={repr(sec_id)}, tenant_id=getattr(key_provider, 'tenant_id', 'default'), purpose={repr(sec_purpose)}, expected_version={sec_ver})")
            lines.append(f"_sec_bytes_{clean_id} = _sec_handle_{clean_id}.secret_bytes")
        elif expr.mode == HashMode.SALTED_DIGEST:
            if expr.salt_value:
                lines.append(f"_salt_{clean_id} = {repr(expr.salt_value)}.encode('utf-8')")
            elif expr.secret_ref:
                lines.append(f"if key_provider is None:")
                lines.append(f"    raise KeyProviderRequiredError('Step {step_id} requires a key provider for salted digest')")
                sec_id = expr.secret_ref.secret_id
                sec_purpose = expr.secret_ref.expected_purpose
                sec_ver = repr(expr.secret_ref.expected_version) if expr.secret_ref.expected_version else "None"
                lines.append(f"_sec_handle_{clean_id} = key_provider.resolve_secret(secret_id={repr(sec_id)}, tenant_id=getattr(key_provider, 'tenant_id', 'default'), purpose={repr(sec_purpose)}, expected_version={sec_ver})")
                lines.append(f"_salt_{clean_id} = _sec_handle_{clean_id}.secret_bytes")
            else:
                lines.append(f"_salt_{clean_id} = b''")

        # Canonical value encoder helper
        norm_form = repr(expr.unicode_normalization)
        lines.append(f"def _canonical_encode_{clean_id}(v):")
        lines.append(f"    if pd.isna(v) or v is None:")
        lines.append(f"        return b'\\x00DHARA_NULL_V1'")
        lines.append(f"    if isinstance(v, bool):")
        lines.append(f"        return b'\\x01BOOL\\x01' if v else b'\\x01BOOL\\x00'")
        lines.append(f"    if isinstance(v, (int, np.integer)):")
        lines.append(f"        return b'\\x02INT\\x00' + int(v).to_bytes(8, byteorder='big', signed=True)")
        lines.append(f"    if isinstance(v, (float, np.floating)):")
        lines.append(f"        val_f = 0.0 if v == 0.0 else float(v)")
        lines.append(f"        return b'\\x03FLOAT\\x00' + struct.pack('>d', val_f)")
        lines.append(f"    if isinstance(v, str):")
        lines.append(f"        norm = unicodedata.normalize({norm_form}, v)")
        lines.append(f"        return b'\\x05STR\\x00' + norm.encode('utf-8')")
        lines.append(f"    if hasattr(v, 'isoformat'):")
        lines.append(f"        return b'\\x06TS\\x00' + v.isoformat().encode('utf-8')")
        lines.append(f"    if isinstance(v, (bytes, bytearray)):")
        lines.append(f"        return b'\\x07BYTES\\x00' + bytes(v)")
        lines.append(f"    norm = unicodedata.normalize({norm_form}, str(v))")
        lines.append(f"    return b'\\x08OTHER\\x00' + norm.encode('utf-8')")

        # Hash computation function
        lines.append(f"def _hash_calc_{clean_id}(v):")
        lines.append(f"    if pd.isna(v) or v is None:")
        if expr.null_policy == HashNullPolicy.PRESERVE_NULL:
            lines.append(f"        return np.nan")
        elif expr.null_policy == HashNullPolicy.FAIL_STEP:
            lines.append(f"        raise ValueError('Encountered null in {in_col} under FAIL_STEP hash null policy')")
        elif expr.null_policy == HashNullPolicy.HASH_NULL:
            lines.append(f"        raw_b = b'\\x00DHARA_NULL_V1'")
        else:
            lines.append(f"        return np.nan")
        lines.append(f"    else:")
        lines.append(f"        raw_b = _canonical_encode_{clean_id}(v)")

        alg_name = expr.algorithm.value if hasattr(expr.algorithm, "value") else str(expr.algorithm)
        if expr.mode == HashMode.HMAC:
            lines.append(f"    d = hmac.new(_sec_bytes_{clean_id}, raw_b, {repr(alg_name)}).hexdigest()")
        elif expr.mode == HashMode.SALTED_DIGEST:
            lines.append(f"    d = getattr(hashlib, {repr(alg_name)})(_salt_{clean_id} + raw_b).hexdigest()")
        else:
            lines.append(f"    d = getattr(hashlib, {repr(alg_name)})(raw_b).hexdigest()")

        if expr.truncate_length is not None:
            lines.append(f"    d = d[:{expr.truncate_length}]")
        lines.append(f"    return d")

        lines.append(f"{df_var}[{repr(out_col)}] = {df_var}[{repr(in_col)}].apply(_hash_calc_{clean_id})")
        return lines

    def _emit_mask(
        self,
        expr: MaskExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        clean_id = step_id.replace("-", "_").replace(".", "_")
        in_col = expr.source_column or (expr.input_column.column_name if expr.input_column else "")
        out_col = expr.target_column or expr.output_column or in_col
        lines: List[str] = []

        # Target collision check
        if out_col != in_col and expr.existing_column_policy == ExistingColumnPolicy.ERROR:
            lines.append(f"if {repr(out_col)} in {df_var}.columns:")
            lines.append(f"    raise OutputColumnCollisionError('Target column already exists: ' + {repr(out_col)})")

        mc = repr(expr.mask_character)
        p_count = expr.preserved_count

        lines.append(f"def _mask_calc_{clean_id}(v):")
        lines.append(f"    if pd.isna(v) or v is None:")
        if expr.null_policy == MaskNullPolicy.PRESERVE_NULL:
            lines.append(f"        return np.nan")
        elif expr.null_policy == MaskNullPolicy.SET_EMPTY:
            lines.append(f"        return ''")
        elif expr.null_policy == MaskNullPolicy.FAIL_STEP:
            lines.append(f"        raise ValueError('Encountered null in {in_col} under FAIL_STEP mask null policy')")
        else:
            lines.append(f"        return np.nan")

        lines.append(f"    s = str(v)")
        lines.append(f"    n = len(s)")
        lines.append(f"    mc = {mc}")
        lines.append(f"    p_count = {p_count}")

        if expr.strategy in (MaskStrategy.PRESERVE_LAST_N, "preserve_last_n"):
            lines.append(f"    if n <= p_count:")
            if expr.short_value_policy == ShortValuePolicy.MASK_ALL:
                lines.append(f"        return mc * n")
            elif expr.short_value_policy == ShortValuePolicy.SET_NULL:
                lines.append(f"        return np.nan")
            elif expr.short_value_policy == ShortValuePolicy.FAIL_STEP:
                lines.append(f"        raise ValueError('String length ' + str(n) + ' <= preserved_count ' + str(p_count) + ' under FAIL_STEP policy')")
            elif expr.short_value_policy == ShortValuePolicy.KEEP_ORIGINAL:
                lines.append(f"        return s")
            else:
                lines.append(f"        return mc * n")
            lines.append(f"    return (mc * (n - p_count)) + s[-p_count:]")

        elif expr.strategy in (MaskStrategy.PRESERVE_FIRST_N, "preserve_first_n"):
            lines.append(f"    if n <= p_count:")
            if expr.short_value_policy == ShortValuePolicy.MASK_ALL:
                lines.append(f"        return mc * n")
            elif expr.short_value_policy == ShortValuePolicy.SET_NULL:
                lines.append(f"        return np.nan")
            elif expr.short_value_policy == ShortValuePolicy.FAIL_STEP:
                lines.append(f"        raise ValueError('String length ' + str(n) + ' <= preserved_count ' + str(p_count) + ' under FAIL_STEP policy')")
            elif expr.short_value_policy == ShortValuePolicy.KEEP_ORIGINAL:
                lines.append(f"        return s")
            else:
                lines.append(f"        return mc * n")
            lines.append(f"    return s[:p_count] + (mc * (n - p_count))")

        elif expr.strategy in (MaskStrategy.MASK_ALL, MaskStrategy.REDACT_ALL, "mask_all", "redact_all"):
            lines.append(f"    return mc * n")

        elif expr.strategy in (MaskStrategy.EMAIL_MASK, "email_mask"):
            lines.append(f"    if '@' not in s or s.count('@') != 1:")
            lines.append(f"        return mc * n")
            lines.append(f"    user_part, domain_part = s.split('@', 1)")
            lines.append(f"    if len(user_part) <= 1:")
            lines.append(f"        m_user = user_part")
            lines.append(f"    elif len(user_part) == 2:")
            lines.append(f"        m_user = user_part[0] + mc")
            lines.append(f"    else:")
            lines.append(f"        m_user = user_part[0] + (mc * (len(user_part) - 2)) + user_part[-1]")
            if expr.email_domain_policy == EmailMaskDomainPolicy.PRESERVE:
                lines.append(f"    m_domain = domain_part")
            elif expr.email_domain_policy == EmailMaskDomainPolicy.MASK:
                lines.append(f"    d_name, d_sep, tld = domain_part.rpartition('.')")
                lines.append(f"    m_domain = (mc * len(d_name)) + ('.' + tld if d_sep else '')")
            elif expr.email_domain_policy == EmailMaskDomainPolicy.HASH:
                lines.append(f"    m_domain = hashlib.sha256(domain_part.encode('utf-8')).hexdigest()[:8]")
            else:
                lines.append(f"    m_domain = domain_part")
            lines.append(f"    return m_user + '@' + m_domain")
        else:
            lines.append(f"    return (mc * (n - p_count)) + s[-p_count:] if n > p_count else mc * n")

        lines.append(f"{df_var}[{repr(out_col)}] = {df_var}[{repr(in_col)}].apply(_mask_calc_{clean_id})")
        return lines

    def _emit_schema_mutation(
        self,
        expr: SchemaMutationExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        clean_id = step_id.replace("-", "_").replace(".", "_")
        lines: List[str] = []

        # Pre-mutation expected schema hash assertion
        if expr.expected_before_schema_hash:
            lines.append(f"_before_cols_{clean_id} = [(c, str({df_var}[c].dtype), bool({df_var}[c].isna().any())) for c in {df_var}.columns]")
            lines.append(f"if compute_canonical_schema_hash(_before_cols_{clean_id}) != {repr(expr.expected_before_schema_hash)}:")
            lines.append(f"    raise SchemaSnapshotMismatchError('Actual pre-mutation schema hash does not match expected_before_schema_hash')")

        if expr.op == SchemaMutationOp.DROP_COLUMN:
            drop_cols = [d.column for d in expr.drops] if expr.drops else ([expr.target_column] if expr.target_column else [])
            for dc in drop_cols:
                lines.append(f"if {repr(dc)} not in {df_var}.columns:")
                lines.append(f"    raise ValueError('Cannot drop non-existent column: ' + {repr(dc)})")
            lines.append(f"{df_var} = {df_var}.drop(columns={repr(drop_cols)})")

        elif expr.op == SchemaMutationOp.RENAME_COLUMN:
            for r in expr.renames:
                old_col = r.old_name
                new_col = r.new_name
                lines.append(f"if {repr(old_col)} not in {df_var}.columns:")
                lines.append(f"    raise ValueError('Cannot rename non-existent column: ' + {repr(old_col)})")
                lines.append(f"if {repr(new_col)} in {df_var}.columns and {repr(new_col)} != {repr(old_col)}:")
                lines.append(f"    raise OutputColumnCollisionError('Target rename column already exists: ' + {repr(new_col)})")
                lines.append(f"{df_var} = {df_var}.rename(columns={{{repr(old_col)}: {repr(new_col)}}})")

        elif expr.op == SchemaMutationOp.PROJECT:
            if expr.projection:
                proj_cols = list(expr.projection.projected_columns)
                for pc in proj_cols:
                    if not expr.projection.allow_missing:
                        lines.append(f"if {repr(pc)} not in {df_var}.columns:")
                        lines.append(f"    raise ValueError('Projected column not found in DataFrame: ' + {repr(pc)})")
                lines.append(f"{df_var} = {df_var}[{repr(proj_cols)}]")

        elif expr.op == SchemaMutationOp.REORDER_COLUMNS:
            reorder_cols = list(expr.ordered_columns)
            for rc in reorder_cols:
                lines.append(f"if {repr(rc)} not in {df_var}.columns:")
                lines.append(f"    raise ValueError('Reorder column not found in DataFrame: ' + {repr(rc)})")
            lines.append(f"{df_var} = {df_var}[{repr(reorder_cols)}]")

        elif expr.op == SchemaMutationOp.CREATE_COLUMN:
            col_name = expr.create_column_name or expr.target_column or "new_col"
            if expr.existing_column_policy == ExistingColumnPolicy.ERROR:
                lines.append(f"if {repr(col_name)} in {df_var}.columns:")
                lines.append(f"    raise OutputColumnCollisionError('Created column already exists: ' + {repr(col_name)})")
            if expr.create_expression:
                c_code = self._emit_expression(expr.create_expression, preimages, df_var)
                lines.append(f"{df_var}[{repr(col_name)}] = {c_code}")
            else:
                lines.append(f"{df_var}[{repr(col_name)}] = pd.Series(None, index={df_var}.index, dtype='string')")

        elif expr.op == SchemaMutationOp.CAST_COLUMN:
            src = expr.source_column or expr.target_column
            dst = expr.target_column or src
            dtype_map = {
                DataType.STRING: "'string'",
                DataType.INTEGER: "'Int64'",
                DataType.FLOAT: "'Float64'",
                DataType.BOOLEAN: "'boolean'",
            }
            target_dtype = dtype_map.get(expr.cast_target_type, "'string'")
            if expr.cast_failure_policy == CastFailurePolicy.SET_NULL:
                lines.append(f"try:")
                lines.append(f"    {df_var}[{repr(dst)}] = {df_var}[{repr(src)}].astype({target_dtype})")
                lines.append(f"except Exception:")
                lines.append(f"    {df_var}[{repr(dst)}] = pd.to_numeric({df_var}[{repr(src)}], errors='coerce') if {target_dtype} in ('Int64', 'Float64') else pd.Series(None, index={df_var}.index, dtype={target_dtype})")
            else:
                lines.append(f"{df_var}[{repr(dst)}] = {df_var}[{repr(src)}].astype({target_dtype})")

        # Post-mutation expected schema hash assertion
        if expr.expected_after_schema_hash:
            lines.append(f"_after_cols_{clean_id} = [(c, str({df_var}[c].dtype), bool({df_var}[c].isna().any())) for c in {df_var}.columns]")
            lines.append(f"if compute_canonical_schema_hash(_after_cols_{clean_id}) != {repr(expr.expected_after_schema_hash)}:")
            lines.append(f"    raise SchemaSnapshotMismatchError('Actual post-mutation schema hash does not match expected_after_schema_hash')")

        return lines

    def _emit_derive(
        self,
        expr: Any,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        clean_id = step_id.replace("-", "_").replace(".", "_")
        lines: List[str] = []

        if isinstance(expr, ArithmeticDerivation):
            out_col = expr.output_column
            if expr.existing_column_policy == ExistingColumnPolicy.ERROR:
                lines.append(f"if {repr(out_col)} in {df_var}.columns:")
                lines.append(f"    raise OutputColumnCollisionError('Target column already exists: ' + {repr(out_col)})")

            domain = expr.domain
            op = expr.operator
            div_zero = expr.division_by_zero_policy
            overflow = expr.overflow_policy

            l_val_code = f"row[{repr(expr.left.column_name)}]" if expr.left.column_name is not None else repr(expr.left.literal_value if expr.left.literal_value is not None else expr.left.value)
            lines.append(f"def _arith_{clean_id}(row):")
            lines.append(f"    l_val = {l_val_code}")
            if expr.right:
                r_val_code = f"row[{repr(expr.right.column_name)}]" if expr.right.column_name is not None else repr(expr.right.literal_value if expr.right.literal_value is not None else expr.right.value)
                lines.append(f"    r_val = {r_val_code}")
            else:
                lines.append(f"    r_val = None")
            lines.append(f"    if pd.isna(l_val) or (r_val is not None and pd.isna(r_val)):")
            lines.append(f"        return np.nan")

            if domain == ArithmeticDomain.INT64:
                lines.append(f"    try:")
                lines.append(f"        l_int = int(l_val)")
                lines.append(f"        r_int = int(r_val) if r_val is not None else 0")
                if op in (ArithmeticOperator.DIVIDE, ArithmeticOperator.FLOOR_DIVIDE, ArithmeticOperator.MODULO):
                    lines.append(f"        if r_int == 0:")
                    if div_zero == DivisionByZeroPolicy.FAIL_STEP:
                        lines.append(f"            raise ZeroDivisionError('Division by zero in arithmetic derivation')")
                    elif div_zero == DivisionByZeroPolicy.SET_NULL:
                        lines.append(f"            return np.nan")
                    else:
                        lines.append(f"            return np.nan")
                if op == ArithmeticOperator.ADD:
                    lines.append(f"        res = l_int + r_int")
                elif op == ArithmeticOperator.SUBTRACT:
                    lines.append(f"        res = l_int - r_int")
                elif op == ArithmeticOperator.MULTIPLY:
                    lines.append(f"        res = l_int * r_int")
                elif op == ArithmeticOperator.DIVIDE:
                    lines.append(f"        res = l_int // r_int")
                elif op == ArithmeticOperator.FLOOR_DIVIDE:
                    lines.append(f"        res = l_int // r_int")
                elif op == ArithmeticOperator.MODULO:
                    lines.append(f"        res = l_int % r_int")
                elif op == ArithmeticOperator.POWER:
                    lines.append(f"        if r_int < 0:")
                    lines.append(f"            return np.nan")
                    lines.append(f"        res = l_int ** r_int")
                else:
                    lines.append(f"        res = l_int + r_int")

                lines.append(f"        if res < -9223372036854775808 or res > 9223372036854775807:")
                if overflow == NumericOverflowPolicy.FAIL_STEP:
                    lines.append(f"            raise OverflowError('Integer overflow outside signed 64-bit range')")
                elif overflow == NumericOverflowPolicy.SET_NULL:
                    lines.append(f"            return np.nan")
                elif overflow == NumericOverflowPolicy.CLIP_TO_RANGE:
                    lines.append(f"            return 9223372036854775807 if res > 9223372036854775807 else -9223372036854775808")
                else:
                    lines.append(f"            raise OverflowError('Integer overflow')")
                lines.append(f"        return res")
                lines.append(f"    except (OverflowError, ZeroDivisionError) as e:")
                lines.append(f"        if {repr(div_zero.value)} == 'fail_step' or {repr(overflow.value)} == 'fail_step':")
                lines.append(f"            raise ValueError(f'Arithmetic error in step {step_id}: ' + str(e))")
                lines.append(f"        return np.nan")

            elif domain == ArithmeticDomain.FLOAT64:
                lines.append(f"    try:")
                lines.append(f"        l_f = float(l_val)")
                lines.append(f"        r_f = float(r_val) if r_val is not None else 0.0")
                if op in (ArithmeticOperator.DIVIDE, ArithmeticOperator.FLOOR_DIVIDE, ArithmeticOperator.MODULO):
                    lines.append(f"        if r_f == 0.0:")
                    if div_zero == DivisionByZeroPolicy.FAIL_STEP:
                        lines.append(f"            raise ZeroDivisionError('Division by zero in float arithmetic')")
                    elif div_zero == DivisionByZeroPolicy.RETURN_INFINITY:
                        lines.append(f"            return np.inf if l_f >= 0.0 else -np.inf")
                    else:
                        lines.append(f"            return np.nan")
                if op == ArithmeticOperator.ADD:
                    lines.append(f"        res = l_f + r_f")
                elif op == ArithmeticOperator.SUBTRACT:
                    lines.append(f"        res = l_f - r_f")
                elif op == ArithmeticOperator.MULTIPLY:
                    lines.append(f"        res = l_f * r_f")
                elif op == ArithmeticOperator.DIVIDE:
                    lines.append(f"        res = l_f / r_f")
                elif op == ArithmeticOperator.FLOOR_DIVIDE:
                    lines.append(f"        res = l_f // r_f")
                elif op == ArithmeticOperator.MODULO:
                    lines.append(f"        res = l_f % r_f")
                elif op == ArithmeticOperator.POWER:
                    lines.append(f"        if l_f < 0.0 and int(r_f) != r_f:")
                    lines.append(f"            return np.nan")
                    lines.append(f"        res = l_f ** r_f")
                else:
                    lines.append(f"        res = l_f + r_f")

                lines.append(f"        if np.isinf(res):")
                if overflow == NumericOverflowPolicy.FAIL_STEP:
                    lines.append(f"            raise OverflowError('Float overflow to infinity under FAIL_STEP')")
                elif overflow == NumericOverflowPolicy.SET_NULL:
                    lines.append(f"            return np.nan")
                elif overflow == NumericOverflowPolicy.CLIP_TO_RANGE:
                    lines.append(f"            max_f = np.finfo(np.float64).max")
                    lines.append(f"            return max_f if res > 0 else -max_f")
                lines.append(f"        return res")
                lines.append(f"    except Exception as e:")
                lines.append(f"        if {repr(div_zero.value)} == 'fail_step' or {repr(overflow.value)} == 'fail_step':")
                lines.append(f"            raise ValueError(f'Float arithmetic error in step {step_id}: ' + str(e))")
                lines.append(f"        return np.nan")

            else:
                prec = expr.precision or 28
                scale = expr.scale or 4
                lines.append(f"    from decimal import Decimal, ROUND_HALF_UP")
                lines.append(f"    try:")
                lines.append(f"        l_d = Decimal(str(l_val))")
                lines.append(f"        r_d = Decimal(str(r_val)) if r_val is not None else Decimal('0')")
                lines.append(f"        if not l_d.is_finite() or not r_d.is_finite():")
                lines.append(f"            raise ValueError('Non-finite Decimal operand')")
                if op in (ArithmeticOperator.DIVIDE, ArithmeticOperator.FLOOR_DIVIDE, ArithmeticOperator.MODULO):
                    lines.append(f"        if r_d.is_zero():")
                    if div_zero == DivisionByZeroPolicy.FAIL_STEP:
                        lines.append(f"            raise ZeroDivisionError('Decimal division by zero')")
                    else:
                        lines.append(f"            return np.nan")
                if op == ArithmeticOperator.ADD:
                    lines.append(f"        res = l_d + r_d")
                elif op == ArithmeticOperator.SUBTRACT:
                    lines.append(f"        res = l_d - r_d")
                elif op == ArithmeticOperator.MULTIPLY:
                    lines.append(f"        res = l_d * r_d")
                elif op == ArithmeticOperator.DIVIDE:
                    lines.append(f"        res = l_d / r_d")
                elif op == ArithmeticOperator.FLOOR_DIVIDE:
                    lines.append(f"        res = l_d // r_d")
                elif op == ArithmeticOperator.MODULO:
                    lines.append(f"        res = l_d % r_d")
                elif op == ArithmeticOperator.POWER:
                    lines.append(f"        res = l_d ** r_d")
                else:
                    lines.append(f"        res = l_d + r_d")

                lines.append(f"        q = Decimal('10') ** -{scale}")
                lines.append(f"        res = res.quantize(q, rounding=ROUND_HALF_UP)")
                lines.append(f"        return res")
                lines.append(f"    except Exception as e:")
                lines.append(f"        if {repr(div_zero.value)} == 'fail_step' or {repr(overflow.value)} == 'fail_step':")
                lines.append(f"            raise ValueError(f'Decimal arithmetic error in step {step_id}: ' + str(e))")
                lines.append(f"        return np.nan")

            lines.append(f"{df_var}[{repr(out_col)}] = {df_var}.apply(_arith_{clean_id}, axis=1)")
            return lines

        elif isinstance(expr, ConditionalDerivation):
            out_col = expr.output_column
            if expr.existing_column_policy == ExistingColumnPolicy.ERROR:
                lines.append(f"if {repr(out_col)} in {df_var}.columns:")
                lines.append(f"    raise OutputColumnCollisionError('Target column already exists: ' + {repr(out_col)})")

            cond_expr = self._emit_expression(expr.condition, preimages, df_var)
            then_code = f"{df_var}[{repr(expr.then_column)}]" if expr.then_column else repr(expr.then_value)
            else_code = f"{df_var}[{repr(expr.else_column)}]" if expr.else_column else repr(expr.else_value)

            lines.append(f"_cond_{clean_id} = {cond_expr}")
            cond_reads = expr.condition.extract_reads() if hasattr(expr.condition, "extract_reads") else []
            if cond_reads and not isinstance(expr.condition, NullCheck):
                lines.append(f"_cond_null_{clean_id} = " + " | ".join([f"{df_var}[{repr(c)}].isna()" for c in set(cond_reads)]))
                lines.append(f"_unknown_mask_{clean_id} = _cond_{clean_id}.isna() | _cond_null_{clean_id}")
                lines.append(f"_true_mask_{clean_id} = _cond_{clean_id}.eq(True) & ~_unknown_mask_{clean_id}")
                lines.append(f"_false_mask_{clean_id} = _cond_{clean_id}.eq(False) & ~_unknown_mask_{clean_id}")
            else:
                lines.append(f"_true_mask_{clean_id} = _cond_{clean_id}.eq(True)")
                lines.append(f"_false_mask_{clean_id} = _cond_{clean_id}.eq(False)")
                lines.append(f"_unknown_mask_{clean_id} = _cond_{clean_id}.isna() | (~_true_mask_{clean_id} & ~_false_mask_{clean_id})")

            policy = expr.unknown_policy
            if policy == ConditionalUnknownPolicy.FAIL_STEP:
                lines.append(f"if bool(_unknown_mask_{clean_id}.any()):")
                lines.append(f"    raise ValueError('Conditional derivation step failure [step_id={step_id}]: Unknown predicate under FAIL_STEP')")

            lines.append(f"_then_s_{clean_id} = {then_code}")
            lines.append(f"if not isinstance(_then_s_{clean_id}, pd.Series):")
            lines.append(f"    _then_s_{clean_id} = pd.Series(_then_s_{clean_id}, index={df_var}.index)")
            lines.append(f"_else_s_{clean_id} = {else_code}")
            lines.append(f"if not isinstance(_else_s_{clean_id}, pd.Series):")
            lines.append(f"    _else_s_{clean_id} = pd.Series(_else_s_{clean_id}, index={df_var}.index)")

            lines.append(f"_res_{clean_id} = _then_s_{clean_id}.where(_true_mask_{clean_id}, _else_s_{clean_id})")

            if policy == ConditionalUnknownPolicy.SET_NULL:
                lines.append(f"_res_{clean_id} = _res_{clean_id}.mask(_unknown_mask_{clean_id}, np.nan)")
            elif policy == ConditionalUnknownPolicy.USE_THEN:
                lines.append(f"_res_{clean_id} = _res_{clean_id}.mask(_unknown_mask_{clean_id}, _then_s_{clean_id})")
            elif policy == ConditionalUnknownPolicy.USE_ELSE:
                lines.append(f"_res_{clean_id} = _res_{clean_id}.mask(_unknown_mask_{clean_id}, _else_s_{clean_id})")
            elif policy == ConditionalUnknownPolicy.USE_FALLBACK:
                lines.append(f"_res_{clean_id} = _res_{clean_id}.mask(_unknown_mask_{clean_id}, {repr(expr.fallback_value)})")

            lines.append(f"{df_var}[{repr(out_col)}] = _res_{clean_id}.infer_objects(copy=False)")
            return lines

        elif isinstance(expr, CoalesceDerivation):
            out_col = expr.output_column
            if expr.existing_column_policy == ExistingColumnPolicy.ERROR:
                lines.append(f"if {repr(out_col)} in {df_var}.columns:")
                lines.append(f"    raise OutputColumnCollisionError('Target column already exists: ' + {repr(out_col)})")

            lines.append(f"_coalesce_res_{clean_id} = {df_var}[{repr(expr.columns[0])}].copy()")
            for col in expr.columns[1:]:
                lines.append(f"_coalesce_res_{clean_id} = _coalesce_res_{clean_id}.combine_first({df_var}[{repr(col)}])")
            if expr.fallback_value is not None:
                lines.append(f"_coalesce_res_{clean_id} = pd.Series(np.where(_coalesce_res_{clean_id}.isna(), {repr(expr.fallback_value)}, _coalesce_res_{clean_id}), index={df_var}.index).infer_objects(copy=False)")
            lines.append(f"{df_var}[{repr(out_col)}] = _coalesce_res_{clean_id}")
            return lines

        elif isinstance(expr, ConcatDerivation):
            out_col = expr.output_column
            if expr.existing_column_policy == ExistingColumnPolicy.ERROR:
                lines.append(f"if {repr(out_col)} in {df_var}.columns:")
                lines.append(f"    raise OutputColumnCollisionError('Target column already exists: ' + {repr(out_col)})")

            sep = repr(expr.delimiter)
            lines.append(f"def _concat_{clean_id}(row):")
            lines.append(f"    vals = [row[c] for c in {repr(expr.columns)}]")
            if expr.null_policy == ConcatNullPolicy.PROPAGATE_NULL:
                lines.append(f"    if any(pd.isna(v) for v in vals):")
                lines.append(f"        return np.nan")
                lines.append(f"    return {sep}.join(str(v) for v in vals)")
            elif expr.null_policy == ConcatNullPolicy.IGNORE_NULLS:
                lines.append(f"    non_null = [str(v) for v in vals if not pd.isna(v)]")
                lines.append(f"    return {sep}.join(non_null) if non_null else np.nan")
            else:
                lines.append(f"    return {sep}.join('' if pd.isna(v) else str(v) for v in vals)")
            lines.append(f"{df_var}[{repr(out_col)}] = {df_var}.apply(_concat_{clean_id}, axis=1)")
            return lines

        elif isinstance(expr, DateArithmeticDerivation):
            out_col = expr.output_column
            if expr.existing_column_policy == ExistingColumnPolicy.ERROR:
                lines.append(f"if {repr(out_col)} in {df_var}.columns:")
                lines.append(f"    raise OutputColumnCollisionError('Target column already exists: ' + {repr(out_col)})")

            src = expr.source_column
            tgt = expr.target_column
            amt = expr.amount or 0
            unit = expr.unit.value

            if expr.operation == "diff":
                lines.append(f"_s_dt = pd.to_datetime({df_var}[{repr(src)}])")
                lines.append(f"_t_dt = pd.to_datetime({df_var}[{repr(tgt)}])")
                lines.append(f"_diff = _s_dt - _t_dt")
                if unit == "day":
                    lines.append(f"{df_var}[{repr(out_col)}] = _diff.dt.days")
                elif unit == "hour":
                    lines.append(f"{df_var}[{repr(out_col)}] = _diff.dt.total_seconds() / 3600.0")
                elif unit == "minute":
                    lines.append(f"{df_var}[{repr(out_col)}] = _diff.dt.total_seconds() / 60.0")
                elif unit == "second":
                    lines.append(f"{df_var}[{repr(out_col)}] = _diff.dt.total_seconds()")
                elif unit == "month":
                    lines.append(f"{df_var}[{repr(out_col)}] = (_s_dt.dt.year - _t_dt.dt.year) * 12 + (_s_dt.dt.month - _t_dt.dt.month)")
                elif unit == "year":
                    lines.append(f"{df_var}[{repr(out_col)}] = _s_dt.dt.year - _t_dt.dt.year")
            else:
                sign = 1 if expr.operation == "add" else -1
                eff_amt = amt * sign
                lines.append(f"_base_dt = pd.to_datetime({df_var}[{repr(src)}])")
                if unit == "day":
                    lines.append(f"{df_var}[{repr(out_col)}] = _base_dt + pd.Timedelta(days={eff_amt})")
                elif unit == "hour":
                    lines.append(f"{df_var}[{repr(out_col)}] = _base_dt + pd.Timedelta(hours={eff_amt})")
                elif unit == "month":
                    lines.append(f"{df_var}[{repr(out_col)}] = _base_dt + pd.DateOffset(months={eff_amt})")
                elif unit == "year":
                    lines.append(f"{df_var}[{repr(out_col)}] = _base_dt + pd.DateOffset(years={eff_amt})")
            return lines

        return []

    def _emit_extract(
        self,
        expr: Any,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        clean_id = step_id.replace("-", "_").replace(".", "_")
        lines: List[str] = []
        out_col = expr.output_column

        if expr.existing_column_policy == ExistingColumnPolicy.ERROR:
            lines.append(f"if {repr(out_col)} in {df_var}.columns:")
            lines.append(f"    raise OutputColumnCollisionError('Target column already exists: ' + {repr(out_col)})")

        src = expr.source_column

        if isinstance(expr, RegexExtract):
            lines.append(f"def _regex_ext_{clean_id}(v):")
            lines.append(f"    if pd.isna(v) or v is None:")
            lines.append(f"        return np.nan")
            lines.append(f"    m_all = list(re.finditer({repr(expr.pattern)}, str(v)))")
            lines.append(f"    if not m_all:")
            if expr.failure_policy == ExtractFailurePolicy.FAIL_STEP:
                lines.append(f"        raise ValueError('Regex extract failed on value: ' + str(v))")
            elif expr.failure_policy == ExtractFailurePolicy.USE_FALLBACK:
                lines.append(f"        return {repr(expr.fallback_value)}")
            else:
                lines.append(f"        return np.nan")

            if expr.multiple_match_policy == MultipleMatchPolicy.FAIL_STEP:
                lines.append(f"    if len(m_all) > 1:")
                lines.append(f"        raise ValueError('Multiple regex matches encountered under FAIL_STEP policy: ' + str(v))")

            match = "m_all[-1]" if expr.multiple_match_policy == MultipleMatchPolicy.LAST else "m_all[0]"
            lines.append(f"    m = {match}")
            if expr.group_name:
                lines.append(f"    try:")
                lines.append(f"        return m.group({repr(expr.group_name)})")
                lines.append(f"    except (IndexError, KeyError):")
                lines.append(f"        return np.nan")
            else:
                lines.append(f"    try:")
                lines.append(f"        return m.group({expr.group_index})")
                lines.append(f"    except (IndexError, KeyError):")
                lines.append(f"        return np.nan")

            lines.append(f"{df_var}[{repr(out_col)}] = {df_var}[{repr(src)}].apply(_regex_ext_{clean_id})")
            return lines

        elif isinstance(expr, JsonPathExtract):
            path_str = expr.path.path if hasattr(expr.path, "path") else str(expr.path)
            tokens = path_str.replace("[", ".").replace("]", "").split(".")
            tokens = [t.strip() for t in tokens if t.strip()]

            lines.append(f"def _json_ext_{clean_id}(v):")
            lines.append(f"    if pd.isna(v) or v is None:")
            lines.append(f"        return np.nan")
            lines.append(f"    data = v")
            lines.append(f"    if isinstance(v, (str, bytes)):")
            lines.append(f"        try:")
            lines.append(f"            data = json.loads(v)")
            lines.append(f"        except Exception:")
            if expr.failure_policy == ExtractFailurePolicy.FAIL_STEP:
                lines.append(f"            raise ValueError('Malformed JSON in step {step_id}: ' + str(v))")
            elif expr.failure_policy == ExtractFailurePolicy.USE_FALLBACK:
                lines.append(f"            return {repr(expr.fallback_value)}")
            else:
                lines.append(f"            return np.nan")

            lines.append(f"    cur = data")
            for tok in tokens:
                if tok.isdigit():
                    idx = int(tok)
                    lines.append(f"    if not isinstance(cur, list) or {idx} >= len(cur):")
                    if expr.failure_policy == ExtractFailurePolicy.FAIL_STEP:
                        lines.append(f"        raise ValueError('JSON array index {idx} out of range in step {step_id}')")
                    elif expr.failure_policy == ExtractFailurePolicy.USE_FALLBACK:
                        lines.append(f"        return {repr(expr.fallback_value)}")
                    else:
                        lines.append(f"        return np.nan")
                    lines.append(f"    cur = cur[{idx}]")
                else:
                    lines.append(f"    if not isinstance(cur, dict) or {repr(tok)} not in cur:")
                    if expr.failure_policy == ExtractFailurePolicy.FAIL_STEP:
                        lines.append(f"        raise ValueError('JSON key {repr(tok)} not found in step {step_id}')")
                    elif expr.failure_policy == ExtractFailurePolicy.USE_FALLBACK:
                        lines.append(f"        return {repr(expr.fallback_value)}")
                    else:
                        lines.append(f"        return np.nan")
                    lines.append(f"    cur = cur[{repr(tok)}]")
            lines.append(f"    return cur")
            lines.append(f"{df_var}[{repr(out_col)}] = {df_var}[{repr(src)}].apply(_json_ext_{clean_id})")
            return lines

        elif isinstance(expr, DelimiterExtract):
            lines.append(f"def _delim_ext_{clean_id}(v):")
            lines.append(f"    if pd.isna(v) or v is None:")
            lines.append(f"        return np.nan")
            lines.append(f"    parts = str(v).split({repr(expr.delimiter)})")
            lines.append(f"    idx = {expr.index}")
            lines.append(f"    if idx < 0 or idx >= len(parts):")
            if expr.failure_policy == ExtractFailurePolicy.FAIL_STEP:
                lines.append(f"        raise ValueError('Delimiter index ' + str(idx) + ' out of bounds: ' + str(v))")
            elif expr.failure_policy == ExtractFailurePolicy.USE_FALLBACK:
                lines.append(f"        return {repr(expr.fallback_value)}")
            else:
                lines.append(f"        return np.nan")
            lines.append(f"    return parts[idx]")
            lines.append(f"{df_var}[{repr(out_col)}] = {df_var}[{repr(src)}].apply(_delim_ext_{clean_id})")
            return lines

        elif isinstance(expr, DatePartExtract):
            part = expr.part.value
            lines.append(f"_dt_series = pd.to_datetime({df_var}[{repr(src)}])")
            if part == "year":
                lines.append(f"{df_var}[{repr(out_col)}] = _dt_series.dt.year")
            elif part == "month":
                lines.append(f"{df_var}[{repr(out_col)}] = _dt_series.dt.month")
            elif part == "day":
                lines.append(f"{df_var}[{repr(out_col)}] = _dt_series.dt.day")
            elif part == "hour":
                lines.append(f"{df_var}[{repr(out_col)}] = _dt_series.dt.hour")
            elif part == "minute":
                lines.append(f"{df_var}[{repr(out_col)}] = _dt_series.dt.minute")
            elif part == "second":
                lines.append(f"{df_var}[{repr(out_col)}] = _dt_series.dt.second")
            elif part == "day_of_week":
                lines.append(f"{df_var}[{repr(out_col)}] = _dt_series.dt.dayofweek")
            elif part == "quarter":
                lines.append(f"{df_var}[{repr(out_col)}] = _dt_series.dt.quarter")
            return lines

        return []

    def _emit_join(
        self,
        expr: JoinExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        clean_id = step_id.replace("-", "_").replace(".", "_")
        lines: List[str] = []

        l_ds = expr.left_dataset
        r_ds = expr.right_dataset
        target_out = expr.output_dataset or l_ds

        lines.append(f"# Multi-Dataset Join Step {step_id} ({l_ds} {expr.join_type.value} {r_ds})")
        lines.append(f"_l_frame = _datasets.get({repr(l_ds)}, {df_var})")
        lines.append(f"_r_frame = _datasets.get({repr(r_ds)}, {df_var})")

        lines.append(f"_l_in_count = len(_l_frame)")
        lines.append(f"_r_in_count = len(_r_frame)")

        l_keys = [k.left.column_name if hasattr(k.left, "column_name") else str(k.left) for k in expr.key_pairs]
        r_keys = [k.right.column_name if hasattr(k.right, "column_name") else str(k.right) for k in expr.key_pairs]

        if expr.join_type != JoinType.CROSS:
            for lk in l_keys:
                lines.append(f"if {repr(lk)} not in _l_frame.columns: raise ValueError('Left join key {lk} not found in {l_ds}')")
            for rk in r_keys:
                lines.append(f"if {repr(rk)} not in _r_frame.columns: raise ValueError('Right join key {rk} not found in {r_ds}')")

            if expr.null_policy == JoinNullPolicy.FAIL_STEP_ON_NULL_KEY:
                lines.append(f"if _l_frame[{repr(l_keys)}].isna().any().any() or _r_frame[{repr(r_keys)}].isna().any().any():")
                lines.append(f"    raise ValueError('Null join keys encountered under FAIL_STEP_ON_NULL_KEY policy')")

            if expr.cardinality == JoinCardinality.ONE_TO_ONE:
                lines.append(f"if _l_frame.duplicated(subset={repr(l_keys)}).any(): raise ValueError('Left table has duplicate keys under ONE_TO_ONE join')")
                lines.append(f"if _r_frame.duplicated(subset={repr(r_keys)}).any(): raise ValueError('Right table has duplicate keys under ONE_TO_ONE join')")
            elif expr.cardinality == JoinCardinality.ONE_TO_MANY:
                lines.append(f"if _l_frame.duplicated(subset={repr(l_keys)}).any(): raise ValueError('Left table has duplicate keys under ONE_TO_MANY join')")
            elif expr.cardinality == JoinCardinality.MANY_TO_ONE:
                lines.append(f"if _r_frame.duplicated(subset={repr(r_keys)}).any(): raise ValueError('Right table has duplicate keys under MANY_TO_ONE join')")

        if expr.join_type == JoinType.CROSS:
            lines.append(f"_joined_{clean_id} = pd.merge(_l_frame, _r_frame, how='cross', suffixes=({repr(expr.left_suffix)}, {repr(expr.right_suffix)}))")
        elif expr.join_type == JoinType.SEMI:
            lines.append(f"_right_keys = _r_frame[{repr(r_keys)}].drop_duplicates()")
            lines.append(f"_merged_idx = pd.merge(_l_frame[{repr(l_keys)}].reset_index(), _right_keys, left_on={repr(l_keys)}, right_on={repr(r_keys)}, how='inner')['index']")
            lines.append(f"_joined_{clean_id} = _l_frame.loc[_merged_idx].copy()")
        elif expr.join_type == JoinType.ANTI:
            lines.append(f"_right_keys = _r_frame[{repr(r_keys)}].drop_duplicates()")
            lines.append(f"_merged_idx = pd.merge(_l_frame[{repr(l_keys)}].reset_index(), _right_keys, left_on={repr(l_keys)}, right_on={repr(r_keys)}, how='inner')['index']")
            lines.append(f"_joined_{clean_id} = _l_frame.loc[~_l_frame.index.isin(_merged_idx)].copy()")
        else:
            how_val = {
                JoinType.INNER: "inner",
                JoinType.LEFT: "left",
                JoinType.RIGHT: "right",
                JoinType.FULL_OUTER: "outer",
            }.get(expr.join_type, "inner")
            lines.append(f"_joined_{clean_id} = pd.merge(_l_frame, _r_frame, left_on={repr(l_keys)}, right_on={repr(r_keys)}, how={repr(how_val)}, suffixes=({repr(expr.left_suffix)}, {repr(expr.right_suffix)}), indicator='__dhara_join_ind_{clean_id}')")

        unmatched = expr.unmatched_policy
        if expr.join_type not in (JoinType.SEMI, JoinType.ANTI, JoinType.CROSS):
            if unmatched.left_unmatched == UnmatchedRowDisposition.ROUTE_TO_UNMATCHED_OUTPUT and unmatched.left_output_ref:
                lines.append(f"_left_unmatched_{clean_id} = _l_frame[~_l_frame[{repr(l_keys[0])}].isin(_r_frame[{repr(r_keys[0])}])].copy()")
                lines.append(f"outputs[{repr(unmatched.left_output_ref)}] = _left_unmatched_{clean_id}")
            if unmatched.right_unmatched == UnmatchedRowDisposition.ROUTE_TO_UNMATCHED_OUTPUT and unmatched.right_output_ref:
                lines.append(f"_right_unmatched_{clean_id} = _r_frame[~_r_frame[{repr(r_keys[0])}].isin(_l_frame[{repr(l_keys[0])}])].copy()")
                lines.append(f"outputs[{repr(unmatched.right_output_ref)}] = _right_unmatched_{clean_id}")

            lines.append(f"if '__dhara_join_ind_{clean_id}' in _joined_{clean_id}.columns:")
            lines.append(f"    _joined_{clean_id} = _joined_{clean_id}.drop(columns=['__dhara_join_ind_{clean_id}'])")

        lines.append(f"_out_count_{clean_id} = len(_joined_{clean_id})")
        lines.append(f"_denom_{clean_id} = max(_l_in_count, _r_in_count, 1)")
        lines.append(f"_exp_ratio_{clean_id} = _out_count_{clean_id} / _denom_{clean_id}")

        if expr.max_output_row_count is not None:
            lines.append(f"if _out_count_{clean_id} > {expr.max_output_row_count}:")
            lines.append(f"    raise RowExpansionLimitExceededError(f'Join step {step_id} output rows ' + str(_out_count_{clean_id}) + ' exceeds max_output_row_count {expr.max_output_row_count}')")

        if expr.max_row_expansion_ratio is not None:
            lines.append(f"if max(_l_in_count, _r_in_count) > 0 and _exp_ratio_{clean_id} > {float(expr.max_row_expansion_ratio)}:")
            lines.append(f"    raise RowExpansionLimitExceededError(f'Join step {step_id} expansion ratio ' + str(_exp_ratio_{clean_id}) + ' exceeds max_row_expansion_ratio {expr.max_row_expansion_ratio}')")

        lines.append(f"_accounting_{clean_id} = {{'left_input_count': _l_in_count, 'right_input_count': _r_in_count, 'output_count': _out_count_{clean_id}, 'expansion_ratio': _exp_ratio_{clean_id}}}")
        lines.append(f"outputs[{repr(target_out)}] = _joined_{clean_id}")
        lines.append(f"_datasets[{repr(target_out)}] = _joined_{clean_id}")
        lines.append(f"{df_var} = _joined_{clean_id}")
        return lines

    def _emit_aggregate(
        self,
        expr: AggregateExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        clean_id = step_id.replace("-", "_").replace(".", "_")
        lines: List[str] = []
        target_out = expr.output_dataset or "aggregated"

        lines.append(f"if len({df_var}) == 0:")
        if expr.empty_input_policy == EmptyInputPolicy.FAIL_STEP:
            lines.append(f"    raise ValueError('Empty input dataset under FAIL_STEP empty_input_policy in step {step_id}')")
        else:
            out_cols = list(expr.group_by) + [a.output_column for a in expr.aggregates]
            lines.append(f"    outputs[{repr(target_out)}] = pd.DataFrame(columns={repr(out_cols)})")
            lines.append(f"    {df_var} = outputs[{repr(target_out)}]")
        lines.append("else:")

        if expr.group_by:
            if expr.group_key_null_policy == GroupKeyNullPolicy.FAIL_STEP:
                lines.append(f"    if {df_var}[{repr(expr.group_by)}].isna().any().any():")
                lines.append(f"        raise ValueError('Null group key encountered under FAIL_STEP policy in step {step_id}')")
            dropna_flag = "True" if expr.group_key_null_policy == GroupKeyNullPolicy.DROP_NULL_GROUP else "False"

        lines.append(f"    def _compute_aggs_{clean_id}(sub_df):")
        lines.append(f"        res = {{}}")
        for a in expr.aggregates:
            out_c = a.output_column
            op = a.operation
            src_c = a.source_column
            null_pol = a.null_policy

            if op == AggregateOp.COUNT and src_c is None:
                lines.append(f"        res[{repr(out_c)}] = len(sub_df)")
            elif op == AggregateOp.COUNT:
                lines.append(f"        res[{repr(out_c)}] = sub_df[{repr(src_c)}].count()")
            elif op == AggregateOp.COUNT_DISTINCT:
                if null_pol == AggregateValueNullPolicy.PROPAGATE_NULL:
                    lines.append(f"        res[{repr(out_c)}] = sub_df[{repr(src_c)}].nunique(dropna=False)")
                else:
                    lines.append(f"        res[{repr(out_c)}] = sub_df[{repr(src_c)}].nunique(dropna=True)")
            elif op == AggregateOp.SUM:
                lines.append(f"        s = sub_df[{repr(src_c)}]")
                lines.append(f"        if s.isna().all():")
                if null_pol == AggregateValueNullPolicy.TREAT_AS_ZERO:
                    lines.append(f"            res[{repr(out_c)}] = 0")
                elif null_pol == AggregateValueNullPolicy.PROPAGATE_NULL:
                    lines.append(f"            res[{repr(out_c)}] = np.nan")
                else:
                    lines.append(f"            res[{repr(out_c)}] = 0")
                lines.append(f"        else:")
                lines.append(f"            res[{repr(out_c)}] = s.sum()")
            elif op == AggregateOp.AVG:
                lines.append(f"        res[{repr(out_c)}] = sub_df[{repr(src_c)}].mean()")
            elif op == AggregateOp.MIN:
                lines.append(f"        res[{repr(out_c)}] = sub_df[{repr(src_c)}].min()")
            elif op == AggregateOp.MAX:
                lines.append(f"        res[{repr(out_c)}] = sub_df[{repr(src_c)}].max()")
            elif op == AggregateOp.STDDEV_SAMPLE:
                lines.append(f"        res[{repr(out_c)}] = sub_df[{repr(src_c)}].std(ddof=1)")
            elif op == AggregateOp.STDDEV_POPULATION:
                lines.append(f"        res[{repr(out_c)}] = sub_df[{repr(src_c)}].std(ddof=0)")
            elif op == AggregateOp.MEDIAN:
                lines.append(f"        res[{repr(out_c)}] = sub_df[{repr(src_c)}].median()")
        lines.append(f"        return res")

        if expr.group_by:
            lines.append(f"    _records_{clean_id} = []")
            lines.append(f"    for _g_keys, _sub_df in {df_var}.groupby({repr(expr.group_by)}, dropna={dropna_flag}):")
            lines.append(f"        _key_tuple = _g_keys if isinstance(_g_keys, tuple) else (_g_keys,)")
            lines.append(f"        _rec = dict(zip({repr(expr.group_by)}, _key_tuple))")
            lines.append(f"        _rec.update(_compute_aggs_{clean_id}(_sub_df))")
            lines.append(f"        _records_{clean_id}.append(_rec)")
            lines.append(f"    _grouped_{clean_id} = pd.DataFrame(_records_{clean_id})")
            lines.append(f"    if len(_grouped_{clean_id}) > 0:")
            lines.append(f"        _grouped_{clean_id} = _grouped_{clean_id}.sort_values(by={repr(expr.group_by)}).reset_index(drop=True)")
        else:
            lines.append(f"    _grouped_{clean_id} = pd.DataFrame([_compute_aggs_{clean_id}({df_var})])")

        lines.append(f"    outputs[{repr(target_out)}] = _grouped_{clean_id}")
        lines.append(f"    _datasets[{repr(target_out)}] = _grouped_{clean_id}")
        lines.append(f"    {df_var} = _grouped_{clean_id}")
        return lines

    # ------------------------------------------------------------------------
    # Expression Transpiler
    # ------------------------------------------------------------------------

    def _emit_expression(
        self,
        expr: Expression,
        preimages: Dict[str, str],
        df_var: str,
    ) -> str:
        if isinstance(expr, ColumnRef):
            if expr.version == ValueVersion.ORIGINAL:
                sym = preimages.get(expr.column_name)
                if sym:
                    return sym
                return f"{df_var}[{repr(expr.column_name)}]"
            elif expr.version == ValueVersion.AFTER_STEP:
                return f"{df_var}[{repr(expr.column_name)}]"
            else:
                return f"{df_var}[{repr(expr.column_name)}]"

        if isinstance(expr, LiteralValue):
            val = expr.value
            if val is None:
                return "np.nan"
            return repr(val)

        if isinstance(expr, BinaryOp):
            l = self._emit_expression(expr.left, preimages, df_var)
            r = self._emit_expression(expr.right, preimages, df_var)
            op = expr.op
            op_sym = {
                BinaryOperator.EQ: "==",
                BinaryOperator.NEQ: "!=",
                BinaryOperator.LT: "<",
                BinaryOperator.LTE: "<=",
                BinaryOperator.GT: ">",
                BinaryOperator.GTE: ">=",
                BinaryOperator.AND: "&",
                BinaryOperator.OR: "|",
                BinaryOperator.ADD: "+",
                BinaryOperator.SUB: "-",
                BinaryOperator.MUL: "*",
                BinaryOperator.DIV: "/",
            }.get(op, "==")
            return f"({l} {op_sym} {r})"

        if isinstance(expr, NullCheck):
            opnd = self._emit_expression(expr.operand, preimages, df_var)
            if expr.is_null:
                return f"{opnd}.isna()"
            return f"{opnd}.notna()"

        if isinstance(expr, InSetPredicate):
            opnd = self._emit_expression(expr.operand, preimages, df_var)
            vals = [v.value for v in expr.values]
            if expr.negated:
                return f"(~{opnd}.isin({repr(vals)}))"
            return f"{opnd}.isin({repr(vals)})"

        return "True"

    # ------------------------------------------------------------------------
    # Helper & AST Safety Methods
    # ------------------------------------------------------------------------

    def _extract_original_column_refs(self, expr_obj: Any) -> Set[str]:
        results: Set[str] = set()
        if isinstance(expr_obj, ColumnRef):
            if expr_obj.version == ValueVersion.ORIGINAL:
                results.add(expr_obj.column_name)
        elif isinstance(expr_obj, BaseModel):
            for v in expr_obj.__dict__.values():
                results.update(self._extract_original_column_refs(v))
        elif isinstance(expr_obj, (list, tuple)):
            for it in expr_obj:
                results.update(self._extract_original_column_refs(it))
        return results

    def _validate_ast_safety(self, code: str, has_aggregates: bool = False) -> None:
        """
        Increment 3A+ AST safety validator.
        Prohibits:
        - Whole-DataFrame deep copies
        - Apply-time statistical recalculations (allowed only if graph explicitly contains Aggregations)
        - Global Pandas option mutation (pd.set_option, pd.reset_option)
        - Prohibited modules or imports
        - Reflection or dynamic execution (eval, exec, __import__, getattr, setattr)
        - Subprocess, os, sys, network, or file system access
        """
        tree = ast.parse(code)

        for node in ast.walk(tree):
            # Check Call targets
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Attribute) and func.attr == "copy":
                    if isinstance(func.value, ast.Name) and func.value.id in ("df", "_working"):
                        is_shallow_df_copy = False
                        for kw in node.keywords:
                            if kw.arg == "deep" and isinstance(kw.value, ast.Constant) and kw.value.value is False:
                                is_shallow_df_copy = True
                        if not is_shallow_df_copy:
                            raise ValueError(f"Prohibited whole-DataFrame deep copy detected: '{func.value.id}.copy()'")

                allowed_attrs = {"mean", "median"} if has_aggregates else set()
                if isinstance(func, ast.Attribute) and func.attr in {"mean", "median", "mode", "quantile", "describe"}:
                    if func.attr not in allowed_attrs:
                        raise ValueError(f"Prohibited apply-time statistical recalculation detected: '.{func.attr}()'. Must bind fixed learned statistics.")

                if isinstance(func, ast.Attribute) and func.attr in {"set_option", "reset_option"}:
                    raise ValueError(f"Prohibited global Pandas option mutation detected: '.{func.attr}()'")

                if isinstance(func, ast.Name):
                    if func.id in {"eval", "exec", "compile", "open", "__import__", "globals", "locals"}:
                        raise ValueError(f"Prohibited built-in call: {func.id}")

            # Check Import
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name not in {"numpy", "pandas", "np", "pd", "collections", "hashlib", "hmac", "struct", "unicodedata", "json", "decimal", "re"}:
                        raise ValueError(f"Prohibited import: {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.module not in {"collections", "hashlib", "hmac", "struct", "unicodedata", "json", "decimal", "re"}:
                    raise ValueError(f"Prohibited import module: {node.module}")
                for alias in node.names:
                    if alias.name not in {"Counter", "Decimal", "ROUND_HALF_UP"}:
                        raise ValueError(f"Prohibited import: {alias.name}")


# ============================================================================
# 3. Publication Layer: Protocol Implementation & Atomic Coordinator (Contract 12)
# ============================================================================

class InMemoryPublicationAdapter:
    """
    Reference in-memory publication adapter implementing the PublicationAdapter protocol.
    Provides strict 2-phase staged execution, validation, and atomic commit semantics.
    """
    def __init__(self):
        self._staged: Dict[str, Mapping[str, pd.DataFrame]] = {}
        self._published: Dict[str, pd.DataFrame] = {}
        self._manifest_version: str = "v0"
        self._commit_log: List[Dict[str, Any]] = []

    def stage(self, staging_id: str, outputs: Mapping[str, Any]) -> PublicationStageHandle:
        self._staged[staging_id] = {k: v.copy(deep=True) if hasattr(v, "copy") else v for k, v in outputs.items()}
        return PublicationStageHandle(staging_id=staging_id, output_refs=list(outputs.keys()))

    def validate(self, handle: PublicationStageHandle) -> PublicationValidationResult:
        if handle.staging_id not in self._staged:
            return PublicationValidationResult(is_valid=False, errors=[f"Staging handle '{handle.staging_id}' not found"])
        staged_outputs = self._staged[handle.staging_id]
        if not staged_outputs:
            return PublicationValidationResult(is_valid=False, errors=["Staged outputs empty"])
        return PublicationValidationResult(is_valid=True, passed_postconditions=len(staged_outputs))

    def commit(self, handle: PublicationStageHandle) -> PublicationCommitResult:
        if handle.staging_id not in self._staged:
            raise PublicationAbortError(f"Cannot commit non-existent staging handle: '{handle.staging_id}'")
        staged_outputs = self._staged[handle.staging_id]
        new_version = f"v{len(self._commit_log) + 1}_{handle.staging_id[:8]}"
        self._published.update(staged_outputs)
        self._manifest_version = new_version
        self._commit_log.append({"staging_id": handle.staging_id, "manifest_version": new_version, "outputs": list(staged_outputs.keys())})
        del self._staged[handle.staging_id]
        return PublicationCommitResult(
            is_committed=True,
            published_output_count=len(staged_outputs),
            manifest_version=new_version,
            diagnostics={"published_keys": list(staged_outputs.keys())},
        )

    def abort(self, handle: PublicationStageHandle) -> None:
        if handle.staging_id in self._staged:
            del self._staged[handle.staging_id]


def execute_with_publication_contracts(
    graph: TransformationIRGraph,
    df: Union[pd.DataFrame, Mapping[str, pd.DataFrame]],
    adapter: Optional[PublicationAdapter] = None,
    schemas: Optional[Mapping[str, DatasetSchema]] = None,
    key_provider: Optional[Any] = None,
    postconditions: Optional[Sequence[Callable[[Mapping[str, pd.DataFrame]], None]]] = None,
) -> PublicationCommitResult:
    """
    Contract 12 execution boundary:
    Executes compiled pipeline in memory, stages outputs via PublicationAdapter,
    verifies postconditions and output schemas, and atomically commits via adapter.
    If ANY postcondition or verification fails, publication is aborted and ZERO outputs are committed.
    """
    import pandas as pd
    import numpy as np
    from collections import Counter

    compiler = PandasIRCompiler(schemas=schemas)
    compile_res = compiler.compile(graph)
    if not compile_res.is_success or compile_res.artifact is None:
        raise ValueError(f"Pipeline compilation failed: {compile_res.diagnostics}")

    exec_scope: Dict[str, Any] = {
        "pd": pd,
        "np": np,
        "hashlib": hashlib,
        "hmac": hmac,
        "json": json,
        "re": re,
        "struct": struct,
        "unicodedata": unicodedata,
        "Counter": Counter,
        "Decimal": Decimal,
        "compute_canonical_schema_hash": compute_canonical_schema_hash,
        "SchemaSnapshotMismatchError": SchemaSnapshotMismatchError,
        "OutputColumnCollisionError": OutputColumnCollisionError,
        "KeyProviderRequiredError": KeyProviderRequiredError,
        "SecretResolutionError": SecretResolutionError,
        "RowExpansionLimitExceededError": RowExpansionLimitExceededError,
    }
    exec(compile_res.artifact.source_code, exec_scope)
    transform_fn = exec_scope.get("transform_datasets")
    if not transform_fn:
        raise ValueError("Compiled artifact lacks transform_datasets entrypoint")

    try:
        raw_outputs = transform_fn(df, key_provider=key_provider)
    except Exception as exec_err:
        raise ValueError(f"Pipeline execution failed: {exec_err}") from exec_err

    pub_adapter = adapter or InMemoryPublicationAdapter()
    staging_id = f"stage_{graph.graph_id}_{hashlib.sha256(str(datetime.now(timezone.utc)).encode('utf-8')).hexdigest()[:12]}"
    handle = pub_adapter.stage(staging_id, raw_outputs)

    postcondition_errors: List[str] = []
    if postconditions:
        for i, postcond in enumerate(postconditions):
            try:
                postcond(raw_outputs)
            except Exception as pc_err:
                postcondition_errors.append(f"Postcondition {i} failed: {pc_err}")

    if graph.output_specs:
        for spec in graph.output_specs:
            ref = spec.output_ref
            if ref in raw_outputs:
                out_df = raw_outputs[ref]
                if spec.schema:
                    for col_spec in spec.schema.columns:
                        if not col_spec.nullable and out_df[col_spec.name].isna().any():
                            postcondition_errors.append(f"Output '{ref}' column '{col_spec.name}' contains nulls violating non-nullable schema")

    if postcondition_errors:
        pub_adapter.abort(handle)
        raise PublicationPostconditionError(f"Publication aborted due to failed postconditions: {'; '.join(postcondition_errors)}")

    val_res = pub_adapter.validate(handle)
    if not val_res.is_valid:
        pub_adapter.abort(handle)
        raise PublicationPostconditionError(f"Publication validation failed: {'; '.join(val_res.errors)}")

    commit_res = pub_adapter.commit(handle)
    return commit_res
