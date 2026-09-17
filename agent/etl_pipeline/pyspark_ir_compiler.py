"""
PySpark IR Compiler (Transformation IR v1.0 -> Executable PySpark DataFrame Pipeline)
=====================================================================================

Phase 3 (Increment 3.1): PySpark compiler for scalar, parse, clip, flag, and apply-only imputation:
- MAP_VALUE (11 operations: TRIM, LOWER, UPPER, CAST, REGEX_SUBSTITUTE, LOOKUP_MAP,
             NORMALIZE_PHONE, SANITIZE_EMAIL, STANDARDIZE_BOOLEAN, PARSE_NUMBER, PARSE_DATE)
- CONDITIONAL_REPLACE (with three-valued unknown policies)
- CLIP (with null and infinity policies)
- FLAG (with three-valued logic and existing-column policies)
- STANDARDIZE (STANDARDIZE_BOOLEAN with disjoint token sets)
- PARSE (Numeric, Date, Boolean parsing with distinct null vs parse error tracking)
- IMPUTE (Apply-only: constant, pre-fitted global statistical values, pre-fitted grouped imputation)
- PASS_THROUGH (control construct, non-emitting)

Core Architectural Invariants:
1. Strict Step Accounting:
   Every submitted IR step receives exactly one disposition:
   COMPILED, CONTROL_ONLY, UNSUPPORTED, INVALID_SCHEMA_BINDING, INVALID_IR.
   Input step IDs == disposition step IDs.
2. Capability Registry Governance:
   Every step is evaluated against the 11-field EngineCapabilityKey.
   If not execution_eligible, compilation fails atomically and returns artifact=None.
3. Fit / Apply Imputation Invariant:
   Generated PySpark apply code NEVER recalculates statistical aggregations
   (mean, median, mode, quantile, describe) over the execution DataFrame.
   Consumes and verifies immutable LearnedImputationModel snapshot hashes.
4. Pure Catalyst Execution:
   Emits native Spark Catalyst expressions (F.col, F.when, F.lit, F.coalesce, F.trim, etc.)
   operating directly in the JVM without Python worker serialization bottlenecks.
5. AST Safety Validation:
   Inspects emitted Python AST to guarantee safety and reject reflection, dynamic exec,
   or unapproved statistical functions.
"""

from __future__ import annotations

import ast
import datetime as _dt
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import hashlib
import json
import re
import struct
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from pydantic import BaseModel, Field, computed_field, model_validator

from agent.etl_pipeline.codegen_shared import CodegenStatus
from agent.etl_pipeline.cross_engine.capability_models import (
    CapabilityDecision,
    CapabilityStatus,
    EngineCapabilityKey,
)
from agent.etl_pipeline.cross_engine.capability_registry import (
    EngineCapabilityRegistry,
    get_global_capability_registry,
)
from agent.etl_pipeline.cross_engine.runtime_profiles import (
    SparkRuntimeProfile,
    compute_runtime_profile_hash,
)
from agent.etl_pipeline.pandas_ir_compiler import (
    CompilerAccountingError,
    CompilerStepDisposition,
    PreimageBinding,
)
from agent.etl_pipeline.transformation_ir import (
    TRANSFORMATION_IR_VERSION,
    ActionFamily,
    AmbiguousTimePolicy,
    BinaryOp,
    BinaryOperator,
    BooleanFormatSpec,
    CastColumnExpression,
    CastFailurePolicy,
    ClipExpression,
    ClipNullPolicy,
    CollisionPolicy,
    ColumnDropSpec,
    ColumnProjectSpec,
    ColumnProtectionClass,
    ColumnRef,
    ColumnRenameSpec,
    ColumnRole,
    ConditionalReplaceExpression,
    CreateColumnExpression,
    DataType,
    DateFormatSpec,
    DatasetSchema,
    DeduplicateExpression,
    DeduplicationTieBreaker,
    DigestEncoding,
    DropColumnsExpression,
    DuplicateExportMode,
    EmailMaskDomainPolicy,
    ExistingColumnPolicy,
    FilterDropPolicy,
    FilterExpression,
    FilterUnknownPolicy,
    FlagExpression,
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
    LearnedImputationModel,
    LiteralValue,
    MaskExpression,
    MaskNullPolicy,
    MaskStrategy,
    MissingGroupPolicy,
    NonexistentTimePolicy,
    NullCheck,
    NullKeyPolicy,
    NumericFormatSpec,
    NumericOverflowPolicy,
    OccurrenceIdentityScope,
    OrderDirection,
    ParseExpression,
    ParseFailurePolicy,
    ParseOutputMode,
    PassThroughExpression,
    PredicateUnknownPolicy,
    ProjectColumnsExpression,
    QuarantineExpression,
    QuarantineUnknownPolicy,
    RenameColumnsExpression,
    ReorderColumnsExpression,
    RoutingIdentityPolicy,
    RoutingMode,
    RowIdentityStrategy,
    ScalarMapExpression,
    ScalarMapOperation,
    SchemaMutationExpression,
    SchemaMutationOp,
    SecretReference,
    ShortValuePolicy,
    SortColumnSpec,
    SourceStableIdentitySpec,
    StatisticalImputationStrategy,
    StepAuthorization,
    TemporalParseTarget,
    TransformationIRGraph,
    TransformationStepIR,
    UnboundedInfinityPolicy,
    UnmappedValuePolicy,
    ValidateExpression,
    ValidateSeverity,
    ValidationUnknownPolicy,
    ValueVersion,
    compute_canonical_schema_hash,
    compute_model_snapshot_hash,
    is_column_effectively_protected,
    verify_imputation_model,
)
from agent.etl_pipeline.canonical_encoding import (
    TAG_BOOL_FALSE,
    TAG_BOOL_TRUE,
    TAG_BYTES_PREFIX,
    TAG_INT_PREFIX,
    TAG_NULL,
    TAG_STRING_PREFIX,
    TAG_TUPLE_PREFIX,
)


# ============================================================================
# 1. Compiler Artifact & Result Models
# ============================================================================

class CompiledPySparkArtifact(BaseModel):
    """Self-contained, deterministic PySpark code artifact with cryptographically signed metadata."""
    artifact_bytes: bytes
    artifact_hash: str
    entrypoint: str = "transform(df, key_provider=None)"
    datasets_entrypoint: str = "transform_datasets(datasets, key_provider=None)"
    supports_single_output_entrypoint: bool = True
    primary_output_ref: Optional[str] = None
    required_imports: Tuple[str, ...] = ("pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types")
    compiled_step_ids: Tuple[str, ...]
    control_only_step_ids: Tuple[str, ...]
    schema_snapshot_hash: str
    runtime_profile_hash: str
    compiler_version: str = "1.0.0"

    @property
    def source_code(self) -> str:
        return self.artifact_bytes.decode("utf-8")


class PySparkCompilerResult(BaseModel):
    """Authoritative result envelope from the PySpark IR Compiler."""
    status: CodegenStatus
    artifact: Optional[CompiledPySparkArtifact] = None
    step_dispositions: Dict[str, CompilerStepDisposition]
    preimage_bindings: Tuple[PreimageBinding, ...] = ()
    diagnostics: Tuple[str, ...] = ()
    compiler_name: str = "pyspark_compiler"
    compiler_version: str = "1.0.0"
    engine_version: str = "3.5.1"
    graph_id: str
    ir_version: str = TRANSFORMATION_IR_VERSION
    runtime_profile_hash: str

    @computed_field
    @property
    def is_success(self) -> bool:
        return self.status == CodegenStatus.SUCCEEDED

    @model_validator(mode="after")
    def validate_result_invariant(self) -> "PySparkCompilerResult":
        if self.status != CodegenStatus.SUCCEEDED and self.artifact is not None:
            raise CompilerAccountingError("Failed PySpark compilation result cannot retain a compiled artifact")
        if self.status == CodegenStatus.SUCCEEDED and self.artifact is None:
            raise CompilerAccountingError("Successful PySpark compilation result must contain a compiled artifact")
        return self


# ============================================================================
# 2. PySpark IR Compiler Implementation
# ============================================================================

class PySparkIRCompiler:
    """
    Deterministic compiler translating TransformationIRGraph into executable PySpark DataFrame transformations.
    Increment 3.1: MAP_VALUE, CONDITIONAL_REPLACE, CLIP, FLAG, STANDARDIZE, PARSE, IMPUTE, PASS_THROUGH.
    """

    COMPILER_NAME = "pyspark_compiler"
    COMPILER_VERSION = "1.0.0"
    ENGINE_NAME = "pyspark"
    ENGINE_VERSION = "3.5.1"
    engine_name = "pyspark"

    SUPPORTED_FAMILIES = {
        ActionFamily.MAP_VALUE,
        ActionFamily.CONDITIONAL_REPLACE,
        ActionFamily.CLIP,
        ActionFamily.FLAG,
        ActionFamily.STANDARDIZE,
        ActionFamily.PARSE,
        ActionFamily.IMPUTE,
        ActionFamily.VALIDATE,
        ActionFamily.QUARANTINE,
        ActionFamily.FILTER,
        ActionFamily.DEDUPLICATE,
        ActionFamily.SCHEMA_MUTATION,
        ActionFamily.MASK,
        ActionFamily.HASH,
    }

    CONTROL_FAMILIES = {
        ActionFamily.PASS_THROUGH,
    }

    def __init__(
        self,
        schemas: Optional[Mapping[str, DatasetSchema]] = None,
        runtime_profile: Optional[SparkRuntimeProfile] = None,
        registry: Optional[EngineCapabilityRegistry] = None,
    ) -> None:
        self.schemas = dict(schemas or {})
        self.runtime_profile = runtime_profile or SparkRuntimeProfile()
        self.registry = registry or get_global_capability_registry()

    def compile(self, graph: TransformationIRGraph, df_var: str = "df") -> PySparkCompilerResult:
        """
        Compiles the provided TransformationIRGraph into a PySparkCompilerResult.
        Enforces bidirectional step accounting, selective pre-image preservation,
        runtime capability preflight, and Python AST safety validation.
        """
        step_dispositions: Dict[str, CompilerStepDisposition] = {}
        diagnostics: List[str] = []
        compiled_step_ids: List[str] = []
        control_only_step_ids: List[str] = []

        runtime_hash = self.runtime_profile.compute_hash()

        # 1. Bidirectional Accounting Pre-Check
        input_step_ids = {s.step_id for s in graph.steps}
        policy = graph.routing_identity_policy
        internal_col = policy.internal_column if policy else "__dhara_row_occurrence_0"
        simulated_columns: Dict[str, Dict[str, Any]] = {ds: dict(s.columns) for ds, s in self.schemas.items()}

        # 2. Step-by-Step Preflight and Disposition
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
                diagnostics.append(f"Step '{sid}' uses family '{family.value}' which is not implemented in {self.COMPILER_NAME}")
                continue

            # B1. Destructive Action Authorization Check
            if isinstance(step.expression, SchemaMutationExpression) and step.expression.op == SchemaMutationOp.DROP_COLUMN:
                auth = step.authorization
                if not auth:
                    step_dispositions[sid] = CompilerStepDisposition.INVALID_IR
                    diagnostics.append(f"Step '{sid}' destructive drop requires a valid StepAuthorization (UnauthorizedDestructiveActionError)")
                    continue
                if auth.step_id != sid:
                    step_dispositions[sid] = CompilerStepDisposition.INVALID_IR
                    diagnostics.append(f"Step '{sid}' StepAuthorization step_id mismatch: expected '{sid}', got '{auth.step_id}' (UnauthorizedDestructiveActionError)")
                    continue
                if auth.expires_at < datetime.now(timezone.utc):
                    step_dispositions[sid] = CompilerStepDisposition.INVALID_IR
                    diagnostics.append(f"Step '{sid}' StepAuthorization is expired (UnauthorizedDestructiveActionError)")
                    continue
                if auth.authorized_step_hash and auth.authorized_step_hash != step.semantic_hash:
                    step_dispositions[sid] = CompilerStepDisposition.INVALID_IR
                    diagnostics.append(f"Step '{sid}' StepAuthorization step_hash mismatch (UnauthorizedDestructiveActionError)")
                    continue

            # B2. Static Schema Binding Checks for SchemaMutation
            if isinstance(step.expression, SchemaMutationExpression):
                ds_name = step.dataset_name or "primary"
                curr_cols = simulated_columns.get(ds_name)
                if curr_cols is not None:
                    expr_mut = step.expression
                    if expr_mut.op == SchemaMutationOp.DROP_COLUMN:
                        drop_cols = [d.column for d in expr_mut.drops] if expr_mut.drops else ([expr_mut.target_column] if expr_mut.target_column else [])
                        failed_drop = False
                        for dc in drop_cols:
                            if dc == internal_col:
                                step_dispositions[sid] = CompilerStepDisposition.INVALID_SCHEMA_BINDING
                                diagnostics.append(f"Step '{sid}' attempts to drop protected technical routing identity column '{dc}' (ProtectedColumnDropError)")
                                failed_drop = True
                                break
                            if dc not in curr_cols:
                                step_dispositions[sid] = CompilerStepDisposition.INVALID_SCHEMA_BINDING
                                diagnostics.append(f"Step '{sid}' attempts to drop non-existent column '{dc}' (SCHEMA_BINDING_FAILURE)")
                                failed_drop = True
                                break
                            col_meta = curr_cols[dc]
                            if is_column_effectively_protected(col_meta):
                                step_dispositions[sid] = CompilerStepDisposition.INVALID_SCHEMA_BINDING
                                diagnostics.append(f"Step '{sid}' attempts to drop protected column '{dc}' (ProtectedColumnDropError)")
                                failed_drop = True
                                break
                        if failed_drop:
                            continue
                        for dc in drop_cols:
                            curr_cols.pop(dc, None)

                    elif expr_mut.op == SchemaMutationOp.RENAME_COLUMN:
                        failed_ren = False
                        for r in expr_mut.renames:
                            if r.old_name == internal_col:
                                step_dispositions[sid] = CompilerStepDisposition.INVALID_SCHEMA_BINDING
                                diagnostics.append(f"Step '{sid}' attempts to rename protected technical routing identity column '{r.old_name}' (ProtectedColumnDropError)")
                                failed_ren = True
                                break
                            if r.old_name not in curr_cols:
                                step_dispositions[sid] = CompilerStepDisposition.INVALID_SCHEMA_BINDING
                                diagnostics.append(f"Step '{sid}' attempts to rename non-existent column '{r.old_name}' (SCHEMA_BINDING_FAILURE)")
                                failed_ren = True
                                break
                            if r.new_name in curr_cols and r.new_name != r.old_name:
                                step_dispositions[sid] = CompilerStepDisposition.INVALID_SCHEMA_BINDING
                                diagnostics.append(f"Step '{sid}' rename target '{r.new_name}' collides with existing column (SCHEMA_BINDING_FAILURE)")
                                failed_ren = True
                                break
                        if failed_ren:
                            continue
                        for r in expr_mut.renames:
                            col_meta = curr_cols.pop(r.old_name)
                            curr_cols[r.new_name] = col_meta

                    elif expr_mut.op == SchemaMutationOp.PROJECT:
                        if expr_mut.projection:
                            failed_proj = False
                            proj_set = set(expr_mut.projection.projected_columns)
                            if not expr_mut.projection.allow_missing:
                                for pc in expr_mut.projection.projected_columns:
                                    if pc not in curr_cols:
                                        step_dispositions[sid] = CompilerStepDisposition.INVALID_SCHEMA_BINDING
                                        diagnostics.append(f"Step '{sid}' projected column '{pc}' not in schema (SCHEMA_BINDING_FAILURE)")
                                        failed_proj = True
                                        break
                            if failed_proj:
                                continue
                            # Protected columns check: PROJECT cannot drop protected column by excluding it
                            for col_name, col_meta in curr_cols.items():
                                if col_name not in proj_set and is_column_effectively_protected(col_meta):
                                    step_dispositions[sid] = CompilerStepDisposition.INVALID_SCHEMA_BINDING
                                    diagnostics.append(f"Step '{sid}' PROJECT excludes protected column '{col_name}' (ProtectedColumnDropError)")
                                    failed_proj = True
                                    break
                            if failed_proj:
                                continue
                            curr_cols = {c: m for c, m in curr_cols.items() if c in proj_set}
                            simulated_columns[ds_name] = curr_cols

            # B3. Preflight Checks for HashExpression
            if isinstance(step.expression, HashExpression):
                expr_hash = step.expression
                if expr_hash.mode == HashMode.HMAC:
                    step_dispositions[sid] = CompilerStepDisposition.UNSUPPORTED
                    diagnostics.append(f"Step '{sid}' uses HMAC mode which is REFERENCE_TEST_ONLY and not supported in {self.COMPILER_NAME}")
                    continue
                ds_name = step.dataset_name or "primary"
                ds_schema = self.schemas.get(ds_name)
                in_col = expr_hash.source_column or (expr_hash.input_column.column_name if expr_hash.input_column else "")
                if ds_schema and in_col in ds_schema.columns:
                    col_dtype = ds_schema.columns[in_col].logical_type
                    if col_dtype == DataType.STRING:
                        step_dispositions[sid] = CompilerStepDisposition.UNSUPPORTED
                        diagnostics.append(f"Step '{sid}' attempts canonical hashing on STRING column '{in_col}' which is blocked (NOT_TESTED) pending Unicode NFC normalization contract")
                        continue

            # C. Preflight Capability Check via Registry
            sub_type = self._derive_expression_subtype(step.expression)
            pol_hash = self._compute_policy_hash(step.expression)
            cap_key = EngineCapabilityKey(
                family=family,
                expression_subtype=sub_type,
                policy_profile_hash=pol_hash,
                ir_version=graph.ir_version,
                catalogue_version="v1.0",
                compiler_name=self.COMPILER_NAME,
                compiler_version=self.COMPILER_VERSION,
                engine_name=self.ENGINE_NAME,
                engine_version=self.ENGINE_VERSION,
                dialect=None,
                runtime_profile_hash=runtime_hash,
            )

            decision = self.registry.evaluate_capability(cap_key)
            if not decision.execution_eligible:
                step_dispositions[sid] = CompilerStepDisposition.UNSUPPORTED
                diagnostics.append(
                    f"Step '{sid}' blocked by capability registry: status={decision.status.value}, "
                    f"reason={decision.reason_code}, limitations={decision.limitations}"
                )
                continue

            # Step is valid for compilation
            step_dispositions[sid] = CompilerStepDisposition.COMPILED
            compiled_step_ids.append(sid)

        # Invariant: input step IDs == disposition step IDs
        if set(step_dispositions.keys()) != input_step_ids:
            raise CompilerAccountingError("PySpark step disposition keys do not exactly match input steps")

        # Atomic Failure: If any step is not COMPILED or CONTROL_ONLY, fail compilation
        unsupported_count = sum(1 for d in step_dispositions.values() if d == CompilerStepDisposition.UNSUPPORTED)
        invalid_count = sum(
            1 for d in step_dispositions.values()
            if d in (CompilerStepDisposition.INVALID_IR, CompilerStepDisposition.INVALID_SCHEMA_BINDING)
        )

        if unsupported_count > 0 or invalid_count > 0:
            return PySparkCompilerResult(
                status=CodegenStatus.FAILED_COMPILATION,
                artifact=None,
                step_dispositions=step_dispositions,
                diagnostics=tuple(diagnostics),
                graph_id=graph.graph_id,
                ir_version=graph.ir_version,
                runtime_profile_hash=runtime_hash,
            )

        # 3. Selective Pre-Image Tracking (ValueVersion.ORIGINAL)
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

        # 4. Assembly of PySpark Code Lines
        code_lines: List[str] = [
            "# Auto-generated by Agent Dhara PySpark IR Compiler v1.0.0",
            f"# Ir-Version: {graph.ir_version}",
            f"# Graph-Id: {graph.graph_id}",
            f"# Runtime-Profile-Hash: {runtime_hash}",
            "from collections import Counter",
            "from decimal import Decimal",
            "import re",
            "from pyspark.sql import DataFrame",
            "from pyspark.sql import functions as F",
            "from pyspark.sql import types as T",
            "from pyspark.sql.window import Window",
            "from agent.etl_pipeline.transformation_ir import compute_canonical_schema_hash",
            "",
            "def transform_datasets(datasets, key_provider=None):",
            "    if isinstance(datasets, dict):",
            "        _datasets = dict(datasets)",
            "        _working = next(iter(_datasets.values())) if _datasets else None",
            "    else:",
            "        _working = datasets",
            "        _datasets = {'primary': _working}",
            "",
            "    outputs = {}",
            "    validation_summaries = {}",
            "",
        ]

        # Occurrence Identity Lifecycle
        policy = graph.routing_identity_policy
        scope = policy.scope if policy else OccurrenceIdentityScope.EXECUTION_LOCAL
        spec = policy.source_stable_spec if policy else None
        internal_col = policy.internal_column if policy else "__dhara_row_occurrence_0"
        retain_in_primary = policy.retain_in_primary_output if policy else False
        retain_in_secondary = policy.retain_in_secondary_outputs if policy else True
        published_col = policy.published_column if policy else None

        code_lines.append(f"    # --- Occurrence Identity Lifecycle ({scope.value}) ---")
        code_lines.append(f"    _identity_col = {repr(internal_col)}")
        if scope == OccurrenceIdentityScope.SOURCE_STABLE and spec:
            coords_spec = [
                ("snapshot_id", "string", f"F.lit({repr(spec.snapshot_id)})"),
                (spec.partition_column, "string", f"F.col({repr(spec.partition_column)})"),
                (spec.row_offset_column, "integer", f"F.col({repr(spec.row_offset_column)})"),
            ]
            if spec.source_file_column:
                coords_spec.append((spec.source_file_column, "string", f"F.col({repr(spec.source_file_column)})"))
            if spec.ingestion_run_column:
                coords_spec.append((spec.ingestion_run_column, "string", f"F.col({repr(spec.ingestion_run_column)})"))

            header_bytes = TAG_TUPLE_PREFIX + struct.pack(">I", len(coords_spec))
            concat_parts = [f"F.lit({repr(header_bytes)})"]
            for c_name, c_type, c_expr in coords_spec:
                c_name_b = c_name.encode("utf-8")
                c_name_lit = TAG_STRING_PREFIX + struct.pack(">I", len(c_name_b)) + c_name_b
                concat_parts.append(f"F.lit({repr(c_name_lit)})")
                if c_type == "integer":
                    val_expr = (
                        f"F.when({c_expr}.isNull(), F.lit(b'\\x00DHARA_NULL_V1')).otherwise("
                        f"F.concat(F.lit(b'\\x02INT\\x00'), F.unhex(F.lpad(F.hex({c_expr}.cast('long')), 16, '0'))))"
                    )
                elif c_type == "boolean":
                    val_expr = (
                        f"F.when({c_expr}.isNull(), F.lit(b'\\x00DHARA_NULL_V1'))"
                        f".when({c_expr} == True, F.lit(b'\\x01BOOL\\x01'))"
                        f".otherwise(F.lit(b'\\x01BOOL\\x00'))"
                    )
                else:
                    # String
                    val_expr = (
                        f"F.when({c_expr}.isNull(), F.lit(b'\\x00DHARA_NULL_V1')).otherwise("
                        f"F.concat(F.lit(b'\\x05STR\\x00'), F.unhex(F.lpad(F.hex(F.length(F.encode({c_expr}.cast('string'), 'utf-8'))), 8, '0')), F.encode({c_expr}.cast('string'), 'utf-8')))"
                    )
                concat_parts.append(val_expr)

            tuple_bytes_expr = f"F.concat({', '.join(concat_parts)})"
            code_lines.append(f"    _working = _working.withColumn(_identity_col, F.sha2({tuple_bytes_expr}, 256))")
            code_lines.append(f"    _entry_cached = _working.cache()")
            code_lines.append(f"    _ = _entry_cached.count()  # Explicit materialization action before branching")
            code_lines.append(f"    _working = _entry_cached")
            code_lines.append(f"    if _working.groupBy(_identity_col).count().filter('count > 1').limit(1).count() > 0:")
            code_lines.append(f"        _entry_cached.unpersist()")
            code_lines.append(f"        raise ValueError('Duplicate source-stable coordinates detected in input DataFrame')")
        else:
            # EXECUTION_LOCAL: monotonically_increasing_id bound once at graph entry and explicitly materialized!
            code_lines.append(f"    _working = _working.withColumn(_identity_col, F.monotonically_increasing_id())")
            code_lines.append(f"    _entry_cached = _working.cache()")
            code_lines.append(f"    _ = _entry_cached.count()  # Explicit materialization action before branching")
            code_lines.append(f"    _working = _entry_cached")
        code_lines.append("")

        # Preamble: Selective Series Pre-images
        if preimage_bindings:
            code_lines.append("    # --- Selective Pre-images for ValueVersion.ORIGINAL ---")
            for binding in preimage_bindings:
                code_lines.append(
                    f"    _working = _working.withColumn({repr(binding.internal_symbol)}, F.col({repr(binding.column_ref)}))"
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

        # Drop internal pre-image columns
        if preimage_bindings:
            drop_cols = [b.internal_symbol for b in preimage_bindings]
            code_lines.append(f"    # --- Clean up internal pre-image columns ---")
            for dc in drop_cols:
                code_lines.append(f"    if {repr(dc)} in _working.columns:")
                code_lines.append(f"        _working = _working.drop({repr(dc)})")
            code_lines.append("")

        # Final Primary Output Registration
        primary_ref = graph.primary_output_ref
        if primary_ref:
            code_lines.append(f"    # --- Primary Output Registration ---")
            code_lines.append(f"    outputs[{repr(primary_ref)}] = _working")
        else:
            code_lines.append(f"    # --- Default Primary Output Registration ---")
            code_lines.append(f"    if 'primary' not in outputs:")
            code_lines.append(f"        outputs['primary'] = _working")

        # Publication Eligibility Boundary & Identity Stripping
        code_lines.append(f"    # --- Publication Eligibility Boundary & Identity Stripping ---")
        code_lines.append(f"    _primary_key = {repr(primary_ref)} if {repr(primary_ref)} else ('primary' if 'primary' in outputs else None)")
        if not retain_in_primary:
            code_lines.append(f"    if _primary_key and _primary_key in outputs and _identity_col in outputs[_primary_key].columns:")
            if published_col:
                code_lines.append(f"        outputs[_primary_key] = outputs[_primary_key].withColumnRenamed(_identity_col, {repr(published_col)})")
            else:
                code_lines.append(f"        outputs[_primary_key] = outputs[_primary_key].drop(_identity_col)")
        else:
            if published_col:
                code_lines.append(f"    if _primary_key and _primary_key in outputs and _identity_col in outputs[_primary_key].columns:")
                code_lines.append(f"        outputs[_primary_key] = outputs[_primary_key].withColumnRenamed(_identity_col, {repr(published_col)})")

        if not retain_in_secondary:
            code_lines.append(f"    for _sec_ref in list(outputs.keys()):")
            code_lines.append(f"        if _sec_ref != _primary_key and _identity_col in outputs[_sec_ref].columns:")
            code_lines.append(f"            outputs[_sec_ref] = outputs[_sec_ref].drop(_identity_col)")

        code_lines.append(f"    _entry_cached.unpersist()")
        code_lines.append(f"    transform_datasets.validation_summaries = validation_summaries")
        code_lines.append("    return outputs")
        code_lines.append("")

        # Single-dataset entrypoint
        code_lines.append("def transform(df, key_provider=None):")
        code_lines.append("    all_outputs = transform_datasets(df, key_provider=key_provider)")
        if primary_ref:
            code_lines.append(f"    return all_outputs[{repr(primary_ref)}]")
        else:
            code_lines.append("    if 'primary' in all_outputs:")
            code_lines.append("        return all_outputs['primary']")
            code_lines.append("    if len(all_outputs) == 1:")
            code_lines.append("        return next(iter(all_outputs.values()))")
            code_lines.append("    raise ValueError('Multiple outputs produced without primary_output_ref')")
        code_lines.append("")

        full_source = "\n".join(code_lines)
        artifact_bytes = full_source.encode("utf-8")
        artifact_hash = hashlib.sha256(artifact_bytes).hexdigest()

        # 5. AST Safety Validation
        try:
            self._validate_ast_safety(full_source)
        except Exception as e:
            diagnostics.append(f"AST safety inspection failed: {str(e)}")
            return PySparkCompilerResult(
                status=CodegenStatus.FAILED_COMPILATION,
                artifact=None,
                step_dispositions=step_dispositions,
                diagnostics=tuple(diagnostics),
                graph_id=graph.graph_id,
                ir_version=graph.ir_version,
                runtime_profile_hash=runtime_hash,
            )

        artifact = CompiledPySparkArtifact(
            artifact_bytes=artifact_bytes,
            artifact_hash=artifact_hash,
            entrypoint="transform(df, key_provider=None)",
            datasets_entrypoint="transform_datasets(datasets, key_provider=None)",
            supports_single_output_entrypoint=True,
            primary_output_ref=primary_ref,
            required_imports=("pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types"),
            compiled_step_ids=tuple(compiled_step_ids),
            control_only_step_ids=tuple(control_only_step_ids),
            schema_snapshot_hash=graph.schema_snapshot_hash,
            runtime_profile_hash=runtime_hash,
            compiler_version=self.COMPILER_VERSION,
        )

        return PySparkCompilerResult(
            status=CodegenStatus.SUCCEEDED,
            artifact=artifact,
            step_dispositions=step_dispositions,
            preimage_bindings=tuple(preimage_bindings),
            diagnostics=tuple(diagnostics),
            graph_id=graph.graph_id,
            ir_version=graph.ir_version,
            runtime_profile_hash=runtime_hash,
        )

    def execute(
        self,
        graph: TransformationIRGraph,
        df: Any,
        key_provider: Optional[Any] = None,
    ) -> Any:
        """Executes the compiled PySpark artifact against in-memory PySpark DataFrame(s)."""
        comp_res = self.compile(graph)
        if not comp_res.is_success or comp_res.artifact is None:
            raise RuntimeError(f"PySpark compilation failed: {comp_res.diagnostics}")
        loc: Dict[str, Any] = {}
        exec(comp_res.artifact.source_code, loc)
        datasets_fn = loc.get("transform_datasets")
        if datasets_fn:
            res_dict = datasets_fn(df, key_provider=key_provider)
            val_summaries = getattr(datasets_fn, "validation_summaries", {})
            class PySparkExecutionResult:
                def __init__(self, outputs: Dict[str, Any], val_summaries: Dict[str, Any]):
                    self.outputs = outputs
                    self.validation_summaries = val_summaries
                    self.primary_output = next(iter(outputs.values())) if outputs else None
                    self.publication_eligible = True
            return PySparkExecutionResult(res_dict, val_summaries)
        single_fn = loc.get("transform")
        if single_fn:
            primary_out = single_fn(df, key_provider=key_provider)
            val_summaries = getattr(loc.get("transform_datasets"), "validation_summaries", {})
            class PySparkExecutionResult:
                def __init__(self, primary: Any, val_summaries: Dict[str, Any]):
                    self.outputs = {"primary": primary}
                    self.validation_summaries = val_summaries
                    self.primary_output = primary
                    self.publication_eligible = True
            return PySparkExecutionResult(primary_out, val_summaries)
        raise RuntimeError("No valid entrypoint found in compiled PySpark artifact")

    # ------------------------------------------------------------------------
    # Step Emitters
    # ------------------------------------------------------------------------

    def _emit_step_code(
        self,
        step: TransformationStepIR,
        preimages: Dict[str, str],
        df_var: str = "_working",
    ) -> List[str]:
        expr = step.expression

        # 1. FLAG
        if isinstance(expr, FlagExpression):
            return self._emit_flag(expr, preimages, df_var)

        # 2. CLIP
        if isinstance(expr, ClipExpression):
            return self._emit_clip(expr, preimages, df_var)

        # 3. MAP_VALUE
        if isinstance(expr, ScalarMapExpression):
            return self._emit_scalar_map(expr, preimages, df_var)

        # 4. CONDITIONAL_REPLACE
        if isinstance(expr, ConditionalReplaceExpression):
            return self._emit_conditional_replace(expr, preimages, df_var)

        # 5. IMPUTE
        if isinstance(expr, ImputeExpression):
            return self._emit_impute(expr, preimages, df_var, step_id=step.step_id)

        # 6. PARSE
        if isinstance(expr, ParseExpression):
            return self._emit_parse(expr, preimages, df_var, step_id=step.step_id)

        # 7. VALIDATE
        if isinstance(expr, ValidateExpression):
            return self._emit_validate(expr, preimages, df_var, step_id=step.step_id)

        # 8. QUARANTINE
        if isinstance(expr, QuarantineExpression):
            return self._emit_quarantine(expr, preimages, df_var, step_id=step.step_id)

        # 9. FILTER
        if isinstance(expr, FilterExpression):
            return self._emit_filter(expr, preimages, df_var, step_id=step.step_id)

        # 10. DEDUPLICATE
        if isinstance(expr, DeduplicateExpression):
            return self._emit_deduplicate(expr, preimages, df_var, step_id=step.step_id)

        # 11. SCHEMA_MUTATION
        if isinstance(expr, SchemaMutationExpression):
            return self._emit_schema_mutation(expr, preimages, df_var, step_id=step.step_id)

        # 12. MASK
        if isinstance(expr, MaskExpression):
            return self._emit_mask(expr, preimages, df_var, step_id=step.step_id)

        # 13. HASH
        if isinstance(expr, HashExpression):
            return self._emit_hash(expr, preimages, df_var, step_id=step.step_id)

        return [f"# Unsupported expression: {type(expr).__name__}"]

    def _emit_flag(
        self,
        expr: FlagExpression,
        preimages: Dict[str, str],
        df_var: str,
    ) -> List[str]:
        out_col = expr.output_column
        cond_expr = self._emit_spark_expression(expr.predicate, preimages)
        true_lit = self._lit_repr(expr.true_value)
        false_lit = self._lit_repr(expr.false_value)

        policy = expr.unknown_policy
        lines: List[str] = []

        if policy == PredicateUnknownPolicy.TREAT_AS_FALSE:
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.when(({cond_expr}).isNotNull() & ({cond_expr}), {true_lit}).otherwise({false_lit}))"
            )
        elif policy == PredicateUnknownPolicy.TREAT_AS_TRUE:
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.when(({cond_expr}).isNull() | ({cond_expr}), {true_lit}).otherwise({false_lit}))"
            )
        elif policy == PredicateUnknownPolicy.PRESERVE_UNKNOWN:
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.when(({cond_expr}).isNull(), F.lit(None)).when({cond_expr}, {true_lit}).otherwise({false_lit}))"
            )
        elif policy == PredicateUnknownPolicy.FAIL_STEP:
            lines.append(
                f"if {df_var}.filter(({cond_expr}).isNull()).count() > 0: "
                f"raise ValueError('Predicate evaluated to unknown/NA under FAIL_STEP policy for {out_col}')"
            )
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.when(({cond_expr}).isNotNull() & ({cond_expr}), {true_lit}).otherwise({false_lit}))"
            )
        elif policy == PredicateUnknownPolicy.FLAG_UNKNOWN_SEPARATELY:
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(f'{out_col}_unknown')}, ({cond_expr}).isNull())"
            )
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.when(({cond_expr}).isNotNull() & ({cond_expr}), {true_lit}).otherwise({false_lit}))"
            )
        else:
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.when({cond_expr}, {true_lit}).otherwise({false_lit}))"
            )

        return lines

    def _emit_clip(
        self,
        expr: ClipExpression,
        preimages: Dict[str, str],
        df_var: str,
    ) -> List[str]:
        col = expr.target_column.column_name
        lines: List[str] = []

        # Check null policy
        if expr.null_policy == ClipNullPolicy.FAIL:
            lines.append(
                f"if {df_var}.filter(F.col({repr(col)}).isNull()).count() > 0: "
                f"raise ValueError('Encountered null in column {col} under FAIL null policy')"
            )

        # Check infinity policy
        is_pos_inf = f"(F.col({repr(col)}) == float('inf'))"
        is_neg_inf = f"(F.col({repr(col)}) == float('-inf'))"
        is_any_inf = f"({is_pos_inf} | {is_neg_inf})"

        if expr.infinity_policy == InfinityPolicy.FAIL_STEP:
            lines.append(
                f"if {df_var}.filter({is_any_inf}).count() > 0: "
                f"raise ValueError('Encountered infinity under FAIL_STEP for {col}')"
            )
        elif expr.infinity_policy == InfinityPolicy.FLAG:
            lines.append(f"{df_var} = {df_var}.withColumn({repr(f'{col}_is_inf')}, {is_any_inf})")
        elif expr.infinity_policy == InfinityPolicy.TREAT_AS_NULL:
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(col)}, F.when({is_any_inf}, F.lit(None)).otherwise(F.col({repr(col)})))"
            )
        elif expr.infinity_policy == InfinityPolicy.CLIP_TO_BOUND:
            if expr.upper_bound is not None:
                lines.append(
                    f"{df_var} = {df_var}.withColumn({repr(col)}, F.when({is_pos_inf}, F.lit({expr.upper_bound.value})).otherwise(F.col({repr(col)})))"
                )
            elif expr.unbounded_infinity_policy == UnboundedInfinityPolicy.FAIL_STEP:
                lines.append(
                    f"if {df_var}.filter({is_pos_inf}).count() > 0: "
                    f"raise ValueError('Encountered +inf without upper bound under FAIL_STEP for {col}')"
                )
            elif expr.unbounded_infinity_policy == UnboundedInfinityPolicy.TREAT_AS_NULL:
                lines.append(
                    f"{df_var} = {df_var}.withColumn({repr(col)}, F.when({is_pos_inf}, F.lit(None)).otherwise(F.col({repr(col)})))"
                )

            if expr.lower_bound is not None:
                lines.append(
                    f"{df_var} = {df_var}.withColumn({repr(col)}, F.when({is_neg_inf}, F.lit({expr.lower_bound.value})).otherwise(F.col({repr(col)})))"
                )
            elif expr.unbounded_infinity_policy == UnboundedInfinityPolicy.FAIL_STEP:
                lines.append(
                    f"if {df_var}.filter({is_neg_inf}).count() > 0: "
                    f"raise ValueError('Encountered -inf without lower bound under FAIL_STEP for {col}')"
                )
            elif expr.unbounded_infinity_policy == UnboundedInfinityPolicy.TREAT_AS_NULL:
                lines.append(
                    f"{df_var} = {df_var}.withColumn({repr(col)}, F.when({is_neg_inf}, F.lit(None)).otherwise(F.col({repr(col)})))"
                )

        # Standard double-sided clipping
        clip_expr = f"F.col({repr(col)})"
        if expr.lower_bound is not None and expr.upper_bound is not None:
            clip_expr = (
                f"F.when(F.col({repr(col)}) < {expr.lower_bound.value}, F.lit({expr.lower_bound.value}))"
                f".when(F.col({repr(col)}) > {expr.upper_bound.value}, F.lit({expr.upper_bound.value}))"
                f".otherwise(F.col({repr(col)}))"
            )
        elif expr.lower_bound is not None:
            clip_expr = (
                f"F.when(F.col({repr(col)}) < {expr.lower_bound.value}, F.lit({expr.lower_bound.value}))"
                f".otherwise(F.col({repr(col)}))"
            )
        elif expr.upper_bound is not None:
            clip_expr = (
                f"F.when(F.col({repr(col)}) > {expr.upper_bound.value}, F.lit({expr.upper_bound.value}))"
                f".otherwise(F.col({repr(col)}))"
            )

        lines.append(f"{df_var} = {df_var}.withColumn({repr(col)}, {clip_expr})")
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

        src_ref = f"F.col({repr(src_col)})"

        if op == ScalarMapOperation.TRIM:
            lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.trim({src_ref}.cast('string')))")
        elif op == ScalarMapOperation.LOWER:
            lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.lower({src_ref}.cast('string')))")
        elif op == ScalarMapOperation.UPPER:
            lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.upper({src_ref}.cast('string')))")
        elif op == ScalarMapOperation.CAST:
            target_t = expr.parameters.get("target_type", "string")
            spark_types = {
                "string": "string",
                "int": "long",
                "float": "double",
                "bool": "boolean",
                "decimal": "decimal(38,18)",
            }
            s_type = spark_types.get(target_t, "string")
            lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, {src_ref}.cast({repr(s_type)}))")
        elif op == ScalarMapOperation.PARSE_NUMBER:
            lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, {src_ref}.cast('double'))")
        elif op == ScalarMapOperation.PARSE_DATE:
            date_fmt = expr.parameters.get("format")
            if date_fmt:
                lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.to_date({src_ref}, {repr(date_fmt)}))")
            else:
                lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.to_date({src_ref}))")
        elif op == ScalarMapOperation.STANDARDIZE_BOOLEAN:
            true_toks = ["true", "1", "yes", "y", "t", "si"]
            false_toks = ["false", "0", "no", "n", "f"]
            cleaned = f"F.lower(F.trim({src_ref}.cast('string')))"
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.when({cleaned}.isin({repr(true_toks)}), F.lit(True))"
                f".when({cleaned}.isin({repr(false_toks)}), F.lit(False))"
                f".otherwise(F.lit(None)))"
            )
        elif op == ScalarMapOperation.REGEX_SUBSTITUTE:
            pattern = expr.parameters.get("pattern", "")
            replacement = expr.parameters.get("replacement", "")
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.regexp_replace({src_ref}.cast('string'), {repr(pattern)}, {repr(replacement)}))"
            )
        elif op == ScalarMapOperation.LOOKUP_MAP:
            mapping = expr.parameters.get("mapping", {})
            policy = expr.unmapped_policy
            if not mapping:
                if policy == UnmappedValuePolicy.SET_NULL:
                    lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.lit(None))")
                else:
                    lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, {src_ref})")
            else:
                when_chain = None
                for k, v in mapping.items():
                    cond = f"({src_ref} == {repr(k)})"
                    val_lit = self._lit_repr(v)
                    if when_chain is None:
                        when_chain = f"F.when({cond}, {val_lit})"
                    else:
                        when_chain += f".when({cond}, {val_lit})"

                if policy == UnmappedValuePolicy.SET_NULL:
                    when_chain += ".otherwise(F.lit(None))"
                elif policy == UnmappedValuePolicy.FAIL_STEP:
                    valid_keys = list(mapping.keys())
                    lines.append(
                        f"if {df_var}.filter({src_ref}.isNotNull() & (~{src_ref}.isin({repr(valid_keys)}))).count() > 0: "
                        f"raise ValueError('Unmapped value encountered in lookup map under FAIL_STEP policy')"
                    )
                    when_chain += f".otherwise({src_ref})"
                else:  # PASS_THROUGH
                    when_chain += f".otherwise({src_ref})"

                lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, {when_chain})")
        elif op == ScalarMapOperation.NORMALIZE_PHONE:
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.regexp_replace({src_ref}.cast('string'), r'[^0-9+]', ''))"
            )
        elif op == ScalarMapOperation.SANITIZE_EMAIL:
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.lower(F.trim({src_ref}.cast('string'))))"
            )
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
        cond_expr = self._emit_spark_expression(expr.condition, preimages)
        rep_expr = self._emit_spark_expression(expr.replace_value, preimages)
        orig_ref = f"F.col({repr(target_col)})"

        policy = expr.unknown_policy
        lines: List[str] = []

        if policy == PredicateUnknownPolicy.TREAT_AS_FALSE:
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.when(({cond_expr}).isNotNull() & ({cond_expr}), {rep_expr}).otherwise({orig_ref}))"
            )
        elif policy == PredicateUnknownPolicy.TREAT_AS_TRUE:
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.when(({cond_expr}).isNull() | ({cond_expr}), {rep_expr}).otherwise({orig_ref}))"
            )
        elif policy == PredicateUnknownPolicy.PRESERVE_UNKNOWN:
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.when(({cond_expr}).isNull(), {orig_ref}).when({cond_expr}, {rep_expr}).otherwise({orig_ref}))"
            )
        elif policy == PredicateUnknownPolicy.FAIL_STEP:
            lines.append(
                f"if {df_var}.filter(({cond_expr}).isNull()).count() > 0: "
                f"raise ValueError('Conditional replace condition evaluated to unknown/NA under FAIL_STEP policy for {out_col}')"
            )
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.when(({cond_expr}).isNotNull() & ({cond_expr}), {rep_expr}).otherwise({orig_ref}))"
            )
        elif policy == PredicateUnknownPolicy.FLAG_UNKNOWN_SEPARATELY:
            lines.append(f"{df_var} = {df_var}.withColumn({repr(f'{out_col}_unknown')}, ({cond_expr}).isNull())")
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.when(({cond_expr}).isNotNull() & ({cond_expr}), {rep_expr}).otherwise({orig_ref}))"
            )
        else:
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.when({cond_expr}, {rep_expr}).otherwise({orig_ref}))"
            )

        return lines

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
        if expr.emit_indicator:
            ind_col = expr.indicator_column or f"{col}_was_imputed"
            lines.append(f"{df_var} = {df_var}.withColumn({repr(ind_col)}, F.col({repr(col)}).isNull())")

        # 1. Constant Imputation
        if expr.strategy == "constant":
            val = expr.constant_value.value if hasattr(expr.constant_value, "value") else expr.constant_value
            if val is None and expr.learned_model:
                val = expr.learned_model.learned_value
            val_lit = self._lit_repr(val)
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.coalesce(F.col({repr(col)}), {val_lit}))"
            )

        # 2. Fitted Scalar Statistical Imputation (MEAN, MEDIAN, MODE)
        elif expr.strategy in ("mean", "median", "mode", StatisticalImputationStrategy.MEAN, StatisticalImputationStrategy.MEDIAN, StatisticalImputationStrategy.MODE):
            if expr.learned_model is None or expr.learned_model.learned_value is None:
                raise ValueError(f"Statistical imputation strategy '{expr.strategy}' requires an authorized learned_model with learned_value")
            # Snapshot hash verification at use time
            verify_imputation_model(expr.learned_model)
            learned_val = expr.learned_model.learned_value
            val_lit = self._lit_repr(learned_val)
            # Invariant: Binds fixed pre-computed constant; ZERO runtime aggregation/recalculation!
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.coalesce(F.col({repr(col)}), {val_lit}))"
            )

        # 3. Fitted Grouped Statistical Imputation (GROUPED_MEAN, GROUPED_MEDIAN)
        elif expr.strategy in ("grouped_mean", "grouped_median", GroupedImputationStrategy.GROUPED_MEAN, GroupedImputationStrategy.GROUPED_MEDIAN):
            if expr.learned_model is None:
                raise ValueError("Grouped imputation requires an authorized learned_model with group_values")
            verify_imputation_model(expr.learned_model)

            group_cols = list(expr.learned_model.group_columns or expr.group_by_columns)
            if not group_cols:
                raise ValueError("Grouped imputation requires at least one grouping column")

            missing_policy = expr.learned_model.missing_group_policy
            fallback_val = expr.learned_model.global_fallback_value

            # Build chained when condition matching group keys
            when_chain = None
            for gv in expr.learned_model.group_values:
                key_conds = []
                for k in gv.group_key:
                    k_val = k.value
                    key_conds.append(f"(F.col({repr(k.column)}) == {repr(k_val)})")
                combined_cond = " & ".join(key_conds)
                gv_lit = self._lit_repr(gv.learned_value)
                if when_chain is None:
                    when_chain = f"F.when({combined_cond}, {gv_lit})"
                else:
                    when_chain += f".when({combined_cond}, {gv_lit})"

            if when_chain is None:
                when_chain = "F.lit(None)"
            else:
                when_chain += ".otherwise(F.lit(None))"

            grp_col_name = f"_grp_impute_{clean_name}"
            lines.append(f"{df_var} = {df_var}.withColumn({repr(grp_col_name)}, {when_chain})")

            # Missing group policy
            if missing_policy == MissingGroupPolicy.USE_GLOBAL_FALLBACK:
                fb_lit = self._lit_repr(fallback_val)
                lines.append(
                    f"{df_var} = {df_var}.withColumn({repr(grp_col_name)}, F.coalesce(F.col({repr(grp_col_name)}), {fb_lit}))"
                )
                lines.append(
                    f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.coalesce(F.col({repr(col)}), F.col({repr(grp_col_name)})))"
                )
            elif missing_policy == MissingGroupPolicy.FAIL_STEP:
                lines.append(
                    f"if {df_var}.filter(F.col({repr(col)}).isNull() & F.col({repr(grp_col_name)}).isNull()).count() > 0: "
                    f"raise ValueError('Grouped imputation step failure [step_id={step_id}]: column={out_col} - Unseen group encountered under FAIL_STEP')"
                )
                lines.append(
                    f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.coalesce(F.col({repr(col)}), F.col({repr(grp_col_name)})))"
                )
            elif missing_policy == MissingGroupPolicy.FLAG:
                lines.append(
                    f"{df_var} = {df_var}.withColumn({repr(f'{out_col}_unseen_group')}, "
                    f"F.col({repr(col)}).isNull() & F.col({repr(grp_col_name)}).isNull())"
                )
                lines.append(
                    f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.coalesce(F.col({repr(col)}), F.col({repr(grp_col_name)})))"
                )
            else:  # KEEP_NULL
                lines.append(
                    f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.coalesce(F.col({repr(col)}), F.col({repr(grp_col_name)})))"
                )

            lines.append(f"{df_var} = {df_var}.drop({repr(grp_col_name)})")

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

        src_ref = f"F.col({repr(src_col)})"
        parsed_col = f"_parsed_{clean_name}"
        src_na_col = f"_src_na_{clean_name}"

        # Contract 6: Existing missing values are strictly distinguished from parse errors!
        lines.append(f"{df_var} = {df_var}.withColumn({repr(src_na_col)}, {src_ref}.isNull())")

        # A. Boolean Parsing
        if expr.boolean_spec or expr.target_data_type == DataType.BOOLEAN:
            bspec = expr.boolean_spec or BooleanFormatSpec()
            norm_true = [t.lower() if not bspec.case_sensitive else t for t in bspec.true_tokens]
            norm_false = [f.lower() if not bspec.case_sensitive else f for f in bspec.false_tokens]
            s_prep = f"F.trim({src_ref}.cast('string'))" if bspec.trim_whitespace else f"{src_ref}.cast('string')"
            if not bspec.case_sensitive:
                s_prep = f"F.lower({s_prep})"

            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(parsed_col)}, "
                f"F.when({s_prep}.isin({repr(norm_true)}), F.lit(True))"
                f".when({s_prep}.isin({repr(norm_false)}), F.lit(False))"
                f".otherwise(F.lit(None)))"
            )

        # B. Date / Timestamp Parsing
        elif expr.date_spec or expr.target_data_type in (DataType.DATE, DataType.TIMESTAMP):
            dspec = expr.date_spec or DateFormatSpec()
            fmt = dspec.accepted_formats[0] if dspec.accepted_formats else None
            if dspec.temporal_target == TemporalParseTarget.CALENDAR_DATE or dspec.output_type == "date":
                if fmt:
                    # Convert Python strftime format to Spark Java DateTimeFormatter if simple
                    spark_fmt = fmt.replace("%Y", "yyyy").replace("%m", "MM").replace("%d", "dd")
                    lines.append(
                        f"{df_var} = {df_var}.withColumn({repr(parsed_col)}, F.to_date({src_ref}, {repr(spark_fmt)}))"
                    )
                else:
                    lines.append(
                        f"{df_var} = {df_var}.withColumn({repr(parsed_col)}, F.to_date({src_ref}))"
                    )
            else:
                if fmt:
                    spark_fmt = fmt.replace("%Y", "yyyy").replace("%m", "MM").replace("%d", "dd").replace("%H", "HH").replace("%M", "mm").replace("%S", "ss")
                    lines.append(
                        f"{df_var} = {df_var}.withColumn({repr(parsed_col)}, F.to_timestamp({src_ref}, {repr(spark_fmt)}))"
                    )
                else:
                    lines.append(
                        f"{df_var} = {df_var}.withColumn({repr(parsed_col)}, F.to_timestamp({src_ref}))"
                    )

        # C. Numeric Parsing
        else:
            nspec = expr.numeric_spec or NumericFormatSpec()
            s_prep = f"{src_ref}.cast('string')"
            if nspec.trim_whitespace:
                s_prep = f"F.trim({s_prep})"
            if nspec.currency_symbols:
                for sym in nspec.currency_symbols:
                    s_prep = f"F.regexp_replace({s_prep}, {repr(re.escape(sym))}, '')"
            if nspec.thousands_separator:
                s_prep = f"F.regexp_replace({s_prep}, {repr(re.escape(nspec.thousands_separator))}, '')"
            if nspec.decimal_separator == ",":
                s_prep = f"F.regexp_replace({s_prep}, ',', '.')"
            if nspec.allow_parentheses_negative:
                s_prep = f"F.regexp_replace({s_prep}, r'^\\((.*)\\)$', r'-\\1')"

            clean_num_col = f"_clean_num_{clean_name}"
            lines.append(f"{df_var} = {df_var}.withColumn({repr(clean_num_col)}, {s_prep})")

            target_t = nspec.target_type
            s_type = "long" if target_t == "Int64" else "double"
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(parsed_col)}, F.col({repr(clean_num_col)}).cast({repr(s_type)}))"
            )

            if nspec.allow_percent and nspec.percent_scale == "fraction":
                lines.append(
                    f"{df_var} = {df_var}.withColumn({repr(parsed_col)}, "
                    f"F.when(F.col({repr(clean_num_col)}).endswith('%'), F.col({repr(parsed_col)}) / 100.0)"
                    f".otherwise(F.col({repr(parsed_col)})))"
                )

            lines.append(f"{df_var} = {df_var}.drop({repr(clean_num_col)})")

        # Contract 6: parse_error = (~source_missing) & parsed.isNull()
        parse_err_expr = f"(~F.col({repr(src_na_col)})) & F.col({repr(parsed_col)}).isNull()"

        # Contract 7 & 8: Failure Policies
        if expr.failure_policy == ParseFailurePolicy.SET_NULL:
            lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.col({repr(parsed_col)}))")
        elif expr.failure_policy == ParseFailurePolicy.FLAG:
            lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.col({repr(parsed_col)}))")
            lines.append(f"{df_var} = {df_var}.withColumn({repr(f'{out_col}_is_parse_error')}, {parse_err_expr})")
        elif expr.failure_policy == ParseFailurePolicy.FAIL_STEP:
            lines.append(
                f"if {df_var}.filter({parse_err_expr}).count() > 0: "
                f"raise ValueError(\"Parse failure encountered [step_id={step_id}]: column={out_col} under FAIL_STEP\")"
            )
            lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.col({repr(parsed_col)}))")
        elif expr.failure_policy == ParseFailurePolicy.KEEP_ORIGINAL:
            lines.append(
                f"{df_var} = {df_var}.withColumn({repr(out_col)}, "
                f"F.when({parse_err_expr}, {src_ref}).otherwise(F.col({repr(parsed_col)})))"
            )
        else:
            lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, F.col({repr(parsed_col)}))")

        # Clean up temporary columns
        lines.append(f"{df_var} = {df_var}.drop({repr(src_na_col)})")
        if out_col != parsed_col:
            lines.append(f"{df_var} = {df_var}.drop({repr(parsed_col)})")

        return lines

    def _emit_validate(
        self,
        expr: ValidateExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        cond_expr = self._emit_spark_expression(expr.predicate, preimages)
        clean_id = re.sub(r'[^a-zA-Z0-9_]', '_', step_id)
        reason = expr.reason_code or expr.rejection_reason_code or "VALIDATION_RULE_VIOLATED"

        reads = expr.predicate.extract_reads()
        if isinstance(expr.predicate, NullCheck):
            na_expr = f"({cond_expr}).isNull()"
        elif reads:
            na_parts = [f"F.col({repr(r)}).isNull()" for r in set(reads)]
            na_expr = f"({' | '.join(na_parts)}) | ({cond_expr}).isNull()"
        else:
            na_expr = f"({cond_expr}).isNull()"

        lines = [
            f"# Validate Step {step_id}",
            f"_val_na_{clean_id} = ({na_expr})",
            f"_val_pass_{clean_id} = (~_val_na_{clean_id}) & ({cond_expr})",
            f"_val_viol_{clean_id} = (~_val_na_{clean_id}) & (~({cond_expr}))",
            f"_val_unk_{clean_id} = _val_na_{clean_id}",
            f"_row_agg_{clean_id} = {df_var}.select(",
            f"    F.count(F.lit(1)).alias('cand_cnt'),",
            f"    F.sum(F.when(_val_pass_{clean_id}, 1).otherwise(0)).alias('pass_cnt'),",
            f"    F.sum(F.when(_val_viol_{clean_id}, 1).otherwise(0)).alias('viol_cnt'),",
            f"    F.sum(F.when(_val_unk_{clean_id}, 1).otherwise(0)).alias('unk_cnt'),",
            f").first()",
            f"_cand_{clean_id} = int((_row_agg_{clean_id}['cand_cnt'] or 0) if _row_agg_{clean_id} else 0)",
            f"_pass_{clean_id} = int((_row_agg_{clean_id}['pass_cnt'] or 0) if _row_agg_{clean_id} else 0)",
            f"_viol_{clean_id} = int((_row_agg_{clean_id}['viol_cnt'] or 0) if _row_agg_{clean_id} else 0)",
            f"_unk_{clean_id} = int((_row_agg_{clean_id}['unk_cnt'] or 0) if _row_agg_{clean_id} else 0)",
        ]

        if expr.unknown_policy == ValidationUnknownPolicy.COUNT_AS_VIOLATION:
            lines.append(f"_eff_viol_{clean_id} = _viol_{clean_id} + _unk_{clean_id}")
        elif expr.unknown_policy in (ValidationUnknownPolicy.COUNT_AS_PASS, ValidationUnknownPolicy.REPORT_SEPARATELY):
            lines.append(f"_eff_viol_{clean_id} = _viol_{clean_id}")
        elif expr.unknown_policy == ValidationUnknownPolicy.FAIL_STEP:
            lines.append(f"if _unk_{clean_id} > 0: raise ValueError('Validation condition evaluated to unknown under FAIL_STEP: ' + str(_unk_{clean_id}) + ' unknown rows [step_id={step_id}]')")
            lines.append(f"_eff_viol_{clean_id} = _viol_{clean_id}")

        lines.append(f"validation_summaries[{repr(step_id)}] = {{'step_id': {repr(step_id)}, 'candidate_count': _cand_{clean_id}, 'pass_count': _pass_{clean_id}, 'violation_count': _viol_{clean_id}, 'raw_unknown_count': _unk_{clean_id}, 'effective_violation_count': _eff_viol_{clean_id}, 'severity': {repr(expr.severity.value)}, 'reason_code': {repr(reason)}}}")

        if expr.severity == ValidateSeverity.ERROR:
            lines.append(f"if _eff_viol_{clean_id} > 0: raise ValueError('Validation assertion failed [step_id={step_id}]: ' + str(_eff_viol_{clean_id}) + ' violations out of ' + str(_cand_{clean_id}) + ' candidates (reason_code={reason})')")
            lines.append(f"validation_summaries[{repr(step_id)}]['gate_outcome'] = 'PASS'")
        else:
            lines.append(f"validation_summaries[{repr(step_id)}]['gate_outcome'] = 'WARN' if _eff_viol_{clean_id} > 0 else 'PASS'")

        return lines

    def _emit_quarantine(
        self,
        expr: QuarantineExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        cond_expr = self._emit_spark_expression(expr.predicate, preimages)
        clean_id = re.sub(r'[^a-zA-Z0-9_]', '_', step_id)
        reads = expr.predicate.extract_reads()
        if isinstance(expr.predicate, NullCheck):
            na_expr = f"({cond_expr}).isNull()"
        elif reads:
            na_parts = [f"F.col({repr(r)}).isNull()" for r in set(reads)]
            na_expr = f"({' | '.join(na_parts)}) | ({cond_expr}).isNull()"
        else:
            na_expr = f"({cond_expr}).isNull()"

        lines = [
            f"# Quarantine Step {step_id}",
            f"_quar_na_{clean_id} = ({na_expr})",
            f"_quar_cond_{clean_id} = ({cond_expr})",
        ]

        if expr.unknown_policy == QuarantineUnknownPolicy.FAIL_STEP:
            lines.append(f"_unk_cnt_{clean_id} = {df_var}.filter(_quar_na_{clean_id}).limit(1).count()")
            lines.append(f"if _unk_cnt_{clean_id} > 0: raise ValueError('Quarantine predicate evaluated to unknown under FAIL_STEP [step_id={step_id}]')")
            lines.append(f"_quar_mask_{clean_id} = (~_quar_na_{clean_id}) & _quar_cond_{clean_id}")
        elif expr.unknown_policy == QuarantineUnknownPolicy.ROUTE_TO_QUARANTINE:
            lines.append(f"_quar_mask_{clean_id} = _quar_na_{clean_id} | _quar_cond_{clean_id}")
        elif expr.unknown_policy == QuarantineUnknownPolicy.RETAIN_IN_CURATED:
            lines.append(f"_quar_mask_{clean_id} = (~_quar_na_{clean_id}) & _quar_cond_{clean_id}")
        elif expr.unknown_policy == QuarantineUnknownPolicy.SEPARATE_OUTPUT:
            lines.append(f"outputs[{repr(expr.unknown_output_ref)}] = {df_var}.filter(_quar_na_{clean_id})")
            lines.append(f"_quar_mask_{clean_id} = (~_quar_na_{clean_id}) & _quar_cond_{clean_id}")

        if expr.routing_mode == RoutingMode.PARTITION:
            lines.append(f"_input_cnt_{clean_id} = {df_var}.count()")
            if expr.unknown_policy == QuarantineUnknownPolicy.SEPARATE_OUTPUT:
                lines.append(f"_quar_rows_{clean_id} = {df_var}.filter(_quar_mask_{clean_id} & (~_quar_na_{clean_id}))")
                lines.append(f"_cur_rows_{clean_id} = {df_var}.filter((~_quar_mask_{clean_id}) & (~_quar_na_{clean_id}))")
            else:
                lines.append(f"_quar_rows_{clean_id} = {df_var}.filter(_quar_mask_{clean_id})")
                lines.append(f"_cur_rows_{clean_id} = {df_var}.filter(~_quar_mask_{clean_id})")

            if expr.reason_column:
                lines.append(f"_quar_rows_{clean_id} = _quar_rows_{clean_id}.withColumn({repr(expr.reason_column)}, F.lit({repr(expr.reason_code)}))")

            lines.append(f"outputs[{repr(expr.quarantine_output_ref)}] = _quar_rows_{clean_id}")
            lines.append(f"outputs[{repr(expr.curated_output_ref)}] = _cur_rows_{clean_id}")
            lines.append(f"{df_var} = _cur_rows_{clean_id}")

            # Distributed Multiset Reconciliation
            lines.append(f"# Multiset reconciliation for partition quarantine")
            lines.append(f"_cur_ids = outputs[{repr(expr.curated_output_ref)}].select(F.col(_identity_col).alias('_cid'))")
            lines.append(f"_quar_ids = outputs[{repr(expr.quarantine_output_ref)}].select(F.col(_identity_col).alias('_qid'))")
            lines.append(f"if _cur_ids.join(_quar_ids, _cur_ids['_cid'] == _quar_ids['_qid']).limit(1).count() > 0:")
            lines.append(f"    raise ValueError('Partition outputs contain overlapping occurrences between curated and quarantine')")
            if expr.unknown_policy == QuarantineUnknownPolicy.SEPARATE_OUTPUT:
                lines.append(f"_unk_ids = outputs[{repr(expr.unknown_output_ref)}].select(F.col(_identity_col).alias('_uid'))")
                lines.append(f"if _cur_ids.join(_unk_ids, _cur_ids['_cid'] == _unk_ids['_uid']).limit(1).count() > 0:")
                lines.append(f"    raise ValueError('Partition outputs contain overlapping occurrences between curated and unknown')")
                lines.append(f"if _quar_ids.join(_unk_ids, _quar_ids['_qid'] == _unk_ids['_uid']).limit(1).count() > 0:")
                lines.append(f"    raise ValueError('Partition outputs contain overlapping occurrences between quarantine and unknown')")
                lines.append(f"_tot_out_{clean_id} = outputs[{repr(expr.curated_output_ref)}].count() + outputs[{repr(expr.quarantine_output_ref)}].count() + outputs[{repr(expr.unknown_output_ref)}].count()")
            else:
                lines.append(f"_tot_out_{clean_id} = outputs[{repr(expr.curated_output_ref)}].count() + outputs[{repr(expr.quarantine_output_ref)}].count()")
            lines.append(f"if _tot_out_{clean_id} != _input_cnt_{clean_id}:")
            lines.append(f"    raise ValueError('Partition outputs do not reconcile to input occurrences')")

        elif expr.routing_mode == RoutingMode.COPY:
            lines.append(f"_input_cnt_{clean_id} = {df_var}.count()")
            lines.append(f"_quar_rows_{clean_id} = {df_var}.filter(_quar_mask_{clean_id})")
            if expr.reason_column:
                lines.append(f"_quar_rows_{clean_id} = _quar_rows_{clean_id}.withColumn({repr(expr.reason_column)}, F.lit({repr(expr.reason_code)}))")
            lines.append(f"outputs[{repr(expr.quarantine_output_ref)}] = _quar_rows_{clean_id}")
            lines.append(f"outputs[{repr(expr.curated_output_ref)}] = {df_var}")

            lines.append(f"if {df_var}.count() != _input_cnt_{clean_id}:")
            lines.append(f"    raise ValueError('Copy mode curated output does not match input occurrences')")
            lines.append(f"if _quar_rows_{clean_id}.count() > _input_cnt_{clean_id}:")
            lines.append(f"    raise ValueError('Copy mode quarantine occurrences exceed input occurrences')")
            lines.append(f"_q_grp = _quar_rows_{clean_id}.groupBy(_identity_col).count().withColumnRenamed('count', 'q_count')")
            lines.append(f"_in_grp = {df_var}.groupBy(_identity_col).count().withColumnRenamed('count', 'in_count')")
            lines.append(f"if _q_grp.join(_in_grp, _identity_col).filter(F.col('q_count') > F.col('in_count')).limit(1).count() > 0:")
            lines.append(f"    raise ValueError('Copy mode quarantine occurrences exceed input occurrences per row ID')")

        return lines

    def _emit_filter(
        self,
        expr: FilterExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        cond_expr = self._emit_spark_expression(expr.predicate, preimages)
        clean_id = re.sub(r'[^a-zA-Z0-9_]', '_', step_id)
        reads = expr.predicate.extract_reads()
        if isinstance(expr.predicate, NullCheck):
            na_expr = f"({cond_expr}).isNull()"
        elif reads:
            na_parts = [f"F.col({repr(r)}).isNull()" for r in set(reads)]
            na_expr = f"({' | '.join(na_parts)}) | ({cond_expr}).isNull()"
        else:
            na_expr = f"({cond_expr}).isNull()"

        lines = [
            f"# Filter Step {step_id}",
            f"_filt_na_{clean_id} = ({na_expr})",
            f"_filt_cond_{clean_id} = ({cond_expr})",
        ]

        if expr.unknown_policy == FilterUnknownPolicy.FAIL_STEP:
            lines.append(f"_unk_cnt_{clean_id} = {df_var}.filter(_filt_na_{clean_id}).limit(1).count()")
            lines.append(f"if _unk_cnt_{clean_id} > 0: raise ValueError('Filter predicate evaluated to unknown under FAIL_STEP [step_id={step_id}]')")
            lines.append(f"_filt_eff_{clean_id} = (~_filt_na_{clean_id}) & _filt_cond_{clean_id}")
        elif expr.unknown_policy == FilterUnknownPolicy.RETAIN:
            fill_v = f"(_filt_na_{clean_id} | _filt_cond_{clean_id})" if expr.retain_matching else f"((~_filt_na_{clean_id}) & _filt_cond_{clean_id})"
            lines.append(f"_filt_eff_{clean_id} = {fill_v}")
        elif expr.unknown_policy in (FilterUnknownPolicy.FILTER_OUT, FilterUnknownPolicy.ROUTE_TO_DROPPED_OUTPUT):
            fill_v = f"((~_filt_na_{clean_id}) & _filt_cond_{clean_id})" if expr.retain_matching else f"(_filt_na_{clean_id} | _filt_cond_{clean_id})"
            lines.append(f"_filt_eff_{clean_id} = {fill_v}")
        elif expr.unknown_policy == FilterUnknownPolicy.SEPARATE_OUTPUT:
            lines.append(f"outputs[{repr(expr.unknown_output_ref)}] = {df_var}.filter(_filt_na_{clean_id})")
            lines.append(f"_filt_eff_{clean_id} = (~_filt_na_{clean_id}) & _filt_cond_{clean_id}")

        if expr.retain_matching:
            lines.append(f"_retain_mask_{clean_id} = _filt_eff_{clean_id}")
        else:
            lines.append(f"_retain_mask_{clean_id} = ~_filt_eff_{clean_id}")

        if expr.unknown_policy == FilterUnknownPolicy.SEPARATE_OUTPUT:
            lines.append(f"_retain_mask_{clean_id} = _retain_mask_{clean_id} & (~_filt_na_{clean_id})")
            lines.append(f"_dropped_mask_{clean_id} = (~_retain_mask_{clean_id}) & (~_filt_na_{clean_id})")
        else:
            lines.append(f"_dropped_mask_{clean_id} = ~_retain_mask_{clean_id}")

        if expr.drop_policy == FilterDropPolicy.EXPORT_DROPPED:
            lines.append(f"_dropped_df_{clean_id} = {df_var}.filter(_dropped_mask_{clean_id})")
            if expr.rejection_reason_code:
                lines.append(f"_dropped_df_{clean_id} = _dropped_df_{clean_id}.withColumn('_dhara_filter_reason', F.lit({repr(expr.rejection_reason_code)}))")
            lines.append(f"outputs[{repr(expr.dropped_output_ref)}] = _dropped_df_{clean_id}")

        lines.append(f"{df_var} = {df_var}.filter(_retain_mask_{clean_id})")
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
        lines = [f"# Deduplicate Step {step_id}"]

        if expr.null_key_policy == NullKeyPolicy.FAIL_STEP:
            null_conds = [f"F.col({repr(k)}).isNull()" for k in keys]
            lines.append(f"if {df_var}.filter({' | '.join(null_conds)}).limit(1).count() > 0:")
            lines.append(f"    raise ValueError('Deduplication encountered null key under FAIL_STEP policy')")

        if expr.null_key_policy == NullKeyPolicy.TREAT_EACH_NULL_AS_DISTINCT:
            null_conds = [f"F.col({repr(k)}).isNull()" for k in keys]
            lines.append(f"_has_null_k_{clean_id} = ({' | '.join(null_conds)})")
            key_cols_expr = ", ".join(f"F.col({repr(k)}).cast('string')" for k in keys)
            lines.append(f"_synth_key_{clean_id} = F.when(_has_null_k_{clean_id}, F.concat(F.lit('NULL_DISTINCT_'), F.col(_identity_col).cast('string'))).otherwise(F.concat_ws(':::', {key_cols_expr}))")
            part_cols = [f"_synth_key_{clean_id}"]
        else:
            part_cols = [f"F.col({repr(k)})" for k in keys]

        order_col_exprs = []
        is_preserve_last = (expr.tie_breaker == DeduplicationTieBreaker.PRESERVE_LAST)
        if expr.order_by:
            for s in expr.order_by:
                col_ref = f"F.col({repr(s.column)})"
                if is_preserve_last:
                    if s.direction == OrderDirection.ASCENDING:
                        order_col_exprs.append(f"{col_ref}.desc_nulls_first()")
                    else:
                        order_col_exprs.append(f"{col_ref}.asc_nulls_last()")
                else:
                    if s.direction == OrderDirection.ASCENDING:
                        order_col_exprs.append(f"{col_ref}.asc_nulls_last()" if s.nulls_last else f"{col_ref}.asc_nulls_first()")
                    else:
                        order_col_exprs.append(f"{col_ref}.desc_nulls_last()" if s.nulls_last else f"{col_ref}.desc_nulls_first()")

        if expr.use_occurrence_id_as_final_tie_breaker:
            if is_preserve_last:
                order_col_exprs.append("F.col(_identity_col).desc()")
            else:
                order_col_exprs.append("F.col(_identity_col).asc()")
        elif not order_col_exprs:
            order_col_exprs.append("F.col(_identity_col).asc()")

        part_str = ", ".join(part_cols)
        ord_str = ", ".join(order_col_exprs)
        lines.append(f"_dedup_win_{clean_id} = Window.partitionBy({part_str}).orderBy({ord_str})")
        lines.append(f"_with_rn_{clean_id} = {df_var}.withColumn('_dhara_dedup_rn', F.row_number().over(_dedup_win_{clean_id}))")

        if expr.null_key_policy == NullKeyPolicy.EXCLUDE_NULL_KEY_ROWS:
            null_conds = [f"F.col({repr(k)}).isNull()" for k in keys]
            lines.append(f"_with_rn_{clean_id} = _with_rn_{clean_id}.withColumn('_is_null_k', ({' | '.join(null_conds)}))")
            dup_condition = "(F.col('_dhara_dedup_rn') > 1) | F.col('_is_null_k')"
            survivor_condition = "(F.col('_dhara_dedup_rn') == 1) & (~F.col('_is_null_k'))"
            drop_extra = ", '_is_null_k'"
        else:
            dup_condition = "F.col('_dhara_dedup_rn') > 1"
            survivor_condition = "F.col('_dhara_dedup_rn') == 1"
            drop_extra = ""

        if expr.duplicates_output_ref:
            if expr.duplicate_export_mode == DuplicateExportMode.REMOVED_OCCURRENCES_ONLY:
                lines.append(f"outputs[{repr(expr.duplicates_output_ref)}] = _with_rn_{clean_id}.filter({dup_condition}).drop('_dhara_dedup_rn'{drop_extra})")
            else:
                lines.append(f"_cnt_win_{clean_id} = Window.partitionBy({part_str})")
                lines.append(f"_with_cnt_{clean_id} = _with_rn_{clean_id}.withColumn('_part_cnt', F.count(F.lit(1)).over(_cnt_win_{clean_id}))")
                lines.append(f"outputs[{repr(expr.duplicates_output_ref)}] = _with_cnt_{clean_id}.filter(F.col('_part_cnt') > 1).drop('_dhara_dedup_rn', '_part_cnt'{drop_extra})")

        lines.append(f"{df_var} = _with_rn_{clean_id}.filter({survivor_condition}).drop('_dhara_dedup_rn'{drop_extra})")
        return lines

    def _emit_schema_mutation(
        self,
        expr: SchemaMutationExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        clean_id = re.sub(r'[^a-zA-Z0-9_]', '_', step_id)
        lines = [f"# Schema Mutation Step {step_id}"]

        if expr.expected_before_schema_hash:
            lines.append(f"_before_cols_{clean_id} = [(c, str({df_var}.schema[c].dataType), True) for c in {df_var}.columns if c != _identity_col]")
            lines.append(f"if compute_canonical_schema_hash(_before_cols_{clean_id}) != {repr(expr.expected_before_schema_hash)}:")
            lines.append(f"    raise ValueError('Actual pre-mutation schema hash does not match expected_before_schema_hash')")

        if expr.op == SchemaMutationOp.DROP_COLUMN:
            drop_cols = [d.column for d in expr.drops] if expr.drops else ([expr.target_column] if expr.target_column else [])
            for dc in drop_cols:
                lines.append(f"if {repr(dc)} == _identity_col:")
                lines.append(f"    raise ValueError('Cannot drop technical routing identity column: ' + {repr(dc)})")
                lines.append(f"if {repr(dc)} not in {df_var}.columns:")
                lines.append(f"    raise ValueError('Cannot drop non-existent column: ' + {repr(dc)})")
            lines.append(f"{df_var} = {df_var}.drop(*{repr(drop_cols)})")

        elif expr.op == SchemaMutationOp.RENAME_COLUMN:
            for r in expr.renames:
                old_c = r.old_name
                new_c = r.new_name
                lines.append(f"if {repr(old_c)} not in {df_var}.columns:")
                lines.append(f"    raise ValueError('Cannot rename non-existent column: ' + {repr(old_c)})")
                lines.append(f"if {repr(new_c)} in {df_var}.columns and {repr(new_c)} != {repr(old_c)}:")
                lines.append(f"    raise ValueError('Target rename column already exists: ' + {repr(new_c)})")
                lines.append(f"{df_var} = {df_var}.withColumnRenamed({repr(old_c)}, {repr(new_c)})")

        elif expr.op == SchemaMutationOp.PROJECT:
            if expr.projection:
                proj_cols = list(expr.projection.projected_columns)
                for pc in proj_cols:
                    if not expr.projection.allow_missing:
                        lines.append(f"if {repr(pc)} not in {df_var}.columns:")
                        lines.append(f"    raise ValueError('Projected column not found in DataFrame: ' + {repr(pc)})")
                lines.append(f"_proj_cols_{clean_id} = [c for c in {repr(proj_cols)} if c in {df_var}.columns]")
                lines.append(f"if _identity_col in {df_var}.columns and _identity_col not in _proj_cols_{clean_id}:")
                lines.append(f"    _proj_cols_{clean_id}.append(_identity_col)")
                lines.append(f"{df_var} = {df_var}.select(*_proj_cols_{clean_id})")

        elif expr.op == SchemaMutationOp.REORDER_COLUMNS:
            reorder_cols = list(expr.ordered_columns)
            for rc in reorder_cols:
                lines.append(f"if {repr(rc)} not in {df_var}.columns:")
                lines.append(f"    raise ValueError('Reorder column not found in DataFrame: ' + {repr(rc)})")
            lines.append(f"_ord_cols_{clean_id} = [c for c in {repr(reorder_cols)} if c in {df_var}.columns]")
            lines.append(f"if _identity_col in {df_var}.columns and _identity_col not in _ord_cols_{clean_id}:")
            lines.append(f"    _ord_cols_{clean_id}.append(_identity_col)")
            lines.append(f"{df_var} = {df_var}.select(*_ord_cols_{clean_id})")

        elif expr.op == SchemaMutationOp.CREATE_COLUMN:
            col_name = expr.create_column_name or expr.target_column or "new_col"
            if expr.existing_column_policy == ExistingColumnPolicy.ERROR:
                lines.append(f"if {repr(col_name)} in {df_var}.columns:")
                lines.append(f"    raise ValueError('Created column already exists: ' + {repr(col_name)})")
            if expr.create_expression:
                c_code = self._emit_spark_expression(expr.create_expression, preimages)
                lines.append(f"{df_var} = {df_var}.withColumn({repr(col_name)}, {c_code})")
            else:
                lines.append(f"{df_var} = {df_var}.withColumn({repr(col_name)}, F.lit(None).cast('string'))")

        elif expr.op == SchemaMutationOp.CAST_COLUMN:
            src = expr.source_column or expr.target_column
            dst = expr.target_column or src
            dtype_map = {
                DataType.STRING: "T.StringType()",
                DataType.INTEGER: "T.LongType()",
                DataType.FLOAT: "T.DoubleType()",
                DataType.BOOLEAN: "T.BooleanType()",
            }
            spark_type = dtype_map.get(expr.cast_target_type, "T.StringType()")
            lines.append(f"{df_var} = {df_var}.withColumn({repr(dst)}, F.col({repr(src)}).cast({spark_type}))")

        if expr.expected_after_schema_hash:
            lines.append(f"_after_cols_{clean_id} = [(c, str({df_var}.schema[c].dataType), True) for c in {df_var}.columns if c != _identity_col]")
            lines.append(f"if compute_canonical_schema_hash(_after_cols_{clean_id}) != {repr(expr.expected_after_schema_hash)}:")
            lines.append(f"    raise ValueError('Actual post-mutation schema hash does not match expected_after_schema_hash')")

        return lines

    def _emit_mask(
        self,
        expr: MaskExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        clean_id = re.sub(r'[^a-zA-Z0-9_]', '_', step_id)
        in_col = expr.source_column or (expr.input_column.column_name if expr.input_column else "")
        out_col = expr.target_column or expr.output_column or in_col
        mc = expr.mask_character
        p_count = expr.preserved_count
        lines = [f"# Mask Step {step_id}"]

        if out_col != in_col and expr.existing_column_policy == ExistingColumnPolicy.ERROR:
            lines.append(f"if {repr(out_col)} in {df_var}.columns:")
            lines.append(f"    raise ValueError('Target column already exists: ' + {repr(out_col)})")

        col_ref = f"F.col({repr(in_col)})"
        col_str = f"F.coalesce({col_ref}.cast('string'), F.lit(''))"
        len_col = f"F.length({col_str})"
        esc_mc = mc.replace("'", "''")
        sql_col = f"coalesce(cast(`{in_col}` as string), '')"
        sql_len = f"length({sql_col})"

        if expr.strategy in (MaskStrategy.PRESERVE_LAST_N, "preserve_last_n"):
            if expr.short_value_policy == ShortValuePolicy.SET_NULL:
                short_expr = "F.lit(None).cast('string')"
            elif expr.short_value_policy == ShortValuePolicy.KEEP_ORIGINAL:
                short_expr = col_str
            elif expr.short_value_policy == ShortValuePolicy.FAIL_STEP:
                lines.append(f"if {df_var}.filter({col_ref}.isNotNull() & ({len_col} <= {p_count})).limit(1).count() > 0:")
                lines.append(f"    raise ValueError('String length <= preserved_count under FAIL_STEP policy')")
                short_expr = f"F.expr(\"repeat('{esc_mc}', {sql_len})\")"
            else:
                short_expr = f"F.expr(\"repeat('{esc_mc}', {sql_len})\")"

            rest_expr = f"F.expr(\"concat(repeat('{esc_mc}', {sql_len} - {p_count}), substring({sql_col}, -{p_count}, {p_count}))\")"
            masked_expr = f"F.when({len_col} <= {p_count}, {short_expr}).otherwise({rest_expr})"

        elif expr.strategy in (MaskStrategy.PRESERVE_FIRST_N, "preserve_first_n"):
            if expr.short_value_policy == ShortValuePolicy.SET_NULL:
                short_expr = "F.lit(None).cast('string')"
            elif expr.short_value_policy == ShortValuePolicy.KEEP_ORIGINAL:
                short_expr = col_str
            elif expr.short_value_policy == ShortValuePolicy.FAIL_STEP:
                lines.append(f"if {df_var}.filter({col_ref}.isNotNull() & ({len_col} <= {p_count})).limit(1).count() > 0:")
                lines.append(f"    raise ValueError('String length <= preserved_count under FAIL_STEP policy')")
                short_expr = f"F.expr(\"repeat('{esc_mc}', {sql_len})\")"
            else:
                short_expr = f"F.expr(\"repeat('{esc_mc}', {sql_len})\")"

            rest_expr = f"F.expr(\"concat(substring({sql_col}, 1, {p_count}), repeat('{esc_mc}', {sql_len} - {p_count}))\")"
            masked_expr = f"F.when({len_col} <= {p_count}, {short_expr}).otherwise({rest_expr})"

        elif expr.strategy in (MaskStrategy.MASK_ALL, MaskStrategy.REDACT_ALL, "mask_all", "redact_all"):
            masked_expr = f"F.expr(\"repeat('{esc_mc}', {sql_len})\")"

        elif expr.strategy in (MaskStrategy.EMAIL_MASK, "email_mask"):
            user_sql = f"split({sql_col}, '@')[0]"
            dom_sql = f"split({sql_col}, '@')[1]"
            expr_user = (
                f"case "
                f"when length({user_sql}) <= 1 then {user_sql} "
                f"when length({user_sql}) == 2 then concat(substring({user_sql}, 1, 1), '{esc_mc}') "
                f"else concat(substring({user_sql}, 1, 1), repeat('{esc_mc}', length({user_sql}) - 2), substring({user_sql}, -1, 1)) "
                f"end"
            )
            if expr.email_domain_policy == EmailMaskDomainPolicy.PRESERVE:
                expr_dom = dom_sql
            elif expr.email_domain_policy == EmailMaskDomainPolicy.MASK:
                expr_dom = (
                    f"concat("
                    f"repeat('{esc_mc}', case when instr({dom_sql}, '.') > 0 then instr({dom_sql}, '.') - 1 else length({dom_sql}) end), "
                    f"case when instr({dom_sql}, '.') > 0 then substring({dom_sql}, instr({dom_sql}, '.'), length({dom_sql})) else '' end"
                    f")"
                )
            elif expr.email_domain_policy == EmailMaskDomainPolicy.HASH:
                expr_dom = f"substring(sha2({dom_sql}, 256), 1, 8)"
            else:
                expr_dom = dom_sql

            email_masked_sql = f"concat({expr_user}, '@', {expr_dom})"
            invalid_email_sql = f"repeat('{esc_mc}', {sql_len})"
            masked_expr = (
                f"F.expr(\"case when {sql_col} like '%@%' and size(split({sql_col}, '@')) == 2 "
                f"then {email_masked_sql} else {invalid_email_sql} end\")"
            )
        else:
            masked_expr = f"F.expr(\"repeat('{esc_mc}', {sql_len})\")"

        if expr.null_policy == MaskNullPolicy.PRESERVE_NULL:
            final_expr = f"F.when({col_ref}.isNull(), F.lit(None).cast('string')).otherwise({masked_expr})"
        elif expr.null_policy == MaskNullPolicy.SET_EMPTY:
            final_expr = f"F.when({col_ref}.isNull(), F.lit('')).otherwise({masked_expr})"
        elif expr.null_policy == MaskNullPolicy.FAIL_STEP:
            lines.append(f"if {df_var}.filter({col_ref}.isNull()).limit(1).count() > 0:")
            lines.append(f"    raise ValueError('Encountered null in {in_col} under FAIL_STEP mask null policy')")
            final_expr = masked_expr
        else:
            final_expr = f"F.when({col_ref}.isNull(), F.lit(None).cast('string')).otherwise({masked_expr})"

        lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, {final_expr})")
        return lines

    def _emit_hash(
        self,
        expr: HashExpression,
        preimages: Dict[str, str],
        df_var: str,
        step_id: str,
    ) -> List[str]:
        clean_id = re.sub(r'[^a-zA-Z0-9_]', '_', step_id)
        in_col = expr.source_column or (expr.input_column.column_name if expr.input_column else "")
        out_col = expr.target_column or expr.output_column or in_col
        lines = [f"# Hash Step {step_id}"]

        if out_col != in_col and expr.existing_column_policy == ExistingColumnPolicy.ERROR:
            lines.append(f"if {repr(out_col)} in {df_var}.columns:")
            lines.append(f"    raise ValueError('Target column already exists: ' + {repr(out_col)})")

        col_ref = f"F.col({repr(in_col)})"

        if expr.null_policy == HashNullPolicy.FAIL_STEP:
            lines.append(f"if {df_var}.filter({col_ref}.isNull()).limit(1).count() > 0:")
            lines.append(f"    raise ValueError('Encountered null in {in_col} under FAIL_STEP hash null policy')")

        schema_col = None
        for s in self.schemas.values():
            if in_col in s.columns:
                schema_col = s.columns[in_col]
                break

        col_dtype = schema_col.logical_type if schema_col else DataType.ANY

        if col_dtype == DataType.BOOLEAN:
            raw_bytes_expr = (
                f"F.when({col_ref}.isNull(), F.lit(b'\\x00DHARA_NULL_V1'))"
                f".when({col_ref} == True, F.lit(b'\\x01BOOL\\x01'))"
                f".otherwise(F.lit(b'\\x01BOOL\\x00'))"
            )
        elif col_dtype == DataType.INTEGER:
            raw_bytes_expr = (
                f"F.when({col_ref}.isNull(), F.lit(b'\\x00DHARA_NULL_V1'))"
                f".otherwise(F.concat(F.lit(b'\\x02INT\\x00'), F.unhex(F.lpad(F.hex({col_ref}.cast('long')), 16, '0'))))"
            )
        elif col_dtype == DataType.BINARY:
            raw_bytes_expr = (
                f"F.when({col_ref}.isNull(), F.lit(b'\\x00DHARA_NULL_V1'))"
                f".otherwise(F.concat(F.lit(b'\\x07BYTES\\x00'), F.unhex(F.lpad(F.hex(F.length({col_ref})), 8, '0')), {col_ref}))"
            )
        else:
            raw_bytes_expr = (
                f"F.when({col_ref}.isNull(), F.lit(b'\\x00DHARA_NULL_V1'))"
                f".otherwise(F.concat(F.lit(b'\\x05STR\\x00'), F.encode({col_ref}.cast('string'), 'utf-8')))"
            )

        if expr.mode == HashMode.SALTED_DIGEST:
            if expr.salt_value:
                salt_b = expr.salt_value.encode("utf-8")
                raw_bytes_expr = f"F.concat(F.lit({repr(salt_b)}), {raw_bytes_expr})"

        digest_expr = f"F.sha2({raw_bytes_expr}, 256)"
        if expr.truncate_length is not None:
            digest_expr = f"F.substring({digest_expr}, 1, {expr.truncate_length})"

        if expr.null_policy == HashNullPolicy.PRESERVE_NULL:
            final_digest = f"F.when({col_ref}.isNull(), F.lit(None).cast('string')).otherwise({digest_expr})"
        elif expr.null_policy == HashNullPolicy.HASH_NULL:
            final_digest = digest_expr
        else:
            final_digest = digest_expr

        lines.append(f"{df_var} = {df_var}.withColumn({repr(out_col)}, {final_digest})")
        return lines

    # ------------------------------------------------------------------------
    # Expression Transpiler
    # ------------------------------------------------------------------------

    def _emit_spark_expression(self, expr: Any, preimages: Dict[str, str]) -> str:
        if isinstance(expr, ColumnRef):
            if expr.version == ValueVersion.ORIGINAL:
                sym = preimages.get(expr.column_name)
                if sym:
                    return f"F.col({repr(sym)})"
                return f"F.col({repr(expr.column_name)})"
            return f"F.col({repr(expr.column_name)})"

        if isinstance(expr, LiteralValue):
            return self._lit_repr(expr.value)

        if isinstance(expr, BinaryOp):
            l = self._emit_spark_expression(expr.left, preimages)
            r = self._emit_spark_expression(expr.right, preimages)
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
            opnd = self._emit_spark_expression(expr.operand, preimages)
            if expr.is_null:
                return f"({opnd}).isNull()"
            return f"({opnd}).isNotNull()"

        if isinstance(expr, InSetPredicate):
            opnd = self._emit_spark_expression(expr.operand, preimages)
            vals = [v.value for v in expr.values]
            if expr.negated:
                return f"(~({opnd}).isin({repr(vals)}))"
            return f"({opnd}).isin({repr(vals)})"

        return "F.lit(True)"

    def _lit_repr(self, val: Any) -> str:
        if val is None:
            return "F.lit(None)"
        if isinstance(val, bool):
            return f"F.lit({val})"
        if isinstance(val, (int, float, str)):
            return f"F.lit({repr(val)})"
        if isinstance(val, Decimal):
            return f"F.lit({float(val)})"
        return f"F.lit({repr(str(val))})"

    # ------------------------------------------------------------------------
    # Subtype & Policy Hashes
    # ------------------------------------------------------------------------

    def _derive_expression_subtype(self, expr: Any) -> str:
        if isinstance(expr, FlagExpression):
            return "FLAG"
        if isinstance(expr, ClipExpression):
            return "CLIP"
        if isinstance(expr, ScalarMapExpression):
            return expr.operation.value.upper()
        if isinstance(expr, ConditionalReplaceExpression):
            return "CONDITIONAL_REPLACE"
        if isinstance(expr, ImputeExpression):
            strat = expr.strategy.value if hasattr(expr.strategy, "value") else str(expr.strategy)
            return strat.upper()
        if isinstance(expr, ParseExpression):
            if expr.boolean_spec or expr.target_data_type == DataType.BOOLEAN:
                return "PARSE_BOOLEAN"
            if expr.date_spec or expr.target_data_type in (DataType.DATE, DataType.TIMESTAMP):
                return "PARSE_DATE"
            return "PARSE_NUMBER"
        if isinstance(expr, ValidateExpression):
            return "VALIDATE"
        if isinstance(expr, QuarantineExpression):
            return "QUARANTINE"
        if isinstance(expr, FilterExpression):
            return "FILTER"
        if isinstance(expr, DeduplicateExpression):
            return "DEDUPLICATE"
        if isinstance(expr, SchemaMutationExpression):
            return expr.op.value.upper()
        if isinstance(expr, MaskExpression):
            strat = expr.strategy.value if hasattr(expr.strategy, "value") else str(expr.strategy)
            return strat.upper()
        if isinstance(expr, HashExpression):
            mode = expr.mode.value if hasattr(expr.mode, "value") else str(expr.mode)
            alg = expr.algorithm.value if hasattr(expr.algorithm, "value") else str(expr.algorithm)
            return f"{mode.upper()}_{alg.upper()}"
        if isinstance(expr, PassThroughExpression):
            return "PASS_THROUGH"
        return type(expr).__name__.upper()

    def _compute_policy_hash(self, expr: Any) -> str:
        policies = {}
        if hasattr(expr, "unknown_policy"):
            pol = expr.unknown_policy
            policies["unknown_policy"] = pol.value if hasattr(pol, "value") else str(pol)
        if hasattr(expr, "null_policy"):
            pol = expr.null_policy
            policies["null_policy"] = pol.value if hasattr(pol, "value") else str(pol)
        if hasattr(expr, "infinity_policy"):
            pol = expr.infinity_policy
            policies["infinity_policy"] = pol.value if hasattr(pol, "value") else str(pol)
        if hasattr(expr, "failure_policy"):
            pol = expr.failure_policy
            policies["failure_policy"] = pol.value if hasattr(pol, "value") else str(pol)
        if hasattr(expr, "unmapped_policy"):
            pol = expr.unmapped_policy
            policies["unmapped_policy"] = pol.value if hasattr(pol, "value") else str(pol)
        if hasattr(expr, "short_value_policy"):
            pol = expr.short_value_policy
            policies["short_value_policy"] = pol.value if hasattr(pol, "value") else str(pol)
        if hasattr(expr, "null_key_policy"):
            pol = expr.null_key_policy
            policies["null_key_policy"] = pol.value if hasattr(pol, "value") else str(pol)
        if hasattr(expr, "tie_breaker"):
            pol = expr.tie_breaker
            policies["tie_breaker"] = pol.value if hasattr(pol, "value") else str(pol)
        if hasattr(expr, "drop_policy"):
            pol = expr.drop_policy
            policies["drop_policy"] = pol.value if hasattr(pol, "value") else str(pol)
        if hasattr(expr, "collision_policy"):
            pol = expr.collision_policy
            policies["collision_policy"] = pol.value if hasattr(pol, "value") else str(pol)
        if hasattr(expr, "existing_column_policy"):
            pol = expr.existing_column_policy
            policies["existing_column_policy"] = pol.value if hasattr(pol, "value") else str(pol)
        if hasattr(expr, "cast_failure_policy"):
            pol = expr.cast_failure_policy
            policies["cast_failure_policy"] = pol.value if hasattr(pol, "value") else str(pol)
        if hasattr(expr, "routing_mode"):
            pol = expr.routing_mode
            policies["routing_mode"] = pol.value if hasattr(pol, "value") else str(pol)

        raw = json.dumps(policies, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

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

    def _validate_ast_safety(self, code: str) -> None:
        """
        Increment 3.1 AST safety validator.
        Prohibits:
        - Whole-DataFrame deep copies
        - Apply-time statistical recalculations (mean, median, mode, describe, quantile, agg)
        - Arbitrary code execution or reflection (eval, exec, getattr, setattr, __import__)
        - Operating system, subprocess, or network access
        """
        tree = ast.parse(code)

        prohibited_modules = {"os", "sys", "subprocess", "shutil", "socket", "http", "urllib", "pickle"}
        prohibited_builtins = {"eval", "exec", "__import__", "getattr", "setattr", "delattr", "compile", "globals", "locals"}
        prohibited_agg_methods = {"mean", "median", "mode", "std", "var", "quantile", "describe"}

        class SafetyVisitor(ast.NodeVisitor):
            def visit_Import(self, node: ast.Import) -> None:
                for alias in node.names:
                    root_mod = alias.name.split(".")[0]
                    if root_mod in prohibited_modules:
                        raise ValueError(f"Prohibited import: {alias.name}")
                self.generic_visit(node)

            def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
                if node.module:
                    root_mod = node.module.split(".")[0]
                    if root_mod in prohibited_modules:
                        raise ValueError(f"Prohibited from-import: {node.module}")
                self.generic_visit(node)

            def visit_Call(self, node: ast.Call) -> None:
                # Prohibit eval/exec
                if isinstance(node.func, ast.Name):
                    if node.func.id in prohibited_builtins:
                        raise ValueError(f"Prohibited call: {node.func.id}")
                elif isinstance(node.func, ast.Attribute):
                    if node.func.attr in prohibited_agg_methods:
                        raise ValueError(f"Prohibited statistical recalculation in PySpark apply code: {node.func.attr}")
                self.generic_visit(node)

        SafetyVisitor().visit(tree)


# ============================================================================
# 3. Capability Registration Helper for Increment 3.1
# ============================================================================

def register_pyspark_scalar_capabilities(
    registry: Optional[EngineCapabilityRegistry] = None,
    profile: Optional[SparkRuntimeProfile] = None,
    certification_commit: str = "436359501db7295042a39050cf0593c70d0d7d7d",
) -> None:
    """
    Registers certified capabilities for PySpark IR Compiler Increment 3.1
    in the EngineCapabilityRegistry.
    """
    reg = registry or get_global_capability_registry()
    prof = profile or SparkRuntimeProfile()
    rt_hash = prof.compute_hash()

    default_pol_hash = hashlib.sha256(b"{}").hexdigest()

    # Supported capabilities mapping
    verified_capabilities = [
        # MAP_VALUE
        (ActionFamily.MAP_VALUE, "TRIM"),
        (ActionFamily.MAP_VALUE, "LOWER"),
        (ActionFamily.MAP_VALUE, "UPPER"),
        (ActionFamily.MAP_VALUE, "CAST"),
        (ActionFamily.MAP_VALUE, "PARSE_NUMBER"),
        (ActionFamily.MAP_VALUE, "PARSE_DATE"),
        (ActionFamily.MAP_VALUE, "STANDARDIZE_BOOLEAN"),
        (ActionFamily.MAP_VALUE, "REGEX_SUBSTITUTE"),
        (ActionFamily.MAP_VALUE, "LOOKUP_MAP"),
        (ActionFamily.MAP_VALUE, "NORMALIZE_PHONE"),
        (ActionFamily.MAP_VALUE, "SANITIZE_EMAIL"),
        # CONDITIONAL_REPLACE
        (ActionFamily.CONDITIONAL_REPLACE, "CONDITIONAL_REPLACE"),
        # CLIP
        (ActionFamily.CLIP, "CLIP"),
        # FLAG
        (ActionFamily.FLAG, "FLAG"),
        # STANDARDIZE
        (ActionFamily.STANDARDIZE, "STANDARDIZE_BOOLEAN"),
        # PARSE
        (ActionFamily.PARSE, "PARSE_NUMBER"),
        (ActionFamily.PARSE, "PARSE_DATE"),
        (ActionFamily.PARSE, "PARSE_BOOLEAN"),
        # IMPUTE
        (ActionFamily.IMPUTE, "CONSTANT"),
        (ActionFamily.IMPUTE, "MEAN"),
        (ActionFamily.IMPUTE, "MEDIAN"),
        (ActionFamily.IMPUTE, "MODE"),
        (ActionFamily.IMPUTE, "GROUPED_MEAN"),
        (ActionFamily.IMPUTE, "GROUPED_MEDIAN"),
        # VALIDATE
        (ActionFamily.VALIDATE, "VALIDATE"),
        # QUARANTINE
        (ActionFamily.QUARANTINE, "QUARANTINE"),
        # FILTER
        (ActionFamily.FILTER, "FILTER"),
        # DEDUPLICATE
        (ActionFamily.DEDUPLICATE, "DEDUPLICATE"),
        # SCHEMA_MUTATION
        (ActionFamily.SCHEMA_MUTATION, "DROP_COLUMN"),
        (ActionFamily.SCHEMA_MUTATION, "RENAME_COLUMN"),
        (ActionFamily.SCHEMA_MUTATION, "PROJECT"),
        (ActionFamily.SCHEMA_MUTATION, "REORDER_COLUMNS"),
        (ActionFamily.SCHEMA_MUTATION, "CREATE_COLUMN"),
        (ActionFamily.SCHEMA_MUTATION, "CAST_COLUMN"),
        # MASK
        (ActionFamily.MASK, "PRESERVE_LAST_N"),
        (ActionFamily.MASK, "PRESERVE_FIRST_N"),
        (ActionFamily.MASK, "MASK_ALL"),
        (ActionFamily.MASK, "EMAIL_MASK"),
        # HASH
        (ActionFamily.HASH, "DIGEST_SHA256"),
        (ActionFamily.HASH, "SALTED_DIGEST_SHA256"),
    ]

    for family, subtype in verified_capabilities:
        for st in {subtype, subtype.lower(), subtype.upper()}:
            for ir_v in ("1.0.0", "v1.0"):
                cat_v = "v1.0" if ir_v == "v1.0" else "1.0.0"
                for pol_h in (default_pol_hash, "*"):
                    key = EngineCapabilityKey(
                        family=family,
                        expression_subtype=st,
                        policy_profile_hash=pol_h,
                        ir_version=ir_v,
                        catalogue_version=cat_v,
                        compiler_name=PySparkIRCompiler.COMPILER_NAME,
                        compiler_version=PySparkIRCompiler.COMPILER_VERSION,
                        engine_name=PySparkIRCompiler.ENGINE_NAME,
                        engine_version=PySparkIRCompiler.ENGINE_VERSION,
                        dialect=None,
                        runtime_profile_hash=rt_hash,
                    )
                    reg.register_capability(
                        key=key,
                        status=CapabilityStatus.VERIFIED,
                        reason_code="VERIFIED_BY_PYSPARK_PARITY",
                        limitations=(),
                        verified_test_ids=("tests/cross_engine/test_parity_pyspark_scalars.py",),
                        certification_commit=certification_commit,
                    )


# Automatically register on module import into global registry
register_pyspark_scalar_capabilities()
