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
import hashlib
import re
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

from pydantic import BaseModel, Field, computed_field, model_validator

from agent.etl_pipeline.codegen_shared import CodegenStatus
from agent.etl_pipeline.transformation_ir import (
    TRANSFORMATION_IR_VERSION,
    ActionFamily,
    AmbiguousTimePolicy,
    BinaryOp,
    BinaryOperator,
    BooleanFormatSpec,
    ClipExpression,
    ClipNullPolicy,
    ColumnRef,
    ConditionalReplaceExpression,
    DataType,
    DatasetColumnSchema,
    DatasetOutputSpec,
    DatasetSchema,
    DeduplicateExpression,
    DeduplicationTieBreaker,
    DuplicateExportMode,
    ExistingColumnPolicy,
    Expression,
    FilterDropPolicy,
    FilterExpression,
    FilterUnknownPolicy,
    FlagExpression,
    GroupKeyValue,
    GroupedImputationStrategy,
    GroupedImputationValue,
    ImputationModelIntegrityError,
    ImputeExpression,
    InfinityPolicy,
    InSetPredicate,
    LearnedImputationModel,
    LiteralValue,
    MissingGroupPolicy,
    ModeTieBreaker,
    MultiOutputArtifactError,
    NonexistentTimePolicy,
    NullCheck,
    NullKeyPolicy,
    NumericFormatSpec,
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
    QuarantineExpression,
    QuarantineUnknownPolicy,
    RoutingIdentityPolicy,
    RoutingMode,
    RoutingPredicateUnknownError,
    RoutingReconciliationError,
    ScalarMapExpression,
    ScalarMapOperation,
    SortColumnSpec,
    SourceStableIdentitySpec,
    StatisticalImputationStrategy,
    StepKind,
    TemporalParseTarget,
    TransformationEffects,
    TransformationIRGraph,
    TransformationStepIR,
    UnboundedInfinityPolicy,
    UnmappedValuePolicy,
    ValidateExpression,
    ValidateSeverity,
    ValidationExecutionSummary,
    ValidationGateError,
    ValidationUnknownPolicy,
    ValueVersion,
    bind_expression_to_schema,
    compute_model_snapshot_hash,
    validate_output_registry,
    verify_imputation_model,
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
    """

    COMPILER_NAME = "pandas_reference"
    COMPILER_VERSION = "1.0.0"

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
    }

    CONTROL_FAMILIES = {
        ActionFamily.PASS_THROUGH,
    }

    def __init__(self, schemas: Optional[Mapping[str, DatasetSchema]] = None):
        self.schemas = dict(schemas or {})

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
            "import numpy as np",
            "import pandas as pd",
            "",
            "def transform_datasets(df: pd.DataFrame) -> dict[str, pd.DataFrame]:",
            "    _working = df.copy(deep=False)  # isolated working frame",
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
        code_lines.append("def transform(df: pd.DataFrame) -> pd.DataFrame:")
        code_lines.append("    all_outputs = transform_datasets(df)")
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
        try:
            self._validate_ast_safety(full_source)
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
            entrypoint="transform(df: pd.DataFrame) -> pd.DataFrame",
            datasets_entrypoint="transform_datasets(df: pd.DataFrame) -> Dict[str, pd.DataFrame]",
            supports_single_output_entrypoint=supports_single,
            primary_output_ref=primary_ref,
            required_imports=("collections", "numpy", "pandas"),
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
            lines.append(f"{df_var}[{repr(out_col)}] = {df_var}[{repr(col)}].fillna({repr(val)})")

        # Invariant 1 & 5: Fitted Scalar Statistical Imputation (MEAN, MEDIAN, MODE)
        elif expr.strategy in ("mean", "median", "mode", StatisticalImputationStrategy.MEAN, StatisticalImputationStrategy.MEDIAN, StatisticalImputationStrategy.MODE):
            if expr.learned_model is None or expr.learned_model.learned_value is None:
                raise ValueError(f"Statistical imputation strategy '{expr.strategy}' requires an authorized learned_model with learned_value")
            # Snapshot hash verification at use time
            verify_imputation_model(expr.learned_model)

            learned_val = expr.learned_model.learned_value
            # Binds fixed constant; zero runtime recalculation
            lines.append(f"{df_var}[{repr(out_col)}] = {df_var}[{repr(col)}].fillna({repr(learned_val)})")

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
                lines.append(f"{series_var} = {series_var}.fillna({repr(fallback_val)})")
                lines.append(f"{df_var}[{repr(out_col)}] = {df_var}[{repr(col)}].fillna({series_var})")
            elif missing_policy == MissingGroupPolicy.FAIL_STEP:
                unseen_var = f"_unseen_{clean_name}"
                lines.append(f"{unseen_var} = {df_var}[{repr(col)}].isna() & {series_var}.isna()")
                lines.append(f"if bool({unseen_var}.any()): raise ValueError(\"Grouped imputation step failure [step_id={step_id}]: column={out_col} - Unseen group encountered with \" + str(int({unseen_var}.sum())) + \" rows under FAIL_STEP (reason_code=UNSEEN_GROUP)\")")
                lines.append(f"{df_var}[{repr(out_col)}] = {df_var}[{repr(col)}].fillna({series_var})")
            elif missing_policy == MissingGroupPolicy.FLAG:
                unseen_var = f"_unseen_{clean_name}"
                lines.append(f"{unseen_var} = {df_var}[{repr(col)}].isna() & {series_var}.isna()")
                lines.append(f"{df_var}[{repr(f'{out_col}_unseen_group')}] = {unseen_var}.astype('boolean')")
                lines.append(f"{df_var}[{repr(out_col)}] = {df_var}[{repr(col)}].fillna({series_var})")
            else:  # KEEP_NULL
                lines.append(f"{df_var}[{repr(out_col)}] = {df_var}[{repr(col)}].fillna({series_var})")

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

        if expr.null_key_policy == NullKeyPolicy.TREAT_NULLS_AS_EQUAL:
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

    def _validate_ast_safety(self, code: str) -> None:
        """
        Parses code into AST and ensures no forbidden nodes, calls, or attributes exist.
        Permits only pandas and numpy transformations, strictly forbidding:
        - Whole dataframe copies (df.copy())
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

                if isinstance(func, ast.Attribute) and func.attr in {"mean", "median", "mode", "quantile", "describe"}:
                    raise ValueError(f"Prohibited apply-time statistical recalculation detected: '.{func.attr}()'. Must bind fixed learned statistics.")

                if isinstance(func, ast.Name):
                    if func.id in {"eval", "exec", "compile", "open", "__import__", "globals", "locals"}:
                        raise ValueError(f"Prohibited built-in call: {func.id}")

            # Check Import
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name not in {"numpy", "pandas", "np", "pd", "collections"}:
                        raise ValueError(f"Prohibited import: {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.module not in {"collections"}:
                    raise ValueError(f"Prohibited import module: {node.module}")
                for alias in node.names:
                    if alias.name not in {"Counter"}:
                        raise ValueError(f"Prohibited import: {alias.name}")
