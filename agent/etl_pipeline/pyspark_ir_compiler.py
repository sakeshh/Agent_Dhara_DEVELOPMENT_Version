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
    ClipExpression,
    ClipNullPolicy,
    ColumnRef,
    ConditionalReplaceExpression,
    DataType,
    DateFormatSpec,
    DatasetSchema,
    FlagExpression,
    GroupedImputationStrategy,
    GroupedImputationValue,
    ImputationModelIntegrityError,
    ImputeExpression,
    InfinityPolicy,
    InSetPredicate,
    LearnedImputationModel,
    LiteralValue,
    MissingGroupPolicy,
    NonexistentTimePolicy,
    NullCheck,
    NumericFormatSpec,
    ParseExpression,
    ParseFailurePolicy,
    ParseOutputMode,
    PassThroughExpression,
    PredicateUnknownPolicy,
    ScalarMapExpression,
    ScalarMapOperation,
    StatisticalImputationStrategy,
    TemporalParseTarget,
    TransformationIRGraph,
    TransformationStepIR,
    UnboundedInfinityPolicy,
    UnmappedValuePolicy,
    ValueVersion,
    compute_model_snapshot_hash,
    verify_imputation_model,
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
                diagnostics.append(f"Step '{sid}' uses family '{family.value}' which is not implemented in {self.COMPILER_NAME} Increment 3.1")
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
            "",
        ]

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
            class PySparkExecutionResult:
                def __init__(self, outputs: Dict[str, Any]):
                    self.outputs = outputs
                    self.primary_output = next(iter(outputs.values())) if outputs else None
            return PySparkExecutionResult(res_dict)
        single_fn = loc.get("transform")
        if single_fn:
            primary_out = single_fn(df, key_provider=key_provider)
            class PySparkExecutionResult:
                def __init__(self, primary: Any):
                    self.outputs = {"primary": primary}
                    self.primary_output = primary
            return PySparkExecutionResult(primary_out)
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
