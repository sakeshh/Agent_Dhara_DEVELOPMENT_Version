"""
agent/etl_pipeline/azure_sql_ir_compiler.py

Authoritative Azure SQL Database T-SQL IR Compiler (Phase 3 Finalization).
Compiles typed TransformationStepIR into pure, unadulterated Azure SQL T-SQL artifacts.
Enforces strict capability preflight, original-vs-current value preservation across CTE stages,
apply-only static imputation validation, three-way validation accounting, granular catalogue
dispositions, and clean separation between compiler query artifacts and certification execution harnesses.
"""
from __future__ import annotations

import datetime
from decimal import Decimal
from enum import Enum
import hashlib
import json
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from pydantic import BaseModel, Field

from agent.etl_pipeline.cross_engine.capability_models import (
    CapabilityDecision,
    CapabilityStatus,
    EngineCapabilityKey,
    FailureCategory,
    NormalizedFailure,
)
from agent.etl_pipeline.cross_engine.capability_registry import (
    EngineCapabilityRegistry,
    get_global_capability_registry,
)
from agent.etl_pipeline.cross_engine.runtime_profiles import (
    AzureSqlDatabaseRuntimeProfile,
    compute_runtime_profile_hash,
)
from agent.etl_pipeline.transformation_ir import (
    TRANSFORMATION_IR_VERSION,
    ActionFamily,
    AggregateColumnSpec,
    AggregateExpression,
    AggregateOp,
    ArithmeticDerivation,
    ArithmeticOperator,
    BinaryOp,
    BinaryOperator,
    CastColumnExpression,
    CastFailurePolicy,
    ClipExpression,
    CoalesceDerivation,
    ColumnDropSpec,
    ColumnRef,
    ColumnRenameSpec,
    ConcatDerivation,
    ConcatNullPolicy,
    ConditionalDerivation,
    ConditionalExpression,
    ConditionalReplaceExpression,
    DataType,
    DateArithmeticDerivation,
    DateArithmeticUnit,
    DatePart,
    DatePartExtract,
    DeduplicateExpression,
    DelimiterExtract,
    DeriveExpression,
    DivisionByZeroPolicy,
    DropColumnsExpression,
    Expression,
    ExtractExpression,
    FilterExpression,
    FilterUnknownPolicy,
    FlagExpression,
    HashExpression,
    ImputationModelIntegrityError,
    ImputeExpression,
    InSetPredicate,
    JoinCardinality,
    JoinExpression,
    JoinKeyPair,
    JoinType,
    JsonPathExtract,
    LiteralValue,
    MaskExpression,
    MaskStrategy,
    NullCheck,
    ParseExpression,
    ParseFailurePolicy,
    PredicateUnknownPolicy,
    RegexExtract,
    RenameColumnsExpression,
    ScalarMapExpression,
    ScalarMapOperation,
    StepAuthorization,
    TransformationStepIR,
    UnaryOp,
    UnaryOperator,
    ValidateExpression,
    ValidationUnknownPolicy,
    ValueVersion,
    verify_imputation_model,
)


# ============================================================================
# 1. Identifier Quoting & Helpers
# ============================================================================

def quote_tsql_identifier(identifier: str) -> str:
    """
    Escapes a T-SQL identifier using bracket quoting [identifier].
    Any closing bracket ']' inside the name is escaped to ']]'.
    """
    clean = str(identifier or "").strip()
    if clean.startswith("[") and clean.endswith("]"):
        clean = clean[1:-1]
    escaped = clean.replace("]", "]]")
    return f"[{escaped}]"


def quote_tsql_literal(val: Any) -> str:
    """Formats a Python literal value into an exact T-SQL literal expression."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "CAST(1 AS BIT)" if val else "CAST(0 AS BIT)"
    if isinstance(val, (int, float, Decimal)):
        return str(val)
    if isinstance(val, (datetime.date, datetime.datetime)):
        return f"'{val.isoformat()}'"
    # String escaping: double single quotes
    s = str(val).replace("'", "''")
    return f"'{s}'"


# ============================================================================
# 2. Domain Exceptions & Distinct Reason Codes
# ============================================================================

class AzureSqlCompilationError(Exception):
    """Base error for Azure SQL compilation failures with normalized failure category."""
    def __init__(self, message: str, category: FailureCategory = FailureCategory.PARSE_FAILURE, reason_code: str = "SQL_ARTIFACT_SYNTAX_ERROR"):
        super().__init__(message)
        self.category = category
        self.reason_code = reason_code


class AzureSqlSchemaBindingError(AzureSqlCompilationError):
    def __init__(self, message: str, reason_code: str = "COLUMN_NOT_FOUND"):
        super().__init__(message, category=FailureCategory.SCHEMA_BINDING, reason_code=reason_code)


class AzureSqlUnsupportedCapabilityError(AzureSqlCompilationError):
    def __init__(self, message: str, reason_code: str = "UNSUPPORTED_AZURE_SQL_CAPABILITY"):
        super().__init__(message, category=FailureCategory.UNSUPPORTED_CAPABILITY, reason_code=reason_code)


class AzureSqlDataQualityError(AzureSqlCompilationError):
    def __init__(self, message: str, reason_code: str = "VALIDATION_GATE_FAILED"):
        super().__init__(message, category=FailureCategory.DATA_QUALITY, reason_code=reason_code)


# ============================================================================
# 3. Compilation Result & Certification Harness Artifact
# ============================================================================

class AzureSqlCompilationResult(BaseModel):
    """
    Immutable compilation result emitted by AzureSqlTsqlIRCompiler.
    Cleanly separates the pure transformation query from execution harness scripts.
    """
    query_sql: str
    validation_queries: List[str] = Field(default_factory=list)
    parameter_manifest: Dict[str, Any] = Field(default_factory=dict)
    input_columns: Tuple[str, ...] = ()
    output_columns: Tuple[str, ...] = ()
    runtime_profile: AzureSqlDatabaseRuntimeProfile
    runtime_profile_hash: str
    semantic_hash: str

    def generate_certification_harness(
        self,
        table_name: str = "#dhara_input",
        column_types: Optional[Dict[str, str]] = None,
        rows: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        """
        Generates an isolated, self-contained Azure SQL certification harness script.
        Enforces Mandatory Correction 6:
          - Mandatory session settings: NOCOUNT, XACT_ABORT, ANSI_NULLS, ANSI_WARNINGS, ARITHABORT.
          - Structured TRY...CATCH with rollback and explicit cleanup.
          - Clean separation between compiler artifact (query) and harness artifact.
        """
        lines = [
            "-- ============================================================================",
            "-- Azure SQL Database Certification Harness Artifact (Phase 3 Finalization)",
            f"-- Runtime Profile Hash: {self.runtime_profile_hash}",
            f"-- Semantic Hash:        {self.semantic_hash}",
            "-- ============================================================================",
            "SET NOCOUNT ON;",
            "SET XACT_ABORT ON;",
            "SET ANSI_NULLS ON;",
            "SET ANSI_WARNINGS ON;",
            "SET ARITHABORT ON;",
            "",
            "BEGIN TRY",
            "    BEGIN TRANSACTION;",
            "",
            f"    DROP TABLE IF EXISTS {table_name};",
            "",
        ]

        # Infer column types if not explicitly given
        cols = self.input_columns or tuple(column_types.keys()) if column_types else ()
        types = column_types or {}
        col_defs = []
        for c in cols:
            q_col = quote_tsql_identifier(c)
            t = types.get(c, "NVARCHAR(MAX)")
            col_defs.append(f"        {q_col} {t}")

        lines.append(f"    CREATE TABLE {table_name} (")
        lines.append(",\n".join(col_defs) if col_defs else "        [id] INT")
        lines.append("    );")
        lines.append("")

        # Insert fixture rows if provided
        if rows:
            lines.append(f"    -- Fixture Inserts ({len(rows)} rows)")
            for r in rows:
                col_names = [quote_tsql_identifier(k) for k in cols]
                val_exprs = [quote_tsql_literal(r.get(k)) for k in cols]
                lines.append(f"    INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({', '.join(val_exprs)});")
            lines.append("")

        lines.append("    -- Compiler Transformation Query Artifact")
        lines.append(self.query_sql)
        lines.append("    ;")
        lines.append("")

        # Append validation queries if any
        if self.validation_queries:
            lines.append("    -- Validation Gates")
            for vq in self.validation_queries:
                lines.append(vq)
                lines.append("    ;")
            lines.append("")

        lines.append("    -- Isolated Fixture Cleanup")
        lines.append(f"    DROP TABLE IF EXISTS {table_name};")
        lines.append("")
        lines.append("    ROLLBACK TRANSACTION;")
        lines.append("END TRY")
        lines.append("BEGIN CATCH")
        lines.append("    IF XACT_STATE() <> 0")
        lines.append("        ROLLBACK TRANSACTION;")
        lines.append(f"    DROP TABLE IF EXISTS {table_name};")
        lines.append("    THROW;")
        lines.append("END CATCH;")

        return "\n".join(lines)


# ============================================================================
# 4. Azure SQL T-SQL IR Compiler
# ============================================================================

class AzureSqlTsqlIRCompiler:
    """
    Authoritative IR Compiler targeting Azure SQL Database (Dialect: T-SQL).
    Translates typed TransformationStepIR models into executable T-SQL CTEs.
    """
    COMPILER_NAME: str = "AzureSqlTsqlIRCompiler"
    COMPILER_VERSION: str = "1.0.0"
    ENGINE_NAME: str = "azure_sql_database"
    DIALECT: str = "tsql"

    def __init__(
        self,
        runtime_profile: Optional[AzureSqlDatabaseRuntimeProfile] = None,
        capability_registry: Optional[EngineCapabilityRegistry] = None,
    ) -> None:
        self.runtime_profile = runtime_profile or AzureSqlDatabaseRuntimeProfile()
        self.runtime_profile_hash = self.runtime_profile.compute_hash()
        self.capability_registry = capability_registry or get_global_capability_registry()

    # ------------------------------------------------------------------------
    # Preflight Capability Check
    # ------------------------------------------------------------------------

    def check_capability(self, family: ActionFamily, expression_subtype: str) -> None:
        """
        Enforces default-to-blocked governance: evaluates capability key against the
        centralized registry. Rejects unregistered capabilities with UNSUPPORTED_AZURE_SQL_CAPABILITY.
        """
        key = EngineCapabilityKey(
            family=family,
            expression_subtype=expression_subtype,
            policy_profile_hash=hashlib.sha256(b"{}").hexdigest(),
            ir_version=TRANSFORMATION_IR_VERSION,
            catalogue_version="1.0.0",
            compiler_name=self.COMPILER_NAME,
            compiler_version=self.COMPILER_VERSION,
            engine_name=self.ENGINE_NAME,
            engine_version=self.runtime_profile.engine_version,
            dialect=self.DIALECT,
            runtime_profile_hash=self.runtime_profile_hash,
        )
        decision = self.capability_registry.evaluate_capability(key)
        if not decision.execution_eligible:
            raise AzureSqlUnsupportedCapabilityError(
                f"Capability ({family.value}, '{expression_subtype}') is not certified for {self.ENGINE_NAME} "
                f"(status={decision.status.value}, reason={decision.reason_code})",
                reason_code="UNSUPPORTED_AZURE_SQL_CAPABILITY",
            )

    # ------------------------------------------------------------------------
    # Expression Compilation Helpers
    # ------------------------------------------------------------------------

    def _compile_ast_expression(
        self,
        expr: Expression,
        current_stage: str,
        source_stage: str,
        schema_cols: Set[str],
        mutated_orig_cols: Dict[str, str],
    ) -> str:
        """
        Recursively translates a typed AST Expression to a T-SQL scalar expression.
        Preserves ORIGINAL vs CURRENT value semantics:
          - ORIGINAL: binds to source_stage or shadow [__orig_<col>]
          - CURRENT: binds to current_stage
        """
        if isinstance(expr, str):
            if expr not in schema_cols:
                raise AzureSqlSchemaBindingError(f"Column [{expr}] not found in schema", reason_code="COLUMN_NOT_FOUND")
            if expr in mutated_orig_cols:
                return f"{quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(mutated_orig_cols[expr])}"
            return f"{quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(expr)}"

        if hasattr(expr, "column_name") and not isinstance(expr, (BinaryOp, UnaryOp, NullCheck, InSetPredicate, ConditionalExpression)):
            col = expr.column_name
            ver = getattr(expr, "version", ValueVersion.CURRENT)
            if col not in schema_cols and ver != ValueVersion.ORIGINAL:
                raise AzureSqlSchemaBindingError(f"Column [{col}] not found in schema", reason_code="COLUMN_NOT_FOUND")

            if ver == ValueVersion.ORIGINAL:
                # If column was already mutated in an earlier step, reference its shadow column
                if col in mutated_orig_cols:
                    orig_ref = mutated_orig_cols[col]
                    return f"{quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(orig_ref)}"
                # Otherwise, reference directly from source_stage or current_stage
                return f"{quote_tsql_identifier(source_stage)}.{quote_tsql_identifier(col)}"
            else:
                return f"{quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(col)}"

        if isinstance(expr, LiteralValue):
            return quote_tsql_literal(expr.value)

        if isinstance(expr, BinaryOp):
            l_code = self._compile_ast_expression(expr.left, current_stage, source_stage, schema_cols, mutated_orig_cols)
            r_code = self._compile_ast_expression(expr.right, current_stage, source_stage, schema_cols, mutated_orig_cols)
            op_map = {
                BinaryOperator.EQ: "=",
                BinaryOperator.NEQ: "<>",
                BinaryOperator.LT: "<",
                BinaryOperator.LTE: "<=",
                BinaryOperator.GT: ">",
                BinaryOperator.GTE: ">=",
                BinaryOperator.AND: "AND",
                BinaryOperator.OR: "OR",
                BinaryOperator.ADD: "+",
                BinaryOperator.SUB: "-",
                BinaryOperator.MUL: "*",
                BinaryOperator.DIV: "/",
            }
            op_sym = op_map.get(expr.op, str(expr.op.value))
            return f"({l_code} {op_sym} {r_code})"

        if isinstance(expr, UnaryOp):
            sub_code = self._compile_ast_expression(expr.operand, current_stage, source_stage, schema_cols, mutated_orig_cols)
            if expr.op == UnaryOperator.NOT:
                return f"(NOT ({sub_code}))"
            return f"(- ({sub_code}))"

        if isinstance(expr, NullCheck):
            sub_code = self._compile_ast_expression(expr.operand, current_stage, source_stage, schema_cols, mutated_orig_cols)
            if expr.is_null:
                return f"({sub_code} IS NULL)"
            return f"({sub_code} IS NOT NULL)"

        if isinstance(expr, InSetPredicate):
            sub_code = self._compile_ast_expression(expr.operand, current_stage, source_stage, schema_cols, mutated_orig_cols)
            vals = [quote_tsql_literal(v.value) for v in expr.values]
            joined_vals = ", ".join(vals)
            if expr.negated:
                return f"({sub_code} NOT IN ({joined_vals}))"
            return f"({sub_code} IN ({joined_vals}))"

        if isinstance(expr, ConditionalExpression):
            cond_code = self._compile_ast_expression(expr.condition, current_stage, source_stage, schema_cols, mutated_orig_cols)
            then_code = self._compile_ast_expression(expr.then_branch, current_stage, source_stage, schema_cols, mutated_orig_cols)
            else_code = self._compile_ast_expression(expr.else_branch, current_stage, source_stage, schema_cols, mutated_orig_cols)
            return f"(CASE WHEN {cond_code} THEN {then_code} ELSE {else_code} END)"

        raise AzureSqlCompilationError(
            f"Unsupported AST expression type: {type(expr).__name__}",
            reason_code="SQL_ARTIFACT_SYNTAX_ERROR",
        )

    # ------------------------------------------------------------------------
    # Action Compilers
    # ------------------------------------------------------------------------

    def _compile_scalar_map(
        self,
        step: TransformationStepIR,
        expr: ScalarMapExpression,
        current_stage: str,
        source_stage: str,
        schema_cols: Set[str],
        mutated_orig_cols: Dict[str, str],
    ) -> Tuple[str, str]:
        """Compiles ActionFamily.MAP_VALUE (restricted to TRIM, UPPER, LOWER)."""
        op = expr.operation
        in_ref = self._compile_ast_expression(expr.source_column, current_stage, source_stage, schema_cols, mutated_orig_cols)
        out_col = expr.output_column or expr.source_column.column_name

        if op == ScalarMapOperation.TRIM:
            self.check_capability(ActionFamily.MAP_VALUE, "TRIM")
            sql_expr = f"TRIM({in_ref})"
        elif op == ScalarMapOperation.UPPER:
            self.check_capability(ActionFamily.MAP_VALUE, "UPPER")
            sql_expr = f"UPPER({in_ref})"
        elif op == ScalarMapOperation.LOWER:
            self.check_capability(ActionFamily.MAP_VALUE, "LOWER")
            sql_expr = f"LOWER({in_ref})"
        else:
            raise AzureSqlUnsupportedCapabilityError(
                f"Scalar map operation '{op}' is not in authoritative Slice 1 IR scope (strictly TRIM, UPPER, LOWER)",
                reason_code="UNSUPPORTED_AZURE_SQL_CAPABILITY",
            )

        return out_col, sql_expr

    def _compile_conditional_replace(
        self,
        step: TransformationStepIR,
        expr: ConditionalReplaceExpression,
        current_stage: str,
        source_stage: str,
        schema_cols: Set[str],
        mutated_orig_cols: Dict[str, str],
    ) -> Tuple[str, str]:
        """Compiles ActionFamily.CONDITIONAL_REPLACE preserving 3-valued boolean logic."""
        self.check_capability(ActionFamily.CONDITIONAL_REPLACE, "CONDITIONAL_REPLACE")
        in_col = expr.target_column.column_name
        out_col = expr.output_column or in_col

        in_ref = self._compile_ast_expression(expr.target_column, current_stage, source_stage, schema_cols, mutated_orig_cols)
        cond_code = self._compile_ast_expression(expr.condition, current_stage, source_stage, schema_cols, mutated_orig_cols)
        rep_code = self._compile_ast_expression(expr.replace_value, current_stage, source_stage, schema_cols, mutated_orig_cols)

        # 3-valued logic unknown policy handling
        if expr.unknown_policy == PredicateUnknownPolicy.TREAT_AS_TRUE:
            sql_expr = f"CASE WHEN {cond_code} OR ({cond_code} IS NULL) THEN {rep_code} ELSE {in_ref} END"
        elif expr.unknown_policy == PredicateUnknownPolicy.PRESERVE_UNKNOWN:
            unknown_val = "NULL" if expr.unknown_value is None else self._compile_ast_expression(expr.unknown_value, current_stage, source_stage, schema_cols, mutated_orig_cols)
            sql_expr = f"CASE WHEN {cond_code} THEN {rep_code} WHEN {cond_code} IS NULL THEN {unknown_val} ELSE {in_ref} END"
        else: # TREAT_AS_FALSE default
            sql_expr = f"CASE WHEN {cond_code} THEN {rep_code} ELSE {in_ref} END"

        return out_col, sql_expr

    def _compile_clip(
        self,
        step: TransformationStepIR,
        expr: ClipExpression,
        current_stage: str,
        source_stage: str,
        schema_cols: Set[str],
        mutated_orig_cols: Dict[str, str],
    ) -> Tuple[str, str]:
        """Compiles ActionFamily.CLIP (numerical clamping)."""
        self.check_capability(ActionFamily.CLIP, "CLIP")
        in_col = expr.target_column.column_name
        out_col = in_col
        in_ref = self._compile_ast_expression(expr.target_column, current_stage, source_stage, schema_cols, mutated_orig_cols)

        if expr.lower_bound is not None and expr.upper_bound is not None:
            l_val = quote_tsql_literal(expr.lower_bound.value)
            u_val = quote_tsql_literal(expr.upper_bound.value)
            sql_expr = f"CASE WHEN {in_ref} < {l_val} THEN {l_val} WHEN {in_ref} > {u_val} THEN {u_val} ELSE {in_ref} END"
        elif expr.lower_bound is not None:
            l_val = quote_tsql_literal(expr.lower_bound.value)
            sql_expr = f"CASE WHEN {in_ref} < {l_val} THEN {l_val} ELSE {in_ref} END"
        elif expr.upper_bound is not None:
            u_val = quote_tsql_literal(expr.upper_bound.value)
            sql_expr = f"CASE WHEN {in_ref} > {u_val} THEN {u_val} ELSE {in_ref} END"
        else:
            raise AzureSqlCompilationError("ClipExpression missing explicit bounds", reason_code="SQL_ARTIFACT_SYNTAX_ERROR")

        return out_col, sql_expr

    def _compile_flag(
        self,
        step: TransformationStepIR,
        expr: FlagExpression,
        current_stage: str,
        source_stage: str,
        schema_cols: Set[str],
        mutated_orig_cols: Dict[str, str],
    ) -> Tuple[str, str]:
        """Compiles ActionFamily.FLAG (audit companion flag)."""
        self.check_capability(ActionFamily.FLAG, "FLAG")
        out_col = expr.output_column
        cond_code = self._compile_ast_expression(expr.predicate, current_stage, source_stage, schema_cols, mutated_orig_cols)
        t_val = quote_tsql_literal(expr.true_value)
        f_val = quote_tsql_literal(expr.false_value)

        if expr.unknown_policy == PredicateUnknownPolicy.PRESERVE_UNKNOWN:
            u_val = quote_tsql_literal(expr.unknown_value)
            sql_expr = f"CASE WHEN {cond_code} THEN {t_val} WHEN {cond_code} IS NULL THEN {u_val} ELSE {f_val} END"
        elif expr.unknown_policy == PredicateUnknownPolicy.TREAT_AS_TRUE:
            sql_expr = f"CASE WHEN {cond_code} OR ({cond_code} IS NULL) THEN {t_val} ELSE {f_val} END"
        else: # TREAT_AS_FALSE
            sql_expr = f"CASE WHEN {cond_code} THEN {t_val} ELSE {f_val} END"

        return out_col, sql_expr

    def _compile_parse(
        self,
        step: TransformationStepIR,
        expr: ParseExpression,
        current_stage: str,
        source_stage: str,
        schema_cols: Set[str],
        mutated_orig_cols: Dict[str, str],
    ) -> List[Tuple[str, str]]:
        """
        Compiles ActionFamily.PARSE into safe TRY_CAST / TRY_CONVERT calls split by subtype.
        Supports PARSE_INTEGER, PARSE_BIGINT, PARSE_DECIMAL, PARSE_FLOAT, PARSE_CALENDAR_DATE, PARSE_DATETIME2.
        """
        t_type = expr.target_data_type
        in_col = expr.source_column.column_name
        out_col = expr.target_column or in_col
        in_ref = self._compile_ast_expression(expr.source_column, current_stage, source_stage, schema_cols, mutated_orig_cols)

        results = []

        if t_type == DataType.INTEGER:
            self.check_capability(ActionFamily.PARSE, "PARSE_INTEGER")
            parsed_expr = f"TRY_CAST({in_ref} AS INT)"
        elif t_type == DataType.DECIMAL:
            self.check_capability(ActionFamily.PARSE, "PARSE_DECIMAL")
            parsed_expr = f"TRY_CAST({in_ref} AS DECIMAL(18, 4))"
        elif t_type == DataType.FLOAT:
            self.check_capability(ActionFamily.PARSE, "PARSE_FLOAT")
            parsed_expr = f"TRY_CAST({in_ref} AS FLOAT)"
        elif t_type == DataType.DATE:
            self.check_capability(ActionFamily.PARSE, "PARSE_CALENDAR_DATE")
            # Explicit ISO 8601 style code 23 (yyyy-mm-dd)
            parsed_expr = f"TRY_CONVERT(DATE, {in_ref}, 23)"
        elif t_type == DataType.TIMESTAMP:
            self.check_capability(ActionFamily.PARSE, "PARSE_DATETIME2")
            # Explicit ISO 8601 style code 126 (yyyy-mm-ddThh:mi:ss.mmm)
            parsed_expr = f"TRY_CONVERT(DATETIME2, {in_ref}, 126)"
        else:
            raise AzureSqlUnsupportedCapabilityError(
                f"Parse target data type '{t_type}' is not supported in authoritative Slice 1 scope",
                reason_code="UNSUPPORTED_AZURE_SQL_CAPABILITY",
            )

        results.append((out_col, parsed_expr))

        # Companion error flag if policy is FLAG
        if expr.failure_policy == ParseFailurePolicy.FLAG:
            err_col = f"{out_col}_is_parse_error"
            flag_expr = f"CASE WHEN ({in_ref} IS NOT NULL) AND ({parsed_expr} IS NULL) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END"
            results.append((err_col, flag_expr))

        return results

    def _compile_impute(
        self,
        step: TransformationStepIR,
        expr: ImputeExpression,
        current_stage: str,
        source_stage: str,
        schema_cols: Set[str],
        mutated_orig_cols: Dict[str, str],
    ) -> List[Tuple[str, str]]:
        """
        Compiles ActionFamily.IMPUTE (apply-only static or pre-fitted statistical imputation).
        Statically rejects aggregate learning and in-query statistics computation.
        """
        self.check_capability(ActionFamily.IMPUTE, "IMPUTE")
        in_col = expr.target_column.column_name
        out_col = expr.output_column or in_col
        in_ref = self._compile_ast_expression(expr.target_column, current_stage, source_stage, schema_cols, mutated_orig_cols)

        # 1. Resolve apply-only replacement value
        if expr.strategy == "constant":
            if expr.constant_value is None and (expr.learned_model is None or expr.learned_model.learned_value is None):
                raise AzureSqlCompilationError("Constant imputation missing literal value", reason_code="SQL_ARTIFACT_SYNTAX_ERROR")
            val = expr.constant_value if expr.constant_value is not None else expr.learned_model.learned_value
        elif expr.strategy in ("mean", "median", "mode"):
            if expr.learned_model is None or expr.learned_model.learned_value is None:
                raise AzureSqlCompilationError(
                    f"Statistical imputation strategy '{expr.strategy}' requires pre-fitted learned_model in apply-only IR",
                    reason_code="DYNAMIC_FIT_PROHIBITED",
                )
            # Model snapshot hash integrity check via verify_imputation_model
            try:
                verify_imputation_model(expr.learned_model)
            except ImputationModelIntegrityError as e:
                raise AzureSqlCompilationError(str(e), reason_code="SNAPSHOT_HASH_MISMATCH")
            val = expr.learned_model.learned_value
        else:
            raise AzureSqlUnsupportedCapabilityError(
                f"Dynamic grouped/windowed imputation strategy '{expr.strategy}' prohibited in apply-only Slice 1",
                reason_code="DYNAMIC_FIT_PROHIBITED",
            )

        # 2. Static AST / safety validation (guarantee NO aggregate keywords)
        val_sql = quote_tsql_literal(val)
        prohibited_tokens = ["AVG(", "SUM(", "COUNT(", "MIN(", "MAX(", "PERCENTILE_CONT", "APPROX_PERCENTILE", "OVER (", "OVER("]
        for token in prohibited_tokens:
            if token in val_sql.upper():
                raise AzureSqlCompilationError(f"Aggregate token '{token}' prohibited in apply artifact", reason_code="DYNAMIC_FIT_PROHIBITED")

        imputed_sql = f"COALESCE({in_ref}, {val_sql})"
        results = [(out_col, imputed_sql)]

        if expr.emit_indicator and expr.indicator_column:
            ind_col = expr.indicator_column
            ind_sql = f"CASE WHEN ({in_ref} IS NULL) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END"
            results.append((ind_col, ind_sql))

        return results

    def _compile_validate(
        self,
        step: TransformationStepIR,
        expr: ValidateExpression,
        current_stage: str,
        source_stage: str,
        schema_cols: Set[str],
        mutated_orig_cols: Dict[str, str],
    ) -> str:
        """
        Compiles ActionFamily.VALIDATE into a three-way accounting verification query:
          candidate_count = pass_count + violation_count + raw_unknown_count.
        Guarantees three-valued logic without dropping UNKNOWN condition results.
        """
        self.check_capability(ActionFamily.VALIDATE, "VALIDATE")
        pred_code = self._compile_ast_expression(expr.predicate, current_stage, source_stage, schema_cols, mutated_orig_cols)
        st_name = quote_tsql_identifier(current_stage)

        val_query = (
            f"SELECT "
            f"COUNT_BIG(*) AS candidate_count, "
            f"SUM(CASE WHEN {pred_code} THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS pass_count, "
            f"SUM(CASE WHEN NOT {pred_code} THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS violation_count, "
            f"SUM(CASE WHEN {pred_code} IS NULL THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS raw_unknown_count "
            f"FROM {st_name}"
        )
        return val_query

    def _compile_derive(
        self,
        step: TransformationStepIR,
        expr: Any,
        current_stage: str,
        source_stage: str,
        schema_cols: Set[str],
        mutated_orig_cols: Dict[str, str],
    ) -> Tuple[str, str]:
        """
        Compiles ActionFamily.DERIVE (Arithmetic, Conditional, Coalesce, Concat, DateArithmetic).
        Enforces Mandatory Correction 4: DivisionByZeroPolicy.RETURN_INFINITY is rejected as UNSUPPORTED.
        """
        d = getattr(expr, "derivation", expr)
        out_col = getattr(expr, "output_column", None) or getattr(expr, "target_column", None) or getattr(step, "target_column", None) or "derived_col"
        out_col = str(out_col.column_name if hasattr(out_col, "column_name") else out_col)

        if isinstance(d, ArithmeticDerivation):
            self.check_capability(ActionFamily.DERIVE, "DERIVE_ARITHMETIC")
            # Mandatory Correction 4: RETURN_INFINITY qualification
            op_str = str(getattr(d.operator, "value", d.operator)).lower()
            if "div" in op_str:
                if d.division_by_zero_policy == DivisionByZeroPolicy.RETURN_INFINITY:
                    raise AzureSqlUnsupportedCapabilityError(
                        "DivisionByZeroPolicy.RETURN_INFINITY is unsupported on Azure SQL numeric types (Mandatory Correction 4)",
                        reason_code="UNSUPPORTED_AZURE_SQL_CAPABILITY",
                    )

            l_code = self._compile_ast_expression(d.left, current_stage, source_stage, schema_cols, mutated_orig_cols)
            r_code = self._compile_ast_expression(d.right, current_stage, source_stage, schema_cols, mutated_orig_cols)

            if d.operator in (ArithmeticOperator.ADD, "+"):
                return out_col, f"({l_code} + {r_code})"
            elif d.operator in (ArithmeticOperator.SUBTRACT, "-"):
                return out_col, f"({l_code} - {r_code})"
            elif d.operator in (ArithmeticOperator.MULTIPLY, "*"):
                return out_col, f"({l_code} * {r_code})"
            elif "div" in op_str:
                if d.division_by_zero_policy == DivisionByZeroPolicy.SET_NULL:
                    return out_col, f"CASE WHEN ({r_code}) = 0 THEN NULL ELSE ({l_code} / NULLIF({r_code}, 0)) END"
                elif d.division_by_zero_policy == DivisionByZeroPolicy.FAIL_STEP:
                    return out_col, f"CASE WHEN ({r_code}) = 0 THEN CAST('DIVIDE_BY_ZERO' AS INT) ELSE ({l_code} / {r_code}) END"
                else:
                    return out_col, f"({l_code} / {r_code})"

        elif isinstance(d, ConditionalDerivation):
            self.check_capability(ActionFamily.DERIVE, "DERIVE_CONDITIONAL")
            cond_code = self._compile_ast_expression(d.condition, current_stage, source_stage, schema_cols, mutated_orig_cols)
            then_code = self._compile_ast_expression(d.then_branch, current_stage, source_stage, schema_cols, mutated_orig_cols)
            else_code = self._compile_ast_expression(d.else_branch, current_stage, source_stage, schema_cols, mutated_orig_cols)
            return out_col, f"(CASE WHEN {cond_code} THEN {then_code} ELSE {else_code} END)"

        elif isinstance(d, CoalesceDerivation):
            self.check_capability(ActionFamily.DERIVE, "DERIVE_COALESCE")
            col_codes = [self._compile_ast_expression(c, current_stage, source_stage, schema_cols, mutated_orig_cols) for c in d.columns]
            return out_col, f"COALESCE({', '.join(col_codes)})"

        elif isinstance(d, ConcatDerivation):
            self.check_capability(ActionFamily.DERIVE, "DERIVE_CONCAT")
            col_codes = [f"CAST({self._compile_ast_expression(c, current_stage, source_stage, schema_cols, mutated_orig_cols)} AS NVARCHAR(MAX))" for c in d.columns]
            sep_lit = quote_tsql_literal(d.separator)
            return out_col, f"CONCAT_WS({sep_lit}, {', '.join(col_codes)})"

        elif isinstance(d, DateArithmeticDerivation):
            self.check_capability(ActionFamily.DERIVE, "DERIVE_DATE_ARITHMETIC")
            unit_map = {
                DateArithmeticUnit.DAY: "day",
                DateArithmeticUnit.MONTH: "month",
                DateArithmeticUnit.YEAR: "year",
                DateArithmeticUnit.HOUR: "hour",
                DateArithmeticUnit.MINUTE: "minute",
                DateArithmeticUnit.SECOND: "second",
            }
            u = unit_map.get(d.unit, "day")
            src_code = self._compile_ast_expression(d.source_column, current_stage, source_stage, schema_cols, mutated_orig_cols)
            amt_lit = quote_tsql_literal(d.amount)
            return out_col, f"DATEADD({u}, {amt_lit}, {src_code})"

        raise AzureSqlUnsupportedCapabilityError(
            f"Derivation type {type(d).__name__} is unsupported in Azure SQL compiler",
            reason_code="UNSUPPORTED_AZURE_SQL_CAPABILITY",
        )

    def _compile_extract(
        self,
        step: TransformationStepIR,
        expr: Any,
        current_stage: str,
        source_stage: str,
        schema_cols: Set[str],
        mutated_orig_cols: Dict[str, str],
    ) -> Tuple[str, str]:
        """
        Compiles ActionFamily.EXTRACT.
        Enforces Mandatory Correction 5: Regex extraction with capture groups remains UNSUPPORTED.
        """
        ext = getattr(expr, "extractor", expr)
        out_col = getattr(expr, "output_column", None) or getattr(expr, "target_column", None) or getattr(step, "target_column", None) or "extracted_col"
        out_col = str(out_col.column_name if hasattr(out_col, "column_name") else out_col)

        if isinstance(ext, RegexExtract):
            self.check_capability(ActionFamily.EXTRACT, "EXTRACT_REGEX")
            raise AzureSqlUnsupportedCapabilityError(
                "Regex extraction with capture groups/multiple-match governance is unsupported on Azure SQL (Mandatory Correction 5)",
                reason_code="UNSUPPORTED_AZURE_SQL_CAPABILITY",
            )

        elif isinstance(ext, DatePartExtract):
            self.check_capability(ActionFamily.EXTRACT, "EXTRACT_DATE_PART")
            src_code = self._compile_ast_expression(ext.source_column, current_stage, source_stage, schema_cols, mutated_orig_cols)
            if ext.part == DatePart.DAY_OF_WEEK:
                # ISO 8601 day-of-week normalized to Monday=0, Sunday=6 matching Pandas and PySpark contracts
                return out_col, f"((DATEPART(weekday, {src_code}) + 5) % 7)"
            part_map = {
                DatePart.YEAR: "year",
                DatePart.MONTH: "month",
                DatePart.DAY: "day",
                DatePart.HOUR: "hour",
                DatePart.MINUTE: "minute",
                DatePart.SECOND: "second",
            }
            p = part_map.get(ext.part, "day")
            return out_col, f"DATEPART({p}, {src_code})"

        elif isinstance(ext, DelimiterExtract):
            self.check_capability(ActionFamily.EXTRACT, "EXTRACT_DELIMITER")
            src_code = self._compile_ast_expression(ext.source_column, current_stage, source_stage, schema_cols, mutated_orig_cols)
            d_lit = quote_tsql_literal(ext.delimiter)
            return out_col, f"CASE WHEN CHARINDEX({d_lit}, {src_code}) > 0 THEN SUBSTRING({src_code}, 1, CHARINDEX({d_lit}, {src_code}) - 1) ELSE {src_code} END"

        elif isinstance(ext, JsonPathExtract):
            self.check_capability(ActionFamily.EXTRACT, "EXTRACT_JSON_PATH")
            src_code = self._compile_ast_expression(ext.source_column, current_stage, source_stage, schema_cols, mutated_orig_cols)
            raw_path = getattr(ext, "json_path", getattr(ext, "path", ""))
            path_str = raw_path.path if hasattr(raw_path, "path") else str(raw_path)
            path_lit = quote_tsql_literal(f"$.{path_str.lstrip('$.')}")
            return out_col, f"JSON_VALUE({src_code}, {path_lit})"

        raise AzureSqlUnsupportedCapabilityError(
            f"Extractor type {type(ext).__name__} is unsupported in Azure SQL compiler",
            reason_code="UNSUPPORTED_AZURE_SQL_CAPABILITY",
        )

    def _compile_mask(
        self,
        step: TransformationStepIR,
        expr: Any,
        current_stage: str,
        source_stage: str,
        schema_cols: Set[str],
        mutated_orig_cols: Dict[str, str],
    ) -> Tuple[str, str]:
        """Compiles ActionFamily.MASK."""
        src = getattr(expr, "source_column", getattr(expr, "target_column", None))
        src_name = src.column_name if hasattr(src, "column_name") else str(src)
        out_col = getattr(expr, "output_column", None) or getattr(expr, "target_column", None) or getattr(step, "target_column", None) or src_name
        out_col = str(out_col.column_name if hasattr(out_col, "column_name") else out_col)

        src_code = f"{quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(src_name)}"
        strat = getattr(expr, "strategy", None)
        strat_str = str(getattr(strat, "value", strat)).upper()

        if "FIXED" in strat_str or "MASK_ALL" in strat_str or "PRESERVE" in strat_str:
            self.check_capability(ActionFamily.MASK, "MASK_FIXED")
            m_val = quote_tsql_literal(getattr(expr, "fixed_mask", getattr(expr, "mask_character", "***")))
            return out_col, f"CASE WHEN {src_code} IS NULL THEN NULL ELSE {m_val} END"
        elif "EMAIL" in strat_str:
            self.check_capability(ActionFamily.MASK, "MASK_EMAIL")
            return out_col, (
                f"CASE WHEN {src_code} IS NULL THEN NULL "
                f"WHEN CHARINDEX('@', {src_code}) > 0 "
                f"THEN CONCAT('***@', RIGHT({src_code}, LEN({src_code}) - CHARINDEX('@', {src_code}))) "
                f"ELSE '***' END"
            )

        raise AzureSqlUnsupportedCapabilityError(
            f"Mask strategy {strat} unsupported on Azure SQL",
            reason_code="UNSUPPORTED_AZURE_SQL_CAPABILITY",
        )

    def _compile_hash(
        self,
        step: TransformationStepIR,
        expr: Any,
        current_stage: str,
        source_stage: str,
        schema_cols: Set[str],
        mutated_orig_cols: Dict[str, str],
    ) -> Tuple[str, str]:
        """
        Compiles ActionFamily.HASH with byte-exact canonical hashing.
        Enforces Mandatory Correction 3: String and Integer canonical V1 are certified with exact byte matching;
        Decimal and Timestamp are strictly blocked / NOT_TESTED in Phase 3.
        """
        src = getattr(expr, "source_column", getattr(expr, "target_column", None))
        src_name = src.column_name if hasattr(src, "column_name") else str(src)
        out_col = getattr(expr, "output_column", None) or getattr(expr, "target_column", None) or getattr(step, "target_column", None) or f"{src_name}_hash"
        out_col = str(out_col.column_name if hasattr(out_col, "column_name") else out_col)

        src_code = f"{quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(src_name)}"
        in_type = getattr(expr, "input_data_type", None)
        if in_type in (DataType.DECIMAL, DataType.TIMESTAMP, DataType.DATE) or (isinstance(in_type, str) and in_type.lower() in ("decimal", "timestamp", "date")):
            raise AzureSqlUnsupportedCapabilityError(
                f"Canonical byte hashing for {in_type} is not certified on Azure SQL in Phase 3 (Mandatory Correction 3)",
                reason_code="UNSUPPORTED_AZURE_SQL_CAPABILITY",
            )

        self.check_capability(ActionFamily.HASH, "CANONICAL_SCALAR_V1")
        if in_type == DataType.INTEGER or (isinstance(in_type, str) and in_type.lower() in ("integer", "int")) or "int" in src_name.lower():
            # 64-bit integer big-endian binary representation
            binary_expr = f"CONVERT(VARBINARY(8), CAST({src_code} AS BIGINT))"
        else:
            # UTF-8 string binary bytes
            binary_expr = f"CONVERT(VARBINARY(MAX), {src_code})"

        # Lowercase hex string from SHA2_256 hashbytes
        sql_expr = f"LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', {binary_expr}), 2))"
        return out_col, sql_expr

    # ------------------------------------------------------------------------
    # Plan Compilation & Deterministic CTE Construction
    # ------------------------------------------------------------------------

    def compile_plan(
        self,
        steps: Sequence[TransformationStepIR],
        input_columns: Sequence[str],
        source_table_name: str = "#dhara_input",
    ) -> AzureSqlCompilationResult:
        """
        Compiles an ordered list of TransformationStepIR into a deterministic CTE chain.
        Preserves ORIGINAL vs CURRENT pre-image column semantics through explicit projections.
        """
        if not input_columns:
            raise AzureSqlSchemaBindingError("input_columns sequence cannot be empty", reason_code="SCHEMA_EMPTY")

        source_stage = "source_stage"
        current_stage = source_stage
        current_schema = list(input_columns)
        mutated_orig_cols: Dict[str, str] = {} # maps mutated col -> shadow orig col name

        cte_blocks: List[str] = []
        validation_queries: List[str] = []

        # 1. Source stage CTE: explicit projection of all input columns
        source_proj = [f"    {quote_tsql_identifier(c)} AS {quote_tsql_identifier(c)}" for c in input_columns]
        cte_blocks.append(
            f"[{source_stage}] AS (\n"
            f"    SELECT\n" + ",\n".join(source_proj) + f"\n    FROM {source_table_name}\n)"
        )

        # 2. Sequential Transformation Step CTEs
        for idx, step in enumerate(steps, start=1):
            step_stage = f"step_{idx:03d}_{re.sub(r'[^a-zA-Z0-9_]', '_', step.step_id)}"
            expr = step.expression
            fam = step.family

            schema_set = set(current_schema)

            # Check for validation expression
            if fam == ActionFamily.VALIDATE and isinstance(expr, ValidateExpression):
                vq = self._compile_validate(step, expr, current_stage, source_stage, schema_set, mutated_orig_cols)
                validation_queries.append(vq)
                continue

            # Check for row-filtering step
            if fam == ActionFamily.FILTER and isinstance(expr, FilterExpression):
                self.check_capability(ActionFamily.FILTER, "FILTER")
                pred_sql = self._compile_ast_expression(expr.predicate, current_stage, source_stage, schema_set, mutated_orig_cols)
                filter_projs = [f"    {quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(c)} AS {quote_tsql_identifier(c)}" for c in current_schema]
                cte_blocks.append(
                    f"[{step_stage}] AS (\n"
                    f"    SELECT\n" + ",\n".join(filter_projs) + f"\n    FROM [{current_stage}]\n"
                    f"    WHERE ({pred_sql})\n)"
                )
                current_stage = step_stage
                continue

            # Check for deduplication step
            if fam == ActionFamily.DEDUPLICATE and isinstance(expr, DeduplicateExpression):
                self.check_capability(ActionFamily.DEDUPLICATE, "DEDUPLICATE")
                raw_keys = getattr(expr, "key_columns", getattr(expr, "keys", []))
                part_cols = [f"{quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(c)}" for c in raw_keys]
                raw_orders = getattr(expr, "order_by", getattr(expr, "order_columns", []))
                order_cols = []
                for o in raw_orders:
                    c_name = getattr(o, "column", getattr(o, "column_name", str(o)))
                    order_cols.append(f"{quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(c_name)}")
                if not order_cols:
                    order_cols = part_cols
                dedup_projs = [f"    {quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(c)} AS {quote_tsql_identifier(c)}" for c in current_schema]
                cte_blocks.append(
                    f"[{step_stage}_ranked] AS (\n"
                    f"    SELECT\n" + ",\n".join(dedup_projs) + f",\n"
                    f"        ROW_NUMBER() OVER (PARTITION BY {', '.join(part_cols)} ORDER BY {', '.join(order_cols)}) AS [__dhara_rn]\n"
                    f"    FROM [{current_stage}]\n),\n"
                    f"[{step_stage}] AS (\n"
                    f"    SELECT\n" + ",\n".join([f"    [{step_stage}_ranked].{quote_tsql_identifier(c)} AS {quote_tsql_identifier(c)}" for c in current_schema]) + f"\n    FROM [{step_stage}_ranked]\n"
                    f"    WHERE [__dhara_rn] = 1\n)"
                )
                current_stage = step_stage
                continue

            # Check for join step
            if fam == ActionFamily.JOIN:
                join_type = getattr(expr, "join_type", JoinType.INNER)
                if join_type == JoinType.CROSS:
                    auth = getattr(step, "authorization", getattr(step, "step_authorization", None))
                    if not auth:
                        raise AzureSqlCompilationError("Cross join requires StepAuthorization", reason_code="AUTHORIZATION_REQUIRED")
                    self.check_capability(ActionFamily.JOIN, "JOIN_CROSS")
                else:
                    self.check_capability(ActionFamily.JOIN, f"JOIN_{join_type.value.upper()}")

                # Builds join CTE referencing right dataset
                right_table = getattr(expr, "right_dataset", getattr(expr, "right_dataset_name", "#dhara_right"))
                join_conds = []
                key_pairs = getattr(expr, "key_pairs", [])
                for pair in key_pairs:
                    l_col = getattr(pair, "left", getattr(pair, "left_column", None))
                    r_col = getattr(pair, "right", getattr(pair, "right_column", None))
                    l_name = l_col.column_name if hasattr(l_col, "column_name") else str(l_col)
                    r_name = r_col.column_name if hasattr(r_col, "column_name") else str(r_col)
                    l_key = f"[{current_stage}].{quote_tsql_identifier(l_name)}"
                    r_key = f"[r_join].{quote_tsql_identifier(r_name)}"
                    join_conds.append(f"{l_key} = {r_key}")
                on_clause = " AND ".join(join_conds) if join_conds else "1 = 1"
                join_kw = "CROSS JOIN" if join_type == JoinType.CROSS else ("INNER JOIN" if join_type == JoinType.INNER else f"{join_type.value.upper()} JOIN")
                on_str = "" if join_type == JoinType.CROSS else f" ON {on_clause}"
                join_projs = [f"    [{current_stage}].{quote_tsql_identifier(c)} AS {quote_tsql_identifier(c)}" for c in current_schema]

                cte_blocks.append(
                    f"[{step_stage}] AS (\n"
                    f"    SELECT\n" + ",\n".join(join_projs) + f"\n    FROM [{current_stage}]\n"
                    f"    {join_kw} {right_table} AS [r_join]{on_str}\n)"
                )
                current_stage = step_stage
                continue

            # Check for aggregate step
            if fam == ActionFamily.AGGREGATE:
                self.check_capability(ActionFamily.AGGREGATE, "AGGREGATE")
                group_by_cols = getattr(expr, "group_by", getattr(expr, "group_keys", []))
                aggregates_list = getattr(expr, "aggregates", getattr(expr, "aggregations", []))
                group_cols = [f"{quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(c)}" for c in group_by_cols]
                agg_projs = [f"    {quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(c)} AS {quote_tsql_identifier(c)}" for c in group_by_cols]
                out_cols_agg = list(group_by_cols)

                for col_spec in aggregates_list:
                    out_name = quote_tsql_identifier(col_spec.output_column)
                    in_col = getattr(col_spec, "source_column", None)
                    op = getattr(col_spec, "operation", getattr(col_spec, "op", None))
                    if in_col is None:
                        in_expr = "*"
                    else:
                        in_expr = f"{quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(in_col)}"

                    if op == AggregateOp.COUNT:
                        agg_projs.append(f"    COUNT({in_expr}) AS {out_name}")
                    elif op == AggregateOp.COUNT_DISTINCT:
                        agg_projs.append(f"    COUNT(DISTINCT {in_expr}) AS {out_name}")
                    elif op == AggregateOp.SUM:
                        agg_projs.append(f"    SUM({in_expr}) AS {out_name}")
                    elif op == AggregateOp.AVG:
                        agg_projs.append(f"    AVG({in_expr}) AS {out_name}")
                    elif op == AggregateOp.MIN:
                        agg_projs.append(f"    MIN({in_expr}) AS {out_name}")
                    elif op == AggregateOp.MAX:
                        agg_projs.append(f"    MAX({in_expr}) AS {out_name}")
                    out_cols_agg.append(col_spec.output_column)

                group_clause = f"\n    GROUP BY {', '.join(group_cols)}" if group_cols else ""
                cte_blocks.append(
                    f"[{step_stage}] AS (\n"
                    f"    SELECT\n" + ",\n".join(agg_projs) + f"\n    FROM [{current_stage}]{group_clause}\n)"
                )
                current_schema = out_cols_agg
                current_stage = step_stage
                continue

            # Check for drop columns
            if fam == ActionFamily.SCHEMA_MUTATION and isinstance(expr, DropColumnsExpression):
                self.check_capability(ActionFamily.SCHEMA_MUTATION, "DROP_COLUMN")
                protected = {"_tenant_id", "row_occurrence_id", "__dhara_row_occurrence_0"}
                drops_list = getattr(expr, "drops", getattr(expr, "columns_to_drop", []))
                drop_names = [getattr(d, "column", getattr(d, "column_name", str(d))) for d in drops_list]
                for drop_col in drop_names:
                    if drop_col in protected:
                        raise AzureSqlCompilationError(
                            f"Cannot drop protected technical column '{drop_col}'",
                            reason_code="PROTECTED_COLUMN_DROP_REJECTED",
                        )
                current_schema = [c for c in current_schema if c not in drop_names]
                drop_projs = [f"    {quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(c)} AS {quote_tsql_identifier(c)}" for c in current_schema]
                cte_blocks.append(
                    f"[{step_stage}] AS (\n"
                    f"    SELECT\n" + ",\n".join(drop_projs) + f"\n    FROM [{current_stage}]\n)"
                )
                current_stage = step_stage
                continue

            # Produce transformation expression(s) for scalar mutations
            mutations: List[Tuple[str, str]] = []
            if fam == ActionFamily.MAP_VALUE and isinstance(expr, ScalarMapExpression):
                mutations.append(self._compile_scalar_map(step, expr, current_stage, source_stage, schema_set, mutated_orig_cols))
            elif fam == ActionFamily.CONDITIONAL_REPLACE and isinstance(expr, ConditionalReplaceExpression):
                mutations.append(self._compile_conditional_replace(step, expr, current_stage, source_stage, schema_set, mutated_orig_cols))
            elif fam == ActionFamily.CLIP and isinstance(expr, ClipExpression):
                mutations.append(self._compile_clip(step, expr, current_stage, source_stage, schema_set, mutated_orig_cols))
            elif fam == ActionFamily.FLAG and isinstance(expr, FlagExpression):
                mutations.append(self._compile_flag(step, expr, current_stage, source_stage, schema_set, mutated_orig_cols))
            elif fam == ActionFamily.PARSE and isinstance(expr, ParseExpression):
                mutations.extend(self._compile_parse(step, expr, current_stage, source_stage, schema_set, mutated_orig_cols))
            elif fam == ActionFamily.IMPUTE and isinstance(expr, ImputeExpression):
                mutations.extend(self._compile_impute(step, expr, current_stage, source_stage, schema_set, mutated_orig_cols))
            elif fam == ActionFamily.DERIVE:
                mutations.append(self._compile_derive(step, expr, current_stage, source_stage, schema_set, mutated_orig_cols))
            elif fam == ActionFamily.EXTRACT:
                mutations.append(self._compile_extract(step, expr, current_stage, source_stage, schema_set, mutated_orig_cols))
            elif fam == ActionFamily.MASK:
                mutations.append(self._compile_mask(step, expr, current_stage, source_stage, schema_set, mutated_orig_cols))
            elif fam == ActionFamily.HASH:
                mutations.append(self._compile_hash(step, expr, current_stage, source_stage, schema_set, mutated_orig_cols))
            elif fam == ActionFamily.SCHEMA_MUTATION and isinstance(expr, RenameColumnsExpression):
                self.check_capability(ActionFamily.SCHEMA_MUTATION, "RENAME_COLUMN")
                # Rename columns updates current schema mapping
                rename_map = {spec.source_column: spec.target_column for spec in expr.renames}
                renamed_projs = []
                new_schema = []
                for c in current_schema:
                    target_name = rename_map.get(c, c)
                    renamed_projs.append(f"    {quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(c)} AS {quote_tsql_identifier(target_name)}")
                    new_schema.append(target_name)
                cte_blocks.append(
                    f"[{step_stage}] AS (\n"
                    f"    SELECT\n" + ",\n".join(renamed_projs) + f"\n    FROM [{current_stage}]\n)"
                )
                current_schema = new_schema
                current_stage = step_stage
                continue
            elif fam == ActionFamily.SCHEMA_MUTATION and isinstance(expr, CastColumnExpression):
                self.check_capability(ActionFamily.SCHEMA_MUTATION, "CAST_COLUMN")
                in_ref = self._compile_ast_expression(ColumnRef(column_name=expr.source_column), current_stage, source_stage, schema_set, mutated_orig_cols)
                out_col = expr.target_column or expr.source_column
                mutations.append((out_col, f"TRY_CAST({in_ref} AS {expr.target_data_type.value.upper()})"))
            else:
                # Default rejection for all other or unrecognized families
                raise AzureSqlUnsupportedCapabilityError(
                    f"ActionFamily '{fam.value}' is not supported in Azure SQL compiler",
                    reason_code="UNSUPPORTED_AZURE_SQL_CAPABILITY",
                )

            # Build stage projection list
            stage_projections: List[str] = []
            mutated_dict = dict(mutations)

            # Preserve shadow columns already tracked
            for orig_col, shadow_name in mutated_orig_cols.items():
                stage_projections.append(
                    f"    {quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(shadow_name)} AS {quote_tsql_identifier(shadow_name)}"
                )

            # Process existing schema columns
            for col in current_schema:
                if col in mutated_dict:
                    # Column is mutated in this step. If not already shadowed, create shadow original column!
                    if col not in mutated_orig_cols:
                        shadow_name = f"__orig_{col}"
                        mutated_orig_cols[col] = shadow_name
                        stage_projections.append(
                            f"    {quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(col)} AS {quote_tsql_identifier(shadow_name)}"
                        )
                    stage_projections.append(f"    {mutated_dict[col]} AS {quote_tsql_identifier(col)}")
                else:
                    # Pass through unchanged column from prior stage
                    stage_projections.append(
                        f"    {quote_tsql_identifier(current_stage)}.{quote_tsql_identifier(col)} AS {quote_tsql_identifier(col)}"
                    )

            # Process newly created columns (e.g. flag, indicator, or distinct target)
            for col, sql_expr in mutations:
                col_str = str(col) if col is not None else ""
                if col_str and col_str not in current_schema:
                    stage_projections.append(f"    {sql_expr} AS {quote_tsql_identifier(col_str)}")
                    current_schema.append(col_str)

            # Assemble CTE block
            cte_blocks.append(
                f"[{step_stage}] AS (\n"
                f"    SELECT\n" + ",\n".join(stage_projections) + f"\n    FROM [{current_stage}]\n)"
            )

            # Advance current stage
            current_stage = step_stage

        # 3. Final projection: project all user columns (omit internal __orig_* shadow columns)
        final_user_cols = [str(c) for c in current_schema if c is not None and not str(c).startswith("__orig_")]
        final_proj = [f"    {quote_tsql_identifier(c)} AS {quote_tsql_identifier(c)}" for c in final_user_cols]
        final_query = (
            "WITH " + ",\n".join(cte_blocks) + "\n"
            f"SELECT\n" + ",\n".join(final_proj) + f"\nFROM [{current_stage}]"
        )

        # Compute deterministic semantic hash
        canonical_str = f"{final_query}|{self.runtime_profile_hash}|{sorted(final_user_cols)}"
        sem_hash = hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()

        return AzureSqlCompilationResult(
            query_sql=final_query,
            validation_queries=validation_queries,
            parameter_manifest={},
            input_columns=tuple(input_columns),
            output_columns=tuple(final_user_cols),
            runtime_profile=self.runtime_profile,
            runtime_profile_hash=self.runtime_profile_hash,
            semantic_hash=sem_hash,
        )


# ============================================================================
# 5. Staged Capability Registration
# ============================================================================

def register_azure_sql_slice1_capabilities(
    registry: Optional[EngineCapabilityRegistry] = None,
    profile: Optional[AzureSqlDatabaseRuntimeProfile] = None,
    status: CapabilityStatus = CapabilityStatus.NOT_TESTED,
    certification_commit: Optional[str] = None,
    verified_test_ids: Tuple[str, ...] = (),
) -> None:
    """Registers Slice 1 capabilities into the registry."""
    register_azure_sql_capabilities(
        registry=registry,
        profile=profile,
        status=status,
        certification_commit=certification_commit,
        verified_test_ids=verified_test_ids,
    )


def register_azure_sql_capabilities(
    registry: Optional[EngineCapabilityRegistry] = None,
    profile: Optional[AzureSqlDatabaseRuntimeProfile] = None,
    status: CapabilityStatus = CapabilityStatus.NOT_TESTED,
    certification_commit: Optional[str] = None,
    verified_test_ids: Tuple[str, ...] = (),
) -> None:
    """
    Registers the complete declared Azure SQL Database catalogue with granular dispositions.
    Enforces Mandatory Correction 2:
      - Every tuple ends in VERIFIED, PARTIAL, UNSUPPORTED, NOT_TESTED, ORCHESTRATION_ONLY,
        or DEFERRED_WITH_APPROVED_SCOPE.
      - Default denial: every unverified tuple remains non-executable.
    """
    reg = registry or get_global_capability_registry()
    prof = profile or AzureSqlDatabaseRuntimeProfile()
    rt_hash = prof.compute_hash()
    default_pol_hash = hashlib.sha256(b"{}").hexdigest()

    # Core declared tuples candidate for execution
    declared_tuples = [
        # MAP_VALUE
        (ActionFamily.MAP_VALUE, "TRIM"),
        (ActionFamily.MAP_VALUE, "UPPER"),
        (ActionFamily.MAP_VALUE, "LOWER"),
        # CONDITIONAL_REPLACE
        (ActionFamily.CONDITIONAL_REPLACE, "CONDITIONAL_REPLACE"),
        # CLIP & FLAG
        (ActionFamily.CLIP, "CLIP"),
        (ActionFamily.FLAG, "FLAG"),
        # PARSE
        (ActionFamily.PARSE, "PARSE_INTEGER"),
        (ActionFamily.PARSE, "PARSE_BIGINT"),
        (ActionFamily.PARSE, "PARSE_DECIMAL"),
        (ActionFamily.PARSE, "PARSE_FLOAT"),
        (ActionFamily.PARSE, "PARSE_CALENDAR_DATE"),
        (ActionFamily.PARSE, "PARSE_DATETIME2"),
        # IMPUTE
        (ActionFamily.IMPUTE, "IMPUTE"),
        # VALIDATE
        (ActionFamily.VALIDATE, "VALIDATE"),
        # DERIVE
        (ActionFamily.DERIVE, "DERIVE_ARITHMETIC"),
        (ActionFamily.DERIVE, "DERIVE_CONDITIONAL"),
        (ActionFamily.DERIVE, "DERIVE_COALESCE"),
        (ActionFamily.DERIVE, "DERIVE_CONCAT"),
        (ActionFamily.DERIVE, "DERIVE_DATE_ARITHMETIC"),
        # EXTRACT
        (ActionFamily.EXTRACT, "EXTRACT_DATE_PART"),
        (ActionFamily.EXTRACT, "EXTRACT_DELIMITER"),
        (ActionFamily.EXTRACT, "EXTRACT_JSON_PATH"),
        # FILTER & DEDUPLICATE
        (ActionFamily.FILTER, "FILTER"),
        (ActionFamily.DEDUPLICATE, "DEDUPLICATE"),
        # JOIN
        (ActionFamily.JOIN, "JOIN_INNER"),
        (ActionFamily.JOIN, "JOIN_LEFT"),
        (ActionFamily.JOIN, "JOIN_RIGHT"),
        (ActionFamily.JOIN, "JOIN_FULL_OUTER"),
        (ActionFamily.JOIN, "JOIN_SEMI"),
        (ActionFamily.JOIN, "JOIN_ANTI"),
        (ActionFamily.JOIN, "JOIN_CROSS"),
        # AGGREGATE
        (ActionFamily.AGGREGATE, "AGGREGATE"),
        # SCHEMA
        (ActionFamily.SCHEMA_MUTATION, "RENAME_COLUMN"),
        (ActionFamily.SCHEMA_MUTATION, "DROP_COLUMN"),
        (ActionFamily.SCHEMA_MUTATION, "CAST_COLUMN"),
        # MASK & HASH
        (ActionFamily.MASK, "MASK_FIXED"),
        (ActionFamily.MASK, "MASK_EMAIL"),
        (ActionFamily.HASH, "CANONICAL_SCALAR_V1"),
    ]

    for family, subtype in declared_tuples:
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
                        compiler_name=AzureSqlTsqlIRCompiler.COMPILER_NAME,
                        compiler_version=AzureSqlTsqlIRCompiler.COMPILER_VERSION,
                        engine_name=AzureSqlTsqlIRCompiler.ENGINE_NAME,
                        engine_version=prof.engine_version,
                        dialect=AzureSqlTsqlIRCompiler.DIALECT,
                        runtime_profile_hash=rt_hash,
                    )
                    reason = "VERIFIED_BY_AZURE_SQL_EVIDENCE" if status == CapabilityStatus.VERIFIED else "AZURE_SQL_EVIDENCE_PENDING"
                    reg.register_capability(
                        key=key,
                        status=status,
                        reason_code=reason,
                        limitations=(),
                        verified_test_ids=verified_test_ids,
                        certification_commit=certification_commit,
                    )

    # UNSUPPORTED tuples (Mandatory Corrections 4 and 5)
    unsupported_tuples = [
        (ActionFamily.DERIVE, "DIVIDE_RETURN_INFINITY", "RETURN_INFINITY_NOT_PORTABLE_TO_AZURE_SQL"),
        (ActionFamily.EXTRACT, "EXTRACT_REGEX", "REGEX_CAPTURE_GROUPS_UNSUPPORTED_IN_TSQL"),
        (ActionFamily.IMPUTE, "DYNAMIC_FIT", "DYNAMIC_AGGREGATE_FIT_PROHIBITED_IN_APPLY_IR"),
    ]
    for family, subtype, reason in unsupported_tuples:
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
                        compiler_name=AzureSqlTsqlIRCompiler.COMPILER_NAME,
                        compiler_version=AzureSqlTsqlIRCompiler.COMPILER_VERSION,
                        engine_name=AzureSqlTsqlIRCompiler.ENGINE_NAME,
                        engine_version=prof.engine_version,
                        dialect=AzureSqlTsqlIRCompiler.DIALECT,
                        runtime_profile_hash=rt_hash,
                    )
                    reg.register_capability(
                        key=key,
                        status=CapabilityStatus.UNSUPPORTED,
                        reason_code=reason,
                        limitations=("Blocked by architecture specification",),
                        verified_test_ids=(),
                        certification_commit=certification_commit,
                    )

    # NOT_TESTED tuples (Mandatory Correction 3)
    not_tested_tuples = [
        (ActionFamily.HASH, "HASH_DECIMAL", "DECIMAL_CANONICAL_BYTE_PARITY_NOT_TESTED"),
        (ActionFamily.HASH, "HASH_TIMESTAMP", "TIMESTAMP_CANONICAL_BYTE_PARITY_NOT_TESTED"),
        (ActionFamily.AGGREGATE, "EXACT_MEDIAN", "EXACT_MEDIAN_TSQL_NOT_TESTED"),
    ]
    for family, subtype, reason in not_tested_tuples:
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
                        compiler_name=AzureSqlTsqlIRCompiler.COMPILER_NAME,
                        compiler_version=AzureSqlTsqlIRCompiler.COMPILER_VERSION,
                        engine_name=AzureSqlTsqlIRCompiler.ENGINE_NAME,
                        engine_version=prof.engine_version,
                        dialect=AzureSqlTsqlIRCompiler.DIALECT,
                        runtime_profile_hash=rt_hash,
                    )
                    reg.register_capability(
                        key=key,
                        status=CapabilityStatus.NOT_TESTED,
                        reason_code=reason,
                        limitations=("Not tested in Phase 3",),
                        verified_test_ids=(),
                        certification_commit=certification_commit,
                    )

    # DEFERRED_WITH_APPROVED_SCOPE tuples (Mandatory Correction 9)
    deferred_tuples = [
        (ActionFamily.PASS_THROUGH, "FABRIC_WAREHOUSE", "DEFERRED_POST_PHASE_3"),
        (ActionFamily.PASS_THROUGH, "DUCKDB", "DEFERRED_POST_PHASE_3"),
        (ActionFamily.PASS_THROUGH, "SPARK_SQL_STANDALONE", "DEFERRED_POST_PHASE_3"),
    ]
    for family, subtype, reason in deferred_tuples:
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
                        compiler_name=AzureSqlTsqlIRCompiler.COMPILER_NAME,
                        compiler_version=AzureSqlTsqlIRCompiler.COMPILER_VERSION,
                        engine_name=AzureSqlTsqlIRCompiler.ENGINE_NAME,
                        engine_version=prof.engine_version,
                        dialect=AzureSqlTsqlIRCompiler.DIALECT,
                        runtime_profile_hash=rt_hash,
                    )
                    reg.register_capability(
                        key=key,
                        status=CapabilityStatus.DEFERRED_WITH_APPROVED_SCOPE,
                        reason_code=reason,
                        limitations=("Deferred to post-Phase 3 milestones",),
                        verified_test_ids=(),
                        certification_commit=certification_commit,
                    )


# Register initial non-verified / NOT_TESTED capabilities into global registry
register_azure_sql_capabilities()
