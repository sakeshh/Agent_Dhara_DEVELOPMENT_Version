"""
agent/etl_pipeline/azure_sql_ir_compiler.py

Authoritative Azure SQL Database T-SQL IR Compiler (Phase 3 Increment 3.4A).
Compiles typed TransformationStepIR into pure, unadulterated Azure SQL T-SQL artifacts.
Enforces strict capability preflight, original-vs-current value preservation across CTE stages,
apply-only static imputation validation, three-way validation accounting, and clean
separation between compiler query artifacts and certification execution harnesses.
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
    BinaryOp,
    BinaryOperator,
    ClipExpression,
    ColumnRef,
    ConditionalExpression,
    ConditionalReplaceExpression,
    DataType,
    Expression,
    FlagExpression,
    ImputationModelIntegrityError,
    ImputeExpression,
    InSetPredicate,
    LiteralValue,
    NullCheck,
    ParseExpression,
    ParseFailurePolicy,
    PredicateUnknownPolicy,
    ScalarMapExpression,
    ScalarMapOperation,
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
        Enforces mandatory session settings, isolated temp table creation, fixture inserts,
        transformation query execution, and guaranteed cleanup.
        """
        lines = [
            "-- ============================================================================",
            "-- Azure SQL Database Certification Harness Artifact (Phase 3 Increment 3.4A)",
            f"-- Runtime Profile Hash: {self.runtime_profile_hash}",
            f"-- Semantic Hash:        {self.semantic_hash}",
            "-- ============================================================================",
            "SET NOCOUNT ON;",
            "SET XACT_ABORT ON;",
            "SET ANSI_NULLS ON;",
            "SET ANSI_WARNINGS ON;",
            "SET ARITHABORT ON;",
            "",
            f"DROP TABLE IF EXISTS {table_name};",
            "",
        ]

        # Infer column types if not explicitly given
        cols = self.input_columns or tuple(column_types.keys()) if column_types else ()
        types = column_types or {}
        col_defs = []
        for c in cols:
            q_col = quote_tsql_identifier(c)
            t = types.get(c, "NVARCHAR(MAX)")
            col_defs.append(f"    {q_col} {t}")

        lines.append(f"CREATE TABLE {table_name} (")
        lines.append(",\n".join(col_defs) if col_defs else "    [id] INT")
        lines.append(");")
        lines.append("")

        # Insert fixture rows if provided
        if rows:
            lines.append(f"-- Fixture Inserts ({len(rows)} rows)")
            for r in rows:
                col_names = [quote_tsql_identifier(k) for k in cols]
                val_exprs = [quote_tsql_literal(r.get(k)) for k in cols]
                lines.append(f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({', '.join(val_exprs)});")
            lines.append("")

        lines.append("-- Compiler Transformation Query Artifact")
        lines.append(self.query_sql)
        lines.append(";")
        lines.append("")

        # Append validation queries if any
        if self.validation_queries:
            lines.append("-- Validation Gates")
            for vq in self.validation_queries:
                lines.append(vq)
                lines.append(";")
            lines.append("")

        lines.append("-- Isolated Fixture Cleanup")
        lines.append(f"DROP TABLE IF EXISTS {table_name};")
        lines.append("")

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
        if isinstance(expr, ColumnRef):
            col = expr.column_name
            if col not in schema_cols and expr.version != ValueVersion.ORIGINAL:
                raise AzureSqlSchemaBindingError(f"Column [{col}] not found in schema", reason_code="COLUMN_NOT_FOUND")

            if expr.version == ValueVersion.ORIGINAL:
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
    # Vertical Slice 1 Action Compilers
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
                # Validation does not mutate schema or create a new data transformation stage
                continue

            # Produce transformation expression(s)
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
            else:
                # Default rejection for all other families (JOIN, AGGREGATE, MASK, HASH, PUBLISH)
                raise AzureSqlUnsupportedCapabilityError(
                    f"ActionFamily '{fam.value}' is not supported in Azure SQL Slice 1",
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
                if col not in current_schema:
                    stage_projections.append(f"    {sql_expr} AS {quote_tsql_identifier(col)}")
                    current_schema.append(col)

            # Assemble CTE block
            cte_blocks.append(
                f"[{step_stage}] AS (\n"
                f"    SELECT\n" + ",\n".join(stage_projections) + f"\n    FROM [{current_stage}]\n)"
            )

            # Advance current stage
            current_stage = step_stage

        # 3. Final projection: project all user columns (omit internal __orig_* shadow columns)
        final_user_cols = [c for c in current_schema if not c.startswith("__orig_")]
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
    """
    Registers Azure SQL Database capabilities for Vertical Slice 1.
    By default, status is NOT_TESTED with execution_eligible = False.
    Only when direct Azure SQL execution evidence is verified does status become VERIFIED.
    """
    reg = registry or get_global_capability_registry()
    prof = profile or AzureSqlDatabaseRuntimeProfile()
    rt_hash = prof.compute_hash()
    default_pol_hash = hashlib.sha256(b"{}").hexdigest()

    slice1_tuples = [
        # MAP_VALUE
        (ActionFamily.MAP_VALUE, "TRIM"),
        (ActionFamily.MAP_VALUE, "UPPER"),
        (ActionFamily.MAP_VALUE, "LOWER"),
        # CONDITIONAL_REPLACE
        (ActionFamily.CONDITIONAL_REPLACE, "CONDITIONAL_REPLACE"),
        # CLIP & FLAG
        (ActionFamily.CLIP, "CLIP"),
        (ActionFamily.FLAG, "FLAG"),
        # PARSE (granular subtypes)
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
    ]

    for family, subtype in slice1_tuples:
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


# Register initial non-verified / NOT_TESTED capabilities into global registry
register_azure_sql_slice1_capabilities()
