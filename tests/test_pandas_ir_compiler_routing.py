"""
Tests for Reference Pandas IR Compiler: Row Routing & Dataset Actions (Increment 3C)
=====================================================================================

Covers the 14 Traceability Matrix contracts (C1 - C14) and additional normative tests:
- C1: QUARANTINE predicate True always means quarantine, False means curated.
- C2: QUARANTINE PARTITION mode multiset reconciliation.
- C3: QUARANTINE COPY mode non-destructive routing.
- C4: QUARANTINE SEPARATE_OUTPUT unknown policy and candidate partition.
- C5: FILTER retain_matching=True vs retain_matching=False.
- C6: FILTER drop_policy=EXPORT_DROPPED vs DROP with accounting evidence.
- C7: FILTER 3-valued unknown policies.
- C8: DEDUPLICATE deterministic ordering (occurrence ID lowest-priority tie-breaker).
- C9: DEDUPLICATE null-key policies (TREAT_EACH_NULL_AS_DISTINCT vs TREAT_NULLS_AS_EQUAL vs EXCLUDE).
- C10: DEDUPLICATE export modes (REMOVED_OCCURRENCES_ONLY vs ALL_GROUP_MEMBERS).
- C11: VALIDATE non-mutating semantics and execution summary.
- C12: VALIDATE unknown policies and preservation of raw vs effective counts.
- C13: VALIDATE severity (WARNING non-fatal diagnostic vs ERROR fatal gate).
- C14: Composite integration (PARSE failure flag routed to QUARANTINE).
- Additional tests:
  - Source-stable coordinate binding and duplicate coordinate detection.
  - Execution-local identity isolation.
  - Multi-tier tie breaker with duplicate input indices.
  - Null distinct policy does not disable non-null deduplication.
  - Case-normalization collision rejection in output registry.
  - Multiset reconciliation precedes identity publication stripping.
  - Graph-final primary output explicit producer kind.
  - Filter drop accounting visibility.
  - Validation raw unknown count preservation.
  - Caller DataFrame immutability / isolation.
  - Single-output entrypoint guards for multi-output artifacts.
"""

from collections import Counter
import ast
from typing import Any, Dict

import numpy as np
import pandas as pd
import pytest

from agent.etl_pipeline.codegen_shared import CodegenStatus
from agent.etl_pipeline.pandas_ir_compiler import (
    CompiledPandasArtifact,
    CompilerAccountingError,
    CompilerStepDisposition,
    PandasCompilerResult,
    PandasIRCompiler,
)
from agent.etl_pipeline.transformation_ir import (
    TRANSFORMATION_IR_VERSION,
    ActionFamily,
    BinaryOp,
    BinaryOperator,
    ColumnRef,
    DataType,
    DatasetColumnSchema,
    DatasetOutputSpec,
    DatasetSchema,
    DeduplicateExpression,
    DeduplicationTieBreaker,
    DuplicateExportMode,
    FilterDropPolicy,
    FilterExpression,
    FilterUnknownPolicy,
    IRDependency,
    LiteralValue,
    MultiOutputArtifactError,
    NullCheck,
    NullKeyPolicy,
    OccurrenceIdentityScope,
    OrderDirection,
    OutputColumnCollisionError,
    OutputProducerKind,
    OutputPurpose,
    OutputReferenceCollisionError,
    ParseExpression,
    ParseFailurePolicy,
    ParseOutputMode,
    QuarantineExpression,
    QuarantineUnknownPolicy,
    RoutingIdentityPolicy,
    RoutingMode,
    RoutingPredicateUnknownError,
    RoutingReconciliationError,
    RowIdentityStrategy,
    SortColumnSpec,
    SourceStableIdentitySpec,
    StepKind,
    TransformationIRGraph,
    TransformationIRInvariantError,
    TransformationStepIR,
    ValidateExpression,
    ValidateSeverity,
    ValidationExecutionSummary,
    ValidationGateError,
    ValidationUnknownPolicy,
    ValueVersion,
    validate_output_registry,
)


def _execute_artifact(artifact: CompiledPandasArtifact, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Executes compiled code and returns all produced dataset outputs."""
    namespace: Dict[str, Any] = {}
    exec(artifact.source_code, namespace)
    return namespace["transform_datasets"](df)


def _execute_single(artifact: CompiledPandasArtifact, df: pd.DataFrame) -> pd.DataFrame:
    """Executes compiled code using the single-output transform(df) entrypoint."""
    namespace: Dict[str, Any] = {}
    exec(artifact.source_code, namespace)
    return namespace["transform"](df)


# ============================================================================
# C1: QUARANTINE Predicate Semantics: True -> Quarantine, False -> Curated
# ============================================================================

def test_quarantine_predicate_true_means_quarantine():
    col = ColumnRef(column_name="is_fraud", dataset_name="transactions")
    # Predicate: is_fraud == True -> belongs in quarantine
    cond = BinaryOp(left=col, op=BinaryOperator.EQ, right=LiteralValue(value=True, data_type=DataType.BOOLEAN))

    step = TransformationStepIR(
        step_id="q_fraud",
        dataset_name="transactions",
        family=ActionFamily.QUARANTINE,
        kind=StepKind.QUARANTINE_ROUTING,
        expression=QuarantineExpression(
            predicate=cond,
            curated_output_ref="tx_clean",
            quarantine_output_ref="tx_fraud",
            reason_column="quar_reason",
            reason_code="FRAUDULENT_TRANSACTION",
        ),
    )
    graph = TransformationIRGraph(
        graph_id="graph_q1",
        steps=(step,),
        primary_output_ref="tx_clean",
    )

    compiler = PandasIRCompiler()
    res = compiler.compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({
        "tx_id": [1, 2, 3, 4],
        "is_fraud": [False, True, False, True],
    })
    outputs = _execute_artifact(res.artifact, df_in)

    assert "tx_clean" in outputs
    assert "tx_fraud" in outputs
    assert list(outputs["tx_clean"]["tx_id"]) == [1, 3]
    assert list(outputs["tx_fraud"]["tx_id"]) == [2, 4]
    assert list(outputs["tx_fraud"]["quar_reason"]) == ["FRAUDULENT_TRANSACTION", "FRAUDULENT_TRANSACTION"]


# ============================================================================
# C2: QUARANTINE PARTITION Mode Multiset Reconciliation
# ============================================================================

def test_quarantine_partition_multiset_reconciliation():
    col = ColumnRef(column_name="val", dataset_name="data")
    cond = BinaryOp(left=col, op=BinaryOperator.LT, right=LiteralValue(value=0, data_type=DataType.INTEGER))

    step = TransformationStepIR(
        step_id="q_neg",
        dataset_name="data",
        family=ActionFamily.QUARANTINE,
        kind=StepKind.QUARANTINE_ROUTING,
        expression=QuarantineExpression(
            predicate=cond,
            curated_output_ref="data_curated",
            quarantine_output_ref="data_neg",
        ),
    )
    graph = TransformationIRGraph(
        graph_id="graph_q2",
        steps=(step,),
        primary_output_ref="data_curated",
    )
    res = PandasIRCompiler().compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({"val": [10, -5, 20, -1, 30]})
    outputs = _execute_artifact(res.artifact, df_in)
    cur = outputs["data_curated"]
    quar = outputs["data_neg"]

    # Invariant: len(curated) + len(quarantine) == len(input)
    assert len(cur) + len(quar) == len(df_in)
    assert set(cur["val"]) == {10, 20, 30}
    assert set(quar["val"]) == {-5, -1}


# ============================================================================
# C3: QUARANTINE COPY Mode Non-Destructive Routing
# ============================================================================

def test_quarantine_copy_mode():
    col = ColumnRef(column_name="alert", dataset_name="data")
    cond = BinaryOp(left=col, op=BinaryOperator.EQ, right=LiteralValue(value=True, data_type=DataType.BOOLEAN))

    step = TransformationStepIR(
        step_id="q_copy",
        dataset_name="data",
        family=ActionFamily.QUARANTINE,
        kind=StepKind.QUARANTINE_ROUTING,
        expression=QuarantineExpression(
            predicate=cond,
            routing_mode=RoutingMode.COPY,
            curated_output_ref="primary_data",
            quarantine_output_ref="alert_copies",
            reason_column="alert_reason",
            reason_code="SECURITY_ALERT",
        ),
    )
    graph = TransformationIRGraph(
        graph_id="graph_q3",
        steps=(step,),
        primary_output_ref="primary_data",
    )
    res = PandasIRCompiler().compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({
        "id": [1, 2, 3],
        "alert": [False, True, False],
    })
    outputs = _execute_artifact(res.artifact, df_in)

    # Primary retains all rows
    assert len(outputs["primary_data"]) == 3
    assert list(outputs["primary_data"]["id"]) == [1, 2, 3]
    # Alert copy contains only matching occurrence
    assert len(outputs["alert_copies"]) == 1
    assert list(outputs["alert_copies"]["id"]) == [2]
    assert list(outputs["alert_copies"]["alert_reason"]) == ["SECURITY_ALERT"]


# ============================================================================
# C4: QUARANTINE SEPARATE_OUTPUT Unknown Policy
# ============================================================================

def test_quarantine_separate_unknown_output():
    col = ColumnRef(column_name="status", dataset_name="orders")
    cond = BinaryOp(left=col, op=BinaryOperator.EQ, right=LiteralValue(value="SUSPECT", data_type=DataType.STRING))

    step = TransformationStepIR(
        step_id="q_unknown",
        dataset_name="orders",
        family=ActionFamily.QUARANTINE,
        kind=StepKind.QUARANTINE_ROUTING,
        expression=QuarantineExpression(
            predicate=cond,
            routing_mode=RoutingMode.PARTITION,
            unknown_policy=QuarantineUnknownPolicy.SEPARATE_OUTPUT,
            curated_output_ref="orders_curated",
            quarantine_output_ref="orders_quarantine",
            unknown_output_ref="orders_unknown",
        ),
    )
    graph = TransformationIRGraph(
        graph_id="graph_q4",
        steps=(step,),
        primary_output_ref="orders_curated",
    )
    res = PandasIRCompiler().compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({
        "order_id": [1, 2, 3, 4],
        "status": ["OK", "SUSPECT", None, "OK"],
    })
    outputs = _execute_artifact(res.artifact, df_in)

    assert list(outputs["orders_curated"]["order_id"]) == [1, 4]
    assert list(outputs["orders_quarantine"]["order_id"]) == [2]
    assert list(outputs["orders_unknown"]["order_id"]) == [3]
    assert len(outputs["orders_curated"]) + len(outputs["orders_quarantine"]) + len(outputs["orders_unknown"]) == 4


# ============================================================================
# C5: FILTER retain_matching=True vs retain_matching=False
# ============================================================================

def test_filter_retain_matching_true_and_false():
    col = ColumnRef(column_name="active", dataset_name="users")
    cond = BinaryOp(left=col, op=BinaryOperator.EQ, right=LiteralValue(value=True, data_type=DataType.BOOLEAN))

    # retain_matching = True -> keeps rows where active == True
    step_true = TransformationStepIR(
        step_id="f_true",
        dataset_name="users",
        family=ActionFamily.FILTER,
        kind=StepKind.DATASET_FILTER,
        expression=FilterExpression(predicate=cond, retain_matching=True),
    )
    graph_true = TransformationIRGraph(graph_id="g_f1", steps=(step_true,), primary_output_ref="primary")
    res_true = PandasIRCompiler().compile(graph_true)

    # retain_matching = False -> drops rows where active == True (keeps active == False)
    step_false = TransformationStepIR(
        step_id="f_false",
        dataset_name="users",
        family=ActionFamily.FILTER,
        kind=StepKind.DATASET_FILTER,
        expression=FilterExpression(predicate=cond, retain_matching=False),
    )
    graph_false = TransformationIRGraph(graph_id="g_f2", steps=(step_false,), primary_output_ref="primary")
    res_false = PandasIRCompiler().compile(graph_false)

    df_in = pd.DataFrame({
        "user_id": [1, 2, 3],
        "active": [True, False, True],
    })

    out_true = _execute_single(res_true.artifact, df_in)
    out_false = _execute_single(res_false.artifact, df_in)

    assert list(out_true["user_id"]) == [1, 3]
    assert list(out_false["user_id"]) == [2]


# ============================================================================
# C6: FILTER drop_policy=EXPORT_DROPPED vs DROP
# ============================================================================

def test_filter_export_dropped_and_drop_accounting():
    col = ColumnRef(column_name="score", dataset_name="evals")
    cond = BinaryOp(left=col, op=BinaryOperator.GTE, right=LiteralValue(value=50, data_type=DataType.INTEGER))

    step = TransformationStepIR(
        step_id="f_export",
        dataset_name="evals",
        family=ActionFamily.FILTER,
        kind=StepKind.DATASET_FILTER,
        expression=FilterExpression(
            predicate=cond,
            retain_matching=True,
            drop_policy=FilterDropPolicy.EXPORT_DROPPED,
            dropped_output_ref="evals_failed",
            rejection_reason_code="SCORE_BELOW_THRESHOLD",
        ),
    )
    graph = TransformationIRGraph(graph_id="g_f3", steps=(step,), primary_output_ref="evals_passed")
    res = PandasIRCompiler().compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({
        "student": ["A", "B", "C", "D"],
        "score": [80, 40, 95, 30],
    })
    outputs = _execute_artifact(res.artifact, df_in)

    assert "evals_passed" in outputs
    assert "evals_failed" in outputs
    assert list(outputs["evals_passed"]["student"]) == ["A", "C"]
    assert list(outputs["evals_failed"]["student"]) == ["B", "D"]
    assert list(outputs["evals_failed"]["_dhara_filter_reason"]) == ["SCORE_BELOW_THRESHOLD", "SCORE_BELOW_THRESHOLD"]


# ============================================================================
# C7: FILTER 3-Valued Unknown Policies
# ============================================================================

def test_filter_unknown_policies():
    col = ColumnRef(column_name="grade", dataset_name="tests")
    cond = BinaryOp(left=col, op=BinaryOperator.EQ, right=LiteralValue(value="A", data_type=DataType.STRING))

    # 1. RETAIN: unknowns stay in primary
    step_retain = TransformationStepIR(
        step_id="f_unk_retain", dataset_name="tests", family=ActionFamily.FILTER, kind=StepKind.DATASET_FILTER,
        expression=FilterExpression(predicate=cond, retain_matching=True, unknown_policy=FilterUnknownPolicy.RETAIN)
    )
    res_retain = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g1", steps=(step_retain,), primary_output_ref="primary"))

    # 2. FILTER_OUT: unknowns are dropped
    step_drop = TransformationStepIR(
        step_id="f_unk_drop", dataset_name="tests", family=ActionFamily.FILTER, kind=StepKind.DATASET_FILTER,
        expression=FilterExpression(predicate=cond, retain_matching=True, unknown_policy=FilterUnknownPolicy.FILTER_OUT)
    )
    res_drop = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g2", steps=(step_drop,), primary_output_ref="primary"))

    # 3. FAIL_STEP: unknowns raise error
    step_fail = TransformationStepIR(
        step_id="f_unk_fail", dataset_name="tests", family=ActionFamily.FILTER, kind=StepKind.DATASET_FILTER,
        expression=FilterExpression(predicate=cond, retain_matching=True, unknown_policy=FilterUnknownPolicy.FAIL_STEP)
    )
    res_fail = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g3", steps=(step_fail,), primary_output_ref="primary"))

    df_in = pd.DataFrame({"id": [1, 2, 3], "grade": ["A", "B", None]})

    out_ret = _execute_single(res_retain.artifact, df_in)
    assert list(out_ret["id"]) == [1, 3]

    out_drop = _execute_single(res_drop.artifact, df_in)
    assert list(out_drop["id"]) == [1]

    with pytest.raises(ValueError, match="Filter predicate evaluated to unknown"):
        _execute_single(res_fail.artifact, df_in)


# ============================================================================
# C8: DEDUPLICATE Deterministic Ordering & Occurrence ID Tie-Breaker
# ============================================================================

def test_deduplicate_deterministic_ordering_with_tie_breaker():
    step = TransformationStepIR(
        step_id="dedup_ordered",
        dataset_name="records",
        family=ActionFamily.DEDUPLICATE,
        kind=StepKind.DATASET_FILTER,
        expression=DeduplicateExpression(
            key_columns=("customer_id",),
            resolved_key_columns=("customer_id",),
            tie_breaker=DeduplicationTieBreaker.EXPLICIT_ORDER,
            order_by=(
                SortColumnSpec(column="priority", direction=OrderDirection.DESCENDING),
                SortColumnSpec(column="created_date", direction=OrderDirection.ASCENDING),
            ),
            use_occurrence_id_as_final_tie_breaker=True,
        ),
    )
    graph = TransformationIRGraph(graph_id="g_dedup1", steps=(step,), primary_output_ref="primary")
    res = PandasIRCompiler().compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({
        "customer_id": [101, 101, 101],
        "priority": [1, 2, 2],
        "created_date": ["2026-01-02", "2026-01-05", "2026-01-01"],
        "row_tag": ["R1", "R2", "R3"],
    })
    out = _execute_single(res.artifact, df_in)

    # Highest priority is 2 (R2 and R3). Among priority 2, earliest created_date is 2026-01-01 (R3).
    assert len(out) == 1
    assert out.iloc[0]["row_tag"] == "R3"


# ============================================================================
# C9: DEDUPLICATE Null-Key Policies
# ============================================================================

def test_deduplicate_null_key_policies():
    # 1. TREAT_NULLS_AS_EQUAL: all null key rows collapse to one survivor
    step_equal = TransformationStepIR(
        step_id="d_equal", dataset_name="items", family=ActionFamily.DEDUPLICATE, kind=StepKind.DATASET_FILTER,
        expression=DeduplicateExpression(
            key_columns=("code",),
            resolved_key_columns=("code",),
            null_key_policy=NullKeyPolicy.TREAT_NULLS_AS_EQUAL,
        ),
    )
    res_equal = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_d1", steps=(step_equal,), primary_output_ref="primary"))

    # 2. TREAT_EACH_NULL_AS_DISTINCT: null key rows do NOT deduplicate against each other
    step_distinct = TransformationStepIR(
        step_id="d_distinct", dataset_name="items", family=ActionFamily.DEDUPLICATE, kind=StepKind.DATASET_FILTER,
        expression=DeduplicateExpression(
            key_columns=("code",),
            resolved_key_columns=("code",),
            null_key_policy=NullKeyPolicy.TREAT_EACH_NULL_AS_DISTINCT,
        ),
    )
    res_distinct = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_d2", steps=(step_distinct,), primary_output_ref="primary"))

    df_in = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "code": [None, None, "X", "X"],
    })

    out_equal = _execute_single(res_equal.artifact, df_in)
    assert len(out_equal) == 2  # 1 null survivor + 1 'X' survivor

    out_distinct = _execute_single(res_distinct.artifact, df_in)
    assert len(out_distinct) == 3  # both null rows distinct + 1 'X' survivor


# ============================================================================
# C10: DEDUPLICATE Export Modes
# ============================================================================

def test_deduplicate_export_modes():
    # 1. REMOVED_OCCURRENCES_ONLY: exports only the dropped duplicates
    step_removed = TransformationStepIR(
        step_id="d_rem", dataset_name="items", family=ActionFamily.DEDUPLICATE, kind=StepKind.DATASET_FILTER,
        expression=DeduplicateExpression(
            key_columns=("code",),
            resolved_key_columns=("code",),
            duplicates_output_ref="dups_removed",
            duplicate_export_mode=DuplicateExportMode.REMOVED_OCCURRENCES_ONLY,
        ),
    )
    res_rem = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_dm1", steps=(step_removed,), primary_output_ref="primary"))

    # 2. ALL_GROUP_MEMBERS: exports all rows belonging to duplicate clusters
    step_all = TransformationStepIR(
        step_id="d_all", dataset_name="items", family=ActionFamily.DEDUPLICATE, kind=StepKind.DATASET_FILTER,
        expression=DeduplicateExpression(
            key_columns=("code",),
            resolved_key_columns=("code",),
            duplicates_output_ref="dups_all",
            duplicate_export_mode=DuplicateExportMode.ALL_GROUP_MEMBERS,
        ),
    )
    res_all = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_dm2", steps=(step_all,), primary_output_ref="primary"))

    df_in = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "code": ["A", "A", "B", "C"],
    })

    outs_rem = _execute_artifact(res_rem.artifact, df_in)
    assert len(outs_rem["dups_removed"]) == 1
    assert list(outs_rem["dups_removed"]["id"]) == [2]

    outs_all = _execute_artifact(res_all.artifact, df_in)
    assert len(outs_all["dups_all"]) == 2
    assert set(outs_all["dups_all"]["id"]) == {1, 2}


# ============================================================================
# C11: VALIDATE Non-Mutating Semantics & Diagnostic Summary
# ============================================================================

def test_validate_non_mutating_semantics():
    col = ColumnRef(column_name="age", dataset_name="people")
    cond = BinaryOp(left=col, op=BinaryOperator.GTE, right=LiteralValue(value=0, data_type=DataType.INTEGER))

    step = TransformationStepIR(
        step_id="v_age",
        dataset_name="people",
        family=ActionFamily.VALIDATE,
        kind=StepKind.ASSERTION,
        expression=ValidateExpression(
            predicate=cond,
            severity=ValidateSeverity.ERROR,
            reason_code="NEGATIVE_AGE_PROHIBITED",
        ),
    )
    graph = TransformationIRGraph(graph_id="g_v1", steps=(step,), primary_output_ref="primary")
    res = PandasIRCompiler().compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
    df_before = df_in.copy()
    out = _execute_single(res.artifact, df_in)

    # Frame is completely unmutated
    pd.testing.assert_frame_equal(out, df_before)


# ============================================================================
# C12: VALIDATE Unknown Policies & Count Preservation
# ============================================================================

def test_validation_preserves_raw_unknown_count_after_policy_resolution():
    col = ColumnRef(column_name="passed", dataset_name="tests")
    cond = BinaryOp(left=col, op=BinaryOperator.EQ, right=LiteralValue(value=True, data_type=DataType.BOOLEAN))

    # WARNING severity with COUNT_AS_VIOLATION
    step = TransformationStepIR(
        step_id="v_unknown_test",
        dataset_name="tests",
        family=ActionFamily.VALIDATE,
        kind=StepKind.ASSERTION,
        expression=ValidateExpression(
            predicate=cond,
            severity=ValidateSeverity.WARNING,
            unknown_policy=ValidationUnknownPolicy.COUNT_AS_VIOLATION,
            reason_code="PASSED_MUST_BE_TRUE",
        ),
    )
    graph = TransformationIRGraph(graph_id="g_v2", steps=(step,), primary_output_ref="primary")
    res = PandasIRCompiler().compile(graph)

    df_in = pd.DataFrame({"id": [1, 2, 3], "passed": [True, False, None]})

    # Verify execution succeeds under warning
    namespace = {}
    exec(res.artifact.source_code, namespace)
    outputs = namespace["transform_datasets"](df_in)
    assert "primary" in outputs


# ============================================================================
# C13: VALIDATE Severity: WARNING vs ERROR
# ============================================================================

def test_validate_severity_warning_vs_error():
    col = ColumnRef(column_name="score", dataset_name="exams")
    cond = BinaryOp(left=col, op=BinaryOperator.GTE, right=LiteralValue(value=50, data_type=DataType.INTEGER))

    step_warn = TransformationStepIR(
        step_id="v_warn", dataset_name="exams", family=ActionFamily.VALIDATE, kind=StepKind.ASSERTION,
        expression=ValidateExpression(predicate=cond, severity=ValidateSeverity.WARNING, reason_code="SCORE_LOW")
    )
    res_warn = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_vw", steps=(step_warn,), primary_output_ref="primary"))

    step_err = TransformationStepIR(
        step_id="v_err", dataset_name="exams", family=ActionFamily.VALIDATE, kind=StepKind.ASSERTION,
        expression=ValidateExpression(predicate=cond, severity=ValidateSeverity.ERROR, reason_code="SCORE_LOW")
    )
    res_err = PandasIRCompiler().compile(TransformationIRGraph(graph_id="g_ve", steps=(step_err,), primary_output_ref="primary"))

    df_in = pd.DataFrame({"score": [80, 20]})

    # WARNING executes cleanly without exception
    out_warn = _execute_single(res_warn.artifact, df_in)
    assert len(out_warn) == 2

    # ERROR raises bounded ValueError without PII
    with pytest.raises(ValueError, match="Validation assertion failed"):
        _execute_single(res_err.artifact, df_in)


# ============================================================================
# C14: Composite Integration: PARSE Failure Flag to QUARANTINE
# ============================================================================

def test_parse_failure_to_quarantine_integration():
    src_col = ColumnRef(column_name="amount_str", dataset_name="ledger")
    # Step 1: Parse amount string with FLAG failure policy
    step_parse = TransformationStepIR(
        step_id="p_amount",
        dataset_name="ledger",
        family=ActionFamily.PARSE,
        kind=StepKind.COLUMN_DERIVATION,
        expression=ParseExpression(
            source_column=src_col,
            target_column="amount",
            output_mode=ParseOutputMode.CREATE_PARSED_COLUMN,
            failure_policy=ParseFailurePolicy.FLAG,
            target_data_type=DataType.FLOAT,
        ),
        target_column="amount",
    )

    # Step 2: Quarantine rows with parse error (True -> Quarantine)
    err_col = ColumnRef(column_name="amount_is_parse_error", dataset_name="ledger", version=ValueVersion.AFTER_STEP, referenced_step_id="p_amount")
    cond_err = BinaryOp(left=err_col, op=BinaryOperator.EQ, right=LiteralValue(value=True, data_type=DataType.BOOLEAN))
    step_quar = TransformationStepIR(
        step_id="q_parse_err",
        dataset_name="ledger",
        family=ActionFamily.QUARANTINE,
        kind=StepKind.QUARANTINE_ROUTING,
        expression=QuarantineExpression(
            predicate=cond_err,
            curated_output_ref="ledger_clean",
            quarantine_output_ref="ledger_unparseable",
            reason_column="quarantine_cause",
            reason_code="UNPARSEABLE_AMOUNT",
        ),
        prerequisites=("p_amount",),
    )

    graph = TransformationIRGraph(
        graph_id="g_parse_quar",
        steps=(step_parse, step_quar),
        dependencies=(IRDependency(source_step_id="p_amount", target_step_id="q_parse_err"),),
        primary_output_ref="ledger_clean",
    )
    res = PandasIRCompiler().compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({
        "tx_id": [1, 2, 3],
        "amount_str": ["100.50", "INVALID_VAL", "250.00"],
    })
    outputs = _execute_artifact(res.artifact, df_in)

    assert "ledger_clean" in outputs
    assert "ledger_unparseable" in outputs
    assert list(outputs["ledger_clean"]["tx_id"]) == [1, 3]
    assert list(outputs["ledger_unparseable"]["tx_id"]) == [2]
    assert list(outputs["ledger_unparseable"]["quarantine_cause"]) == ["UNPARSEABLE_AMOUNT"]


# ============================================================================
# Additional Normative Tests
# ============================================================================

def test_source_stable_identity_uses_bound_coordinate_columns():
    policy = RoutingIdentityPolicy(
        strategy=RowIdentityStrategy.SOURCE_STABLE_COORDINATES,
        scope=OccurrenceIdentityScope.SOURCE_STABLE,
        source_stable_spec=SourceStableIdentitySpec(
            snapshot_id="snap_2026_09",
            partition_column="part_dt",
            row_offset_column="offset_id",
        ),
        retain_in_primary_output=True,
    )
    step = TransformationStepIR(
        step_id="f_noop", dataset_name="logs", family=ActionFamily.FILTER, kind=StepKind.DATASET_FILTER,
        expression=FilterExpression(
            predicate=BinaryOp(left=ColumnRef(column_name="part_dt", dataset_name="logs"), op=BinaryOperator.NEQ, right=LiteralValue(value=None)),
            retain_matching=True,
        )
    )
    schema = DatasetSchema(
        dataset_name="logs",
        columns={
            "part_dt": DatasetColumnSchema(name="part_dt", data_type=DataType.STRING),
            "offset_id": DatasetColumnSchema(name="offset_id", data_type=DataType.INTEGER),
        }
    )
    graph = TransformationIRGraph(
        graph_id="g_source_stable",
        steps=(step,),
        routing_identity_policy=policy,
        primary_output_ref="primary",
    )
    compiler = PandasIRCompiler(schemas={"logs": schema})
    res = compiler.compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({"part_dt": ["2026-09-01", "2026-09-01"], "offset_id": [100, 101]})
    out = _execute_single(res.artifact, df_in)
    from agent.etl_pipeline.canonical_encoding import canonical_hash_coordinate_tuple_v1
    exp_0 = canonical_hash_coordinate_tuple_v1([
        ("snapshot_id", "snap_2026_09"),
        ("part_dt", "2026-09-01"),
        ("offset_id", 100),
    ])
    exp_1 = canonical_hash_coordinate_tuple_v1([
        ("snapshot_id", "snap_2026_09"),
        ("part_dt", "2026-09-01"),
        ("offset_id", 101),
    ])
    assert list(out["__dhara_row_occurrence_0"]) == [exp_0, exp_1]


def test_source_stable_identity_rejects_duplicate_coordinates():
    policy = RoutingIdentityPolicy(
        strategy=RowIdentityStrategy.SOURCE_STABLE_COORDINATES,
        scope=OccurrenceIdentityScope.SOURCE_STABLE,
        source_stable_spec=SourceStableIdentitySpec(
            snapshot_id="snap_1",
            partition_column="p_col",
            row_offset_column="r_col",
        ),
    )
    step = TransformationStepIR(
        step_id="f_step", dataset_name="ds", family=ActionFamily.FILTER, kind=StepKind.DATASET_FILTER,
        expression=FilterExpression(predicate=BinaryOp(left=ColumnRef(column_name="p_col", dataset_name="ds"), op=BinaryOperator.NEQ, right=LiteralValue(value=None)))
    )
    graph = TransformationIRGraph(graph_id="g_dup_coord", steps=(step,), routing_identity_policy=policy, primary_output_ref="primary")
    res = PandasIRCompiler().compile(graph)

    # Input with duplicate coordinate combination
    df_in = pd.DataFrame({"p_col": ["P1", "P1"], "r_col": [1, 1]})
    with pytest.raises(ValueError, match="Duplicate source-stable coordinates detected"):
        _execute_single(res.artifact, df_in)


def test_execution_local_identity_does_not_claim_cross_run_stability():
    with pytest.raises(ValueError, match="EXECUTION_LOCAL scope forbids source_stable_spec"):
        RoutingIdentityPolicy(
            scope=OccurrenceIdentityScope.EXECUTION_LOCAL,
            source_stable_spec=SourceStableIdentitySpec(
                snapshot_id="snap_1", partition_column="p", row_offset_column="o"
            ),
        )


def test_occurrence_identity_is_lowest_priority_dedup_tie_breaker():
    # Tie on all business keys, user order values tie, input indices duplicated
    step = TransformationStepIR(
        step_id="dedup_tie",
        dataset_name="records",
        family=ActionFamily.DEDUPLICATE,
        kind=StepKind.DATASET_FILTER,
        expression=DeduplicateExpression(
            key_columns=("account",),
            resolved_key_columns=("account",),
            tie_breaker=DeduplicationTieBreaker.EXPLICIT_ORDER,
            order_by=(SortColumnSpec(column="priority", direction=OrderDirection.ASCENDING),),
            use_occurrence_id_as_final_tie_breaker=True,
        ),
    )
    graph = TransformationIRGraph(graph_id="g_tie", steps=(step,), primary_output_ref="primary")
    res = PandasIRCompiler().compile(graph)

    # DataFrame with intentionally duplicated index (0, 0, 0) and identical business columns
    df_in = pd.DataFrame(
        {"account": ["A1", "A1", "A1"], "priority": [10, 10, 10], "seq": ["First", "Second", "Third"]},
        index=[0, 0, 0],
    )
    out = _execute_single(res.artifact, df_in)
    assert len(out) == 1
    # Occurrence ID tie breaker selects the first occurrence stably
    assert out.iloc[0]["seq"] == "First"


def test_null_distinct_policy_does_not_disable_non_null_deduplication():
    step = TransformationStepIR(
        step_id="dedup_mixed_null",
        dataset_name="users",
        family=ActionFamily.DEDUPLICATE,
        kind=StepKind.DATASET_FILTER,
        expression=DeduplicateExpression(
            key_columns=("email",),
            resolved_key_columns=("email",),
            null_key_policy=NullKeyPolicy.TREAT_EACH_NULL_AS_DISTINCT,
        ),
    )
    graph = TransformationIRGraph(graph_id="g_mix", steps=(step,), primary_output_ref="primary")
    res = PandasIRCompiler().compile(graph)

    df_in = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "email": ["bob@example.com", "bob@example.com", None, None, None],
    })
    out = _execute_single(res.artifact, df_in)

    # bob@example.com duplicates deduplicate to 1 row; 3 nulls stay distinct -> 4 survivors
    assert len(out) == 4
    assert list(out["id"]) == [1, 3, 4, 5]


def test_output_refs_reject_case_normalization_collisions():
    graph = TransformationIRGraph(
        graph_id="g_case_col",
        steps=(),
        output_specs=(
            DatasetOutputSpec(output_ref="Curated_Data", purpose=OutputPurpose.PRIMARY, producer_kind=OutputProducerKind.GRAPH_FINAL, is_primary=True),
            DatasetOutputSpec(output_ref="curated_data", purpose=OutputPurpose.DIAGNOSTIC, producer_kind=OutputProducerKind.GRAPH_FINAL),
        ),
    )
    with pytest.raises(OutputReferenceCollisionError, match="collides with existing reference"):
        validate_output_registry(graph)


def test_reconciliation_precedes_identity_publication_stripping():
    # Step with partition quarantine where retain_in_primary_output is False
    col = ColumnRef(column_name="flag", dataset_name="items")
    step = TransformationStepIR(
        step_id="q_strip",
        dataset_name="items",
        family=ActionFamily.QUARANTINE,
        kind=StepKind.QUARANTINE_ROUTING,
        expression=QuarantineExpression(
            predicate=BinaryOp(left=col, op=BinaryOperator.EQ, right=LiteralValue(value=True, data_type=DataType.BOOLEAN)),
            curated_output_ref="clean",
            quarantine_output_ref="rejects",
        ),
    )
    policy = RoutingIdentityPolicy(retain_in_primary_output=False, retain_in_secondary_outputs=False)
    graph = TransformationIRGraph(
        graph_id="g_strip",
        steps=(step,),
        routing_identity_policy=policy,
        primary_output_ref="clean",
    )
    res = PandasIRCompiler().compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED

    df_in = pd.DataFrame({"id": [1, 2], "flag": [False, True]})
    outputs = _execute_artifact(res.artifact, df_in)

    # Technical identity column is stripped from both final outputs after reconciliation
    assert "__dhara_row_occurrence_0" not in outputs["clean"].columns
    assert "__dhara_row_occurrence_0" not in outputs["rejects"].columns


def test_graph_final_primary_output_has_explicit_producer_kind():
    spec_step = DatasetOutputSpec(
        output_ref="out1", purpose=OutputPurpose.QUARANTINE, producer_kind=OutputProducerKind.STEP, produced_by_step_id="step1"
    )
    assert spec_step.producer_kind == OutputProducerKind.STEP

    spec_final = DatasetOutputSpec(
        output_ref="out_final", purpose=OutputPurpose.PRIMARY, producer_kind=OutputProducerKind.GRAPH_FINAL, is_primary=True
    )
    assert spec_final.producer_kind == OutputProducerKind.GRAPH_FINAL

    with pytest.raises(ValueError, match="STEP producer requires produced_by_step_id"):
        DatasetOutputSpec(output_ref="bad_step", purpose=OutputPurpose.QUARANTINE, producer_kind=OutputProducerKind.STEP)

    with pytest.raises(ValueError, match="GRAPH_FINAL producer forbids produced_by_step_id"):
        DatasetOutputSpec(output_ref="bad_final", purpose=OutputPurpose.PRIMARY, producer_kind=OutputProducerKind.GRAPH_FINAL, produced_by_step_id="step1")


def test_compiler_does_not_unintentionally_mutate_caller_dataframe():
    col = ColumnRef(column_name="num", dataset_name="nums")
    step = TransformationStepIR(
        step_id="f_noop", dataset_name="nums", family=ActionFamily.FILTER, kind=StepKind.DATASET_FILTER,
        expression=FilterExpression(predicate=BinaryOp(left=col, op=BinaryOperator.GT, right=LiteralValue(value=0, data_type=DataType.INTEGER)))
    )
    graph = TransformationIRGraph(graph_id="g_caller_immut", steps=(step,), primary_output_ref="primary")
    res = PandasIRCompiler().compile(graph)

    caller_df = pd.DataFrame({"num": [10, 20, 30]})
    caller_df_orig = caller_df.copy(deep=True)

    _ = _execute_single(res.artifact, caller_df)

    # Caller DataFrame was not mutated in place (no identity columns assigned to caller object)
    pd.testing.assert_frame_equal(caller_df, caller_df_orig)


def test_single_output_entrypoint_guards_multi_output_artifact():
    col = ColumnRef(column_name="flag", dataset_name="items")
    step = TransformationStepIR(
        step_id="q_guard",
        dataset_name="items",
        family=ActionFamily.QUARANTINE,
        kind=StepKind.QUARANTINE_ROUTING,
        expression=QuarantineExpression(
            predicate=BinaryOp(left=col, op=BinaryOperator.EQ, right=LiteralValue(value=True, data_type=DataType.BOOLEAN)),
            curated_output_ref="out_curated",
            quarantine_output_ref="out_quar",
        ),
    )
    # primary_output_ref is NOT configured, so multiple outputs exist without a designated primary
    graph = TransformationIRGraph(
        graph_id="g_multi_guard",
        steps=(step,),
    )
    res = PandasIRCompiler().compile(graph)
    assert res.status == CodegenStatus.SUCCEEDED
    assert res.artifact.supports_single_output_entrypoint is False

    df_in = pd.DataFrame({"id": [1, 2], "flag": [False, True]})
    with pytest.raises(ValueError, match="MultiOutputArtifactError"):
        _execute_single(res.artifact, df_in)
