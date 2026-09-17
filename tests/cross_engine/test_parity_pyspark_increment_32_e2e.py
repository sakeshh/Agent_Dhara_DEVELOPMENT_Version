"""
tests/cross_engine/test_parity_pyspark_increment_32_e2e.py

Phase 3 Increment 3.2 Combined E2E Parity Scenario.

Verifies end-to-end multi-step pipeline execution parity between:
- Pandas Reference IR Compiler
- Native PySpark Catalyst IR Compiler

Pipeline Flow:
1. Occurrence Identity Binding (EXECUTION_LOCAL, cached single-assignment)
2. Parse Step: Parse raw amount string to float
3. Validate Step: Non-mutating validation asserting amount > 0 (emits validation metric summary)
4. Quarantine Step: Route suspicious transactions (status == 'FRAUD') to 'quarantine' branch
5. Deduplicate Step: Deduplicate by customer_id, preserve first by timestamp
6. Mask Step: Mask sensitive credit card numbers (PRESERVE_LAST_N, last 4)
7. Schema Mutation: Drop raw string columns, rename cleaned columns
8. Publication Eligibility Boundary: Strip internal technical identity columns
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
import tempfile
from typing import Any, Dict

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import SparkSession

from agent.etl_pipeline.codegen_shared import CodegenStatus
from agent.etl_pipeline.pandas_ir_compiler import PandasIRCompiler
from agent.etl_pipeline.pyspark_ir_compiler import PySparkIRCompiler
from agent.etl_pipeline.transformation_ir import (
    ActionFamily,
    BinaryOp,
    BinaryOperator,
    ColumnDropSpec,
    ColumnMetadata,
    ColumnRef,
    ColumnRenameSpec,
    DataType,
    DatasetColumnSchema,
    DatasetSchema,
    DeduplicateExpression,
    DeduplicationTieBreaker,
    DropColumnsExpression,
    DuplicateExportMode,
    ExistingColumnPolicy,
    LiteralValue,
    MaskExpression,
    MaskNullPolicy,
    MaskStrategy,
    NullKeyPolicy,
    NumericFormatSpec,
    OccurrenceIdentityScope,
    ParseExpression,
    ParseOutputMode,
    QuarantineExpression,
    RenameColumnsExpression,
    RoutingIdentityPolicy,
    RoutingMode,
    ShortValuePolicy,
    SortColumnSpec,
    OrderDirection,
    StepAuthorization,
    StepKind,
    TransformationIRGraph,
    TransformationStepIR,
    ValidateExpression,
    ValidateSeverity,
    ValidationUnknownPolicy,
)





def df_to_spark(spark: SparkSession, pdf: pd.DataFrame):
    """Safely loads a pandas DataFrame into Spark via temp JSON without socket worker crashes."""
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False, encoding="utf-8") as f:
        pdf.to_json(f.name, orient="records", lines=True, date_format="iso")
        tmp_path = f.name
    try:
        sdf = spark.read.json(tmp_path).select(*list(pdf.columns))
        sdf.cache()
        _ = sdf.count()
        return sdf
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_increment_32_combined_multi_step_parity(spark_session: SparkSession):
    """
    Executes a complete 6-stage transformation pipeline on both Pandas reference
    and PySpark compiler, asserting exact dataset parity, quarantine partition,
    validation metrics, and publication boundary enforcement.
    """
    # 1. Realistic input test dataset
    raw_data = {
        "cust_id": ["C100", "C100", "C200", "C300", "C400"],
        "txn_time": [100, 200, 150, 300, 250],
        "raw_amt": ["100.50", "150.75", "-5.00", "999.99", "50.00"],
        "status_code": ["OK", "OK", "FRAUD", "OK", "OK"],
        "ccn": ["1111222233334444", "1111222233334444", "5555666677778888", "9999000011112222", "1234"],
    }
    pdf = pd.DataFrame(raw_data)
    sdf = df_to_spark(spark_session, pdf)

    # 2. Define IR steps
    # Step 1: Parse raw_amt string to float
    step_parse = TransformationStepIR(
        step_id="step_1_parse",
        family=ActionFamily.PARSE,
        expression=ParseExpression(
            source_column=ColumnRef(column_name="raw_amt", dataset_name="primary"),
            target_column="amt",
            output_mode=ParseOutputMode.CREATE_PARSED_COLUMN,
            target_data_type=DataType.FLOAT,
            numeric_spec=NumericFormatSpec(),
        ),
        dataset_name="primary",
        kind=StepKind.ROW_TRANSFORMATION,
    )

    # Step 2: Validate amt > 0 (WARN severity, non-mutating)
    step_val = TransformationStepIR(
        step_id="step_2_validate",
        family=ActionFamily.VALIDATE,
        expression=ValidateExpression(
            predicate=BinaryOp(
                left=ColumnRef(column_name="amt", dataset_name="primary"),
                op=BinaryOperator.GT,
                right=LiteralValue(value=0.0, data_type=DataType.FLOAT),
            ),
            severity=ValidateSeverity.WARNING,
            unknown_policy=ValidationUnknownPolicy.COUNT_AS_VIOLATION,
            reason_code="NON_POSITIVE_AMOUNT",
        ),
        dataset_name="primary",
        kind=StepKind.ASSERTION,
    )

    # Step 3: Quarantine rows where status_code == 'FRAUD' into 'quarantine_txns'
    step_quarantine = TransformationStepIR(
        step_id="step_3_quarantine",
        family=ActionFamily.QUARANTINE,
        expression=QuarantineExpression(
            predicate=BinaryOp(
                left=ColumnRef(column_name="status_code", dataset_name="primary"),
                op=BinaryOperator.EQ,
                right=LiteralValue(value="FRAUD", data_type=DataType.STRING),
            ),
            routing_mode=RoutingMode.PARTITION,
            curated_output_ref="curated_txns",
            quarantine_output_ref="quarantine_txns",
            reason_code="FRAUD",
        ),
        dataset_name="primary",
        kind=StepKind.QUARANTINE_ROUTING,
    )

    # Step 4: Deduplicate cust_id, preserve latest (descending txn_time)
    step_dedup = TransformationStepIR(
        step_id="step_4_dedup",
        family=ActionFamily.DEDUPLICATE,
        expression=DeduplicateExpression(
            key_columns=("cust_id",),
            order_by=(SortColumnSpec(column="txn_time", direction=OrderDirection.DESCENDING),),
            tie_breaker=DeduplicationTieBreaker.PRESERVE_FIRST,
            null_key_policy=NullKeyPolicy.GROUP_NULLS_TOGETHER,
            duplicate_export_mode=DuplicateExportMode.REMOVED_OCCURRENCES_ONLY,
            duplicates_output_ref="duplicate_txns",
        ),
        dataset_name="primary",
        kind=StepKind.DATASET_FILTER,
    )

    # Step 5: Mask CCN (preserve last 4 with '*')
    step_mask = TransformationStepIR(
        step_id="step_5_mask",
        family=ActionFamily.MASK,
        expression=MaskExpression(
            source_column="ccn",
            target_column="ccn_masked",
            strategy=MaskStrategy.PRESERVE_LAST_N,
            mask_character="*",
            preserved_count=4,
            short_value_policy=ShortValuePolicy.KEEP_ORIGINAL,
            null_policy=MaskNullPolicy.PRESERVE_NULL,
        ),
        dataset_name="primary",
        kind=StepKind.ROW_TRANSFORMATION,
    )

    # Step 6: Schema Mutation: Drop raw_amt and ccn (authorized)
    valid_auth = StepAuthorization(
        step_id="step_6_schema_clean",
        policy_decision_id="POL-E2E-DROP",
        approved_by_subject_id="admin",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=2),
        authorized_step_hash="",
    )
    step_schema = TransformationStepIR(
        step_id="step_6_schema_clean",
        family=ActionFamily.SCHEMA_MUTATION,
        expression=DropColumnsExpression(
            drops=(ColumnDropSpec(column="raw_amt"), ColumnDropSpec(column="ccn")),
        ),
        authorization=valid_auth,
        dataset_name="primary",
        kind=StepKind.SCHEMA_MUTATION,
    )
    step_schema.authorization.authorized_step_hash = step_schema.semantic_hash

    # 3. Assemble Graph with RoutingIdentityPolicy
    graph = TransformationIRGraph(
        graph_id="e2e_inc_32_pipeline",
        steps=[step_parse, step_val, step_quarantine, step_dedup, step_mask, step_schema],
        routing_identity_policy=RoutingIdentityPolicy(
            scope=OccurrenceIdentityScope.EXECUTION_LOCAL,
            internal_column="_dhara_row_id",
            retain_in_primary_output=False,
            retain_in_secondary_outputs=False,
        ),
        primary_output_ref="primary",
    )

    # 4. Execute on Pandas Reference Compiler
    pd_compiler = PandasIRCompiler()
    pd_res = pd_compiler.execute(graph, pdf)

    # 5. Execute on Native PySpark IR Compiler
    py_compiler = PySparkIRCompiler()
    py_comp = py_compiler.compile(graph)
    assert py_comp.is_success, py_comp.diagnostics
    py_res = py_compiler.execute(graph, sdf)

    # 6. Verify Datasets Parity
    # Outputs produced: primary, curated_txns, quarantine_txns, duplicate_txns
    assert set(py_res.outputs.keys()) == set(pd_res.outputs.keys()) == {"primary", "curated_txns", "quarantine_txns", "duplicate_txns"}

    # Verify Primary Output
    pd_primary = pd_res.outputs["primary"].sort_values("cust_id").reset_index(drop=True)
    py_primary = py_res.outputs["primary"].toPandas().sort_values("cust_id").reset_index(drop=True)

    # Columns must match and technical identities must be stripped
    assert list(py_primary.columns) == list(pd_primary.columns)
    assert "_dhara_row_id" not in py_primary.columns
    assert "raw_amt" not in py_primary.columns
    assert "ccn" not in py_primary.columns

    # Check primary survivor count:
    # 5 input rows - 1 quarantined (C200) - 1 duplicate removed (C100 older) = 3 primary rows (C100 txn 200, C300, C400)
    assert len(py_primary) == len(pd_primary) == 3
    assert py_primary["cust_id"].tolist() == ["C100", "C300", "C400"]
    assert py_primary["ccn_masked"].tolist() == ["************4444", "************2222", "1234"]
    assert py_primary["amt"].tolist() == [150.75, 999.99, 50.00]

    # Verify Quarantine Output
    pd_quar = pd_res.outputs["quarantine_txns"].reset_index(drop=True)
    py_quar = py_res.outputs["quarantine_txns"].toPandas().reset_index(drop=True)
    assert len(py_quar) == len(pd_quar) == 1
    assert py_quar["cust_id"].iloc[0] == "C200"
    assert "_dhara_row_id" not in py_quar.columns

    # Verify Duplicate Output
    pd_dup = pd_res.outputs["duplicate_txns"].reset_index(drop=True)
    py_dup = py_res.outputs["duplicate_txns"].toPandas().reset_index(drop=True)
    assert len(py_dup) == len(pd_dup) == 1
    assert py_dup["cust_id"].iloc[0] == "C100"
    assert py_dup["txn_time"].iloc[0] == 100
    assert "_dhara_row_id" not in py_dup.columns

    # Verify Validation Summaries Parity
    py_val_sum = py_res.validation_summaries["step_2_validate"]
    pd_val_sum = pd_res.validation_summaries["step_2_validate"]
    assert py_val_sum["candidate_count"] == pd_val_sum["candidate_count"] == 5
    assert py_val_sum["pass_count"] == pd_val_sum["pass_count"] == 4
    assert py_val_sum["violation_count"] == pd_val_sum["violation_count"] == 1  # -5.00
    assert py_val_sum["gate_outcome"] == pd_val_sum["gate_outcome"] == "WARN"
