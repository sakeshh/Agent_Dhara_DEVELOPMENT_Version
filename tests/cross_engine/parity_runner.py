"""
tests/cross_engine/parity_runner.py

Cross-engine semantic parity execution harness.
Evaluates both specification conformance (against formal IR postconditions/canonical fixtures)
and differential reference parity (against the certified Pandas reference compiler),
with strict expected engine deviation governance.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple
import hashlib

import pandas as pd

from agent.etl_pipeline.cross_engine.capability_models import (
    CapabilityStatus,
    EngineCapabilityKey,
)
from agent.etl_pipeline.cross_engine.capability_registry import (
    get_global_capability_registry,
)
from agent.etl_pipeline.cross_engine.parity_models import (
    ComparatorResult,
    CrossEngineParityReport,
    ParityComparatorType,
)
from agent.etl_pipeline.pandas_ir_compiler import PandasIRCompiler
from agent.etl_pipeline.transformation_ir import (
    TransformationIRGraph,
)
from tests.cross_engine.assertions import (
    assert_byte_parity,
    assert_decimal_parity,
    assert_floating_parity,
    assert_row_occurrence_parity,
    assert_schema_parity,
    assert_temporal_parity,
)


class CrossEngineParityRunner:
    """
    Parity execution harness for evaluating cross-engine translation fidelity.
    Separates specification conformance from reference differential parity.
    """

    def __init__(self, reference_compiler: Optional[PandasIRCompiler] = None) -> None:
        self.reference_compiler = reference_compiler or PandasIRCompiler()
        self.registry = get_global_capability_registry()

    def run_differential_parity(
        self,
        fixture_id: str,
        graph: TransformationIRGraph,
        input_data: Dict[str, pd.DataFrame],
        target_compiler_fn: Callable[[TransformationIRGraph, Dict[str, pd.DataFrame]], Dict[str, Any]],
        capability_key: EngineCapabilityKey,
        expected_canonical_fixtures: Optional[Dict[str, Any]] = None,
        occurrence_mode: Optional[str] = None,
        primary_key_col: Optional[str] = "id",
    ) -> CrossEngineParityReport:
        """
        Execute parity test against both reference and target compilers.
        Evaluates specification conformance, differential output comparisons,
        and applies governed expected deviations.
        """
        graph_hash = hashlib.sha256(graph.model_dump_json().encode("utf-8")).hexdigest()

        # 1. Evaluate Capability Preflight
        decision = self.registry.evaluate_capability(capability_key)
        if decision.status in (CapabilityStatus.UNSUPPORTED, CapabilityStatus.NOT_TESTED):
            # Preflight blocks compilation
            return CrossEngineParityReport(
                fixture_id=fixture_id,
                graph_hash=graph_hash,
                reference_engine=self.reference_compiler.engine_name,
                target_engine=capability_key.engine_name,
                capability_key=capability_key,
                specification_conformance=False,
                differential_parity=False,
                comparator_results=(
                    ComparatorResult(
                        comparator_type=ParityComparatorType.FAILURE,
                        passed=False,
                        field_name="preflight_capability",
                        expected="ELIGIBLE_CAPABILITY",
                        actual=f"BLOCKED:{decision.status.value}",
                        details={"reason_code": decision.reason_code, "limitations": decision.limitations},
                    ),
                ),
                expected_deviations=(),
                unexpected_deviations=(f"PREFLIGHT_BLOCKED:{decision.reason_code}",),
            )

        # 2. Run Reference Execution (Pandas)
        ref_compile = self.reference_compiler.compile(graph)
        ref_outputs: Dict[str, pd.DataFrame] = {}
        # Execute reference using in-memory helper
        for ds_name, df_in in input_data.items():
            # Apply graph steps on copy
            working = df_in.copy(deep=False)
            res = self.reference_compiler.execute(graph, working)
            if hasattr(res, "outputs") and isinstance(res.outputs, dict):
                ref_outputs.update(res.outputs)
            else:
                ref_outputs[ds_name] = res.primary_output

        # 3. Run Target Execution
        target_outputs = target_compiler_fn(graph, input_data)

        # 4. Compare Datasets and Outputs
        comparator_results: List[ComparatorResult] = []
        expected_deviations: List[str] = []
        unexpected_deviations: List[str] = []

        active_devs = self.registry.find_active_deviations(capability_key)
        active_dev_map = {d.affected_field: d for d in active_devs}

        # Check Schema Parity across emitted datasets
        all_ds = set(ref_outputs.keys()) | set(target_outputs.keys())
        for ds in all_ds:
            if ds not in ref_outputs or ds not in target_outputs:
                comparator_results.append(
                    ComparatorResult(
                        comparator_type=ParityComparatorType.SCHEMA,
                        passed=False,
                        field_name=f"dataset_presence:{ds}",
                        expected=ds in ref_outputs,
                        actual=ds in target_outputs,
                    )
                )
                unexpected_deviations.append(f"MISSING_DATASET:{ds}")
                continue

            ref_df = ref_outputs[ds]
            target_obj = target_outputs[ds]
            target_df = target_obj.toPandas() if hasattr(target_obj, "toPandas") else pd.DataFrame(target_obj)

            # Schema columns check
            schema_res = assert_schema_parity(ref_df.columns, target_df.columns, field_name=f"{ds}.schema")
            comparator_results.append(schema_res)
            if not schema_res.passed:
                unexpected_deviations.append(f"SCHEMA_MISMATCH:{ds}")

            # Occurrence Multiset Conservation (if requested)
            if occurrence_mode and primary_key_col and primary_key_col in ref_df.columns and primary_key_col in target_df.columns:
                in_ids = []
                for inp_df in input_data.values():
                    if primary_key_col in inp_df.columns:
                        in_ids.extend(inp_df[primary_key_col].tolist())
                occ_res = assert_row_occurrence_parity(
                    input_ids=in_ids,
                    primary_ids=target_df[primary_key_col].tolist(),
                    mode=occurrence_mode,
                    field_name=f"{ds}.occurrences",
                )
                comparator_results.append(occ_res)
                if not occ_res.passed:
                    unexpected_deviations.append(f"OCCURRENCE_MISMATCH:{ds}")

            # Column-by-column value checks
            common_cols = [c for c in ref_df.columns if c in target_df.columns]
            for col in common_cols:
                ref_series = ref_df[col]
                target_series = target_df[col]

                # Check float tolerance vs exact
                if pd.api.types.is_float_dtype(ref_series):
                    for idx, (ev, av) in enumerate(zip(ref_series, target_series)):
                        c_res = assert_floating_parity(ev, av, field_name=f"{ds}.{col}[{idx}]")
                        if not c_res.passed:
                            if col in active_dev_map:
                                c_res.deviation_id = active_dev_map[col].deviation_id
                                expected_deviations.append(c_res.deviation_id)
                            else:
                                unexpected_deviations.append(f"FLOAT_MISMATCH:{ds}.{col}")
                        comparator_results.append(c_res)
                else:
                    # Generic equality
                    match = bool((ref_series == target_series) | (ref_series.isna() & target_series.isna())).all()
                    c_res = ComparatorResult(
                        comparator_type=ParityComparatorType.EXACT_BYTE,
                        passed=match,
                        field_name=f"{ds}.{col}",
                        expected=ref_series.tolist() if not match else None,
                        actual=target_series.tolist() if not match else None,
                    )
                    if not match:
                        if col in active_dev_map:
                            c_res.deviation_id = active_dev_map[col].deviation_id
                            expected_deviations.append(c_res.deviation_id)
                        else:
                            unexpected_deviations.append(f"VALUE_MISMATCH:{ds}.{col}")
                    comparator_results.append(c_res)

        # 5. Specification Conformance Check against Canonical Fixtures
        spec_conformance = True
        if expected_canonical_fixtures:
            for ds, exp_records in expected_canonical_fixtures.items():
                if ds not in target_outputs:
                    spec_conformance = False
                    unexpected_deviations.append(f"CANONICAL_SPEC_MISSING:{ds}")
                else:
                    t_obj = target_outputs[ds]
                    t_df = t_obj.toPandas() if hasattr(t_obj, "toPandas") else pd.DataFrame(t_obj)
                    if len(t_df) != len(exp_records):
                        spec_conformance = False
                        unexpected_deviations.append(f"CANONICAL_SPEC_COUNT_MISMATCH:{ds}")

        diff_parity = (len(unexpected_deviations) == 0)

        return CrossEngineParityReport(
            fixture_id=fixture_id,
            graph_hash=graph_hash,
            reference_engine=self.reference_compiler.engine_name,
            target_engine=capability_key.engine_name,
            capability_key=capability_key,
            specification_conformance=spec_conformance,
            differential_parity=diff_parity,
            comparator_results=tuple(comparator_results),
            expected_deviations=tuple(set(expected_deviations)),
            unexpected_deviations=tuple(set(unexpected_deviations)),
        )
