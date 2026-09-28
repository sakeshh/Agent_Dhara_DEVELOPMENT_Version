from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional, Type
from agent.compiler.base.validation import BaseValidationAgent
from agent.compiler.execution_validation.cost_estimator import RuntimeCostEstimator
from agent.compiler.execution_validation.explain_generator import ExplainPlanGenerator
from agent.compiler.execution_validation.models import CostEstimate, ExecutionValidationResult
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.validation import DiagnosticSeverity, ValidationResult
from agent.compiler.validators.adf_validator import ADFValidator
from agent.compiler.validators.databricks_validator import DatabricksValidator
from agent.compiler.validators.fabric_validator import FabricValidator
from agent.compiler.validators.python_validator import PythonValidator
from agent.compiler.validators.spark_validator import SparkValidator
from agent.compiler.validators.sql_validator import SQLValidator

logger = logging.getLogger("agent.compiler.execution_validation")


class ExecutionValidator:
    """
    Phase 7 & 7.5 Execution Validation Layer for Agent Dhara.
    Orchestrates:
    1. Syntax & dialect validation via target-specific validator
    2. Missing reference / schema integrity checks
    3. Simulated EXPLAIN plan generation (SHOWPLAN / df.explain() / ADF DAG)
    4. Deterministic Cost Estimation (shuffle operations, memory, broadcast recommendations, runtime)
    """

    _VALIDATOR_MAP: Dict[ExecutionEngine, Type[BaseValidationAgent]] = {
        ExecutionEngine.AZURE_SQL: SQLValidator,
        ExecutionEngine.PYSPARK: SparkValidator,
        ExecutionEngine.DATABRICKS: DatabricksValidator,
        ExecutionEngine.ADF: ADFValidator,
        ExecutionEngine.FABRIC: FabricValidator,
        ExecutionEngine.PYTHON: PythonValidator,
    }

    @classmethod
    def validate(cls, context: CompilerContext) -> ExecutionValidationResult:
        start_time = time.perf_counter()
        target_engine = context.target_engine
        artifact = context.code_artifact

        errors: List[str] = []
        warnings: List[str] = []

        if not artifact:
            return ExecutionValidationResult(
                valid=False,
                target_engine=target_engine.value,
                errors=["No code artifact present in CompilerContext to validate."],
                validation_latency_ms=round((time.perf_counter() - start_time) * 1000, 2),
            )

        # 1. Target-Specific Syntax & Structure Validation
        validator_cls = cls._VALIDATOR_MAP.get(target_engine, SQLValidator)
        try:
            validator_instance = validator_cls()
            val_result: ValidationResult = validator_instance.validate(artifact, context)
            if not val_result.is_valid:
                for issue in val_result.issues:
                    if issue.severity in (DiagnosticSeverity.ERROR, DiagnosticSeverity.FATAL):
                        errors.append(f"[{issue.issue_code}] {issue.message}")
                    else:
                        warnings.append(f"[{issue.issue_code}] {issue.message}")
        except Exception as e:
            logger.warning(f"Engine validator {validator_cls.__name__} raised exception: {e}")
            warnings.append(f"Engine validation warning: {str(e)}")

        # 2. Semantic Integrity Checks (References, Joins, Tables)
        cls._validate_schema_references(context, artifact, errors, warnings)

        # 3. Generate EXPLAIN Plan
        try:
            explain_plan = ExplainPlanGenerator.generate(context)
        except Exception as e:
            logger.warning(f"Explain plan generation error: {e}")
            explain_plan = f"-- Explain plan generation unavailable: {e}"

        # 4. Generate Cost Estimation
        try:
            cost_estimate = RuntimeCostEstimator.estimate(context)
        except Exception as e:
            logger.warning(f"Cost estimation error: {e}")
            cost_estimate = CostEstimate(
                estimated_cost="unknown",
                warnings=[f"Cost estimator error: {e}"],
            )

        # 5. Extract Recommendations & Optimization Suggestions
        recommendations: List[str] = list(cost_estimate.recommendations)
        optimization_suggestions: List[str] = cls._generate_optimization_suggestions(context, cost_estimate)

        latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
        is_valid = len(errors) == 0

        # 6. Calculate Execution Confidence Score
        confidence = cls._calculate_confidence_score(is_valid, errors, warnings, cost_estimate)

        result = ExecutionValidationResult(
            valid=is_valid,
            target_engine=target_engine.value,
            explain_type="simulated",
            confidence_score=confidence,
            errors=errors,
            warnings=warnings,
            recommendations=recommendations,
            optimization_suggestions=optimization_suggestions,
            explain_plan=explain_plan,
            cost_estimate=cost_estimate,
            validation_latency_ms=latency_ms,
        )

        # Store in metadata for observability
        context.metadata["execution_validation"] = result.model_dump()
        artifact.metadata["execution_validation"] = result.model_dump()

        return result

    @classmethod
    def _generate_optimization_suggestions(
        cls,
        context: CompilerContext,
        cost_estimate: CostEstimate,
    ) -> List[str]:
        suggestions: List[str] = []
        if cost_estimate.broadcast_recommended:
            suggestions.append("Consider broadcast join on lookup dimension to eliminate network exchange shuffle.")

        if cost_estimate.shuffle_operations > 2:
            suggestions.append("High shuffle operation count; verify join keys align with existing partitioning.")

        steps = context.logical_plan.steps if context.logical_plan else []
        saw_dedup_or_agg = False
        saw_filter_after = False
        for s in steps:
            act = s.action.value.lower()
            if act in ("deduplicate", "group_by", "aggregate"):
                saw_dedup_or_agg = True
            elif ("filter" in act or s.parameters.get("expression")) and saw_dedup_or_agg:
                saw_filter_after = True

        if saw_filter_after:
            suggestions.append("Push predicate filters before deduplication/aggregation to reduce intermediate dataset volume.")

        suggestions.append("Project only required target columns to minimize cluster memory serialization footprint.")
        return suggestions

    @classmethod
    def _calculate_confidence_score(
        cls,
        is_valid: bool,
        errors: List[str],
        warnings: List[str],
        cost_estimate: CostEstimate,
    ) -> float:
        if not is_valid:
            score = max(0.1, 1.0 - (0.25 * len(errors)))
        else:
            score = 1.0

        # Penalty for warnings
        score -= min(0.2, 0.04 * len(warnings))

        # Penalty for high complexity/shuffle
        if cost_estimate.estimated_cost == "high":
            score -= 0.06
        elif cost_estimate.estimated_cost == "medium":
            score -= 0.02

        return round(max(0.0, min(1.0, score)), 2)

    @classmethod
    def _validate_schema_references(
        cls,
        context: CompilerContext,
        artifact: CodeArtifact,
        errors: List[str],
        warnings: List[str],
    ) -> None:
        """Validates that tables and critical columns referenced in the plan exist in profiles."""
        known_tables = {k.lower() for k in context.dataset_profiles.keys()}
        known_columns: Dict[str, set] = {}
        for t, p in context.dataset_profiles.items():
            if isinstance(p.columns, dict):
                known_columns[t.lower()] = {str(c).lower() for c in p.columns.keys()}
            elif isinstance(p.columns, (list, set, tuple)):
                known_columns[t.lower()] = {
                    getattr(c, "name", str(c)).lower() for c in p.columns
                }
            else:
                known_columns[t.lower()] = set()

        steps = context.logical_plan.steps if context.logical_plan else []
        for step in steps:
            # Check target columns
            for col in step.target_columns:
                col_lower = col.lower()
                # Check if column belongs to any table
                found = any(col_lower in cols for cols in known_columns.values())
                if not found and known_columns:
                    warnings.append(
                        f"Step '{step.step_id}' references column '{col}' not found in registered schema profiles."
                    )

            # Check joins
            if "join" in step.action.value.lower():
                join_table = step.parameters.get("right_dataset") or step.parameters.get("table")
                if join_table and join_table.lower() not in known_tables and known_tables:
                    errors.append(
                        f"Join operation in step '{step.step_id}' references unknown table '{join_table}'."
                    )
