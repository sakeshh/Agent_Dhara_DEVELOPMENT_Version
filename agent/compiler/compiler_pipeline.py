from __future__ import annotations

import logging
from typing import Dict, Optional, Type

from agent.compiler.base.agent import BaseAgent
from agent.compiler.base.validation import BaseValidationAgent
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.optimizers.optimization_agent import OptimizationAgent
from agent.compiler.physical.physical_planner import PhysicalPlanner
from agent.compiler.planners.logical_planner import LogicalPlanner
from agent.compiler.router.language_router import LanguageRouter
from agent.compiler.snapshot_store import CompilerSnapshot, SnapshotStore
from agent.compiler.validators.python_validator import PythonValidator
from agent.compiler.validators.sql_validator import SQLValidator
from agent.compiler.validators.spark_validator import SparkValidator
from agent.compiler.validators.fabric_validator import FabricValidator
from agent.compiler.validators.adf_validator import ADFValidator
from agent.compiler.validators.databricks_validator import DatabricksValidator

from agent.compiler.contracts.contract_agent import ContractAgent
from agent.compiler.lineage.lineage_agent import LineageAgent
from agent.compiler.manifests.artifact_manifest import ArtifactManifestBuilder, DeploymentPackage
from agent.compiler.metrics.compiler_metrics import CompilerMetricsCollector
from agent.compiler.patterns.pattern_library import PatternLibrary
from agent.compiler.planners.cost_estimator import CostEstimator
from agent.compiler.repair.repair_agent import RepairAgent
from agent.compiler.testing.test_generator import TestGenerator

logger = logging.getLogger("agent.compiler.pipeline")


class CompilerPipeline:
    """Master orchestrator for the Agent Dhara Compiler Lifecycle."""

    def __init__(
        self,
        logical_planner: Optional[LogicalPlanner] = None,
        optimization_agent: Optional[OptimizationAgent] = None,
        cost_estimator: Optional[CostEstimator] = None,
        physical_planner: Optional[PhysicalPlanner] = None,
        language_router: Optional[LanguageRouter] = None,
        pattern_library: Optional[PatternLibrary] = None,
        snapshot_store: Optional[SnapshotStore] = None,
        repair_agent: Optional[RepairAgent] = None,
        test_generator: Optional[TestGenerator] = None,
        contract_agent: Optional[ContractAgent] = None,
        lineage_agent: Optional[LineageAgent] = None,
        manifest_builder: Optional[ArtifactManifestBuilder] = None,
        metrics_collector: Optional[CompilerMetricsCollector] = None,
    ):
        self.pattern_library = pattern_library or PatternLibrary()
        self.logical_planner = logical_planner or LogicalPlanner()
        self.optimization_agent = optimization_agent or OptimizationAgent()
        self.cost_estimator = cost_estimator or CostEstimator()
        self.physical_planner = physical_planner or PhysicalPlanner()
        self.language_router = language_router or LanguageRouter.create_default()
        self.snapshot_store = snapshot_store or SnapshotStore.get_instance()
        self.repair_agent = repair_agent or RepairAgent()
        self.test_generator = test_generator or TestGenerator()
        self.contract_agent = contract_agent or ContractAgent()
        self.lineage_agent = lineage_agent or LineageAgent()
        self.manifest_builder = manifest_builder or ArtifactManifestBuilder()
        self.metrics_collector = metrics_collector or CompilerMetricsCollector()
        self._validators: Dict[ExecutionEngine, BaseValidationAgent] = {
            ExecutionEngine.PYTHON: PythonValidator(),
            ExecutionEngine.AZURE_SQL: SQLValidator(),
            ExecutionEngine.PYSPARK: SparkValidator(),
            ExecutionEngine.FABRIC: FabricValidator(),
            ExecutionEngine.ADF: ADFValidator(),
            ExecutionEngine.DATABRICKS: DatabricksValidator(),
        }

    def register_validator(self, engine: ExecutionEngine, validator: BaseValidationAgent) -> None:
        self._validators[engine] = validator

    def compile(self, context: CompilerContext) -> tuple[CompilerContext, CompilerSnapshot]:
        """Runs the compilation lifecycle synchronously."""
        logger.info(f"Starting compiler pipeline for session '{context.session_id}' [target: {context.target_engine.value}]")
        logger.info("[CompilerPipeline] generation_path=deterministic_compiler")

        # 1. Enterprise Domain Pattern Matching
        context, _ = self.pattern_library.run_with_metrics(context)

        # 2. Logical Planning (Issue Compilation + Priority Topological Ordering + Confidence)
        context, _ = self.logical_planner.run_with_metrics(context)

        # 2b. Universal Output Contract Resolution & Strict Lifecycle Validation
        from agent.compiler.schema.schema_resolver import SchemaResolver
        from agent.compiler.schema.schema_validator import SchemaValidator
        contracts = SchemaResolver.resolve_all_contracts(context)
        for ds_name, contract in contracts.items():
            valid, schema_errs = SchemaValidator.validate_contract(contract)
            if not valid:
                logger.error(f"Output schema validation failed for '{ds_name}': {schema_errs}")
                raise ValueError(f"Schema Contract Violation for '{ds_name}': {'; '.join(schema_errs)}")
        context.metadata["output_contracts"] = {k: v.model_dump() for k, v in contracts.items()}

        # 3. AST Optimization (Operator Fusion + Redundancy Removal)
        context, _ = self.optimization_agent.run_with_metrics(context)

        # 4. Computational & Cloud Resource Cost Estimation
        context, _ = self.cost_estimator.run_with_metrics(context)

        # 5. Physical Planning (Engine Strategy Resolution)
        context, _ = self.physical_planner.run_with_metrics(context)

        # 6. Language Routing & Code Generation
        context, _ = self.language_router.run_with_metrics(context)

        # 7. Target Validation & Closed-Loop Auto-Repair
        validator = self._validators.get(context.target_engine)
        if validator and context.code_artifact:
            context, _ = validator.run_with_metrics(context)
            if context.validation_result and not context.validation_result.is_valid:
                logger.info(f"Validation reported {context.validation_result.error_count} error(s). Triggering Auto-Repair agent...")
                context = self.repair_agent.repair(context, validator)
            if context.validation_result and context.validation_result.has_blocking_errors:
                from agent.errors import CompilationBlockedError
                from agent.compiler.models.validation import DiagnosticSeverity
                blocking_msgs = [i.message for i in context.validation_result.issues if i.severity == DiagnosticSeverity.BLOCKING]
                raise CompilationBlockedError(
                    f"Execution blocked by compiler validation: {'; '.join(blocking_msgs)}"
                )

        # 8. Companion Verification Test Suite Generation
        context, _ = self.test_generator.run_with_metrics(context)

        # 9. Data Contract Generation
        context, _ = self.contract_agent.run_with_metrics(context)

        # 10. Column-level Lineage DAG Generation
        context, _ = self.lineage_agent.run_with_metrics(context)

        # 11. Collect Compiler Execution Metrics
        metrics_report = self.metrics_collector.collect(context)
        context.metadata["compiler_metrics"] = metrics_report.model_dump()

        # 12. Assemble Unified Deployment Package / Artifact Manifest
        deployment_manifest = self.manifest_builder.build_manifest(context)
        context.metadata["deployment_manifest"] = deployment_manifest.model_dump()

        # 13. Auditing Snapshot
        snapshot = self.snapshot_store.create_snapshot(context)
        context.metadata["latest_snapshot_id"] = snapshot.snapshot_id

        logger.info(f"Compilation finished for session '{context.session_id}' (snapshot: {snapshot.snapshot_id})")
        return context, snapshot
