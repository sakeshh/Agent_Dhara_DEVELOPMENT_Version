"""Agent Dhara Compiler Subsystem.

Enterprise AI ETL Compiler Core — Model-driven, optimizing, multi-target code generation engine.
"""
from __future__ import annotations

from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.manifests.artifact_manifest import ArtifactManifestBuilder, DeploymentPackage
from agent.compiler.metrics.compiler_metrics import CompilerMetricsCollector, CompilerMetricsReport
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.patterns.pattern_library import PatternLibrary
from agent.compiler.planners.cost_estimator import CostEstimator
from agent.compiler.snapshot_store import CompilerSnapshot, SnapshotStore

from agent.codegen.adf import ADFCodegenAgent
from agent.codegen.databricks import DatabricksCodegenAgent
from agent.codegen.fabric import FabricCodegenAgent
from agent.codegen.pyspark import PySparkCodegenAgent
from agent.codegen.python import PythonCodegenAgent
from agent.codegen.sql import SQLCodegenAgent

__all__ = [
    "CompilerPipeline",
    "CompilerContext",
    "ExecutionEngine",
    "ArtifactManifestBuilder",
    "DeploymentPackage",
    "CostEstimator",
    "PatternLibrary",
    "CompilerMetricsCollector",
    "CompilerMetricsReport",
    "SnapshotStore",
    "CompilerSnapshot",
    "PythonCodegenAgent",
    "SQLCodegenAgent",
    "PySparkCodegenAgent",
    "FabricCodegenAgent",
    "ADFCodegenAgent",
    "DatabricksCodegenAgent",
]
