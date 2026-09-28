from __future__ import annotations

import logging
from typing import Dict, Optional
from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import DatasetProfile
from agent.ir.builder import IrBuilder
from agent.ir.emitters.base import BaseEmitter
from agent.ir.models import IrPlan

logger = logging.getLogger("agent.ir.emitters")


class _GenericEngineEmitter(BaseEmitter):
    def __init__(self, engine: ExecutionEngine) -> None:
        self.engine = engine

    def emit(
        self,
        ir_plan: IrPlan,
        dataset_profiles: Dict[str, DatasetProfile],
        session_id: str = "default",
    ) -> CodeArtifact:
        compiler_ctx = IrBuilder.build(
            ir_plan=ir_plan,
            session_id=session_id,
            target_engine=self.engine,
            existing_profiles=dataset_profiles,
        )
        pipeline = CompilerPipeline()
        compiled_ctx, _ = pipeline.compile(compiler_ctx)

        from agent.compiler.execution_validation import ExecutionValidator

        if not compiled_ctx.code_artifact:
            raise RuntimeError(f"CompilerPipeline failed to produce an artifact for engine: {self.engine.value}")

        # Phase 7 & 7.5: Pre-execution validation, EXPLAIN plan generation, and cost estimation
        ExecutionValidator.validate(compiled_ctx)

        return compiled_ctx.code_artifact


class SqlEmitter(_GenericEngineEmitter):
    def __init__(self) -> None:
        super().__init__(ExecutionEngine.AZURE_SQL)


class PySparkEmitter(_GenericEngineEmitter):
    def __init__(self) -> None:
        super().__init__(ExecutionEngine.PYSPARK)


class DatabricksEmitter(_GenericEngineEmitter):
    def __init__(self) -> None:
        super().__init__(ExecutionEngine.DATABRICKS)


class AdfEmitter(_GenericEngineEmitter):
    def __init__(self) -> None:
        super().__init__(ExecutionEngine.ADF)


class FabricEmitter(_GenericEngineEmitter):
    def __init__(self) -> None:
        super().__init__(ExecutionEngine.FABRIC)


class PythonEmitter(_GenericEngineEmitter):
    def __init__(self) -> None:
        super().__init__(ExecutionEngine.PYTHON)
