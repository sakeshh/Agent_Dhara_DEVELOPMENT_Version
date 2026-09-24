from __future__ import annotations

from typing import Dict, Type
from agent.compiler.base.agent import BaseAgent
from agent.compiler.base.codegen import BaseCodegenAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.physical import ExecutionEngine


class UnsupportedEngineError(ValueError):
    pass


class LanguageRouter(BaseAgent):
    """Routes execution to specialized language codegen agents based on target engine."""

    def __init__(self):
        self._registry: Dict[ExecutionEngine, Type[BaseCodegenAgent]] = {}

    def register(self, engine: ExecutionEngine, agent_cls: Type[BaseCodegenAgent]) -> None:
        self._registry[engine] = agent_cls

    @classmethod
    def create_default(cls) -> LanguageRouter:
        router = cls()
        from agent.codegen.python.python_codegen_agent import PythonCodegenAgent
        from agent.codegen.sql.sql_codegen_agent import SQLCodegenAgent
        from agent.codegen.pyspark.pyspark_codegen_agent import PySparkCodegenAgent
        from agent.codegen.fabric.fabric_codegen_agent import FabricCodegenAgent
        from agent.codegen.adf.adf_codegen_agent import ADFCodegenAgent
        from agent.codegen.databricks.databricks_codegen_agent import DatabricksCodegenAgent

        router.register(ExecutionEngine.PYTHON, PythonCodegenAgent)
        router.register(ExecutionEngine.AZURE_SQL, SQLCodegenAgent)
        router.register(ExecutionEngine.PYSPARK, PySparkCodegenAgent)
        router.register(ExecutionEngine.FABRIC, FabricCodegenAgent)
        router.register(ExecutionEngine.ADF, ADFCodegenAgent)
        router.register(ExecutionEngine.DATABRICKS, DatabricksCodegenAgent)
        return router

    def get_agent(self, engine: ExecutionEngine) -> BaseCodegenAgent:
        agent_cls = self._registry.get(engine)
        if not agent_cls:
            raise UnsupportedEngineError(f"No codegen agent registered for engine: '{engine.value}'")
        return agent_cls()

    def execute(self, context: CompilerContext) -> CompilerContext:
        EventBus.get_instance().emit(
            event_type=CompilerEventType.LANGUAGE_ROUTED,
            session_id=context.session_id,
            payload={"target_engine": context.target_engine.value},
            source_agent=self.__class__.__name__,
        )

        agent = self.get_agent(context.target_engine)
        return agent.execute(context)
