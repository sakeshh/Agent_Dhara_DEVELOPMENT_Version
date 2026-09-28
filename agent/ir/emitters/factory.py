from __future__ import annotations

import logging
from typing import Dict, List, Optional, Union
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.profile import DatasetProfile
from agent.ir.emitters.base import BaseEmitter
from agent.ir.emitters.emitters import (
    AdfEmitter,
    DatabricksEmitter,
    FabricEmitter,
    PySparkEmitter,
    PythonEmitter,
    SqlEmitter,
)
from agent.ir.emitters.models import CompilerTarget
from agent.ir.models import IrPlan

logger = logging.getLogger("agent.ir.emitters.factory")


class EmitterFactory:
    """
    Factory for instantiating and orchestrating compiler emitters.
    Consumes a single canonical IrPlan to emit any supported target:
    SQL, PySpark, Databricks, ADF, Fabric Notebook, or Python.
    """

    _EMITTER_MAP: Dict[CompilerTarget, BaseEmitter] = {
        CompilerTarget.SQL: SqlEmitter(),
        CompilerTarget.SYNAPSE: SqlEmitter(),
        CompilerTarget.PYSPARK: PySparkEmitter(),
        CompilerTarget.DATABRICKS: DatabricksEmitter(),
        CompilerTarget.ADF: AdfEmitter(),
        CompilerTarget.FABRIC: FabricEmitter(),
        CompilerTarget.PYTHON: PythonEmitter(),
    }

    @classmethod
    def get_emitter(cls, target: Union[CompilerTarget, str]) -> BaseEmitter:
        if isinstance(target, str):
            try:
                target = CompilerTarget(target.lower())
            except ValueError:
                target = CompilerTarget.SQL

        emitter = cls._EMITTER_MAP.get(target)
        if not emitter:
            logger.warning(f"Unknown compiler target '{target}'; defaulting to SQL emitter.")
            return cls._EMITTER_MAP[CompilerTarget.SQL]
        return emitter

    @classmethod
    def emit(
        cls,
        target: Union[CompilerTarget, str],
        ir_plan: IrPlan,
        dataset_profiles: Dict[str, DatasetProfile],
        session_id: str = "default",
    ) -> CodeArtifact:
        emitter = cls.get_emitter(target)
        return emitter.emit(ir_plan, dataset_profiles, session_id=session_id)

    @classmethod
    def emit_all(
        cls,
        targets: List[Union[CompilerTarget, str]],
        ir_plan: IrPlan,
        dataset_profiles: Dict[str, DatasetProfile],
        session_id: str = "default",
    ) -> Dict[str, CodeArtifact]:
        """Emits multiple language/engine artifacts from a single canonical IrPlan."""
        results: Dict[str, CodeArtifact] = {}
        for tgt in targets:
            tgt_key = tgt.value if isinstance(tgt, CompilerTarget) else str(tgt)
            results[tgt_key] = cls.emit(tgt, ir_plan, dataset_profiles, session_id=session_id)
        return results
