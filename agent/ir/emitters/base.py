from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Optional
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.profile import DatasetProfile
from agent.ir.models import IrPlan


class BaseEmitter(ABC):
    """
    Abstract base class for all compiler target emitters.
    Transforms a single canonical IrPlan into target-specific code artifacts
    through the deterministic CompilerPipeline.
    """

    @abstractmethod
    def emit(
        self,
        ir_plan: IrPlan,
        dataset_profiles: Dict[str, DatasetProfile],
        session_id: str = "default",
    ) -> CodeArtifact:
        pass
