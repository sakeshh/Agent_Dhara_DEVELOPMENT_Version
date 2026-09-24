from __future__ import annotations

from abc import abstractmethod

from agent.compiler.base.agent import BaseAgent
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.physical import PhysicalPlan


class BaseCodegenAgent(BaseAgent):
    """Abstract base class for all language-specialized code generation agents."""

    @abstractmethod
    def generate(self, physical_plan: PhysicalPlan, context: CompilerContext) -> CodeArtifact:
        """Synthesizes code from the physical execution plan using templates and instructions."""
        pass

    def execute(self, context: CompilerContext) -> CompilerContext:
        if not context.physical_plan:
            raise ValueError(f"Cannot run {self.__class__.__name__} without a valid PhysicalPlan in CompilerContext.")
        
        artifact = self.generate(context.physical_plan, context)
        context.code_artifact = artifact
        return context
