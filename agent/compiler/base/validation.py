from __future__ import annotations

from abc import abstractmethod

from agent.compiler.base.agent import BaseAgent
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.validation import ValidationResult


class BaseValidationAgent(BaseAgent):
    """Abstract base class for dialect and security validation agents."""

    @abstractmethod
    def validate(self, artifact: CodeArtifact, context: CompilerContext) -> ValidationResult:
        """Validates code artifact for syntax, security, anti-patterns, and invariants."""
        pass

    def execute(self, context: CompilerContext) -> CompilerContext:
        if not context.code_artifact:
            raise ValueError(f"Cannot run {self.__class__.__name__} without a CodeArtifact in CompilerContext.")
        
        result = self.validate(context.code_artifact, context)
        context.validation_result = result
        return context
