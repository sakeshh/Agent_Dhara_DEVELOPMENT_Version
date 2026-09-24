from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from agent.compiler.models.context import CompilerContext
from agent.compiler.models.validation import DiagnosticIssue


class BaseRepairStrategy(ABC):
    """Abstract base class for modular self-healing repair strategies."""

    @abstractmethod
    def can_handle(self, diagnostic: DiagnosticIssue) -> bool:
        """Returns True if this repair strategy is capable of resolving the diagnostic issue."""
        pass

    @abstractmethod
    def patch(self, code: str, diagnostic: DiagnosticIssue, context: CompilerContext) -> Optional[str]:
        """Applies a targeted fix to the code string. Returns patched code or None if patch failed."""
        pass
