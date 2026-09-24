from __future__ import annotations

from typing import List, Optional

from agent.compiler.base.repair import BaseRepairStrategy
from agent.compiler.models.validation import DiagnosticIssue
from agent.compiler.repair.strategies.dialect_repair import DialectRepair
from agent.compiler.repair.strategies.import_repair import ImportRepair
from agent.compiler.repair.strategies.reference_repair import ReferenceRepair
from agent.compiler.repair.strategies.syntax_repair import SyntaxRepair


class RepairClassifier:
    """Classifies validation diagnostics and resolves the best self-healing repair strategy."""

    def __init__(self, strategies: Optional[List[BaseRepairStrategy]] = None):
        self.strategies: List[BaseRepairStrategy] = strategies or [
            SyntaxRepair(),
            ImportRepair(),
            ReferenceRepair(),
            DialectRepair(),
        ]

    def classify(self, diagnostic: DiagnosticIssue) -> Optional[BaseRepairStrategy]:
        for strategy in self.strategies:
            if strategy.can_handle(diagnostic):
                return strategy
        return None
