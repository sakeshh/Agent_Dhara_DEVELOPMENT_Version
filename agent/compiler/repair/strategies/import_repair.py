from __future__ import annotations

import re
from typing import Optional

from agent.compiler.base.repair import BaseRepairStrategy
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.validation import DiagnosticIssue, ValidationIssueType


class ImportRepair(BaseRepairStrategy):
    """Repairs missing or broken module imports."""

    REQUIRED_IMPORTS = {
        "PY_MISSING_PANDAS_IMPORT": "import pandas as pd",
        "PY_MISSING_NUMPY_IMPORT": "import numpy as np",
    }

    def can_handle(self, diagnostic: DiagnosticIssue) -> bool:
        return diagnostic.issue_type == ValidationIssueType.IMPORT or "IMPORT" in diagnostic.issue_code

    def patch(self, code: str, diagnostic: DiagnosticIssue, context: CompilerContext) -> Optional[str]:
        if diagnostic.issue_code in self.REQUIRED_IMPORTS:
            needed_import = self.REQUIRED_IMPORTS[diagnostic.issue_code]
            if needed_import not in code:
                # Add to top after __future__ or docstring
                lines = code.splitlines()
                insert_idx = 0
                for i, line in enumerate(lines):
                    if line.startswith("from __future__") or line.startswith('"""') or line.startswith("#"):
                        insert_idx = i + 1
                    else:
                        break
                lines.insert(insert_idx, needed_import)
                return "\n".join(lines)

        return None
