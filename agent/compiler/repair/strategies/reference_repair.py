from __future__ import annotations

import difflib
import re
from typing import Optional

from agent.compiler.base.repair import BaseRepairStrategy
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.validation import DiagnosticIssue, ValidationIssueType


class ReferenceRepair(BaseRepairStrategy):
    """Repairs mismatched or misspelled column and table references using metadata context."""

    def can_handle(self, diagnostic: DiagnosticIssue) -> bool:
        return diagnostic.issue_type == ValidationIssueType.REFERENCE or "REFERENCE" in diagnostic.issue_code

    def patch(self, code: str, diagnostic: DiagnosticIssue, context: CompilerContext) -> Optional[str]:
        # Collect all valid column names from dataset profiles
        valid_columns = set()
        for ds in context.dataset_profiles.values():
            valid_columns.update(ds.columns.keys())

        if not valid_columns:
            return None

        # Look for referenced columns in code that closely match a valid column
        # Matches patterns like df["col"] or df['col'] or [col]
        referenced_identifiers = set(re.findall(r'\["([^"]+)"\]|\[\'([^\']+)\'\]|\[([a-zA-Z0-9_]+)\]', code))
        all_refs = {item for sub in referenced_identifiers for item in sub if item}

        patched_code = code
        modified = False

        for ref in all_refs:
            if ref not in valid_columns:
                matches = difflib.get_close_matches(ref, valid_columns, n=1, cutoff=0.7)
                if matches:
                    best_match = matches[0]
                    # Replace reference
                    patched_code = re.sub(rf'\b{re.escape(ref)}\b', best_match, patched_code)
                    modified = True

        return patched_code if modified else None
