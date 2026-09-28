from __future__ import annotations

import re
from typing import List, Optional, Tuple
from agent.compiler.rules.models import DecompositionStatus


class AmbiguityDetector:
    """
    Detects under-specified, ambiguous, or unsupported business rules before execution.
    Prevents the compiler from inventing missing business parameters.
    """

    # Patterns indicating unsupported external / proprietary business logic
    UNSUPPORTED_PATTERNS = [
        r"\bproprietary\b",
        r"\bregional\s+algorithm\b",
        r"\bcustom\s+machine\s+learning\b",
        r"\bneural\s+network\b",
        r"\bexternal\s+api\b",
        r"\bthird[- ]party\b",
        r"\brandom\s+forest\b",
    ]

    # Patterns indicating under-specified rules missing required parameters
    AMBIGUOUS_PATTERNS: List[Tuple[str, str]] = [
        (r"\bshould\s+have\s+a\s+prefix\b(?!\s*['\"])", "Rule requests prefix but does not specify the prefix string."),
        (r"\badd\s+prefix\s+according\s+to\b", "Rule requests dynamic prefixing but lacks the value lookup table."),
        (r"\bconvert\s+\w+\s+to\s+(?:the\s+)?standard\s+values\b", "Rule requests standard value conversion but lacks the target mappings."),
        (r"\bclean\s+up\s+\w+\s+properly\b", "Rule is vague ('clean up properly') without specifying transformation rules."),
        (r"\bnormalize\s+\w+\s+appropriately\b", "Rule requests normalization without specifying expected format."),
        (r"\bmap\s+\w+\s+to\s+valid\s+codes\b(?!\s*:\s*)", "Rule requests mapping without providing the dictionary of codes."),
    ]

    @classmethod
    def check(cls, rule_text: str) -> Optional[Tuple[DecompositionStatus, List[str]]]:
        text = rule_text.strip().lower()

        # 1. Check for clearly unsupported external algorithms
        for pat in cls.UNSUPPORTED_PATTERNS:
            if re.search(pat, text):
                return (
                    DecompositionStatus.UNSUPPORTED,
                    [f"Rule references external or proprietary algorithm '{pat}' which cannot be represented as deterministic operators."],
                )

        # 2. Check for missing required business parameters
        for pat, reason in cls.AMBIGUOUS_PATTERNS:
            if re.search(pat, text):
                return (
                    DecompositionStatus.AMBIGUOUS,
                    [reason],
                )

        return None
