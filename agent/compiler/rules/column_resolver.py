from __future__ import annotations

import re
from typing import Dict, List, Optional, Set
from agent.compiler.models.profile import DatasetProfile


class ColumnResolver:
    """
    Deterministic schema-aware column resolution.
    Resolves natural language or raw column identifiers to exact schema column names:
      1. Exact match (case-sensitive)
      2. Case-insensitive exact match
      3. Normalized match (ignoring spaces, underscores, hyphens)
      4. Unique prefix/word boundary match
    Returns None if ambiguous (multiple candidates match) or not found.
    """

    @staticmethod
    def normalize_token(token: str) -> str:
        return re.sub(r"[^a-zA-Z0-9]", "", token).lower()

    @classmethod
    def resolve_column(
        cls, candidate_token: str, available_columns: List[str]
    ) -> Optional[str]:
        if not candidate_token:
            return None

        cand_raw = candidate_token.strip().replace("[", "").replace("]", "")
        if not cand_raw:
            return None

        if not available_columns:
            return cand_raw

        # 1. Exact match (case-sensitive)
        if cand_raw in available_columns:
            return cand_raw

        # 2. Case-insensitive match
        cand_lower = cand_raw.lower()
        lower_matches = [c for c in available_columns if c.lower() == cand_lower]
        if len(lower_matches) == 1:
            return lower_matches[0]

        # 3. Normalized match (strip underscores, spaces, hyphens)
        norm_cand = cls.normalize_token(cand_raw)
        norm_matches = [c for c in available_columns if cls.normalize_token(c) == norm_cand]
        if len(norm_matches) == 1:
            return norm_matches[0]

        # 4. Conservative word-boundary alias check
        # e.g., "employee code" -> "EmployeeCode"
        cand_words = set(re.findall(r"[a-zA-Z0-9]+", cand_raw.lower()))
        if cand_words:
            scored = []
            for col in available_columns:
                col_words = set(re.findall(r"[A-Z]?[a-z]+|[0-9]+", col.lower()))
                if cand_words == col_words:
                    scored.append(col)
            if len(scored) == 1:
                return scored[0]

        # Multiple matches = Ambiguous -> return None
        # Zero matches = Not found -> return None
        return None

    @classmethod
    def resolve_column_for_dataset(
        cls, candidate_token: str, dataset_name: str, profiles: Dict[str, DatasetProfile]
    ) -> Optional[str]:
        profile = profiles.get(dataset_name)
        if not profile:
            return None
        return cls.resolve_column(candidate_token, list(profile.columns.keys()))
