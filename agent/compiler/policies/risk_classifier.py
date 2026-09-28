from __future__ import annotations

from typing import Any, Dict, List
from agent.compiler.policies.row_disposition import RiskLevel, RowDisposition


class RiskClassifier:
    """Classifies actions and row dispositions into explicit RiskLevels."""

    @staticmethod
    def classify_action(action: str, disposition: RowDisposition) -> RiskLevel:
        act = action.lower().strip()

        # High risk: row deletions, key imputations, near duplicate merges
        if disposition in (RowDisposition.REMOVE, RowDisposition.MERGE):
            if act in ("deduplicate_exact", "exact_duplicate"):
                return RiskLevel.LOW
            return RiskLevel.HIGH

        if act in ("impute_business_key", "merge_near_duplicates", "infer_foreign_keys"):
            return RiskLevel.HIGH

        # Medium risk: bound clipping, sentinel replacement, deduplication with verified key
        if act in ("range_clip", "clip_outliers", "replace_sentinels", "quarantine_row", "quarantine"):
            return RiskLevel.MEDIUM

        # Low risk: formatting, whitespace, lowercase, parsing
        return RiskLevel.LOW
