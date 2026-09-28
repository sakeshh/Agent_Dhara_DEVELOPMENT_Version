from __future__ import annotations

from typing import List, Set

# Penalty keywords for non-key attributes
DATE_KEYWORDS: Set[str] = {
    "date", "timestamp", "created_at", "updated_at", "dob", "birth", "hire", "time", "year", "month", "day"
}
STATUS_KEYWORDS: Set[str] = {
    "status", "state", "flag", "active", "is_", "has_", "indicator", "stage", "phase"
}
MEASURE_KEYWORDS: Set[str] = {
    "amount", "price", "cost", "total", "qty", "quantity", "salary", "balance", "fee", "tax", "score", "rate"
}
FREE_TEXT_KEYWORDS: Set[str] = {
    "notes", "comment", "description", "memo", "text", "body", "summary", "address", "details"
}


class KeyPolicy:
    """Safeguards preventing unsafe columns from being used as business keys."""

    @staticmethod
    def is_penalized_column(col_name: str) -> tuple[bool, List[str]]:
        col_lower = col_name.lower().strip()
        reasons = []

        if any(k in col_lower for k in DATE_KEYWORDS):
            reasons.append(f"Column '{col_name}' contains date/time attributes (mutable)")
        if any(k in col_lower for k in STATUS_KEYWORDS):
            reasons.append(f"Column '{col_name}' contains mutable status/flag attributes")
        if any(k in col_lower for k in MEASURE_KEYWORDS):
            reasons.append(f"Column '{col_name}' contains financial/measure values")
        if any(k in col_lower for k in FREE_TEXT_KEYWORDS):
            reasons.append(f"Column '{col_name}' contains free-text comments")

        return (len(reasons) > 0, reasons)

    @staticmethod
    def calculate_score(
        col_name: str,
        null_ratio: float,
        uniqueness_ratio: float,
        has_id_naming: bool,
        is_database_pk: bool = False,
    ) -> tuple[float, List[str], List[str]]:
        score = 0.0
        evidence = []
        warnings = []

        if is_database_pk:
            score += 90.0
            evidence.append("Authoritative database primary key constraint")

        if null_ratio == 0.0:
            score += 25.0
            evidence.append("Zero null values (100% complete)")
        elif null_ratio > 0.05:
            score -= 30.0
            warnings.append(f"High null ratio: {null_ratio*100:.1f}%")

        if uniqueness_ratio >= 0.999:
            score += 40.0
            evidence.append(f"High uniqueness ratio: {uniqueness_ratio*100:.2f}%")
        elif uniqueness_ratio >= 0.95:
            score += 25.0
            evidence.append(f"Near-unique ratio: {uniqueness_ratio*100:.2f}%")
        else:
            score -= 20.0
            warnings.append(f"Low uniqueness ratio: {uniqueness_ratio*100:.2f}%")

        if has_id_naming:
            score += 25.0
            evidence.append("Strong identifier naming convention ('_id' or 'id')")

        penalized, pen_reasons = KeyPolicy.is_penalized_column(col_name)
        if penalized:
            score -= 50.0
            warnings.extend(pen_reasons)

        final_confidence = max(0.0, min(100.0, score))
        return final_confidence, evidence, warnings
