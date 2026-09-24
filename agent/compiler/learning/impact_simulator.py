"""Impact Simulator: Simulates cleaning impact on dataset before code execution."""

import random
from typing import Dict, List, Optional
from agent.compiler.learning.models import ImpactPreviewRequest, ImpactPreviewResponse


class ImpactSimulator:
    """Predicts data transformation impact, volume of row changes, and overall risk rating."""

    def simulate_impact(self, request: ImpactPreviewRequest) -> ImpactPreviewResponse:
        total = max(100, request.total_rows)
        strategies = [s.lower() for s in request.planned_strategies]

        has_dedup = any("dedup" in s for s in strategies)
        has_email = any("email" in s for s in strategies)
        has_null = any("null" in s or "impute" in s for s in strategies)
        has_mask = any("mask" in s or "pii" in s or "ssn" in s for s in strategies)
        has_delete = any("drop" in s or "delete" in s or "remove" in s for s in strategies)

        # Baseline projections
        dup_removed = int(total * 0.005) if has_dedup else 0
        emails_fixed = int(total * 0.032) if has_email else 0
        nulls_fixed = int(total * 0.045) if has_null else 0
        pii_masked = 2 if has_mask else 0
        rows_removed = dup_removed + (int(total * 0.01) if has_delete else 0)
        rows_modified = emails_fixed + nulls_fixed + (int(total * 0.08) if strategies else 0)

        # Risk assessment:
        # HIGH if rows_removed > 5% of dataset
        # MEDIUM if rows_removed > 1% or rows_modified > 25%
        # LOW otherwise
        removal_ratio = rows_removed / total
        mod_ratio = rows_modified / total

        if removal_ratio > 0.05 or has_delete:
            risk = "HIGH"
        elif removal_ratio > 0.01 or mod_ratio > 0.20:
            risk = "MEDIUM"
        else:
            risk = "LOW"

        return ImpactPreviewResponse(
            rows_scanned=total,
            rows_modified=rows_modified,
            rows_removed=rows_removed,
            duplicates_removed=dup_removed,
            emails_fixed=emails_fixed,
            nulls_fixed=nulls_fixed,
            pii_fields_masked=pii_masked,
            risk=risk,
        )
