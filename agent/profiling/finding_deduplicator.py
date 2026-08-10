"""
Finding Taxonomy, Hierarchy, and Deduplication Engine.
Generates canonical finding IDs, links root causes to secondary symptoms, and merges
duplicate scanner findings to prevent double-penalty scoring.
"""

import hashlib
import json
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from agent.models.assessment_result import DQIssue


# Hierarchy mapping: Root Cause -> List of secondary symptom issue types
ROOT_CAUSE_HIERARCHY: Dict[str, List[str]] = {
    "physical_type_mismatch": ["invalid_numerical_value", "unexpected_string", "calculation_failure"],
    "schema_violation": ["unexpected_column", "missing_required_column", "type_mismatch"],
    "duplicate_primary_key": ["duplicate_row", "uniqueness_violation"],
    "unresolvable_entity": ["orphan_foreign_key", "referential_integrity_violation"]
}


def generate_finding_key(
    dataset: str,
    column: Optional[str],
    rule_id: str,
    contract_id: Optional[str] = None,
    scope: str = "column"
) -> str:
    """
    Generates a stable, reproducible finding hash key.
    """
    raw_key = f"{dataset}::{column or 'DATASET'}::{rule_id}::{contract_id or 'DEFAULT'}::{scope}"
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:16]


def deduplicate_and_link_findings(issues: List[DQIssue]) -> tuple[List[DQIssue], List[Dict[str, Any]]]:
    """
    Merges duplicate findings, sets canonical finding IDs, and links root causes to secondary symptoms.
    """
    finding_map: Dict[str, DQIssue] = {}
    diagnostics: List[Dict[str, Any]] = []

    # Map for symptom linking
    root_cause_by_column: Dict[Tuple[str, str], DQIssue] = {}

    for issue in issues:
        col_str = issue.column or "DATASET"
        rule_str = issue.rule_id or issue.type
        f_id = generate_finding_key(
            dataset="dataset",
            column=issue.column,
            rule_id=rule_str
        )
        issue.finding_id = f_id

        # Determine root cause vs symptom
        is_root = True
        for root_type, symptoms in ROOT_CAUSE_HIERARCHY.items():
            if issue.type in symptoms:
                is_root = False
                break
        issue.is_root_cause = is_root

        if f_id not in finding_map:
            finding_map[f_id] = issue
        else:
            # Merge evidence, row indexes, and samples
            existing = finding_map[f_id]
            if issue.count and existing.count:
                existing.count += issue.count
            if issue.row_indexes and existing.row_indexes:
                existing.row_indexes = list(set(existing.row_indexes + issue.row_indexes))[:100]
            if issue.sample_values and existing.sample_values:
                existing.sample_values = list(set(existing.sample_values + issue.sample_values))[:10]
            diagnostics.append({"action": "merged_duplicate_finding", "finding_id": f_id, "type": issue.type})

    deduped = list(finding_map.values())
    return deduped, diagnostics
