"""
issue_identity.py — Deterministic identity, normalization, and step merging for Agent Dhara.

Provides canonical normalization for datasets, columns, actions, issue types, and message patterns,
as well as deterministic hash-based IDs and step-merging logic with complete provenance preservation.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from agent.etl_pipeline.issue_registry import canonicalize_issue_type

_SEVERITY_WEIGHTS: Dict[str, int] = {
    "critical": 5,
    "blocker": 5,
    "high": 4,
    "medium": 3,
    "low": 2,
    "info": 1,
    "advisory": 1,
}


def normalize_dataset_name(dataset: Optional[str]) -> str:
    """Normalize dataset name: lowercase, strip brackets/quotes and dbo. prefix."""
    if not dataset:
        return "_global"
    s = str(dataset).strip()
    # Remove leading dbo. or [dbo].
    s = re.sub(r"^(?:\[?dbo\]?\.)", "", s, flags=re.IGNORECASE)
    # Strip brackets and quotes
    s = s.strip("[]\"' ")
    s = s.lower()
    return s or "_global"


def normalize_column_name(column: Optional[str]) -> str:
    """Normalize column name: lowercase, strip brackets/quotes."""
    if not column:
        return "*"
    s = str(column).strip().strip("[]\"' ").lower()
    return s or "*"


def normalize_action(action: Optional[str]) -> str:
    """Normalize action name."""
    if not action:
        return "noop"
    s = str(action).strip().lower()
    return s or "noop"


def normalize_issue_type(issue_type: Optional[str]) -> str:
    """Canonicalize issue type using the central registry."""
    if not issue_type:
        return "unknown"
    return canonicalize_issue_type(str(issue_type).strip().lower())


def normalize_message_pattern(message: Optional[str]) -> str:
    """
    Normalize error/quality message to a pattern for deduplication and budgeting.
    Replaces numbers, timestamps, and quoted literals with generic tokens.
    """
    if not message:
        return ""
    s = str(message).strip().lower()
    # Replace ISO dates/datetimes
    s = re.sub(r"\b\d{4}-\d{2}-\d{2}(?:[ t]\d{2}:\d{2}:\d{2})?\b", "<DATE>", s)
    # Replace quoted strings ('abc', "xyz")
    s = re.sub(r"'[^']*'", "<STR>", s)
    s = re.sub(r'"[^"]*"', "<STR>", s)
    # Replace integers/floats
    s = re.sub(r"\b\d+(?:\.\d+)?\b", "<NUM>", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def step_identity(dataset: Optional[str], column: Optional[str], action: Optional[str]) -> str:
    """Compute canonical composite key for a step."""
    return f"{normalize_dataset_name(dataset)}::{normalize_column_name(column)}::{normalize_action(action)}"


def issue_identity(dataset: Optional[str], column: Optional[str], issue_type: Optional[str]) -> str:
    """Compute canonical composite key for an issue."""
    return f"{normalize_dataset_name(dataset)}::{normalize_column_name(column)}::{normalize_issue_type(issue_type)}"


def make_step_id(
    dataset: Optional[str],
    column: Optional[str],
    action: Optional[str],
    index: Optional[Union[int, str]] = None,
) -> str:
    """Generate a deterministic, short step ID."""
    ident = step_identity(dataset, column, action)
    if index is not None and str(index) != "":
        ident = f"{ident}::{index}"
    h = hashlib.sha256(ident.encode("utf-8")).hexdigest()[:8]
    return f"STEP-{h}"


def make_issue_id(
    dataset: Optional[str],
    column: Optional[str],
    issue_type: Optional[str],
    index: Optional[int] = None,
) -> str:
    """Generate a deterministic, short issue ID."""
    ident = issue_identity(dataset, column, issue_type)
    if index is not None:
        ident = f"{ident}::{index}"
    h = hashlib.sha256(ident.encode("utf-8")).hexdigest()[:8]
    return f"ISSUE-{h}"


def make_resolution_signature(
    issue_type: Optional[str],
    col_semantic_type: Optional[str],
    message: Optional[str],
) -> str:
    """Generate unique resolution signature for LLM fallback caching and budgeting."""
    norm_it = normalize_issue_type(issue_type)
    norm_sem = str(col_semantic_type or "unknown").strip().lower()
    norm_msg = normalize_message_pattern(message)
    raw = f"{norm_it}::{norm_sem}::{norm_msg}"
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
    return f"SIG-{h}"


def _get_severity_score(sev: Any) -> int:
    return _SEVERITY_WEIGHTS.get(str(sev or "").lower().strip(), 2)


def merge_steps(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deterministically merge two plan steps targeting the exact same (dataset, column, action).
    Preserves all provenance:
      - source_issue_types: combined list of all unique issue types
      - source_issue_ids: combined list of all unique issue IDs
      - source_messages: combined list of unique messages
    Tie-breaking for step winner (parameters, severity, evidence):
      1. Severity (critical > high > medium > low > info)
      2. estimated_affected_rows (higher wins)
      3. confidence in evidence / llm_recommendation (higher wins)
      4. existing step wins by default (first-in-order stability)
    """
    merged = dict(existing)

    # Collect source issue types
    issue_types: List[str] = []
    for it in [existing.get("source_issue_type")] + (existing.get("source_issue_types") or []):
        if it and it not in issue_types:
            issue_types.append(it)
    for it in [incoming.get("source_issue_type")] + (incoming.get("source_issue_types") or []):
        if it and it not in issue_types:
            issue_types.append(it)

    # Collect source issue IDs
    issue_ids: List[str] = []
    for iid in [existing.get("issue_id")] + (existing.get("source_issue_ids") or []):
        if iid and iid not in issue_ids:
            issue_ids.append(iid)
    for iid in [incoming.get("issue_id")] + (incoming.get("source_issue_ids") or []):
        if iid and iid not in issue_ids:
            issue_ids.append(iid)

    # Ensure source issue IDs exist for all collected issue types
    ds_for_id = merged.get("dataset") or incoming.get("dataset") or "_global"
    col_for_id = merged.get("column") or incoming.get("column")
    for it in issue_types:
        iid = make_issue_id(ds_for_id, col_for_id, it)
        if iid not in issue_ids:
            issue_ids.append(iid)

    # Collect messages / notes
    messages: List[str] = []
    for m in [existing.get("message"), existing.get("note")]:
        if m and m not in messages:
            messages.append(m)
    for m in [incoming.get("message"), incoming.get("note")]:
        if m and m not in messages:
            messages.append(m)

    # Tie-breaking
    ex_sev_score = _get_severity_score(existing.get("severity"))
    in_sev_score = _get_severity_score(incoming.get("severity"))

    ex_rows = int(existing.get("estimated_affected_rows") or 0)
    in_rows = int(incoming.get("estimated_affected_rows") or 0)

    ex_conf = float((existing.get("evidence") or {}).get("confidence") or 0.5)
    in_conf = float((incoming.get("evidence") or {}).get("confidence") or 0.5)

    incoming_wins = False
    if in_sev_score > ex_sev_score:
        incoming_wins = True
    elif in_sev_score == ex_sev_score:
        if in_rows > ex_rows:
            incoming_wins = True
        elif in_rows == ex_rows:
            if in_conf > ex_conf:
                incoming_wins = True

    winner = incoming if incoming_wins else existing

    # Adopt winner's core attributes
    merged["severity"] = winner.get("severity") or existing.get("severity") or "medium"
    merged["priority"] = winner.get("priority", existing.get("priority", 80))
    merged["estimated_affected_rows"] = max(ex_rows, in_rows) or None
    merged["note"] = winner.get("note") or existing.get("note")
    merged["message"] = winner.get("message") or existing.get("message")

    # Merge params: start with winner params, backfill with other params
    loser = existing if incoming_wins else incoming
    merged_params = dict(loser.get("params") or {})
    merged_params.update(winner.get("params") or {})
    merged["params"] = merged_params

    # Evidence merge
    merged_ev = dict(loser.get("evidence") or {})
    merged_ev.update(winner.get("evidence") or {})
    if incoming.get("llm_recommendation") and not merged.get("llm_recommendation"):
        merged["llm_recommendation"] = incoming["llm_recommendation"]
    merged["evidence"] = merged_ev

    # Store full provenance
    merged["source_issue_types"] = issue_types
    if issue_types:
        merged["source_issue_type"] = issue_types[0]
    merged["source_issue_ids"] = issue_ids
    if issue_ids:
        merged["issue_id"] = issue_ids[0]
    merged["source_messages"] = messages

    # Ensure dataset and step_id are reliably assigned
    ds = (
        merged.get("dataset")
        or loser.get("dataset")
        or incoming.get("dataset")
        or existing.get("dataset")
        or "_global"
    )
    merged["dataset"] = ds
    col = merged.get("column")
    act = merged.get("action")
    merged["step_id"] = make_step_id(ds, col, act)

    return merged
