from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

from agent.compiler.models.actions import StructuredAction
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import LogicalStep

COMPILER_VERSION = "2.1.0"


def compute_plan_fingerprint(context: CompilerContext, steps: Optional[List[LogicalStep]] = None) -> str:
    """
    Computes a deterministic SHA-256 fingerprint for a compilation plan.
    Strictly excludes volatile values like timestamps, RunId, or request correlation IDs.
    
    Includes:
    - compiler version
    - target engine
    - normalized dataset names & column schemas
    - normalized detected issues
    - structured actions
    - planned logical steps (if provided)
    - enabled business rules
    - safe code-generation options
    """
    canonical_payload: Dict[str, Any] = {
        "compiler_version": COMPILER_VERSION,
        "target_engine": context.target_engine.value if hasattr(context.target_engine, "value") else str(context.target_engine),
        "dataset_names": sorted(list(context.dataset_profiles.keys())),
    }

    # Normalized issues
    normalized_issues = []
    for iss in (context.detected_issues or []):
        normalized_issues.append({
            "issue_id": iss.issue_id,
            "dataset_name": iss.dataset_name,
            "column_name": iss.column_name or "",
            "category": iss.category.value if hasattr(iss.category, "value") else str(iss.category),
            "suggested_action": iss.suggested_action or "",
        })
    normalized_issues.sort(key=lambda x: (x["dataset_name"], x.get("column_name") or "", x["issue_id"]))
    canonical_payload["issues"] = normalized_issues

    # Structured actions
    normalized_actions = []
    for act in (context.structured_actions or []):
        if isinstance(act, StructuredAction):
            act_dict = {
                "action_id": getattr(act, "action_id", ""),
                "dataset_name": act.dataset_name,
                "action_type": act.action_type.value if hasattr(act.action_type, "value") else str(act.action_type),
                "target_columns": sorted(list(act.target_columns)),
                "parameters": {k: str(v) for k, v in sorted(act.parameters.items())},
            }
        elif isinstance(act, dict):
            act_dict = {
                "action_id": act.get("action_id", ""),
                "dataset_name": act.get("dataset_name", ""),
                "action_type": str(act.get("action_type")),
                "target_columns": sorted(list(act.get("target_columns") or [])),
                "parameters": {k: str(v) for k, v in sorted((act.get("parameters") or {}).items())},
            }
        else:
            continue
        normalized_actions.append(act_dict)
    normalized_actions.sort(key=lambda x: (x["dataset_name"], x["action_type"], ",".join(x["target_columns"])))
    canonical_payload["structured_actions"] = normalized_actions

    # Planned steps (if provided)
    if steps:
        step_payloads = []
        for s in steps:
            step_payloads.append({
                "dataset_name": s.dataset_name,
                "action": s.action.value if hasattr(s.action, "value") else str(s.action),
                "target_columns": sorted(list(s.target_columns)),
                "parameters": {k: str(v) for k, v in sorted(s.parameters.items())},
                "priority": s.priority,
            })
        canonical_payload["steps"] = step_payloads

    # Business rules (enabled names)
    enabled_rules = sorted([
        r.name for r in (context.business_rules or []) if getattr(r, "enabled", True)
    ])
    canonical_payload["business_rules"] = enabled_rules

    # Column signatures / schemas
    col_signatures: Dict[str, Dict[str, str]] = {}
    for ds_name, profile in sorted(context.dataset_profiles.items()):
        cols = {
            col_name: getattr(col_prof, "dtype", "unknown")
            for col_name, col_prof in sorted(profile.columns.items())
        }
        col_signatures[ds_name] = cols
    canonical_payload["dataset_signatures"] = col_signatures

    # Safe deterministic options
    safe_options: Dict[str, Any] = {}
    excluded_keys = {"session_id", "run_id", "timestamp", "correlation_id", "request_id", "io_config"}
    for k, v in sorted(context.options.items()):
        if k.lower() not in excluded_keys and isinstance(v, (str, int, float, bool)):
            safe_options[k] = v
    canonical_payload["options"] = safe_options

    raw_json = json.dumps(canonical_payload, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(raw_json.encode('utf-8')).hexdigest()[:16]
