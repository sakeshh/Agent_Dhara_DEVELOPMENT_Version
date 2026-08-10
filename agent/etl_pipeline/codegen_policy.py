"""Shared ETL policy text for template and LLM codegen."""
from __future__ import annotations

import json
from typing import Any, Dict, List




def plan_policy_block(plan: Dict[str, Any]) -> str:
    """Human-readable policy block from plan + business rules."""
    rules = plan.get("business_rules") or {}
    rel = plan.get("relationships") or {}
    joins = rel.get("joins") or []
    never_drop = bool(rules.get("never_drop_rows"))
    join_strategy = str(joins[0].get("join_type") or "left") if joins else "none"
    privacy = []
    for block in (plan.get("datasets") or {}).values():
        for st in (block or {}).get("steps") or []:
            p = (st.get("params") or {}).get("privacy")
            if p:
                privacy.append(f"{st.get('column')}:{p}")
                
    generation_mode = str(plan.get("generation_mode") or "").lower().strip()
    is_phase1 = generation_mode in ("cleanse_only", "phase_1_cleaning", "cleanse")
    
    lines = []
    if is_phase1:
        lines.extend([
            "Generation mode: PHASE_1_CLEANSE_ONLY",
            "Mandatory constraints:",
            "  - Preserve every source row (clean + quarantine = source total)",
            "  - Preserve every raw column and raw value",
            "  - Do not deduplicate or merge records",
            "  - Do not drop rows or columns",
            "  - Do not impute business identifiers",
            "  - Do not clip or cap business measures",
            "  - Route invalid/null mandatory records to quarantine",
            "  - Flag all uncertain findings for review",
            f"Assessment signature: {plan.get('assessment_signature', 'unknown')}",
            f"Normalization contract: Version 1.0.0",
            ""
        ])

    lines.extend([
        f"plan_id: {plan.get('plan_id')}",
        f"row_preservation: {'preserve all rows' if never_drop else 'subset drops allowed'}",
        f"join_strategy: {join_strategy}",
        f"privacy: {', '.join(privacy) if privacy else 'none'}",
        f"outlier_strategy: {rules.get('outlier_strategy', 'flag')}",
        f"required_columns: {rules.get('required_columns') or []}",
        f"exclude_columns: {rules.get('exclude_columns') or []}",
        f"valid_values: {list((rules.get('valid_values') or {}).keys())}",
    ])
    return "\n".join(lines)


def plan_steps_with_params(plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Compact step list for LLM payloads."""
    out: List[Dict[str, Any]] = []
    for ds, block in (plan.get("datasets") or {}).items():
        for st in sorted((block or {}).get("steps") or [], key=lambda x: int(x.get("order") or 0)):
            out.append(
                {
                    "dataset": ds,
                    "order": st.get("order"),
                    "column": st.get("column"),
                    "action": st.get("action"),
                    "params": st.get("params") or {},
                    "severity": st.get("severity"),
                }
            )
    return out


def llm_codegen_extra_context(plan: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "policy": plan_policy_block(plan),
        "steps_with_params": plan_steps_with_params(plan),
        "invariants": plan.get("invariants") or [],
    }
