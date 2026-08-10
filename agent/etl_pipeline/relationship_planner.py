"""
Derive join keys, load order, M:N bridge models, and relationship steps from assessment.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def _parent_child_from_rel(rel: Dict[str, Any]) -> Optional[Tuple[str, str, str, str]]:
    card = str(rel.get("cardinality") or "").lower()
    n1 = str(rel.get("dataset_a") or "")
    n2 = str(rel.get("dataset_b") or "")
    c1 = str(rel.get("column_a") or "")
    c2 = str(rel.get("column_b") or "")
    m1 = int(rel.get("max_rows_per_key_a") or 1)
    m2 = int(rel.get("max_rows_per_key_b") or 1)

    if card == "one_to_many" or (m1 <= 1 < m2):
        return n1, c1, n2, c2
    if card == "many_to_one" or (m2 <= 1 < m1):
        return n2, c2, n1, c1
    if card == "one_to_one" and m1 <= 1 and m2 <= 1:
        return n1, c1, n2, c2
    return None


def calculate_join_confidence(rel: Dict[str, Any], assessment: Dict[str, Any]) -> float:
    overlap = float(rel.get("overlap_count") or 0)
    card = str(rel.get("cardinality") or "").lower()
    
    ds_a = rel.get("dataset_a")
    ds_b = rel.get("dataset_b")
    
    row_count_a = 1
    row_count_b = 1
    if assessment and isinstance(assessment, dict):
        datasets = assessment.get("datasets") or {}
        row_count_a = float(datasets.get(ds_a, {}).get("row_count") or 1)
        row_count_b = float(datasets.get(ds_b, {}).get("row_count") or 1)
        
    max_rows = max(row_count_a, row_count_b, 1)
    overlap_ratio = overlap / max_rows
    
    confidence = 0.5
    
    if overlap > 0:
        confidence += min(0.3, overlap_ratio * 0.3)
        if overlap > 100:
            confidence += 0.1
            
    col_a = str(rel.get("column_a") or "").lower()
    col_b = str(rel.get("column_b") or "").lower()
    if col_a == col_b:
        confidence += 0.15
    else:
        suffixes = ["_id", "id", "_key", "key", "_code", "code", "_num", "num"]
        shares_suffix = False
        for s in suffixes:
            if col_a.endswith(s) and col_b.endswith(s):
                shares_suffix = True
                break
        if shares_suffix:
            confidence += 0.08
            
    if card in ("one_to_many", "many_to_one", "one_to_one"):
        confidence += 0.05
    elif card == "many_to_many":
        confidence -= 0.15
        
    return round(max(0.1, min(1.0, confidence)), 2)


def _build_join_evidence(rel: Dict[str, Any], assessment: Dict[str, Any], orphan_count: int = 0) -> Dict[str, Any]:
    why = [
        rel.get("summary") or "",
        f"{rel.get('overlap_count', 0):,} overlapping key values",
        f"cardinality: {rel.get('cardinality', 'unknown')}",
    ]
    alts: List[str] = []
    card = str(rel.get("cardinality") or "")
    if card == "many_to_many":
        alts.append("Use bridge table (implemented as model_many_to_many step)")
        alts.append("Keep datasets separate and aggregate before reporting")
    if orphan_count > 0:
        alts.append("Left join + orphan quarantine table to preserve child rows")
    else:
        alts.append("Inner join if only matching keys should survive")
        
    conf = calculate_join_confidence(rel, assessment)
    if orphan_count > 0:
        conf = max(0.1, conf - 0.15)
        
    return {
        "why_this_action": " | ".join(x for x in why if x),
        "alternatives": [a for a in alts if a],
        "confidence": conf,
        "overlap_count": rel.get("overlap_count"),
        "cardinality": card,
        "orphan_row_count": orphan_count or None,
    }


def _bridge_name(a: str, b: str, col: str) -> str:
    import re

    sa = re.sub(r"[^0-9a-zA-Z_]+", "_", a)[:20]
    sb = re.sub(r"[^0-9a-zA-Z_]+", "_", b)[:20]
    sc = re.sub(r"[^0-9a-zA-Z_]+", "_", col)[:20]
    return f"bridge_{sa}_{sb}_{sc}"


def assessed_dataset_names(assessment: Dict[str, Any]) -> set[str]:
    """Helper to return a set of dataset names present in assessment datasets block."""
    datasets = assessment.get("datasets") or {}
    if isinstance(datasets, dict):
        return set(datasets.keys())
    return {item.get("name") or item.get("dataset") for item in datasets if isinstance(item, dict)}


def build_relationship_plan(assessment: Dict[str, Any]) -> Dict[str, Any]:
    rels: List[Dict[str, Any]] = list(assessment.get("relationships") or [])
    dq = assessment.get("data_quality_issues") or {}
    global_issues = dq.get("global_issues") if isinstance(dq, dict) else {}
    if not isinstance(global_issues, dict):
        global_issues = {}
    row_issues: List[Dict[str, Any]] = list(global_issues.get("relationship_row_issues") or [])
    warnings: List[Dict[str, Any]] = list(global_issues.get("relationship_warnings") or [])

    assessed_datasets = assessed_dataset_names(assessment)
    out_of_scope_relationships = []
    active_rels = []

    for rel in rels:
        n1 = str(rel.get("dataset_a") or "")
        n2 = str(rel.get("dataset_b") or "")
        if n1 not in assessed_datasets or n2 not in assessed_datasets:
            missing = [d for d in [n1, n2] if d not in assessed_datasets]
            is_required = bool(rel.get("required", True))
            status = "SCOPE_INCOMPLETE" if is_required else "NOT_APPLICABLE"
            gating_result = "NOT_APPLICABLE" # Not failing DQ gate, just not applicable/incomplete
            out_of_scope_relationships.append({
                "relationship_id": rel.get("relationship_id") or f"{n1}_{n2}",
                "dataset_a": n1,
                "dataset_b": n2,
                "status": status,
                "gating_result": gating_result,
                "reason": f"Dataset(s) {missing} not present in assessment scope",
                "missing_datasets": missing,
            })
        else:
            active_rels.append(rel)

    orphan_by_pair: Dict[Tuple[str, str, str], int] = {}
    for iss in row_issues:
        if str(iss.get("type") or "") != "orphan_foreign_key_rows":
            continue
        key = (
            str(iss.get("related_dataset") or ""),
            str(iss.get("related_column") or ""),
            str(iss.get("dataset") or ""),
        )
        orphan_by_pair[key] = int(iss.get("count") or 0)

    joins: List[Dict[str, Any]] = []
    many_to_many: List[Dict[str, Any]] = []
    parent_scores: Dict[str, int] = {}

    for rel in active_rels:
        card = str(rel.get("cardinality") or "").lower()
        n1 = str(rel.get("dataset_a") or "")
        n2 = str(rel.get("dataset_b") or "")
        c1 = str(rel.get("column_a") or "")
        c2 = str(rel.get("column_b") or "")

        if card == "many_to_many" or (
            int(rel.get("max_rows_per_key_a") or 1) > 1
            and int(rel.get("max_rows_per_key_b") or 1) > 1
        ):
            bname = _bridge_name(n1, n2, c1)
            many_to_many.append(
                {
                    "dataset_a": n1,
                    "dataset_b": n2,
                    "column_a": c1,
                    "column_b": c2,
                    "bridge_name": bname,
                    "cardinality": "many_to_many",
                    "overlap_count": rel.get("overlap_count"),
                    "evidence": _build_join_evidence(rel, assessment),
                    "recommended_action": "model_many_to_many",
                    "resolution_options": [
                        "bridge_table",
                        "keep_separate",
                        "aggregate_then_join",
                    ],
                    "default_resolution": "bridge_table",
                }
            )
            joins.append(
                {
                    "left_dataset": n1,
                    "right_dataset": n2,
                    "left_key": c1,
                    "right_key": c2,
                    "join_type": "review",
                    "cardinality": "many_to_many",
                    "overlap_count": rel.get("overlap_count"),
                    "evidence": _build_join_evidence(rel, assessment),
                    "note": "M:N — use bridge table codegen, not direct join",
                    "auto_fixable": False,
                    "bridge_name": bname,
                }
            )
            continue

        pc = _parent_child_from_rel(rel)
        if not pc:
            joins.append(
                {
                    "left_dataset": n1,
                    "right_dataset": n2,
                    "left_key": c1,
                    "right_key": c2,
                    "join_type": "review",
                    "cardinality": rel.get("cardinality"),
                    "overlap_count": rel.get("overlap_count"),
                    "evidence": _build_join_evidence(rel, assessment),
                    "note": "Ambiguous cardinality — review before joining",
                    "auto_fixable": False,
                }
            )
            continue

        p_ds, p_col, c_ds, c_col = pc
        key = (p_ds, p_col, c_ds)
        
        # 3-state orphan count: None = missing evidence, 0 = no orphans, N > 0 = orphans
        if key in orphan_by_pair:
            assessment_orphan_row_count = orphan_by_pair[key]
            assessment_status = "EVALUATED"
        else:
            if p_ds in assessed_datasets and c_ds in assessed_datasets:
                assessment_orphan_row_count = 0
                assessment_status = "EVALUATED"
            else:
                assessment_orphan_row_count = None
                assessment_status = "EVIDENCE_NOT_FOUND"

        join_type = "left_semi_anti" # Always use left_semi_anti partition for Phase 1 FK relationships
        parent_scores[p_ds] = parent_scores.get(p_ds, 0) + 1

        joins.append(
            {
                "parent_dataset": p_ds,
                "child_dataset": c_ds,
                "parent_key": p_col,
                "child_key": c_col,
                "left_dataset": p_ds,
                "right_dataset": c_ds,
                "left_key": p_col,
                "right_key": c_col,
                "join_type": join_type,
                "cardinality": rel.get("cardinality"),
                "from_a_to_b": rel.get("from_a_to_b"),
                "overlap_count": rel.get("overlap_count"),
                "orphan_row_count": assessment_orphan_row_count,
                "assessment_status": assessment_status,
                "evidence": _build_join_evidence(rel, assessment, orphan_count=(assessment_orphan_row_count or 0)),
                "recommended_action": "validate_and_partition_relationship",
                "auto_fixable": True,
            }
        )

    ds_names = list(assessed_datasets)
    load_order: List[str] = []
    seen = set()
    for ds, _ in sorted(parent_scores.items(), key=lambda x: -x[1]):
        if ds in ds_names and ds not in seen:
            load_order.append(ds)
            seen.add(ds)
    for ds in ds_names:
        if ds not in seen:
            load_order.append(ds)
            seen.add(ds)

    relationship_steps: List[Dict[str, Any]] = []
    order = 0
    for b in many_to_many:
        order += 1
        relationship_steps.append(
            {
                "order": order,
                "action": "model_many_to_many",
                "dataset_a": b.get("dataset_a"),
                "dataset_b": b.get("dataset_b"),
                "column_a": b.get("column_a"),
                "column_b": b.get("column_b"),
                "bridge_name": b.get("bridge_name"),
                "bucket": "review",
                "evidence": b.get("evidence"),
            }
        )

    for j in joins:
        if j.get("join_type") == "review":
            continue
        order += 1
        relationship_steps.append(
            {
                "order": order,
                "action": "validate_and_partition_relationship",
                "parent_dataset": j.get("parent_dataset"),
                "child_dataset": j.get("child_dataset"),
                "parent_key": j.get("parent_key"),
                "child_key": j.get("child_key"),
                "join_type": j.get("join_type"),
                "bucket": "review" if j.get("orphan_row_count") else "auto",
                "evidence": j.get("evidence"),
            }
        )

    for iss in row_issues:
        if str(iss.get("type") or "") != "orphan_foreign_key_rows":
            continue
        order += 1
        relationship_steps.append(
            {
                "order": order,
                "action": "validate_and_partition_relationship",
                "dataset": iss.get("dataset"),
                "column": iss.get("column"),
                "related_dataset": iss.get("related_dataset"),
                "related_column": iss.get("related_column"),
                "estimated_affected_rows": iss.get("count"),
                "bucket": "review",
                "evidence": {
                    "why_this_action": iss.get("message") or "Orphan foreign keys detected",
                    "alternatives": [
                        "Reject orphan rows",
                        "Load orphans to quarantine table",
                        iss.get("recommendation") or "",
                    ],
                    "confidence": 0.75,
                    "orphan_row_count": iss.get("count"),
                },
            }
        )

    return {
        "joins": joins,
        "many_to_many": many_to_many,
        "load_order": load_order,
        "relationship_steps": relationship_steps,
        "orphan_issues": row_issues,
        "warnings": warnings,
        "join_count": len([j for j in joins if j.get("join_type") != "review"]),
        "mn_count": len(many_to_many),
        "out_of_scope_relationships": out_of_scope_relationships,
    }

