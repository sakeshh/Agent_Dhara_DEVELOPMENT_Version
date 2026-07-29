from typing import Any, Dict, List, Optional
from agent.semantic_context import SemanticCleaningPlan, ColumnRole

def _is_start_end_pair(c1: str, c2: str) -> bool:
    """
    Returns True if c1 and c2 form a chronological start-end pair.
    """
    n1, n2 = c1.lower().strip(), c2.lower().strip()
    
    def clean(n):
        return n.replace("_at", "").replace("_date", "").replace("_time", "").replace("date", "").replace("time", "")
        
    w1, w2 = clean(n1), clean(n2)
    
    pairs = [
        ("start", "end"),
        ("begin", "end"),
        ("from", "to"),
        ("created", "completed"),
        ("created", "closed"),
        ("open", "close"),
        ("placed", "delivered"),
        ("placed", "shipped"),
        ("order", "ship"),
        ("birth", "death"),
        ("checkin", "checkout"),
        ("arrival", "departure"),
    ]
    for p1, p2 in pairs:
        if (p1 in w1 and p2 in w2) or (w1 == p1 and w2 == p2):
            return True
    return False

def generate_auto_cross_field_rules(
    assessment: Dict[str, Any],
    semantic_context: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Generate auto cross-field rules based on column profiles and semantic context.
    """
    auto_rules = []
    datasets = assessment.get("datasets") or {}
    
    from agent.semantic_context import SemanticCleaningPlan
    semantic_plan = None
    if semantic_context:
        sem_model = semantic_context.get("semantic_model")
        if isinstance(sem_model, dict) and "entities" in sem_model:
            try:
                semantic_plan = SemanticCleaningPlan(
                    entities=sem_model.get("entities") or {},
                    relationships=sem_model.get("relationships") or [],
                )
            except Exception:
                pass
        if semantic_plan is None and "entities" in semantic_context:
            try:
                semantic_plan = SemanticCleaningPlan(
                    entities=semantic_context.get("entities") or {},
                    relationships=semantic_context.get("relationships") or [],
                )
            except Exception:
                pass
                
    for ds_name, ds_meta in datasets.items():
        if not isinstance(ds_meta, dict):
            continue
        cols = ds_meta.get("columns") or {}
        
        date_cols = []
        metric_cols = []
        
        contracts = {}
        if semantic_plan:
            contracts = semantic_plan.get_cleaning_contracts(ds_name) or {}
            
        for col_name, col_meta in cols.items():
            if not isinstance(col_meta, dict):
                continue
                
            role = None
            if col_name in contracts:
                role = contracts[col_name].role
                
            st = str(col_meta.get("semantic_type") or "").lower()
            dt = str(col_meta.get("dtype") or "").lower()
            
            is_date = (
                role in (ColumnRole.DATE, ColumnRole.TIMESTAMP) or
                st == "date" or
                any(x in col_name.lower() for x in ("date", "time", "dob", "stamp")) or
                col_name.lower().endswith("_at")
            )
            if is_date:
                date_cols.append(col_name)
                
            is_metric = (
                role == ColumnRole.METRIC or
                any(x in col_name.lower() for x in ("amount", "price", "quantity", "total", "count", "qty", "rate", "revenue", "sales")) or
                any(x in dt for x in ("int", "float", "double", "decimal", "numeric"))
            )
            if is_metric and not is_date and not col_name.lower().endswith("id"):
                metric_cols.append(col_name)
                
        # Date order pairs
        for i in range(len(date_cols)):
            for j in range(i + 1, len(date_cols)):
                c1, c2 = date_cols[i], date_cols[j]
                if _is_start_end_pair(c1, c2):
                    auto_rules.append({
                        "dataset": ds_name,
                        "type": "date_order",
                        "start_column": c1,
                        "end_column": c2,
                        "severity": "medium",
                        "source": "auto_detected"
                    })
                elif _is_start_end_pair(c2, c1):
                    auto_rules.append({
                        "dataset": ds_name,
                        "type": "date_order",
                        "start_column": c2,
                        "end_column": c1,
                        "severity": "medium",
                        "source": "auto_detected"
                    })
                        
        # Non-negative columns
        for col in metric_cols:
            auto_rules.append({
                "dataset": ds_name,
                "type": "non_negative",
                "column": col,
                "severity": "medium",
                "source": "auto_detected"
            })
            
    return auto_rules
