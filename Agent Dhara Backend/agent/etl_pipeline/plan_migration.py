"""
plan_migration.py — Safe load-time migration of saved plans with legacy actions.
"""
from __future__ import annotations
import copy
from typing import Any, Dict, Tuple, List

def migrate_legacy_plan_actions(plan: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str], List[str]]:
    """
    Scans the plan's datasets for steps with legacy/invalid actions.
    If the step can be automatically migrated without inventing policy (e.g. from parameters),
    it performs the migration and records it in applied list.
    Otherwise, sets action=None, migration_status='REVIEW_REQUIRED', and adds the step to review list.
    """
    migrated_plan = copy.deepcopy(plan)
    applied = []
    review_required = []

    datasets = migrated_plan.get("datasets") or {}
    for ds_name, block in datasets.items():
        if not isinstance(block, dict):
            continue
        steps = block.get("steps") or []
        for st in steps:
            if not isinstance(st, dict):
                continue
            action = str(st.get("action") or "").lower()
            params = st.get("params") or {}
            col = st.get("column")

            if action == "fill_nulls_simple":
                # Check parameters to see if we can safely infer intent
                strat = params.get("fill_strategy") or params.get("strategy")
                val = params.get("fill_value") or params.get("value")
                
                # Check if user choice was already recorded in manual_resolution
                mr = st.get("manual_resolution") or {}
                selected_opt = mr.get("selected_option_id")
                
                if selected_opt == "preserve_and_flag":
                    st["action"] = "preserve_null_and_flag"
                    params["status_column"] = params.get("status_column", f"{col}_validation_status")
                    params["reason_column"] = params.get("reason_column", f"{col}_validation_reason")
                    mr["selected_action"] = "preserve_null_and_flag"
                    st["manual_resolution"] = mr
                    applied.append(f"Migrated fill_nulls_simple to preserve_null_and_flag on {ds_name}.{col} via user choice")
                elif strat == "constant" and val is not None:
                    st["action"] = "fill_null_with_constant"
                    params["value"] = val
                    params["preserve_raw"] = params.get("preserve_raw", True)
                    params["policy_id"] = params.get("policy_id", f"POL-MIG-{ds_name}-{col}".upper().replace(".", "_"))
                    mr["selected_option_id"] = mr.get("selected_option_id") or "fill_constant"
                    mr["selected_action"] = "fill_null_with_constant"
                    st["manual_resolution"] = mr
                    applied.append(f"Migrated fill_nulls_simple to fill_null_with_constant on {ds_name}.{col}")
                elif strat in ("mean", "median", "mode", "grouped_mean", "grouped_median"):
                    st["action"] = "impute_numeric"
                    params["strategy"] = strat
                    params["preserve_raw"] = params.get("preserve_raw", True)
                    params["output_column"] = params.get("output_column", f"{col}_clean")
                    params["policy_id"] = params.get("policy_id", f"POL-MIG-{ds_name}-{col}".upper().replace(".", "_"))
                    mr["selected_option_id"] = mr.get("selected_option_id") or f"impute_{strat}"
                    mr["selected_action"] = "impute_numeric"
                    st["manual_resolution"] = mr
                    applied.append(f"Migrated fill_nulls_simple to impute_numeric on {ds_name}.{col}")
                else:
                    st["action"] = None
                    st["migration_status"] = "REVIEW_REQUIRED"
                    st["legacy_action"] = "fill_nulls_simple"
                    st["blocking_reason"] = "Original null disposition was not persisted"
                    review_required.append(f"Ambiguous null handling on {ds_name}.{col} requires manual review")
            elif action == "clip_or_flag":
                method = params.get("outlier_method") or params.get("method")
                mr = st.get("manual_resolution") or {}
                selected_opt = mr.get("selected_option_id")
                
                if selected_opt == "flag_outliers" or method == "flag":
                    st["action"] = "flag_outliers"
                    mr["selected_option_id"] = mr.get("selected_option_id") or "flag_outliers"
                    mr["selected_action"] = "flag_outliers"
                    st["manual_resolution"] = mr
                    applied.append(f"Migrated clip_or_flag to flag_outliers on {ds_name}.{col}")
                elif selected_opt == "clip_outliers" or method == "clip":
                    st["action"] = "clip_outliers"
                    mr["selected_option_id"] = mr.get("selected_option_id") or "clip_outliers"
                    mr["selected_action"] = "clip_outliers"
                    st["manual_resolution"] = mr
                    applied.append(f"Migrated clip_or_flag to clip_outliers on {ds_name}.{col}")
                else:
                    st["action"] = None
                    st["migration_status"] = "REVIEW_REQUIRED"
                    st["legacy_action"] = "clip_or_flag"
                    st["blocking_reason"] = "Original outlier disposition was not persisted"
                    review_required.append(f"Ambiguous outlier handling on {ds_name}.{col} requires manual review")
            elif action == "fill_or_drop":
                # Check manual_resolution user selection
                mr = st.get("manual_resolution") or {}
                selected_opt = mr.get("selected_option_id")
                if selected_opt == "preserve_and_flag":
                    st["action"] = "preserve_null_and_flag"
                    params["status_column"] = params.get("status_column", f"{col}_validation_status")
                    params["reason_column"] = params.get("reason_column", f"{col}_validation_reason")
                    mr["selected_action"] = "preserve_null_and_flag"
                    st["manual_resolution"] = mr
                    applied.append(f"Migrated fill_or_drop to preserve_null_and_flag on {ds_name}.{col}")
                else:
                    st["action"] = None
                    st["migration_status"] = "REVIEW_REQUIRED"
                    st["legacy_action"] = "fill_or_drop"
                    st["blocking_reason"] = "Legacy action is fill_or_drop and has no parameters"
                    review_required.append(f"Legacy fill_or_drop action on {ds_name}.{col} requires manual review")

    return migrated_plan, applied, review_required
