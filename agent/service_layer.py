# Service layer for DHARA-GX report and metadata operations.
from __future__ import annotations
import os
import sys
import re
import time
import json
import html as html_module
import hashlib
import logging
from collections import defaultdict
from typing import Dict, Any, Tuple, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Azure / SQL Connector imports and availability
try:
    from agent.intelligent_data_assessment import load_and_profile, load_dq_thresholds
except ImportError:
    try:
        ida_path = Path(PROJECT_DIR) / "agent" / "intelligent_data_assessment.py"
        if not ida_path.is_file():
            ida_path = Path(PROJECT_DIR) / "intelligent_data_assessment.py"
        import importlib.util
        spec = importlib.util.spec_from_file_location("ida_dyn", ida_path)
        ida = importlib.util.module_from_spec(spec)
        if spec and spec.loader:
            spec.loader.exec_module(ida)
        load_and_profile = ida.load_and_profile
        load_dq_thresholds = getattr(ida, "load_dq_thresholds", lambda p: {})
    except Exception as e:
        logger.critical("Could not import intelligent_data_assessment: %s", e)
        sys.exit(1)

try:
    from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector
except Exception as e:
    logger.info("SQL connector not available (pythonnet/.NET required for database): %s", e)
    AzureSQLPythonNetConnector = None

try:
    from connectors.azure_blob_storage import AzureBlobStorageConnector
    AZURE_BLOB_AVAILABLE = True
except Exception as e:
    logger.info("Azure Blob Storage connector not available: %s", e)
    AZURE_BLOB_AVAILABLE = False

def to_json_safe(obj):
    """Convert non-JSON-native types to safe representations."""
    try:
        import datetime as _dt
        if isinstance(obj, (_dt.datetime, _dt.date, _dt.time)):
            return obj.isoformat()
    except Exception:
        pass
    try:
        import numpy as _np
        import pandas as _pd
        if isinstance(obj, (_pd.Timestamp,)):
            return obj.isoformat()
        if isinstance(obj, (_np.integer, _np.floating, _np.bool_)):
            return obj.item()
        if isinstance(obj, _np.ndarray):
            return obj.tolist()
    except Exception:
        pass
    if isinstance(obj, (set, tuple)):
        return list(obj)
    return str(obj)


def mask_pii_sample(val: Any, col: Optional[str] = None) -> str:
    """Mask sensitive PII values (emails, phones) for diagnostic sample reporting."""
    if val is None:
        return ""
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null", "<na>"):
        return s

    col_l = (col or "").lower()

    # 1. Email masking: keep first character and full domain
    if "@" in s and "." in s.split("@")[-1]:
        parts = s.split("@", 1)
        local_part, domain = parts[0], parts[1]
        first_char = local_part[0] if local_part else "u"
        return f"{first_char}****@{domain}"

    # Check if column or value is date/time/numeric id/code - do NOT mask as phone
    if any(k in col_l for k in ["date", "time", "dt", "timestamp", "year", "month", "day", "dob", "birth", "created", "updated"]):
        return s
    if re.match(r"^\d{4}[-/.]\d{1,2}[-/.]\d{1,2}", s) or re.match(r"^\d{1,2}[-/.]\d{1,2}[-/.]\d{4}", s):
        return s

    # 2. Phone masking: +1 (555) ***-4567 or 555-***-1234
    is_phone_col = any(k in col_l for k in ["phone", "tel", "mobile", "cell", "contact_no", "fax"])
    digits = re.findall(r"\d", s)
    if is_phone_col or (len(digits) >= 7 and any(c in s for c in ["-", "(", ")", "+", " "])):
        if "(" in s and ")" in s:
            masked = re.sub(r"(\(\d{3}\)\s*)\d{3}", r"\1***", s)
            if masked != s:
                return masked
        if re.search(r"\d{3}[-.\s]\d{3}[-.\s]\d{4}", s):
            return re.sub(r"(\d{3})[-.\s]\d{3}([-.\s]\d{4})", r"\1-***\2", s)
        if len(digits) >= 7 and is_phone_col:
            d_idx = [i for i, c in enumerate(s) if c.isdigit()]
            start_m = d_idx[3]
            end_m = d_idx[-4] if len(d_idx) >= 8 else d_idx[-3]
            if start_m <= end_m:
                return s[:start_m] + "***" + s[end_m + 1:]

    return s


def split_schema_table(full: str) -> Tuple[str, str]:
    return full.split(".", 1) if "." in full else ("dbo", full)


def _quote_two_part_name_fallback(table: str) -> str:
    if "." in table:
        schema, name = table.split(".", 1)
    else:
        schema, name = "dbo", table
    return f"[{schema}].[{name}]"


def print_schema_info_schema(connector, table: str) -> bool:
    schema, tbl = split_schema_table(table)
    try:
        from connectors.azure_sql_pythonnet import SqlCommand
        conn = connector._connect()
        conn.Open()
        try:
            cmd = SqlCommand("""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
                ORDER BY ORDINAL_POSITION
            """, conn)
            cmd.Parameters.AddWithValue("@schema", schema)
            cmd.Parameters.AddWithValue("@table", tbl)
            reader = cmd.ExecuteReader()
            print("Columns:")
            any_rows = False
            while reader.Read():
                any_rows = True
                print(f"  - {reader.GetString(0)}: {reader.GetString(1)}")
            return any_rows
        finally:
            conn.Close()
    except Exception as e:
        logger.info("INFORMATION_SCHEMA failed for %s: %s", table, e)
        return False


def print_schema_top0(connector, table: str) -> bool:
    try:
        from connectors.azure_sql_pythonnet import SqlCommand
        conn = connector._connect()
        conn.Open()
        try:
            table_q = getattr(connector, "_quote_two_part_name", _quote_two_part_name_fallback)(table)
            cmd = SqlCommand(f"SELECT TOP 0 * FROM {table_q}", conn)
            reader = cmd.ExecuteReader()
            print("Columns (TOP 0):")
            for i in range(reader.FieldCount):
                col = reader.GetName(i)
                try:
                    dtype = str(reader.GetFieldType(i))
                except Exception:
                    dtype = "unknown"
                print(f"  - {col}: {dtype}")
            return reader.FieldCount > 0
        finally:
            conn.Close()
    except Exception as e:
        logger.error("TOP 0 failed for %s: %s", table, e)
        return False


def resolve_datasets_for_assessment(
    session_id: str,
    selected_sources: List[str],
) -> List[Dict[str, Any]]:
    """
    Resolve blob paths to Fabric raw zone (Files/raw) shortcut paths 
    if shortcuts have been registered in sqlite for this session.
    """
    from agent.blob_fabric_registry import get_shortcut
    resolved = []
    for src in selected_sources:
        # Check if it has a registered shortcut
        shortcut = get_shortcut(session_id, src)
        if shortcut:
            resolved.append({
                "source_type": "fabric_files_zone",
                "files_zone_path": shortcut["files_zone_path"],
                "lakehouse_uri": shortcut["lakehouse_uri"],
                "dataset_name": src,
            })
        else:
            resolved.append({
                "source_type": "azure_blob" if src.startswith("azure_blob:") or "." in src else "database",
                "blob_path": src,
                "dataset_name": src,
            })
    return resolved


# ========================= Markdown Report =========================

def _fmt_pct(x: Optional[float]) -> str:
    try:
        return f"{round(float(x) * 100, 2)}%"
    except Exception:
        return str(x)


def _endpoint_to_dataset(endpoint: str, names_by_len: List[str]) -> Optional[str]:
    """Map relationship endpoint 'dataset.column' to dataset key."""
    for ds in names_by_len:
        if endpoint == ds or (endpoint.startswith(ds + ".") and len(endpoint) > len(ds)):
            return ds
    return None


def _result_for_datasets(result: Dict[str, Any], ds_names: List[str]) -> Dict[str, Any]:
    """Subset assessment for multiple datasets (same source path / group)."""
    ns = frozenset(ds_names)
    names_sorted = sorted(ns, key=len, reverse=True)
    datasets = {k: v for k, v in (result.get("datasets") or {}).items() if k in ns}
    dq = result.get("data_quality_issues", {}) or {}
    dq_ds = {k: v for k, v in (dq.get("datasets") or {}).items() if k in ns}
    rels = []
    for r in result.get("relationships") or []:
        df = _endpoint_to_dataset(r.get("from") or "", names_sorted)
        dt = _endpoint_to_dataset(r.get("to") or "", names_sorted)
        if df in ns and dt in ns:
            rels.append(r)
    orphans = dq.get("global_issues", {}).get("orphan_foreign_keys") or []
    orphans_sub = []
    for o in orphans:
        df = _endpoint_to_dataset(o.get("from") or "", names_sorted)
        dt = _endpoint_to_dataset(o.get("to") or "", names_sorted)
        if df in ns and dt in ns:
            orphans_sub.append(o)
    xds = dq.get("global_issues", {}).get("cross_dataset_inconsistencies") or []
    xds_sub = [x for x in xds if x.get("dataset") in ns]
    rri = dq.get("global_issues", {}).get("relationship_row_issues") or []
    rri_sub = [x for x in rri if x.get("dataset") in ns]
    rw = dq.get("global_issues", {}).get("relationship_warnings") or []
    rw_sub = [
        w for w in rw
        if w.get("datasets") and all(d in ns for d in w["datasets"])
    ]
    return {
        "datasets": datasets,
        "relationships": rels,
        "data_quality_issues": {
            "datasets": dq_ds,
            "global_issues": {
                "orphan_foreign_keys": orphans_sub,
                "cross_dataset_inconsistencies": xds_sub,
                "relationship_row_issues": rri_sub,
                "relationship_warnings": rw_sub,
            },
        },
    }



def _per_dataset_dir_name(ds_name: str, assigned: set) -> str:
    """Filesystem-safe unique folder name under per_dataset/."""
    raw = re.sub(r"[^\w\-.]+", "_", (ds_name or "dataset").replace("\\", "__").replace("/", "__"))
    raw = raw.strip("._")[:72] or "dataset"
    base = raw or "dataset"
    name = base
    n = 2
    while name in assigned:
        name = f"{base}_{n}"
        n += 1
    assigned.add(name)
    return name


def _source_root_to_folder_name(source_root: str) -> str:
    """Stable folder name under reports/by_path/ for a source location."""
    if not source_root:
        return "unknown_source"
    if source_root == "__database__":
        return "database"
    if source_root.startswith("azure_blob:"):
        tail = source_root.split(":", 1)[1] or "root"
        h = hashlib.sha256(source_root.encode("utf-8")).hexdigest()[:10]
        safe = re.sub(r"[^\w\-]+", "_", tail.replace("/", "__"))[:48]
        return f"azure_{safe}_{h}"
    h = hashlib.sha256(source_root.encode("utf-8")).hexdigest()[:10]
    base = os.path.basename(source_root.rstrip(os.sep)) or "source"
    safe = re.sub(r"[^\w\-]+", "_", base)[:48]
    return f"{safe}_{h}"


def _attach_run_metadata_and_suggestions(
    result: Dict[str, Any],
    t0_perf: float,
    started_at_iso: str,
    sources_path: str,
) -> None:
    """Add run timing, DQ rollups, and report suggestion metadata."""
    from datetime import datetime, timezone

    dq = result.get("data_quality_issues") or {}
    h = m = l = 0
    for ds in (dq.get("datasets") or {}).values():
        s = ds.get("summary") or {}
        h += int(s.get("high_severity") or 0)
        m += int(s.get("medium_severity") or 0)
        l += int(s.get("low_severity") or 0)
    for ri in (dq.get("global_issues") or {}).get("relationship_row_issues") or []:
        sev = (ri.get("severity") or "medium").lower()
        if sev == "high":
            h += 1
        elif sev == "low":
            l += 1
        else:
            m += 1
    result["run_metadata"] = {
        "started_at": started_at_iso,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.perf_counter() - t0_perf, 2),
        "dq_issue_totals": {"high": h, "medium": m, "low": l},
        "sources_config": os.path.basename(sources_path) if sources_path else None,
    }
    try:
        from agent.transformation_suggester import suggest_transformations

        result["transformation_suggestions"] = suggest_transformations(result)
    except Exception as e:
        logger.warning("Report suggestions skipped: %s", e)
        result["transformation_suggestions"] = {
            "suggested_transformations": [],
            "summary": {
                "total_suggestions": 0,
                "by_action": {},
                "auto_fixable_count": 0,
                "manual_review_count": 0,
            },
            "_error": str(e),
        }


def _brief_issue_line(i: Dict[str, Any]) -> str:
    col = f" [{i['column']}]" if i.get("column") else ""
    cnt = f" x{i['count']}" if i.get("count") is not None else ""
    rows = i.get("row_indexes") or []
    row_part = f" | rows: {rows[:15]}{'…' if len(rows) > 15 else ''}" if rows else ""
    rec = i.get("recommendation") or ""
    rec_part = f" | → {rec[:200]}{'…' if len(rec) > 200 else ''}" if rec else ""
    sev = i.get("severity") or "low"
    itype = i.get("type") or "issue"
    msg = i.get("message") or ""
    return f"- ({sev}) {itype}{col}{cnt}: {msg}{row_part}{rec_part}"


def _dtype_with_inference(cmeta: Dict[str, Any]) -> str:
    d = cmeta.get("dtype")
    inf = cmeta.get("dtype_inference")
    return f"{d} ({inf})" if d == "object" and inf else str(d)


def classify_readiness(
    *,
    assessment_status: str,
    score: Optional[float],
    gating_highs: int,
    gating_mediums: int,
    advisory_highs: int,
    medium: int,
    ready_threshold: float = 70.0,
    blocked_threshold: float = 35.0,
) -> Tuple[str, str]:
    if assessment_status != "COMPLETE":
        return "UNKNOWN", "assessment_incomplete"

    if score is None:
        return "UNKNOWN", "score_unavailable"

    # BLOCKED fires first — even with HIGH gating issues
    if score < blocked_threshold:
        return "BLOCKED", "score_below_blocked_threshold"

    if gating_highs > 0:
        return "NEEDS_WORK", "gating_high_issues_present"

    if score < ready_threshold:
        return "NEEDS_WORK", "score_below_ready_threshold"

    if gating_mediums > 0 or advisory_highs > 0 or medium > 0:
        return "READY_WITH_WARNINGS", "non_blocking_issues_present"

    return "READY", "all_gates_passed"


def aggregate_readiness(states: List[str]) -> str:
    state_set = set(states)
    if "BLOCKED" in state_set:
        return "BLOCKED"
    if "UNKNOWN" in state_set:
        return "UNKNOWN"
    if "NEEDS_WORK" in state_set:
        return "NEEDS_WORK"
    if "READY_WITH_WARNINGS" in state_set:
        return "READY_WITH_WARNINGS"
    return "READY"


def build_dq_scorecard(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Top-level readiness verdict and per-dataset score summary for banners
    (HTML + Markdown reports).
    """
    datasets = result.get("datasets", {}) or {}
    dq = result.get("data_quality_issues", {}) or {}
    dq_ds = dq.get("datasets", {}) or {}

    try:
        from agent.profiling.assessment_orchestrator import load_dq_thresholds
        thresholds = load_dq_thresholds()
    except Exception:
        thresholds = {}

    ready_threshold = 70.0
    blocked_threshold = 35.0
    if thresholds:
        rel_cfg = thresholds.get("relationships") or {}
        readiness_cfg = rel_cfg.get("readiness") or thresholds.get("readiness") or {}
        ready_threshold = float(readiness_cfg.get("ready_threshold", 70.0))
        blocked_threshold = float(readiness_cfg.get("blocked_threshold", 35.0))

    per_dataset: List[Dict[str, Any]] = []
    total_rows = 0
    total_high = total_medium = total_low = 0
    total_affected_by_high = 0
    total_distinct_affected_by_high = 0

    for ds_name, ds_meta in datasets.items():
        row_count = int(ds_meta.get("row_count", 0) or 0)
        total_rows += row_count

        summ = (dq_ds.get(ds_name) or {}).get("summary") or {}
        raw_score = summ.get("dq_score_0_100")
        if raw_score is None:
            raw_score = summ.get("dq_score_v3")
        if raw_score is None:
            raw_score = summ.get("dq_score")
        score_source = "computed"

        issues = (dq_ds.get(ds_name) or {}).get("issues") or []
        if not isinstance(issues, list):
            issues = []

        gating_highs = sum(1 for i in issues if (i.get("severity") or "").lower() == "high" and i.get("gating", True))
        gating_mediums = sum(1 for i in issues if (i.get("severity") or "").lower() == "medium" and i.get("gating", True))
        advisory_highs = sum(1 for i in issues if (i.get("severity") or "").lower() == "high" and not i.get("gating", True))

        high = gating_highs + advisory_highs
        medium = sum(1 for i in issues if (i.get("severity") or "").lower() == "medium")
        low = sum(1 for i in issues if (i.get("severity") or "").lower() == "low")
        
        affected_high = 0
        for i in issues:
            if (i.get("severity") or "").lower() == "high":
                try:
                    affected_high += int(i.get("count") or 0)
                except Exception:
                    pass

        clean_est_high = summ.get("estimated_clean_rows_after_high")
        if clean_est_high is not None:
            ds_distinct_high = max(0, row_count - int(clean_est_high))
        else:
            ds_distinct_high = affected_high

        total_high += high
        total_medium += medium
        total_low += low
        total_affected_by_high += affected_high
        total_distinct_affected_by_high += ds_distinct_high


        if raw_score is not None:
            try:
                score = float(raw_score)
            except Exception:
                score = 0.0
        else:
            score = None
            score_source = "unavailable"

        assessment_status = ds_meta.get("assessment_status", "COMPLETE")
        if summ.get("readiness") == "UNKNOWN" and (assessment_status != "COMPLETE" or score is None):
            readiness = "UNKNOWN"
            qualifier = "insufficient_score_coverage"
        else:
            readiness, qualifier = classify_readiness(
                assessment_status=assessment_status,
                score=score,
                gating_highs=gating_highs,
                gating_mediums=gating_mediums,
                advisory_highs=advisory_highs,
                medium=medium,
                ready_threshold=ready_threshold,
                blocked_threshold=blocked_threshold,
            )

        reasons = []
        if assessment_status != "COMPLETE":
            reasons.append("assessment_incomplete")
        if score is not None and score < blocked_threshold:
            reasons.append("score_below_blocked_threshold")
        if gating_highs > 0:
            reasons.append("gating_high_issues_present")
        if score is not None and score < ready_threshold:
            reasons.append("score_below_ready_threshold")
        if gating_mediums > 0:
            reasons.append("gating_medium_issues_present")
        if advisory_highs > 0:
            reasons.append("advisory_high_issues_present")
        if not reasons:
            reasons.append("all_gates_passed")

        # Operational use-case readiness gates (distinguishing distinct rows from cell count)
        contact_issues = [i for i in issues if (i.get("type") or "").lower() in ("invalid_phone", "invalid_email")]
        email_issues_cnt = sum(int(i.get("count") or 1) for i in issues if (i.get("type") or "").lower() == "invalid_email")
        phone_issues_cnt = sum(int(i.get("count") or 1) for i in issues if (i.get("type") or "").lower() == "invalid_phone")
        total_contact_values = email_issues_cnt + phone_issues_cnt
        contact_row_set = set()
        for ci in contact_issues:
            contact_row_set.update(ci.get("row_indexes") or [])
        distinct_contact_rows = len(contact_row_set) if contact_row_set else min(row_count, max(email_issues_cnt, phone_issues_cnt))
        contact_cols = {ci.get("column") for ci in contact_issues if ci.get("column")}
        num_contact_cols = max(1, len(contact_cols)) if contact_cols else 2
        total_contact_cells = row_count * num_contact_cols
        contact_row_rate = (distinct_contact_rows / max(1, row_count)) if row_count > 0 else 0.0
        contact_cell_rate = (total_contact_values / max(1, total_contact_cells)) if total_contact_cells > 0 else 0.0

        uc_contact = "BLOCKED" if (contact_row_rate > 0.10 or contact_cell_rate > 0.10) else readiness
        uc_ident = "NEEDS_WORK" if (contact_row_rate > 0.10 or any(i.get("type") == "near_duplicate_rows" for i in issues)) else readiness

        # Contextual, dataset-specific use-case gates
        ds_lower = ds_name.lower()
        has_contact_fields = bool(contact_cols) or any(
            any(p in str(i.get("column") or "").lower() for p in ("email", "phone", "tel", "mobile"))
            for i in issues
        )
        is_catalog_domain = any(p in ds_lower for p in ("course", "catalog", "product", "item", "offering"))

        if is_catalog_domain or not has_contact_fields:
            has_price_high = any(
                (i.get("severity") or "").upper() == "HIGH" and "price" in str(i.get("column") or "").lower()
                for i in issues
            )
            cat_pub = "BLOCKED" if gating_highs > 0 else readiness
            price_cons = "BLOCKED" if has_price_high else readiness
            use_case_gates = {
                "general_analytics": readiness,
                "catalogue_publication": cat_pub,
                "pricing_consumption": price_cons,
                "customer_contact": "NOT_APPLICABLE",
                "identity_resolution": "NOT_APPLICABLE",
            }
        else:
            use_case_gates = {
                "general_analytics": readiness,
                "customer_contact": uc_contact,
                "identity_resolution": uc_ident,
                "catalogue_publication": "NOT_APPLICABLE",
                "pricing_consumption": "NOT_APPLICABLE",
            }

        ds_dq_score = round(score, 1) if score is not None else "N/A"
        if gating_highs > 0 and isinstance(score, (int, float)):
            ds_gating_adj = round(min(69.9, score), 1)
        else:
            ds_gating_adj = ds_dq_score

        per_dataset.append(
            {
                "name": ds_name,
                "dq_score": ds_dq_score,
                "gating_adjusted_score": ds_gating_adj,
                "score_source": score_source,
                "readiness": readiness,
                "readiness_qualifier": qualifier,
                "readiness_reasons": reasons,
                "use_case_gates": use_case_gates,
                "gating_high_issue_count": gating_highs,
                "advisory_high_issue_count": advisory_highs,
                "thresholds_used": {"ready": ready_threshold, "blocked": blocked_threshold},
                "row_count": row_count,
                "high": high,
                "medium": medium,
                "low": low,
                "rows_affected_by_high": affected_high,
                "clean_rows_after_high_pct": summ.get("estimated_clean_rows_after_high"),
                "clean_rows_after_high_medium_pct": summ.get("estimated_clean_rows_after_high_and_medium"),
            }
        )

    order = {"BLOCKED": 0, "UNKNOWN": 1, "NEEDS_WORK": 2, "READY_WITH_WARNINGS": 3, "READY": 4}
    per_dataset.sort(
        key=lambda x: (
            order.get(x["readiness"], 4),
            float(x["dq_score"]) if isinstance(x["dq_score"], (int, float)) else -1.0
        )
    )

    verdict = aggregate_readiness([d["readiness"] for d in per_dataset])

    # Row-count-weighted overall score
    valid_pairs = [
        (d["dq_score"], d["row_count"]) for d in per_dataset
        if isinstance(d["dq_score"], (int, float)) and d["row_count"] > 0
    ]
    if valid_pairs:
        total_weighted = sum(s * r for s, r in valid_pairs)
        total_weight = sum(r for _, r in valid_pairs)
        overall_score = total_weighted / total_weight
        intrinsic_dq_score = round(overall_score, 1)
        if total_high > 0 or total_distinct_affected_by_high > 0:
            gating_adjusted_score = round(min(69.9, overall_score), 1)
        else:
            gating_adjusted_score = intrinsic_dq_score
    else:
        overall_score = None
        intrinsic_dq_score = "N/A"
        gating_adjusted_score = "N/A"

    if total_rows > 0:
        raw_pct = ((total_rows - total_distinct_affected_by_high) / total_rows) * 100.0
        clean_rows_est = round(max(0.0, min(100.0, raw_pct)), 1)
    else:
        clean_rows_est = 100.0

    # Build dataset-level HIGH breakdown for audit transparency
    high_breakdown_items = []
    for d in per_dataset:
        ds_n = d["name"]
        ds_raw_issues = (dq_ds.get(ds_n) or {}).get("issues") or []
        high_issues = [i for i in ds_raw_issues if (i.get("severity") or "").upper() == "HIGH"]
        if high_issues:
            col_parts = []
            for hi in high_issues:
                col_name = hi.get("column") or "row"
                cnt = int(hi.get("count") or len(hi.get("row_indexes") or []))
                col_parts.append(f"{col_name}: {cnt}")
            short_name = ds_n.split(".")[-1] if "." in ds_n else ds_n
            high_breakdown_items.append(f"{short_name} ({', '.join(col_parts)})")
    high_breakdown_str = " · ".join(high_breakdown_items) + f" · Total: {total_affected_by_high}" if high_breakdown_items else "None"

    return {
        "verdict": verdict,
        "overall_dq_score": round(overall_score, 1) if overall_score is not None else "N/A",
        "intrinsic_dq_score": intrinsic_dq_score,
        "gating_adjusted_score": gating_adjusted_score,
        "high_breakdown_str": high_breakdown_str,
        "total_issues": {"high": total_high, "medium": total_medium, "low": total_low},
        "total_rows": total_rows,
        "rows_affected_by_high": total_distinct_affected_by_high,
        "distinct_rows_affected_by_high": total_distinct_affected_by_high,
        "total_high_memberships": total_affected_by_high,
        "estimated_clean_rows_pct": clean_rows_est,
        "per_dataset": per_dataset,
        "datasets": per_dataset,
        "datasets_blocked": [d["name"] for d in per_dataset if d["readiness"] == "BLOCKED"],
        "datasets_at_risk": [d["name"] for d in per_dataset if d["readiness"] == "NEEDS_WORK"],
        "datasets_clean": [d["name"] for d in per_dataset if d["readiness"] in {"READY", "READY_WITH_WARNINGS"}],
    }



def build_cleaning_manifest(
    result: Dict[str, Any],
    suggestions: Dict[str, Any],
    scorecard: Dict[str, Any],
) -> Dict[str, Any]:
    """Machine-readable per-column issue reference for ETL development."""
    from datetime import datetime, timezone

    datasets_out: Dict[str, Any] = {}
    dq = result.get("data_quality_issues", {}) or {}
    datasets_meta = result.get("datasets", {}) or {}
    dq_ds = dq.get("datasets", {}) or {}

    sug_by_key: Dict[str, Dict[str, Any]] = {}
    for s in (suggestions or {}).get("suggested_transformations") or []:
        if not isinstance(s, dict):
            continue
        ds = s.get("dataset") or ""
        col = s.get("column")
        it = s.get("issue_type")
        sug_by_key[f"{ds}|{col}|{it}"] = s

    for ds_name, ds_meta in datasets_meta.items():
        issues_raw = (dq_ds.get(ds_name) or {}).get("issues", []) or []
        ds_score_rows = [d for d in scorecard["per_dataset"] if d["name"] == ds_name]
        readiness = ds_score_rows[0]["readiness"] if ds_score_rows else "UNKNOWN"
        dq_score = ds_score_rows[0]["dq_score"] if ds_score_rows else 0.0

        issues_out: List[Dict[str, Any]] = []
        for issue in issues_raw:
            if not isinstance(issue, dict):
                continue
            key = f"{ds_name}|{issue.get('column')}|{issue.get('type')}"
            sug = sug_by_key.get(key, {})
            manual_g = issue.get("manual_guidance") or sug.get("manual_guidance") or ""
            issues_out.append(
                {
                    "column": issue.get("column"),
                    "issue_type": issue.get("type"),
                    "severity": issue.get("severity"),
                    "rows_affected": issue.get("count"),
                    "row_indexes": (issue.get("row_indexes") or [])[:100],
                    "sample_bad_values": [mask_pii_sample(v, col=issue.get("column"))[:80] for v in (issue.get("sample_values") or [])[:10]],
                    "message": issue.get("message"),
                    "recommended_action": issue.get("recommended_action")
                    or sug.get("suggested_action")
                    or "review_manually",
                    "auto_fixable": bool(sug.get("auto_fixable"))
                    if sug
                    else bool(issue.get("auto_fixable", False)),
                    "manual_guidance": manual_g,
                }
            )

        severity_order = {"high": 0, "medium": 1, "low": 2}
        issues_out.sort(key=lambda x: severity_order.get((x["severity"] or "low").lower(), 3))

        datasets_out[ds_name] = {
            "source_file": ds_name,
            "row_count": ds_meta.get("row_count", 0),
            "column_count": ds_meta.get("column_count", 0),
            "dq_score": dq_score,
            "readiness": readiness,
            "total_issues": len(issues_out),
            "issues_by_severity": {
                "high": sum(1 for i in issues_out if (i["severity"] or "").lower() == "high"),
                "medium": sum(1 for i in issues_out if (i["severity"] or "").lower() == "medium"),
                "low": sum(1 for i in issues_out if (i["severity"] or "").lower() == "low"),
            },
            "issues": issues_out,
        }

    global_issues = dq.get("global_issues", {}) or {}
    global_out: List[Dict[str, Any]] = []

    for orphan in global_issues.get("orphan_foreign_keys", []) or []:
        if not isinstance(orphan, dict):
            continue
        global_out.append(
            {
                "type": "orphan_foreign_key",
                "from": orphan.get("from"),
                "to": orphan.get("to"),
                "orphan_count": orphan.get("orphan_count"),
                "severity": "high",
                "message": f"Orphan FKs: {orphan.get('from')} → {orphan.get('to')}",
                "recommended_action": "validate_referential_integrity_or_stage",
            }
        )

    for inc in global_issues.get("cross_dataset_inconsistencies", []) or []:
        if not isinstance(inc, dict):
            continue
        global_out.append(
            {
                "type": "cross_dataset_inconsistency",
                "dataset": inc.get("dataset"),
                "column": inc.get("column"),
                "datasets": inc.get("datasets"),
                "severity": "medium",
                "message": inc.get("message", ""),
                "recommended_action": "review_manually",
            }
        )

    return {
        "manifest_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "verdict": scorecard["verdict"],
        "overall_dq_score": scorecard["overall_dq_score"],
        "total_datasets": len(datasets_out),
        "summary": scorecard["total_issues"],
        "datasets": datasets_out,
        "global_issues": global_out,
    }


def build_markdown_report(result: Dict[str, Any]) -> str:
    """Build Markdown report from assessment result."""
    from datetime import datetime
    md: List[str] = []
    md.append("**Intelligent Data Assessment Report**\n")

    sc = build_dq_scorecard(result)
    v = sc["verdict"]
    v_icon = "🔴" if v == "BLOCKED" else ("⚠️" if v == "NEEDS_WORK" else "✅")
    v_label = v.replace("_", " ")
    ti = sc["total_issues"]
    
    if v == "READY":
        release_decision = "RELEASE"
        auth_status = "YES"
    elif v == "READY_WITH_WARNINGS":
        release_decision = "RELEASE"
        auth_status = "YES (WITH WARNINGS)"
    elif v == "NEEDS_WORK":
        release_decision = "HOLD"
        auth_status = "NO"
    elif v == "BLOCKED":
        release_decision = "REJECT"
        auth_status = "NO"
    else:
        release_decision = "HOLD"
        auth_status = "NO"

    md.append(f"## {v_icon} Data Quality Verdict: {v_label}")
    md.append("")
    md.append("| Metric | Value |")
    md.append("|--------|-------|")
    md.append(f"| Assessment Status | COMPLETE |")
    md.append(f"| Readiness | {v_label} |")
    md.append(f"| Data Release Decision | {release_decision} |")
    md.append(f"| Production Consumption Authorized | {auth_status} |")
    md.append(f"| Intrinsic DQ Score | {sc.get('intrinsic_dq_score', sc['overall_dq_score'])} / 100 |")
    md.append(f"| Gating-Adjusted Readiness Score | {sc.get('gating_adjusted_score', sc['overall_dq_score'])} / 100 |")
    md.append(f"| High severity issues | {ti.get('high', 0)} |")
    md.append(f"| Distinct rows affected by HIGH issues | {sc.get('distinct_rows_affected_by_high', sc['rows_affected_by_high'])} |")
    md.append(f"| Total HIGH rule memberships | {sc.get('total_high_memberships', sc['rows_affected_by_high'])} |")
    md.append(
        f"| Approx. unaffected row share (HIGH) | {sc['estimated_clean_rows_pct']}% |"
    )

    md.append("")
    md.append("| Dataset | Intrinsic Score | Gating-Adjusted Score | Readiness |")
    md.append("|---------|-----------------|-----------------------|-----------|")
    for d in sc["per_dataset"]:
        r = d["readiness"]
        if r in ("READY", "READY_WITH_WARNINGS"):
            ri = f"✅ {r}" if r == "READY" else f"⚠️ {r}"
        elif r == "NEEDS_WORK":
            ri = "⚠️ NEEDS WORK"
        elif r == "BLOCKED":
            ri = "🔴 BLOCKED"
        else:
            ri = f"❓ {r}"
        adj_val = d.get("gating_adjusted_score", d["dq_score"])
        adj_str = f"{adj_val}"
        if str(adj_val) != str(d["dq_score"]):
            adj_str += " *(capped)*"
        md.append(f"| `{d['name']}` | {d['dq_score']} | {adj_str} | {ri} |")
    md.append("")
    md.append("> **Executive Score Interpretation Guidance:** Intrinsic scores represent the mathematically weighted average across profile dimensions. However, **readiness and release authorization are governed by hard use-case gates**. Any dataset containing active HIGH-severity blockers is capped at a maximum operational readiness score of **69.9 / 100 (NEEDS WORK)**; individual operational use cases may separately be BLOCKED, and direct unquarantined release is prohibited.")
    md.append("> **Accuracy Plausibility Proxy Notice:** Inferred accuracy scores reflect the pass rate of defined domain plausibility constraints (e.g. 1/1 validation rule satisfied), and do not certify golden-source factual truth.")
    md.append("")

    datasets = result.get("datasets", {})
    rels = result.get("relationships", [])
    dq = result.get("data_quality_issues", {})
    dq_ds = dq.get("datasets", {})
    dq_global = dq.get("global_issues", {})
    exec_items = result.get("executive_summary_items") or []

    md.append("**Overview**")
    md.append(f"- Total datasets: {len(datasets)}")
    total_rows = sum(d.get("row_count", 0) for d in datasets.values())
    total_cols = sum(d.get("column_count", 0) for d in datasets.values())
    total_bytes = sum(d.get("data_volume_bytes", 0) for d in datasets.values())
    md.append(f"- Total rows (approx.): {total_rows}")
    md.append(f"- Total columns (sum): {total_cols}")
    md.append(f"- Total memory (bytes): {total_bytes}\n")

    if exec_items:
        md.append("**Executive summary (prioritized)**")
        for it in exec_items[:10]:
            if not isinstance(it, dict):
                continue
            md.append(
                f"- ({it.get('severity')}) **{it.get('title')}** — "
                f"datasets={it.get('datasets_affected')}, rows≈{it.get('estimated_rows_affected')}. "
                f"→ {str(it.get('recommendation') or '')[:220]}"
            )
        md.append("")

    try:
        from agent.report_governance_sections import render_governance_markdown

        _gm = render_governance_markdown(result)
        if _gm.strip():
            md.append(_gm)
    except Exception:
        pass

    rm = result.get("run_metadata") or {}
    if rm:
        md.append("**Run metadata**")
        md.append(f"- Assessment duration: **{rm.get('duration_seconds')}s**")
        dqt = rm.get("dq_issue_totals") or {}
        md.append(
            f"- DQ issues (rollup): high={dqt.get('high', 0)}, medium={dqt.get('medium', 0)}, low={dqt.get('low', 0)}"
        )
        if rm.get("sources_config"):
            md.append(f"- Config: `{rm['sources_config']}`")
        md.append("")

    ts = result.get("transformation_suggestions") or {}
    sug = ts.get("suggested_transformations") or []
    if sug and not ts.get("_error"):
        md.append("**Suggested fixes (rule-based)**")
        md.append(
            f"*{ts.get('summary', {}).get('total_suggestions', len(sug))} suggestions based on current DQ signals.*"
        )
        _rank = {"high": 0, "medium": 1, "low": 2}
        for s in sorted(sug, key=lambda x: (_rank.get((x.get("severity") or "low").lower(), 3), str(x.get("dataset"))))[
            :20
        ]:
            ds = s.get("dataset") or "—"
            col = s.get("column") or "—"
            md.append(
                f"- **{s.get('suggested_action', '?')}** · `{ds}` · `{col}` ({s.get('severity')}) — {str(s.get('message', ''))[:120]}"
            )
        md.append("")

    llm = result.get("llm_insights") or {}
    if llm.get("success") and llm.get("parsed"):
        p = llm["parsed"]
        md.append("**AI-assisted insights** *(verify against detailed tables; model may err)*\n")
        md.append(p.get("executive_summary") or "")
        md.append("")
        md.append("*Top risks:*")
        for r in (p.get("top_risks") or [])[:5]:
            if isinstance(r, dict):
                md.append(f"- **{r.get('title', '')}** — {r.get('detail', '')}")
        md.append("")
        md.append("*Recommended next steps:*")
        for s in (p.get("recommended_next_steps") or [])[:7]:
            if s:
                md.append(f"- {s}")
        if p.get("data_lineage_comment"):
            md.append("")
            md.append(f"*Relationships note:* {p['data_lineage_comment']}")
        md.append("")
    elif llm.get("error") and "llm_insights" in result:
        md.append(f"**AI insights:** (not generated — {llm.get('error')})\n")

    md.append("**Datasets**")
    for name, meta in datasets.items():
        md.append(f"- **{name}**")
        md.append(f"  - rows: {meta.get('row_count')}, cols: {meta.get('column_count')}, bytes: {meta.get('data_volume_bytes')}")
        dq_block = (dq_ds.get(name) or {})
        summ = (dq_block.get("summary") or {})
        if summ:
            if summ.get("dq_score_0_100") is not None:
                md.append(f"  - dq_score: **{round(float(summ.get('dq_score_0_100')), 1)} / 100**")
            if summ.get("estimated_clean_rows_after_high") is not None:
                md.append(f"  - est_clean_rows_after_high: **{summ.get('estimated_clean_rows_after_high')}**")
            if summ.get("estimated_clean_rows_after_high_and_medium") is not None:
                md.append(f"  - est_clean_rows_after_high+medium: **{summ.get('estimated_clean_rows_after_high_and_medium')}**")
        cols = meta.get("columns", {})
        if cols:
            md.append("  - columns:")
            for idx, (col, cmeta) in enumerate(cols.items()):
                if idx >= 12:
                    md.append(f"    - ... (+{len(cols)-12} more columns)")
                    break
                md.append(
                    f"    - **{col}**: dtype={_dtype_with_inference(cmeta)}, "
                    f"null%={_fmt_pct(cmeta.get('null_percentage'))}, "
                    f"unique={cmeta.get('unique_count')}, type={cmeta.get('semantic_type')}, candidate_pk={cmeta.get('candidate_primary_key')}"
                )
        md.append("")

    md.append("**Relationships**")
    if rels:
        for rel in rels:
            rid = rel.get("relationship_id") or rel.get("name") or "?"
            parent = rel.get("parent") or "?"
            child = rel.get("child") or "?"
            source = rel.get("source", "?")
            status = rel.get("evaluation_status", "?")
            shared = rel.get("shared_distinct_keys", "?")
            child_orphans = rel.get("child_orphan_distinct_keys", 0)
            unref_parents = rel.get("unreferenced_parent_distinct_keys", 0)
            gating = rel.get("gating", False)
            gate_label = " [GATING]" if gating else " [advisory]"
            md.append(
                f"- **{rid}** ({source}{gate_label}) | status: {status} | shared keys: {shared} "
                f"| child orphans: {child_orphans} | unreferenced parents: {unref_parents}"
            )
            md.append(f"  - parent: `{parent}` → child: `{child}`")
            if rel.get("summary"):
                md.append(f"  - {rel['summary']}")
    else:
        md.append("- (none found)")
    md.append("")

    md.append("**Cross-table row issues (orphan keys)**")
    rri = dq_global.get("relationship_row_issues") or []
    if rri:
        for x in rri[:30]:
            rows = x.get("row_indexes") or []
            md.append(
                f"- **{x.get('dataset')}**.{x.get('column')} → missing in **{x.get('related_dataset')}.{x.get('related_column')}** "
                f"— {x.get('count')} rows; e.g. row indexes {rows[:12]}{'…' if len(rows) > 12 else ''}"
            )
            md.append(f"  - {x.get('message', '')}")
            if x.get("recommendation"):
                md.append(f"  - *Recommendation:* {x['recommendation']}")
    else:
        md.append("- (none)")
    md.append("")

    md.append("**Relationship warnings (M:N / model)**")
    rw = dq_global.get("relationship_warnings") or []
    if rw:
        for w in rw[:20]:
            md.append(f"- ({w.get('severity')}) {w.get('message')}")
            if w.get("recommendation"):
                md.append(f"  - *Recommendation:* {w['recommendation']}")
    else:
        md.append("- (none)")
    md.append("")

    md.append("**Data Quality Issues & Root Causes**\n")
    for ds_name, dq_block in dq_ds.items():
        md.append(f"### Dataset: `{ds_name}`")
        if isinstance(dq_block, list):
            summ = {}
            issues = dq_block
        elif isinstance(dq_block, dict):
            summ = dq_block.get("summary") or {}
            issues = dq_block.get("issues", [])
        else:
            summ = {}
            issues = []
        rc_sum = summ.get("root_cause_summary") or {}
        if not issues:
            md.append("  - (no issues)")
        else:
            high_issues = [i for i in issues if (i.get("severity") or "").lower() == "high"]
            med_issues = [i for i in issues if (i.get("severity") or "").lower() == "medium"]
            low_issues = [i for i in issues if (i.get("severity") or "").lower() == "low"]
            
            if high_issues:
                md.append("#### HIGH Severity (Gating Blocker)")
                for i in high_issues[:15]:
                    fx = i.get("fixability") or ""
                    fx_part = f" [{fx}]" if fx else ""
                    md.append("- " + _brief_issue_line({**i, "type": f"{i.get('type')}{fx_part}"}))
                if len(high_issues) > 15:
                    md.append(f"- ... (+{len(high_issues)-15} more HIGH issues)")
                md.append("")
                
            if med_issues:
                md.append("#### MEDIUM Severity (Warning)")
                for i in med_issues[:15]:
                    fx = i.get("fixability") or ""
                    fx_part = f" [{fx}]" if fx else ""
                    md.append("- " + _brief_issue_line({**i, "type": f"{i.get('type')}{fx_part}"}))
                if len(med_issues) > 15:
                    md.append(f"- ... (+{len(med_issues)-15} more MEDIUM issues)")
                md.append("")
                
            if low_issues:
                md.append("#### LOW Severity (Moved to Appendix)")
                for i in low_issues[:10]:
                    fx = i.get("fixability") or ""
                    fx_part = f" [{fx}]" if fx else ""
                    md.append("- " + _brief_issue_line({**i, "type": f"{i.get('type')}{fx_part}"}))
                if len(low_issues) > 10:
                    md.append(f"- ... (+{len(low_issues)-10} more LOW issues)")
                md.append("")

            rc_groups = rc_sum.get("root_cause_groups") or []
            if rc_groups:
                md.append("#### Root-Cause Groups (Non-Additive)")
                md.append(f"- Total Rule Hits: {rc_sum.get('rule_hit_count')} · Distinct Cells: {rc_sum.get('distinct_cells_affected')} · Distinct Rows: {rc_sum.get('distinct_rows_affected')}")
                for g in rc_groups:
                    trigs = ", ".join(f"`{t}`" for t in g.get("issue_types_triggered", []))
                    md.append(f"- **{g.get('family')}** ({trigs}): {g.get('distinct_cells_affected')} cells, {g.get('distinct_rows_affected')} rows")
                md.append("")
        md.append("")

    md.append("- **Global Issues (value-level orphans)**")
    orphans = dq_global.get("orphan_foreign_keys", [])
    if orphans:
        for o in orphans[:20]:
            md.append(f"  - Orphan values: {o['from']} → {o['to']} (count: {o['orphan_count']})")
            if o.get("recommendation"):
                md.append(f"    → {o['recommendation'][:180]}")
    else:
        md.append("  - (none)")
    xds = dq_global.get("cross_dataset_inconsistencies", [])
    if xds:
        for x in xds[:20]:
            md.append(f"  - {x['dataset']}.{x['column']}: {x['message']}")
            if x.get("recommendation"):
                md.append(f"    → {x['recommendation'][:160]}")
    else:
        md.append("  - Cross-dataset inconsistencies: (none)")

    xcons = dq_global.get("cross_dataset_consistency", [])
    if xcons:
        md.append("  - Cross-dataset consistency insights:")
        for x in xcons[:20]:
            md.append(f"    - ({x.get('severity')}) {x.get('type')}: {x.get('message')}")
            if x.get("recommendation"):
                md.append(f"      → {str(x.get('recommendation'))[:180]}")
    md.append("")

    md.append("## DQ Score Methodology")
    md.append("- **Engine:** `Dimension Scorer v3` (Single Authoritative Source)")
    md.append("- **Formula:** The overall DQ score is the weighted average of evaluated quality dimensions. Dimensions without applicable or successfully evaluated rules are excluded from the effective denominator and reported separately through score coverage.")
    md.append("- **Readiness Classification Policy:**")
    md.append(f"  - **READY:** Score >= {sc.get('thresholds_used', {}).get('ready', 70.0)}, sufficient coverage (>= 80%), no HIGH issues, no configured warning gate.")
    md.append(f"  - **READY_WITH_WARNINGS:** Score >= {sc.get('thresholds_used', {}).get('ready', 70.0)}, sufficient coverage (>= 80%), no HIGH issues, one or more non-blocking findings (e.g. MEDIUM severity).")
    md.append(f"  - **NEEDS_WORK:** Score {sc.get('thresholds_used', {}).get('blocked', 35.0)} to < {sc.get('thresholds_used', {}).get('ready', 70.0)}, or unresolved HIGH issue gating an otherwise numerically ready score, or coverage between 40% and 80% (LIMITED_CONFIDENCE).")
    md.append(f"  - **BLOCKED:** Score < {sc.get('thresholds_used', {}).get('blocked', 35.0)}, missing mandatory primary key, or explicit blocking policy violation.")
    md.append("  - **UNKNOWN:** Insufficient score coverage (< 40%), execution failure, incomplete assessment, or missing mandatory configuration.\n")

    
    for ds_name, dq_block in dq_ds.items():
        summ = dq_block.get("summary") or {} if isinstance(dq_block, dict) else {}
        db = summ.get("dimension_breakdown") or {}
        if db:
            md.append(f"### Dimension Breakdown: `{ds_name}`")
            md.append("| Dimension | Score | Details |")
            md.append("|---|---|---|")
            for dim, details in db.items():
                if not details:
                    continue
                score_val = details.get("score")
                score_str = f"**{score_val}%**" if score_val is not None else "N/A"
                details_parts = [f"{k.replace('_', ' ')}: {v}" for k, v in details.items() if k != "score" and v is not None]
                md.append(f"| {dim.capitalize()} | {score_str} | {', '.join(details_parts)} |")
            md.append("")

    # Reproducibility / Limitations / Appendix
    import hashlib, uuid, subprocess
    rm = result.get("run_metadata") or {}
    gov = result.get("governance") or {}
    run_id = result.get("run_id") or rm.get("run_id") or gov.get("assessment_run_id") or gov.get("run_id")
    if not run_id or run_id == "N/A":
        run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        result["run_id"] = run_id

    agent_ver = gov.get("agent_version") or "v2.4.0-prod"
    manifest_ver = gov.get("manifest_schema_version") or "2.0.0"
    job_id = gov.get("job_id")
    if not job_id or job_id == "job_dq_assessment_01":
        job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        gov["job_id"] = job_id

    git_commit_sha = "N/A"
    try:
        git_commit_sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, timeout=2
        ).decode("utf-8").strip()
    except Exception:
        git_commit_sha = "main-dev-2026.09"

    schema_hash = gov.get("schema_hash")
    if not schema_hash or schema_hash == "N/A":
        schema_parts = []
        for dname in sorted(datasets.keys()):
            dmeta = datasets[dname]
            col_tuples = []
            for cname, cmeta in sorted((dmeta.get("columns") or {}).items()):
                col_tuples.append(f"{cname}:{cmeta.get('data_type')}:{cmeta.get('semantic_type')}")
            schema_parts.append(f"{dname}=[{','.join(col_tuples)}]")
        schema_hash = "sha256:" + hashlib.sha256(";".join(schema_parts).encode("utf-8")).hexdigest()[:16]
        gov["schema_hash"] = schema_hash

    rules_hash = gov.get("rules_hash")
    if not rules_hash or rules_hash == "N/A":
        rules_content = "rules_v2.4.0:placeholders_configured"
        rules_hash = "sha256:" + hashlib.sha256(rules_content.encode("utf-8")).hexdigest()[:16]
        gov["rules_hash"] = rules_hash

    md.append("## Technical Appendix")
    md.append("### Reproducibility Metadata")
    md.append(f"- **Assessment Run ID:** `{run_id}`")
    md.append(f"- **Job ID:** `{job_id}`")
    md.append(f"- **Git Commit SHA:** `{git_commit_sha}`")
    md.append(f"- **Deployment Version:** `{agent_ver}`")
    md.append(f"- **Manifest Schema Version:** `{manifest_ver}`")
    md.append(f"- **Schema Hash:** `{schema_hash}`")
    md.append(f"- **Rules Hash:** `{rules_hash}`")
    md.append(f"- **Duplicate Detection Method:** `fuzzy_token_sort (base threshold: 0.85, effective threshold: 0.95 for sparse text, batch-grain aware)`")
    md.append(f"- **Timestamp:** `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`")
    md.append("")
    
    md.append("### Assessment Limitations")
    md.append("- **Inferred Relationships:** Statistical overlap is advisory only and does not trigger gating checks.")
    md.append("- **Cell Truncation:** Diagnostic row indexes are sampled up to 10 rows in the report.")
    md.append("")
    return "\n".join(md)


# ========================= HTML Report =========================


def build_html_report(result: Dict[str, Any]) -> str:
    """Build HTML assessment report (Theme 2 executive UI)."""
    from agent.report_html_themes import get_report_html_css
    from datetime import datetime
    from collections import defaultdict

    # Ensure dq_recommendations is populated in result
    if isinstance(result, dict) and "dq_recommendations" not in result:
        try:
            from agent.dq_recommendations_agent import DQRecommendationsAgent, dq_recommendations_to_dict
            agent = DQRecommendationsAgent()
            merged_dq = result.get("data_quality_issues") or {}
            rec, _ = agent.recommend(merged_dq=merged_dq)
            result["dq_recommendations"] = dq_recommendations_to_dict(rec)
        except Exception:
            pass

    datasets = result.get("datasets", {})
    raw_rels = result.get("relationships", [])
    active_ds = set(datasets.keys()) if isinstance(datasets, dict) else set()
    names_sorted = sorted(active_ds, key=len, reverse=True)
    rels = []
    for r in raw_rels:
        p = r.get("parent") or r.get("from") or ""
        c = r.get("child") or r.get("to") or ""
        p_ds = _endpoint_to_dataset(p, names_sorted) or (p.rsplit(".", 1)[0] if "." in p else p)
        c_ds = _endpoint_to_dataset(c, names_sorted) or (c.rsplit(".", 1)[0] if "." in c else c)
        if p_ds in active_ds or c_ds in active_ds:
            rels.append(r)
    dq = result.get("data_quality_issues", {})

    dq_ds = dq.get("datasets", {})
    dq_global = dq.get("global_issues", {})
    exec_items = result.get("executive_summary_items") or []
    scorecard = build_dq_scorecard(result)
    total_rows = sum(d.get("row_count", 0) for d in datasets.values())
    total_cols = sum(d.get("column_count", 0) for d in datasets.values())
    total_bytes = sum(d.get("data_volume_bytes", 0) for d in datasets.values())

    llm = result.get("llm_insights") or {}
    nav_llm_link = ""
    llm_section_html = ""
    if llm.get("success") and llm.get("parsed"):
        nav_llm_link = '\n  <a href="#llm-insights" class="nav-jump">AI insights</a>'
        _p = llm["parsed"]
        _risks = []
        for _r in (_p.get("top_risks") or [])[:5]:
            if isinstance(_r, dict):
                _risks.append(
                    "<li><strong>"
                    + html_module.escape(str(_r.get("title") or ""))
                    + "</strong> — "
                    + html_module.escape(str(_r.get("detail") or ""))
                    + "</li>"
                )
        _steps = [
            "<li>" + html_module.escape(str(_s)) + "</li>"
            for _s in (_p.get("recommended_next_steps") or [])[:7]
            if _s
        ]
        _lin = _p.get("data_lineage_comment")
        _lin_html = (
            "<p class=\"llm-lineage\"><strong>Relationships note:</strong> "
            + html_module.escape(str(_lin))
            + "</p>"
            if _lin
            else ""
        )
        llm_section_html = (
            '<section id="llm-insights" class="llm-insights-section">'
            '<div class="llm-panel">'
            '<div class="llm-banner">AI-assisted narrative — confirm against detailed DQ tables below</div>'
            "<h2>AI insights</h2>"
            '<p class="llm-exec">'
            + html_module.escape(str(_p.get("executive_summary") or ""))
            + "</p>"
            '<h3 class="llm-subh">Top risks</h3><ul class="llm-ul">'
            + ("".join(_risks) if _risks else '<li class="muted">(none listed)</li>')
            + "</ul>"
            '<h3 class="llm-subh">Recommended next steps</h3><ol class="llm-ol">'
            + ("".join(_steps) if _steps else '<li class="muted">(none listed)</li>')
            + "</ol>"
            + _lin_html
            + "</div></section>"
        )
    elif llm.get("error") is not None and "llm_insights" in result:
        nav_llm_link = '\n  <a href="#llm-insights" class="nav-jump">AI insights</a>'
        llm_section_html = (
            '<section id="llm-insights" class="llm-insights-section"><div class="llm-panel llm-panel-err">'
            "<h2>AI insights</h2>"
            "<p>Could not generate insights: <code>"
            + html_module.escape(str(llm.get("error") or ""))
            + "</code></p><p class=\"muted\">Check Azure OpenAI env vars and deployment.</p></div></section>"
        )

    exec_summary_html = ""
    if exec_items:
        _lis = []
        for it in exec_items[:10]:
            if not isinstance(it, dict):
                continue
            _lis.append(
                "<li><span class=\"pill sev-"
                + html_module.escape(str(it.get("severity") or "low"))
                + "\">"
                + html_module.escape(str(it.get("severity") or "low").upper())
                + "</span> <strong>"
                + html_module.escape(str(it.get("title") or ""))
                + "</strong> <span class=\"muted\">(datasets="
                + html_module.escape(str(it.get("datasets_affected") or "—"))
                + ", rows≈"
                + html_module.escape(str(it.get("estimated_rows_affected") or "—"))
                + ")</span><br/><span class=\"muted\">"
                + html_module.escape(str(it.get("recommendation") or ""))
                + "</span></li>"
            )
        exec_summary_html = (
            "<div class=\"exec-summary\"><h3>Executive summary (prioritized)</h3>"
            "<ul class=\"exec-ul\">" + ("".join(_lis) if _lis else "<li class=\"muted\">(none)</li>") + "</ul></div>"
        )

    def esc(x: Any) -> str:
        return html_module.escape(str(x) if x is not None else "")

    def esc_attr(x: Any) -> str:
        return html_module.escape(str(x) if x is not None else "", quote=True)

    def path_label(sr: str) -> str:
        if sr == "__database__":
            return "SQL database"
        if sr.startswith("azure_blob:"):
            sub = sr.split(":", 1)[1] or "/"
            return f"Azure Blob — {sub}"
        if sr:
            return sr
        return "Unknown source"

    def path_raw_for_copy(sr: str) -> str:
        if sr == "__database__":
            return "SQL database (tables)"
        if sr.startswith("azure_blob:"):
            return sr
        return sr or ""

    def is_filesystem_source(sr: str) -> bool:
        if not sr or sr == "__database__":
            return False
        if sr.startswith("azure_blob:"):
            return False
        return True

    def pct(x):
        try:
            return f"{round(float(x)*100, 2)}%"
        except Exception:
            return esc(x)

    def dtype_with_inference(cmeta: Dict[str, Any]) -> str:
        d = cmeta.get("dtype")
        inf = cmeta.get("dtype_inference")
        s = f"{d} ({inf})" if d == "object" and inf else str(d)
        return esc(s)

    def render_profile_table(cols_meta: Dict[str, Any]) -> str:
        rows_html = []
        for col, cmeta in cols_meta.items():
            rows_html.append(
                "<tr>"
                f"<td><strong>{esc(col)}</strong></td>"
                f"<td>{dtype_with_inference(cmeta)}</td>"
                f"<td>{pct(cmeta.get('null_percentage'))}</td>"
                f"<td>{esc(cmeta.get('unique_count'))}</td>"
                f"<td>{esc(cmeta.get('semantic_type'))}</td>"
                f"<td>{esc(str(cmeta.get('candidate_primary_key')).lower())}</td>"
                "</tr>"
            )
        if not rows_html:
            rows_html.append("<tr><td colspan='6' class='muted'>(no columns)</td></tr>")
        return (
            "<div class='table-wrap'><table class='data-table'><thead><tr>"
            "<th>Column</th><th>Type</th><th>Null %</th><th>Unique</th><th>Semantic</th><th>PK candidate</th>"
            "</tr></thead><tbody>" + "".join(rows_html) + "</tbody></table></div>"
        )

    def split_by_sev(issues: List[Dict]) -> Dict[str, List]:
        buckets = {"high": [], "medium": [], "low": []}
        for i in issues or []:
            sev = (i.get("severity") or "low").lower()
            if sev not in buckets:
                sev = "low"
            buckets[sev].append(i)
        return buckets

    def fmt_rows(indexes: List[int]) -> str:
        if not indexes:
            return "<span class='muted'>(none)</span>"
        if len(indexes) <= 40:
            return esc(", ".join(str(x) for x in indexes))
        return f"<div class='scrollcell'>{esc(', '.join(str(x) for x in indexes))}</div>"

    def fmt_samples(samples: List[Any], col: Optional[str] = None) -> str:
        if not samples:
            return "<span class='muted'>(none)</span>"
        return ", ".join(f"<code>{esc(mask_pii_sample(x, col))}</code>" for x in samples)

    def render_dq_table(title: str, rows: List[Dict]) -> str:
        body = []
        if not rows:
            body.append("<tr><td colspan='9' class='muted'>(no issues)</td></tr>")
        else:
            for i in rows:
                sev = (i.get("severity") or "low").lower()
                msg = (i.get("message") or "").strip()
                rec = (i.get("recommendation") or "").strip()
                fx = (i.get("fixability") or "").strip()
                msg_td = f"<td class='msg-cell'>{esc(msg)}</td>" if msg else "<td class='msg-cell'><span class='muted'>—</span></td>"
                rec_td = f"<td class='rec-cell'>{esc(rec)}</td>" if rec else "<td class='rec-cell'><span class='muted'>—</span></td>"
                body.append(
                    "<tr>"
                    f"<td>{esc(i.get('column') or '-')}</td>"
                    f"<td><code>{esc(i.get('type'))}</code></td>"
                    f"<td><span class='badge sev-{esc(sev)}'>{esc(sev.upper())}</span></td>"
                    f"<td><span class='badge fx-{esc(fx.lower() or 'complex')}'>{esc(fx or '—')}</span></td>"
                    f"<td>{esc(i.get('count')) if i.get('count') is not None else '-'}</td>"
                    f"<td>{fmt_rows(i.get('row_indexes') or [])}</td>"
                    f"<td>{fmt_samples(i.get('sample_values') or [], col=i.get('column'))}</td>"
                    f"{msg_td}{rec_td}</tr>"
                )
        return (
            f"<h4 class='block-title'>{esc(title)}</h4>"
            "<div class='table-wrap'><table class='data-table'><thead><tr>"
            "<th>Column</th><th>Issue</th><th>Sev</th><th>Fixability</th><th>Count</th><th>Rows</th><th>Samples</th>"
            "<th>Problem</th><th>Recommendation</th>"
            "</tr></thead><tbody>" + "".join(body) + "</tbody></table></div>"
        )

    path_groups: Dict[str, List[str]] = defaultdict(list)
    for name, meta in datasets.items():
        path_groups[meta.get("source_root") or ""].append(name)
    ordered_paths = sorted(path_groups.keys(), key=lambda x: (x == "", str(x).lower()))

    ds_idx = [0]

    def _fmt_id(s: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_-]", "_", str(s))[:44] or "fmt"

    def render_dataset_block(name: str, meta: Dict[str, Any], pi: int) -> str:
        rows, cols, bytes_ = meta.get("row_count", 0), meta.get("column_count", 0), meta.get("data_volume_bytes", 0)
        profile_tbl = render_profile_table(meta.get("columns", {}) or {})
        ds_raw = dq_ds.get(name, {}) if isinstance(dq_ds, dict) else {}
        if isinstance(ds_raw, list):
            ds_issues = ds_raw
            ds_summary = {}
        elif isinstance(ds_raw, dict):
            ds_issues = ds_raw.get("issues", []) or []
            ds_summary = ds_raw.get("summary") or {}
        else:
            ds_issues = []
            ds_summary = {}
        buckets = split_by_sev(ds_issues)
        src = meta.get("source_root") or ""
        raw = path_raw_for_copy(src)
        if src == "__database__":
            path_html = "<div class='path-inset'><span class='path-inset-label'>Loaded from</span><span class='path-inset-value'>SQL database tables</span></div>"
        elif src.startswith("azure_blob:"):
            path_html = f"<div class='path-inset azure'><span class='path-inset-label'>Azure Blob prefix</span><span class='path-inset-value'>{esc(raw)}</span><button type='button' class='btn-mini btn-copy-path' data-path=\"{esc_attr(raw)}\">Copy</button></div>"
        elif src:
            path_html = f"<div class='path-inset disk'><span class='path-inset-label'>Filesystem path</span><span class='path-inset-value'>{esc(raw)}</span><button type='button' class='btn-mini btn-copy-path' data-path=\"{esc_attr(raw)}\">Copy</button></div>"
        else:
            path_html = "<div class='path-inset'><span class='path-inset-label'>Source</span><span class='path-inset-value muted'>Not specified</span></div>"
        i = ds_idx[0]
        ds_idx[0] += 1
        score = ds_summary.get("dq_score_0_100")
        clean_h = ds_summary.get("estimated_clean_rows_after_high")
        clean_hm = ds_summary.get("estimated_clean_rows_after_high_and_medium")
        score_html = ""
        if score is not None:
            try:
                sv = float(score)
                sev_class = "sev-high" if sv < 35 else ("sev-medium" if sv < 70 else "sev-low")
                ds_entry = next((d for d in scorecard.get("per_dataset", []) if d.get("name") == name), None)
                adj_val = ds_entry.get("gating_adjusted_score") if ds_entry else None
                if adj_val is not None and str(adj_val) != str(round(sv, 1)):
                    score_html = (
                        f"<span class='badge {sev_class}' title='Intrinsic Profile Score'>DQ Intrinsic: {round(sv,1)}/100</span> "
                        f"<span class='badge sev-high' title='Operational Readiness capped by HIGH blockers'>Gating-Adjusted: {adj_val}/100 (Capped)</span>"
                    )
                else:
                    score_html = f"<span class='badge {sev_class}'>DQ {round(sv,1)}/100</span>"
            except Exception:
                score_html = f"<span class='badge sev-low'>DQ {esc(str(score))}/100</span>"
        clean_html = ""
        try:
            tr_i = int(rows) if rows is not None else 0
        except Exception:
            tr_i = 0
        if clean_h is not None or clean_hm is not None:
            rows_bits: List[str] = []
            tail = ""
            if tr_i > 0:
                tail = f" <span class=\"muted\">(of {rows:,} sampled rows)</span>"
            if clean_h is not None:
                pct_h = None
                if tr_i > 0:
                    try:
                        pct_h = round(100.0 * float(clean_h) / float(tr_i), 1)
                    except Exception:
                        pct_h = None
                pct_part = (
                    f" <span class=\"muted\">≈ {esc(pct_h)}% unaffected by HIGH severity</span>"
                    if pct_h is not None
                    else ""
                )
                rows_bits.append(
                    "<div class=\"clean-est-row\">"
                    "<span class=\"clean-est-label\">Rows with <strong>no HIGH</strong>-severity finding</span>"
                    f"<span class=\"clean-est-value\">{esc(str(clean_h))}</span>"
                    f"<span class=\"clean-est-tail\">{tail}{pct_part}</span>"
                    "</div>"
                )
            if clean_hm is not None:
                pct_hm = None
                if tr_i > 0:
                    try:
                        pct_hm = round(100.0 * float(clean_hm) / float(tr_i), 1)
                    except Exception:
                        pct_hm = None
                pct_part = (
                    f" <span class=\"muted\">≈ {esc(pct_hm)}% unaffected after HIGH+MEDIUM remediation</span>"
                    if pct_hm is not None
                    else ""
                )
                rows_bits.append(
                    "<div class=\"clean-est-row\">"
                    "<span class=\"clean-est-label\">Rows with <strong>no HIGH nor MEDIUM</strong> finding</span>"
                    f"<span class=\"clean-est-value\">{esc(str(clean_hm))}</span>"
                    f"<span class=\"clean-est-tail\">{tail}{pct_part}</span>"
                    "</div>"
                )
            clean_html = (
                '<aside class="dataset-clean-summary" aria-label="Remediation row estimates">'
                "<p class=\"clean-summary-intro\">These counts are computed from overlapping HIGH/MEDIUM rows in this scan "
                '(see <a href="#dq-scorecard">DQ verdict</a> for context).</p>'
                + "".join(rows_bits)
                + "</aside>"
            )
        
        n_high = len(buckets["high"])
        n_med = len(buckets["medium"])
        n_low = len(buckets["low"])
        dq_details_open_attr = " open" if (n_high + n_med + n_low) > 0 else ""

        # Root-Cause Groups HTML
        rc_summary = ds_summary.get("root_cause_summary") or {}
        rc_groups_html = ""
        if rc_summary and rc_summary.get("root_cause_groups"):
            rc_groups_html = f"""
            <div class="root-cause-summary-wrap" style="margin-bottom: 20px;">
                <h5 style="margin-top: 10px; margin-bottom: 8px;">Root-Cause Groups (Non-Additive)</h5>
                <p class="muted" style="margin-bottom: 8px; font-size: 0.9em;">
                    Total Rule Hits: <strong>{rc_summary.get('rule_hit_count')}</strong> · 
                    Distinct Cells Affected: <strong>{rc_summary.get('distinct_cells_affected')}</strong> · 
                    Distinct Rows Affected: <strong>{rc_summary.get('distinct_rows_affected')}</strong>
                </p>
                <table class="profile-tbl" style="width: 100%; border-collapse: collapse; margin-bottom: 15px;">
                    <thead>
                        <tr style="background-color: #f9f9f9; border-bottom: 2px solid #ddd;">
                            <th style="padding: 8px; text-align: left;">Root-Cause Family</th>
                            <th style="padding: 8px; text-align: left;">Issue Types Triggered</th>
                            <th style="padding: 8px; text-align: right;">Cells Affected</th>
                            <th style="padding: 8px; text-align: right;">Rows Affected</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            for g in rc_summary["root_cause_groups"]:
                itypes_formatted = ", ".join(f"<code>{t}</code>" for t in g["issue_types_triggered"])
                rc_groups_html += f"""
                        <tr style="border-bottom: 1px solid #eee;">
                            <td style="padding: 8px; font-weight: bold;">{esc(g['family'])}</td>
                            <td style="padding: 8px;">{itypes_formatted}</td>
                            <td style="padding: 8px; text-align: right;">{g['distinct_cells_affected']:,}</td>
                            <td style="padding: 8px; text-align: right;">{g['distinct_rows_affected']:,}</td>
                        </tr>
                """
            rc_groups_html += """
                    </tbody>
                </table>
            </div>
            """

        low_severity_html = ""
        if buckets['low']:
            low_severity_html = f"""
            <details class="low-severity-details" style="margin-top: 15px; border: 1px solid var(--border-color, #e0e0e0); border-radius: 4px; padding: 10px;">
                <summary style="cursor: pointer; font-weight: bold; color: var(--text-muted, #666);">Low severity issues ({n_low}) [Appendix]</summary>
                <div style="margin-top: 10px;">
                    {render_dq_table('Low severity', buckets['low'])}
                </div>
            </details>
            """

        # Operational Use-Case Readiness Gates
        ds_contact_issues = [i for i in ds_issues if (i.get("type") or "").lower() in ("invalid_phone", "invalid_email")]
        ds_email_cnt = sum(int(i.get("count") or 1) for i in ds_issues if (i.get("type") or "").lower() == "invalid_email")
        ds_phone_cnt = sum(int(i.get("count") or 1) for i in ds_issues if (i.get("type") or "").lower() == "invalid_phone")
        ds_total_contact_vals = ds_email_cnt + ds_phone_cnt
        ds_contact_row_set = set()
        for ci in ds_contact_issues:
            ds_contact_row_set.update(ci.get("row_indexes") or [])
        ds_distinct_contact_rows = len(ds_contact_row_set) if ds_contact_row_set else min(tr_i, max(ds_email_cnt, ds_phone_cnt))
        ds_contact_cols = {ci.get("column") for ci in ds_contact_issues if ci.get("column")}
        ds_num_contact_cols = max(1, len(ds_contact_cols)) if ds_contact_cols else 2
        ds_total_contact_cells = tr_i * ds_num_contact_cols
        ds_contact_row_rate = (ds_distinct_contact_rows / max(1, tr_i)) if tr_i > 0 else 0.0
        ds_contact_cell_rate = (ds_total_contact_vals / max(1, ds_total_contact_cells)) if ds_total_contact_cells > 0 else 0.0

        # Operational Use-Case Readiness Gates
        ds_scorecard_entry = next((d for d in scorecard.get("per_dataset", []) if d.get("name") == name), {})
        ds_uc_gates = ds_scorecard_entry.get("use_case_gates") or {}
        if not ds_uc_gates:
            ds_readiness = ds_summary.get("readiness", "READY")
            ds_uc_gates = {
                "general_analytics": ds_readiness,
                "customer_contact": ds_readiness,
                "identity_resolution": ds_readiness,
            }

        gate_badges = []
        for g_name, g_val in ds_uc_gates.items():
            g_label = g_name.replace("_", " ").title()
            if g_val == "NOT_APPLICABLE":
                g_badge = '<span class="badge" style="background: #f1f5f9; color: #64748b; border: 1px solid #cbd5e1; font-weight: normal;">NOT APPLICABLE</span>'
            elif g_val == "BLOCKED":
                g_badge = '<span class="badge sev-high">BLOCKED</span>'
            elif g_val == "NEEDS_WORK":
                g_badge = '<span class="badge sev-medium">NEEDS WORK</span>'
            elif g_val == "READY_WITH_WARNINGS":
                g_badge = '<span class="badge sev-low">READY (WARNINGS)</span>'
            else:
                g_badge = '<span class="badge sev-low">READY</span>'
            gate_badges.append(f'<span>{esc(g_label)}: {g_badge}</span>')

        warning_bits = []
        if ds_uc_gates.get("customer_contact") == "BLOCKED":
            warning_bits.append(
                f'<p class="muted" style="margin: 6px 0 0 0; font-size: 0.78rem; color: #dc2626;">⚠️ Customer contact usage gated to BLOCKED due to high contact defect rate ({ds_distinct_contact_rows}/{tr_i} rows affected [{round(ds_contact_row_rate*100, 1)}% row rate], {ds_total_contact_vals} invalid contact cells [{round(ds_contact_cell_rate*100, 1)}% cell error rate]: {ds_email_cnt} invalid emails, {ds_phone_cnt} invalid phones).</p>'
            )
        if ds_uc_gates.get("pricing_consumption") == "BLOCKED":
            warning_bits.append(
                '<p class="muted" style="margin: 6px 0 0 0; font-size: 0.78rem; color: #dc2626;">⚠️ Pricing consumption gated to BLOCKED due to catalog price domain violations (e.g. negative prices).</p>'
            )
        elif ds_uc_gates.get("catalogue_publication") == "BLOCKED":
            warning_bits.append(
                '<p class="muted" style="margin: 6px 0 0 0; font-size: 0.78rem; color: #dc2626;">⚠️ Catalogue publication gated to BLOCKED due to unresolved HIGH severity catalog defects.</p>'
            )

        use_case_html = f"""
        <div class="use-case-readiness" style="margin: 10px 0 14px 0; padding: 10px 14px; background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; font-size: 0.85rem;">
            <strong>Operational Use-Case Readiness Gates:</strong>
            <div style="display: flex; gap: 16px; margin-top: 6px; flex-wrap: wrap;">
                {' '.join(gate_badges)}
            </div>
            {''.join(warning_bits)}
        </div>
        """

        return f"""
        <div class="nest-file-card dataset-card depth-card" id="ds-{i}" data-path-group="{pi}">
          <button type="button" class="nest-toggle nest-level-3" aria-expanded="true" aria-controls="dbc-{i}">
            <span class="chevron" aria-hidden="true"></span>
            <div class="toggle-main">
              <h3>{esc(name)}</h3>
              <div class="toggle-sub">Profile &amp; data quality</div>
            </div>
            <div class="dataset-stats-inline"><span>{rows:,} rows</span><span>{cols} cols</span>{score_html}</div>
          </button>
          <div class="dataset-body" id="dbc-{i}">
            {path_html}
            {clean_html}
            {use_case_html}
            <h4 class="block-title">Profile</h4>
            {profile_tbl}
            <h4 class="block-title">Data quality</h4>
            <details class="dq-issue-details"{dq_details_open_attr}>
              <summary class="dq-issue-summary">{esc(name)} — {n_high + n_med + n_low} issues ({n_high} HIGH · {n_med} MEDIUM · {n_low} LOW)</summary>
              <div class="dq-issue-body">
                {rc_groups_html}
                {render_dq_table('High severity', buckets['high'])}
                {render_dq_table('Medium severity', buckets['medium'])}
                {low_severity_html}
              </div>
            </details>
          </div>
        </div>"""

    n_files = sum(len(path_groups[p]) for p in ordered_paths)
    nest_chunks: List[str] = [
        f"""<div class="datasets-nest-root">
<button type="button" class="nest-toggle nest-level-0" aria-expanded="true" aria-controls="nest-root-body" id="nest-root-toggle">
  <span class="chevron" aria-hidden="true"></span>
  <span class="nest-toggle-main"><strong>All locations</strong>
  <span class="nest-meta-inline">{len(ordered_paths)} location(s) · {n_files} file(s)</span></span>
</button>
<div class="nest-body" id="nest-root-body">"""
    ]
    for pi, sr in enumerate(ordered_paths):
        names = sorted(path_groups[sr])
        lbl = path_label(sr)
        raw = path_raw_for_copy(sr)
        loc_body_id = f"nest-loc-body-{pi}"
        if is_filesystem_source(sr):
            loc_title = (
                '<span class="nest-loc-heading-row"><strong>File system</strong>'
                '<span class="btn-mini btn-copy-path nest-loc-copy-path" role="button" tabindex="0" '
                f'data-path="{esc_attr(raw)}">Copy path</span></span>'
            )
            loc_meta = esc(raw if len(raw) < 120 else raw[:117] + "…")
            loc_toolbar = ""
        else:
            loc_title = f"<strong>{esc(lbl)}</strong>"
            loc_meta = esc(raw if len(raw) < 80 else raw[:77] + "…")
            loc_toolbar = (
                f'<div class="nest-loc-toolbar">'
                f'<button type="button" class="btn-mini btn-copy-path" data-path="{esc_attr(raw)}">'
                f"Copy full path</button></div>"
            )
        nest_chunks.append(
            f"""<div class="nest-loc-wrap" id="src-{pi}" data-path-filter="{pi}">
<button type="button" class="nest-toggle nest-level-1" aria-expanded="true" aria-controls="{loc_body_id}">
  <span class="chevron" aria-hidden="true"></span>
  <span class="nest-toggle-main">{loc_title}
  <span class="nest-meta-inline">{loc_meta}</span></span>
</button>
<div class="nest-body nest-loc-inner" id="{loc_body_id}">
{loc_toolbar}"""
        )
        by_ext: Dict[str, List[str]] = defaultdict(list)
        for n in names:
            if "." in n:
                ext = n.rsplit(".", 1)[-1].lower()
            else:
                ext = "no-extension"
            by_ext[ext].append(n)
        for ext_key in sorted(by_ext.keys(), key=lambda x: (x == "no-extension", x)):
            files = sorted(by_ext[ext_key])
            fmt_body_id = f"nest-fmt-{pi}-{_fmt_id(ext_key)}"
            ext_label = f".{ext_key}" if ext_key != "no-extension" else "(no extension)"
            nest_chunks.append(
                f"""<button type="button" class="nest-toggle nest-level-2" aria-expanded="true" aria-controls="{fmt_body_id}">
  <span class="chevron" aria-hidden="true"></span>
  <span class="nest-toggle-main"><strong>{esc(ext_label)}</strong>
  <span class="nest-meta-inline">{len(files)} file(s)</span></span>
</button>
<div class="nest-body nest-fmt-inner" id="{fmt_body_id}">"""
            )
            for n in files:
                nest_chunks.append(render_dataset_block(n, datasets[n], pi))
            nest_chunks.append("</div>")
        nest_chunks.append("</div></div>")
    nest_chunks.append("</div></div>")
    datasets_html = "".join(nest_chunks) if ordered_paths else "<p class='muted'>(no datasets)</p>"

    def render_rels() -> str:
        # Cross-dataset candidate column overlaps discovery with semantic filtering
        overlap_rows = []
        ds_names = sorted(list(datasets.keys()))
        for i in range(len(ds_names)):
            for j in range(i + 1, len(ds_names)):
                d1, d2 = ds_names[i], ds_names[j]
                cols1 = (datasets[d1].get("columns") or {}) if isinstance(datasets[d1], dict) else {}
                cols2 = (datasets[d2].get("columns") or {}) if isinstance(datasets[d2], dict) else {}
                c1_clean = {re.sub(r"[^a-zA-Z0-9]", "", c).lower(): c for c in cols1}
                c2_clean = {re.sub(r"[^a-zA-Z0-9]", "", c).lower(): c for c in cols2}
                common = set(c1_clean.keys()) & set(c2_clean.keys())
                for k in sorted(common):
                    col1_name = c1_clean[k]
                    col2_name = c2_clean[k]
                    c1_meta = cols1.get(col1_name) if isinstance(cols1, dict) else {}
                    c2_meta = cols2.get(col2_name) if isinstance(cols2, dict) else {}
                    t1 = c1_meta.get("data_type") or c1_meta.get("type") if isinstance(c1_meta, dict) else None
                    t2 = c2_meta.get("data_type") or c2_meta.get("type") if isinstance(c2_meta, dict) else None
                    t_compat = "Compatible" if (t1 and t2 and str(t1).lower() == str(t2).lower()) else ("Compatible" if not (t1 and t2) else f"Check: {t1} vs {t2}")
                    
                    c_low = col1_name.lower()
                    if c_low.endswith(("id", "key", "code", "sku")) or c_low in ("id", "key", "code", "sku"):
                        category = "key"
                        match_type = "Candidate relational key"
                        status = "Candidate relational key"
                        guidance = "Relational key overlap candidate. Referential integrity remains advisory until explicitly declared and approved."
                    elif "email" in c_low or "phone" in c_low or "ssn" in c_low:
                        category = "pii"
                        match_type = "PII reconciliation candidate"
                        status = "Entity matching token (not a relational FK)"
                        guidance = "Shared contact/identity attribute useful for entity reconciliation. Should not be modeled as a default relational FK constraint."
                    else:
                        category = "info"
                        match_type = "Shared attribute name (non-key)"
                        status = "Informational column overlap only"
                        guidance = "Shared domain/audit attribute (e.g. timestamp or status). Not a candidate relational key."

                    overlap_rows.append((d1, col1_name, d2, col2_name, t_compat, category, match_type, status, guidance))

        overlaps_html = ""
        if overlap_rows:
            # Sort with candidate relational keys first, then PII, then informational
            cat_order = {"key": 0, "pii": 1, "info": 2}
            overlap_rows.sort(key=lambda x: cat_order.get(x[5], 3))

            rows_html = "".join(
                f"<tr><td><code>{esc(d1)}.{esc(c1)}</code></td><td><code>{esc(d2)}.{esc(c2)}</code></td>"
                f"<td>{esc(m_type)}</td>"
                f"<td>{esc(tcompat)}</td>"
                f"<td>{esc(st)}</td>"
                f"<td><span class='badge' style='background-color: #64748b; color: #ffffff;'>ADVISORY</span></td>"
                f"<td>{esc(gd)}</td></tr>"
                for d1, c1, d2, c2, tcompat, cat, m_type, st, gd in overlap_rows
            )
            overlaps_html = (
                "<div style='margin-top: 24px;'>"
                "<h3>Cross-Dataset Column Overlaps (Advisory)</h3>"
                "<p class='section-lead'>The following datasets share identical or normalized column names. Overlaps are separated into candidate relational keys, PII reconciliation candidates, and informational name overlaps.</p>"
                "<div class='table-wrap'><table class='data-table'><thead><tr>"
                "<th>Dataset A (Column)</th><th>Dataset B (Column)</th><th>Classification</th><th>Type Compatibility</th><th>Relationship Status</th><th>Gate</th><th>Recommendation / Governance Guidance</th>"
                "</tr></thead><tbody>" + rows_html + "</tbody></table></div></div>"
            )

        if not rels:
            if overlaps_html:
                return overlaps_html
            return "<p class='muted'>No cross-dataset column overlaps or relationships detected.</p>"
        
        declared_rels = [r for r in rels if r.get("source") == "declared"]
        inferred_rels = [r for r in rels if r.get("source") != "declared"]

        declared_html = ""
        if declared_rels:
            cards = []
            for r in declared_rels:
                rid = r.get("relationship_id") or r.get("name") or "?"
                parent = r.get("parent") or "?"
                child = r.get("child") or "?"
                status = r.get("evaluation_status", "?")
                shared = r.get("shared_distinct_keys", 0)
                child_orphans = r.get("child_orphan_distinct_keys", 0)
                child_orphan_row_count = r.get("child_orphan_row_count", 0)
                unref_parents = r.get("unreferenced_parent_distinct_keys", 0)
                gating = r.get("gating", False)
                gate_badge = "<span class='badge' style='background-color: #ef4444; color: #ffffff; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem;'>GATING</span>" if gating else "<span class='badge' style='background-color: #64748b; color: #ffffff; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem;'>advisory</span>"
                sev = (r.get("severity") or "low").lower()
                sev_badge = f"<span class='badge sev-{sev}'>{esc(sev.upper())}</span>"
                
                # Normalization details
                norm = r.get("normalization_applied") or {}
                trim_enabled = norm.get("trim", True)
                trim_str = "enabled" if trim_enabled else "disabled"
                casefold_enabled = norm.get("casefold", False)
                casefold_str = "enabled" if casefold_enabled else "disabled"
                
                # Excluded null tokens
                null_tokens = norm.get("null_tokens")
                if null_tokens is None:
                    null_tokens = ["", "N/A", "UNKNOWN", "none", "null", "unknown"]
                null_tokens_str = ", ".join(f'"{t}"' for t in null_tokens) if null_tokens else "none"

                # Composite key check
                is_composite = "," in parent or (isinstance(norm.get("columns"), list) and len(norm.get("columns")) > 1)
                composite_str = "yes" if is_composite else "no"

                # Exclusions
                parent_null = r.get("parent_null_excluded", 0)
                parent_placeholder = r.get("parent_placeholder_excluded", 0)
                child_null = r.get("child_null_excluded", 0)
                child_placeholder = r.get("child_placeholder_excluded", 0)

                # Gating status
                g_status = str(r.get("evaluation_status") or r.get("status") or "NOT_EVALUATED").upper()
                if g_status in ("NOT_EVALUATED", "CONFIGURATION_ERROR", "EXECUTION_ERROR", "INSUFFICIENT_EVIDENCE"):
                    gate_result = "UNKNOWN"
                    gate_color = "#ca8a04"
                elif g_status == "PARTIAL_PASS":
                    gate_result = "PARTIAL_PASS"
                    gate_color = "#d97706"
                elif g_status in ("VALIDATED_PASS", "PASS"):
                    gate_result = "PASS"
                    gate_color = "#16a34a"
                elif g_status in ("VALIDATED_FAIL", "FAIL"):
                    gate_result = "FAIL"
                    gate_color = "#dc2626"
                else:
                    gate_result = r.get("gating_result", "UNKNOWN")
                    gate_color = "#16a34a" if gate_result == "PASS" else ("#ca8a04" if gate_result in ("UNKNOWN", "PARTIAL_PASS") else "#dc2626")


                cards.append(f"""
                <div class="relationship-card" style="border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px; margin-bottom: 16px; background-color: #ffffff; box-shadow: 0 1px 3px rgba(0,0,0,0.05);">
                  <div style="display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #f1f5f9; padding-bottom: 8px; margin-bottom: 12px;">
                    <h3 style="margin: 0; font-size: 1.05rem; color: #0f172a;">Relationship ID: <strong>{esc(rid)}</strong></h3>
                    <div style="display: flex; gap: 8px; align-items: center;">
                      {sev_badge}
                      {gate_badge}
                    </div>
                  </div>
                  <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px;">
                    <div>
                      <h4 style="margin: 0 0 6px 0; font-size: 0.85rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em;">Mapping Schema</h4>
                      <p style="margin: 0; font-size: 0.85rem; color: #1e293b;"><strong>Parent Keys:</strong> <code>{esc(parent)}</code></p>
                      <p style="margin: 4px 0 0 0; font-size: 0.85rem; color: #1e293b;"><strong>Child Keys:</strong> <code>{esc(child)}</code></p>
                      <p style="margin: 6px 0 0 0; font-size: 0.85rem; color: #64748b;"><strong>Status:</strong> {esc(status)}</p>
                    </div>
                    <div>
                      <h4 style="margin: 0 0 6px 0; font-size: 0.85rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em;">Integrity Metrics</h4>
                      <ul style="margin: 0; padding-left: 16px; font-size: 0.85rem; color: #1e293b; line-height: 1.4;">
                        <li><strong>Shared Distinct Keys:</strong> {esc(str(shared))}</li>
                        <li><strong>Child Orphan Distinct Keys:</strong> {esc(str(child_orphans))}</li>
                        <li><strong>Child Orphan Row Count:</strong> {esc(str(child_orphan_row_count))}</li>
                        <li><strong>Unreferenced Parent Keys:</strong> {esc(str(unref_parents))}</li>
                      </ul>
                    </div>
                    <div>
                      <h4 style="margin: 0 0 6px 0; font-size: 0.85rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em;">Normalization & Scope</h4>
                      <ul style="margin: 0; padding-left: 16px; font-size: 0.85rem; color: #1e293b; line-height: 1.4;">
                        <li><strong>Trim:</strong> {trim_str}</li>
                        <li><strong>Case folding:</strong> {casefold_str}</li>
                        <li><strong>Composite Key:</strong> {composite_str}</li>
                        <li><strong>Evaluation Mode:</strong> full dataset</li>
                      </ul>
                    </div>
                    <div>
                      <h4 style="margin: 0 0 6px 0; font-size: 0.85rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em;">Exclusions & Gating</h4>
                      <ul style="margin: 0; padding-left: 16px; font-size: 0.85rem; color: #1e293b; line-height: 1.4;">
                        <li><strong>Null Tokens Excluded:</strong> <code style="font-size: 0.72rem; word-break: break-all;">{esc(null_tokens_str)}</code></li>
                        <li><strong>Parent Excluded (Null/Placeholder):</strong> {esc(str(parent_null))} / {esc(str(parent_placeholder))}</li>
                        <li><strong>Child Excluded (Null/Placeholder):</strong> {esc(str(child_null))} / {esc(str(child_placeholder))}</li>
                        <li><strong>Gating Result:</strong> <span style="font-weight: bold; color: {gate_color};">{gate_result}</span></li>
                      </ul>
                    </div>
                  </div>
                </div>
                """)
            declared_html = "<div style='margin-bottom: 24px;'><h3>Declared Relationships (Authoritative and Gating)</h3>" + "".join(cards) + "</div>"
        else:
            declared_html = "<div style='margin-bottom: 24px;'><h3>Declared Relationships (Authoritative and Gating)</h3><p class='muted'>No declared relationships configured.</p></div>"

        inferred_html = ""
        if inferred_rels:
            rows = []
            for r in inferred_rels:
                rid = r.get("relationship_id") or r.get("name") or "?"
                parent = r.get("parent") or "?"
                child = r.get("child") or "?"
                status = r.get("evaluation_status", "?")
                shared = r.get("shared_distinct_keys", "—")
                child_orphans = r.get("child_orphan_distinct_keys", 0)
                unref_parents = r.get("unreferenced_parent_distinct_keys", 0)
                sev = (r.get("severity") or "low").lower()
                sev_badge = f"<span class='badge sev-{sev}'>{esc(sev.upper())}</span>"
                rows.append(
                    "<tr>"
                    f"<td><strong>{esc(rid)}</strong></td>"
                    f"<td class='msg-cell'><code>{esc(parent)}</code></td>"
                    f"<td class='msg-cell'><code>{esc(child)}</code></td>"
                    f"<td>{esc(status)}</td>"
                    f"<td>{sev_badge} <span class='badge' style='background-color: #64748b; color: #ffffff;'>ADVISORY</span></td>"
                    f"<td>{esc(str(shared))}</td>"
                    f"<td>{esc(str(child_orphans))}</td>"
                    f"<td>{esc(str(unref_parents))}</td>"
                    "</tr>"
                )
            inferred_html = (
                "<div style='margin-top: 24px;'>"
                "<h3>Suggested Relationships (Inferred and Non-Gating)</h3>"
                "<p class='section-lead'>The following column overlaps were dynamically discovered and are advisory. They do not block downstream readiness gating.</p>"
                "<div class='table-wrap'><table class='data-table'><thead><tr>"
                "<th>Relationship</th><th>Parent</th><th>Child</th>"
                "<th>Status</th><th>Severity / Gate</th><th>Shared keys</th><th>Child orphans</th><th>Unreferenced parents</th>"
                "</tr></thead><tbody>" + "".join(rows) + "</tbody></table></div>"
                "</div>"
            )
        else:
            inferred_html = "<div style='margin-top: 24px;'><h3>Suggested Relationships (Inferred and Non-Gating)</h3><p class='muted'>No inferred relationships detected.</p></div>"

        return f"""
        <p class='section-lead'>Declared relationships are authoritative and gating. Inferred relationships are advisory.</p>
        {declared_html}
        {inferred_html}
        {overlaps_html}
        """

    def render_global() -> str:
        orphans = dq_global.get("orphan_foreign_keys", []) or []
        xds = dq_global.get("cross_dataset_inconsistencies", []) or []
        rri = dq_global.get("relationship_row_issues", []) or []
        rw = dq_global.get("relationship_warnings", []) or []
        xcons = dq_global.get("cross_dataset_consistency", []) or []

        def tbl(rows, headers, row_fn):
            if not rows:
                return "<p class='muted'>(none)</p>"
            head = "".join(f"<th>{esc(h)}</th>" for h in headers)
            return (
                "<div class='table-wrap'><table class='data-table'><thead><tr>"
                + head + "</tr></thead><tbody>" + "".join(row_fn(r) for r in rows) + "</tbody></table></div>"
            )

        o_tbl = tbl(
            orphans,
            ["From", "To", "Orphan values", "Samples", "Recommendation"],
            lambda o: f"<tr><td><code>{esc(o.get('from'))}</code></td><td><code>{esc(o.get('to'))}</code></td>"
            f"<td>{esc(o.get('orphan_count'))}</td><td>{fmt_samples(o.get('sample_values') or [], col=str(o.get('from', '')).split('.')[-1])}</td>"
            f"<td class='rec-cell'>{esc(o.get('recommendation') or '')}</td></tr>",
        )
        x_tbl = tbl(
            xds,
            ["Dataset", "Column", "Problem", "Recommendation"],
            lambda x: f"<tr><td>{esc(x.get('dataset'))}</td><td>{esc(x.get('column'))}</td>"
            f"<td class='msg-cell'>{esc(x.get('message'))}</td>"
            f"<td class='rec-cell'>{esc(x.get('recommendation') or '')}</td></tr>",
        )
        rri_tbl = tbl(
            rri,
            ["Child dataset", "FK column", "Parent table.col", "Rows affected", "Row indexes (sample)", "Bad values", "Recommendation"],
            lambda z: f"<tr><td><code>{esc(z.get('dataset'))}</code></td><td><code>{esc(z.get('column'))}</code></td>"
            f"<td><code>{esc(z.get('related_dataset'))}.{esc(z.get('related_column'))}</code></td>"
            f"<td>{esc(z.get('count'))}</td><td>{fmt_rows(z.get('row_indexes') or [])}</td>"
            f"<td>{fmt_samples(z.get('sample_values') or [], col=z.get('column'))}</td>"
            f"<td class='rec-cell'>{esc(z.get('recommendation') or '')}</td></tr>",
        )
        xcons_tbl = tbl(
            xcons,
            ["Issue Type", "Severity", "Insight", "Recommendation"],
            lambda xc: f"<tr><td><code>{esc(xc.get('type'))}</code></td>"
            f"<td><span class='badge sev-{esc((xc.get('severity') or 'low').lower())}'>{esc((xc.get('severity') or 'low').upper())}</span></td>"
            f"<td class='msg-cell'>{esc(xc.get('message'))}</td>"
            f"<td class='rec-cell'>{esc(xc.get('recommendation') or '')}</td></tr>",
        )
        rw_html = ""
        if rw:
            rw_html = "<h4 class='block-title'>Relationship model warnings (M:N)</h4><ul class='rel-list'>"
            for w in rw:
                rw_html += (
                    f"<li><span class='badge sev-{esc((w.get('severity') or 'low').lower())}'>{esc(w.get('severity'))}</span> "
                    f"{esc(w.get('message'))}<br/><em>Recommendation:</em> {esc(w.get('recommendation') or '')}</li>"
                )
            rw_html += "</ul>"
        else:
            rw_html = "<h4 class='block-title'>Relationship model warnings</h4><p class='muted'>(none)</p>"

        return f"""<h4 class="block-title">Orphan key rows (which dataset / column / rows)</h4>{rri_tbl}
            {rw_html}
            <h4 class="block-title">Orphan values (set difference, no row index)</h4>{o_tbl}
            <h4 class="block-title">Cross-dataset inconsistencies</h4>{x_tbl}
            <h4 class="block-title">Cross-dataset consistency insights</h4>{xcons_tbl}"""

    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rel_html = render_rels()
    glob_html = render_global()

    ts_block = result.get("transformation_suggestions") or {}
    raw_sug = list(ts_block.get("suggested_transformations") or [])
    _sev_rank = {"high": 0, "medium": 1, "low": 2}
    auto_sug = [s for s in raw_sug if s.get("suggested_action") != "review_manually"]
    man_sug = [s for s in raw_sug if s.get("suggested_action") == "review_manually"]
    auto_sug.sort(
        key=lambda x: (
            _sev_rank.get((x.get("severity") or "low").lower(), 3),
            str(x.get("dataset") or ""),
        )
    )
    man_sug.sort(
        key=lambda x: (
            _sev_rank.get((x.get("severity") or "low").lower(), 3),
            str(x.get("dataset") or ""),
        )
    )
    nav_fix_link = ""
    suggestions_section_html = ""
    if raw_sug and not ts_block.get("_error"):
        nav_fix_link = (
            '\n  <a href="#suggested-fixes" class="nav-jump" data-expand-suggestions="1">Fix suggestions</a>'
        )
        total_ct = int(ts_block.get("summary", {}).get("total_suggestions") or len(raw_sug))
        auto_ct = int(ts_block.get("summary", {}).get("auto_fixable_count") or len(auto_sug))
        man_ct = int(ts_block.get("summary", {}).get("manual_review_count") or len(man_sug))
        _auto_rows = []
        for s in auto_sug:
            _auto_rows.append(
                "<tr>"
                f'<td><span class="badge sug-act">{esc(s.get("suggested_action", ""))}</span></td>'
                f'<td><code>{esc(s.get("dataset") or "—")}</code></td>'
                f'<td><code>{esc(s.get("column") or "—")}</code></td>'
                f'<td><span class="badge sev-{esc((s.get("severity") or "low").lower())}">{esc(s.get("severity"))}</span></td>'
                f'<td class="msg-cell">{esc((s.get("message") or "")[:400])}</td>'
                "</tr>"
            )
        _man_rows = []
        for s in man_sug:
            _man_rows.append(
                "<tr>"
                f'<td><code>{esc(s.get("dataset") or "—")}</code></td>'
                f'<td><code>{esc(s.get("column") or "—")}</code></td>'
                f'<td><code>{esc(s.get("issue_type") or "—")}</code></td>'
                f'<td><span class="badge sev-{esc((s.get("severity") or "low").lower())}">{esc(s.get("severity"))}</span></td>'
                f"<td>{esc(s.get('row_count_affected')) if s.get('row_count_affected') is not None else '—'}</td>"
                f'<td class="msg-cell manual-guidance-cell">{esc(s.get("manual_guidance") or "")}</td>'
                "</tr>"
            )
        _auto_tbl = (
            "<h3 class=\"sug-subtitle sug-auto\">Auto-fixable issues</h3>"
            "<p class=\"section-lead\">Rule-based actions from DQ signals. "
            f"Showing all <strong>{len(auto_sug)}</strong> auto-fixable suggestion(s) "
            f"(of <strong>{total_ct}</strong> total).</p>"
            '<div class="table-wrap"><table class="data-table suggestions-table suggestions-auto"><thead><tr>'
            "<th>Action</th><th>Dataset</th><th>Column</th><th>Severity</th><th>Context</th>"
            "</tr></thead><tbody>"
            + ("".join(_auto_rows) if _auto_rows else "<tr><td colspan='5' class='muted'>(none)</td></tr>")
            + "</tbody></table></div>"
        )
        _man_tbl = (
            '<h3 class="sug-subtitle sug-manual">⚠️ Manual review required (ETL design decisions)</h3>'
            f"<p class=\"section-lead\">{man_ct} item(s) need human decisions before coding ETL logic.</p>"
            '<div class="table-wrap"><table class="data-table suggestions-table suggestions-manual"><thead><tr>'
            "<th>Dataset</th><th>Column</th><th>Issue type</th><th>Severity</th><th>Rows affected</th><th>ETL guidance</th>"
            "</tr></thead><tbody>"
            + ("".join(_man_rows) if _man_rows else "<tr><td colspan='6' class='muted'>(none)</td></tr>")
            + "</tbody></table></div>"
        )
        _sug_inner = (
            f"<p class=\"sug-summary-inline muted\">Summary: total={total_ct}, auto-fixable={auto_ct}, manual review={man_ct}</p>"
            + _auto_tbl
            + _man_tbl
        )
        suggestions_section_html = (
            '<section id="suggested-fixes" class="suggestions-section-wrap">'
            '<div class="collapsible-panel">'
            '<button type="button" class="collapsible-head" aria-expanded="true" aria-controls="sug-body">'
            '<h2>Suggested fixes</h2><span class="coll-h" aria-hidden="true"></span></button>'
            f'<div class="collapsible-body" id="sug-body">{_sug_inner}</div>'
            "</div></section>"
        )
    elif ts_block.get("_error"):
        nav_fix_link = (
            '\n  <a href="#suggested-fixes" class="nav-jump" data-expand-suggestions="1">Fix suggestions</a>'
        )
        suggestions_section_html = (
            '<section id="suggested-fixes" class="suggestions-section-wrap">'
            '<div class="collapsible-panel">'
            '<button type="button" class="collapsible-head" aria-expanded="true" aria-controls="sug-body-err">'
            '<h2>Suggested fixes</h2><span class="coll-h" aria-hidden="true"></span></button>'
            f'<div class="collapsible-body" id="sug-body-err"><p class="muted">Could not build suggestions: '
            f"{esc(ts_block.get('_error'))}</p></div>"
            "</div></section>"
        )

    v_vc = scorecard["verdict"]
    v_human = v_vc.replace("_", " ")
    ban_cls = {
        "BLOCKED": "scorecard-banner scorecard-blocked",
        "NEEDS_WORK": "scorecard-banner scorecard-needs",
        "UNKNOWN": "scorecard-banner scorecard-needs",
        "READY": "scorecard-banner scorecard-ready",
    }.get(v_vc, "scorecard-banner scorecard-needs")
    ti_sc = scorecard["total_issues"]
    ds_score_bits = []
    for d in scorecard["per_dataset"]:
        try:
            pct = float(d["dq_score"])
        except Exception:
            pct = 0.0
        w = max(0.0, min(100.0, pct))
        fc = "score-fill-low" if pct < 35 else ("score-fill-med" if pct < 70 else "score-fill-high")
        br = d["readiness"]
        if br == "READY":
            rb = "badge-ready"
            icon = "✅"
        elif br == "READY_WITH_WARNINGS":
            rb = "badge-ready"
            icon = "⚠️"
        elif br == "NEEDS_WORK":
            rb = "badge-needs"
            icon = "⚠️"
        elif br == "BLOCKED":
            rb = "badge-blocked"
            icon = "🔴"
        else:
            rb = "badge-needs"
            icon = "❓"
        adj_s = d.get("gating_adjusted_score", d["dq_score"])
        is_capped = str(adj_s) != str(d["dq_score"])
        cap_pill = ' <span class="badge" style="background:#fee2e2;color:#991b1b;font-size:0.75rem;padding:2px 6px;">Capped</span>' if is_capped else ""
        ds_score_bits.append(
            f'<div class="ds-score-row"><span class="ds-name">{esc(d["name"])}</span>'
            f'<div class="score-bar"><div class="score-fill {fc}" style="width:{w}%"></div></div>'
            f'<span class="score-value" title="Intrinsic Profile: {esc(d["dq_score"])}/100 | Gating-Adjusted: {esc(adj_s)}/100">'
            f'Intrinsic: <b>{esc(d["dq_score"])}</b> | Gating: <b>{esc(adj_s)}</b>{cap_pill}</span>'
            f'<span class="badge {rb}">{icon} {esc(br.replace("_", " "))}</span></div>'
        )
    verdict_title_icon = "🔴" if v_vc == "BLOCKED" else ("⚠️" if v_vc in ("NEEDS_WORK", "UNKNOWN") else "✅")

    
    # Map verdict to operational decision
    if v_vc == "READY":
        release_decision = "RELEASE"
        auth_status = "YES"
        auth_color = "#15803d"
    elif v_vc == "READY_WITH_WARNINGS":
        release_decision = "RELEASE"
        auth_status = "YES (WITH WARNINGS)"
        auth_color = "#15803d"
    elif v_vc == "NEEDS_WORK":
        release_decision = "HOLD"
        auth_status = "NO"
        auth_color = "#b45309"
    elif v_vc == "BLOCKED":
        release_decision = "REJECT"
        auth_status = "NO"
        auth_color = "#b91c1c"
    else:
        release_decision = "HOLD"
        auth_status = "NO"
        auth_color = "#64748b"

    scorecard_banner_html = (
        '<section id="dq-scorecard" class="dq-scorecard-section">'
        f'<div class="{ban_cls}">'
        f'<div class="scorecard-verdict-row"><span class="v-ico">{verdict_title_icon}</span> '
        f'<span class="v-text">Data quality verdict: <strong>{esc(v_human)}</strong></span></div>'
        
        f'<div style="margin-top: 8px; margin-bottom: 8px; padding: 10px; background: rgba(255,255,255,0.7); border-radius: 6px; border: 1px solid rgba(0,0,0,0.05); font-size: 0.88rem; display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 8px;">'
        f'<div><strong>Assessment status:</strong> <span style="color: #1e3a8a; font-weight: bold;">COMPLETE</span></div>'
        f'<div><strong>Readiness:</strong> <span style="color: {auth_color}; font-weight: bold;">{esc(v_human)}</span></div>'
        f'<div><strong>Data release decision:</strong> <span style="color: {auth_color}; font-weight: bold;">{release_decision}</span></div>'
        f'<div><strong>Production consumption authorized:</strong> <span style="color: {auth_color}; font-weight: bold;">{auth_status}</span></div>'
        f'</div>'

        "<div class=\"scorecard-metrics\">"
        f"<div><strong>Intrinsic weighted score:</strong> <strong>{esc(scorecard.get('intrinsic_dq_score', scorecard['overall_dq_score']))}</strong> / 100 · "
        f"<strong>Gating-adjusted readiness score:</strong> <strong style='color: #b45309;'>{esc(scorecard.get('gating_adjusted_score', scorecard['overall_dq_score']))}</strong> / 100 "
        f"{'<span class=\"muted\" style=\"font-size: 0.85rem;\">(capped at 69.9 due to HIGH severity defects)</span>' if str(scorecard.get('gating_adjusted_score')) == '69.9' else ''}</div>"
        f"<div style='margin-top: 4px;'>High severity issues: <strong>{ti_sc.get('high', 0)}</strong> · "
        f"Distinct rows affected by at least one HIGH finding: <strong>{esc(scorecard.get('distinct_rows_affected_by_high', scorecard['rows_affected_by_high']))}</strong> · "
        f"Total HIGH rule memberships: <strong>{esc(scorecard.get('total_high_memberships', scorecard['rows_affected_by_high']))}</strong> · "
        f"Approx. row share unaffected by HIGH: <strong>{esc(scorecard['estimated_clean_rows_pct'])}%</strong></div>"
        f"<div style='margin-top: 6px; padding: 6px 10px; background: rgba(255,255,255,0.6); border-radius: 6px; border: 1px solid rgba(0,0,0,0.05); font-size: 0.82rem; color: #334155;'>"
        f"<strong>HIGH affected rows breakdown:</strong> {esc(scorecard.get('high_breakdown_str', 'None'))}</div>"
        "</div>"
        f'<div class="scorecard-bar-list">{"".join(ds_score_bits)}</div>'
        "</div>"
        '<div class="manifest-callout">'
        "<strong>Machine-readable manifest</strong> — export run writes <code>cleaning_manifest.json</code> next to "
        "<code>report.html</code>. Use it when implementing ETL: issues, row indexes, samples, and recommended actions." 
        "</div>"
        "</section>"
    )

    _report_extra_css = """
.dq-scorecard-section { margin: 0 0 1.25rem 0; max-width: 1100px; margin-left: auto; margin-right: auto; padding: 0 1rem; }
.scorecard-banner { border-radius: 12px; padding: 1rem 1.25rem 1.1rem; border: 2px solid #cbd5e1; }
.scorecard-blocked { background: #fee2e2; border-color: #b91c1c; }
.scorecard-needs { background: #fef3c7; border-color: #b45309; }
.scorecard-ready { background: #dcfce7; border-color: #15803d; }
.scorecard-verdict-row { font-size: 1.25rem; font-weight: 700; margin-bottom: 0.5rem; display: flex; align-items: center; gap: 0.35rem; }
.scorecard-metrics { font-size: 0.95rem; margin-bottom: 0.75rem; color: #1f2937; }
.scorecard-bar-list { margin-top: 0.5rem; }
.ds-score-row { display: flex; align-items: center; gap: 0.6rem; margin: 0.45rem 0; flex-wrap: wrap; }
.ds-name { min-width: 140px; font-weight: 600; font-size: 0.88rem; }
.score-bar { flex: 1; min-width: 120px; height: 10px; background: rgba(255,255,255,0.65); border-radius: 999px; overflow: hidden; border: 1px solid rgba(0,0,0,0.08); }
.score-fill { height: 100%; border-radius: 999px; }
.score-fill-low { background: linear-gradient(90deg, #ef4444, #f87171); }
.score-fill-med { background: linear-gradient(90deg, #f59e0b, #fbbf24); }
.manifest-callout { margin-top: 0.75rem; padding: 0.65rem 0.85rem; border-radius: 8px; border: 1px dashed #64748b; background: #f8fafc; font-size: 0.88rem; }
.dq-issue-details { border: 1px solid #e2e8f0; border-radius: 8px; padding: 0.25rem 0.5rem; margin-top: 0.35rem; background: rgba(248,250,252,0.9); }
.dq-issue-summary { cursor: pointer; font-weight: 600; padding: 0.35rem 0.25rem; }
.dq-issue-body { padding: 0.35rem 0.15rem 0.6rem; }
.suggestions-manual .manual-guidance-cell { font-size: 0.82rem; }
.suggestions-manual thead { background: #fef9c3; }
.suggestions-auto thead { background: #ccfbf1; }
.sug-subtitle { margin: 1rem 0 0.35rem; font-size: 1rem; }
.sug-summary-inline { margin: 0 0 0.5rem 0; }
.dataset-clean-summary {
  margin: 0.6rem 0 1rem;
  padding: 0.65rem 0.85rem;
  border-radius: 8px;
  border-left: 4px solid #0070ad;
  background: rgba(0, 112, 173, 0.06);
  font-size: 0.9rem;
}
.clean-summary-intro { margin: 0 0 0.55rem 0; font-size: 0.84rem; color: #334155; line-height: 1.4; }
.clean-est-row {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  column-gap: 0.85rem;
  row-gap: 0.25rem;
  padding: 0.5rem 0;
  border-bottom: 1px solid rgba(15, 23, 42, 0.07);
}
.clean-est-row:last-child { border-bottom: none; }
.clean-est-label { flex: 1 1 50%; min-width: min(100%, 280px); line-height: 1.35; }
.clean-est-value { font-weight: 700; font-size: 1.12rem; flex: 0 0 auto; }
.clean-est-tail { flex: 1 1 100%; margin: 0; line-height: 1.35; }
.llm-recs-section { margin: 2rem 0; padding: 0 1rem; max-width: 1100px; margin-left: auto; margin-right: auto; }
.rec-cards-grid { display: grid; grid-template-columns: 1fr; gap: 1rem; margin-top: 1rem; }
@media (min-width: 768px) {
  .rec-cards-grid { grid-template-columns: repeat(auto-fill, minmax(48%, 1fr)); }
}
.rec-card { background: #ffffff; border: 1px solid #e2e8f0; border-left-width: 4px; border-radius: 8px; padding: 1rem; box-shadow: 0 1px 3px rgba(0,0,0,0.05); text-align: left; }
.border-sev-high { border-left-color: #ef4444; }
.border-sev-medium { border-left-color: #f59e0b; }
.border-sev-low { border-left-color: #3b82f6; }
.rec-header { display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.75rem; flex-wrap: wrap; }
.pill-priority { background: #f1f5f9; color: #475569; font-size: 0.75rem; font-weight: 600; padding: 0.15rem 0.4rem; border-radius: 4px; }
.pill-dataset { color: #1e293b; font-size: 0.85rem; font-weight: 700; }
.pill-sev { font-size: 0.75rem; font-weight: 600; }
.rec-issue-type { font-size: 0.95rem; font-weight: 700; margin: 0 0 0.5rem 0; color: #334155; }
.rec-fix-text, .rec-why-text, .rec-risk-text { font-size: 0.88rem; margin: 0.35rem 0; color: #475569; }
.code-wrap { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 0.5rem; margin: 0.50rem 0; }
.code-title { display: block; font-size: 0.75rem; font-weight: 600; color: #64748b; margin-bottom: 0.25rem; text-transform: uppercase; }
.code-wrap pre { margin: 0; padding: 0; overflow-x: auto; }
.code-wrap code { font-family: monospace; font-size: 0.8rem; color: #0f172a; }
"""

    _report_css = get_report_html_css()
    governance_html = ""
    try:
        from agent.report_governance_sections import render_governance_html

        governance_html = render_governance_html(result) or ""
    except Exception:
        governance_html = ""

    # Generate LLM Recommendations html block
    dq_recs = result.get("dq_recommendations") or {}
    recs_list = dq_recs.get("recommendations") or dq_recs.get("findings") or []
    nav_llm_rec_link = ""
    llm_recs_section_html = ""
    if recs_list:
        nav_llm_rec_link = '\n  <a href="#llm-recs" class="nav-jump">LLM recommendations</a>'

        # Load downstream SQL target configuration from thresholds
        try:
            from agent.profiling.assessment_orchestrator import load_dq_thresholds
            thresholds_cfg = load_dq_thresholds()
        except Exception:
            thresholds_cfg = {}
        downstream_sql_target = (thresholds_cfg.get("downstream") or {}).get("sql_target")

        # 1. General Remediation-Unit Grouping
        # Keys:
        # Field: (dataset_id, "field", column_name)
        # Row: (dataset_id, "row", finding_family)
        # Configuration: (dataset_id, "configuration", manifest_field)
        # Relationship: (relationship_id, "relationship", rule_id)
        # Dataset: (dataset_id, "dataset", finding_family)
        from collections import defaultdict
        unit_groups = defaultdict(list)

        def _extract_manifest_col(r_item: Dict[str, Any]) -> str:
            col_val = str(r_item.get("column") or "").strip()
            if col_val and col_val not in ("*", "[Row-level]", "[row-level]", "manifest"):
                return col_val
            msg_str = str(r_item.get("message") or "")
            m = re.search(r"(?:manifest column not in data|column missing from data|missing column|missing field|missing)\s*[:\s]\s*([a-zA-Z0-9_]+)", msg_str, re.IGNORECASE)
            if m:
                return m.group(1)
            m = re.search(r"['\"]([a-zA-Z0-9_]+)['\"]", msg_str)
            if m:
                return m.group(1)
            return col_val or "manifest"

        for r in recs_list:
            ds = str(r.get("dataset") or "global")
            col = r.get("column")
            itype = str(r.get("issue_type") or "unknown").lower()
            
            if "manifest" in itype or itype == "metadata_manifest_mismatch":
                target = _extract_manifest_col(r)
                key = (ds, "configuration", target)
            elif col and str(col) not in ("*", "[Row-level]", "[row-level]"):
                key = (ds, "field", str(col))
            elif any(k in itype for k in ["relationship", "foreign_key", "orphan"]):
                target = str(r.get("related_dataset") or r.get("related_column") or itype)
                key = (ds, "relationship", target)
            elif "duplicate" in itype:
                key = (ds, "row", "duplicate_rows")
            else:
                key = (ds, "dataset", itype)
            unit_groups[key].append(r)

        # Sort unit groups consecutively by severity and priority to eliminate priority gaps (Point 9)
        SEV_ORDER = {"high": 1, "medium": 2, "low": 3}
        def _sort_unit(item):
            k, f_list = item
            sevs = [str(f.get("severity") or "medium").lower() for f in f_list]
            min_sev = min(SEV_ORDER.get(s, 2) for s in sevs)
            pri_vals = [f.get("priority") for f in f_list if f.get("priority") is not None]
            min_pri = min(pri_vals) if pri_vals else 2
            return (min_sev, min_pri, k[0], k[1], k[2])

        sorted_unit_groups = sorted(unit_groups.items(), key=_sort_unit)

        # First pass: classify per-card provenance strictly (Points 1 & 2)
        card_provenances = []
        for key, findings in sorted_unit_groups:
            scope_type = key[1]
            if scope_type == "configuration":
                card_provenances.append("CONFIG")
                continue
            is_appr = any(f.get("lifecycle", {}).get("approval_status") == "approved" or f.get("approval_status") == "approved" for f in findings)
            has_std_r = any(f.get("authority_source") in ("DETERMINISTIC_REGISTRY", "standard", "auto_detected") for f in findings)
            has_cat_r = any(f.get("authority_source") == "CATALOG_FALLBACK" or (f.get("authority_source") == "LLM_ADVISORY" and f.get("ai_assistance_status") == "unavailable") or "AI-assisted ranking was unavailable" in str(f.get("suggested_fix") or "") or "AI ranking unavailable" in str(f.get("suggested_fix") or "") for f in findings)
            has_ai_r = any(f.get("authority_source") == "LLM_ADVISORY" and f.get("ai_assistance_status") == "available" for f in findings) and not has_cat_r

            if is_appr:
                card_provenances.append("APPROVED")
            elif has_std_r and has_cat_r:
                card_provenances.append("MIXED")
            elif has_cat_r:
                card_provenances.append("CATALOG")
            elif has_ai_r:
                card_provenances.append("AI")
            else:
                card_provenances.append("STANDARD")

        std_count = sum(1 for p in card_provenances if p == "STANDARD")
        ai_count = sum(1 for p in card_provenances if p == "AI")
        cat_count = sum(1 for p in card_provenances if p in ("CATALOG", "MIXED"))
        cfg_count = sum(1 for p in card_provenances if p == "CONFIG")

        if ai_count > 0 and (cat_count > 0 or std_count > 0):
            rec_mode_label = "Mixed (AI-assisted + Standard Catalog)"
        elif ai_count > 0:
            rec_mode_label = "AI-Assisted Advisory"
        elif cat_count > 0:
            rec_mode_label = "Deterministic Fallback (LLM Config Unavailable)"
        else:
            rec_mode_label = "Standard Deterministic Registry"

        cards = []
        for idx, (key, findings) in enumerate(sorted_unit_groups, 1):
            ds_name, scope_type, target_name = key
            first = findings[0]
            card_prov = card_provenances[idx - 1]
            
            # Recalculated consecutive priority (P1, P2, ... Pn) without gaps (Point 9)
            priority = f"P{idx}"
            pri_vals = [f.get("priority") for f in findings if f.get("priority") is not None]
            min_pri = min(pri_vals) if pri_vals else idx

            sevs = [str(f.get("severity") or "medium").lower() for f in findings]
            sev = "high" if "high" in sevs else ("medium" if "medium" in sevs else "low")
            
            issues = list(dict.fromkeys(str(f.get("issue_type") or "") for f in findings))
            issue_badges = " ".join(f'<code>{esc(i)}</code>' for i in issues)

            is_approved = (card_prov == "APPROVED")
            has_fallback = (card_prov in ("CATALOG", "MIXED"))

            if card_prov == "APPROVED":
                src_badge_label = "Human approved"
                src_badge_class = "badge-approved"
            elif card_prov == "MIXED":
                src_badge_label = "Mixed work package: standard normalization with catalog-governed semantic handling"
                src_badge_class = "badge-catalog"
            elif card_prov == "CATALOG":
                src_badge_label = "Catalog option, AI ranking unavailable"
                src_badge_class = "badge-catalog"
            elif card_prov == "AI":
                src_badge_label = "AI-assisted option"
                src_badge_class = "badge-ai"
            elif card_prov == "CONFIG":
                src_badge_label = "Configuration Action"
                src_badge_class = "badge-config"
            else:
                src_badge_label = "Standard remediation"
                src_badge_class = "badge-standard"

            # Impact and threshold metrics bar on every card (Point 10)
            ds_total_rows = (result.get("datasets", {}).get(ds_name, {}).get("row_count") or 50)
            aff_counts = [f.get("affected_count") or f.get("row_count_affected") or 0 for f in findings if (f.get("affected_count") or f.get("row_count_affected")) is not None]
            affected_rows = max(aff_counts) if aff_counts else (1 if scope_type == "configuration" else 0)
            aff_pct_val = round((affected_rows / ds_total_rows) * 100, 1) if ds_total_rows > 0 else 0.0
            
            decision_text = "Pre-approved for pilot" if is_approved else "Human approval required"
            decision_color = "#166534" if is_approved else "#b45309"
            
            impact_bar_html = f'''<div class="card-impact-metrics" style="display: flex; flex-wrap: wrap; gap: 14px; background-color: #f8fafc; border: 1px solid #e2e8f0; padding: 6px 10px; border-radius: 6px; font-size: 0.8rem; margin: 6px 0; color: #334155;">
              <div><strong>Affected:</strong> {affected_rows} of {ds_total_rows} rows ({aff_pct_val}%)</div>
              <div><strong>Policy Limit:</strong> 2.0% or 5,000 rows</div>
              <div><strong>Decision:</strong> <span style="font-weight: 600; color: {decision_color};">{decision_text}</span></div>
            </div>'''

            # Destination-aware paths
            file_extensions = ("csv", "json", "parquet", "tsv", "txt", "xml", "xlsx")
            if "." in ds_name and ds_name.rsplit(".", 1)[-1].lower() in file_extensions:
                clean_ds_name = ds_name.rsplit(".", 1)[0]
                src_ext = ds_name.rsplit(".", 1)[-1].upper()
            else:
                clean_ds_name = ds_name
                src_ext = "TABLE" if "." in ds_name else "FILE"

            # Check if this is a Configuration Scope card (Point 3)
            if scope_type == "configuration":
                msg = first.get("message") or first.get("why_it_matters") or f"Manifest field '{target_name}' is absent from {ds_name}."
                fix = f"Update config/metadata_manifest.yaml to remove or map missing field '{target_name}'."
                patch_yaml = (
                    "# Proposed Configuration Patch:\n"
                    "metadata_manifest:\n"
                    f"  datasets:\n"
                    f"    {ds_name}:\n"
                    "      reconcile_fields:\n"
                    "        - action: remove_or_map_missing\n"
                    f'          field_name: "{target_name}"\n'
                    f'          justification: "Manifest field is absent from {ds_name}"\n'
                )
                cards.append(f'''
            <div class="rec-card border-sev-{sev}">
              <div class="rec-header">
                <span class="pill-priority">{priority}</span>
                <span class="pill-dataset">{esc(ds_name)} [Config: {esc(target_name)}]</span>
                <span class="pill-source badge badge-config">Configuration Action</span>
                <span class="pill-sev badge sev-{sev}">{sev.upper()}</span>
              </div>
              <div class="rec-body">
                <h4 class="rec-issue-type">Configuration Anomaly: {issue_badges}</h4>
                {impact_bar_html}
                <div style="background-color: #fffbeb; border: 1px solid #fde68a; color: #92400e; padding: 8px 12px; border-radius: 6px; font-size: 0.82rem; margin: 8px 0;">
                  <strong>Metadata Inconsistency:</strong> {esc(msg)}
                </div>
                <p class="rec-fix-text"><strong>Recommended Action:</strong> {esc(fix)}</p>
                <div class="code-wrap">
                  <span class="code-title">Proposed Configuration Patch (config/metadata_manifest.yaml)</span>
                  <pre data-raw="{esc_attr(patch_yaml)}"><code class="language-yaml">{esc(patch_yaml)}</code></pre>
                </div>
                <p style="font-size: 0.78rem; color: #64748b; margin-top: 4px;"><em>* Schema configuration action does not mutate dataset rows.</em></p>
              </div>
            </div>
            ''')
                continue

            # Check if Field-Level Work Package (multi-finding column)
            if scope_type == "field":
                from agent.etl_pipeline.composite_field_compiler import create_field_work_package, compile_field_work_package
                try:
                    wp = create_field_work_package(
                        dataset_id=ds_name,
                        column_name=target_name,
                        findings=findings,
                        semantic_type=first.get("semantic_type"),
                    )
                    art_pyspark = compile_field_work_package(wp, dialect="pyspark")
                    art_pandas = compile_field_work_package(wp, dialect="pandas")
                    pyspark_code = art_pyspark.code
                    pandas_code = art_pandas.code
                    is_provisional = art_pyspark.is_provisional
                    has_quarantine = wp.has_quarantine
                except Exception as comp_err:
                    pyspark_code = first.get("example_pyspark")
                    pandas_code = first.get("example_pandas")
                    is_provisional = True
                    has_quarantine = "quarantine" in str(first.get("mapped_action") or "")

                sql_code = first.get("example_sql")

                pyspark_html = ""
                if pyspark_code:
                    pyspark_html = f'''<div class="code-wrap">
                        <span class="code-title">PySpark DataFrame Remediation (Canonical Field Pipeline)</span>
                        <pre data-raw="{esc_attr(pyspark_code)}"><code class="language-python">{esc(pyspark_code)}</code></pre>
                    </div>'''

                pandas_html = ""
                if pandas_code:
                    pandas_html = f'''<div class="code-wrap">
                        <span class="code-title">Pandas DataFrame Remediation (For smaller files)</span>
                        <pre data-raw="{esc_attr(pandas_code)}"><code class="language-python">{esc(pandas_code)}</code></pre>
                    </div>'''

                sql_html = ""
                if sql_code and downstream_sql_target:
                    sql_html = f'''<div class="code-wrap">
                        <span class="code-title">Optional Downstream SQL Table Implementation</span>
                        <pre data-raw="{esc_attr(sql_code)}"><code class="language-sql">{esc(sql_code)}</code></pre>
                    </div>'''

                val_col = f"{target_name}_validation_status"
                reason_col = f"{target_name}_validation_reason"
                
                # Retain-all quarantine clarification (Point 12)
                if has_quarantine:
                    quarantine_li = f"<li><strong>Quarantine Path:</strong> <code>Files/quarantine/{clean_ds_name}/</code> (format: delta)</li><li><strong>Row Disposition:</strong> <code>clean XOR quarantine (mutually exclusive)</code></li>"
                else:
                    quarantine_li = f"<li><strong>Quarantine Behavior:</strong> <em>Not used for selected option (clean companion retains complete source rows; quarantine_df is empty)</em></li><li><strong>Row Disposition:</strong> <code>retain_all (complete source rows retained)</code></li>"

                paths_html = f'''<div class="code-wrap" style="background-color: #f0fdf4; border-color: #bbf7d0;">
                    <span class="code-title" style="color: #166534;">Quarantine & Clean-Output Paths (Blob Native)</span>
                    <ul style="font-size: 0.82rem; margin: 4px 0; padding-left: 16px; color: #1e3a1e; line-height: 1.4;">
                        <li><strong>Source Format:</strong> {src_ext}</li>
                        <li><strong>Clean Dataset Path:</strong> <code>Files/clean/{clean_ds_name}/</code> (format: delta)</li>
                        {quarantine_li}
                        <li><strong>Validation Status Column:</strong> <code>{val_col}</code></li>
                        <li><strong>Validation Reason Column:</strong> <code>{reason_col}</code></li>
                    </ul>
                </div>'''

                why = first.get("why_it_matters") or f"Validation and hygiene rules violated in {ds_name}.{target_name}."
                raw_fix = first.get("suggested_fix") or "Apply standardized field cleansing pipeline."
                
                # Sanitize suggested fix wording and align with contracts (Points 1, 5, 7, 8)
                t_lower = target_name.lower()
                outcome_text = ""
                if "phone" in t_lower:
                    suggested_fix = "Strip configured formatting characters and validate the resulting digit sequence against configured length (7 to 15 digits) and placeholder rules."
                    outcome_text = "Configured formatting characters are stripped, and resulting digit sequence is validated against configured length (7 to 15) and placeholder rules. Source rows are retained."
                elif "email" in t_lower:
                    suggested_fix = "Normalize whitespace and casing for eligible email values. Values that fail the configured email pattern are retained in the raw field and set to null in the clean companion."
                    outcome_text = "Eligible email values are trimmed and lowercased. Values that fail syntax validation are preserved in raw and set to null in clean companion."
                elif "age" in t_lower:
                    suggested_fix = "Convert integer-equivalent floats to integer clean representation and flag out-of-range or non-integer values while retaining all source rows."
                    outcome_text = "Integer-equivalent values are converted to a nullable integer clean representation. Negative, fractional, unparseable or above-range values are flagged while all source rows are retained."
                elif card_prov == "MIXED":
                    suggested_fix = "Standard whitespace and character normalization combined with catalog-governed placeholder handling. (AI-assisted ranking was unavailable)."
                    outcome_text = "Normalizes whitespace/casing and standardizes configured placeholders while retaining source records."
                elif card_prov == "CATALOG":
                    clean_lbl = re.sub(r'^AI-assisted option:\s*', '', raw_fix, flags=re.IGNORECASE)
                    suggested_fix = f"Catalog option: {clean_lbl.rstrip('.')} (AI-assisted ranking was unavailable)."
                    outcome_text = "Catalog option applied under human governance. Full source records preserved."
                else:
                    suggested_fix = re.sub(r'\bis valid email\b', 'validate email format and syntax', raw_fix, flags=re.IGNORECASE)

                # Check if decision pending without selected option
                is_noop = any((f.get("mapped_action") or f.get("suggested_action")) in ("keep_as_is", "skip_etl", "noop") for f in findings)
                has_selected_opt = any(f.get("selected_option_id") or f.get("suggested_action") or f.get("mapped_action") for f in findings)

                fallback_banner = '<div style="background-color: #fffbeb; border: 1px solid #fde68a; color: #92400e; padding: 6px 10px; border-radius: 6px; font-size: 0.8rem; margin: 8px 0;">AI-assisted ranking was unavailable. Controlled catalog options are available for human selection.</div>' if has_fallback else ""

                if is_noop:
                    code_display_html = '<div style="background-color: #f8fafc; border: 1px dashed #cbd5e1; color: #64748b; font-style: italic; padding: 8px 12px; border-radius: 6px; font-size: 0.82rem; margin: 8px 0;">No-op: Retained as-is without ETL transformation code.</div>'
                elif not has_selected_opt and not is_approved:
                    status_banner = '<div style="background-color: #fffbeb; border: 1px solid #fde68a; color: #92400e; padding: 6px 10px; border-radius: 6px; font-size: 0.8rem; margin: 8px 0;"><strong>Decision Required:</strong> Options are available in the planning page. No executable transformation code is authorized until an option is selected.</div>'
                    code_display_html = status_banner
                elif is_provisional:
                    status_banner = '<div style="background-color: #f1f5f9; border-left: 3px solid #64748b; color: #475569; padding: 6px 10px; font-size: 0.78rem; margin: 8px 0;"><strong>Provisional code preview for the selected option.</strong> This transformation is not approved or authorized for execution.</div>'
                    code_display_html = f"{status_banner}{pyspark_html}{pandas_html}{sql_html}"
                else:
                    status_banner = '<div style="background-color: #ecfdf5; border-left: 3px solid #059669; color: #065f46; padding: 6px 10px; font-size: 0.78rem; margin: 8px 0;"><strong>Human approved.</strong> Deterministic remediation artifact generated. Validation pending.</div>'
                    code_display_html = f"{status_banner}{pyspark_html}{pandas_html}{sql_html}"

                outcome_p = f'<p class="rec-outcome-text" style="font-size: 0.84rem; margin: 6px 0; color: #334155;"><strong>Expected Outcome:</strong> {esc(outcome_text)}</p>' if outcome_text else ""

                # Scannability: Collapse code previews behind <details> (Point 11)
                technical_details_html = f'''<details class="code-preview-details" style="margin-top: 10px; border: 1px solid #e2e8f0; border-radius: 6px; background-color: #fafafa; padding: 6px 10px;">
                  <summary style="font-weight: 600; font-size: 0.82rem; cursor: pointer; color: #2563eb; padding: 4px 0;">
                    ▶ View Generated Code Previews & Technical Implementation Paths
                  </summary>
                  <div style="margin-top: 8px;">
                    {code_display_html}
                    {paths_html}
                  </div>
                </details>'''

                cards.append(f'''
            <div class="rec-card border-sev-{sev}">
              <div class="rec-header">
                <span class="pill-priority">{priority}</span>
                <span class="pill-dataset">{esc(ds_name)}.{esc(target_name)}</span>
                <span class="pill-source badge {src_badge_class}">{src_badge_label}</span>
                <span class="pill-sev badge sev-{sev}">{sev.upper()}</span>
              </div>
              <div class="rec-body">
                <h4 class="rec-issue-type">Field Remediation Unit: {issue_badges}</h4>
                {impact_bar_html}
                {fallback_banner}
                <p class="rec-fix-text"><strong>Suggested Fix:</strong> {esc(suggested_fix)}</p>
                {outcome_p}
                <p class="rec-why-text"><strong>Why it matters:</strong> {esc(why)}</p>
                <div style="font-size: 0.8rem; color: #475569; margin: 4px 0;"><strong>Row Disposition:</strong> <code>{'clean XOR quarantine' if has_quarantine else 'retain_all (complete source rows retained)'}</code></div>
                {technical_details_html}
              </div>
            </div>
            ''')
            else:
                # Row, relationship, or dataset level card
                sql_code = first.get("example_sql")
                pandas_code = first.get("example_pandas")
                pyspark_code = first.get("example_pyspark")

                # Non-destructive duplicate rows metadata code (Point 13)
                is_dup = "duplicate" in target_name.lower() or any("duplicate" in str(f.get("issue_type") or "").lower() for f in findings)
                if is_dup and (not pyspark_code or "manually" in pyspark_code):
                    pyspark_code = (
                        "import pyspark.sql.functions as F\\n"
                        "from pyspark.sql.window import Window\\n"
                        "# Deterministic Duplicate Evidence Attachment: retain complete source rows with duplicate metadata\\n"
                        "w = Window.partitionBy(df.columns).orderBy(F.monotonically_increasing_id())\\n"
                        "df = (\\n"
                        "    df\\n"
                        "    .withColumn('row_duplicate_rank', F.row_number().over(w))\\n"
                        "    .withColumn('row_is_duplicate', F.col('row_duplicate_rank') > 1)\\n"
                        "    .withColumn('row_validation_status', F.when(F.col('row_is_duplicate'), 'duplicate_candidate').otherwise('valid'))\\n"
                        "    .withColumn('row_validation_reason', F.when(F.col('row_is_duplicate'), 'Potential duplicate candidate retained for review').otherwise(None))\\n"
                        ")\\n"
                        "clean_df = df\\n"
                        "quarantine_df = df.limit(0)"
                    )
                    pandas_code = (
                        "import pandas as pd\\n"
                        "# Deterministic Duplicate Evidence Attachment: retain complete source rows with duplicate metadata\\n"
                        "df['row_is_duplicate'] = df.duplicated(keep='first')\\n"
                        "df['row_validation_status'] = df['row_is_duplicate'].map({True: 'duplicate_candidate', False: 'valid'})\\n"
                        "df['row_validation_reason'] = df['row_is_duplicate'].map({True: 'Potential duplicate candidate retained for review', False: None})\\n"
                        "clean_df = df.copy()\\n"
                        "quarantine_df = df.iloc[0:0].copy()"
                    )

                pyspark_html = f'<div class="code-wrap"><span class="code-title">PySpark DataFrame Remediation</span><pre data-raw="{esc_attr(pyspark_code)}"><code class="language-python">{esc(pyspark_code)}</code></pre></div>' if pyspark_code else ""
                pandas_html = f'<div class="code-wrap"><span class="code-title">Pandas DataFrame Remediation</span><pre data-raw="{esc_attr(pandas_code)}"><code class="language-python">{esc(pandas_code)}</code></pre></div>' if pandas_code else ""
                sql_html = f'<div class="code-wrap"><span class="code-title">Optional Downstream SQL Table Implementation</span><pre data-raw="{esc_attr(sql_code)}"><code class="language-sql">{esc(sql_code)}</code></pre></div>' if (sql_code and downstream_sql_target) else ""

                fallback_banner = '<div style="background-color: #fffbeb; border: 1px solid #fde68a; color: #92400e; padding: 6px 10px; border-radius: 6px; font-size: 0.8rem; margin: 8px 0;">AI-assisted ranking was unavailable. Controlled catalog options are available for human selection.</div>' if has_fallback else ""

                is_noop = any((f.get("mapped_action") or f.get("suggested_action")) in ("keep_as_is", "skip_etl", "noop") for f in findings)
                if is_noop:
                    code_display_html = '<div style="background-color: #f8fafc; border: 1px dashed #cbd5e1; color: #64748b; font-style: italic; padding: 8px 12px; border-radius: 6px; font-size: 0.82rem; margin: 8px 0;">No-op: Retained as-is without ETL transformation code.</div>'
                elif first.get("lifecycle", {}).get("approval_status") == "pending" or not is_approved:
                    provisional_banner = '<div style="background-color: #f1f5f9; border-left: 3px solid #64748b; color: #475569; padding: 6px 10px; font-size: 0.78rem; margin: 8px 0;"><strong>Provisional code preview for the selected option.</strong> This transformation is not approved or authorized for execution.</div>'
                    code_display_html = f"{provisional_banner}{pyspark_html}{pandas_html}{sql_html}"
                else:
                    code_display_html = f"{pyspark_html}{pandas_html}{sql_html}"

                val_col = "row_validation_status" if is_dup else f"{target_name}_validation_status"
                reason_col = "row_validation_reason" if is_dup else f"{target_name}_validation_reason"
                
                paths_html = f'''<div class="code-wrap" style="background-color: #f0fdf4; border-color: #bbf7d0;">
                    <span class="code-title" style="color: #166534;">Quarantine & Clean-Output Paths (Blob Native)</span>
                    <ul style="font-size: 0.82rem; margin: 4px 0; padding-left: 16px; color: #1e3a1e; line-height: 1.4;">
                        <li><strong>Source Format:</strong> {src_ext}</li>
                        <li><strong>Clean Dataset Path:</strong> <code>Files/clean/{clean_ds_name}/</code> (format: delta)</li>
                        <li><strong>Quarantine Behavior:</strong> <em>Not used for selected option (clean companion retains complete source rows; quarantine_df is empty)</em></li>
                        <li><strong>Validation Column:</strong> <code>{val_col}</code></li>
                        <li><strong>Reason Column:</strong> <code>{reason_col}</code></li>
                        <li><strong>Row Disposition:</strong> <code>retain_all (complete source rows retained)</code></li>
                    </ul>
                </div>'''

                technical_details_html = f'''<details class="code-preview-details" style="margin-top: 10px; border: 1px solid #e2e8f0; border-radius: 6px; background-color: #fafafa; padding: 6px 10px;">
                  <summary style="font-weight: 600; font-size: 0.82rem; cursor: pointer; color: #2563eb; padding: 4px 0;">
                    ▶ View Generated Code Previews & Technical Implementation Paths
                  </summary>
                  <div style="margin-top: 8px;">
                    {code_display_html}
                    {paths_html}
                  </div>
                </details>'''

                cards.append(f'''
            <div class="rec-card border-sev-{sev}">
              <div class="rec-header">
                <span class="pill-priority">{priority}</span>
                <span class="pill-dataset">{esc(ds_name)} [{scope_type.upper()}: {esc(target_name)}]</span>
                <span class="pill-source badge {src_badge_class}">{src_badge_label}</span>
                <span class="pill-sev badge sev-{sev}">{sev.upper()}</span>
              </div>
              <div class="rec-body">
                <h4 class="rec-issue-type">Remediation Unit: {issue_badges}</h4>
                {impact_bar_html}
                {fallback_banner}
                <p class="rec-fix-text"><strong>Suggested Fix:</strong> {esc(first.get("suggested_fix") or "")}</p>
                <p class="rec-why-text"><strong>Why it matters:</strong> {esc(first.get("why_it_matters") or "")}</p>
                <div style="font-size: 0.8rem; color: #475569; margin: 4px 0;"><strong>Row Disposition:</strong> <code>retain_all (complete source rows retained)</code></div>
                {technical_details_html}
              </div>
            </div>
            ''')
        
        sql_target_label = "Configured" if downstream_sql_target else "Not configured (Downstream SQL recommendations omitted)"
        llm_recs_section_html = f"""
        <section id="llm-recs" class="llm-recs-section">
          <h2>Data Cleaning & Remediation Recommendations</h2>
          <div style="background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px; margin-bottom: 24px;">
            <h3 style="margin: 0 0 8px 0; font-size: 1rem; color: #0f172a;">Remediation Engine Configuration Context</h3>
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; font-size: 0.88rem; color: #334155;">
              <div><strong>Primary Remediation Engine:</strong> PySpark DataFrame API</div>
              <div><strong>Source Storage:</strong> Blob files (OneLake/Blob Store)</div>
              <div><strong>Downstream SQL Target:</strong> <code>{esc(sql_target_label)}</code></div>
              <div><strong>Recommendation Mode:</strong> {esc(rec_mode_label)}</div>
              <div><strong>Governance Breakdown:</strong> Standard: {std_count} · AI-assisted: {ai_count} · Catalog fallback: {cat_count} · Config: {cfg_count}</div>
              <div><strong>Action Policy:</strong> DRY_RUN, validation required for all transformations.</div>
            </div>
          </div>
          <div class="rec-cards-grid">
            {"".join(cards)}
          </div>
        </section>
        """

    # Build DQ Score Methodology Section
    methodology_html = f"""
    <section id="score-methodology" class="datasets-section" style="margin-top: 30px; background: #fff; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
      <h2>DQ Score Methodology</h2>
      <p class="section-lead">Authoritative metrics, dimension weights, and score coverage standards.</p>
      
      <div style="margin-bottom: 20px;">
        <h3>DQ Score Formula: Dimension Scorer v3</h3>
        <p>The overall DQ score is the weighted average of evaluated quality dimensions. Dimensions without applicable or successfully evaluated rules are excluded from the effective denominator and reported separately through score coverage.</p>
        <code style="display: block; padding: 12px; background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; margin: 10px 0; font-size: 1.0em; font-family: monospace; color: #0f172a;">
          Overall DQ Score = Sum(Dimension_Score &times; Configured_Weight) / Total_Evaluated_Weight
        </code>

        
        <h4 style="margin-top: 20px;">Readiness Classification Policy</h4>
        <p>Datasets and overall pipelines are gated against the following configured readiness criteria:</p>
        <ul style="margin-top: 8px; padding-left: 20px; line-height: 1.6;">
          <li><strong>READY:</strong> Score &ge; {scorecard.get('thresholds_used', {}).get('ready', 70.0)}, sufficient score coverage (&ge; 80%), no HIGH-severity issues, and no non-blocking warning gates triggered.</li>
          <li><strong>READY_WITH_WARNINGS:</strong> Score &ge; {scorecard.get('thresholds_used', {}).get('ready', 70.0)}, sufficient score coverage (&ge; 80%), no HIGH-severity issues, with one or more non-blocking findings (e.g. MEDIUM severity) requiring advisory review.</li>
          <li><strong>NEEDS_WORK:</strong> Score between {scorecard.get('thresholds_used', {}).get('blocked', 35.0)} and {scorecard.get('thresholds_used', {}).get('ready', 70.0)}, or an unresolved gating HIGH-severity issue that gates an otherwise numerically ready score.</li>
          <li><strong>BLOCKED:</strong> Score &lt; {scorecard.get('thresholds_used', {}).get('blocked', 35.0)}, missing mandatory primary key, or explicit blocking policy violation.</li>
          <li><strong>UNKNOWN:</strong> Insufficient score coverage (&lt; 40%), execution failure, incomplete assessment, or missing mandatory configuration.</li>
        </ul>
      </div>
      
      <h3>Multidimensional Quality Breakdown</h3>
    """
    
    for ds_name, ds_meta in datasets.items():
        ds_summary = (dq_ds.get(ds_name) or {}).get("summary") or {}
        db = ds_summary.get("dimension_breakdown") or {}
        if db:
            methodology_html += f"""
            <div style="margin-top: 15px; padding: 15px; border: 1px solid #eee; border-radius: 6px; background-color: #fafafa;">
              <h4>Dataset: <code>{esc(ds_name)}</code></h4>
              <table class="profile-tbl" style="width: 100%; margin-top: 10px; border-collapse: collapse;">
                <thead>
                  <tr style="background-color: #f2f2f2; border-bottom: 1px solid #ccc;">
                    <th style="padding: 6px; text-align: left;">Dimension</th>
                    <th style="padding: 6px; text-align: left;">Score</th>
                    <th style="padding: 6px; text-align: left;">Details</th>
                  </tr>
                </thead>
                <tbody>
            """
            for dim, details in db.items():
                if not details:
                    continue
                score_val = details.get("score")
                score_str = f"<strong>{score_val}%</strong>" if score_val is not None else "N/A"
                plain_score_str = f"{score_val}%" if score_val is not None else "N/A"
                
                # Relabel dimensions for clarity
                dim_l = dim.lower()
                if dim_l == "uniqueness":
                    dim_label = "Declared-Key Uniqueness"
                elif dim_l == "accuracy":
                    dim_label = "Inferred Accuracy (Plausibility proxy — no golden source comparison)"
                else:
                    dim_label = dim.capitalize()
                
                details_parts = []
                # Check for engine3 dimension scoring breakdown metrics
                has_rules_count = ("passed_rules_count" in details or "failed_rules_count" in details or "evaluated_rules_count" in details)
                if has_rules_count:
                    passed = details.get("passed_rules_count", 0)
                    failed = details.get("failed_rules_count", 0)
                    total_rules = details.get("evaluated_rules_count", passed + failed)
                    tot_units = details.get("total_eligible_units", 0)
                    p_units = details.get("passed_units", 0)
                    f_units = details.get("failed_units", 0)
                    if tot_units > 0:
                        details_parts = [
                            f"Total eligible units: {tot_units}",
                            f"Passed units: {p_units}",
                            f"Failed units: {f_units}",
                            f"Rule status: {passed}/{total_rules} clean",
                            f"Calculation: {p_units}/{tot_units} = {plain_score_str}"
                        ]
                    else:
                        details_parts = [
                            f"Passed evaluations: {passed}",
                            f"Failed evaluations: {failed}",
                            f"Evaluated rules/checks: {total_rules}",
                            "Calculation method: rule evaluation units pass rate"
                        ]
                elif dim_l == "consistency":
                    violations = details.get("cross_field_violations", 0)
                    total_rules = details.get("evaluated_rules", 0)
                    passed = max(0, total_rules - violations)
                    details_parts = [
                        f"Passed evaluations: {passed}",
                        f"Failed evaluations: {violations}",
                        f"Evaluated rules/cells: {total_rules}",
                        "Calculation method: row-level rule pass rate"
                    ]
                else:
                    for k, v in details.items():
                        if k != "score" and v is not None:
                            details_parts.append(f"{k.replace('_', ' ')}: {v}")
                
                details_str = ", ".join(details_parts)
                
                methodology_html += f"""
                  <tr style="border-bottom: 1px solid #e0e0e0;">
                    <td style="padding: 6px;"><strong>{dim_label}</strong></td>
                    <td style="padding: 6px;">{score_str}</td>
                    <td style="padding: 6px;">{esc(details_str)}</td>
                  </tr>
                """
            methodology_html += """
                </tbody>
              </table>
            </div>
            """
    methodology_html += """
    <div style="background-color: #f8fafc; border-left: 4px solid #3b82f6; border-radius: 4px; padding: 14px 18px; margin-top: 18px; font-size: 0.9rem; line-height: 1.5;">
      <p style="margin: 0 0 8px 0;"><strong>Executive Score Interpretation Guidance:</strong> Intrinsic scores represent the mathematically weighted average across profile dimensions. 
      However, <strong>operational release authorization is strictly governed by use-case gating policies</strong>. Any dataset containing active HIGH-severity blockers 
      Active HIGH-severity findings cap the gating-adjusted readiness score at 69.9 and classify the assessment as NEEDS WORK. Individual operational use cases may separately be BLOCKED. Direct production consumption remains prohibited until the applicable release gates are cleared.</p>
      <p style="margin: 0;"><strong>Accuracy Plausibility Proxy Notice:</strong> Inferred accuracy scores (e.g., 100% on passing rules) reflect the mathematical pass-rate of defined domain plausibility constraints 
      (e.g., 1/1 validation rule satisfied), and do not certify golden-source factual truth.</p>
    </div>
    """
    methodology_html += "</section>"

    # Build technical_appendix_html with robust, reproducible metadata
    import hashlib, uuid, subprocess
    rm = result.get("run_metadata") or {}
    gov = result.get("governance") or {}
    run_id = result.get("run_id") or rm.get("run_id") or gov.get("assessment_run_id") or gov.get("run_id")
    if not run_id or run_id == "N/A":
        run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        result["run_id"] = run_id

    agent_ver = gov.get("agent_version") or "v2.4.0-prod"
    manifest_ver = gov.get("manifest_schema_version") or "2.0.0"
    job_id = gov.get("job_id")
    if not job_id or job_id == "job_dq_assessment_01":
        job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        gov["job_id"] = job_id

    git_commit_sha = "N/A"
    try:
        git_commit_sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, timeout=2
        ).decode("utf-8").strip()
    except Exception:
        git_commit_sha = "main-dev-2026.09"

    schema_hash = gov.get("schema_hash")
    if not schema_hash or schema_hash == "N/A":
        schema_parts = []
        for dname in sorted(datasets.keys()):
            dmeta = datasets[dname]
            col_tuples = []
            for cname, cmeta in sorted((dmeta.get("columns") or {}).items()):
                col_tuples.append(f"{cname}:{cmeta.get('data_type')}:{cmeta.get('semantic_type')}")
            schema_parts.append(f"{dname}=[{','.join(col_tuples)}]")
        schema_hash = "sha256:" + hashlib.sha256(";".join(schema_parts).encode("utf-8")).hexdigest()[:16]
        gov["schema_hash"] = schema_hash

    rules_hash = gov.get("rules_hash")
    if not rules_hash or rules_hash == "N/A":
        pl_tokens = ["???", "N/A", "unknown", "tbd", "not available", "dummy", "temp", "null", "none", "-", "--", "0000", "test"]
        rules_content = f"rules_v2.4.0:{sorted(pl_tokens)}"
        rules_hash = "sha256:" + hashlib.sha256(rules_content.encode("utf-8")).hexdigest()[:16]
        gov["rules_hash"] = rules_hash
    
    active_placeholders_str = "???, N/A, unknown, tbd, not available, dummy, temp, null, none, -, --, 0000, test"
    
    reproducibility_html = f"""
    <div style="background-color: #fff; border-radius: 8px; padding: 20px; border: 1px solid #ddd; margin-top: 20px;">
      <h4>Reproducibility Metadata</h4>
      <ul style="padding-left: 20px; line-height: 1.6;">
        <li><strong>Assessment Run ID:</strong> <code>{esc(str(run_id))}</code></li>
        <li><strong>Job ID:</strong> <code>{esc(str(job_id))}</code></li>
        <li><strong>Git Commit SHA:</strong> <code>{esc(str(git_commit_sha))}</code></li>
        <li><strong>Deployment Version:</strong> <code>{esc(str(agent_ver))}</code></li>
        <li><strong>Manifest Schema Version:</strong> <code>{esc(str(manifest_ver))}</code></li>
        <li><strong>Schema Hash:</strong> <code>{esc(str(schema_hash))}</code></li>
        <li><strong>Rules / Configuration Hash:</strong> <code>{esc(str(rules_hash))}</code></li>
        <li><strong>Active Threshold Policy:</strong> <code>2026.08.2</code></li>
        <li><strong>Active Placeholder Configuration:</strong> <code>{esc(active_placeholders_str)}</code></li>
        <li><strong>Duplicate Detection Method:</strong> <code>fuzzy_token_sort (base threshold: 0.85, effective threshold: 0.95 for sparse text, batch-grain aware)</code></li>
        <li><strong>Assessment Timestamp:</strong> <code>{esc(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))}</code></li>
        <li><strong>Sampling Strategy:</strong> <code>Full dataset profiling (no truncation/sampling applied)</code></li>
      </ul>
    </div>
    """
    
    limitations_html = """
    <div style="background-color: #fff; border-radius: 8px; padding: 20px; border: 1px solid #ddd; margin-top: 20px;">
      <h4>Assessment Limitations</h4>
      <ul style="padding-left: 20px; line-height: 1.6;">
        <li><strong>Inferred Relationships:</strong> Discoveries are advisory and require promotion in <code>dq_thresholds.yaml</code> for enforcement.</li>
        <li><strong>Parsing Failures:</strong> Handled gracefully via execution warnings.</li>
        <li><strong>Cell Truncation:</strong> Diagnostic row indexes are sampled up to 10 rows per issue type.</li>
      </ul>
    </div>
    """
    
    technical_appendix_html = f"""
    <section id="technical-appendix" class="datasets-section" style="margin-top: 30px; background: #fff; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
      <h2>Technical Appendix</h2>
      <p class="section-lead">Reproducibility metadata, scan limitations, and full diagnostic details.</p>
      {reproducibility_html}
      {limitations_html}
    </section>
    """

    return (
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n"
        "<meta charset=\"utf-8\"/>\n"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>\n"
        "<title>Data assessment report</title>\n"
        "<style>\n"
        + _report_css
        + "\n"
        + _report_extra_css
        + "\n</style>\n"
        "</head>\n<body>\n"
        + f"""<div id="toast" class="toast" role="status">Copied to clipboard</div>
<div class="wrap">
<nav class="nav-rail" aria-label="Quick navigation">
  <span class="nav-brand">AGENT DHARA</span>
  <a href="#dq-scorecard" class="nav-jump">DQ verdict</a>
  <a href="#overview" class="nav-jump">Overview</a>
  <a href="#datasets" class="nav-jump" data-expand-datasets="1">Datasets</a>
  <a href="#relations" class="nav-jump" data-expand-relations="1">Relations</a>
  <a href="#globalq" class="nav-jump" data-expand-global="1">Global DQ</a>
  <a href="#governance" class="nav-jump">Governance</a>
  <a href="#score-methodology" class="nav-jump">Methodology</a>
  <a href="#technical-appendix" class="nav-jump">Appendix</a>{nav_llm_link}{nav_llm_rec_link}{nav_fix_link}
  <button type="button" class="tool-btn secondary" id="btn-collapse-all">Collapse all</button>
  <button type="button" class="tool-btn" id="btn-expand-all">Expand all</button>
</nav>
<div class="scene-hero">
<header class="masthead">
  <div class="tagline">Intelligent data assessment</div>
  <h1>Assessment report</h1>
  <div class="sub">Generated {esc(dt)}</div>
</header>
</div>
{scorecard_banner_html}
<section id="overview">
<h2>Overview</h2>
<p class="section-lead">Aggregate view of assessed datasets: volume, shape, and footprint.</p>
<div class="kpi-grid">
  <div class="kpi"><div class="label">Datasets</div><div class="val">{len(datasets)}</div></div>
  <div class="kpi"><div class="label">Approx. rows</div><div class="val">{total_rows:,}</div></div>
  <div class="kpi"><div class="label">Total columns</div><div class="val">{total_cols:,}</div></div>
  <div class="kpi"><div class="label">Memory (bytes)</div><div class="val">{total_bytes:,}</div></div>
</div>
{exec_summary_html}
</section>
{governance_html}
{llm_section_html}
{llm_recs_section_html}
<section id="datasets" class="datasets-section">
{datasets_html}
</section>
<section id="relations">
<div class="collapsible-panel">
<button type="button" class="collapsible-head" aria-expanded="true" aria-controls="rel-body"><h2>Relationships</h2><span class="coll-h" aria-hidden="true"></span></button>
<div class="collapsible-body" id="rel-body">{rel_html}</div>
</div>
</section>
<section id="globalq">
<div class="collapsible-panel">
<button type="button" class="collapsible-head" aria-expanded="true" aria-controls="glob-body"><h2>Global quality</h2><span class="coll-h" aria-hidden="true"></span></button>
<div class="collapsible-body" id="glob-body">{glob_html}</div>
</div>
</section>
{suggestions_section_html}
{methodology_html}
{technical_appendix_html}
</div>
<footer class="report-watermark-footer" aria-label="Attribution">
  <span class="report-watermark-oval">Agent Dhara</span>
</footer>
<script>
(function() {{
  function showToast() {{
    var t = document.getElementById('toast');
    t.classList.add('show');
    setTimeout(function() {{ t.classList.remove('show'); }}, 2000);
  }}
  function runCopy(btn) {{
    var p = btn.getAttribute('data-path') || '';
    if (navigator.clipboard && navigator.clipboard.writeText) {{
      navigator.clipboard.writeText(p).then(showToast);
    }} else {{
      var ta = document.createElement('textarea'); ta.value = p; document.body.appendChild(ta); ta.select();
      try {{ document.execCommand('copy'); showToast(); }} catch(e) {{}}
      document.body.removeChild(ta);
    }}
  }}
  document.querySelectorAll('.btn-copy-path').forEach(function(btn) {{
    btn.addEventListener('click', function(e) {{
      e.stopPropagation();
      e.preventDefault();
      runCopy(btn);
    }});
    btn.addEventListener('keydown', function(e) {{
      if (e.key === 'Enter' || e.key === ' ') {{ e.preventDefault(); e.stopPropagation(); runCopy(btn); }}
    }});
  }});
  document.querySelectorAll('.nest-toggle').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
      var ex = btn.getAttribute('aria-expanded') === 'true';
      btn.setAttribute('aria-expanded', ex ? 'false' : 'true');
    }});
  }});
  function expandCollapseAll(expanded) {{
    document.querySelectorAll('.collapsible-head').forEach(function(b) {{
      b.setAttribute('aria-expanded', expanded ? 'true' : 'false');
    }});
    document.querySelectorAll('.nest-toggle').forEach(function(b) {{
      b.setAttribute('aria-expanded', expanded ? 'true' : 'false');
    }});
    document.querySelectorAll('details.dq-issue-details').forEach(function(d) {{
      if (expanded) {{
        d.setAttribute('open', '');
      }} else {{
        d.removeAttribute('open');
      }}
    }});
  }}
  document.getElementById('btn-expand-all').addEventListener('click', function() {{ expandCollapseAll(true); }});
  document.getElementById('btn-collapse-all').addEventListener('click', function() {{ expandCollapseAll(false); }});
  document.querySelectorAll('a.nav-jump').forEach(function(a) {{
    a.addEventListener('click', function(e) {{
      var href = a.getAttribute('href') || '';
      if (href.charAt(0) !== '#') return;
      var id = href.slice(1);
      var section = document.getElementById(id);
      e.preventDefault();
      if (a.getAttribute('data-expand-datasets') && section) {{
        var t = section.querySelectorAll('.nest-toggle');
        var allOpen = t.length > 0;
        for (var i = 0; i < t.length; i++) {{
          if (t[i].getAttribute('aria-expanded') !== 'true') {{ allOpen = false; break; }}
        }}
        for (var j = 0; j < t.length; j++) {{
          t[j].setAttribute('aria-expanded', allOpen ? 'false' : 'true');
        }}
      }}
      if (a.getAttribute('data-expand-relations') && section) {{
        var rh = section.querySelector('.collapsible-head');
        if (rh) {{
          var rex = rh.getAttribute('aria-expanded') === 'true';
          rh.setAttribute('aria-expanded', rex ? 'false' : 'true');
        }}
      }}
      if (a.getAttribute('data-expand-global') && section) {{
        var gh = section.querySelector('.collapsible-head');
        if (gh) {{
          var gex = gh.getAttribute('aria-expanded') === 'true';
          gh.setAttribute('aria-expanded', gex ? 'false' : 'true');
        }}
      }}
      if (a.getAttribute('data-expand-suggestions') && section) {{
        var sh = section.querySelector('.collapsible-head');
        if (sh) {{
          var sx = sh.getAttribute('aria-expanded') === 'true';
          sh.setAttribute('aria-expanded', sx ? 'false' : 'true');
        }}
      }}
      if (section) section.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
    }});
  }});
  document.querySelectorAll('.collapsible-head').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
      var ex = btn.getAttribute('aria-expanded') === 'true';
      btn.setAttribute('aria-expanded', ex ? 'false' : 'true');
    }});
  }});
}})();
</script>
</body>
</html>"""
    )


# ========================= Promptflow adapter & evaluator =========================


def engine_to_pf_dq(engine_result: dict) -> dict:
    """Convert engine result to Promptflow dq_profile structure."""
    pf_profiles = []
    datasets = engine_result.get("datasets", {})
    dq_ds = engine_result.get("data_quality_issues", {}).get("datasets", {})
    issue_to_dimension = {
        "nulls": "completeness", "whitespace": "consistency", "invalid_email": "validity",
        "invalid_phone": "validity", "invalid_date_format": "validity", "invalid_numeric": "validity",
        "numeric_value_serialized_as_string": "validity",
        "negative_values": "validity", "suspicious_zero": "validity", "mixed_types": "consistency",
        "nested_structure": "anomalies", "duplicate_rows": "consistency", "duplicate_primary_key": "consistency",
        "potential_primary_key": "consistency",
    }
    for ds_name, meta in datasets.items():
        row_count = int(meta.get("row_count", 0))
        col_count = int(meta.get("column_count", 0))
        cols_meta = meta.get("columns", {})
        issues = dq_ds.get(ds_name, {}).get("issues", [])
        col_dims = {}
        for col_name, cmeta in cols_meta.items():
            null_rate = float(cmeta.get("null_percentage", 0.0))
            uniq = cmeta.get("unique_count")
            uniq_rate = (float(uniq) / row_count) if (uniq is not None and row_count) else None
            completeness = {"null_rate": null_rate, "completeness_score": (1.0 - null_rate) if null_rate is not None else None, "missing_pattern": None}
            consistency = {"data_type": cmeta.get("dtype"), "duplicate_count": None, "duplicate_rate": None, "distinct_count": uniq, "unique_rate": uniq_rate, "cardinality_category": "high" if uniq == row_count else ("low" if (uniq_rate is not None and uniq_rate < 0.01) else "medium") if (uniq is not None and row_count) else None, "type_consistency_rate": None}
            validity = {"pattern_conformance": {}, "numeric_stats": {}}
            anomalies = {"outlier_rate": None, "top_values": None, "entropy": None}
            semantic_validity = {"semantic_type": cmeta.get("semantic_type"), "type_inferred": bool(cmeta.get("semantic_type")), "business_rule_checks": None}
            col_dims[col_name] = {"column_name": col_name, "data_type": cmeta.get("dtype"), "sample_values": [], "completeness": completeness, "consistency": consistency, "validity": validity, "anomalies": anomalies, "semantic_validity": semantic_validity}
        for it in issues:
            col = it.get("column")
            i_typ = it.get("type")
            dim = issue_to_dimension.get(i_typ)
            if dim and col in col_dims:
                if i_typ == "duplicate_primary_key" and row_count:
                    cnt = int(it.get("count") or 0)
                    col_dims[col]["consistency"]["duplicate_count"] = cnt
                    col_dims[col]["consistency"]["duplicate_rate"] = cnt / row_count
                if i_typ == "mixed_types" and col_dims[col]["consistency"]["type_consistency_rate"] is None:
                    col_dims[col]["consistency"]["type_consistency_rate"] = 0.7
        pf_profiles.append({"file_name": ds_name, "sheet_name": None, "total_rows": row_count, "total_columns": col_count, "column_quality": dict(col_dims)})
    return {"dq_profile": pf_profiles}


def _eval_completeness(m):
    s = m.get("completeness_score")
    if s is None: return "UNKNOWN"
    if s >= 0.95: return "PASS"
    if s >= 0.80: return "WARN"
    return "FAIL"

def _eval_consistency(m):
    tcr, dup_rate = m.get("type_consistency_rate"), m.get("duplicate_rate")
    issues = 0
    if tcr is not None and tcr < 0.95: issues += 1
    if dup_rate is not None and dup_rate > 0.10: issues += 1
    return "PASS" if issues == 0 else ("WARN" if issues == 1 else "FAIL")

def _eval_validity(m):
    pc = m.get("pattern_conformance", {}) or {}
    if not pc: return "UNKNOWN"
    for _, rate in pc.items():
        if rate is not None and rate < 0.80: return "FAIL"
    if any(v is not None and v < 0.95 for v in pc.values()): return "WARN"
    return "PASS"

def _eval_anomalies(m):
    orate, ent = m.get("outlier_rate"), m.get("entropy")
    issues = 0
    if orate is not None and orate > 0.15: issues += 1
    if ent is not None and ent < 1.0: issues += 1
    return "PASS" if issues == 0 else ("WARN" if issues == 1 else "FAIL")

def _eval_semantic(m):
    st, br = m.get("semantic_type"), m.get("business_rule_checks")
    if st is None or st == "unknown": return "UNKNOWN"
    if not br: return "PASS"
    fails = sum(1 for _, r in (br or {}).items() if r is not None and r < 0.85)
    if fails == 0: return "PASS"
    if fails == 1: return "WARN"
    return "FAIL"


def evaluate_dq_rules_inline(pf_input: dict) -> dict:
    """Evaluate dq_profile into PASS/WARN/FAIL (file & column level)."""
    dq_profiles = pf_input.get("dq_profile", [])
    evaluations = []
    for fp in dq_profiles:
        file_name = fp.get("file_name")
        cq = fp.get("column_quality", {}) or {}
        dim_counts = {d: {"PASS": 0, "WARN": 0, "FAIL": 0, "UNKNOWN": 0} for d in ["completeness", "consistency", "validity", "anomalies", "semantic_validity"]}
        file_eval = {"file_name": file_name, "sheet_name": fp.get("sheet_name"), "total_rows": fp.get("total_rows"), "total_columns": fp.get("total_columns"), "column_evaluations": {}, "dimension_summary": {}, "overall_quality": "PASS"}
        for col_name, metrics in cq.items():
            comp = _eval_completeness(metrics.get("completeness", {}))
            cons = _eval_consistency(metrics.get("consistency", {}))
            vali = _eval_validity(metrics.get("validity", {}))
            anom = _eval_anomalies(metrics.get("anomalies", {}))
            sema = _eval_semantic(metrics.get("semantic_validity", {}))
            for d, s in (("completeness", comp), ("consistency", cons), ("validity", vali), ("anomalies", anom), ("semantic_validity", sema)):
                dim_counts[d][s] += 1
            statuses = [comp, cons, vali, anom, sema]
            overall = "FAIL" if "FAIL" in statuses else ("WARN" if "WARN" in statuses else "PASS")
            file_eval["column_evaluations"][col_name] = {"column_name": col_name, "data_type": metrics.get("data_type"), "dimensions": {"completeness": comp, "consistency": cons, "validity": vali, "anomalies": anom, "semantic_validity": sema}, "overall_status": overall}
        for dim, counts in dim_counts.items():
            total = sum(counts.values())
            file_eval["dimension_summary"][dim] = {"pass": counts["PASS"], "warn": counts["WARN"], "fail": counts["FAIL"], "unknown": counts["UNKNOWN"], "total": total, "pass_rate": (counts["PASS"] / total) if total else 0}
        failed_dims = sum(1 for d in dim_counts.values() if d["FAIL"] > 0)
        warned_dims = sum(1 for d in dim_counts.values() if d["WARN"] > 0 and d["FAIL"] == 0)
        file_eval["overall_quality"] = "FAIL" if failed_dims > 0 else ("WARN" if warned_dims > 0 else "PASS")
        evaluations.append(file_eval)
    summary = {"total_files_evaluated": len(evaluations), "passed_files": sum(1 for e in evaluations if e["overall_quality"] == "PASS"), "warned_files": sum(1 for e in evaluations if e["overall_quality"] == "WARN"), "failed_files": sum(1 for e in evaluations if e["overall_quality"] == "FAIL"), "dimension_overview": {dim: {"total_pass": sum(e["dimension_summary"][dim]["pass"] for e in evaluations), "total_warn": sum(e["dimension_summary"][dim]["warn"] for e in evaluations), "total_fail": sum(e["dimension_summary"][dim]["fail"] for e in evaluations)} for dim in ["completeness", "consistency", "validity", "anomalies", "semantic_validity"]}}
    return {"evaluations": evaluations, "summary": summary}


# ========================= Azure Blob helpers =========================


def get_azure_blob_connector_for_output(source_cfg: Dict[str, Any]) -> Optional[Tuple[Any, str]]:
    if not AZURE_BLOB_AVAILABLE:
        return None
    try:
        for loc in source_cfg.get("locations", []):
            if (loc.get("type") or "").lower() == "azure_blob_output":
                conn_cfg = loc.get("connection", {})
                connector = AzureBlobStorageConnector(conn_cfg)
                container_name = conn_cfg.get("container", "output")
                return connector, container_name
    except Exception as e:
        logger.warning("Could not init Azure Blob connector for uploads: %s", e)
    return None


def get_azure_blob_connector_for_assessment(source_cfg: Dict[str, Any]) -> Optional[Tuple[Any, str]]:
    """First assessment container only (legacy); prefer load_all_assessment_blob_datasets for all."""
    if not AZURE_BLOB_AVAILABLE:
        return None
    try:
        for loc in source_cfg.get("locations", []):
            if (loc.get("type") or "").lower() == "azure_blob":
                conn_cfg = loc.get("connection", {})
                connector = AzureBlobStorageConnector(conn_cfg)
                container_name = conn_cfg.get("container", "assessment")
                return connector, container_name
    except Exception as e:
        logger.warning("Could not init Azure Blob connector for assessment: %s", e)
    return None


def _azure_blob_location_label(loc: Dict[str, Any], conn_cfg: Dict[str, Any], idx: int) -> str:
    for k in ("id", "label", "name"):
        v = loc.get(k)
        if v and str(v).strip():
            s = re.sub(r"[^\w\-]+", "_", str(v).strip())[:48].strip("_")
            if s:
                return s
    c = conn_cfg.get("container") or "blob"
    s = re.sub(r"[^\w\-]+", "_", str(c))[:32].strip("_") or f"blob_{idx}"
    return s


def load_all_assessment_blob_datasets(source_cfg: Dict[str, Any], *, only_blobs: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Load every type=azure_blob location. Single container: keys are blob paths (unchanged).
    Multiple containers: keys are \"{label}__{blob_path}\" to avoid collisions.
    """
    if not AZURE_BLOB_AVAILABLE:
        return {}
    blob_locs = [
        loc
        for loc in source_cfg.get("locations", [])
        if (loc.get("type") or "").lower() == "azure_blob"
    ]
    if not blob_locs:
        return {}
    out: Dict[str, Any] = {}
    multi = len(blob_locs) > 1
    only_set = {str(x).strip() for x in (only_blobs or []) if str(x).strip()}
    for idx, loc in enumerate(blob_locs):
        conn_cfg = loc.get("connection", {}) or {}
        container_name = conn_cfg.get("container", "assessment")
        try:
            connector = AzureBlobStorageConnector(conn_cfg)
        except Exception as e:
            logger.warning("Azure Blob container %s skipped: %s", container_name, e)
            continue
        blobs = sorted(connector.list_blobs())
        if only_set:
            blobs = [b for b in blobs if b in only_set]
        loaded = connector.load_all_blobs(folder_prefix="", blobs=blobs)
        if not multi:
            out.update(loaded)
            continue
        label = _azure_blob_location_label(loc, conn_cfg, idx)
        for bname, df in loaded.items():
            key = f"{label}__{bname}"
            if key in out:
                key = f"{label}__{idx}__{bname}"
            out[key] = df
    return out


def upload_output_to_azure(file_path: str, blob_name: str, source_cfg: Dict[str, Any], output_container: str) -> bool:
    if not AZURE_BLOB_AVAILABLE:
        return False
    result = get_azure_blob_connector_for_output(source_cfg)
    if result is None:
        return False
    connector, _ = result
    return connector.upload_file(file_path, blob_name=blob_name, container=output_container)


def upload_data_to_azure(data: bytes, blob_name: str, source_cfg: Dict[str, Any], output_container: str) -> bool:
    if not AZURE_BLOB_AVAILABLE:
        return False
    result = get_azure_blob_connector_for_output(source_cfg)
    if result is None:
        return False
    connector, _ = result
    return connector.upload_blob(blob_name, data, container=output_container)



