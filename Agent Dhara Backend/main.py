#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py — SQL Runner + Intelligent Data Assessment (DB + Filesystem + Azure Blob) + Promptflow

- Loads all SQL DBs + filesystem paths + all Azure Blob assessment containers; profiles; DQ; relationships.
- Exports: JSON, Markdown, HTML; optional PF input/eval; optional upload to Azure Blob.
- Improvements: structured logging, config-driven DQ thresholds, sorted blob order (idempotent), --skip-azure.
"""







import os
# Force Azure CLI path to PATH environment variable if not already present
for _az_dir in [r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin", r"C:\Program Files (x86)\Microsoft SDKs\Azure\CLI2\wbin"]:
    if os.path.isdir(_az_dir) and _az_dir not in os.environ.get("PATH", ""):
        os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + _az_dir

import sys
import re
import time
import json
import html as html_module
import hashlib
import argparse
import logging
from collections import defaultdict
from typing import Dict, Any, Tuple, List, Optional
from pathlib import Path

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

logger = logging.getLogger(__name__)

# Load local .env automatically (developer convenience).
# This avoids Windows `setx` quoting/truncation problems and keeps secrets out of code/config.
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None  # type: ignore

if load_dotenv is not None:
    try:
        load_dotenv(os.path.join(PROJECT_DIR, ".env"), override=False)
    except Exception:
        pass


def _doctor_env_hint() -> None:
    """
    Print a short preflight summary of env/config that commonly breaks Azure access.
    This intentionally avoids printing secret values.
    """
    keys = [
        "AZURE_STORAGE_CONNECTION_STRING",
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_STORAGE_ACCOUNT_KEY",
        "AZURE_ASSESSMENT_CONTAINER",
        "AZURE_OUTPUT_CONTAINER",
        "AZURE_SQL_SERVER",
        "AZURE_SQL_DATABASE",
        "AZURE_SQL_USERNAME",
        "AZURE_SQL_PASSWORD",
    ]
    present = {k: (True if os.environ.get(k) else False) for k in keys}
    missing = [k for k, ok in present.items() if not ok]
    if missing:
        logger.warning(
            "Azure env preflight: missing %d var(s): %s. "
            "Tip: create '%s' and set vars there (or set in current shell).",
            len(missing),
            ", ".join(missing),
            os.path.join(PROJECT_DIR, ".env"),
        )
    else:
        logger.info("Azure env preflight: all required vars appear set.")

try:
    from agent.intelligent_data_assessment import load_and_profile, load_dq_thresholds
except ImportError:
    try:
        ida_path = Path(PROJECT_DIR) / "agent" / "intelligent_data_assessment.py"
        if not ida_path.is_file():
            ida_path = Path(PROJECT_DIR) / "intelligent_data_assessment.py"
        import importlib.util
        spec = importlib.util.spec_from_file_location("ida_dyn", ida_path)
        ida = importlib.util.module_from_spec(spec)  # type: ignore
        if spec and spec.loader:
            spec.loader.exec_module(ida)  # type: ignore
        load_and_profile = ida.load_and_profile  # type: ignore
        load_dq_thresholds = getattr(ida, "load_dq_thresholds", lambda p: {})  # type: ignore
    except Exception as e:
        logger.critical("Could not import intelligent_data_assessment: %s", e)
        sys.exit(1)

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

try:
    from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector
except Exception as e:
    logger.info("SQL connector not available (pythonnet/.NET required for database): %s", e)
    AzureSQLPythonNetConnector = None  # type: ignore

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


def load_config(path: str) -> Dict[str, Any]:
    """Load YAML or JSON config."""
    if not os.path.isfile(path):
        logger.critical("Config file not found: %s", path)
        sys.exit(2)

    def _expand_env(obj: Any) -> Any:
        """
        Recursively expand ${VAR} (and ${VAR:default}) in config values.
        Leaves the placeholder unchanged if VAR is unset and no default is provided.
        """
        if isinstance(obj, dict):
            return {k: _expand_env(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_expand_env(v) for v in obj]
        if not isinstance(obj, str):
            return obj
        pattern = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}")

        def _sub(m: "re.Match") -> str:
            var = m.group(1)
            default = m.group(2)
            val = os.environ.get(var)
            if val:
                return val
            if default is not None:
                return default
            return m.group(0)

        return pattern.sub(_sub, obj)

    ext = os.path.splitext(path)[1].lower()
    with open(path, "r", encoding="utf-8") as f:
        if ext == ".json":
            return _expand_env(json.load(f))
        if ext in (".yaml", ".yml"):
            if yaml is None:
                logger.critical("PyYAML required for YAML configs. pip install pyyaml")
                sys.exit(3)
            return _expand_env(yaml.safe_load(f) or {})
        logger.critical("Unsupported config format: %s", ext)
        sys.exit(4)


def get_db_connection_cfg(cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the first database connection block from locations, or None."""
    for loc in cfg.get("locations", []):
        if (loc.get("type") or "").lower() == "database":
            return loc.get("connection", {}) or {}
    return None


def build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="SQL Runner + Intelligent Data Assessment",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--sources", default="config/sources.yaml", help="Path to YAML or JSON config")
    p.add_argument("--rows", type=int, default=5, help="Preview N rows per SQL table")
    p.add_argument("--list-only", action="store_true", help="Only list SQL tables")
    p.add_argument("--schema-only", action="store_true", help="Only show SQL schema")
    p.add_argument("--with-schema", action="store_true", help="List SQL tables AND show schema")
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    p.add_argument("--stream-file", default=None, help="Path to JSON array of records to validate")
    p.add_argument("--stream-name", default="stream", help="Logical name for stream dataset")
    p.add_argument(
        "--blob-names",
        default="",
        help=(
            "Comma-separated blob filenames to assess (Azure Blob only). "
            "If set, only these blobs are loaded from the azure_blob container."
        ),
    )
    p.add_argument("--export-json", default=None, help="Write full JSON assessment (whole report)")
    p.add_argument("--export-report", default=None, help="Write Markdown report (whole report)")
    p.add_argument("--export-html", default=None, help="Write HTML report (whole report)")
    p.add_argument(
        "--reports-dir",
        default=None,
        help="Directory for overall report.* outputs (report.json/.md/.html).",
    )
    p.add_argument("--export-pf-input", default=None, help="Write Promptflow dq_profile JSON")
    p.add_argument("--export-pf-eval", default=None, help="Write PF evaluation PASS/WARN/FAIL")
    p.add_argument("--export-to-azure", action="store_true", help="Upload outputs to Azure Blob")
    p.add_argument("--azure-only", action="store_true", help="Save only to Azure (no local files)")
    p.add_argument("--azure-output-container", default="output", help="Output container name")
    p.add_argument("--skip-azure", action="store_true", help="Do not access Azure Blob (offline)")
    p.add_argument("--dq-thresholds", default=None, help="Path to dq_thresholds.yaml (default: config/dq_thresholds.yaml)")
    p.add_argument(
        "--export-manifest",
        metavar="PATH",
        default=None,
        help="Write cleaning_manifest.json to PATH "
        "(default with --reports-dir: <reports-dir>/cleaning_manifest.json unless overridden here).",
    )
    p.add_argument(
        "--llm-insights",
        action="store_true",
        help="After assessment, call Azure OpenAI for executive summary & risks (needs AZURE_OPENAI_* env).",
    )
    p.add_argument(
        "--evaluate",
        default="auto",
        choices=["auto", "interactive", "sql", "blob", "local", "stream", "all"],
        help=(
            "Which data to evaluate: sql|blob|local|stream|all, interactive menu, or auto "
            "(menu if stdin is a TTY, else all). Stream uses --stream-file or prompts for path."
        ),
    )
    return p.parse_args()


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
    return f"- ({i['severity']}) {i['type']}{col}{cnt}: {i['message']}{row_part}{rec_part}"


def _dtype_with_inference(cmeta: Dict[str, Any]) -> str:
    d = cmeta.get("dtype")
    inf = cmeta.get("dtype_inference")
    return f"{d} ({inf})" if d == "object" and inf else str(d)


def build_dq_scorecard(result: Dict[str, Any]) -> Dict[str, Any]:
    from agent.service_layer import build_dq_scorecard as _build
    return _build(result)


def build_cleaning_manifest(result: Dict[str, Any], suggestions: Dict[str, Any], scorecard: Dict[str, Any]) -> Dict[str, Any]:
    from agent.service_layer import build_cleaning_manifest as _build
    return _build(result, suggestions, scorecard)


def build_markdown_report(result: Dict[str, Any]) -> str:
    from agent.service_layer import build_markdown_report as _build
    return _build(result)


def build_html_report(result: Dict[str, Any]) -> str:
    from agent.service_layer import build_html_report as _build
    return _build(result)

# ========================= Promptflow adapter & evaluator =========================

def engine_to_pf_dq(engine_result: dict) -> dict:
    """Convert engine result to Promptflow dq_profile structure."""
    pf_profiles = []

    datasets = engine_result.get("datasets", {})
    dq_ds = engine_result.get("data_quality_issues", {}).get("datasets", {})
    issue_to_dimension = {
        "nulls": "completeness", "whitespace": "consistency", "invalid_email": "validity",
        "invalid_phone": "validity", "invalid_date_format": "validity", "invalid_numeric": "validity",
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


# ========================= MAIN =========================

def main():
    args = build_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _doctor_env_hint()

    # Legacy: --stream-file always runs stream batch and exits (no scope menu).
    if args.stream_file:
        try:
            from agent.mcp_interface import process_stream_chunk
            with open(args.stream_file, "r", encoding="utf-8") as f:
                records = json.load(f)
            if not isinstance(records, list):
                logger.error("Stream file must contain a JSON array of objects")
                sys.exit(5)
            report = process_stream_chunk(records, name=args.stream_name)
            print("=== STREAM BATCH REPORT ===")
            print(json.dumps(report, indent=2, default=to_json_safe))
            print("=== DONE ===")
            return
        except Exception as e:
            logger.exception("Could not process stream file: %s", e)
            sys.exit(6)

    from agent.evaluation_scope import (
        MODE_ALL,
        MODE_BLOB,
        MODE_INTERACTIVE,
        MODE_LOCAL,
        MODE_SQL,
        MODE_STREAM,
        default_evaluate_mode as _default_eval_mode,
        interactive_select_mode,
        location_types_for_mode,
        prompt_stream_file_path,
    )

    eval_mode = (args.evaluate or "auto").lower()
    if eval_mode == "auto":
        eval_mode = _default_eval_mode()
    if eval_mode == MODE_INTERACTIVE:
        eval_mode = interactive_select_mode()

    if eval_mode == MODE_STREAM:
        stream_path = prompt_stream_file_path()
        if not stream_path:
            logger.error("Stream mode requires a JSON array file path.")
            sys.exit(6)
        try:
            from agent.mcp_interface import process_stream_chunk
            with open(stream_path, "r", encoding="utf-8") as f:
                records = json.load(f)
            if not isinstance(records, list):
                logger.error("Stream file must contain a JSON array of objects")
                sys.exit(5)
            report = process_stream_chunk(records, name=args.stream_name)
            print("=== STREAM BATCH REPORT ===")
            print(json.dumps(report, indent=2, default=to_json_safe))
            print("=== DONE ===")
            return
        except Exception as e:
            logger.exception("Could not process stream file: %s", e)
            sys.exit(6)

    cfg_root = load_config(args.sources)
    source_cfg = cfg_root.get("source", cfg_root)
    has_azure_blob = any((loc.get("type") or "").lower() == "azure_blob" for loc in source_cfg.get("locations", []))
    has_filesystem = any((loc.get("type") or "").lower() == "filesystem" for loc in source_cfg.get("locations", []))
    db_locs = [
        loc for loc in source_cfg.get("locations", [])
        if (loc.get("type") or "").lower() == "database"
    ]
    has_sql = bool(db_locs) and AzureSQLPythonNetConnector is not None

    if eval_mode == MODE_SQL:
        if not has_sql:
            logger.critical("SQL-only evaluation requires a database location and SQL connector (pythonnet).")
            sys.exit(6)
    elif eval_mode == MODE_BLOB:
        if not has_azure_blob:
            logger.critical("Blob-only evaluation requires at least one azure_blob location in sources.yaml.")
            sys.exit(6)
        if args.skip_azure:
            logger.critical("Blob-only evaluation cannot use --skip-azure.")
            sys.exit(6)
    elif eval_mode == MODE_LOCAL:
        if not has_filesystem:
            logger.critical("Local-only evaluation requires a filesystem location in sources.yaml.")
            sys.exit(6)
    elif eval_mode == MODE_ALL:
        if not has_azure_blob and not has_filesystem and not has_sql:
            logger.critical(
                "Need at least one data source: azure_blob, filesystem, or database (with SQL connector available)"
            )
            sys.exit(6)
    else:
        logger.critical("Unknown evaluation mode: %s", eval_mode)
        sys.exit(6)

    multi_sql = len(db_locs) > 1

    if has_sql and eval_mode in (MODE_SQL, MODE_ALL):
        if args.list_only:
            for db_idx, db_loc in enumerate(db_locs):
                conn_cfg = db_loc.get("connection", {}) or {}
                lbl = (db_loc.get("id") or db_loc.get("label") or "").strip() or conn_cfg.get(
                    "database", f"database_{db_idx}"
                )
                print(f"\n=== Azure SQL [{lbl}] ===")
                print("Server   :", conn_cfg.get("server"))
                print("Database :", conn_cfg.get("database"))
                try:
                    connector = AzureSQLPythonNetConnector(conn_cfg)
                    tables = connector.discover_tables()
                except Exception as e:
                    logger.info("Table discovery failed [%s]: %s", lbl, str(e)[:200])
                    tables = []
                if not tables:
                    logger.info("No tables found for [%s]", lbl)
                else:
                    for t in tables:
                        print(" -", t if not multi_sql else f"{lbl} :: {t}")
            print("\n=== DONE (listed only) ===")
            return

        for db_idx, db_loc in enumerate(db_locs):
            conn_cfg = db_loc.get("connection", {}) or {}
            lbl = (db_loc.get("id") or db_loc.get("label") or "").strip() or conn_cfg.get(
                "database", f"database_{db_idx}"
            )
            logger.debug("DB [%s] connection keys: %s", lbl, list(conn_cfg.keys()))
            print(f"\n=== Connecting to Azure SQL [{lbl}] ===")
            print("Server   :", conn_cfg.get("server"))
            print("Database :", conn_cfg.get("database"))
            try:
                connector = AzureSQLPythonNetConnector(conn_cfg)
                tables = connector.discover_tables()
            except Exception as e:
                logger.info("Table discovery failed [%s]: %s", lbl, str(e)[:200])
                tables = []
                connector = None
            if not tables:
                logger.info("No tables found for [%s]", lbl)
            else:
                for t in tables:
                    print(" -", t if not multi_sql else f"{lbl} :: {t}")

            if args.schema_only or args.with_schema:
                print(f"\n=== Table Schemas [{lbl}] ===")
                if connector and tables:
                    for table in tables:
                        disp = table if not multi_sql else f"{lbl} / {table}"
                        print(f"\n--- {disp} ---")
                        ok = print_schema_info_schema(connector, table)
                        if not ok:
                            print_schema_top0(connector, table)

            if not args.schema_only and connector and tables:
                print(f"\n=== Table Previews [{lbl}] ===")
                for table in tables:
                    disp = table if not multi_sql else f"{lbl} / {table}"
                    print(f"\n--- {disp} ---")
                    try:
                        n = args.rows if args.rows > 0 else 5
                        df = connector.preview_table(table, n)
                        if df is None or df.empty:
                            print("[INFO] (empty table)")
                        else:
                            print(df.head(n))
                    except Exception as e:
                        logger.error("Preview failed for %s: %s", table, e)

        if args.schema_only:
            print("\n=== DONE ===")
            return
        if args.with_schema and args.rows <= 0:
            print("\n=== DONE ===")
            return
    else:
        if eval_mode in (MODE_SQL, MODE_ALL):
            print("\n[INFO] Skipping SQL Runner (no database configured)")
        else:
            print("\n[INFO] Skipping SQL (evaluation scope excludes SQL)")

    print("\n=== Running Intelligent Data Assessment ===")
    print(f"=== Scope: {eval_mode} ===")
    _run_t0 = time.perf_counter()
    from datetime import datetime as _dt_utc, timezone as _tz

    _run_started = _dt_utc.now(_tz.utc).isoformat()
    blob_data = {}
    load_blob = eval_mode in (MODE_BLOB, MODE_ALL) and not args.skip_azure
    if load_blob:
        blob_locs = [
            loc
            for loc in source_cfg.get("locations", [])
            if (loc.get("type") or "").lower() == "azure_blob"
        ]
        if blob_locs:
            only_blobs: List[str] = []
            if (args.blob_names or "").strip():
                only_blobs = [x.strip() for x in (args.blob_names or "").split(",") if x.strip()]
            blob_data = load_all_assessment_blob_datasets(source_cfg, only_blobs=only_blobs or None)
            logger.info(
                "Loaded %d datasets from %d Azure Blob container(s)",
                len(blob_data),
                len(blob_locs),
            )
            for blob_name, df in sorted(blob_data.items()):
                print(f"  - {blob_name}: {len(df)} rows x {len(df.columns)} columns")
        else:
            logger.info("No azure_blob locations in config")
    elif args.skip_azure:
        logger.info("Skipping Azure Blob (--skip-azure)")
    else:
        logger.info("Skipping Azure Blob (evaluation scope excludes blob)")

    dq_thresholds_path = args.dq_thresholds or os.environ.get("DQ_THRESHOLDS_PATH")
    if not dq_thresholds_path and os.path.isfile(os.path.join("config", "dq_thresholds.yaml")):
        dq_thresholds_path = os.path.join("config", "dq_thresholds.yaml")
    location_filter = location_types_for_mode(eval_mode)
    result = load_and_profile(
        source_cfg,
        additional_data=blob_data,
        dq_thresholds_path=dq_thresholds_path,
        return_datasets=False,
        location_types=location_filter,
    )
    _attach_run_metadata_and_suggestions(result, _run_t0, _run_started, args.sources or "")
    result.setdefault("run_metadata", {})["evaluation_scope"] = eval_mode
    logger.info(
        "Assessment finished in %.2fs | DQ rollup high/medium/low: %s",
        result.get("run_metadata", {}).get("duration_seconds", 0),
        result.get("run_metadata", {}).get("dq_issue_totals"),
    )

    if getattr(args, "llm_insights", False):
        try:
            from agent.llm_assessment_enhancer import generate_llm_insights

            result["llm_insights"] = generate_llm_insights(result)
            li = result["llm_insights"]
            if li.get("success"):
                logger.info("LLM insights generated (Azure OpenAI)")
            else:
                logger.warning("LLM insights not generated: %s", li.get("error"))
        except Exception as e:
            logger.exception("LLM insights failed: %s", e)
            result["llm_insights"] = {"success": False, "error": str(e), "parsed": None}

    def _ensure_parent(path: Optional[str]):
        if not path:
            return
        parent = os.path.dirname(path)
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)

    def _export(path_opt: Optional[str], content_bytes: bytes, content_str: Optional[str], blob_basename: str, desc: str):
        if not path_opt:
            return
        try:
            if args.azure_only and args.export_to_azure:
                if upload_data_to_azure(content_bytes, blob_basename, source_cfg, args.azure_output_container):
                    logger.info("Uploaded %s to Azure container '%s'", desc, args.azure_output_container)
                else:
                    logger.error("Failed to upload %s to Azure", desc)
            else:
                _ensure_parent(path_opt)
                if content_str is not None:
                    with open(path_opt, "w", encoding="utf-8") as f:
                        f.write(content_str)
                else:
                    with open(path_opt, "wb") as f:
                        f.write(content_bytes)
                logger.info("Wrote %s to: %s", desc, path_opt)
                if args.export_to_azure:
                    if upload_output_to_azure(path_opt, blob_basename, source_cfg, args.azure_output_container):
                        logger.info("Uploaded %s to Azure container '%s'", desc, args.azure_output_container)
        except Exception as e:
            logger.exception("Could not write/upload %s: %s", desc, e)

    if args.export_json:
        json_bytes = json.dumps(result, ensure_ascii=False, indent=2, default=to_json_safe).encode("utf-8")
        _export(args.export_json, json_bytes, None, os.path.basename(args.export_json), "JSON report")

    if args.export_report:
        md = build_markdown_report(result)
        md_bytes = md.encode("utf-8")
        _export(args.export_report, md_bytes, md, os.path.basename(args.export_report), "Markdown report")

    if args.export_html:
        html = build_html_report(result)
        html_bytes = html.encode("utf-8")
        _export(args.export_html, html_bytes, html, os.path.basename(args.export_html), "HTML report")

    # Consolidated reports: report.* at root + by_path/<folder>/report.* per source location
    if args.reports_dir:
        reports_dir = args.reports_dir
        os.makedirs(reports_dir, exist_ok=True)
        datasets = result.get("datasets", {})
        json_bytes = json.dumps(result, ensure_ascii=False, indent=2, default=to_json_safe).encode("utf-8")
        with open(os.path.join(reports_dir, "report.json"), "wb") as f:
            f.write(json_bytes)
        logger.info("Wrote %s", os.path.join(reports_dir, "report.json"))
        md_full = build_markdown_report(result)
        with open(os.path.join(reports_dir, "report.md"), "w", encoding="utf-8") as f:
            f.write(md_full)
        logger.info("Wrote %s", os.path.join(reports_dir, "report.md"))
        html_full = build_html_report(result)
        with open(os.path.join(reports_dir, "report.html"), "w", encoding="utf-8") as f:
            f.write(html_full)
        logger.info("Wrote %s", os.path.join(reports_dir, "report.html"))
        if args.export_manifest:
            manifest_path = args.export_manifest
        else:
            manifest_path = os.path.join(reports_dir, "cleaning_manifest.json")
        try:
            sc_m = build_dq_scorecard(result)
            mf = build_cleaning_manifest(result, result.get("transformation_suggestions") or {}, sc_m)
            _ensure_parent(manifest_path)
            with open(manifest_path, "w", encoding="utf-8") as mf_h:
                json.dump(mf, mf_h, indent=2, default=to_json_safe)
            logger.info("Cleaning manifest written: %s", manifest_path)
            print(f"✅ Cleaning manifest written: {manifest_path}")
        except Exception as e:
            logger.warning("Cleaning manifest skipped: %s", e)

    elif getattr(args, "export_manifest", None):
        try:
            sc_m = build_dq_scorecard(result)
            mf = build_cleaning_manifest(result, result.get("transformation_suggestions") or {}, sc_m)
            manifest_path = args.export_manifest
            _ensure_parent(manifest_path)
            with open(manifest_path, "w", encoding="utf-8") as mf_h:
                json.dump(mf, mf_h, indent=2, default=to_json_safe)
            logger.info("Cleaning manifest written: %s", manifest_path)
            print(f"✅ Cleaning manifest written: {manifest_path}")
        except Exception as e:
            logger.warning("Cleaning manifest failed: %s", e)

    pf_input = None
    try:
        pf_input = engine_to_pf_dq(result)
    except Exception as e:
        logger.exception("Could not build Promptflow input: %s", e)

    if args.export_pf_input and pf_input:
        pf_bytes = json.dumps(pf_input, ensure_ascii=False, indent=2, default=to_json_safe).encode("utf-8")
        _export(args.export_pf_input, pf_bytes, None, os.path.basename(args.export_pf_input), "PF input")

    if args.export_pf_eval and pf_input:
        try:
            pf_eval = evaluate_dq_rules_inline(pf_input)
            pf_eval_bytes = json.dumps(pf_eval, ensure_ascii=False, indent=2, default=to_json_safe).encode("utf-8")
            _export(args.export_pf_eval, pf_eval_bytes, None, os.path.basename(args.export_pf_eval), "PF evaluation")
        except Exception as e:
            logger.exception("Inline evaluation failed: %s", e)

    print("\n=== FULL RESULT (JSON) ===")
    print(json.dumps(result, indent=2, default=to_json_safe))
    _rm = result.get("run_metadata") or {}
    _tsn = (result.get("transformation_suggestions") or {}).get("summary") or {}
    print(
        f"\n=== DONE ===  ({_rm.get('duration_seconds')}s | "
        f"DQ high/med/low: {_rm.get('dq_issue_totals')} | "
        f"suggested fixes: {_tsn.get('total_suggestions', 0)})"
    )


if __name__ == "__main__":
    main()
