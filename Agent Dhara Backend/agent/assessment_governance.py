from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from agent.cross_field_rules import evaluate_cross_field_rules, load_cross_field_rules
from agent.drift_analyzer import aggregate_drift
from agent.drift_detector import compare_snapshots
from agent.gx_issue_mapper import map_gx_to_unified_issues
from agent.metadata_registry import (
    load_metadata_manifest,
    manifest_hash,
    resolve_dataset_manifest,
    validate_manifest_against_schema,
)
from agent.profile_snapshot_store import build_profile_fingerprint, load_latest_snapshot, save_snapshot
from agent.raw_payload_registry import empty_registry
from agent.reconciliation_analyzer import analyze_reconciliation_bundle
from agent.reconciliation_tracker import build_reconciliation_from_profile, merge_reconciliations
from agent.semantic_context_builder import build_all_semantic_contexts

logger = logging.getLogger(__name__)


def _collect_semantic_ambiguity(assessment: Dict[str, Any]) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for ds_name, ds_meta in (assessment.get("datasets") or {}).items():
        if not isinstance(ds_meta, dict):
            continue
        for cname, col in (ds_meta.get("columns") or {}).items():
            if not isinstance(col, dict):
                continue
            tc = col.get("type_confidence")
            if isinstance(tc, (int, float)) and float(tc) < 0.72:
                rows.append({"dataset": str(ds_name), "column": str(cname), "type_confidence": float(tc)})
    return {"columns": rows[:50]}


def _attach_join_hints(assessment: Dict[str, Any]) -> None:
    rels = assessment.get("relationships") or []
    for ds_name, ds_meta in (assessment.get("datasets") or {}).items():
        if not isinstance(ds_meta, dict):
            continue
        hints: List[Dict[str, Any]] = []
        for r in rels:
            if not isinstance(r, dict):
                continue
            if str(r.get("dataset_a")) == str(ds_name):
                hints.append(
                    {
                        "with_dataset": r.get("dataset_b"),
                        "join_columns": [(r.get("column_a"), r.get("column_b"))],
                    }
                )
            elif str(r.get("dataset_b")) == str(ds_name):
                hints.append(
                    {
                        "with_dataset": r.get("dataset_a"),
                        "join_columns": [(r.get("column_b"), r.get("column_a"))],
                    }
                )
        if hints:
            ds_meta["join_hints"] = hints[:30]


def _schema_hash(assessment: Dict[str, Any]) -> str:
    parts: List[str] = []
    for ds_name in sorted((assessment.get("datasets") or {}).keys()):
        parts.append(f"ds:{ds_name}")
        cols = (assessment.get("datasets") or {}).get(ds_name, {}).get("columns") or {}
        for col_name in sorted(cols.keys(), key=lambda x: str(x).lower()):
            c = cols.get(col_name) or {}
            parts.append(f"col:{col_name}|{c.get('dtype')}")
    return hashlib.sha256("\n".join(parts).encode("utf-8", errors="ignore")).hexdigest()[:16]


def _business_rules_hash() -> str:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(root, "config", "business_rules.yaml")
    if not os.path.isfile(path):
        return ""
    try:
        with open(path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()[:16]
    except Exception:
        return ""


def _parse_failures_estimate(ds_meta: Dict[str, Any]) -> int:
    cols = ds_meta.get("columns") or {}
    n = 0
    if not isinstance(cols, dict):
        return 0
    total_rows = int(ds_meta.get("row_count") or 0)
    for c in cols.values():
        if not isinstance(c, dict):
            continue
        if c.get("dtype_inference") == "unparseable" or c.get("parse_failure_ratio"):
            try:
                ratio = float(c.get("parse_failure_ratio") or 0)
                n += int(ratio * total_rows) if total_rows else 0
            except (TypeError, ValueError):
                pass
    return n


def enrich_assessment_with_governance(
    assessment: Dict[str, Any],
    datasets: Optional[Dict[str, pd.DataFrame]] = None,
    *,
    job_id: Optional[str] = None,
    business_rules: Optional[Dict[str, Any]] = None,
    run_gx: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Post-process an assessment dict: semantic context, manifest versioning, drift,
    reconciliation, cross-field rules, optional GX + unified issues.
    Mutates assessment in place and returns it.
    """
    if not isinstance(assessment, dict) or not assessment.get("datasets"):
        return assessment

    datasets = datasets or {}
    business_rules = business_rules or {}
    notes = str(business_rules.get("notes") or "")[:4000]
    prior = business_rules.get("prior_report_hints") or business_rules.get("report_hints")

    manifest = load_metadata_manifest()
    m_hash = manifest_hash(manifest)
    gloss_hash = hashlib.sha256(json.dumps(manifest.get("datasets") or {}, sort_keys=True, default=str).encode()).hexdigest()[:16]
    schema_h = _schema_hash(assessment)
    rules_h = _business_rules_hash()

    assessment.setdefault("governance", {})
    gov = assessment["governance"]
    if not isinstance(gov, dict):
        gov = {}
        assessment["governance"] = gov
    gov.update(
        {
            "manifest_version": str(manifest.get("version") or "0"),
            "manifest_hash": m_hash,
            "glossary_hash": gloss_hash,
            "schema_hash": schema_h,
            "rules_hash": rules_h,
            "profile_version": "1",
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )

    warning = check_manifest_staleness(assessment)
    if warning:
        gov["manifest_stale_warning"] = warning

    assessment.setdefault("raw_payload_registry", empty_registry())

    # Semantic contexts
    sem_pkg = build_all_semantic_contexts(
        assessment,
        sample_by_dataset=datasets,
        manifest=manifest,
        user_notes=notes,
        prior_hints=prior if isinstance(prior, dict) else None,
    )
    assessment["semantic_context"] = sem_pkg
    assessment["semantic_ambiguity"] = _collect_semantic_ambiguity(assessment)
    _attach_join_hints(assessment)

    to_archive: Optional[Dict[str, Any]] = None
    if isinstance(sem_pkg, dict):
        c0 = sem_pkg.get("contract")
        if isinstance(c0, dict) and c0:
            to_archive = c0
        elif sem_pkg.get("by_dataset"):
            to_archive = {
                "overall_semantic_confidence": sem_pkg.get("overall_semantic_confidence"),
                "by_dataset": sem_pkg.get("by_dataset"),
            }
    if to_archive:
        try:
            from agent.manifest_version_store import save_contract_snapshot

            rid = str(job_id or gov.get("run_timestamp") or "run").strip()
            snap = save_contract_snapshot(
                rid,
                to_archive,
                schema_hash=schema_h,
                storage_path=os.getenv("DHARA_MANIFEST_HISTORY_DIR") or None,
            )
            gov["contract_snapshot"] = snap
            if snap.get("warning"):
                gov["contract_snapshot_warning"] = str(snap["warning"])[:500]
        except Exception as ex:
            gov["contract_snapshot_warning"] = str(ex)[:500]

    # Per-dataset manifest errors
    for ds_name, ds_meta in (assessment.get("datasets") or {}).items():
        if not isinstance(ds_meta, dict):
            continue
        entry, _matched = resolve_dataset_manifest(str(ds_name), manifest)
        cols = list((ds_meta.get("columns") or {}).keys())
        if entry:
            errs = validate_manifest_against_schema(entry, cols)
            if errs:
                ds_meta.setdefault("manifest_validation_errors", errs)
                dq = (assessment.setdefault("data_quality_issues", {}).setdefault("datasets", {}))
                block = dq.setdefault(str(ds_name), {"issues": [], "summary": {"issue_count": 0, "high_severity": 0, "medium_severity": 0, "low_severity": 0}})
                for e in errs[:20]:
                    block["issues"].append(
                        {
                            "type": "metadata_manifest_mismatch",
                            "severity": "medium",
                            "column": "",
                            "count": 1,
                            "message": e,
                            "recommendation": "Update config/metadata_manifest.yaml to match the data.",
                        }
                    )
                summ = block.setdefault(
                    "summary",
                    {"issue_count": 0, "high_severity": 0, "medium_severity": 0, "low_severity": 0},
                )
                summ["issue_count"] = len(block["issues"])
                summ["high_severity"] = sum(1 for i in block["issues"] if str(i.get("severity")).lower() == "high")
                summ["medium_severity"] = sum(1 for i in block["issues"] if str(i.get("severity")).lower() == "medium")
                summ["low_severity"] = sum(1 for i in block["issues"] if str(i.get("severity")).lower() == "low")

    # Reconciliation + drift
    recon_items: List[Dict[str, Any]] = []
    drift_by: Dict[str, Any] = {}
    for ds_name, ds_meta in (assessment.get("datasets") or {}).items():
        if not isinstance(ds_meta, dict):
            continue
        rc = int(ds_meta.get("row_count") or 0)
        pf = _parse_failures_estimate(ds_meta)
        recon_items.append(build_reconciliation_from_profile(str(ds_name), rc, parse_failures=pf))
        fp = build_profile_fingerprint(ds_meta)
        prev = load_latest_snapshot(str(ds_name))
        drift = compare_snapshots(prev, fp)
        drift_by[str(ds_name)] = drift
        try:
            save_snapshot(str(ds_name), fp, run_id=job_id or gov.get("run_timestamp", "run"))
        except Exception as ex:
            logger.debug("snapshot skip: %s", ex)

    assessment["reconciliation"] = merge_reconciliations(recon_items)
    assessment["drift"] = {"by_dataset": drift_by}
    assessment["drift_analysis"] = aggregate_drift(drift_by)
    assessment["reconciliation_analysis"] = analyze_reconciliation_bundle(assessment.get("reconciliation"))

    # Promote drift signals (severity >= medium) to DQ issues
    _drift_signals_to_dq_issues(assessment, drift_by)

    # Cross-field rules
    rules = load_cross_field_rules()
    from agent.auto_cross_field_rules import generate_auto_cross_field_rules
    auto_rules = generate_auto_cross_field_rules(assessment, assessment.get("semantic_context"))
    
    combined_rules = list(rules)
    for ar in auto_rules:
        duplicate = False
        for cr in combined_rules:
            if cr.get("dataset") == ar.get("dataset") and cr.get("type") == ar.get("type"):
                if ar.get("type") == "date_order" and cr.get("start_column") == ar.get("start_column") and cr.get("end_column") == ar.get("end_column"):
                    duplicate = True
                    break
                elif ar.get("type") == "non_negative" and cr.get("column") == ar.get("column"):
                    duplicate = True
                    break
        if not duplicate:
            combined_rules.append(ar)

    dq = assessment.setdefault("data_quality_issues", {}).setdefault("datasets", {})
    for ds_name, df in datasets.items():
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue
        extra = evaluate_cross_field_rules(str(ds_name), df, combined_rules)
        if not extra:
            continue
        from agent.etl_pipeline.dq_issue_schema import normalize_issue_dict
        normalized_extra = [normalize_issue_dict(i, str(ds_name), "cross_field") for i in extra]
        block = dq.setdefault(str(ds_name), {"issues": [], "summary": {"issue_count": 0, "high_severity": 0, "medium_severity": 0, "low_severity": 0}})
        block.setdefault("issues", []).extend(normalized_extra)
        summ = block.setdefault("summary", {"issue_count": 0, "high_severity": 0, "medium_severity": 0, "low_severity": 0})
        summ["issue_count"] = len(block["issues"])
        summ["high_severity"] = sum(1 for i in block["issues"] if str(i.get("severity")).lower() == "high")
        summ["medium_severity"] = sum(1 for i in block["issues"] if str(i.get("severity")).lower() == "medium")
        summ["low_severity"] = sum(1 for i in block["issues"] if str(i.get("severity")).lower() == "low")

    # Supplemental relationship checks (SQL-style anti-joins on loaded frames)
    if os.getenv("DHARA_RUN_REL_CHECKS", "").strip().lower() in ("1", "true", "yes") and len(datasets) >= 2:
        from agent.relationship_checker import (
            merge_supplemental_relationship_issues,
            relationship_issue_key,
            run_relationship_checks,
        )

        rels = assessment.get("relationships") or []
        new_issues, _w = run_relationship_checks(datasets, rels)
        gi = assessment.setdefault("data_quality_issues", {}).setdefault("global_issues", {})
        ex = gi.get("relationship_row_issues")
        if not isinstance(ex, list):
            ex = []
        old_keys = {relationship_issue_key(y) for y in ex if isinstance(y, dict)}
        merged = merge_supplemental_relationship_issues(ex, new_issues)
        gi["relationship_row_issues"] = merged
        gi["relationship_row_issues_supplemental"] = [
            x for x in merged if isinstance(x, dict) and relationship_issue_key(x) not in old_keys
        ]

    try:
        from agent.gx_suite_builder import list_expectation_descriptors_from_assessment

        assessment["gx_expectation_descriptors"] = list_expectation_descriptors_from_assessment(assessment)
    except Exception:
        pass

    # Optional GX
    if run_gx is None:
        run_gx = os.getenv("DHARA_RUN_GX", "").strip().lower() in ("1", "true", "yes")
    gx_results: Dict[str, Any] = {}
    if run_gx and datasets:
        try:
            from agent.gx_runner import run_suite

            gx_results = run_suite(datasets, assessment) or {}
        except Exception as e:
            logger.warning("GX run skipped: %s", e)
            gx_results = {"_error": str(e)}
    assessment["gx_results"] = gx_results

    unified = map_gx_to_unified_issues(
        gx_results if isinstance(gx_results, dict) else {},
        semantic_by_dataset=(sem_pkg.get("by_dataset") or {}),
        drift_by_dataset=drift_by,
        reconciliation=assessment.get("reconciliation"),
    )
    assessment["unified_issues"] = unified

    # ETL hints for planner/codegen
    assessment.setdefault("enriched_etl_context", {})
    ee = assessment["enriched_etl_context"]
    if isinstance(ee, dict):
        ee.update(
            {
                "semantic_overall_confidence": sem_pkg.get("overall_semantic_confidence"),
                "drift_summary": {k: v.get("severity") for k, v in drift_by.items() if isinstance(v, dict)},
                "drift_score": (assessment.get("drift_analysis") or {}).get("drift_score"),
                "reconciliation_ok": all(
                    (r.get("balanced") is not False) for r in (assessment.get("reconciliation") or {}).get("by_dataset", {}).values()
                ),
                "gx_evaluated": bool(gx_results) and "_error" not in gx_results,
            }
        )

    try:
        from agent.etl_readiness_scorer import compute_etl_readiness

        assessment["etl_readiness"] = compute_etl_readiness(assessment)
    except Exception:
        pass

    return assessment


def check_manifest_staleness(assessment: Dict[str, Any]) -> Optional[str]:
    """
    Compares the metadata manifest modified time with the run timestamp of the assessment.
    Returns a warning message if the manifest has been modified since the assessment.
    """
    if not isinstance(assessment, dict):
        return None
    gov = assessment.get("governance")
    if not isinstance(gov, dict) or "run_timestamp" not in gov:
        return None

    manifest_path = Path(os.getenv("DHARA_METADATA_MANIFEST", str(Path(__file__).resolve().parent.parent / "config" / "metadata_manifest.yaml")))
    if not manifest_path.is_file():
        return None

    try:
        from datetime import datetime, timezone
        last_run = datetime.fromisoformat(gov["run_timestamp"])
        manifest_mtime = datetime.fromtimestamp(manifest_path.stat().st_mtime, tz=timezone.utc if last_run.tzinfo else None)
        if manifest_mtime > last_run:
            return "The metadata manifest has been modified since this assessment was generated. Please re-run assessment to apply changes."
    except Exception as e:
        logger.debug("Manifest staleness check failed: %s", e)
    return None


def _drift_signals_to_dq_issues(assessment: Dict[str, Any], drift_by: Dict[str, Any]) -> None:
    dq = assessment.setdefault("data_quality_issues", {}).setdefault("datasets", {})
    for ds_name, drift in drift_by.items():
        if not isinstance(drift, dict) or not drift.get("signals"):
            continue
        extra_issues = []
        for sig in drift["signals"]:
            if not isinstance(sig, dict):
                continue
            severity = str(sig.get("severity") or "low").lower()
            if severity not in ("medium", "high"):
                continue

            kind = sig.get("kind", "unknown_drift")
            col = sig.get("column")

            if kind == "row_count_drift":
                msg = f"Dataset row count drifted from {sig.get('previous')} to {sig.get('current')} (delta: {sig.get('relative_delta')})."
            elif kind == "schema_drift":
                added = sig.get("columns_added") or []
                removed = sig.get("columns_removed") or []
                parts = []
                if added:
                    parts.append(f"added: {', '.join(added)}")
                if removed:
                    parts.append(f"removed: {', '.join(removed)}")
                msg = f"Schema drifted: {'; '.join(parts)}."
            elif kind == "dtype_drift":
                msg = f"Column '{col}' data type drifted from {sig.get('previous')} to {sig.get('current')}."
            elif kind == "null_rate_drift":
                msg = f"Column '{col}' null rate drifted from {sig.get('previous')} to {sig.get('current')}."
            else:
                msg = f"Drift detected ({kind}): previous={sig.get('previous')}, current={sig.get('current')}."

            extra_issues.append({
                "dataset": ds_name,
                "column": col or None,
                "issue_type": kind,
                "severity": severity,
                "message": msg,
                "value": sig.get("current")
            })

        if extra_issues:
            block = dq.setdefault(ds_name, {
                "issues": [],
                "summary": {"issue_count": 0, "high_severity": 0, "medium_severity": 0, "low_severity": 0}
            })
            block.setdefault("issues", []).extend(extra_issues)
            block["summary"]["issue_count"] = len(block["issues"])
            block["summary"]["high_severity"] = sum(1 for i in block["issues"] if str(i.get("severity")).lower() == "high")
            block["summary"]["medium_severity"] = sum(1 for i in block["issues"] if str(i.get("severity")).lower() == "medium")
            block["summary"]["low_severity"] = sum(1 for i in block["issues"] if str(i.get("severity")).lower() == "low")

