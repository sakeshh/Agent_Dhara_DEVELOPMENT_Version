from __future__ import annotations
import os
import re
import time
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd

from agent.profiling.constants import *
from agent.profiling.data_loaders import load_file_datasets, load_sql_datasets
from agent.profiling.statistical_profiling import profile_dataframe, select_top_priority_columns, _strip, safe_nunique, _to_key
from agent.profiling.database_profiler import profile_database_table_full, merge_in_db_profile
from agent.profiling.dq_checks import (
    analyze_dataset_quality,
    run_custom_rules,
    enrich_issue_with_recommendation,
    enrich_issue_with_fixability,
    make_json_serializable,
)
from agent.profiling.contracts import FIXABILITY_BY_ISSUE_TYPE, DQ_ISSUE_RECOMMENDATIONS, _DEFAULT_REC

logger = logging.getLogger("agent.profiler")

def load_dq_thresholds(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load DQ thresholds from YAML. If path is None, use env DQ_THRESHOLDS_PATH or config/dq_thresholds.yaml."""
    path = config_path or os.environ.get("DQ_THRESHOLDS_PATH")
    if not path and os.path.isdir("config"):
        path = os.path.join("config", "dq_thresholds.yaml")
    if not path or not os.path.isfile(path):
        return {}
    try:
        import yaml
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}

def _get_threshold(thresholds: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Get nested key from thresholds, e.g. _get_threshold(t, 'severity', 'null_pct_high', default=0.25)."""
    d = thresholds
    for k in keys:
        d = (d or {}).get(k)
        if d is None:
            return default
    return d if d is not None else default

def _guess_parent_child_tables(
    n1: str, df1: pd.DataFrame, c1: str,
    n2: str, df2: pd.DataFrame, c2: str,
    meta1: Dict[str, Any], meta2: Dict[str, Any],
) -> Optional[Tuple[str, pd.DataFrame, str, str, pd.DataFrame, str]]:
    """
    Return (parent_ds, parent_df, parent_col, child_ds, child_df, child_col) for FK-style checks, or None.
    """
    nn1 = int(df1[c1].notna().sum())
    nn2 = int(df2[c2].notna().sum())
    if nn1 == 0 or nn2 == 0:
        return None
    u1, u2 = safe_nunique(df1[c1]), safe_nunique(df2[c2])
    r1, r2 = u1 / max(nn1, 1), u2 / max(nn2, 1)

    # Use sampling for large datasets when checking for overlap and cardinality
    if len(df1) > 100_000 or len(df2) > 100_000:
        sample_size = 50_000
        s1 = df1[c1].dropna().sample(min(len(df1[c1].dropna()), sample_size), random_state=42)
        s2 = df2[c2].dropna().sample(min(len(df2[c2].dropna()), sample_size), random_state=42)
        k1 = s1.map(_to_key)
        k2 = s2.map(_to_key)
        try:
            vc1 = k1.value_counts()
            vc2 = k2.value_counts()
            common = vc1.index.intersection(vc2.index)
        except Exception:
            return None
        if len(common) == 0:
            return None
        m1 = int(vc1.reindex(common).fillna(0).max())
        m2 = int(vc2.reindex(common).fillna(0).max())
    else:
        k1 = df1[c1].map(_to_key)
        k2 = df2[c2].map(_to_key)
        try:
            vc1 = k1.dropna().value_counts()
            vc2 = k2.dropna().value_counts()
            common = vc1.index.intersection(vc2.index)
        except Exception:
            return None
        if len(common) == 0:
            return None
        m1 = int(vc1.reindex(common).fillna(0).max())
        m2 = int(vc2.reindex(common).fillna(0).max())

    pk1 = (meta1.get("columns") or {}).get(c1, {}).get("candidate_primary_key")
    pk2 = (meta2.get("columns") or {}).get(c2, {}).get("candidate_primary_key")
    if pk1 and not pk2:
        return (n1, df1, c1, n2, df2, c2)
    if pk2 and not pk1:
        return (n2, df2, c2, n1, df1, c1)
    if r1 >= 0.995 and r2 < 0.97:
        return (n1, df1, c1, n2, df2, c2)
    if r2 >= 0.995 and r1 < 0.97:
        return (n2, df2, c2, n1, df1, c1)
    if m1 == 1 and m2 > 1:
        return (n1, df1, c1, n2, df2, c2)
    if m2 == 1 and m1 > 1:
        return (n2, df2, c2, n1, df1, c1)
    return None

def _classify_cardinality(m1: int, m2: int) -> Tuple[str, str]:
    """
    m1 = max rows per shared key in table A; m2 = max in table B.
    Returns (cardinality_code, human_summary).
    """
    if m1 <= 1 and m2 <= 1:
        return ("one_to_one", "Each key appears at most once in both tables (1:1 on overlapping keys).")
    if m1 <= 1 < m2:
        return ("one_to_many", f"Table A has at most one row per key; table B has up to {m2} rows per key (1:N from A to B).")
    if m2 <= 1 < m1:
        return ("many_to_one", f"Table B has at most one row per key; table A has up to {m1} rows per key (N:1 from A to B).")
    return ("many_to_many", f"Keys repeat on both sides (up to {m1} vs {m2} rows per key) - M:N or bridge-style.")

def _same_dataset_representation(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    *,
    min_id_overlap: float = 0.90,
    max_row_diff: float = 0.10,
) -> bool:
    """Check if two DataFrames represent the same dataset in different formats."""
    try:
        cols1 = {str(c).strip().lower() for c in df1.columns}
        cols2 = {str(c).strip().lower() for c in df2.columns}
        if cols1 != cols2 or "id" not in cols1:
            return False
        c1 = next(c for c in df1.columns if str(c).lower() == "id")
        c2 = next(c for c in df2.columns if str(c).lower() == "id")
        
        s1 = df1[c1].map(_to_key).dropna()
        s2 = df2[c2].map(_to_key).dropna()
        k1 = set(s1.tolist())
        k2 = set(s2.tolist())
        if not k1 or not k2:
            return False
        inter = k1 & k2
        overlap_ratio = len(inter) / max(1, min(len(k1), len(k2)))
        r1, r2 = len(df1), len(df2)
        row_diff_ratio = abs(r1 - r2) / max(1, max(r1, r2))
        if not (overlap_ratio >= min_id_overlap and row_diff_ratio <= max_row_diff):
            return False

        # Stronger check: do rows actually match on shared IDs?
        # Only check a sample of 50 shared IDs for performance.
        inter_list = list(inter)
        if len(inter_list) > 50:
            inter_list = inter_list[:50]
        
        cols = [c for c in df1.columns if str(c).lower() != "id"]
        if not cols:
            return True
            
        # Filter both dataframes to the 50 sample IDs
        df1_sub = df1[df1[c1].map(_to_key).isin(inter_list)]
        df2_sub = df2[df2[c2].map(_to_key).isin(inter_list)]
        
        # Build signatures for the small subset
        def _row_sig(df: pd.DataFrame, id_col: str) -> Dict[Any, Tuple[Any, ...]]:
            out = {}
            for _, row in df.iterrows():
                ik = _to_key(row[id_col])
                if ik is None or ik in out:
                    continue
                out[ik] = tuple(_to_key(row[c]) for c in cols)
            return out

        m1 = _row_sig(df1_sub, c1)
        m2 = _row_sig(df2_sub, c2)
        
        if not m1 or not m2:
            return False
            
        matches = 0
        total = 0
        for ik in inter_list:
            if ik in m1 and ik in m2:
                total += 1
                if m1[ik] == m2[ik]:
                    matches += 1
        if total == 0:
            return False
        return (matches / total) >= 0.80
    except Exception:
        return False

def analyze_cross_dataset_relationships(
    datasets: Dict[str, pd.DataFrame],
    metadata: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    For each pair of datasets sharing a column name (case-insensitive):
    - overlap count, cardinality (one_to_one / one_to_many / many_to_one / many_to_many)
    - Row-level orphan FK issues (child rows whose key is missing from parent)
    - Warnings for ambiguous M:N on id-like columns
    """
    relationships: List[Dict[str, Any]] = []
    row_issues: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    thresholds = thresholds or {}
    rel_cfg = thresholds.get("relationships") or {}
    include_non_key = bool(rel_cfg.get("include_non_key_columns", False))
    orphan_only_if_same_data = bool(rel_cfg.get("orphan_only_if_same_dataset", True))
    same_data_min_id_overlap = float(rel_cfg.get("same_dataset_min_id_overlap_ratio", 0.90))
    same_data_max_row_diff = float(rel_cfg.get("same_dataset_max_rowcount_diff_ratio", 0.10))

    def _is_key_like(col_lower: str) -> bool:
        return (col_lower.endswith("_id") or col_lower == "id" 
                or col_lower.endswith("_key") or col_lower.endswith("_code")
                or col_lower.endswith("_sku") or col_lower == "sku")

    names = list(datasets.keys())

    for i in range(len(names)):
        n1, df1 = names[i], datasets[names[i]]
        meta1 = metadata.get(n1, {}) or {}
        for j in range(i + 1, len(names)):
            n2, df2 = names[j], datasets[names[j]]
            meta2 = metadata.get(n2, {}) or {}
            if df1.empty or df2.empty:
                continue
            common = set(map(str.lower, df1.columns)) & set(map(str.lower, df2.columns))
            for col_lower in common:
                if (not include_non_key) and (not _is_key_like(col_lower)):
                    continue
                c1 = next(x for x in df1.columns if str(x).lower() == col_lower)
                c2 = next(x for x in df2.columns if str(x).lower() == col_lower)
                try:
                    k1_full = df1[c1]
                    k2_full = df2[c2]
                    # No sampling, user wants full analysis
                    k1 = k1_full.map(_to_key)
                    k2 = k2_full.map(_to_key)
                    s1k = set(k1.dropna().tolist())
                    s2k = set(k2.dropna().tolist())
                    overlap = s1k & s2k
                except Exception:
                    continue
                if not overlap:
                    continue
                vc1 = k1.dropna().value_counts()
                vc2 = k2.dropna().value_counts()
                common_idx = vc1.index.intersection(vc2.index)
                m1 = int(vc1.reindex(common_idx).fillna(0).max()) if len(common_idx) else 1
                m2 = int(vc2.reindex(common_idx).fillna(0).max()) if len(common_idx) else 1
                card, summary = _classify_cardinality(m1, m2)
                rel = {
                    "from": f"{n1}.{c1}",
                    "to": f"{n2}.{c2}",
                    "dataset_a": n1,
                    "dataset_b": n2,
                    "column_a": c1,
                    "column_b": c2,
                    "overlap_count": len(overlap),
                    "cardinality": card,
                    "max_rows_per_key_a": m1,
                    "max_rows_per_key_b": m2,
                    "summary": summary,
                    "from_a_to_b": (
                        "one_to_many" if m1 <= 1 < m2 else
                        "many_to_one" if m2 <= 1 < m1 else
                        "one_to_one" if m1 <= 1 and m2 <= 1 else
                        "many_to_many"
                    ),
                }
                relationships.append(rel)

                if m1 > 1 and m2 > 1:
                    id_like = any(
                        x in col_lower for x in ("_id", "id", "key", "code", "sku")
                    )
                    sev = "medium" if id_like else "low"
                    warnings.append({
                        "severity": sev,
                        "type": "many_to_many_relationship",
                        "datasets": [n1, n2],
                        "columns": [c1, c2],
                        "message": (
                            f"{n1}.{c1} <-> {n2}.{c2}: keys repeat on both sides "
                            f"(max {m1} rows per key in {n1}, max {m2} in {n2})."
                        ),
                        "recommendation": (
                            "If you expected a parent-child (1:N) model, deduplicate keys on the 'one' side "
                            "or fix source extraction. If M:N is correct (e.g. orders-products), model it with "
                            "a junction table and FK constraints."
                        ),
                    })

                guess = _guess_parent_child_tables(n1, df1, c1, n2, df2, c2, meta1, meta2)
                if guess:
                    _pn, pdf, pc, cn, cdf, cc = guess
                    if orphan_only_if_same_data and not _same_dataset_representation(pdf, cdf, min_id_overlap=same_data_min_id_overlap, max_row_diff=same_data_max_row_diff):
                        continue
                    try:
                        parent_keys = set(_to_key(x) for x in pdf[pc].dropna())
                    except Exception:
                        parent_keys = set()
                    if not parent_keys:
                        continue
                    ck = cdf[cc].map(lambda x: _to_key(x) if pd.notna(x) else None)
                    orphan = cdf[cc].notna() & ~ck.isin(parent_keys)
                    oc = int(orphan.sum())
                    if oc > 0:
                        oidx = cdf.index[orphan].tolist()[:MAX_REL_ROW_INDEXES]
                        samples = list(cdf.loc[orphan, cc].head(8))
                        row_issues.append({
                            "severity": "high",
                            "type": "orphan_foreign_key_rows",
                            "dataset": cn,
                            "column": cc,
                            "related_dataset": _pn,
                            "related_column": pc,
                            "count": oc,
                            "row_indexes": oidx,
                            "sample_values": samples,
                            "message": (
                                f"{oc} row(s) in '{cn}' column '{cc}' reference value(s) not found in "
                                f"'{_pn}'.'{pc}' (orphan / broken FK)."
                            ),
                            "recommendation": (
                                f"1) Add missing keys to '{_pn}' or remove bad rows from '{cn}'. "
                                f"2) Enforce FK in the source DB or pipeline. "
                                f"3) Trim/normalize keys (whitespace, type) if mismatch is format-only."
                            ),
                        })

    return {
        "relationships": relationships,
        "relationship_row_issues": row_issues,
        "relationship_warnings": warnings,
    }

def detect_relationships(
    datasets: Dict[str, pd.DataFrame],
    metadata: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Returns enriched relationship list (cardinality, summaries)."""
    return analyze_cross_dataset_relationships(datasets, metadata or {})["relationships"]

def analyze_cross_dataset_consistency(
    datasets: Dict[str, pd.DataFrame],
    metadata: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Cross-dataset insights for data engineers:
    - ID type drift across datasets (e.g., JSON mixed str/int vs CSV int)
    - Likely duplicate representations (same schema + high ID overlap)
    """
    thresholds = thresholds or {}
    out: List[Dict[str, Any]] = []

    # ID type drift
    try:
        id_summaries: Dict[str, Any] = {}
        for name, df in datasets.items():
            id_col = None
            for c in df.columns:
                cl = str(c).lower()
                if cl == "id" or cl.endswith("_id"):
                    id_col = c
                    break
            if id_col is None:
                continue
            td = scalar_type_distribution(df[id_col])
            id_summaries[name] = {"column": str(id_col), "type_distribution": td}

        if len(id_summaries) >= 2:
            def _bucket(td: Dict[str, Any]) -> str:
                pct = (td.get("pct") or {})
                strp = float(pct.get("str", 0.0))
                nump = float(pct.get("int", 0.0)) + float(pct.get("float", 0.0))
                if strp >= 0.10 and nump >= 0.10:
                    return "mixed_str_num"
                if nump >= 0.80:
                    return "mostly_numeric"
                if strp >= 0.80:
                    return "mostly_string"
                return "other"

            buckets = {ds: _bucket(v["type_distribution"]) for ds, v in id_summaries.items()}
            if len(set(buckets.values())) >= 2:
                out.append({
                    "severity": "high",
                    "type": "id_type_drift_across_datasets",
                    "message": "ID column uses inconsistent scalar types across datasets (serialization/type drift).",
                    "details": {"buckets": buckets, "samples": id_summaries},
                })
    except Exception:
        pass

    # Duplicate representation candidates: schema match + high ID overlap
    try:
        dupe_cfg = thresholds.get("duplicate_detection") or {}
        min_overlap = float(dupe_cfg.get("min_id_overlap_ratio", 0.95))
        max_row_diff = float(dupe_cfg.get("max_rowcount_diff_ratio", 0.05))

        def _schema_sig(df: pd.DataFrame) -> Tuple[str, ...]:
            return tuple(sorted({str(c).strip().lower() for c in df.columns}))

        groups: Dict[Tuple[str, ...], List[str]] = {}
        for name, df in datasets.items():
            groups.setdefault(_schema_sig(df), []).append(name)

        for sig, names in groups.items():
            if len(names) < 2 or len(sig) == 0:
                continue
            for i in range(len(names)):
                for j in range(i + 1, len(names)):
                    a, b = names[i], names[j]
                    dfa, dfb = datasets[a], datasets[b]
                    if "id" not in [str(c).lower() for c in dfa.columns] or "id" not in [str(c).lower() for c in dfb.columns]:
                        continue
                    ca = next(c for c in dfa.columns if str(c).lower() == "id")
                    cb = next(c for c in dfb.columns if str(c).lower() == "id")
                    inter = set()
                    # Sampling for large datasets in duplicate representation check
                    if len(dfa) > 100_000 or len(dfb) > 100_000:
                        sample_a = dfa[ca].dropna().sample(min(len(dfa), 50_000), random_state=42).map(_to_key)
                        sample_b = dfb[cb].dropna().sample(min(len(dfb), 50_000), random_state=42).map(_to_key)
                        ka = set(sample_a.tolist())
                        kb = set(sample_b.tolist())
                    else:
                        ka = set(dfa[ca].map(_to_key).dropna().tolist())
                        kb = set(dfb[cb].map(_to_key).dropna().tolist())
                    
                    if not ka or not kb:
                        continue
                    inter = ka & kb
                    overlap_ratio = len(inter) / max(1, min(len(ka), len(kb)))
                    ra, rb = len(dfa), len(dfb)
                    row_diff_ratio = abs(ra - rb) / max(1, max(ra, rb))
                    if overlap_ratio >= min_overlap and row_diff_ratio <= max_row_diff:
                        out.append({
                            "severity": "medium",
                            "type": "duplicate_representation_candidate",
                            "message": f"Datasets '{a}' and '{b}' likely represent the same records in different formats.",
                            "details": {
                                "schema_columns": list(sig)[:30],
                                "id_overlap_ratio": round(overlap_ratio, 4),
                                "id_overlap_count": len(inter),
                                "row_counts": {a: ra, b: rb},
                            },
                        })
    except Exception:
        pass

    # Enrich recommendations
    for it in out:
        enrich_issue_with_recommendation(it)
    return out

def build_executive_summary_items(
    per_dataset_dq: Dict[str, Any],
    global_issues: Dict[str, Any],
    thresholds: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Business-first summary: rank the most impactful signals into a small list.
    Uses a lightweight scoring model (severity * datasets affected).
    """
    thresholds = thresholds or {}
    cfg = thresholds.get("executive_summary") or {}
    max_items = int(cfg.get("max_items", 15))
    sev_w = {"high": 3.0, "medium": 2.0, "low": 1.0}

    rollup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for ds, block in (per_dataset_dq or {}).items():
        issues = (block or {}).get("issues") or []
        for it in issues:
            typ = str(it.get("type") or "")
            col = str(it.get("column") or "")
            key = (typ, col)
            r = rollup.setdefault(key, {"type": typ, "column": col, "datasets": set(), "sev_max": "low", "rows": 0})
            r["datasets"].add(ds)
            r["rows"] += int(it.get("count") or 0)
            if sev_w.get(str(it.get("severity") or "low"), 1) > sev_w.get(r["sev_max"], 1):
                r["sev_max"] = str(it.get("severity") or "low")

    # add cross-dataset consistency signals
    for it in (global_issues.get("cross_dataset_consistency") or []):
        if not isinstance(it, dict):
            continue
        key = (str(it.get("type") or ""), "")
        r = rollup.setdefault(key, {"type": key[0], "column": "", "datasets": set(), "sev_max": "low", "rows": 0})
        if sev_w.get(str(it.get("severity") or "low"), 1) > sev_w.get(r["sev_max"], 1):
            r["sev_max"] = str(it.get("severity") or "low")
        
        # Populate datasets affected for cross-dataset consistency signals
        details = it.get("details") or {}
        if "buckets" in details:
            for ds in details["buckets"].keys():
                r["datasets"].add(ds)
        elif "row_counts" in details:
            for ds in details["row_counts"].keys():
                r["datasets"].add(ds)

    ranked = []
    for r in rollup.values():
        ds_count = len(r["datasets"]) if r["datasets"] else 1
        score = sev_w.get(r["sev_max"], 1.0) * (1.0 + min(3.0, ds_count / 2.0))
        ranked.append({**r, "datasets_affected": ds_count, "score": float(score)})
    ranked.sort(key=lambda x: (-x.get("score", 0.0), -x.get("datasets_affected", 0), -x.get("rows", 0)))

    items = []
    for x in ranked[:max_items]:
        items.append({
            "title": x["type"] + (f" ({x['column']})" if x.get("column") else ""),
            "severity": x.get("sev_max"),
            "datasets_affected": x.get("datasets_affected"),
            "estimated_rows_affected": x.get("rows"),
            "recommendation": DQ_ISSUE_RECOMMENDATIONS.get(x["type"], _DEFAULT_REC),
        })
    return items

def detect_global_issues(datasets: Dict[str, pd.DataFrame], thresholds: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    - Orphan foreign keys: values present in one dataset.column but not in the counterpart
    - Cross-dataset inconsistencies: coarse mixed numeric/text indicator per column by parse-rate
    """
    thresholds = thresholds or {}
    rel_cfg = thresholds.get("relationships") or {}
    orphan_only_if_same_data = bool(rel_cfg.get("orphan_only_if_same_dataset", True))
    same_data_min_id_overlap = float(rel_cfg.get("same_dataset_min_id_overlap_ratio", 0.90))
    same_data_max_row_diff = float(rel_cfg.get("same_dataset_max_rowcount_diff_ratio", 0.10))

    global_issues = {
        "orphan_foreign_keys": [],
        "cross_dataset_inconsistencies": [],
        "schema_drift": []
    }

    names = list(datasets.keys())
    for i in range(len(names)):
        df1 = datasets[names[i]]
        for j in range(i + 1, len(names)):
            df2 = datasets[names[j]]
            same_data = _same_dataset_representation(df1, df2, min_id_overlap=same_data_min_id_overlap, max_row_diff=same_data_max_row_diff)

            common = set(map(str.lower, df1.columns)) & set(map(str.lower, df2.columns))
            for col in common:
                if not col.endswith("id"):
                    continue
                c1 = next(x for x in df1.columns if x.lower() == col)
                c2 = next(x for x in df2.columns if x.lower() == col)

                s1 = df1[c1].dropna()
                s2 = df2[c2].dropna()

                try:
                    set1 = set(s1.map(_to_key).dropna())
                    set2 = set(s2.map(_to_key).dropna())
                except Exception:
                    continue

                only_left = list(set1 - set2)
                only_right = list(set2 - set1)

                _orph_rec = (
                    "Align keys between datasets (trim, type cast). Add missing reference rows or remove "
                    "orphan facts in the child extract. Prefer FK constraints in the source system."
                )
                if (not orphan_only_if_same_data) or same_data:
                    if only_left:
                        global_issues["orphan_foreign_keys"].append({
                            "from": f"{names[i]}.{c1}",
                            "to": f"{names[j]}.{c2}",
                            "orphan_count": len(only_left),
                            "sample_values": only_left[:10],
                            "recommendation": _orph_rec,
                        })
                    if only_right:
                        global_issues["orphan_foreign_keys"].append({
                            "from": f"{names[j]}.{c2}",
                            "to": f"{names[i]}.{c1}",
                            "orphan_count": len(only_right),
                            "sample_values": only_right[:10],
                            "recommendation": _orph_rec,
                        })

            for nm, df in ((names[i], df1), (names[j], df2)):
                for col in df.columns:
                    s = df[col].map(_strip)
                    num = pd.to_numeric(s, errors="coerce")
                    parse_rate = 1.0 - float(num.isna().mean())
                    if 0.2 < parse_rate < 0.8:
                        global_issues["cross_dataset_inconsistencies"].append({
                            "dataset": nm,
                            "column": col,
                            "issue_type": "mixed_types",
                            "message": f"Mixed numeric/text values (parse={round(parse_rate*100,1)}%)",
                            "recommendation": (
                                "Standardize to one type in staging: coerce numerics after validation, "
                                "or split into _raw and _numeric columns."
                            ),
                        })

    # Deduplicate cross-dataset inconsistencies: one row per (dataset, column, issue_type)
    try:
        seen = set()
        deduped = []
        for x in global_issues.get("cross_dataset_inconsistencies", []) or []:
            key = (x.get("dataset"), x.get("column"), x.get("issue_type"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(x)
        global_issues["cross_dataset_inconsistencies"] = deduped
    except Exception:
        pass

    # ------------------------------------------------------------
    # 8. Schema Drift Detection Across Runs
    # ------------------------------------------------------------
    import os
    import json
    
    schema_cache_file = os.path.join("config", "schema_cache.json")
    # Build current schema representation
    current_schema = {}
    for name, df in datasets.items():
        current_schema[name] = {
            col: str(df[col].dtype) for col in df.columns
        }
        
    prev_schema = {}
    if os.path.exists(schema_cache_file):
        try:
            with open(schema_cache_file, "r", encoding="utf-8") as f:
                prev_schema = json.load(f)
        except Exception:
            pass
            
    # Save current schema for next runs
    try:
        os.makedirs(os.path.dirname(schema_cache_file), exist_ok=True)
        with open(schema_cache_file, "w", encoding="utf-8") as f:
            json.dump(current_schema, f, indent=4)
    except Exception:
        pass
        
    # Compare current schema with previous schema
    if prev_schema:
        for ds_name, curr_cols in current_schema.items():
            if ds_name in prev_schema:
                prev_cols = prev_schema[ds_name]
                added = [c for c in curr_cols if c not in prev_cols]
                removed = [c for c in prev_cols if c not in curr_cols]
                type_changed = []
                for c in curr_cols:
                    if c in prev_cols and curr_cols[c] != prev_cols[c]:
                        type_changed.append({"column": c, "from": prev_cols[c], "to": curr_cols[c]})
                        
                if added or removed or type_changed:
                    global_issues["schema_drift"].append({
                        "dataset": ds_name,
                        "added_columns": added,
                        "removed_columns": removed,
                        "type_changes": type_changed,
                        "message": f"Schema drift detected on '{ds_name}'. Added: {added}, Removed: {removed}, Type changes: {type_changed}"
                    })

    return global_issues

def detect_date_format_variants(series: pd.Series) -> list[dict]:
    """
    For object/string columns suspected as dates, count format variants.
    Returns list of {"format": str, "count": int, "pct": float}
    """
    import re
    patterns = {
        "DD/MM/YYYY": r"^\d{2}/\d{2}/\d{4}$",
        "YYYY-MM-DD": r"^\d{4}-\d{2}-\d{2}$",
        "MM-DD-YYYY": r"^\d{2}-\d{2}-\d{4}$",
        "YYYY/MM/DD": r"^\d{4}/\d{2}/\d{2}$",
        "Mon D YYYY": r"^[A-Za-z]+ \d{1,2} \d{4}$",
        "DD-Mon-YYYY": r"^\d{2}-[A-Za-z]+-\d{4}$",
    }
    sample = series.dropna().astype(str).str.strip().head(5000)
    total = len(sample)
    results = []
    if total == 0:
        return []
    for fmt_name, pattern in patterns.items():
        count = sample.str.match(pattern).sum()
        if count > 0:
            results.append({"format": fmt_name, "count": int(count), "pct": round(float(count / total), 4)})
    return sorted(results, key=lambda x: -x["count"])

def confirm_business_key_duplicates(df: pd.DataFrame, pk_cols: list[str]) -> dict:
    """
    Given LLM-suggested PK columns, confirm actual duplicate count on valid non-null key values.
    Rows with NULL or placeholder key values are tracked as missing_business_key_count rather than duplicates.
    """
    available = [c for c in pk_cols if c in df.columns]
    if not available:
        return {"confirmed": False, "reason": "pk_cols not found in dataframe"}
    
    # Filter out rows where key columns are null or placeholders
    from agent.profiling.constants import PLACEHOLDERS
    valid_key_mask = pd.Series(True, index=df.index)
    for c in available:
        s = df[c]
        s_clean = s.dropna().astype(str).str.strip().str.lower()
        is_invalid = s.isna() | s_clean.isin(PLACEHOLDERS)
        valid_key_mask &= (~is_invalid)
        
    valid_keys_df = df[valid_key_mask]
    dup_count = int(valid_keys_df.duplicated(subset=available).sum()) if not valid_keys_df.empty else 0
    missing_key_count = int((~valid_key_mask).sum())
    
    return {
        "confirmed": True,
        "business_key_cols": available,
        "business_key_duplicate_count": dup_count,
        "missing_business_key_count": missing_key_count,
        "dedup_strategy_hint": "keep_last" if dup_count > 0 else "no_action_needed"
    }

def detect_null_pattern(df: pd.DataFrame, col_name: str) -> dict:
    """
    Check if nulls in col_name correlate with a specific categorical column (MNAR detection).
    Caps at top-5 categorical columns to keep performance O(n).
    """
    null_mask = df[col_name].isnull()
    total_nulls = null_mask.sum()
    if total_nulls == 0:
        return {"type": "none"}
    cat_cols = [c for c in df.columns if c != col_name and df[c].dtype == object][:5]
    for cat_col in cat_cols:
        try:
            null_by_cat = df.groupby(cat_col)[col_name].apply(lambda x: x.isnull().mean())
            if not null_by_cat.empty and null_by_cat.max() > 0.8: # 80%+ nulls concentrated in one category
                return {
                    "type": "MNAR",
                    "concentrated_in_col": cat_col,
                    "concentrated_in_value": str(null_by_cat.idxmax()),
                    "fill_strategy_hint": "flag_only"
                }
        except Exception:
            pass
    return {"type": "MCAR", "fill_strategy_hint": "median_or_mode"}

def load_and_profile(
    source_cfg: Dict[str, Any],
    *,
    additional_data: Optional[Dict[str, pd.DataFrame]] = None,
    dq_thresholds_path: Optional[str] = None,
    dq_thresholds: Optional[Dict[str, Any]] = None,
    return_datasets: bool = False,
    location_types: Optional[Collection[str]] = None,
    job_id: Optional[str] = None,
    max_rows: Optional[int] = None,
    business_rules: Optional[Dict[str, Any]] = None,
    db_connectors: Optional[Dict[str, Any]] = None,
    approved_semantics: Optional[Dict[str, Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """
    Orchestrator:
    - Iterate over source_cfg["locations"]: all database + filesystem entries (azure_blob via additional_data)
    - Multiple databases: table keys prefixed (id/label or db hash) so names never collide
    - Merge with additional_data if provided (e.g., from Azure Blob Storage)
    - Profile each dataset; per-dataset DQ; relationships; global issues.
    - dq_thresholds: optional dict (if None, loaded from dq_thresholds_path or config).
    - return_datasets: if True, add result["_datasets"] = raw DataFrames (pop before JSON serialize).
    - location_types: optional set/list of lowercase location type strings (e.g. {"database","azure_blob"}).
      If set, only those location blocks are loaded from YAML. Blob data still comes only via additional_data
      (caller should pass {} when blob is excluded). If None, all location types are processed.
    """
    thresholds = dq_thresholds
    if thresholds is None:
        thresholds = load_dq_thresholds(dq_thresholds_path)

    datasets: Dict[str, pd.DataFrame] = {}
    source_root_by_dataset: Dict[str, str] = {}

    db_connectors_by_dataset: Dict[str, Tuple[Any, str]] = {}
    if db_connectors:
        for k, v in db_connectors.items():
            if isinstance(v, tuple) and len(v) == 2:
                db_connectors_by_dataset[k] = v
            else:
                table_name = k.split("__")[-1]
                db_connectors_by_dataset[k] = (v, table_name)

    locations = list(source_cfg.get("locations", []) or [])
    if location_types is not None:
        allowed = {str(t).lower() for t in location_types}
        locations = [loc for loc in locations if (loc.get("type") or "").lower() in allowed]
    db_locs = [loc for loc in locations if (loc.get("type") or "").lower() == "database"]
    multi_db = len(db_locs) > 1
    db_seen = 0

    for loc in locations:
        typ = (loc.get("type") or "").lower()

        if typ == "database":
            conn = loc.get("connection", {}) or {}
            prefix = _sql_location_key_prefix(loc, conn, db_seen, multi_db)
            label = (prefix.rstrip("_") if prefix else "") or "__default__"
            for table_key, df in load_sql_datasets(
                conn, dataset_key_prefix=prefix, max_rows=max_rows, db_connectors_by_dataset=db_connectors_by_dataset
            ).items():
                datasets[table_key] = df
                source_root_by_dataset[table_key] = (
                    f"__database__:{label}" if multi_db else "__database__"
                )
            db_seen += 1

        elif typ == "filesystem":
            fp = loc.get("path")
            if fp:
                root = os.path.abspath(os.path.normpath(fp))
                for fname, df in load_file_datasets(root, max_rows=max_rows).items():
                    key = fname
                    if key in datasets:
                        key = f"{os.path.basename(root.rstrip(os.sep))}__{fname}"
                    if key in datasets:
                        key = f"{hashlib.md5(root.encode('utf-8')).hexdigest()[:8]}__{fname}"
                    datasets[key] = df
                    source_root_by_dataset[key] = root

    if additional_data:
        for name, df in additional_data.items():
            datasets[name] = df
            norm = (name or "").replace("\\", "/")
            parent = os.path.dirname(norm).strip("/")
            source_root_by_dataset[name] = (
                f"azure_blob:{parent}" if parent else "azure_blob:"
            )

    metadata = {}
    for name, df in datasets.items():
        if job_id:
            from agent.jobs_store import add_event
            add_event(job_id=job_id, level="info", message=f"Profiling dataset: {name}")
        meta = profile_dataframe(df, job_id=job_id)
        try:
            from agent.specialists.ydata_profiler import enrich_assessment_with_profile
            meta = enrich_assessment_with_profile(df, meta)
        except ImportError:
            pass # ydata-profiling optional — graceful skip
        except Exception as e:
            logger.warning("ydata enrichment failed for %s: %s", name, e)

        # S4-02: SQL Server Pushdown — compute stats server-side when connector available
        if name in db_connectors_by_dataset:
            connector, table = db_connectors_by_dataset[name]

            # Pushdown aggregate stats (null counts, distinct counts, min/max)
            try:
                if hasattr(connector, 'compute_column_stats'):
                    pushdown_stats = connector.compute_column_stats(table)
                    if pushdown_stats.get("columns"):
                        for col_name, pstats in pushdown_stats["columns"].items():
                            if col_name in meta.get("columns", {}):
                                col_meta = meta["columns"][col_name]
                                # Use server-side null_percentage as authoritative
                                if "null_percentage" in pstats:
                                    col_meta["null_percentage"] = pstats["null_percentage"]
                                if "distinct_count" in pstats and pstats["distinct_count"] is not None:
                                    col_meta["unique_count"] = pstats["distinct_count"]
                                if "min_value" in pstats:
                                    col_meta["server_min"] = pstats["min_value"]
                                if "max_value" in pstats:
                                    col_meta["server_max"] = pstats["max_value"]
                                if "sql_data_type" in pstats:
                                    col_meta["sql_data_type"] = pstats["sql_data_type"]
                                # Update candidate_primary_key based on exact distinct count
                                if pushdown_stats.get("row_count") and pstats.get("distinct_count"):
                                    col_meta["candidate_primary_key"] = (
                                        pstats["distinct_count"] == pushdown_stats["row_count"]
                                        and pstats.get("null_count", 0) == 0
                                    )
                        if job_id:
                            from agent.jobs_store import add_event
                            add_event(job_id=job_id, level="info", message=f"SQL pushdown stats enriched for {name}")
            except Exception as e:
                if job_id:
                    from agent.jobs_store import add_event
                    add_event(job_id=job_id, level="warning", message=f"SQL pushdown stats failed for {name}: {e}")

            # Full database profiling (existing)
            try:
                db_prof = profile_database_table_full(connector, table, df, job_id=job_id)
                meta = merge_in_db_profile(meta, db_prof)
            except Exception as e:
                if job_id:
                    from agent.jobs_store import add_event
                    add_event(job_id=job_id, level="warning", message=f"Full database profiling failed for {name}: {e}")
                    
        meta["source_root"] = source_root_by_dataset.get(name, "")
        metadata[name] = meta

    if approved_semantics:
        import re
        def _norm_key(k: str) -> str:
            return re.sub(r"[^\w]+", "", str(k).lower())

        norm_metadata = {_norm_key(k): k for k in metadata.keys()}
        for name, table_sem in approved_semantics.items():
            norm_name = _norm_key(name)
            if norm_name in norm_metadata:
                meta = metadata[norm_metadata[norm_name]]
                norm_cols = {_norm_key(c): c for c in meta.get("columns", {})}
                for col, tag in table_sem.items():
                    norm_col = _norm_key(col)
                    if norm_col in norm_cols:
                        meta["columns"][norm_cols[norm_col]]["semantic_type"] = tag

    if len(datasets) >= 2:
        try:
            from agent.specialists.cross_dataset_agent import generate_sweetviz_comparison
            names = list(datasets.keys())
            generate_sweetviz_comparison(
                datasets[names[0]], datasets[names[1]], names[0], names[1]
            )
        except ImportError:
            pass
        except Exception as e:
            logger.warning("sweetviz comparison failed: %s", e)

    per_dataset_dq = {}
    ds_list = list(datasets.keys())
    for idx, name in enumerate(ds_list):
        df = datasets[name]
        if job_id:
            from agent.jobs_store import add_event
            add_event(job_id=job_id, level="info", message=f"Analyzing data quality: {name}")
        per_dataset_dq[name] = analyze_dataset_quality(name, df, metadata[name], thresholds, job_id=job_id, business_rules=business_rules)
        metadata[name]["quality"] = per_dataset_dq[name]
        if job_id:
            add_event(job_id=job_id, level="info", message=f"Quality check complete for {name}")
            try:
                from agent.jobs_store import update_job_progress
                pct = int(60 + ((idx + 1) / len(ds_list)) * 20)
                update_job_progress(job_id, pct)
            except Exception:
                pass

    # Apply custom rules from config and merge into per_dataset_dq
    custom_rules = (thresholds or {}).get("custom_rules") or []
    if isinstance(custom_rules, list):
        extra_issues = run_custom_rules(datasets, custom_rules)
        for ds_name, issues in extra_issues.items():
            if ds_name in per_dataset_dq:
                per_dataset_dq[ds_name]["issues"].extend(issues)
                per_dataset_dq[ds_name]["summary"]["issue_count"] = len(per_dataset_dq[ds_name]["issues"])
                per_dataset_dq[ds_name]["summary"]["medium_severity"] = sum(
                    1 for i in per_dataset_dq[ds_name]["issues"] if i.get("severity") == "medium"
                )
                per_dataset_dq[ds_name]["summary"]["high_severity"] = sum(
                    1 for i in per_dataset_dq[ds_name]["issues"] if i.get("severity") == "high"
                )

    rel_bundle = analyze_cross_dataset_relationships(datasets, metadata, thresholds)
    relationships = rel_bundle["relationships"]
    global_issues = detect_global_issues(datasets, thresholds)
    global_issues["relationship_row_issues"] = rel_bundle["relationship_row_issues"]
    global_issues["relationship_warnings"] = rel_bundle["relationship_warnings"]
    global_issues["cross_dataset_consistency"] = analyze_cross_dataset_consistency(datasets, metadata, thresholds)

    is_sampled = (max_rows is not None)
    for ds_name, block in per_dataset_dq.items():
        ds_sampled = is_sampled or (metadata.get(ds_name, {}).get("row_count", 0) > HEAVY_OPERATION_THRESHOLD)
        for iss in block.get("issues", []):
            iss.setdefault("dataset", ds_name)
            enrich_issue_with_recommendation(iss)
            enrich_issue_with_fixability(iss)
            if ds_sampled and iss.get("row_indexes"):
                iss["row_indexes_estimated"] = True
                if "estimated" not in str(iss.get("message")).lower():
                    iss["message"] = f"{iss['message']} (Row indexes are estimated based on a sampled subset of the data)"

    # Enrich global/cross-dataset issues
    try:
        for iss in (global_issues.get("relationship_row_issues") or []):
            enrich_issue_with_recommendation(iss)
            enrich_issue_with_fixability(iss)
            if is_sampled and iss.get("row_indexes"):
                iss["row_indexes_estimated"] = True
                if "estimated" not in str(iss.get("message")).lower():
                    iss["message"] = f"{iss['message']} (Row indexes are estimated based on a sampled subset of the data)"
        for iss in (global_issues.get("relationship_warnings") or []):
            enrich_issue_with_recommendation(iss)
            enrich_issue_with_fixability(iss)
            if is_sampled and iss.get("row_indexes"):
                iss["row_indexes_estimated"] = True
                if "estimated" not in str(iss.get("message")).lower():
                    iss["message"] = f"{iss['message']} (Row indexes are estimated based on a sampled subset of the data)"
        for iss in (global_issues.get("cross_dataset_consistency") or []):
            enrich_issue_with_recommendation(iss)
            enrich_issue_with_fixability(iss)
            if is_sampled and iss.get("row_indexes"):
                iss["row_indexes_estimated"] = True
                if "estimated" not in str(iss.get("message")).lower():
                    iss["message"] = f"{iss['message']} (Row indexes are estimated based on a sampled subset of the data)"
        for iss in (global_issues.get("cross_dataset_inconsistencies") or []):
            # these use issue_type, not type
            if isinstance(iss, dict) and iss.get("issue_type") and not iss.get("fixability"):
                iss["fixability"] = FIXABILITY_BY_ISSUE_TYPE.get(str(iss.get("issue_type")), "COMPLEX")
    except Exception:
        pass

    out = {
        "datasets": metadata,
        "relationships": relationships,
        "data_quality_issues": {
            "datasets": per_dataset_dq,
            "global_issues": global_issues
        },
        "executive_summary_items": build_executive_summary_items(per_dataset_dq, global_issues, thresholds),
    }

    # 1. Run LLM Schema Enrichment first
    try:
        from agent.llm_schema_enricher import enrich_assessment_with_schema_llm
        out = enrich_assessment_with_schema_llm(out)
    except Exception as e:
        logger.error(f"Enrichment error: {e}")

    # 2. Run the Pandas Confirmation Pass using the loaded dataframes
    for name, df in datasets.items():
        if name not in out["datasets"]:
            continue
        ds_meta = out["datasets"][name]
        
        # A. Business Key duplicate confirmation
        llm_ds_hints = ds_meta.setdefault("llm_hints", {})
        probable_pks = llm_ds_hints.get("probable_pk_columns") or []
        if probable_pks:
            dup_info = confirm_business_key_duplicates(df, probable_pks)
            llm_ds_hints["business_key_confirmation"] = dup_info
            
        # B. Date variant and Null patterns per column
        for col_name, col_meta in ds_meta.get("columns", {}).items():
            if col_name not in df.columns:
                continue
            hints = col_meta.setdefault("llm_hints", {})
            
            # Date check
            if hints.get("mixed_formats_suspected") or hints.get("semantic_type") == "date":
                fmt_vars = detect_date_format_variants(df[col_name])
                hints["format_variants"] = fmt_vars
                if len(fmt_vars) > 1:
                    hints["mixed_formats_suspected"] = True
                    
            # Null pattern check
            if col_meta.get("null_percentage", 0) > 0:
                null_pat = detect_null_pattern(df, col_name)
                hints["null_pattern"] = null_pat

    if job_id:
        try:
            from agent.jobs_store import update_job_progress
            update_job_progress(job_id, 100)
        except Exception:
            pass

    try:
        from agent.assessment_governance import enrich_assessment_with_governance

        out = enrich_assessment_with_governance(
            out,
            datasets,
            job_id=job_id,
            business_rules=business_rules,
        )
    except Exception as e:
        logger.warning("governance enrichment failed: %s", e)

    if return_datasets:
        datasets_temp = out.pop("_datasets", None)
        out = make_json_serializable(out)
        if datasets_temp is not None:
            out["_datasets"] = datasets_temp
        else:
            out["_datasets"] = datasets
    else:
        out = make_json_serializable(out)

    return out

