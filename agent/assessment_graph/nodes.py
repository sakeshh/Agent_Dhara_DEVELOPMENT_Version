"""
Nodes for the LangGraph data assessment sub-graph with checkpointing.
"""
from __future__ import annotations

import logging
import os
import hashlib
from typing import Any, Dict, List, Optional
import pandas as pd

from agent.assessment_graph.state import AssessmentState, DatasetState
from agent.profiling.data_loaders import load_file_datasets, load_sql_datasets
from agent.profiling.statistical_profiling import profile_dataframe, select_top_priority_columns
from agent.profiling.database_profiler import profile_database_table_full, merge_in_db_profile
from agent.profiling.dq_checks import (
    analyze_dataset_quality,
    run_custom_rules,
)
from agent.profiling.assessment_orchestrator import (
    analyze_cross_dataset_relationships,
    confirm_business_key_duplicates,
    detect_date_format_variants,
    detect_null_pattern,
    detect_global_issues,
)
from agent.assessment_governance import enrich_assessment_with_governance

logger = logging.getLogger("agent.assessment_graph.nodes")


def source_loader(state: AssessmentState) -> Dict[str, Any]:
    """Node 1: Load datasets from file/database/blob sources."""
    job_id = state.get("job_id")
    if job_id:
        from agent.jobs_store import add_event, load_checkpoint
        add_event(job_id=job_id, level="info", message="Assessment Node 1: Loading sources")
        cached = load_checkpoint(job_id, "source_loader")
        if cached:
            datasets = {name: pd.read_json(js, orient="split") for name, js in cached["datasets"].items()}
            return {"datasets": datasets, "metadata": cached["metadata"]}

    source_cfg = state["source_cfg"] or {}
    additional_data = state["additional_data"] or {}
    max_rows = state["max_rows"]
    db_connectors = state["db_connectors"] or {}

    datasets: Dict[str, pd.DataFrame] = {}
    db_connectors_by_dataset = {}
    source_root_by_dataset = {}

    locations = list(source_cfg.get("locations", []) or [])
    
    # Simple extraction logic similar to original load_and_profile
    db_seen = 0
    multi_db = len([l for l in locations if (l.get("type") or "").lower() == "database"]) > 1

    for loc in locations:
        typ = str(loc.get("type") or "").lower()
        if typ == "database":
            conn_cfg = loc.get("connection", {}) or {}
            for table_name, df in load_sql_datasets(conn_cfg, max_rows=max_rows).items():
                datasets[table_name] = df
                from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector
                db_connectors_by_dataset[table_name] = (AzureSQLPythonNetConnector(conn_cfg), table_name)
                label = (loc.get("id") or loc.get("label") or loc.get("name") or "").strip()
                source_root_by_dataset[table_name] = (
                    f"__database__:{label}" if label else ("__database__" if not multi_db else f"__database__:{db_seen}")
                )
            db_seen += 1
        elif typ == "filesystem":
            fp = loc.get("path")
            if fp:
                root = os.path.abspath(os.path.normpath(fp))
                for fname, df in load_file_datasets(root, max_rows=max_rows).items():
                    datasets[fname] = df
                    source_root_by_dataset[fname] = root

    if additional_data:
        for name, df in additional_data.items():
            datasets[name] = df
            if name in db_connectors:
                db_connectors_by_dataset[name] = (db_connectors[name], name)
            norm = (name or "").replace("\\", "/")
            parent = os.path.dirname(norm).strip("/")
            source_root_by_dataset[name] = f"azure_blob:{parent}" if parent else "azure_blob:"

    # Store loaded database connectors and sources back in the state context
    res = {
        "datasets": datasets,
        "metadata": {
            name: {
                "source_root": source_root_by_dataset.get(name, ""),
                "db_connector": db_connectors_by_dataset.get(name)
            } for name in datasets
        }
    }
    if job_id:
        from agent.jobs_store import save_checkpoint
        # db_connector is not serializable, pop it
        serializable_metadata = {}
        for name, meta in res["metadata"].items():
            meta_copy = dict(meta)
            if "db_connector" in meta_copy:
                meta_copy.pop("db_connector")
            serializable_metadata[name] = meta_copy

        save_checkpoint(job_id, "source_loader", {
            "datasets": {name: df.to_json(orient="split") for name, df in datasets.items()},
            "metadata": serializable_metadata
        })
    return res


def profiler(state: DatasetState) -> Dict[str, Any]:
    """Node 2: Statistical profiling on a single dataset (runs in parallel via Send)."""
    dataset_name = state["dataset_name"]
    df = state["df"]
    job_id = state["job_id"]

    if job_id:
        from agent.jobs_store import add_event, load_checkpoint
        add_event(job_id=job_id, level="info", message=f"Assessment Node 2: Profiling dataset {dataset_name}")
        cached = load_checkpoint(job_id, f"profiler:{dataset_name}")
        if cached:
            return cached

    meta = profile_dataframe(df, job_id=job_id)

    # Optional ydata profiler
    try:
        from agent.specialists.ydata_profiler import enrich_assessment_with_profile
        meta = enrich_assessment_with_profile(df, meta)
    except ImportError:
        pass
    except Exception as e:
        logger.warning("ydata enrichment failed for %s: %s", dataset_name, e)

    res = {"metadata": {dataset_name: meta}}
    if job_id:
        from agent.jobs_store import save_checkpoint
        save_checkpoint(job_id, f"profiler:{dataset_name}", res)
    return res


def db_pushdown_enricher(state: AssessmentState) -> Dict[str, Any]:
    """Node 3: Pushdown SQL server aggregate statistics for database sources."""
    metadata = dict(state["metadata"])
    job_id = state["job_id"]

    if job_id:
        from agent.jobs_store import load_checkpoint
        cached = load_checkpoint(job_id, "db_pushdown_enricher")
        if cached:
            return cached

    for name, df in state["datasets"].items():
        meta = metadata.get(name) or {}
        db_conn_info = meta.get("db_connector")
        if db_conn_info:
            connector, table = db_conn_info
            try:
                if hasattr(connector, 'compute_column_stats'):
                    pushdown_stats = connector.compute_column_stats(table)
                    if pushdown_stats.get("columns"):
                        for col_name, pstats in pushdown_stats["columns"].items():
                            if col_name in meta.get("columns", {}):
                                col_meta = meta["columns"][col_name]
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
                        if job_id:
                            from agent.jobs_store import add_event
                            add_event(job_id=job_id, level="info", message=f"SQL pushdown stats enriched for {name}")
            except Exception as e:
                logger.warning("SQL pushdown stats failed: %s", e)

            # Full db profiling
            try:
                db_prof = profile_database_table_full(connector, table, df, job_id=job_id)
                from agent.intelligent_data_assessment import merge_in_db_profile
                meta = merge_in_db_profile(meta, db_prof)
                metadata[name] = meta
            except Exception as e:
                logger.warning("Database table full profiling failed: %s", e)

    res = {"metadata": metadata}
    if job_id:
        from agent.jobs_store import save_checkpoint
        serializable_metadata = {}
        for name, m in metadata.items():
            m_copy = dict(m)
            if "db_connector" in m_copy:
                m_copy.pop("db_connector")
            serializable_metadata[name] = m_copy
        save_checkpoint(job_id, "db_pushdown_enricher", {"metadata": serializable_metadata})
    return res


def gx_validator(state: DatasetState) -> Dict[str, Any]:
    """Node 4: Execute Great Expectations suite check on a single dataset."""
    dataset_name = state["dataset_name"]
    df = state["df"]
    job_id = state["job_id"]
    run_gx = os.getenv("DHARA_RUN_GX", "").strip().lower() in ("1", "true", "yes")

    if job_id:
        from agent.jobs_store import load_checkpoint
        cached = load_checkpoint(job_id, f"gx_validator:{dataset_name}")
        if cached:
            return cached

    if not run_gx:
        res = {"gx_results": {dataset_name: {}}}
    else:
        try:
            from agent.gx_runner import run_suite
            val_res = run_suite(dataset_name, df)
            res = {"gx_results": {dataset_name: val_res}}
        except Exception as e:
            logger.warning("GX suite runner failed for %s: %s", dataset_name, e)
            res = {"gx_results": {dataset_name: {"_error": str(e)}}}

    if job_id:
        from agent.jobs_store import save_checkpoint
        save_checkpoint(job_id, f"gx_validator:{dataset_name}", res)
    return res


def custom_rules_runner(state: DatasetState) -> Dict[str, Any]:
    """Node 5: Evaluate custom assertions and custom thresholds."""
    dataset_name = state["dataset_name"]
    df = state["df"]
    thresholds = state["thresholds"] or {}
    business_rules = state["business_rules"] or {}
    job_id = state["job_id"]

    if job_id:
        from agent.jobs_store import load_checkpoint
        cached = load_checkpoint(job_id, f"custom_rules_runner:{dataset_name}")
        if cached:
            return cached

    # Basic data quality checks
    meta = profile_dataframe(df, job_id=job_id)
    dq_issues = analyze_dataset_quality(
        dataset_name,
        df,
        meta,
        thresholds,
        job_id=job_id,
        business_rules=business_rules,
    )

    # Custom rules evaluation
    custom_rules = thresholds.get("custom_rules") or []
    if isinstance(custom_rules, list):
        extra = run_custom_rules({dataset_name: df}, custom_rules)
        if dataset_name in extra:
            dq_issues["issues"].extend(extra[dataset_name])
            dq_issues["summary"]["issue_count"] = len(dq_issues["issues"])

    res = {"per_dataset_dq": {dataset_name: dq_issues}}
    if job_id:
        from agent.jobs_store import save_checkpoint
        save_checkpoint(job_id, f"custom_rules_runner:{dataset_name}", res)
    return res


def relationship_analyzer(state: AssessmentState) -> Dict[str, Any]:
    """Node 6: Run cross-dataset relationship mapping checks."""
    datasets = state["datasets"]
    thresholds = state["thresholds"] or {}
    job_id = state.get("job_id")

    if job_id:
        from agent.jobs_store import load_checkpoint
        cached = load_checkpoint(job_id, "relationship_analyzer")
        if cached:
            return cached

    rels = []
    if len(datasets) >= 2:
        try:
            rels = analyze_cross_dataset_relationships(datasets, thresholds.get("primary_keys"))
        except Exception as e:
            logger.warning("Cross dataset relationship analysis failed: %s", e)

    # Sweetviz visual comparisons
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
            logger.warning("Sweetviz comparison failed: %s", e)

    global_issues = detect_global_issues(datasets, thresholds)

    res = {
        "relationships": rels,
        "global_issues": global_issues,
    }
    if job_id:
        from agent.jobs_store import save_checkpoint
        save_checkpoint(job_id, "relationship_analyzer", res)
    return res


def llm_enricher(state: AssessmentState) -> Dict[str, Any]:
    """Node 7: Apply semantic models, confirmations, and date formats."""
    metadata = dict(state["metadata"])
    datasets = state["datasets"]
    approved_semantics = state["approved_semantics"]
    job_id = state.get("job_id")

    if job_id:
        from agent.jobs_store import load_checkpoint
        cached = load_checkpoint(job_id, "llm_enricher")
        if cached:
            return cached

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

    # Confirm business key duplicates and date format variants
    for name, df in datasets.items():
        meta = metadata.get(name) or {}
        probable_pks = [c for c, m in meta.get("columns", {}).items() if m.get("candidate_primary_key")]
        if probable_pks:
            meta["business_key_duplicates"] = confirm_business_key_duplicates(df, probable_pks)
        
        # Date variants and null patterns
        for col_name, col_meta in meta.get("columns", {}).items():
            if col_meta.get("data_type") == "datetime":
                col_meta["date_format_variants"] = detect_date_format_variants(df[col_name])
                col_meta["null_pattern"] = detect_null_pattern(df, col_name)

    res = {"metadata": metadata}
    if job_id:
        from agent.jobs_store import save_checkpoint
        save_checkpoint(job_id, "llm_enricher", res)
    return res


def report_assembler(state: AssessmentState) -> Dict[str, Any]:
    """Node 8: Assemble final metrics dictionary and apply governance rules."""
    metadata = dict(state["metadata"])
    per_dataset_dq = state["per_dataset_dq"] or {}
    job_id = state.get("job_id")

    if job_id:
        from agent.jobs_store import load_checkpoint
        cached = load_checkpoint(job_id, "report_assembler")
        if cached:
            return cached

    # Bind DQ results into metadata
    for name in metadata:
        metadata[name]["quality"] = per_dataset_dq.get(name) or {
            "issues": [],
            "summary": {"issue_count": 0, "high_severity": 0, "medium_severity": 0, "low_severity": 0}
        }
        # Clean up database connection reference before serializing
        if "db_connector" in metadata[name]:
            metadata[name].pop("db_connector")

    # Assemble raw assessment
    res = {
        "datasets": metadata,
        "relationships": state["relationships"] or [],
        "data_quality_issues": {
            "datasets": per_dataset_dq,
            "global_issues": state["global_issues"] or {}
        }
    }

    # Call governance enricher (drift, reconciliation etc.)
    enriched = enrich_assessment_with_governance(
        res,
        datasets=state["datasets"],
        job_id=state["job_id"],
        business_rules=state["business_rules"],
        run_gx=state["run_gx"],
    )

    res_out = {"result": enriched}
    if job_id:
        from agent.jobs_store import save_checkpoint
        save_checkpoint(job_id, "report_assembler", res_out)
    return res_out
