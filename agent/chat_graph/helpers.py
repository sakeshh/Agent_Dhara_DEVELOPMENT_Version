# Helper utilities for formatting and rendering responses.
from __future__ import annotations
import json
import time
from typing import Any, Dict, List, Optional, Tuple

import agent.chat_graph
from agent.chat_graph.state import ChatState
from agent.master_agent import load_sources_config
from agent.model_config import load_llm_config
from agent.openai_usage import usage_dict_from_response
from agent.session_store import add_experience, list_recent_experiences, SessionJSONEncoder

def load_session(*args, **kwargs):
    import agent.chat_graph
    func = getattr(agent.chat_graph, "load_session", None)
    if func and (not hasattr(func, "__code__") or func.__code__ != load_session.__code__):
        return func(*args, **kwargs)
    from agent.session_store import load_session as real_load
    return real_load(*args, **kwargs)

def save_session(*args, **kwargs):
    import agent.chat_graph
    func = getattr(agent.chat_graph, "save_session", None)
    if func and (not hasattr(func, "__code__") or func.__code__ != save_session.__code__):
        return func(*args, **kwargs)
    from agent.session_store import save_session as real_save
    return real_save(*args, **kwargs)

def _flow_options(*items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Options are consumed by the frontend to render buttons.
    Each option: {id, text, send}
    """
    out: List[Dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        if not it.get("text") or not it.get("send"):
            continue
        out.append({"id": str(it.get("id") or it["text"]), "text": str(it["text"]), "send": str(it["send"])})
    return out


def _prompt_choose_action() -> Dict[str, Any]:
    reply = "📌 Choose Action:\n1. View Data in Files\n2. Generate Report"
    return {
        "reply": reply,
        "payload": {
            "step": "action",
            "options": _flow_options(
                {"id": "view", "text": "👁️ View Data", "send": "view data"},
                {"id": "report", "text": "📑 Generate Report", "send": "generate report"},
                {"id": "back", "text": "🔙 Back", "send": "back"},
                {"id": "restart", "text": "✅ Restart", "send": "restart"},
            ),
        },
    }


def _real_first_location_index(source_root: Dict[str, Any], want_type: str) -> Optional[int]:
    locs = list(((source_root or {}).get("locations") or []))
    for i, loc in enumerate(locs):
        if str(loc.get("type") or "").lower() == want_type:
            return i
    return None



def _first_location_index(*args, **kwargs):
    import agent.chat_graph
    func = getattr(agent.chat_graph, "_first_location_index", None)
    if func and (not hasattr(func, "__code__") or func.__code__ != _first_location_index.__code__):
        return func(*args, **kwargs)
    return _real_first_location_index(*args, **kwargs)

def _render_report_markdown(result: Dict[str, Any]) -> Optional[str]:
    """
    Render a formal report as Markdown when report builder is available.
    """
    try:
        import main as _main  # type: ignore

        if hasattr(_main, "build_markdown_report"):
            return _main.build_markdown_report(result)  # type: ignore
    except Exception:
        return None
    return None


def _render_report_html(result: Dict[str, Any]) -> Optional[str]:
    """
    Render a formal report as HTML when the report builder is available.
    """
    try:
        import main as _main  # type: ignore

        if hasattr(_main, "build_html_report"):
            return _main.build_html_report(result)  # type: ignore
    except Exception:
        return None
    return None


def _override_source_root_for_datasets(result: Dict[str, Any], dataset_names: List[str], source_root: str) -> None:
    """
    The core engine tags any `additional_data` datasets as azure_blob:* by default.
    In some chat flows we pass DataFrames that originate from SQL or local filesystem, so we
    override `datasets[ds].source_root` here to reflect the real source used.
    """
    if not isinstance(result, dict):
        return
    ds = result.get("datasets")
    if not isinstance(ds, dict):
        return
    for name in dataset_names or []:
        meta = ds.get(name)
        if isinstance(meta, dict):
            meta["source_root"] = source_root


def _theme_wrap_html(*, title: str, body_html: str) -> str:
    """
    Wrap arbitrary HTML content in the same Theme 2 CSS as the main report.
    """
    import html as html_module
    from agent.report_html_themes import get_report_html_css

    css = get_report_html_css()
    return (
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n"
        "<meta charset=\"utf-8\"/>\n"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>\n"
        f"<title>{html_module.escape(str(title) if title else 'Details')}</title>\n"
        "<style>\n"
        + css
        + "\n</style>\n"
        "</head>\n<body>\n"
        + '<div class="wrap">'
        + f'<header class="masthead"><div class="tagline">AGENT DHARA</div><h1>{html_module.escape(str(title))}</h1></header>'
        + '<section id="details" class="datasets-section">'
        + body_html
        + "</section></div></body></html>"
    )


def _html_table(headers: List[str], rows: List[List[Any]]) -> str:
    import html as html_module

    thead = "".join(f"<th>{html_module.escape(str(h))}</th>" for h in headers)
    if not rows:
        return (
            "<div class='table-wrap'><table class='data-table'><thead><tr>"
            + thead
            + "</tr></thead><tbody><tr><td colspan='"
            + str(len(headers) or 1)
            + "' class='muted'>(none)</td></tr></tbody></table></div>"
        )
    body = []
    for r in rows:
        tds = "".join(f"<td>{html_module.escape('' if v is None else str(v))}</td>" for v in r)
        body.append("<tr>" + tds + "</tr>")
    return (
        "<div class='table-wrap'><table class='data-table'><thead><tr>"
        + thead
        + "</tr></thead><tbody>"
        + "".join(body)
        + "</tbody></table></div>"
    )


def _md_escape(text: Any) -> str:
    s = "" if text is None else str(text)
    return s.replace("|", "\\|").replace("\n", " ").strip()


def _make_validation(*, title: str, checks: List[Dict[str, Any]]) -> Dict[str, Any]:
    ok = True
    for c in checks or []:
        if not bool(c.get("ok", False)):
            ok = False
            break
    return {"title": title, "ok": ok, "checks": checks}


def _validate_schema_markdown(*, reply_md: str, schemas: Dict[str, Any], names: List[str]) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    shown = list(names[:10])
    checks.append(
        {
            "id": "files_in_payload",
            "ok": set(shown) == set((schemas or {}).keys()),
            "detail": f"payload.schemas has {len((schemas or {}).keys())} file(s); expected {len(shown)}.",
        }
    )
    for fname in shown:
        cols = (schemas or {}).get(fname) or []
        checks.append(
            {
                "id": f"schema_block_present::{fname}",
                "ok": f"### Schema — `{fname}`" in (reply_md or ""),
                "detail": f"Markdown contains schema section for `{fname}`.",
            }
        )
        if isinstance(cols, list) and len(cols) > 80:
            checks.append(
                {
                    "id": f"schema_truncation_notice::{fname}",
                    "ok": "_…(+".lower() in (reply_md or "").lower() and f"more columns" in (reply_md or "").lower(),
                    "detail": f"`{fname}` has {len(cols)} columns; markdown should show a truncation notice after first 80.",
                }
            )
    return _make_validation(title="Schema validation", checks=checks)


def _validate_metadata_markdown(*, reply_md: str, meta: Dict[str, Any], names: List[str]) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    shown = list(names[:15])
    checks.append(
        {
            "id": "files_in_payload",
            "ok": set(shown) == set((meta or {}).keys()),
            "detail": f"payload.metadata has {len((meta or {}).keys())} file(s); expected {len(shown)}.",
        }
    )
    for fname in shown:
        checks.append(
            {
                "id": f"metadata_row_present::{fname}",
                "ok": f"| `{fname}` |" in (reply_md or ""),
                "detail": f"Markdown table contains a row for `{fname}`.",
            }
        )
        m = (meta or {}).get(fname) or {}
        rows = m.get("rows")
        cols = m.get("columns")
        checks.append(
            {
                "id": f"metadata_values_present::{fname}",
                "ok": rows is not None or cols is not None,
                "detail": f"`{fname}` metadata rows={rows}, columns={cols}.",
            }
        )
    return _make_validation(title="Metadata validation", checks=checks)


def _validate_report_payload(*, report_md: str, result: Dict[str, Any]) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    datasets = (result or {}).get("datasets") or {}
    ds_names = list(datasets.keys()) if isinstance(datasets, dict) else []
    checks.append(
        {
            "id": "dataset_count",
            "ok": isinstance(datasets, dict),
            "detail": f"result.datasets count = {len(ds_names) if isinstance(datasets, dict) else 'n/a'}",
        }
    )
    missing = []
    for n in ds_names[:25]:
        if f"`{n}`" not in (report_md or ""):
            missing.append(n)
    checks.append(
        {
            "id": "dataset_names_in_markdown",
            "ok": len(missing) == 0,
            "detail": "All dataset names appear in report markdown." if not missing else f"Missing dataset names in markdown: {missing[:8]}",
        }
    )
    dq = (result or {}).get("data_quality_issues") or {}
    dq_ds = (dq.get("datasets") or {}) if isinstance(dq, dict) else {}
    # Basic sanity: if there are any DQ issues objects, markdown should include the "Top issues" section header.
    has_any_issues = False
    if isinstance(dq_ds, dict):
        for b in dq_ds.values():
            if isinstance(b, dict) and (b.get("issues") or []):
                has_any_issues = True
                break
    checks.append(
        {
            "id": "dq_section_present",
            "ok": (not has_any_issues) or ("Top issues" in (report_md or "")),
            "detail": "DQ issues exist -> report markdown includes issues section header.",
        }
    )
    return _make_validation(title="Report validation", checks=checks)


def _build_report_tables_markdown(result: Dict[str, Any]) -> str:
    """
    Build a presentable markdown report using tables wherever possible.
    This is used as an enhancement layer (or fallback) for chat reports.
    """
    if not isinstance(result, dict):
        return ""

    # Ensure dq_recommendations is populated in result
    if "dq_recommendations" not in result:
        try:
            from agent.dq_recommendations_agent import DQRecommendationsAgent, dq_recommendations_to_dict
            agent = DQRecommendationsAgent()
            merged_dq = result.get("data_quality_issues") or {}
            rec, _ = agent.recommend(merged_dq=merged_dq)
            result["dq_recommendations"] = dq_recommendations_to_dict(rec)
        except Exception:
            pass
    datasets = result.get("datasets") or {}
    dq = (result.get("data_quality_issues") or {}).get("datasets") or {}
    rels = result.get("relationships") or []

    parts: List[str] = []
    ds_names = list(datasets.keys()) if isinstance(datasets, dict) else []
    if len(ds_names) == 1:
        parts.append(f"## Assessment Report of `{_md_escape(ds_names[0])}`")
    else:
        parts.append("## Assessment Report")

    # Dataset summary table
    rows = []
    if isinstance(datasets, dict):
        for name, meta in datasets.items():
            meta = meta or {}
            nrows = meta.get("row_count")
            ncols = meta.get("column_count")
            src_root = meta.get("source_root") or ""
            if isinstance(src_root, str) and src_root.startswith("__database__"):
                # "__database__" or "__database__:label"
                label = src_root.split(":", 1)[1] if ":" in src_root else ""
                src = f"Azure SQL{f' ({label})' if label else ''}"
            elif isinstance(src_root, str) and src_root.startswith("azure_blob:"):
                prefix = src_root.split(":", 1)[1]
                src = f"Azure Blob{f' ({prefix})' if prefix else ''}"
            elif src_root:
                src = f"Filesystem ({src_root})"
            else:
                src = ""
            summ = (dq.get(name) or {}).get("summary") or {}
            issues = summ.get("issue_count")
            high = summ.get("high_severity")
            med = summ.get("medium_severity")
            low = summ.get("low_severity")
            rows.append(
                f"| `{_md_escape(name)}` | {_md_escape(src)} | {nrows if nrows is not None else ''} | {ncols if ncols is not None else ''} | {issues if issues is not None else 0} | {high if high is not None else 0} | {med if med is not None else 0} | {low if low is not None else 0} |"
            )
    # Sampling info
    for name, meta in datasets.items():
        si = (meta or {}).get("sampling_info")
        if si:
            parts.append(f"> [!NOTE]\n> **{_md_escape(name)}**: {si}")

    parts.append(
        "### Datasets (summary)\n\n"
        "| Dataset | Source | Rows | Cols | Issues | High | Med | Low |\n"
        "|---|---|---:|---:|---:|---:|---:|---:|\n"
        + ("\n".join(rows) if rows else "|  |  |  |  |  |  |  |  |")
    )

    # Columns table (per dataset) - mirrors the "columns:" bullets in your screenshot.
    if isinstance(datasets, dict) and datasets:
        parts.append("### Columns (per dataset)\n")
        for name, meta in datasets.items():
            meta = meta or {}
            cols = meta.get("columns") or {}
            if not isinstance(cols, dict) or not cols:
                continue
            lines = [
                "| Column | dtype | null% | unique | semantic type | candidate_pk |",
                "|---|---|---:|---:|---|:---:|",
            ]
            # keep stable order for readability
            for col_name in sorted(cols.keys(), key=lambda x: str(x).lower()):
                c = cols.get(col_name) or {}
                dtype = _md_escape(c.get("dtype"))
                nullp = c.get("null_percentage")
                nullp_txt = f"{round(100*float(nullp), 1)}%" if isinstance(nullp, (int, float)) else ""
                uq = c.get("unique_count")
                sem = _md_escape(c.get("semantic_type"))
                cand = c.get("candidate_primary_key")
                cand_txt = "✓" if cand is True else ("✗" if cand is False else "")
                lines.append(
                    f"| `{_md_escape(col_name)}` | `{dtype}` | {nullp_txt} | {uq if isinstance(uq, int) else ''} | `{sem}` | {cand_txt} |"
                )
            parts.append(f"#### `{_md_escape(name)}`\n\n" + "\n".join(lines))

    # Per-dataset issues (top N) as table
    if isinstance(dq, dict) and dq:
        parts.append("### Top issues (per dataset)")
        for name, block in dq.items():
            issues = (block or {}).get("issues") or []
            if not isinstance(issues, list) or not issues:
                continue
            # Show everything (no truncation) – user requested full tabular view.
            top = issues
            lines = [
                "| Severity | Type | Column | Count | Message | Recommendation |",
                "|:--:|---|---|---:|---|---|",
            ]
            for it in top:
                sev = _md_escape(it.get("severity"))
                typ = _md_escape(it.get("type"))
                col = _md_escape(it.get("column"))
                cnt = it.get("count")
                msg = _md_escape(it.get("message"))
                rec = _md_escape(it.get("recommendation"))
                if isinstance(cnt, int):
                    cnt_txt = str(cnt)
                elif isinstance(cnt, float):
                    # Keep readable (some rules may emit ratios)
                    cnt_txt = str(round(cnt, 4))
                elif cnt is None:
                    cnt_txt = "-"
                else:
                    cnt_txt = _md_escape(cnt)
                lines.append(
                    f"| {sev} | `{typ}` | `{col}` | {cnt_txt} | {_md_escape(msg)} | {_md_escape(rec)} |"
                )
            # No "…(+N more)" – show all rows.
            parts.append(f"#### `{_md_escape(name)}`\n\n" + "\n".join(lines))

    # Relationships table (engine emits dataset_a/column_a + dataset_b/column_b)
    rel_rows = []
    if isinstance(rels, list) and rels:
        for r in rels:
            rel_rows.append(
                f"| `{_md_escape(r.get('dataset_a'))}` | `{_md_escape(r.get('column_a'))}` | `{_md_escape(r.get('dataset_b'))}` | `{_md_escape(r.get('column_b'))}` | `{_md_escape(r.get('cardinality'))}` | {_md_escape(r.get('overlap_count'))} |"
            )
    parts.append(
        "### Relationships\n\n"
        "| Dataset A | Column A | Dataset B | Column B | Cardinality | Shared keys |\n"
        "|---|---|---|---|---|---:|\n"
        + ("\n".join(rel_rows) if rel_rows else "| _none_ |  | _none_ |  |  |  |")
    )

    # Global issues + relationship warnings in tables
    global_issues = (
        ((result.get("data_quality_issues") or {}).get("global_issues") or {})
        if isinstance(result.get("data_quality_issues"), dict)
        else {}
    )
    if isinstance(global_issues, dict) and global_issues:
        parts.append("### Global issues\n")
        # Relationship row issues (orphans) - engine uses a list of dicts
        row_issues = global_issues.get("relationship_row_issues") or []
        if isinstance(row_issues, list) and row_issues:
            gi_rows = []
            for it in row_issues:
                gi_rows.append(
                    "| "
                    + f"`{_md_escape(it.get('dataset'))}` | `{_md_escape(it.get('column'))}` | "
                    + f"`{_md_escape(it.get('related_dataset'))}` | `{_md_escape(it.get('related_column'))}` | "
                    + f"{_md_escape(it.get('count'))} |"
                )
            parts.append(
                "#### Cross-table row issues (orphan keys)\n\n"
                "| Child dataset | FK column | Parent dataset | Parent column | Rows affected |\n"
                "|---|---|---|---|---:|\n"
                + ("\n".join(gi_rows) if gi_rows else "| _none_ |  |  |  |  |")
            )
        else:
            parts.append("#### Cross-table row issues (orphan keys)\n\n- (none)")

        warnings = global_issues.get("relationship_warnings")
        if isinstance(warnings, list) and warnings:
            w_rows = []
            for w in warnings:
                if isinstance(w, dict):
                    w_rows.append(f"| {_md_escape(w.get('severity'))} | {_md_escape(w.get('message'))} |")
                else:
                    w_rows.append(f"|  | {_md_escape(w)} |")
            parts.append(
                "#### Relationship warnings\n\n"
                "| Severity | Warning |\n"
                "|---|---|\n"
                + "\n".join(w_rows)
            )
        else:
            parts.append("#### Relationship warnings\n\n- (none)")

    # --- Governance extensions (semantic, drift, reconciliation, GX summary) ---
    sem = (result.get("semantic_context") or {}) if isinstance(result, dict) else {}
    if isinstance(sem, dict) and sem.get("by_dataset"):
        parts.append("### Semantic summary")
        parts.append("| Metric | Value |\n|---|---|")
        parts.append(f"| Overall semantic confidence | `{_md_escape(sem.get('overall_semantic_confidence'))}` |")
        for ds_name, ctx in (sem.get("by_dataset") or {}).items():
            if not isinstance(ctx, dict):
                continue
            crit = ", ".join(f"`{_md_escape(c)}`" for c in (ctx.get("critical_columns") or [])[:12])
            keys = ", ".join(f"`{_md_escape(c)}`" for c in (ctx.get("likely_key_columns") or [])[:8])
            parts.append(f"#### `{_md_escape(ds_name)}`")
            parts.append(f"- **Critical columns:** {crit or '_(none)_'}")
            parts.append(f"- **Likely keys:** {keys or '_(none)_'}")
            terms = ctx.get("business_terms") or {}
            if isinstance(terms, dict) and terms:
                tlines = [f"| `{_md_escape(k)}` | {_md_escape(v)} |" for k, v in list(terms.items())[:20]]
                parts.append("| Column | Business term |\n|---|---|\n" + "\n".join(tlines))

    drift = (result.get("drift") or {}).get("by_dataset") or {}
    if isinstance(drift, dict) and drift:
        parts.append("### Drift summary (vs last snapshot)")
        drows = []
        for dn, dblock in drift.items():
            if not isinstance(dblock, dict):
                continue
            sigs = dblock.get("signals") or []
            drows.append(
                f"| `{_md_escape(dn)}` | {_md_escape(dblock.get('severity'))} | {len(sigs) if isinstance(sigs, list) else 0} |"
            )
        parts.append("| Dataset | Severity | #Signals |\n|---|---:|---:|\n" + ("\n".join(drows) if drows else "|  |  |  |"))

    da = (result.get("drift_analysis") or {}) if isinstance(result, dict) else {}
    if isinstance(da, dict) and da.get("per_dataset"):
        parts.append("### Drift analysis (rollup)")
        parts.append(
            f"- **Drift score:** `{_md_escape(da.get('drift_score'))}` · **worst:** `{_md_escape(da.get('worst_severity'))}` · **signals:** `{_md_escape(da.get('total_signal_count'))}`"
        )

    ra = (result.get("reconciliation_analysis") or {}) if isinstance(result, dict) else {}
    rbd = ra.get("by_dataset") or {}
    if isinstance(rbd, dict) and rbd:
        parts.append("### Reconciliation analysis (deltas)")
        for dn, block in rbd.items():
            if not isinstance(block, dict):
                continue
            d = block.get("deltas") or {}
            parts.append(
                f"- `{_md_escape(dn)}`: parsed_loss={_md_escape(d.get('source_to_parsed_loss'))}, "
                f"write_delta={_md_escape(d.get('parsed_to_written_loss'))}"
            )

    gi = ((result.get("data_quality_issues") or {}).get("global_issues") or {}) if isinstance(result, dict) else {}
    sup = gi.get("relationship_row_issues_supplemental") or []
    if isinstance(sup, list) and sup:
        parts.append("### Relationship integrity (supplemental)")
        for it in sup[:12]:
            if not isinstance(it, dict):
                continue
            parts.append(
                f"- `{_md_escape(it.get('dataset'))}`.{_md_escape(it.get('column'))} → "
                f"`{_md_escape(it.get('related_dataset'))}`.{_md_escape(it.get('related_column'))} — count={_md_escape(it.get('count'))}"
            )

    amb = (result.get("semantic_ambiguity") or {}) if isinstance(result, dict) else {}
    amb_cols = amb.get("columns") or []
    if amb_cols:
        parts.append("### Unresolved ambiguity (low type confidence)")
        for row in amb_cols[:12]:
            if isinstance(row, dict):
                parts.append(
                    f"- `{_md_escape(row.get('dataset'))}`.`{_md_escape(row.get('column'))}` — "
                    f"type_confidence={_md_escape(row.get('type_confidence'))}"
                )

    rec = (result.get("reconciliation") or {}).get("by_dataset") or {}
    if isinstance(rec, dict) and rec:
        parts.append("### Data movement audit (reconciliation)")
        rrows = []
        for dn, rb in rec.items():
            if not isinstance(rb, dict):
                continue
            st = rb.get("stages") or {}
            bal = rb.get("balanced")
            rrows.append(
                f"| `{_md_escape(dn)}` | {st.get('source', '')} | {st.get('parsed', '')} | {st.get('written', '')} | {_md_escape(bal)} |"
            )
        parts.append(
            "| Dataset | source | parsed | written | balanced |\n|---|---:|---:|---:|:---:|\n"
            + ("\n".join(rrows) if rrows else "|  |  |  |  |  |")
        )

    gx = result.get("gx_results") if isinstance(result, dict) else None
    if isinstance(gx, dict) and gx and "_error" not in gx:
        parts.append("### GX validation summary")
        grows = []
        for gn, gb in gx.items():
            if str(gn).startswith("_") or not isinstance(gb, dict):
                continue
            stats = gb.get("statistics") or {}
            grows.append(
                f"| `{_md_escape(gn)}` | {_md_escape(gb.get('success'))} | "
                f"{stats.get('successful_expectations', '')} / {stats.get('evaluated_expectations', '')} |"
            )
        parts.append("| Dataset | success | successful / evaluated |\n|---|---|---|\n" + ("\n".join(grows) if grows else "|  |  |  |"))

    etl_ready = result.get("etl_readiness") if isinstance(result, dict) else None
    if isinstance(etl_ready, dict):
        parts.append("### ETL readiness")
        parts.append(
            f"- **Score:** {etl_ready.get('score')} ({etl_ready.get('grade')})\n"
            f"- **Recommendation:** {_md_escape(etl_ready.get('etl_recommendation'))}"
        )

    # LLM Cleaning Recommendations table
    dq_recs = result.get("dq_recommendations") or {}
    recs_list = dq_recs.get("recommendations") or []
    if recs_list:
        parts.append("### LLM Cleaning Recommendations\n")
        lines = [
            "| Priority | Dataset | Column | Severity | Suggested Fix | Risk |",
            "|---|---|---|---|---|---|",
        ]
        for r in recs_list:
            priority = r.get("priority") or ""
            ds = _md_escape(r.get("dataset") or "")
            col = _md_escape(r.get("column") or "")
            sev = _md_escape(r.get("severity") or "")
            fix = _md_escape(r.get("suggested_fix") or "")
            risk = _md_escape(r.get("risk") or "")
            lines.append(f"| {priority} | `{ds}` | `{col}` | {sev} | {fix} | {risk} |")
        parts.append("\n".join(lines))

    parts.append("### What might still be missed")
    parts.append(
        "- Unmodelled business rules not captured in metadata manifest or cross-field rules.\n"
        "- Drift in tails of distributions when only moments/null/distinct are snapshotted.\n"
        "- Nested payload loss if raw JSON/XML is flattened without registry entries.\n"
        "- GX expectations are sampled on very large tables (see GX logs)."
    )

    return "\n\n".join([p for p in parts if p.strip()])


def _write_report_artifacts(
    *,
    result: Dict[str, Any],
    report_markdown: Optional[str] = None,
    report_html: Optional[str] = None,
    base_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Persist "fresh" report artifacts for the chat workflow.

    The CLI (`main.py --reports-dir`) writes `output/reports/report.*`, but the chat API historically
    returned reports without writing files. Users expect the output folder to update on each run.

    This function overwrites `report.json/.md/.html`.
    """
    import os
    from datetime import datetime, timezone

    if not isinstance(result, dict):
        return {}

    here = os.path.dirname(os.path.abspath(__file__))
    default_dir = os.path.abspath(os.path.join(here, "..", "output", "reports"))
    reports_dir = os.path.abspath(base_dir) if base_dir else default_dir
    os.makedirs(reports_dir, exist_ok=True)

    meta = result.setdefault("run_metadata", {}) if isinstance(result.get("run_metadata"), dict) or result.get("run_metadata") is None else {}
    if isinstance(meta, dict):
        meta["generated_at"] = datetime.now(timezone.utc).isoformat()
        # Intentionally do not include local/FS paths in the user-facing report payload.
        # This directory is internal project storage and should not be shown in the UI.

    json_bytes = json.dumps(result, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    paths = {"json": os.path.join(reports_dir, "report.json")}
    with open(paths["json"], "wb") as f:
        f.write(json_bytes)

    if report_markdown:
        md_path = os.path.join(reports_dir, "report.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(report_markdown)
        paths["md"] = md_path

    if report_html:
        html_path = os.path.join(reports_dir, "report.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(report_html)
        paths["html"] = html_path

    try:
        import sys as _sys

        _root = os.path.abspath(os.path.join(here, ".."))
        if _root not in _sys.path:
            _sys.path.insert(0, _root)
        import main as _main_mod

        _sc = _main_mod.build_dq_scorecard(result)
        _sug = result.get("transformation_suggestions") if isinstance(result.get("transformation_suggestions"), dict) else {}
        if not isinstance(_sug.get("suggested_transformations"), list) or (_sug.get("_error")):
            try:
                from agent.transformation_suggester import suggest_transformations

                _sug = suggest_transformations(result)
            except Exception:
                _sug = {"suggested_transformations": [], "summary": {}}
        _mf = _main_mod.build_cleaning_manifest(result, _sug, _sc)
        _mpath = os.path.join(reports_dir, "cleaning_manifest.json")
        with open(_mpath, "w", encoding="utf-8") as _mfh:
            json.dump(_mf, _mfh, indent=2, default=str)
        paths["cleaning_manifest"] = _mpath
    except Exception:
        pass

    return {"reports_dir": reports_dir, "paths": paths}


def _pick_single_active_dataset(ctx: Dict[str, Any]) -> Optional[str]:
    """
    Choose a single dataset key to answer follow-up DQ questions.
    Priority:
    - selected_table
    - exactly 1 selected_local_files
    - exactly 1 selected_blob_files
    - exactly 1 selected_tables
    - last_assessment_datasets if exactly 1
    """
    t = (ctx or {}).get("selected_table")
    if t:
        return str(t)

    for k in ("selected_local_files", "selected_blob_files", "selected_tables"):
        lst = (ctx or {}).get(k) or []
        if isinstance(lst, list) and len(lst) == 1:
            return str(lst[0])

    last_ds = (ctx or {}).get("last_assessment_datasets") or []
    if isinstance(last_ds, list) and len(last_ds) == 1:
        return str(last_ds[0])
    return None


def _dataset_null_percent_rank(prof: Any, top_shown: int = 50) -> Tuple[List[Tuple[str, float]], str]:
    """
    From one dataset profile, build sorted (column, null_fraction) pairs and a short Markdown block for chat.
    """
    cols = (prof.get("columns") or {}) if isinstance(prof, dict) else {}
    if not isinstance(cols, dict) or not cols:
        return [], "*(no column profile)*"

    null_cols: List[Tuple[str, float]] = []
    for col, meta in cols.items():
        if not isinstance(meta, dict):
            continue
        try:
            pct = float(meta.get("null_percentage") or 0.0)
        except Exception:
            pct = 0.0
        if pct > 0:
            null_cols.append((str(col), pct))
    null_cols.sort(key=lambda x: x[1], reverse=True)

    if not null_cols:
        return [], "✅ No null values detected (based on the last assessment sample)."

    top = null_cols[: top_shown if top_shown > 0 else len(null_cols)]
    lines = [f"- `{c}`: {round(p*100, 2)}%" for c, p in top]
    more = f"\n…(+{len(null_cols)-len(top)} more columns with nulls)" if len(null_cols) > len(top) else ""
    body = "\n".join(lines) + more
    return null_cols, body


def _assessment_signature(ctx: Dict[str, Any]) -> Dict[str, Any]:
    def _norm_list(x: Any) -> List[str]:
        if not isinstance(x, list):
            return []
        return sorted([str(v) for v in x if str(v).strip()])

    return {
        "selected_table": str(ctx.get("selected_table") or ""),
        "selected_tables": _norm_list(ctx.get("selected_tables")),
        "selected_blob_files": _norm_list(ctx.get("selected_blob_files")),
        "selected_local_files": _norm_list(ctx.get("selected_local_files")),
        "selected_db_location_index": int(ctx.get("selected_db_location_index") or 0),
        "selected_blob_location_index": int(ctx.get("selected_blob_location_index") or 0),
        "selected_fs_location_index": int(ctx.get("selected_fs_location_index") or 0),
        "local_files_root": str(ctx.get("local_files_root") or ""),
    }


def _router_assessment_hints(ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Compact hints from last_assessment_result for the NL router LLM (supervisor context).
    """
    raw = ctx.get("last_assessment_result")
    if not isinstance(raw, dict):
        return None
    ds_keys = raw.get("datasets") or {}
    datasets = list(ds_keys.keys())[:35] if isinstance(ds_keys, dict) else []

    rels_raw = raw.get("relationships") or []
    rels_list = rels_raw if isinstance(rels_raw, list) else []
    n_rels = len(rels_list)
    cards_seen: List[str] = []
    if rels_list:
        for rel in rels_list[:80]:
            if isinstance(rel, dict):
                c = str(rel.get("cardinality") or "").strip()
                if c:
                    cards_seen.append(c)
    cards_seen = sorted(set(cards_seen))[:10]

    type_counts: Dict[str, int] = {}
    dq_root = raw.get("data_quality_issues") or {}
    per = dq_root.get("datasets") if isinstance(dq_root, dict) else None
    if isinstance(per, dict):
        for _dsn, block in per.items():
            issues = (block or {}).get("issues") if isinstance(block, dict) else None
            if not isinstance(issues, list):
                continue
            for iss in issues:
                if isinstance(iss, dict):
                    t = str(iss.get("type") or "").strip()
                    if t:
                        type_counts[t] = type_counts.get(t, 0) + 1
    top_types = sorted(type_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:12]

    n_orphan = 0
    dq_root = raw.get("data_quality_issues") or {}
    gib = dq_root.get("global_issues") if isinstance(dq_root, dict) else None
    if isinstance(gib, dict):
        o = gib.get("orphan_foreign_keys")
        if isinstance(o, list):
            n_orphan = len(o)

    from agent.context_compressor import compress_assessment_for_llm
    compressed_view = compress_assessment_for_llm(raw)

    return {
        "has_cached_assessment": True,
        "dataset_names": datasets,
        "relationship_count": n_rels,
        "cardinality_labels_present": cards_seen,
        "top_data_quality_issue_types": [{"type": k, "occurrences_in_issue_list": v} for k, v in top_types],
        "orphan_foreign_key_hint_count": n_orphan,
        "compressed_assessment": compressed_view,
    }


def _ensure_latest_assessment(state: ChatState) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Ensure we have a fresh assessment for the current selection (single source, multiple datasets allowed).
    Returns (result, error_message).
    """
    ctx = state["session"].setdefault("context", {})
    sig = _assessment_signature(ctx)
    prev_sig = ctx.get("last_assessment_signature")
    prev = ctx.get("last_assessment_result")
    if isinstance(prev, dict) and isinstance(prev_sig, dict) and prev_sig == sig:
        return prev, None

    # Prefer explicit multi-selection
    if ctx.get("selected_tables") or ctx.get("selected_table"):
        if not ctx.get("selected_tables") and ctx.get("selected_table"):
            ctx["selected_tables"] = [str(ctx["selected_table"])]
        out = _node_assess_selected_tables(state)
        res = out.get("payload", {}).get("result")
        if isinstance(res, dict):
            ctx["last_assessment_signature"] = sig
            return res, None
        return None, out.get("reply") or "Failed to assess selected tables."

    if ctx.get("selected_blob_files"):
        out = _node_assess_selected_files(state)
        res = out.get("payload", {}).get("result")
        if isinstance(res, dict):
            ctx["last_assessment_signature"] = sig
            return res, None
        return None, out.get("reply") or "Failed to assess selected files."

    if ctx.get("selected_local_files"):
        out = _node_assess_selected_local_files(state)
        res = out.get("payload", {}).get("result")
        if isinstance(res, dict):
            ctx["last_assessment_signature"] = sig
            return res, None
        return None, out.get("reply") or "Failed to assess selected local files."

    return None, "No datasets selected. Select one or more tables/files first, then ask again."

def _user_asks_selection_status(raw: str) -> bool:
    """
    Intent: how many / what items are selected in session (no assessment, no DQ overview).
    Kept narrow to avoid clashing with 'how many nulls/issues in selected'.
    """
    r = (raw or "").strip().lower()
    if not r:
        return False
    if any(
        x in r
        for x in ("null", "missing", "duplicate", "data quality", "issue", "problem", "assessment")
    ):
        return False

    trivial = (
        "selection status",
        "selection count",
        "what is selected",
        "what's selected",
        "whats selected",
        "show selection",
        "show my selection",
        "show selected",
        "list selection",
        "list selected",
        "which files are selected",
        "which tables are selected",
        "have i selected anything",
        "anything selected",
    )
    if r in trivial:
        return True

    if ("what " in r or "which " in r or "tell me " in r) and ("selected" in r or "selection" in r):
        return True

    if ("how many" in r or "how much" in r) and (
        r.endswith(" selected")
        or " are selected" in r
        or " are currently selected" in r
        or "files selected" in r
        or "file selected" in r
        or "tables selected" in r
        or "table selected" in r
        or " of them selected" in r  # how many of them are selected
    ):
        return True

    return False


def _user_wants_narrative_report_summary(raw: str) -> bool:
    """Natural-language intents that should yield a prose + prioritized summary (not bare counts)."""
    r = (raw or "").strip().lower()
    if not r:
        return False
    exact = {
        "summarize the report",
        "summarize report",
        "report summary",
        "summary of the report",
        "summary report",
        "executive summary",
        "give me an executive summary",
        "give me a summary",
        "high level summary",
        "high-level summary",
        "tldr",
        "tl;dr",
        "what does this report say",
        "what does the report say",
        "explain the report",
        "explain this report",
        "brief me on the report",
    }
    if r in exact:
        return True
    if r.startswith("summarize ") and any(x in r for x in ("report", "findings", "assessment", "results")):
        return True
    if ("summary" in r or "summarize" in r) and any(x in r for x in ("report", "assessment", "finding", "findings")):
        return True
    return "in plain english" in r and ("report" in r or "assessment" in r)


def _user_asks_relationships_focus(raw: str) -> bool:
    """Follow-up intents about cardinality / joins / keys (not general executive summary)."""
    r = (raw or "").strip().lower()
    if not r:
        return False
    if _user_wants_narrative_report_summary(raw):
        return False
    # Avoid clashing with selection-only questions
    if _user_asks_selection_status(raw):
        return False
    rel_tokens = (
        "cardinality",
        "relationship",
        "relationships",
        "how are the",
        "how do the",
        "how are my",
        "how do my",
        "link between",
        "linked between",
        "linking",
        "foreign key",
        "foreign keys",
        "orphan key",
        "orphan keys",
        "orphan fk",
        "dangling key",
        "referential",
        "overlap count",
        "shared keys",
        "which tables link",
        "which files link",
        "datasets link",
        "join between",
        "join the tables",
        "joined",
    )
    if any(t in r for t in rel_tokens):
        return True
    if "join" in r and any(x in r for x in ("table", "file", "dataset", "data")):
        return True
    return False


def _truncate_summary_text(s: str, max_len: int) -> str:
    s = (s or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


def _markdown_narrative_assessment_summary(result: Dict[str, Any]) -> Optional[str]:
    """
    Readable summary: storyline + prioritized themes + sample issues + linkage + compact scorecard.
    Uses deterministic text (no LLM) from fields already emitted by the assessment pipeline.
    """
    if not isinstance(result, dict):
        return None
    dq = result.get("data_quality_issues") or {}
    if not isinstance(dq, dict):
        return None
    per = dq.get("datasets") or {}
    if not isinstance(per, dict) or not per:
        return None

    rows: List[Tuple[str, int, int, int, int]] = []
    total = {"issues": 0, "high": 0, "medium": 0, "low": 0}
    for ds_name, block in per.items():
        summ = (block or {}).get("summary") or {}
        try:
            ic = int(summ.get("issue_count") or 0)
            hi = int(summ.get("high_severity") or 0)
            me = int(summ.get("medium_severity") or 0)
            lo = int(summ.get("low_severity") or 0)
        except Exception:
            ic = hi = me = lo = 0
        total["issues"] += ic
        total["high"] += hi
        total["medium"] += me
        total["low"] += lo
        rows.append((str(ds_name), ic, hi, me, lo))
    rows.sort(key=lambda x: (x[2], x[1]), reverse=True)
    n_ds = len(rows)

    if total["high"] > 0:
        lead = (
            f"Across **{n_ds}** dataset(s), the scan raised **{total['issues']}** quality signals "
            f"**(High {total['high']}, Medium {total['medium']}, Low {total['low']})**. "
            f"Start with the **{total['high']} high-severity** items—these usually point to broken formats, missing keys, duplicates, or columns that block reliable joins."
        )
    elif total["medium"] > 0:
        lead = (
            f"The **{n_ds}** dataset(s) show **{total['issues']}** signals, mostly **medium/low** severity "
            f"(medium **{total['medium']}**, low **{total['low']}**). "
            "That pattern usually means clean-up work (normalization, trimming, type coercion) rather than structural failure."
        )
    else:
        lead = (
            f"**{n_ds}** dataset(s) report **{total['issues']}** low-severity observations in this sample—"
            "worth tidying, but nothing urgent from a severity standpoint."
        )

    parts: List[str] = ["### Report summary", "", lead, ""]

    exec_items = result.get("executive_summary_items") or []
    if isinstance(exec_items, list) and exec_items:
        parts.extend(["#### What to fix first (ranked themes)", ""])
        for it in exec_items[:7]:
            if not isinstance(it, dict):
                continue
            title = str(it.get("title") or "").strip() or "Issue"
            sev = str(it.get("severity") or "").strip()
            rec = _truncate_summary_text(str(it.get("recommendation") or ""), 220)
            line = f"- **{title}**"
            if sev:
                line += f" — *{sev}*"
            if rec:
                line += f"  \n  *Next step:* {rec}"
            parts.append(line)
        parts.append("")

    sev_rank = {"high": 3, "medium": 2, "low": 1}
    sample_lines: List[str] = []
    for ds_name, _ic, _hi, _me, _lo in rows[:12]:
        issues = (per.get(ds_name) or {}).get("issues") or []
        if not isinstance(issues, list) or not issues:
            continue
        scored = sorted(
            issues,
            key=lambda it: (
                sev_rank.get(str((it or {}).get("severity") or "low").lower(), 0),
                int((it or {}).get("count") or 0),
            ),
            reverse=True,
        )
        seen_types: set = set()
        for it in scored:
            if not isinstance(it, dict):
                continue
            typ = str(it.get("type") or "")
            if typ in seen_types:
                continue
            seen_types.add(typ)
            sev = str(it.get("severity") or "")
            col = it.get("column")
            msg = _truncate_summary_text(str(it.get("message") or it.get("detail") or ""), 140)
            col_s = f" — column `{col}`" if col else ""
            bit = f"- **`{ds_name}`** · {sev} · `{typ}`{col_s}"
            if msg:
                bit += f" — _{msg}_"
            sample_lines.append(bit)
            if len(seen_types) >= 2:
                break
    if sample_lines:
        parts.extend(["#### Concrete examples (one line each)", ""] + sample_lines[:14] + [""])

    rels = result.get("relationships") or []
    if isinstance(rels, list) and rels:
        parts.extend(["#### How the files relate", ""])
        for rel in rels[:10]:
            if not isinstance(rel, dict):
                continue
            a = rel.get("dataset_a") or rel.get("from") or "?"
            b = rel.get("dataset_b") or rel.get("to") or "?"
            ca = rel.get("column_a") or ""
            cb = rel.get("column_b") or ""
            card = rel.get("cardinality") or ""
            ov = rel.get("overlap_count")
            summ = str(rel.get("summary") or "").strip()
            ov_s = f", ~**{ov}** overlapping keys" if ov is not None else ""
            line = f"- `{a}` **{ca}** ↔ `{b}` **{cb}** — _{card}_{ov_s}._"
            if summ:
                line += f" {summ}"
            parts.append(line)
        parts.append("")

    parts.extend(
        [
            "#### Quick scorecard",
            "",
            "| Dataset | Issues | High | Med | Low |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for n, ic, hi, me, lo in rows[:25]:
        parts.append(f"| `{n}` | {ic} | {hi} | {me} | {lo} |")
    if len(rows) > 25:
        parts.append(f"| … | *+{len(rows) - 25} more datasets* |  |  |  |")
    parts.extend(
        [
            "",
            "---",
            "",
            "*Tip:* ask about **relationships / cardinality**, **duplicates**, **columns with nulls**, or **cleaning recommendations** to go deeper.",
        ]
    )
    return "\n".join(parts)


def _cardinality_glossary_line(label: str) -> str:
    """Short deterministic explanation for common cardinality strings (G: grounded prose)."""
    n = (label or "").strip().lower().replace(" ", "").replace("-", "_")
    if not n:
        return ""
    if "manytomany" in n or n.endswith("m_n") or ("many" in n and n.count("many") >= 2):
        return "Many rows on both sides can line up through the same key pattern; often modeled with a bridge entity in production schemas."
    if "onetomany" in n or n == "one_to_many" or "1tom" in n:
        return "One row on dataset A maps to potentially many matching rows on dataset B via the shared key column."
    if "manytoone" in n or n == "many_to_one":
        return "Many rows on A point to one matching row on B (same key value repeated on the A side)."
    if "onetoone" in n or n == "one_to_one":
        return "At most one row on each side aligns for a given key (1:1 in the sampled overlap)."
    if "unknown" in n:
        return "The engine could not infer a confident directional pattern from overlapping keys in the sample."
    return "Inferred from key overlap in your loaded sample—it is not a guarantee of database-enforced constraints."
def _real_load_sample_dfs_for_discovery(ctx: Dict[str, Any], selected: List[str]) -> Dict[str, Any]:
    import os
    import json
    import pandas as pd
    
    dfs = {}
    
    # 1. Database tables
    selected_tables = ctx.get("selected_tables") or []
    if selected_tables:
        sources_path = ctx.get("sources_path") or "config/sources.yaml"
        source_root = load_sources_config(sources_path)
        db_locs = [loc for loc in (source_root.get("locations") or []) if (loc.get("type") or "").lower() == "database"]
        if db_locs:
            db_idx = int(ctx.get("selected_db_location_index") or 0)
            db_idx = max(0, min(db_idx, len(db_locs) - 1))
            conn_cfg = db_locs[db_idx].get("connection") or {}
            from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector
            conn = AzureSQLPythonNetConnector(conn_cfg)
            for t in selected:
                if t in selected_tables:
                    dfs[t] = conn.load_table(t, max_rows=5)

    # 2. Local files
    selected_local = ctx.get("selected_local_files") or []
    local_root = ctx.get("local_files_root") or ""
    if selected_local and local_root:
        for name in selected:
            if name in selected_local:
                p = os.path.join(local_root, name)
                if os.path.isfile(p):
                    low = p.lower()
                    if low.endswith(".csv"):
                        dfs[name] = pd.read_csv(p, nrows=5, low_memory=False)
                    elif low.endswith(".tsv"):
                        dfs[name] = pd.read_csv(p, sep="\t", nrows=5, low_memory=False)
                    elif low.endswith(".jsonl"):
                        rows = []
                        with open(p, "r", encoding="utf-8") as f:
                            for line in f:
                                line = line.strip()
                                if not line:
                                    continue
                                try:
                                    rows.append(json.loads(line))
                                except Exception:
                                    rows.append({"value": line})
                                if len(rows) >= 5:
                                    break
                        dfs[name] = pd.json_normalize(rows, max_level=1) if rows else pd.DataFrame()
                    elif low.endswith((".xlsx", ".xls")):
                        dfs[name] = pd.read_excel(p, nrows=5)
                    elif low.endswith(".parquet"):
                        dfs[name] = pd.read_parquet(p).head(5)
                    else:
                        dfs[name] = pd.read_json(p).head(5)

    # 3. Azure Blob files
    selected_blob = ctx.get("selected_blob_files") or ctx.get("selected_files") or []
    if selected_blob:
        sources_path = ctx.get("sources_path") or "config/sources.yaml"
        source_root = load_sources_config(sources_path)
        blob_locs = _azure_blob_locations(source_root)
        if blob_locs:
            blob_loc_idx = int(ctx.get("selected_blob_location_index") or 0)
            blob_loc_idx = max(0, min(blob_loc_idx, len(blob_locs) - 1))
            from agent.mcp_clients import _single_location_config
            from agent.mcp_interface import load_selected_blob_datasets
            cfg_text = _single_location_config({"name": source_root.get("name") or "source"}, blob_locs[blob_loc_idx])
            blob_names_to_load = [name for name in selected if name in selected_blob]
            if blob_names_to_load:
                loaded = load_selected_blob_datasets(
                    cfg_text,
                    location_index=0,
                    blob_names=blob_names_to_load,
                    max_rows=5,
                    max_bytes=10_737_418_240,
                )
                for name, df in loaded.items():
                    dfs[name] = df.head(5)
                    
    return dfs

def _load_sample_dfs_for_discovery(*args, **kwargs):
    import agent.chat_graph
    func = getattr(agent.chat_graph, "_load_sample_dfs_for_discovery", None)
    if func and (not hasattr(func, "__code__") or func.__code__ != _load_sample_dfs_for_discovery.__code__):
        return func(*args, **kwargs)
    return _real_load_sample_dfs_for_discovery(*args, **kwargs)
