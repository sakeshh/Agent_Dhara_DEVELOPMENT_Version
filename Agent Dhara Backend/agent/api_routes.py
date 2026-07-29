# API routes and schemas for the MCP server.
from __future__ import annotations
import os
import json
import logging
import time
from typing import Any, Dict, List, Optional
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, Request, UploadFile, File, Form, Depends
from fastapi.responses import FileResponse, JSONResponse

from agent.logging_setup import setup_logging
from agent.security import InMemoryRateLimiter, client_ip, get_request_id, require_backend_token
from agent.jobs_store import create_job, fetch_events, fetch_job
from agent.jobs_worker import JobWorker
from agent.mcp_interface import (
    run_assessment,
    list_tables,
    process_stream_chunk,
    load_path,
    process_uploaded_file,
)
from agent.transformation_suggester import suggest_transformations
from agent.requirements_to_config import build_user_request_text, requirements_to_selected_sources

logger = logging.getLogger("mcp_server")

# Import report generation from service_layer and bootstrap
from agent.service_layer import (
    build_html_report as _build_html,
    build_markdown_report as _build_md
)
from agent.bootstrap import load_config as _load_config

router = APIRouter()

_assess_limiter = InMemoryRateLimiter(max_requests=10, window_seconds=60)
_plan_limiter = InMemoryRateLimiter(max_requests=30, window_seconds=60)
_generate_limiter = InMemoryRateLimiter(max_requests=20, window_seconds=60)
_execute_limiter = InMemoryRateLimiter(max_requests=5, window_seconds=60)

class ConfigText(BaseModel):
    config: str
    approved_semantics: Optional[Dict[str, Dict[str, str]]] = None


class StreamPayload(BaseModel):
    records: List[Dict[str, Any]]
    name: Optional[str] = "stream"


class PathPayload(BaseModel):
    path: str


class AssessPayload(BaseModel):
    """
    Frontend requirement payload.
    - sources: list of ids/types (same semantics as selected_sources in langgraph_orchestrator)
    - user_request: optional natural language query
    - requirements: optional structured requirement object (will be serialized into user_request for now)
    - sources_path: optional override; defaults to MCP_SOURCES_PATH or config/sources.yaml
    - do_transform: optional hint (future)
    """

    sources: Optional[List[str]] = None
    user_request: Optional[str] = None
    requirements: Optional[Dict[str, Any]] = None
    sources_path: Optional[str] = None
    do_transform: Optional[bool] = None
    approved_semantics: Optional[Dict[str, Dict[str, str]]] = None
    session_id: Optional[str] = "default"


class SemanticInferencePayload(BaseModel):
    sources_path: Optional[str] = None
    sources: Optional[List[str]] = None


class ChatPayload(BaseModel):
    session_id: Optional[str] = "default"
    message: str
    thread_id: Optional[str] = None
    resume_value: Optional[str] = None


class SessionContextPayload(BaseModel):
    session_id: str
    context: Dict[str, Any]


class EtlPlanPayload(BaseModel):
    session_id: str = "default"
    business_rules: Optional[Dict[str, Any]] = None
    assessment_result: Optional[Dict[str, Any]] = None
    engine: Optional[str] = "python"
    codegen_engine: Optional[str] = None
    sql_dialect: Optional[str] = "tsql"
    target_destination: Optional[str] = "dataframe_only"
    target_path: Optional[str] = None
    tenant_id: Optional[str] = "default"
    source_context: Optional[Dict[str, Any]] = None
    engine_user_override: Optional[bool] = False
    generation_mode: Optional[str] = "full"


class EtlConfirmPayload(BaseModel):
    session_id: str = "default"
    plan: Optional[Dict[str, Any]] = None


class EtlApplyManualResolutionsPayload(BaseModel):
    session_id: str = "default"
    plan: Optional[Dict[str, Any]] = None
    resolutions: List[Dict[str, Any]] = []


class EtlEnrichReviewOptionsPayload(BaseModel):
    session_id: str = "default"
    issue_type: str
    item: Dict[str, Any]


class EtlNonFixableResolutionsPayload(BaseModel):
    session_id: str = "default"
    resolutions: List[Dict[str, Any]] = []


class EtlPatchRegenPayload(BaseModel):
    session_id: str = "default"
    post_validation_report: Dict[str, Any] = {}


class EtlPreflightPayload(BaseModel):
    sql: str


class EtlGeneratePayload(BaseModel):
    session_id: str = "default"
    engine: Optional[str] = "python"
    sql_dialect: Optional[str] = "tsql"
    codegen_mode: Optional[str] = None  # template | llm | llm_then_template
    generation_mode: Optional[str] = "full"


class EtlDeployPayload(BaseModel):
    session_id: str = "default"


class EtlUpdateCodePayload(BaseModel):
    session_id: str
    code: str
    phase: str


class EtlExecutePayload(BaseModel):
    session_id: str
    approved: Optional[bool] = False
    dry_run: Optional[bool] = False
    timeout_s: Optional[int] = 600


class TestConnectionPayload(BaseModel):
    connection_string: Optional[str] = None


class ExecutionApprovalPayload(BaseModel):
    session_id: str
    approved: bool


class PipelineRunPayload(BaseModel):
    session_id: Optional[str] = "default"
    user_request: Optional[str] = ""
    sources_path: Optional[str] = None
    sources: Optional[List[str]] = None
    generation_mode: Optional[str] = "full"
    engine: Optional[str] = "python"
    business_rules: Optional[Dict[str, Any]] = None
    job_id: Optional[str] = ""




def _get_config_text(body_config: str) -> str:
    """Use request body config, or fall back to MCP_DEFAULT_CONFIG_PATH file if set and body empty."""
    if (body_config or "").strip():
        return body_config.strip()
    default_path = os.environ.get("MCP_DEFAULT_CONFIG_PATH")
    if default_path and os.path.isfile(default_path):
        with open(default_path, "r", encoding="utf-8") as f:
            return f.read()
    return body_config or "{}"




# We bind the JobWorker and endpoint paths to the router
_worker = None

def set_worker(w):
    global _worker
    _worker = w

# Register all routes on the router
@router.post("/run")
def api_run(cfg: ConfigText, _auth: None = Depends(require_backend_token), additional_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Run a full assessment. Config from body, or from file at MCP_DEFAULT_CONFIG_PATH if body empty."""
    config_text = _get_config_text(cfg.config)
    return run_assessment(config_text, additional_data=additional_data, approved_semantics=cfg.approved_semantics)


@router.post("/list_tables")
def api_list_tables(cfg: ConfigText, _auth: None = Depends(require_backend_token)) -> Dict[str, List[str]]:
    """List SQL tables. Config from body, or from MCP_DEFAULT_CONFIG_PATH if body empty."""
    config_text = _get_config_text(cfg.config)
    return {"tables": list_tables(config_text)}


_SCHEMA_CACHE: Dict[str, Any] = {
    "sources_path": None,
    "sources_mtime": None,
    "cached_at": None,
    "tables": None,
}


@router.get("/schema/tables")
def api_schema_tables(ttl_seconds: int = 30, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """
    Discover Azure SQL tables from the configured sources file.

    Intended for UI discovery: when a new table is added, the frontend can refresh
    and see it without editing sources.yaml.

    Caching:
    - In-memory cache with TTL
    - Auto-invalidates when sources.yaml mtime changes
    """
    sources_path = os.environ.get("MCP_SOURCES_PATH") or "config/sources.yaml"
    ttl = max(0, int(ttl_seconds))

    try:
        mtime = os.path.getmtime(sources_path) if os.path.isfile(sources_path) else None
    except Exception:
        mtime = None

    now = time.time()
    cached_at = _SCHEMA_CACHE.get("cached_at")
    same_file = _SCHEMA_CACHE.get("sources_path") == sources_path and _SCHEMA_CACHE.get("sources_mtime") == mtime
    fresh = isinstance(cached_at, (int, float)) and (now - float(cached_at) <= ttl)
    if same_file and fresh and isinstance(_SCHEMA_CACHE.get("tables"), list):
        return {
            "ok": True,
            "sources_path": sources_path,
            "cached": True,
            "tables": _SCHEMA_CACHE.get("tables"),
        }

    try:
        with open(sources_path, "r", encoding="utf-8") as f:
            config_text = f.read()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read sources config: {e}")

    try:
        tables = list_tables(config_text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to discover tables: {e}")

    _SCHEMA_CACHE.update(
        {
            "sources_path": sources_path,
            "sources_mtime": mtime,
            "cached_at": now,
            "tables": tables,
        }
    )

    return {"ok": True, "sources_path": sources_path, "cached": False, "tables": tables}


@router.post("/transform_suggest")
def api_transform_suggest(payload: Dict[str, Any], _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """
    Generate transformation suggestions from an existing assessment result.
    Payload:
      { "assessment_result": <dict returned by load_and_profile/run_assessment> }
    """
    ar = payload.get("assessment_result")
    if not isinstance(ar, dict):
        raise HTTPException(status_code=400, detail="assessment_result must be an object")
    try:
        return {"ok": True, "suggestions": suggest_transformations(ar)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/dq_recommend")
def api_dq_recommend(payload: Dict[str, Any], _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """
    Generate LLM-assisted cleaning recommendations from merged DQ issues.
    Payload:
      { "data_quality": <dict like load_and_profile()['data_quality_issues']>, "user_intent": "..." }
    """
    dq = payload.get("data_quality")
    if not isinstance(dq, dict):
        raise HTTPException(status_code=400, detail="data_quality must be an object")
    user_intent = str(payload.get("user_intent") or "")
    try:
        from agent.dq_recommendations_agent import DQRecommendationsAgent, dq_recommendations_to_dict

        agent = DQRecommendationsAgent()
        rec, _usage = agent.recommend(merged_dq=dq, user_intent=user_intent)
        return {"ok": True, "recommendations": dq_recommendations_to_dict(rec)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stream")
def api_stream(payload: StreamPayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    return process_stream_chunk(payload.records, name=payload.name or "stream")


@router.post("/load_path")
def api_load_path(payload: PathPayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """Load datasets from a filesystem path (returns dict of name -> dataframe; JSON serialization may omit raw data)."""
    data = load_path(payload.path)
    return {"datasets": list(data.keys()), "count": len(data)}


@router.get("/sources")
def api_sources(_auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """
    Return available source configurations from sources.yaml (no secrets redacted here; keep it internal).
    """
    sources_path = os.environ.get("MCP_SOURCES_PATH") or "config/sources.yaml"
    try:
        import main as _main
        cfg = _main.load_config(sources_path)
        source_cfg = cfg.get("source", cfg) or {}
        locations = source_cfg.get("locations", []) or []
        # Return only minimal location metadata
        out = []
        for idx, loc in enumerate(locations):
            out.append(
                {
                    "index": idx,
                    "id": loc.get("id") or loc.get("label") or loc.get("name") or None,
                    "type": loc.get("type"),
                }
            )
        return {"sources_path": sources_path, "location_count": len(out), "locations": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/assess")
def api_assess(payload: AssessPayload, request: Request, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """
    High-level orchestration endpoint:
    uses LangGraph orchestrator (route -> extract -> transform).
    """
    _assess_limiter.check(client_ip(request))
    from agent.langgraph_orchestrator import run_orchestrator

    sources_path = (
        (payload.sources_path or "").strip()
        or os.environ.get("MCP_SOURCES_PATH")
        or "config/sources.yaml"
    )
    selected_sources = payload.sources or requirements_to_selected_sources(payload.requirements)

    user_request = build_user_request_text(payload.user_request or "", payload.requirements)

    if not user_request:
        return {"error": "Provide user_request or requirements"}

    result = run_orchestrator(
        user_request=user_request,
        sources_path=sources_path,
        selected_sources=selected_sources,
        request_id=getattr(getattr(request, "state", None), "request_id", "") or "",
        approved_semantics=payload.approved_semantics,
        session_id=payload.session_id or "default",
    )
    
    session_id = payload.session_id or "default"
    from agent.session_store import load_session, save_session
    sess = load_session(session_id)
    sess["session_state"] = "assessed"
    sess.setdefault("context", {})["last_assessment_result"] = result
    save_session(sess)

    try:
        from agent.session_store import save_pipeline_run
        from agent.etl_readiness_scorer import compute_etl_readiness
        from agent.etl_handlers import _assessment_schema_signature
        datasets = list((result.get("datasets") or {}).keys())
        readiness = compute_etl_readiness(result)
        schema_hash = _assessment_schema_signature(result)
        save_pipeline_run(
            session_id=session_id,
            dataset_names=datasets,
            schema_hash=schema_hash,
            dq_score=readiness["score"],
            dq_issue_count=len(readiness["blockers"]) + len(readiness["warnings"]),
            etl_phase="assessed",
            notes=readiness["etl_recommendation"],
        )
    except Exception:
        pass   # never block assessment on memory write failure

    return {"ok": True, "result": result}



@router.post("/chat")
def api_chat(payload: ChatPayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """
    Conversational endpoint using 3-agent LangGraph chat workflow.
    """
    from agent.chat_graph import run_chat

    sid = (payload.session_id or "default").strip() or "default"
    tid = payload.thread_id or sid
    out = run_chat(
        session_id=sid,
        message=payload.message,
        thread_id=tid,
        resume_value=payload.resume_value
    )
    try:
        logger.info(
            "chat_routed",
            extra={
                "session_id": sid,
                "thread_id": tid,
                "message": (payload.message or "")[:200],
                "action": out.get("action"),
            },
        )
    except Exception:
        pass
    return {
        "ok": True,
        "reply": out.get("reply"),
        "payload": out.get("payload") or {},
        "session_id": sid,
        "thread_id": tid
    }


@router.get("/sessions")
def api_list_sessions(limit: int = 50, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    from agent.session_store import list_sessions

    return {"ok": True, "sessions": list_sessions(limit=limit)}


@router.get("/sessions/{session_id}")
def api_get_session(session_id: str, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    from agent.session_store import load_session

    return {"ok": True, "session": load_session(session_id)}


@router.post("/sessions/context")
def api_update_session_context(payload: SessionContextPayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """
    Merge arbitrary keys into a session's context.
    Used by the UI to persist uploaded report text and other user artifacts.
    """
    from agent.session_store import load_session, save_session

    sid = (payload.session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = sess.setdefault("context", {})
    if not isinstance(ctx, dict):
        ctx = {}
        sess["context"] = ctx
    for k, v in (payload.context or {}).items():
        ctx[str(k)] = v
    save_session(sess)
    return {"ok": True, "session_id": sid, "context_keys": list(ctx.keys())}


@router.post("/etl/infer-semantics")
def api_infer_semantics(payload: SemanticInferencePayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """
    Load sample data for the selected sources/tables and infer column semantics.
    Returns: {"ok": True, "semantics": { table_name: { col_name: tag } }}
    """
    from agent.specialists.semantic_infer_agent import SemanticInferAgent
    from agent.intelligent_data_assessment import load_sql_datasets, load_file_datasets, _sql_location_key_prefix
    from agent.mcp_interface import _parse_config_text
    import pandas as pd

    sources_path = (
        (payload.sources_path or "").strip()
        or os.environ.get("MCP_SOURCES_PATH")
        or "config/sources.yaml"
    )
    if not os.path.isfile(sources_path):
        raise HTTPException(status_code=404, detail=f"Sources config file not found: {sources_path}")

    try:
        with open(sources_path, "r", encoding="utf-8") as f:
            config_text = f.read()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read sources config: {e}")

    try:
        cfg = _parse_config_text(config_text)
        source_cfg = cfg.get("source", cfg) if isinstance(cfg, dict) else {}
        locations = list(source_cfg.get("locations", []) or [])
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse sources configuration: {e}")

    # Determine which locations to process
    locs = []
    if payload.sources:
        selected_set = {s.lower() for s in payload.sources}
        for loc in locations:
            lid = loc.get("id") or loc.get("label") or loc.get("name") or ""
            if lid.lower() in selected_set:
                locs.append(loc)
        # If no direct location matched, let's keep all database locations and filter tables later
        if not locs:
            locs = [l for l in locations if (l.get("type") or "").lower() == "database"]
    else:
        locs = locations

    datasets: Dict[str, pd.DataFrame] = {}
    db_seen = 0
    db_locs = [l for l in locs if (l.get("type") or "").lower() == "database"]
    multi_db = len(db_locs) > 1

    for loc in locs:
        typ = (loc.get("type") or "").lower()
        if typ == "database":
            conn = loc.get("connection", {}) or {}
            prefix = _sql_location_key_prefix(loc, conn, db_seen, multi_db)
            try:
                for table_key, df in load_sql_datasets(
                    conn, dataset_key_prefix=prefix, max_rows=5, only_tables=payload.sources
                ).items():
                    datasets[table_key] = df
            except Exception as e:
                # Log and skip single DB errors to avoid failing the whole endpoint
                logging.getLogger(__name__).warning(f"Failed to load sample from DB: {e}")
            db_seen += 1
        elif typ == "filesystem":
            fp = loc.get("path")
            if fp:
                root = os.path.abspath(os.path.normpath(fp))
                try:
                    for fname, df in load_file_datasets(root, max_rows=5, only_files=payload.sources).items():
                        key = fname
                        if key in datasets:
                            key = f"{os.path.basename(root.rstrip(os.sep))}__{fname}"
                        datasets[key] = df
                except Exception as e:
                    logging.getLogger(__name__).warning(f"Failed to load sample from filesystem: {e}")

    # If payload.sources is specified, filter datasets keys that match payload.sources
    if payload.sources:
        selected_set = {s.lower() for s in payload.sources}
        datasets = {k: v for k, v in datasets.items() if k.lower() in selected_set}

    if not datasets:
        return {"ok": True, "semantics": {}, "message": "No matching datasets found to infer semantics."}

    # Run semantic inference in parallel using a ThreadPoolExecutor
    from concurrent.futures import ThreadPoolExecutor
    agent = SemanticInferAgent()
    result_semantics = {}
    samples = {}

    def process_dataset(table_name, df):
        try:
            sem = agent.infer_semantics(table_name=table_name, df=df)
            smp = {
                col: [str(x) for x in df[col].dropna().head(3).tolist()]
                for col in df.columns
            }
            return table_name, sem, smp
        except Exception as e:
            logging.getLogger(__name__).error(f"Inference failed for {table_name}: {e}")
            return table_name, {}, {}

    with ThreadPoolExecutor(max_workers=min(len(datasets), 8)) as executor:
        results = list(executor.map(lambda item: process_dataset(item[0], item[1]), datasets.items()))

    for table_name, sem, smp in results:
        result_semantics[table_name] = sem
        samples[table_name] = smp

    return {"ok": True, "semantics": result_semantics, "samples": samples}


@router.post("/etl/enrich-semantics")
def api_enrich_semantics(payload: Dict[str, Any], _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """
    Enrich low-confidence columns.
    """
    from agent.etl_pipeline.semantic_llm_enricher import enrich_low_confidence_columns
    low_confidence_cols = payload.get("low_confidence_cols") or {}
    enriched = enrich_low_confidence_columns(low_confidence_cols)
    return {"ok": True, "enriched": enriched}


@router.post("/etl/plan")
def api_etl_plan(payload: EtlPlanPayload, request: Request, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """Build ETL plan from assessment + business rules; stores under session.context.etl_flow."""
    _plan_limiter.check(client_ip(request))
    from agent.etl_handlers import etl_plan_start

    return etl_plan_start(
        payload.session_id,
        payload.business_rules,
        assessment_result=payload.assessment_result,
        engine=payload.engine or "python",
        codegen_engine=payload.codegen_engine,
        sql_dialect=payload.sql_dialect or "tsql",
        target_destination=payload.target_destination or "dataframe_only",
        target_path=payload.target_path,
        tenant_id=payload.tenant_id or "default",
        source_context=payload.source_context,
        engine_user_override=bool(payload.engine_user_override),
        generation_mode=payload.generation_mode,
    )


@router.get("/etl/tenants")
def api_etl_tenants(_auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    from agent.etl_handlers import etl_list_tenants

    return etl_list_tenants()


@router.post("/etl/apply-manual-resolutions")
def api_etl_apply_manual_resolutions(payload: EtlApplyManualResolutionsPayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """Promote user-selected manual review resolutions into plan steps."""
    from agent.etl_handlers import etl_apply_manual_resolutions

    return etl_apply_manual_resolutions(
        payload.session_id,
        payload.resolutions or [],
        plan_override=payload.plan,
    )


@router.post("/etl/enrich-review-options")
def api_etl_enrich_review_options(payload: EtlEnrichReviewOptionsPayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """Enrich dynamic resolution options for an unmapped anomaly type in the plan using the LLM."""
    from agent.etl_pipeline.manual_review_catalog import get_dynamic_resolution_options, enrich_manual_review_item
    from agent.session_store import load_session, save_session
    import logging

    logger = logging.getLogger("agent.api_routes")
    opts = get_dynamic_resolution_options(payload.issue_type, payload.item, allow_llm_call=True)
    
    sid = (payload.session_id or "default").strip() or "default"
    try:
        sess = load_session(sid)
        if sess and "context" in sess and "etl_flow" in sess["context"]:
            flow = sess["context"]["etl_flow"]
            plan = flow.get("plan")
            if plan and "manual_review" in plan:
                updated_mr = []
                for mr in plan["manual_review"]:
                    if mr.get("issue_type") == payload.issue_type and mr.get("column") == payload.item.get("column") and mr.get("dataset") == payload.item.get("dataset"):
                        mr["resolution_options"] = opts
                        mr = enrich_manual_review_item(mr)
                    updated_mr.append(mr)
                plan["manual_review"] = updated_mr
                flow["plan"] = plan
                save_session(sess)
    except Exception as e:
        logger.warning(f"Failed to update session plan with enriched options: {e}")
        
    return {"ok": True, "options": opts}


@router.post("/etl/non-fixable-resolutions")
def api_etl_non_fixable_resolutions(payload: EtlNonFixableResolutionsPayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """Accept user triage decisions for non-fixable issues."""
    from agent.etl_handlers import etl_save_non_fixable_resolutions

    return etl_save_non_fixable_resolutions(
        payload.session_id,
        payload.resolutions or [],
    )


@router.post("/etl/patch-regen")
def api_etl_patch_regen(payload: EtlPatchRegenPayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """Trigger ETL regen from post-ETL regression report."""
    from agent.etl_handlers import etl_patch_regen_code

    return etl_patch_regen_code(
        payload.session_id,
        payload.post_validation_report or {},
    )


@router.post("/etl/preflight")
def api_etl_preflight(payload: EtlPreflightPayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """Run SQL preflight validation checks without executing."""
    from agent.sql_preflight import run_sql_preflight

    res = run_sql_preflight(payload.sql)
    return {"ok": res.get("passed", True), "preflight": res}



@router.post("/etl/confirm")
def api_etl_confirm(payload: EtlConfirmPayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """Confirm (optionally edited) plan and compute impact preview."""
    from agent.etl_handlers import etl_confirm_plan

    return etl_confirm_plan(payload.session_id, plan_override=payload.plan)


@router.post("/etl/generate")
def api_etl_generate(payload: EtlGeneratePayload, request: Request, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """Generate ETL from approved plan; LLM + template fallback with validation."""
    _generate_limiter.check(client_ip(request))
    from agent.etl_handlers import etl_generate_code

    result = etl_generate_code(
        payload.session_id,
        engine=payload.engine or "python",
        sql_dialect=payload.sql_dialect or "tsql",
        codegen_mode=payload.codegen_mode,
        generation_mode=payload.generation_mode,
    )
    if not result.get("ok") and result.get("http_status") == 409:
        raise HTTPException(status_code=409, detail=result)
    return result


class GenerateEtlRequest(BaseModel):
    plan: Dict[str, Any]
    assessment: Dict[str, Any]
    engine: Optional[str] = "python"
    sql_dialect: Optional[str] = "tsql"
    output_mode: Optional[str] = "dataframe_only"
    output_path: Optional[str] = None
    validation_errors: Optional[List[str]] = None


@router.post("/generate-etl")
async def generate_etl_endpoint(payload: GenerateEtlRequest, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    from agent.etl_pipeline.llm_codegen import generate_etl_with_llm
    from agent.errors import ConnectorConfigError
    try:
        code, err = await generate_etl_with_llm(
            payload.plan,
            payload.assessment,
            engine=payload.engine or "python",
            sql_dialect=payload.sql_dialect or "tsql",
            output_mode=payload.output_mode or "dataframe_only",
            output_path=payload.output_path,
            validation_errors=payload.validation_errors,
        )
        if err:
            raise HTTPException(status_code=400, detail=err)
        return {"ok": True, "code": code}
    except ConnectorConfigError as e:
        raise HTTPException(status_code=422, detail=str(e))



@router.post("/etl/deploy")
def api_etl_deploy(payload: EtlDeployPayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    from agent.etl_handlers import etl_deploy
    return etl_deploy(payload.session_id)


@router.get("/etl/dq-gate")
def api_etl_dq_gate(session_id: str, dataset: str, threshold: float = 70.0, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """Check dataset against the DQ Gate threshold."""
    from agent.session_store import load_session
    from agent.etl_pipeline.dq_gate import check_dq_gate

    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = sess.setdefault("context", {})
    assess = ctx.get("last_assessment_result")
    if not assess:
        raise HTTPException(status_code=400, detail="No assessment found for session")

    try:
        res = check_dq_gate(assess, dataset, threshold=threshold)
        return {"ok": True, "gate": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/etl/phases")
def api_etl_phases(session_id: str, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """Retrieve split cleanse and transform plans for visual/phase routing."""
    from agent.session_store import load_session
    from agent.etl_pipeline.phase_classifier import split_plan_phases

    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = sess.setdefault("context", {})
    flow = ctx.get("etl_flow") or {}
    plan = flow.get("plan")
    if not plan:
        raise HTTPException(status_code=400, detail="No plan found for session")

    try:
        cleanse_plan, transform_plan = split_plan_phases(plan)
        return {
            "ok": True,
            "cleanse_plan": cleanse_plan,
            "transform_plan": transform_plan,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/etl/plan-coverage")
def api_etl_plan_coverage(session_id: str, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """Retrieve coverage report for the session's ETL plan."""
    from agent.session_store import load_session
    from agent.etl_pipeline.plan_coverage_report import build_coverage_report

    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = sess.setdefault("context", {})
    assess = ctx.get("last_assessment_result")
    flow = ctx.get("etl_flow") or {}
    plan = flow.get("plan")
    
    if not plan or not assess:
        raise HTTPException(status_code=400, detail="Plan or assessment result not found for session")

    try:
        report = build_coverage_report(assess, plan)
        return {"ok": True, "coverage_report": report}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



def _etl_safe_segment(s: str) -> str:
    import re

    t = re.sub(r"[^a-zA-Z0-9_-]+", "_", (s or "default").strip())[:80]
    return t or "default"


@router.get("/etl/lineage")
def api_etl_lineage(session_id: str, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """Column lineage map (source → transforms → target) after plan confirm."""
    from agent.etl_handlers import etl_get_lineage

    return etl_get_lineage(session_id)


@router.get("/etl/download/{plan_id}")
def api_etl_download_by_plan_id(plan_id: str, _auth: None = Depends(require_backend_token)):
    """Download ETL artifact by plan_id (path-traversal safe)."""
    from agent.session_store import load_session, save_session

    base_dir = os.environ.get(
        "DHARA_ETL_OUTPUT_DIR",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", "etl_code"),
    )
    safe_pid = _etl_safe_segment(plan_id)
    file_path = os.path.join(base_dir, f"{safe_pid}.py")

    real_base = os.path.realpath(base_dir)
    real_file = os.path.realpath(file_path)
    if not real_file.startswith(real_base):
        raise HTTPException(status_code=403, detail={"error": "PATH_TRAVERSAL_BLOCKED"})

    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404,
            detail={"error": "ETL_NOT_FOUND", "message": "ETL code not found"},
        )

    return FileResponse(file_path, filename=f"etl_{safe_pid}.py", media_type="application/octet-stream")


@router.get("/etl/download")
def api_etl_download(session_id: str, _auth: None = Depends(require_backend_token)):
    """Download validated ETL artifact for a session (path-traversal safe)."""
    from agent.session_store import load_session

    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    flow = (sess.get("context") or {}).get("etl_flow") or {}

    if not flow.get("validation_ok"):
        raise HTTPException(
            status_code=409,
            detail={
                "error": "CODE_NOT_VALIDATED",
                "message": "Code did not pass validation. Download blocked.",
                "is_draft": flow.get("is_draft", False),
                "validation_errors": flow.get("validation_errors") or [],
            },
        )

    rel_path = flow.get("artifact_rel_path")
    if not rel_path:
        raise HTTPException(
            status_code=404,
            detail={"error": "NO_ARTIFACT", "message": "No code artifact. Run POST /etl/generate first."},
        )

    root = os.path.dirname(os.path.abspath(__file__))
    safe_root = os.path.realpath(os.path.join(root, "output", "etl_code"))
    abs_path = os.path.realpath(os.path.join(root, rel_path))

    if not abs_path.startswith(safe_root):
        raise HTTPException(status_code=403, detail={"error": "PATH_TRAVERSAL_BLOCKED"})

    if _etl_safe_segment(sid) not in abs_path and sid not in abs_path:
        raise HTTPException(status_code=403, detail={"error": "SESSION_MISMATCH"})

    if not os.path.exists(abs_path):
        raise HTTPException(
            status_code=404,
            detail={"error": "FILE_NOT_FOUND", "message": "Artifact file missing from disk."},
        )

    from agent.etl_handlers import _can_transition, _transition

    if _can_transition(flow.get("phase", "code_ready"), "downloadable"):
        try:
            _transition(flow, "downloadable", by="user", reason="download_requested")
            save_session(sess)
        except ValueError:
            pass

    return FileResponse(
        abs_path,
        filename=os.path.basename(abs_path),
        media_type="application/octet-stream",
    )


def _job_public_view(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Omit bulky fields from GET /jobs/:id so Next.js (and other proxies) do not time out.
    Chat jobs store the full `messages` array in input and historically returned full `session` in result.
    """
    out = dict(job)
    kind = str(out.get("kind") or "")
    inp = out.get("input")
    if kind == "chat" and isinstance(inp, dict):
        out["input"] = {
            "session_id": inp.get("session_id"),
            "threadId": inp.get("threadId"),
            "message": inp.get("message"),
            "gx_enabled": inp.get("gx_enabled"),
        }
    res = out.get("result")
    if isinstance(res, dict) and "session" in res:
        out["result"] = {k: v for k, v in res.items() if k != "session"}
    return out


@router.post("/jobs")
def api_create_job(payload: Dict[str, Any], request: Request, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """
    Create an async job (assess/chat). Returns job_id immediately.
    Body:
      { "kind": "assess"|"chat", "input": {...} }
    """
    kind = str(payload.get("kind") or "").strip()
    inp = payload.get("input") if isinstance(payload.get("input"), dict) else {}
    if kind not in ("assess", "chat"):
        raise HTTPException(status_code=400, detail="Invalid kind")
    job_id = create_job(kind=kind, input=inp)
    return {"ok": True, "job_id": job_id}


@router.get("/jobs/{job_id}")
def api_get_job(job_id: str, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    j = fetch_job(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="Job not found")
    v = _job_public_view(j)
    return {
        "ok": True,
        "job": v,
        "status": v.get("status"),
        "result": v.get("result"),
        "error": v.get("error"),
    }


@router.get("/etl/assessment/status/{job_id}")
def api_etl_assessment_status(job_id: str, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    j = fetch_job(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="Job not found")
    status = j.get("status")
    if status == "succeeded":
        status = "completed"
    return {
        "ok": True,
        "job_id": job_id,
        "status": status,
        "progress": j.get("progress", 0),
        "error": j.get("error"),
        "result": j.get("result") if status == "completed" else None
    }


@router.get("/jobs/{job_id}/events")
def api_get_job_events(job_id: str, after_id: int = 0, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    # Simple polling endpoint; SSE can be added on top.
    ev = fetch_events(job_id, after_id=int(after_id), limit=200)
    return {"ok": True, "events": ev}


@router.post("/upload")
async def api_upload(
    file: UploadFile = File(...),
    request: Request = None,
    _auth: None = Depends(require_backend_token),
) -> Dict[str, Any]:
    """Accept a file upload and run the assessment. Query param format=html|md returns rendered report."""
    contents = await file.read()
    try:
        result = process_uploaded_file(contents, file.filename)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    fmt = (request.query_params.get("format", "").lower() if request else None) or ""
    if fmt == "html" and _build_html:
        return {"report": _build_html(result)}
    if fmt == "md" and _build_md:
        return {"report": _build_md(result)}
    return {"result": result}


@router.post("/business-rules/parse")
async def api_parse_business_rules(
    session_id: str = Form("default"),
    text: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    _auth: None = Depends(require_backend_token),
) -> Dict[str, Any]:
    """Accept text prompt and/or requirements file, parse them using LLM, and save to session."""
    file_bytes = await file.read() if file else None
    extracted = ""
    if file_bytes:
        from agent.business_requirements_parser import extract_text_from_file
        try:
            extracted = extract_text_from_file(file_bytes, file.filename)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to parse file: {str(e)}")

    combined_text = "\n".join(x for x in (text, extracted) if x)
    if not combined_text.strip():
        raise HTTPException(status_code=400, detail="Provide either business requirements text or upload a document file.")

    from agent.business_requirements_parser import get_dataset_schemas, parse_requirements_to_rules
    try:
        schemas = get_dataset_schemas(session_id)
        rules = parse_requirements_to_rules(combined_text, schemas)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse requirements to rules: {str(e)}")

    from agent.session_store import load_session, save_session
    try:
        sess = load_session(session_id)
        sess.setdefault("context", {})["pending_business_rules"] = rules
        save_session(sess)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update session context: {str(e)}")

    return {"ok": True, "rules": rules, "combined_text": combined_text}


@router.post("/etl/execute")
def api_etl_execute(payload: EtlExecutePayload, request: Request, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    _execute_limiter.check(client_ip(request))
    from agent.jobs_store import create_job
    job_id = create_job(
        kind="etl_execute",
        input={
            "session_id": payload.session_id,
            "approved": bool(payload.approved),
            "dry_run": bool(payload.dry_run),
            "timeout_s": payload.timeout_s or 240,
        }
    )
    return {"ok": True, "job_id": job_id, "status": "queued"}


@router.post("/etl/update-code")
def api_etl_update_code(payload: EtlUpdateCodePayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    from agent.session_store import load_session, save_session
    sid = (payload.session_id or "default").strip() or "default"
    sess = load_session(sid)
    ctx = sess.setdefault("context", {})
    flow = ctx.setdefault("etl_flow", {})

    eng = (flow.get("target_engine") or "sql").lower()

    # Save code to specific phase
    phase = str(payload.phase).strip().lower()
    if phase == "phase1":
        flow["code_cleanse"] = payload.code
    elif phase == "phase2":
        flow["code_transform"] = payload.code

    # Recombine
    cleanse_code = flow.get("code_cleanse") or ""
    transform_code = flow.get("code_transform") or ""

    if eng in ("python", "pyspark", "spark"):
        combined = payload.code
    # If both phases contain the same code (e.g. full-mode stored as cleanse),
    # don't concatenate — just use one copy.
    elif cleanse_code and transform_code and cleanse_code.strip() == transform_code.strip():
        combined = cleanse_code
    elif eng in ("sql", "tsql", "ansi"):
        if cleanse_code and transform_code:
            combined = cleanse_code + "\nGO\n\n" + transform_code
        else:
            combined = cleanse_code or transform_code
    else:
        if cleanse_code and transform_code:
            combined = cleanse_code + "\n\n# ============================================================\n# Phase 2: Transform\n# ============================================================\n\n" + transform_code
        else:
            combined = cleanse_code or transform_code

    flow["code"] = combined

    # Write to physical file if it exists
    rel_path = flow.get("artifact_rel_path")
    if rel_path:
        try:
            root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            abs_path = os.path.join(root, rel_path)
            if os.path.exists(abs_path):
                with open(abs_path, "w", encoding="utf-8") as f:
                    f.write(combined)
        except Exception as e:
            import logging
            logging.getLogger("mcp_server").warning(f"Failed to write updated code to file: {e}")

    save_session(sess)
    return {"ok": True, "session_id": sid, "message": "Code updated successfully"}


@router.post("/etl/run-full")
def api_etl_run_full(payload: EtlPlanPayload, request: Request, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """Single-call ETL: plan → confirm → generate in one graph invocation."""
    _plan_limiter.check(client_ip(request))
    from agent.etl_graph import run_etl_graph
    state = run_etl_graph(
        session_id=payload.session_id or "default",
        generation_mode=payload.generation_mode or "full",
        engine=payload.engine or "python",
        sql_dialect=payload.sql_dialect or "tsql",
        business_rules=payload.business_rules or {},
        assessment_result=payload.assessment_result,
    )
    return {"ok": bool(state.get("ok")), "state": state}



@router.get("/etl/execution-status/{session_id}")
def api_etl_execution_status_route(session_id: str, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    from agent.session_store import load_session
    sid = (session_id or "default").strip() or "default"
    sess = load_session(sid)
    flow = sess.get("context", {}).get("etl_flow", {})
    res = flow.get("sql_execution_result")
    if not res:
        raise HTTPException(
            status_code=404,
            detail={"error": "NOT_FOUND", "message": "No execution results found for this session."}
        )
    return res


@router.post("/etl/test-connection")
def api_etl_test_connection(payload: TestConnectionPayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    from agent.azure_sql_executor import test_connection
    return test_connection(payload.connection_string)


@router.post("/etl/execution-approval")
def api_etl_execution_approval(payload: ExecutionApprovalPayload, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    from agent.session_store import load_session, save_session
    sid = (payload.session_id or "default").strip() or "default"
    sess = load_session(sid)
    flow = sess.setdefault("context", {}).setdefault("etl_flow", {})
    flow["execution_approved"] = bool(payload.approved)
    save_session(sess)
    return {
        "ok": True,
        "session_id": sid,
        "approved": bool(payload.approved)
    }


@router.get("/pipeline/history/{session_id}")
def api_pipeline_history(session_id: str, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    from agent.session_store import _connect
    import json
    sid = (session_id or "default").strip() or "default"
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT id, session_id, run_ts, dataset_names, schema_hash, dq_score, "
            "dq_issue_count, etl_phase, etl_engine, etl_outcome, generation_mode, notes "
            "FROM pipeline_runs WHERE session_id = ? ORDER BY run_ts DESC",
            (sid,)
        ).fetchall()
        
        runs = []
        for r in rows:
            try:
                ds_list = json.loads(r[3] or "[]")
            except Exception:
                ds_list = []
            runs.append({
                "id": r[0],
                "session_id": r[1],
                "run_ts": r[2],
                "dataset_names": ds_list,
                "schema_hash": r[4],
                "dq_score": r[5],
                "dq_issue_count": r[6],
                "etl_phase": r[7],
                "etl_engine": r[8],
                "etl_outcome": r[9],
                "generation_mode": r[10],
                "notes": r[11],
            })
        
        return {"ok": True, "session_id": sid, "history": runs}
    finally:
        conn.close()


@router.post("/pipeline/run")
def api_pipeline_run(payload: PipelineRunPayload, request: Request, _auth: None = Depends(require_backend_token)) -> Dict[str, Any]:
    """
    Unified entry point: assess + ETL in one graph invocation.
    Feature-flagged: DHARA_UNIFIED_PIPELINE=1
    """
    import os
    if not os.getenv("DHARA_UNIFIED_PIPELINE"):
        raise HTTPException(status_code=404, detail="Unified pipeline not enabled")
    from agent.unified_graph import run_unified_graph
    
    return run_unified_graph(
        session_id=payload.session_id or "default",
        user_request=payload.user_request or "",
        sources_path=payload.sources_path or "config/sources.yaml",
        selected_sources=payload.sources or [],
        generation_mode=payload.generation_mode or "full",
        engine=payload.engine or "python",
        business_rules=payload.business_rules or {},
        job_id=payload.job_id or "",
    )




