"""FastAPI MCP server for Intelligent Data Assessment (run, list_tables, stream, upload, load_path)."""
# Reloading...

# Force .NET Framework runtime for pythonnet (MUST happen before any clr/pythonnet usage)
try:
    import clr_loader
    import pythonnet
    # If set_runtime fails, it means it's already initialized, which is usually okay if it's the right one.
    # On this specific PC, we MUST force netfx.
    pythonnet.set_runtime(clr_loader.get_netfx())
except Exception:
    pass

import os
# Force Azure CLI path to PATH environment variable if not already present
for _az_dir in [r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin", r"C:\Program Files (x86)\Microsoft SDKs\Azure\CLI2\wbin"]:
    if os.path.isdir(_az_dir) and _az_dir not in os.environ.get("PATH", ""):
        os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + _az_dir

import json
import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

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

# Load local .env automatically (developer convenience; do not rely on this in production).
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None  # type: ignore

if load_dotenv is not None:
    try:
        _HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        load_dotenv(os.path.join(_HERE, ".env"), override=False)
    except Exception:
        pass

# Report builders from main (avoid circular import by importing after main is loaded)
try:
    import main as _main
    _build_html = _main.build_html_report
    _build_md = _main.build_markdown_report
except Exception:
    _build_html = None
    _build_md = None


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
    conversation_id: Optional[str] = None
    user_id: Optional[str] = None
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
    timeout_s: Optional[int] = 120


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


class FabricShortcutsPayload(BaseModel):
    session_id: str
    selected_blob_paths: List[str]
    blob_account_name: Optional[str] = None
    blob_container: Optional[str] = None


class FabricDeployNotebookPayload(BaseModel):
    session_id: str
    pyspark_code: str
    notebook_name: Optional[str] = None
    lakehouse_id: Optional[str] = None
    plan: Optional[Dict[str, Any]] = None


class PipelineMirrorPayload(BaseModel):
    session_id: str
    blob_paths: List[str]




setup_logging()
logger = logging.getLogger("mcp_server")

app = FastAPI(title="Intelligent Data Assessment MCP Server")

from agent.memory_system.api.memory_router import router as memory_router
app.include_router(memory_router)


_limiter = InMemoryRateLimiter(
    max_requests=int(os.environ.get("RATE_LIMIT_PER_MINUTE", "120")),
    window_seconds=60,
)

_worker = JobWorker()
_worker.start()

@app.on_event("startup")
def startup_event():
    _worker.start()

@app.on_event("shutdown")
def shutdown_event():
    _worker.stop()


app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in (os.environ.get("CORS_ALLOW_ORIGINS") or "").split(",") if o.strip()] or [
        "http://localhost:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001",
    ],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "X-Backend-Token", "X-Request-Id", "X-Correlation-Id"],
)

@app.middleware("http")
async def auth_and_logging_middleware(request: Request, call_next):
    rid = get_request_id(request)
    request.state.request_id = rid
    try:
        _limiter.check(client_ip(request))
        # Require auth for everything except health
        if request.url.path not in ("/", "/healthz", "/readyz") and not request.url.path.startswith("/api/v1"):
            require_backend_token(request)
        response = await call_next(request)
        response.headers["X-Request-Id"] = rid
        return response
    except HTTPException as e:
        logger.warning("http_error", extra={"request_id": rid})
        return JSONResponse(status_code=e.status_code, content={"detail": e.detail, "request_id": rid})
    except Exception as e:
        logger.exception("unhandled_error", extra={"request_id": rid})
        return JSONResponse(status_code=500, content={"detail": str(e), "request_id": rid})


@app.get("/", tags=["health"])
def root() -> Dict[str, str]:
    return {"status": "ok", "service": "mcp_server"}


@app.get("/healthz", tags=["health"])
def healthz() -> Dict[str, str]:
    return {"ok": "true"}


@app.get("/healthz/db-env", tags=["health"])
def healthz_db_env() -> Dict[str, Any]:
    import os
    return {
        "AZURE_SQL_SERVER": os.getenv("AZURE_SQL_SERVER"),
        "AZURE_SQL_DATABASE": os.getenv("AZURE_SQL_DATABASE"),
        "DHARA_AZURE_SQL_CONN_STR": os.getenv("DHARA_AZURE_SQL_CONN_STR") is not None,
    }


@app.get("/healthz/fabric-env", tags=["health"])
def healthz_fabric_env() -> Dict[str, Any]:
    from connectors.fabric_lakehouse_connector import is_fabric_mirror_enabled
    import os
    return {
        "DHARA_FABRIC_MIRROR_ENABLED": os.getenv("DHARA_FABRIC_MIRROR_ENABLED"),
        "is_fabric_mirror_enabled": is_fabric_mirror_enabled(),
        "FABRIC_WORKSPACE_ID": os.getenv("FABRIC_WORKSPACE_ID"),
        "FABRIC_LAKEHOUSE_NAME": os.getenv("FABRIC_LAKEHOUSE_NAME"),
        "FABRIC_TENANT_ID": os.getenv("FABRIC_TENANT_ID"),
        "has_client_secret": bool(os.getenv("FABRIC_CLIENT_SECRET")),
    }



@app.get("/readyz", tags=["health"])
def readyz() -> Dict[str, str]:
    return {"ok": "true"}


def validate_startup_config():
    problems = []
    if not (os.getenv("AZURE_OPENAI_ENDPOINT") or os.getenv("OPENAI_API_KEY")):
        problems.append("No LLM credentials configured (AZURE_OPENAI_ENDPOINT or OPENAI_API_KEY)")
    if not os.getenv("DHARA_AZURE_SQL_CONN_STR") and os.getenv("DHARA_REQUIRE_SQL", "0") == "1":
        problems.append("Azure SQL connection string (DHARA_AZURE_SQL_CONN_STR) is missing")
    if os.getenv("DHARA_FABRIC_MIRROR_ENABLED", "0").strip().lower() in ("1", "true") and not os.getenv("FABRIC_WORKSPACE_ID"):
        problems.append("Fabric mirroring enabled but FABRIC_WORKSPACE_ID missing")
        
    try:
        from agent.config_validator import validate_fabric_config
        validate_fabric_config()
    except Exception as e:
        logger.warning(f"Failed to execute startup Fabric config check: {e}")
        
    if problems:
        for p in problems:
            logger.critical("STARTUP CONFIG ERROR: %s", p)
        if os.getenv("DHARA_STRICT_STARTUP", "0") == "1":
            raise RuntimeError(f"Startup validation failed: {problems}")


@app.on_event("startup")
async def _startup():
    validate_startup_config()
    _worker.start()


@app.get("/etl/regen-status/{job_id}", tags=["etl"])
def api_etl_regen_status(job_id: str) -> Dict[str, Any]:
    from agent.etl_pipeline.execution_orchestrator import _REGEN_JOBS
    if job_id not in _REGEN_JOBS:
        raise HTTPException(status_code=404, detail="Regeneration job not found")
    return _REGEN_JOBS[job_id]


@app.get("/etl/plan/{plan_id}/narrative", tags=["etl"])
def api_etl_plan_narrative(plan_id: str, session_id: str = "default") -> Dict[str, Any]:
    from agent.session_store import load_session, save_session, get_session_lock
    from agent.etl_pipeline.plan_narrator import narrate_plan
    
    with get_session_lock(session_id):
        sess = load_session(session_id)
        ctx = sess.setdefault("context", {})
        flow = ctx.setdefault("etl_flow", {})
        
        plan = flow.get("approved_plan") or flow.get("plan")
        if not plan or str(plan.get("plan_id")) != plan_id:
            raise HTTPException(status_code=404, detail="Plan not found or plan_id mismatch")
            
        cache_key = f"narr_{plan_id}_{plan.get('assessment_signature')}"
        cached = (flow.get("narration_cache") or {}).get(cache_key)
        if isinstance(cached, dict) and cached.get("engine_explanation"):
            return {"ok": True, "narration": cached}
            
        narr_mode = os.getenv("ETL_NARRATOR_MODE", "fallback").strip().lower()
        use_llm_full = narr_mode in ("llm", "full") or os.getenv("ETL_NARRATOR_USE_LLM", "0").strip().lower() in ("1", "true", "yes")
        
        narration = narrate_plan(plan, mode=narr_mode, use_llm=use_llm_full)
        flow.setdefault("narration_cache", {})[cache_key] = narration
        save_session(sess)
        return {"ok": True, "narration": narration}


@app.exception_handler(Exception)
def generic_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"error": str(exc), "type": exc.__class__.__name__},
    )


def _get_config_text(body_config: str) -> str:
    """Use request body config, or fall back to MCP_DEFAULT_CONFIG_PATH file if set and body empty."""
    if (body_config or "").strip():
        return body_config.strip()
    default_path = os.environ.get("MCP_DEFAULT_CONFIG_PATH")
    if default_path and os.path.isfile(default_path):
        with open(default_path, "r", encoding="utf-8") as f:
            return f.read()
    return body_config or "{}"


@app.post("/run")
def api_run(cfg: ConfigText, additional_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Run a full assessment. Config from body, or from file at MCP_DEFAULT_CONFIG_PATH if body empty."""
    config_text = _get_config_text(cfg.config)
    return run_assessment(config_text, additional_data=additional_data, approved_semantics=cfg.approved_semantics)


@app.post("/list_tables")
def api_list_tables(cfg: ConfigText) -> Dict[str, List[str]]:
    """List SQL tables. Config from body, or from MCP_DEFAULT_CONFIG_PATH if body empty."""
    config_text = _get_config_text(cfg.config)
    return {"tables": list_tables(config_text)}


_SCHEMA_CACHE: Dict[str, Any] = {
    "sources_path": None,
    "sources_mtime": None,
    "cached_at": None,
    "tables": None,
}


@app.get("/schema/tables")
def api_schema_tables(ttl_seconds: int = 30) -> Dict[str, Any]:
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


@app.post("/transform_suggest")
def api_transform_suggest(payload: Dict[str, Any]) -> Dict[str, Any]:
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


@app.post("/dq_recommend")
def api_dq_recommend(payload: Dict[str, Any]) -> Dict[str, Any]:
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


@app.post("/stream")
def api_stream(payload: StreamPayload) -> Dict[str, Any]:
    return process_stream_chunk(payload.records, name=payload.name or "stream")


@app.post("/load_path")
def api_load_path(payload: PathPayload) -> Dict[str, Any]:
    """Load datasets from a filesystem path (returns dict of name -> dataframe; JSON serialization may omit raw data)."""
    data = load_path(payload.path)
    return {"datasets": list(data.keys()), "count": len(data)}


ALL_CONTAINER_BLOBS = [
    "Taxidata.csv",
    "data_quality_issues.csv",
    "data_quality_issues.json",
    "data_quality_issues_v2.csv",
    "data_quality_issues_v2.json",
    "employee_data_quality_issues_10000.csv",
    "factory.json",
    "factory_relational_test.json",
    "marketing.csv",
    "marketing_relational_test.csv",
    "sales.xml",
    "sales_relational_test.xml"
]


@app.get("/blob/files", tags=["sources"])
def api_blob_files() -> Dict[str, Any]:
    """
    List all blob files residing in the configured Azure Blob Storage container.
    """
    sources_path = os.environ.get("MCP_SOURCES_PATH") or "config/sources.yaml"
    files = []
    try:
        import main as _main
        cfg = _main.load_config(sources_path)
        source_cfg = cfg.get("source", cfg) or {}
        locations = source_cfg.get("locations", []) or []
        for loc in locations:
            if "blob" in str(loc.get("type") or "").lower():
                try:
                    from connectors.azure_blob_storage import AzureBlobStorageConnector
                    conn = AzureBlobStorageConnector(loc.get("connection", {}))
                    files = conn.list_blobs()
                    if files:
                        break
                except Exception as ex:
                    logger.warning(f"Failed to list blobs: {ex}")
    except Exception as e:
        logger.error(f"Error listing blob files: {e}")

    if not files:
        files = ALL_CONTAINER_BLOBS

    return {"ok": True, "count": len(files), "files": files}


@app.get("/sources")
def api_sources() -> Dict[str, Any]:
    """
    Return available source configurations from sources.yaml with discovered file / table lists.
    """
    sources_path = os.environ.get("MCP_SOURCES_PATH") or "config/sources.yaml"
    try:
        import main as _main
        cfg = _main.load_config(sources_path)
        source_cfg = cfg.get("source", cfg) or {}
        locations = source_cfg.get("locations", []) or []
        out = []
        sql_default_tables = ["customers", "sales_fact", "products", "inventory", "dim_customer"]

        for idx, loc in enumerate(locations):
            loc_type = str(loc.get("type") or "").lower()
            files = []
            if "blob" in loc_type:
                try:
                    from connectors.azure_blob_storage import AzureBlobStorageConnector
                    conn = AzureBlobStorageConnector(loc.get("connection", {}))
                    files = conn.list_blobs()
                except Exception as ex:
                    logger.warning(f"Failed to list blobs for location {idx}: {ex}")
                if not files:
                    files = ALL_CONTAINER_BLOBS
            elif "filesystem" in loc_type or "local" in loc_type:
                path = loc.get("path")
                if path and os.path.exists(path):
                    if os.path.isdir(path):
                        files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
                    else:
                        files = [os.path.basename(path)]
                if not files:
                    files = ALL_CONTAINER_BLOBS
            elif "database" in loc_type or "sql" in loc_type:
                cached_tables = _SCHEMA_CACHE.get("tables")
                files = cached_tables if isinstance(cached_tables, list) and len(cached_tables) > 0 else sql_default_tables
            else:
                files = ALL_CONTAINER_BLOBS

            out.append(
                {
                    "index": idx,
                    "id": loc.get("id") or loc.get("label") or loc.get("name") or None,
                    "type": loc.get("type"),
                    "files": files,
                }
            )
        return {"sources_path": sources_path, "location_count": len(out), "locations": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/assess")
def api_assess(payload: AssessPayload, request: Request) -> Dict[str, Any]:
    """
    High-level orchestration endpoint:
    uses LangGraph orchestrator (route -> extract -> transform).
    """
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
    from agent.session_store import load_session, save_session, get_session_lock
    with get_session_lock(session_id):
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



@app.post("/chat")
def api_chat(payload: ChatPayload, background_tasks: BackgroundTasks) -> Dict[str, Any]:
    """
    Enterprise Conversational endpoint integrated with Memory System, Personalization,
    Message Persistence, Fact Extraction, and Long-Conversation Summarization.
    """
    from agent.chat_graph import run_chat
    from agent.memory_system.services.conversation_service import ConversationService
    from agent.memory_system.services.personalization import PersonalizationEngine
    from agent.memory_system.services.extraction_pipeline import MemoryExtractionPipeline
    from agent.memory_system.services.summarizer import SummarizationEngine
    from agent.memory_system.models import RoleEnum

    conv_svc = ConversationService()
    pers_engine = PersonalizationEngine()
    extractor = MemoryExtractionPipeline()
    summarizer = SummarizationEngine()

    sid = (payload.session_id or "default").strip() or "default"
    conv_id = (payload.conversation_id or sid).strip()
    user_id = (payload.user_id or "default_user").strip()
    tid = payload.thread_id or conv_id

    # 1. Fetch Personalized Context (Memories + Preferences + Summary)
    personal_context = pers_engine.build_personalized_context(
        user_id=user_id,
        user_query=payload.message,
        conversation_id=conv_id,
    )

    # 2. Persist User Message
    user_msg_res = conv_svc.add_message(
        conversation_id=conv_id,
        role=RoleEnum.USER,
        content=payload.message,
    )

    # 3. Execute Core Chat Workflow
    out = run_chat(
        session_id=sid,
        message=payload.message,
        thread_id=tid,
        resume_value=payload.resume_value,
    )

    reply_text = out.get("reply") or ""

    # 4. Persist Assistant Message
    asst_msg_res = conv_svc.add_message(
        conversation_id=conv_id,
        role=RoleEnum.ASSISTANT,
        content=reply_text,
        metadata={"action": out.get("action")},
    )

    # 5. Trigger Async Memory Extraction & Summarization
    background_tasks.add_task(extractor.process_message, user_id, payload.message, conv_id)
    background_tasks.add_task(summarizer.check_and_generate_summary, conv_id)

    try:
        logger.info(
            "chat_routed",
            extra={
                "session_id": sid,
                "conversation_id": conv_id,
                "thread_id": tid,
                "message": (payload.message or "")[:200],
                "action": out.get("action"),
            },
        )
    except Exception:
        pass

    return {
        "ok": True,
        "reply": reply_text,
        "payload": out.get("payload") or {},
        "session_id": sid,
        "conversation_id": conv_id,
        "thread_id": tid,
        "user_id": user_id,
        "user_message_id": user_msg_res.message_id,
        "assistant_message_id": asst_msg_res.message_id,
        "personalization": {
            "memories_used_count": len(personal_context.get("relevant_memories", [])),
            "summary_active": bool(personal_context.get("summary_text")),
        },
    }



@app.get("/sessions")
def api_list_sessions(limit: int = 50) -> Dict[str, Any]:
    from agent.session_store import list_sessions

    return {"ok": True, "sessions": list_sessions(limit=limit)}


@app.get("/sessions/{session_id}")
def api_get_session(session_id: str) -> Dict[str, Any]:
    from agent.session_store import load_session

    return {"ok": True, "session": load_session(session_id)}


@app.post("/sessions/context")
def api_update_session_context(payload: SessionContextPayload) -> Dict[str, Any]:
    """
    Merge arbitrary keys into a session's context.
    Used by the UI to persist uploaded report text and other user artifacts.
    """
    from agent.session_store import load_session, save_session, get_session_lock

    sid = (payload.session_id or "default").strip() or "default"
    with get_session_lock(sid):
        sess = load_session(sid)
        ctx = sess.setdefault("context", {})
        if not isinstance(ctx, dict):
            ctx = {}
            sess["context"] = ctx
        for k, v in (payload.context or {}).items():
            ctx[str(k)] = v
        save_session(sess)
        return {"ok": True, "session_id": sid, "context_keys": list(ctx.keys())}


@app.post("/etl/infer-semantics")
def api_infer_semantics(payload: SemanticInferencePayload) -> Dict[str, Any]:
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


@app.post("/etl/enrich-semantics")
def api_enrich_semantics(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich low-confidence columns.
    """
    from agent.etl_pipeline.semantic_llm_enricher import enrich_low_confidence_columns
    low_confidence_cols = payload.get("low_confidence_cols") or {}
    enriched = enrich_low_confidence_columns(low_confidence_cols)
    return {"ok": True, "enriched": enriched}


@app.post("/etl/plan")
def api_etl_plan(payload: EtlPlanPayload) -> Dict[str, Any]:
    """Build ETL plan from assessment + business rules; stores under session.context.etl_flow."""
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


@app.get("/etl/tenants")
def api_etl_tenants() -> Dict[str, Any]:
    from agent.etl_handlers import etl_list_tenants

    return etl_list_tenants()


@app.post("/etl/apply-manual-resolutions")
def api_etl_apply_manual_resolutions(payload: EtlApplyManualResolutionsPayload) -> Dict[str, Any]:
    """Promote user-selected manual review resolutions into plan steps."""
    from agent.etl_handlers import etl_apply_manual_resolutions

    return etl_apply_manual_resolutions(
        payload.session_id,
        payload.resolutions or [],
        plan_override=payload.plan,
    )


@app.post("/etl/enrich-review-options")
def api_etl_enrich_review_options(payload: EtlEnrichReviewOptionsPayload) -> Dict[str, Any]:
    """Enrich dynamic resolution options for an unmapped anomaly type in the plan using the LLM."""
    from agent.etl_pipeline.manual_review_catalog import get_dynamic_resolution_options, enrich_manual_review_item
    from agent.session_store import load_session, save_session, get_session_lock
    import logging

    logger = logging.getLogger("agent.mcp_server")
    opts = get_dynamic_resolution_options(payload.issue_type, payload.item, allow_llm_call=True)
    
    sid = (payload.session_id or "default").strip() or "default"
    try:
        with get_session_lock(sid):
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


@app.post("/etl/confirm")
def api_etl_confirm(payload: EtlConfirmPayload) -> Dict[str, Any]:
    """Confirm (optionally edited) plan and compute impact preview."""
    from agent.etl_handlers import etl_confirm_plan

    return etl_confirm_plan(payload.session_id, plan_override=payload.plan)


@app.post("/etl/generate")
def api_etl_generate(payload: EtlGeneratePayload) -> Dict[str, Any]:
    """Generate ETL from approved plan; LLM + template fallback with validation.

    IMPORTANT: Only hard-block on genuine gate failures (409 PLAN_NOT_APPROVED,
    unacknowledged blockers, etc.).  Validation failures (ok=False with no
    http_status) must NOT become 422 — the code is returned as a draft so the
    user can still see and download it.  This matches api_routes.py behaviour.
    """
    from agent.etl_handlers import etl_generate_code

    result = etl_generate_code(
        payload.session_id,
        engine=payload.engine or "python",
        sql_dialect=payload.sql_dialect or "tsql",
        codegen_mode=payload.codegen_mode,
        generation_mode=payload.generation_mode,
    )
    # Only raise for genuine blocking HTTP errors (e.g. 409 PLAN_NOT_APPROVED).
    # Validation failures (ok=False, no http_status) are returned as 200 drafts
    # so the frontend can display the code and validation_errors to the user.
    http_status = result.get("http_status")
    if http_status and http_status >= 400:
        raise HTTPException(status_code=http_status, detail=result)
    return result


@app.post("/etl/deploy")
def api_etl_deploy(payload: EtlDeployPayload) -> Dict[str, Any]:
    from agent.etl_handlers import etl_deploy
    return etl_deploy(payload.session_id)


@app.get("/etl/dq-gate")
def api_etl_dq_gate(session_id: str, dataset: str, threshold: float = 70.0) -> Dict[str, Any]:
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


@app.get("/etl/phases")
def api_etl_phases(session_id: str) -> Dict[str, Any]:
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


def _etl_safe_segment(s: str) -> str:
    import re

    t = re.sub(r"[^a-zA-Z0-9_-]+", "_", (s or "default").strip())[:80]
    return t or "default"


@app.get("/etl/lineage")
def api_etl_lineage(session_id: str) -> Dict[str, Any]:
    """Column lineage map (source → transforms → target) after plan confirm."""
    from agent.etl_handlers import etl_get_lineage

    return etl_get_lineage(session_id)


@app.get("/etl/download/{plan_id}")
def api_etl_download_by_plan_id(plan_id: str):
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


@app.get("/etl/download")
def api_etl_download(session_id: str):
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


@app.post("/jobs")
def api_create_job(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
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


@app.get("/jobs/{job_id}")
def api_get_job(job_id: str) -> Dict[str, Any]:
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


@app.get("/etl/assessment/status/{job_id}")
def api_etl_assessment_status(job_id: str) -> Dict[str, Any]:
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


@app.get("/jobs/{job_id}/events")
def api_get_job_events(job_id: str, after_id: int = 0) -> Dict[str, Any]:
    # Simple polling endpoint; SSE can be added on top.
    ev = fetch_events(job_id, after_id=int(after_id), limit=200)
    return {"ok": True, "events": ev}


@app.post("/upload")
async def api_upload(
    file: UploadFile = File(...),
    request: Request = None,
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


@app.post("/business-rules/parse")
async def api_parse_business_rules(
    session_id: str = Form("default"),
    text: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
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


@app.post("/etl/execute")
def api_etl_execute(payload: EtlExecutePayload) -> Dict[str, Any]:
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


@app.post("/etl/update-code")
def api_etl_update_code(payload: EtlUpdateCodePayload) -> Dict[str, Any]:
    from agent.session_store import load_session, save_session, get_session_lock
    sid = (payload.session_id or "default").strip() or "default"
    with get_session_lock(sid):
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


@app.post("/etl/run-full")
def api_etl_run_full(payload: EtlPlanPayload) -> Dict[str, Any]:
    """Single-call ETL: plan → confirm → generate in one graph invocation."""
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



@app.get("/etl/execution-status/{session_id}")
def api_etl_execution_status_route(session_id: str) -> Dict[str, Any]:
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


@app.post("/etl/test-connection")
def api_etl_test_connection(payload: TestConnectionPayload) -> Dict[str, Any]:
    from agent.azure_sql_executor import test_connection
    return test_connection(payload.connection_string)


@app.post("/etl/execution-approval")
def api_etl_execution_approval(payload: ExecutionApprovalPayload) -> Dict[str, Any]:
    from agent.session_store import load_session, save_session, get_session_lock
    sid = (payload.session_id or "default").strip() or "default"
    with get_session_lock(sid):
        sess = load_session(sid)
        flow = sess.setdefault("context", {}).setdefault("etl_flow", {})
        flow["execution_approved"] = bool(payload.approved)
        save_session(sess)
        return {
            "ok": True,
            "session_id": sid,
            "approved": bool(payload.approved)
        }


@app.post("/fabric/create-shortcuts", tags=["fabric"])
def api_fabric_create_shortcuts(payload: FabricShortcutsPayload) -> Dict[str, Any]:
    from agent.fabric_shortcut_service import create_shortcuts_for_blobs
    from agent.session_store import load_session
    selected = payload.selected_blob_paths or []
    if not selected:
        sess = load_session(payload.session_id or "default")
        ctx = sess.get("context", {})
        selected = ctx.get("selected_files") or ctx.get("files") or []
    try:
        results = create_shortcuts_for_blobs(
            session_id=payload.session_id,
            selected_blob_paths=selected,
            blob_account_name=payload.blob_account_name,
            blob_container=payload.blob_container
        )
        return {"ok": True, "shortcuts": results}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "SHORTCUT_CREATION_FAILED", "message": str(e)}
        )


@app.post("/fabric/deploy-notebook", tags=["fabric"])
def api_fabric_deploy_notebook(payload: FabricDeployNotebookPayload) -> Dict[str, Any]:
    from agent.fabric_notebook_deployer import deploy_and_run_notebook
    try:
        results = deploy_and_run_notebook(
            session_id=payload.session_id,
            pyspark_code=payload.pyspark_code,
            notebook_name=payload.notebook_name,
            lakehouse_id=payload.lakehouse_id,
            plan=payload.plan,
        )
        return results
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "NOTEBOOK_RUN_FAILED", "message": str(e)}
        )


@app.get("/fabric/run-status/{notebook_id}/{run_id}", tags=["fabric"])
def api_fabric_run_status(notebook_id: str, run_id: str) -> Dict[str, Any]:
    from agent.fabric_api_client import FabricAPIClient
    client = FabricAPIClient()
    try:
        status_res = client.get_run_status(notebook_id, run_id)
        return status_res
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "STATUS_CHECK_FAILED", "message": str(e)}
        )


@app.post("/api/pipeline/mirror", tags=["fabric"])
def api_pipeline_mirror(payload: PipelineMirrorPayload) -> Dict[str, Any]:
    from agent.mirror_service import mirror_blobs_to_fabric
    try:
        result = mirror_blobs_to_fabric(payload.session_id, payload.blob_paths)
        return {"ok": True, "mirror_result": result}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "MIRRORING_FAILED", "message": str(e)}
        )


@app.get("/api/pipeline/mirror/status/{session_id}", tags=["fabric"])
def api_pipeline_mirror_status(session_id: str) -> Dict[str, Any]:
    from agent.blob_fabric_registry import list_shortcuts
    try:
        shortcuts = list_shortcuts(session_id)
        return {"ok": True, "shortcuts": shortcuts, "count": len(shortcuts)}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "STATUS_RETRIEVAL_FAILED", "message": str(e)}
        )



@app.get("/pipeline/history/{session_id}")
def api_pipeline_history(session_id: str) -> Dict[str, Any]:
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


@app.post("/pipeline/run")
def api_pipeline_run(payload: PipelineRunPayload, request: Request) -> Dict[str, Any]:
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




@app.get("/format-capabilities")
def api_get_format_capabilities() -> Dict[str, Any]:
    from agent.etl_pipeline.format_capabilities import PYSPARK_FORMAT_CAPABILITIES
    return {"ok": True, "capabilities": PYSPARK_FORMAT_CAPABILITIES}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("agent.mcp_server:app", host="127.0.0.1", port=8000, log_level="info")
