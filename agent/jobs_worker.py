from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger("agent.jobs_worker")

from agent.jobs_store import (
    add_event,
    claim_next_job,
    update_job_status,
    save_checkpoint,
    load_checkpoint,
    requeue_stale_jobs,
    _ensure_db_ready,
    recover_stale_claimed_jobs,
)


def _run_job(job: Dict[str, Any]) -> Dict[str, Any]:
    job_id = job.get("job_id")
    kind = job.get("kind")
    inp = job.get("input") or {}
    add_event(job_id=job_id, level="debug", message=f"JobWorker processing {kind}")
    
    if kind == "assess":
        from agent.langgraph_orchestrator import run_orchestrator

        user_req = inp.get("user_request")
        if not user_req and inp.get("messages"):
            msgs = inp.get("messages")
            if isinstance(msgs, list) and len(msgs) > 0:
                user_req = msgs[-1].get("content")

        sid = str(inp.get("sessionId") or inp.get("session_id") or "default").strip()
        business_rules = None
        from agent.session_store import get_session_lock
        with get_session_lock(sid):
            try:
                from agent.session_store import load_session
                sess = load_session(sid)
                ctx = sess.get("context") or {}
                business_rules = ctx.get("pending_business_rules") or ctx.get("business_rules")
            except Exception:
                pass

        # Try to load from checkpoint first
        state = load_checkpoint(job_id, "orchestrator")
        if state is None:
            add_event(job_id=job_id, level="info", message="running stage: orchestrator")
            update_job_status(job_id, status="running", progress=35)
            state = run_orchestrator(
                user_request=str(user_req or ""),
                sources_path=str(inp.get("sources_path") or "config/sources.yaml"),
                selected_sources=inp.get("selected_sources") or [],
                job_id=job_id,
                session_id=sid,
                business_rules=business_rules,
            )
            save_checkpoint(job_id, "orchestrator", state)
            add_event(job_id=job_id, level="info", message="completed stage: orchestrator")
            update_job_status(job_id, status="running", progress=75)
        else:
            add_event(job_id=job_id, level="info", message="restored stage from checkpoint: orchestrator")
            update_job_status(job_id, status="running", progress=75)

        extractions = state.get("extractions") or []
        extraction_errors = state.get("extraction_errors") or []
        merged_result = {
            "datasets": {},
            "relationships": [],
            "data_quality_issues": {
                "datasets": {},
                "global_issues": {}
            }
        }
        for ex in extractions:
            res = ex.get("result")
            if isinstance(res, dict):
                merged_result["datasets"].update(res.get("datasets") or {})
                merged_result["relationships"].extend(res.get("relationships") or [])
                dq = res.get("data_quality_issues") or {}
                merged_result["data_quality_issues"]["datasets"].update(dq.get("datasets") or {})
                merged_result["data_quality_issues"]["global_issues"].update(dq.get("global_issues") or {})

        if not merged_result["datasets"] and extraction_errors:
            err_details = "; ".join([f"{e.get('source')}: {e.get('error')}" for e in extraction_errors])
            logger.error(f"Job {job_id} assessment data extraction failed completely: {err_details}")
            add_event(job_id=job_id, level="error", message=f"Data extraction failed: {err_details}", data={"errors": extraction_errors})
            raise RuntimeError(f"Data extraction failed: {err_details}")

        try:
            from agent.session_store import load_session, save_session, get_session_lock
            sid = str(inp.get("sessionId") or inp.get("session_id") or "default").strip()
            with get_session_lock(sid):
                sess = load_session(sid)
                ctx = sess.setdefault("context", {})
                ctx["last_assessment_result"] = merged_result
                save_session(sess)
        except Exception:
            pass

        reports = load_checkpoint(job_id, "reports")
        if reports is None:
            add_event(job_id=job_id, level="info", message="running stage: reports")
            update_job_status(job_id, status="running", progress=90, current_stage="reports")
            from agent.chat_graph import _build_report_tables_markdown, _render_report_html, _write_report_artifacts
            from agent.durable_artifact_store import (
                save_durable_artifact,
                verify_durable_artifact_exists,
            )

            report_md = _build_report_tables_markdown(merged_result)
            report_html = _render_report_html(merged_result)
            artifacts = _write_report_artifacts(
                result=merged_result,
                report_markdown=report_md,
                report_html=report_html,
                job_id=None,  # worker handles blob uploads below; passing job_id here causes double-upload
            )

            # Upload artifacts to durable store (disk + Azure Blob Storage)
            report_blob = save_durable_artifact(
                job_id=job_id,
                filename="report.html",
                content=report_html,
                content_type="text/html",
            )
            manifest_dict = artifacts.get("cleaning_manifest_data") or {}
            manifest_blob = save_durable_artifact(
                job_id=job_id,
                filename="cleaning_manifest.json",
                content=json.dumps(manifest_dict, ensure_ascii=False, indent=2, default=str),
                content_type="application/json",
            )
            save_durable_artifact(
                job_id=job_id,
                filename="report.json",
                content=json.dumps(merged_result, ensure_ascii=False, indent=2, default=str),
                content_type="application/json",
            )
            if report_md:
                save_durable_artifact(
                    job_id=job_id,
                    filename="report.md",
                    content=report_md,
                    content_type="text/markdown",
                )

            # Artifact gating: verify uploaded objects exist in durable storage
            if not verify_durable_artifact_exists(job_id, "report.html"):
                raise RuntimeError(f"Report artifact upload verification failed for job {job_id}")

            reports = {
                "report_markdown": report_md,
                "report_html": report_html,
                "report_files": artifacts,
                "report_blob_path": report_blob,
                "manifest_blob_path": manifest_blob,
            }
            save_checkpoint(job_id, "reports", reports)
            add_event(job_id=job_id, level="info", message="completed stage: reports")
            update_job_status(job_id, status="running", progress=98, current_stage="reports_verified")
        else:
            add_event(job_id=job_id, level="info", message="restored stage from checkpoint: reports")
            update_job_status(job_id, status="running", progress=98, current_stage="reports_verified")

        return {
            "result": merged_result,
            "report_markdown": reports["report_markdown"],
            "report_html": reports["report_html"],
            "report_files": reports["report_files"],
            "report_blob_path": reports.get("report_blob_path"),
            "manifest_blob_path": reports.get("manifest_blob_path"),
        }

    if kind == "chat":
        from agent.chat_graph import run_chat

        return run_chat(
            session_id=str(inp.get("session_id") or "default"), 
            message=str(inp.get("message") or ""),
            job_id=job_id,
        )

    if kind == "etl_execute":
        from agent.etl_handlers import etl_execute_sql
        return etl_execute_sql(
            session_id=inp.get("session_id"),
            approved=bool(inp.get("approved", False)),
            dry_run=bool(inp.get("dry_run", False)),
            timeout_s=int(inp.get("timeout_s", 240)),
        )

    raise ValueError(f"Unknown job kind: {kind}")


class JobWorker:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        # Issue 1 Fix: Wait until SQLite DB is fully ready before starting the
        # worker thread. Eliminates 'unable to open database file' on cold start.
        _ensure_db_ready()
        # Issue 7 Fix: Reset any 'running' jobs left by a previous crashed container
        # so they get re-claimed and completed by this worker instance.
        recover_stale_claimed_jobs(stale_minutes=5)
        requeue_stale_jobs(stale_after_seconds=0)
        logger.info("Starting JobWorker background thread...")
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        logger.info("Stopping JobWorker...")
        self._stop.set()

    def _loop(self) -> None:
        logger.info("JobWorker background loop is now running.")
        while not self._stop.is_set():
            try:
                job = claim_next_job()
            except Exception as e:
                logger.error(f"JobWorker error while claiming job: {e}")
                time.sleep(1.0)
                continue
            if not job:
                time.sleep(0.25)
                continue
            job_id = job["job_id"]
            logger.info(f"JobWorker claimed job {job_id} (kind: {job.get('kind')})")
            try:
                add_event(job_id=job_id, level="info", message="started")
                result = _run_job(job)
                report_blob = result.get("report_blob_path") if isinstance(result, dict) else None
                manifest_blob = result.get("manifest_blob_path") if isinstance(result, dict) else None
                update_job_status(
                    job_id,
                    status="succeeded",
                    result=result,
                    progress=100,
                    current_stage="completed",
                    report_blob_path=report_blob,
                    manifest_blob_path=manifest_blob,
                )
                add_event(job_id=job_id, level="info", message="succeeded")
                logger.info(f"JobWorker job {job_id} completed successfully.")
            except Exception as e:
                logger.exception(f"JobWorker job {job_id} failed: {e}")
                update_job_status(job_id, status="failed", error=str(e), current_stage="failed")
                add_event(job_id=job_id, level="error", message="failed", data={"error": str(e)})

