"""
ETL execution orchestrator for Agent Dhara.
Sits between etl_handlers.py code output and the Azure SQL executor.
Handles approval logic, calls executor, runs post-execution reconciliation,
and packages results for governance/report attachment.
"""

from __future__ import annotations

import uuid
import logging
import hashlib
import concurrent.futures
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import threading

_ORCHESTRATOR_LOCK = threading.Lock()

from agent.etl_pipeline.validate_sql import validate_sql_basic
from agent.sql_preflight import run_sql_preflight
from agent.azure_sql_executor import (
    check_requires_approval,
    get_connection,
)
from agent.execution import execute_plan, ExecutionPlan, ExecutionEngine

logger = logging.getLogger("agent.etl_pipeline.execution_orchestrator")

import os
import json
import time
import tempfile

class FileExecutionCache:
    def __init__(self, cache_dir: str | None = None, ttl: int = 3600):
        if cache_dir is None:
            cache_dir = os.environ.get("EXEC_CACHE_DIR", os.path.join(tempfile.gettempdir(), "dhara_exec_cache"))
        self.cache_dir = cache_dir
        self.ttl = ttl

    def _get_path(self, key: str) -> str:
        return os.path.join(self.cache_dir, f"{key}.json")

    def __contains__(self, key: str) -> bool:
        path = self._get_path(key)
        if not os.path.exists(path):
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                rec = json.load(f)
            if time.time() - rec.get("ts", 0) > self.ttl:
                os.remove(path)
                return False
            return True
        except Exception:
            return False

    def __getitem__(self, key: str) -> dict:
        path = self._get_path(key)
        with open(path, "r", encoding="utf-8") as f:
            rec = json.load(f)
        return rec["data"]

    def __setitem__(self, key: str, value: dict):
        try:
            os.makedirs(self.cache_dir, exist_ok=True)
            path = self._get_path(key)
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"ts": time.time(), "data": value}, f)
        except Exception as e:
            logger.warning(f"Failed to write to file cache: {e}")

_EXEC_CACHE = FileExecutionCache()
_REGEN_JOBS: Dict[str, dict] = {}


def start_regen_job(
    job_id: str,
    post_validation_report: dict,
    original_plan: dict,
    assessment: dict,
    engine: str,
    connection_string: str | None = None,
):
    _REGEN_JOBS[job_id] = {"status": "running", "result": None, "error": None}
    
    def run():
        try:
            patched = post_etl_regen_if_needed(
                post_validation_report,
                original_plan,
                assessment,
                engine,
                connection_string
            )
            _REGEN_JOBS[job_id] = {"status": "succeeded", "result": patched, "error": None}
        except Exception as e:
            _REGEN_JOBS[job_id] = {"status": "failed", "result": None, "error": str(e)}

    import threading
    t = threading.Thread(target=run)
    t.daemon = True
    t.start()


def post_etl_regen_if_needed(
    post_validation_report: dict,
    original_plan: dict,
    assessment: dict,
    engine: str,
    connection_string: str | None = None,
) -> str | None:
    """
    If post-ETL validation found regressions, attempt one ETL patch.
    Returns patched SQL or None if no regen needed.
    """
    if post_validation_report.get("ok", True):
        return None
    
    regressions = post_validation_report.get("deltas", {}).get("regressions") or []
    if not regressions:
        return None
    
    from agent.etl_pipeline.llm_codegen import generate_etl_with_llm
    
    fix_hints = []
    for reg in regressions:
        col = reg.get("column") or "?"
        ds = reg.get("table") or "?"
        issue = reg.get("issue") or "quality regression"
        fix_hints.append(f"[{ds}] column {col}: {issue} worsened after ETL — fix the transform")
    
    uncovered = original_plan.get("coverage", {}).get("uncovered_items", [])
    pre_hints = [f"[{u['dataset']}] {u['column']}: {u['issue_type']} uncovered in plan" for u in uncovered]
    fix_hints = pre_hints + fix_hints
    
    from agent.etl_pipeline.llm_codegen import run_codegen_sync

    coro = generate_etl_with_llm(
        original_plan, assessment, engine=engine,
        validation_errors=fix_hints
    )
    patched_code, err = run_codegen_sync(coro)
    
    if err or not patched_code or patched_code.startswith("# Error") or patched_code.startswith("Error:"):
        return None
    
    return patched_code


def orchestrate_sql_execution(
    sql: str,
    *,
    session_id: str,
    run_id: str | None = None,
    approved: bool = False,
    dry_run: bool = False,
    connection_string: str | None = None,
    pre_execution_counts: dict | None = None,
    assessment: dict | None = None,
    timeout_s: int = 120,
) -> dict:
    with _ORCHESTRATOR_LOCK:
        return _orchestrate_sql_execution_impl(
            sql,
            session_id=session_id,
            run_id=run_id,
            approved=approved,
            dry_run=dry_run,
            connection_string=connection_string,
            pre_execution_counts=pre_execution_counts,
            assessment=assessment,
            timeout_s=timeout_s,
        )


def _orchestrate_sql_execution_impl(
    sql: str,
    *,
    session_id: str,
    run_id: str | None = None,
    approved: bool = False,
    dry_run: bool = False,
    connection_string: str | None = None,
    pre_execution_counts: dict | None = None,
    assessment: dict | None = None,
    timeout_s: int = 120,
) -> dict:
    """
    Coordinate SQL generation output -> approval gate -> execution -> reconciliation.
    """
    rid = run_id or str(uuid.uuid4())
    now_str = datetime.now(timezone.utc).isoformat()

    # Check data quality gate (B2)
    rules = {}
    try:
        from agent.session_store import load_session
        sess = load_session(session_id)
        if sess:
            flow = sess.get("context", {}).get("etl_flow", {}) or {}
            plan = flow.get("approved_plan") or flow.get("plan") or {}
            rules = plan.get("business_rules") or {}
    except Exception:
        pass

    if assessment:
        from agent.etl_pipeline.dq_gate import evaluate_dq_gate
        gate = evaluate_dq_gate(assessment, rules)
        if gate.get("blocking_issues"):
            return {
                "ok": False,
                "stage": "dq_gate",
                "error": "DQ gate blocked",
                "details": gate["blocking_issues"],
                "run_id": rid,
                "session_id": session_id,
                "approved": approved,
                "dry_run": dry_run,
                "timestamp_utc": now_str,
                "post_execution_summary": {
                    "transaction_committed": False,
                    "total_rows_affected": 0,
                    "total_duration_ms": 0.0,
                    "batch_count": 0,
                    "row_deltas": {},
                    "rollback_reason": "DQ gate blocked",
                },
            }

    # 3.3 Idempotency Cache Guard
    idempotency_key = hashlib.sha256(f"{sql}_{dry_run}".encode("utf-8")).hexdigest()
    if idempotency_key in _EXEC_CACHE:
        logger.info(f"Returning cached execution result for key: {idempotency_key}")
        cached_res = dict(_EXEC_CACHE[idempotency_key])
        cached_res["run_id"] = rid
        cached_res["timestamp_utc"] = now_str
        return cached_res

    # 1. Validate SQL with validate_sql_basic
    valid, hard_errors, advisory_warnings = validate_sql_basic(sql)
    if advisory_warnings:
        logger.warning(f"[SQL Advisory] {advisory_warnings}")
    if not valid:
        res = {
            "ok": False,
            "stage": "validation",
            "failure_class": "retryable_pregate",
            "run_id": rid,
            "session_id": session_id,
            "approved": approved,
            "dry_run": dry_run,
            "validation_errors": hard_errors,
            "requires_approval": False,
            "ops_found": [],
            "execution": {},
            "post_execution_summary": {
                "transaction_committed": False,
                "total_rows_affected": 0,
                "total_duration_ms": 0.0,
                "batch_count": 0,
                "row_deltas": {},
                "rollback_reason": None,
            },
            "timestamp_utc": now_str,
        }
        return res

    # 1b. T-SQL preflight with 3.5 hard 15s timeout
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run_sql_preflight, sql)
        try:
            preflight = future.result(timeout=15.0)
        except concurrent.futures.TimeoutError:
            preflight = {"passed": False, "errors": ["SQL preflight validation timed out (15s limit)"]}

    if not preflight.get("passed", True):
        preflight_errors = preflight.get("errors") or []
        res = {
            "ok": False,
            "stage": "preflight",
            "failure_class": "retryable_pregate",
            "run_id": rid,
            "session_id": session_id,
            "approved": approved,
            "dry_run": dry_run,
            "validation_errors": preflight_errors,
            "requires_approval": False,
            "ops_found": [],
            "execution": {},
            "post_execution_summary": {
                "transaction_committed": False,
                "total_rows_affected": 0,
                "total_duration_ms": 0.0,
                "batch_count": 0,
                "row_deltas": {},
                "rollback_reason": None,
            },
            "timestamp_utc": now_str,
        }
        return res

    # 2. Check approval requirement via check_requires_approval()
    app_check = check_requires_approval(sql)
    if app_check["requires_approval"] and not approved and not dry_run:
        res = {
            "ok": False,
            "stage": "approval_required",
            "run_id": rid,
            "session_id": session_id,
            "approved": approved,
            "dry_run": dry_run,
            "validation_errors": [],
            "requires_approval": True,
            "ops_found": app_check["ops_found"],
            "execution": {},
            "post_execution_summary": {
                "transaction_committed": False,
                "total_rows_affected": 0,
                "total_duration_ms": 0.0,
                "batch_count": 0,
                "row_deltas": {},
                "rollback_reason": "Approval required",
            },
            "timestamp_utc": now_str,
        }
        return res

    # 3. Call execution registry
    plan = ExecutionPlan(
        plan_id=rid,
        engine=ExecutionEngine.AZURE_SQL,
        code=sql,
        requires_approval=app_check["requires_approval"],
        destructive_ops=app_check["ops_found"],
    )
    exec_result = execute_plan(
        plan,
        connection_string=connection_string,
        dry_run=dry_run,
        approved=approved,
        timeout_s=timeout_s,
        run_id=rid,
    )
    exec_res = {
        "ok": exec_result.ok,
        "run_id": exec_result.run_id,
        "total_rows_affected": exec_result.rows_affected,
        "total_duration_ms": exec_result.duration_ms,
        "transaction_committed": exec_result.committed,
        "rollback_reason": exec_result.error,
        "batches_run": len(exec_result.batch_results),
        "batch_results": exec_result.batch_results,
        "artifacts": exec_result.artifacts,
    }

    # 4. Build post_execution_summary
    committed = exec_res.get("transaction_committed", False)
    total_rows = exec_res.get("total_rows_affected", 0)
    duration = exec_res.get("total_duration_ms", 0.0)
    batch_count = exec_res.get("batches_run", 0)
    rollback_reason = exec_res.get("rollback_reason")

    row_deltas = {}
    post_validation_report = {"ok": True, "deltas": {"improvements": [], "regressions": []}}
    if pre_execution_counts and committed and not dry_run:
        table_names = list(pre_execution_counts.keys())
        post_execution_counts = build_pre_execution_counts(table_names, connection_string)
        for t in table_names:
            before = pre_execution_counts[t]
            after = post_execution_counts.get(t)
            if before is not None and after is not None:
                delta = after - before
            else:
                delta = None
            row_deltas[t] = {
                "before": before,
                "after": after,
                "delta": delta,
            }
        
        # Run post-ETL validation (Component 14)
        from agent.post_etl_validator import run_post_etl_validation
        try:
            post_validation_report = run_post_etl_validation(
                target_tables=table_names,
                connection_string=connection_string,
                pre_assessment=assessment
            )
        except Exception as e:
            logger.debug(f"Post-ETL validation run failed: {e}")

    # Attempt regen on regressions asynchronously
    regen_patch_job_id = None
    if not post_validation_report.get("ok", True) and assessment:
        try:
            from agent.session_store import load_session
            sess = load_session(session_id)
            ctx = sess.get("context") or {}
            flow = ctx.get("etl_flow") or {}
            original_plan = flow.get("approved_plan") or flow.get("plan")
            engine = flow.get("codegen_engine") or "sql"
            if original_plan:
                regen_patch_job_id = str(uuid.uuid4())
                start_regen_job(
                    regen_patch_job_id,
                    post_validation_report,
                    original_plan,
                    assessment,
                    engine,
                    connection_string
                )
        except Exception as e:
            logger.debug(f"Regen patch queue failed: {e}")

    post_execution_summary = {
        "transaction_committed": committed,
        "total_rows_affected": total_rows,
        "total_duration_ms": duration,
        "batch_count": batch_count,
        "row_deltas": row_deltas,
        "rollback_reason": rollback_reason,
        "post_etl_validation": post_validation_report,
        "regen_patch_job_id": regen_patch_job_id,
    }

    final_res = {
        "ok": exec_res.get("ok", False),
        "stage": "execution",
        "run_id": rid,
        "session_id": session_id,
        "approved": approved,
        "dry_run": dry_run,
        "validation_errors": [],
        "requires_approval": app_check["requires_approval"],
        "ops_found": app_check["ops_found"],
        "execution": exec_res,
        "post_execution_summary": post_execution_summary,
        "timestamp_utc": now_str,
    }

    if final_res["ok"] and not dry_run:
        _EXEC_CACHE[idempotency_key] = final_res

    return final_res


def _safe_bracket_quote(name: str) -> str:
    """
    Safely bracket-quote a table or schema name, escaping any ']' by doubling it.
    If the name has parts separated by '.', each part is quoted and escaped.
    """
    if not name:
        return ""
    parts = name.split(".")
    quoted_parts = []
    for part in parts:
        part = part.strip()
        if part.startswith("[") and part.endswith("]"):
            part = part[1:-1]
        escaped = part.replace("]", "]]")
        quoted_parts.append(f"[{escaped}]")
    return ".".join(quoted_parts)


def build_pre_execution_counts(
    table_names: list[str],
    connection_string: str | None = None,
) -> dict:
    """
    Run SELECT COUNT(*) FROM <table> for each table_name.
    Uses a batched UNION ALL query for single roundtrip, falling back to iterative query on error.
    """
    counts = {t: None for t in table_names}
    if not table_names:
        return counts

    union_parts = []
    for t in table_names:
        safe_table = _safe_bracket_quote(t)
        union_parts.append(f"SELECT {repr(t)} AS tbl, COUNT(*) AS cnt FROM {safe_table}")
    query = "\nUNION ALL\n".join(union_parts)

    conn = None
    try:
        conn = get_connection(connection_string)
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                counts[row[0]] = row[1]
        except Exception as e:
            logger.warning(f"Batched pre-execution counting failed, falling back to iterative query: {e}")
            for table in table_names:
                safe_table = _safe_bracket_quote(table)
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {safe_table}")
                    row = cursor.fetchone()
                    counts[table] = row[0] if row else None
                except Exception:
                    counts[table] = None
    except Exception:
        for table in table_names:
            counts[table] = None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return counts
