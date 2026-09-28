from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("agent.jobs_store")

from agent.durable_artifact_store import (
    get_instance_id,
    save_durable_job_state,
    load_durable_job_state,
)

_RESOLVED_DB_PATH: Optional[str] = None


def _db_path() -> str:
    global _RESOLVED_DB_PATH
    if _RESOLVED_DB_PATH:
        return _RESOLVED_DB_PATH

    data_dir = os.environ.get("DHARA_DATA_DIR")
    if not data_dir:
        # Check /home/data on Azure Linux App Service
        if os.environ.get("WEBSITE_SITE_NAME") and os.path.isdir("/home"):
            candidate = "/home/data"
            try:
                os.makedirs(candidate, exist_ok=True)
                test_file = os.path.join(candidate, ".write_test")
                with open(test_file, "w") as f:
                    f.write("ok")
                os.remove(test_file)
                data_dir = candidate
            except Exception as e:
                logger.warning(f"/home/data not writable ({e}), falling back to local workspace/tmp")
                data_dir = None

        if not data_dir:
            root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            candidate = os.path.join(root, "output")
            try:
                os.makedirs(candidate, exist_ok=True)
                test_file = os.path.join(candidate, ".write_test")
                with open(test_file, "w") as f:
                    f.write("ok")
                os.remove(test_file)
                data_dir = candidate
            except Exception:
                data_dir = "/tmp/dhara_data"
                os.makedirs(data_dir, exist_ok=True)

    db_file = os.path.join(data_dir, "jobs.sqlite3")
    logger.info(f"Opening SQLite database at: {db_file} (parent directory exists: {os.path.exists(data_dir)})")
    _RESOLVED_DB_PATH = db_file
    return db_file


def _connect() -> sqlite3.Connection:
    path = _db_path()
    try:
        conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    except sqlite3.OperationalError as e:
        logger.error(f"Failed to connect to SQLite at {path}: {e}. Falling back to /tmp/jobs.sqlite3")
        conn = sqlite3.connect("/tmp/jobs.sqlite3", timeout=30, check_same_thread=False)

    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception:
        try:
            conn.execute("PRAGMA journal_mode=DELETE")
        except Exception:
            pass
    try:
        conn.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
          job_id TEXT PRIMARY KEY,
          created_at REAL NOT NULL,
          updated_at REAL NOT NULL,
          status TEXT NOT NULL,
          kind TEXT NOT NULL,
          input_json TEXT NOT NULL,
          result_json TEXT,
          error TEXT,
          progress INTEGER DEFAULT 0,
          current_stage TEXT,
          worker_instance_id TEXT,
          heartbeat_at REAL,
          report_blob_path TEXT,
          manifest_blob_path TEXT
        )
        """
    )
    for col_def in (
        "progress INTEGER DEFAULT 0",
        "current_stage TEXT",
        "worker_instance_id TEXT",
        "heartbeat_at REAL",
        "report_blob_path TEXT",
        "manifest_blob_path TEXT",
    ):
        try:
            col_name = col_def.split()[0]
            conn.execute(f"ALTER TABLE jobs ADD COLUMN {col_def}")
            conn.commit()
        except sqlite3.OperationalError:
            pass
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS job_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_id TEXT NOT NULL,
          ts REAL NOT NULL,
          level TEXT NOT NULL,
          message TEXT NOT NULL,
          data_json TEXT,
          FOREIGN KEY(job_id) REFERENCES jobs(job_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_job_events_job_id ON job_events(job_id, id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS job_checkpoints (
          job_id TEXT NOT NULL,
          stage TEXT NOT NULL,
          ts REAL NOT NULL,
          stage_output_json TEXT NOT NULL,
          PRIMARY KEY (job_id, stage)
        )
        """
    )
    return conn


def _ensure_db_ready(max_retries: int = 10, delay: float = 0.5) -> None:
    """Block until the SQLite database is confirmed ready.

    On Azure App Service, the /home/data directory may not be fully mounted
    when the process starts. This retries the connection with an exponential
    back-off to avoid the 'unable to open database file' race condition.
    """
    db_path = _db_path()
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(db_path, timeout=5)
            conn.execute("SELECT 1")
            conn.close()
            logger.info(f"SQLite DB ready at: {db_path} (attempt {attempt + 1})")
            return
        except Exception as exc:
            wait = delay * (attempt + 1)
            logger.warning(
                f"DB not ready yet (attempt {attempt + 1}/{max_retries}): {exc} — "
                f"retrying in {wait:.1f}s"
            )
            time.sleep(wait)
    logger.error(
        f"SQLite DB at {db_path} not available after {max_retries} attempts. "
        "Falling back to /tmp/jobs.sqlite3 — jobs will not persist across restarts."
    )


def create_job(*, kind: str, input: Dict[str, Any]) -> str:
    job_id = str(uuid.uuid4())
    now = time.time()
    instance_id = get_instance_id()
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO jobs(job_id, created_at, updated_at, status, kind, input_json,
                             progress, current_stage, worker_instance_id, heartbeat_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (job_id, now, now, "queued", kind, json.dumps(input, ensure_ascii=False, default=str),
             0, "queued", instance_id, now),
        )
        conn.commit()
        add_event(job_id=job_id, level="info", message="queued", data={"kind": kind})
    finally:
        conn.close()

    # Persist durable record immediately before returning to caller
    save_durable_job_state(job_id, {
        "job_id": job_id,
        "created_at": now,
        "updated_at": now,
        "status": "queued",
        "kind": kind,
        "input": input,
        "result": None,
        "error": None,
        "progress": 0,
        "current_stage": "queued",
        "worker_instance_id": instance_id,
        "heartbeat_at": now,
        "report_blob_path": None,
        "manifest_blob_path": None,
    })

    return job_id


def add_event(*, job_id: str, level: str, message: str, data: Optional[Dict[str, Any]] = None) -> None:
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO job_events(job_id, ts, level, message, data_json) VALUES(?,?,?,?,?)",
            (
                job_id,
                time.time(),
                level,
                message,
                json.dumps(data, ensure_ascii=False, default=str) if data is not None else None,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def update_job_status(
    job_id: str,
    *,
    status: str,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    progress: Optional[int] = None,
    current_stage: Optional[str] = None,
    report_blob_path: Optional[str] = None,
    manifest_blob_path: Optional[str] = None,
) -> None:
    now = time.time()
    instance_id = get_instance_id()
    conn = _connect()
    try:
        # Extract report/manifest blob paths from result if present
        if isinstance(result, dict):
            if not report_blob_path and result.get("report_blob_path"):
                report_blob_path = result.get("report_blob_path")
            if not manifest_blob_path and result.get("manifest_blob_path"):
                manifest_blob_path = result.get("manifest_blob_path")

        updates = ["updated_at=?", "status=?", "result_json=?", "error=?", "heartbeat_at=?", "worker_instance_id=?"]
        params = [
            now,
            status,
            json.dumps(result, ensure_ascii=False, default=str) if result is not None else None,
            error,
            now,
            instance_id,
        ]
        if progress is not None:
            updates.append("progress=?")
            params.append(progress)
        if current_stage is not None:
            updates.append("current_stage=?")
            params.append(current_stage)
        if report_blob_path is not None:
            updates.append("report_blob_path=?")
            params.append(report_blob_path)
        if manifest_blob_path is not None:
            updates.append("manifest_blob_path=?")
            params.append(manifest_blob_path)

        params.append(job_id)
        sql = f"UPDATE jobs SET {', '.join(updates)} WHERE job_id=?"
        conn.execute(sql, tuple(params))
        conn.commit()
    finally:
        conn.close()

    # Sync durable record so any instance polling this job gets updated state
    j = fetch_job(job_id, skip_durable_sync=True)
    if j:
        save_durable_job_state(job_id, j)


def fetch_job(job_id: str, *, skip_durable_sync: bool = False) -> Optional[Dict[str, Any]]:
    conn = _connect()
    row = None
    try:
        row = conn.execute(
            """
            SELECT job_id, created_at, updated_at, status, kind, input_json, result_json,
                   error, progress, current_stage, worker_instance_id, heartbeat_at,
                   report_blob_path, manifest_blob_path
            FROM jobs WHERE job_id=?
            """,
            (job_id,),
        ).fetchone()
    finally:
        conn.close()

    local_job = None
    if row:
        local_job = {
            "job_id": row[0],
            "created_at": row[1],
            "updated_at": row[2],
            "status": row[3],
            "kind": row[4],
            "input": json.loads(row[5]) if row[5] else {},
            "result": json.loads(row[6]) if row[6] else None,
            "error": row[7],
            "progress": row[8] if row[8] is not None else 0,
            "current_stage": row[9],
            "worker_instance_id": row[10],
            "heartbeat_at": row[11],
            "report_blob_path": row[12],
            "manifest_blob_path": row[13],
        }

    if skip_durable_sync:
        return local_job

    # If job not found locally OR status is not final (another container may have finished it)
    if not local_job or local_job.get("status") not in ("succeeded", "failed"):
        durable = load_durable_job_state(job_id)
        if durable:
            # If durable state has newer updated_at or job is missing locally, re-hydrate SQLite
            d_updated = durable.get("updated_at", 0)
            l_updated = local_job.get("updated_at", 0) if local_job else -1
            if d_updated >= l_updated:
                conn = _connect()
                try:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO jobs(
                            job_id, created_at, updated_at, status, kind, input_json,
                            result_json, error, progress, current_stage,
                            worker_instance_id, heartbeat_at, report_blob_path, manifest_blob_path
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            durable.get("job_id", job_id),
                            durable.get("created_at", time.time()),
                            durable.get("updated_at", time.time()),
                            durable.get("status", "queued"),
                            durable.get("kind", "assess"),
                            json.dumps(durable.get("input") or {}, ensure_ascii=False, default=str),
                            json.dumps(durable.get("result"), ensure_ascii=False, default=str) if durable.get("result") is not None else None,
                            durable.get("error"),
                            durable.get("progress", 0),
                            durable.get("current_stage"),
                            durable.get("worker_instance_id"),
                            durable.get("heartbeat_at"),
                            durable.get("report_blob_path"),
                            durable.get("manifest_blob_path"),
                        ),
                    )
                    conn.commit()
                finally:
                    conn.close()
                return durable

    return local_job


def claim_next_job(*, kinds: Optional[List[str]] = None, max_retries: int = 10) -> Optional[Dict[str, Any]]:
    """
    Claim job under transactional BEGIN IMMEDIATE with retry to prevent double-claiming race conditions.
    """
    for attempt in range(max_retries):
        conn = _connect()
        conn.isolation_level = None  # Disable python sqlite3 implicit transaction wrapping to allow manual BEGIN IMMEDIATE
        try:
            conn.execute("BEGIN IMMEDIATE")
            if kinds:
                q = "SELECT job_id FROM jobs WHERE status='queued' AND kind IN ({}) ORDER BY created_at LIMIT 1".format(
                    ",".join(["?"] * len(kinds))
                )
                row = conn.execute(q, tuple(kinds)).fetchone()
            else:
                row = conn.execute("SELECT job_id FROM jobs WHERE status='queued' ORDER BY created_at LIMIT 1").fetchone()
            if not row:
                conn.execute("COMMIT")
                return None
            job_id = row[0]
            conn.execute("UPDATE jobs SET status='running', updated_at=? WHERE job_id=?", (time.time(), job_id))
            conn.execute("COMMIT")
            add_event(job_id=job_id, level="info", message="running")
            return fetch_job(job_id)
        except sqlite3.OperationalError:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            time.sleep(0.02 * (attempt + 1))
        finally:
            try:
                conn.close()
            except Exception:
                pass
    return None


def fetch_events(job_id: str, *, after_id: int = 0, limit: int = 200) -> List[Dict[str, Any]]:
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT id, ts, level, message, data_json FROM job_events WHERE job_id=? AND id>? ORDER BY id LIMIT ?",
            (job_id, int(after_id), int(limit)),
        ).fetchall()
        out = []
        for r in rows:
            out.append(
                {
                    "id": r[0],
                    "ts": r[1],
                    "level": r[2],
                    "message": r[3],
                    "data": json.loads(r[4]) if r[4] else None,
                }
            )
        return out
    finally:
        conn.close()


def update_job_progress(job_id: str, progress: int) -> None:
    conn = _connect()
    try:
        conn.execute(
            "UPDATE jobs SET updated_at=?, progress=? WHERE job_id=?",
            (time.time(), int(progress), job_id),
        )
        conn.commit()
    finally:
        conn.close()


def save_checkpoint(job_id: str, stage: str, stage_output: Dict[str, Any]) -> None:
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO job_checkpoints(job_id, stage, ts, stage_output_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                job_id,
                stage,
                time.time(),
                json.dumps(stage_output, ensure_ascii=False, default=str),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def load_checkpoint(job_id: str, stage: str) -> Optional[Dict[str, Any]]:
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT stage_output_json FROM job_checkpoints WHERE job_id=? AND stage=?",
            (job_id, stage),
        ).fetchone()
        if not row:
            return None
        return json.loads(row[0])
    finally:
        conn.close()


def requeue_stale_jobs(*, stale_after_seconds: int = 1800) -> int:
    """Requeue jobs stuck in 'running' for longer than stale_after_seconds."""
    cutoff = time.time() - stale_after_seconds
    conn = _connect()
    try:
        cur = conn.execute(
            "UPDATE jobs SET status='queued', updated_at=? WHERE status='running' AND updated_at < ?",
            (time.time(), cutoff),
        )
        count = cur.rowcount
        conn.commit()
        return count
    finally:
        conn.close()


def recover_stale_claimed_jobs(*, stale_minutes: int = 5) -> int:
    """Reset jobs stuck in 'running' from a previous crashed container to 'queued'."""
    cutoff = time.time() - (stale_minutes * 60)
    conn = _connect()
    try:
        cur = conn.execute(
            """
            UPDATE jobs
            SET status = 'queued',
                updated_at = ?,
                current_stage = 'recovered',
                error = 'Recovered from crashed/restarted container'
            WHERE status = 'running'
              AND updated_at < ?
            """,
            (time.time(), cutoff),
        )
        count = cur.rowcount
        conn.commit()
        if count:
            logger.warning(
                f"recover_stale_claimed_jobs: recovered {count} orphaned job(s) "
                f"(stuck in 'running' for >{stale_minutes} min) — likely from a previous container crash."
            )
        else:
            logger.info("recover_stale_claimed_jobs: no orphaned jobs found.")
        return count
    except Exception as exc:
        logger.error(f"recover_stale_claimed_jobs failed: {exc}")
        return 0
    finally:
        conn.close()


