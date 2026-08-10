from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger("agent.session_store")


class SessionJSONEncoder(json.JSONEncoder):
    """Handles pandas Timestamps, datetime objects, and numpy scalars."""

    def default(self, obj: Any) -> Any:
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        if hasattr(obj, "item") and callable(obj.item):
            # Handles numpy types (int64, float64, etc.)
            try:
                return obj.item()
            except Exception:
                pass
        return super().default(obj)


def _db_path() -> str:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_dir = os.path.join(root, "output")
    os.makedirs(out_dir, exist_ok=True)
    return os.path.join(out_dir, "chat_sessions.sqlite3")


import threading

_local_conn = threading.local()


class ConnectionProxy:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def close(self):
        # Keep connection alive for thread local cache
        pass


_CONN_MAX_AGE = 300  # 5 minutes


def _connect() -> ConnectionProxy:
    now = time.time()
    
    # Recycle if connection is too old
    conn_age = getattr(_local_conn, "conn_created_at", 0)
    if hasattr(_local_conn, "conn") and _local_conn.conn and (now - conn_age) > _CONN_MAX_AGE:
        try:
            _local_conn.conn.close()
        except Exception:
            pass
        _local_conn.conn = None
        
    if not hasattr(_local_conn, "conn") or _local_conn.conn is None:
        conn = sqlite3.connect(_db_path(), timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
              session_id TEXT PRIMARY KEY,
              created_at REAL NOT NULL,
              updated_at REAL NOT NULL,
              version INTEGER DEFAULT 1,
              payload_json TEXT NOT NULL
            )
            """
        )
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(sessions)")
        columns = [r[1] for r in cursor.fetchall()]
        if "version" not in columns:
            conn.execute("ALTER TABLE sessions ADD COLUMN version INTEGER DEFAULT 1")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS experiences (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              session_id TEXT NOT NULL,
              ts REAL NOT NULL,
              user_text TEXT,
              action TEXT,
              success INTEGER,
              notes TEXT,
              FOREIGN KEY(session_id) REFERENCES sessions(session_id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_experiences_session_ts ON experiences(session_id, ts DESC)")
        
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id      TEXT    NOT NULL,
                run_ts          REAL    NOT NULL,
                dataset_names   TEXT,          -- JSON array: ["orders", "customers"]
                schema_hash     TEXT,          -- SHA256 from _assessment_schema_signature()
                dq_score        INTEGER,       -- 0-100 from etl_readiness_scorer
                dq_issue_count  INTEGER,
                etl_phase       TEXT,          -- last phase reached: planned/confirmed/generated/executed
                etl_engine      TEXT,          -- python/sql/pyspark/adf
                etl_outcome     TEXT,          -- succeeded/failed/skipped
                generation_mode TEXT,
                notes           TEXT,          -- free text for narrative
                FOREIGN KEY(session_id) REFERENCES sessions(session_id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_session ON pipeline_runs(session_id, run_ts DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_datasets ON pipeline_runs(dataset_names)")
        conn.commit()
        _local_conn.conn = conn
        _local_conn.conn_created_at = now
        
    return ConnectionProxy(_local_conn.conn)



# FIX (2026-05-07): New sessions now initialise last_step as
# 'awaiting_source_selection' instead of 'unknown'.  This ensures the
# guard_fresh_session_fallback() in routing_guards.py fires correctly and
# the frontend status display never shows 'unknown'.
_DEFAULT_SESSION_PAYLOAD: Dict[str, Any] = {
    "selected_source": None,
    "selected_blob_files": [],
    "selected_local_files": [],
    "selected_tables": [],
    "selected_table": None,
    "last_assessment_result": None,
    "last_assessment_signature": None,
    "last_assessment_datasets": [],
    "last_step": "awaiting_source_selection",  # FIX: was absent / 'unknown'
    "selected_db_location_index": None,
    "selected_blob_location_index": None,
    "selected_fs_location_index": None,
}


def load_session(session_id: str) -> Dict[str, Any]:
    sid = (session_id or "default").strip() or "default"
    now = time.time()
    conn = _connect()
    try:
        row = conn.execute("SELECT payload_json, version FROM sessions WHERE session_id = ?", (sid,)).fetchone()
        if not row:
            # Brand-new session — persist immediately with correct defaults
            payload = dict(_DEFAULT_SESSION_PAYLOAD)
            payload["session_id"] = sid
            payload["_version"] = 1
            conn.execute(
                "INSERT INTO sessions (session_id, created_at, updated_at, version, payload_json) VALUES (?,?,?,?,?)",
                (sid, now, now, 1, json.dumps(payload, cls=SessionJSONEncoder)),
            )
            conn.commit()
            return payload
        payload = json.loads(row[0])
        payload["session_id"] = sid
        payload["_version"] = row[1] if row[1] is not None else 1
        # Back-fill missing last_step for sessions created before this fix
        if payload.get("last_step") in (None, "", "unknown") and not payload.get("selected_source"):
            payload["last_step"] = "awaiting_source_selection"
        return payload
    finally:
        conn.close()


def save_session(session_id_or_payload: str | Dict[str, Any], payload: Optional[Dict[str, Any]] = None) -> None:
    if isinstance(session_id_or_payload, dict):
        payload = session_id_or_payload
        sid = str(payload.get("session_id") or "default")
    else:
        sid = str(session_id_or_payload or "default").strip() or "default"
        if payload is None:
            payload = {}

    loaded_version = payload.pop("_version", None)
    now = time.time()

    # ── Payload size guard ───────────────────────────────────────────────────
    # Prevent bloated last_assessment_result from causing SQLITE_TOOBIG errors
    # or slow deserialization. Cap at 5 MB; store a lightweight stub instead.
    _MAX_ASSESSMENT_BYTES = 5 * 1024 * 1024  # 5 MB
    ctx = payload.get("context") if isinstance(payload.get("context"), dict) else None
    raw_assess = ctx.get("last_assessment_result") if ctx else payload.get("last_assessment_result")
    if raw_assess is not None and isinstance(raw_assess, dict):
        try:
            raw_bytes = len(json.dumps(raw_assess, cls=SessionJSONEncoder).encode("utf-8"))
            if raw_bytes > _MAX_ASSESSMENT_BYTES:
                logger.warning(
                    f"Session '{sid}': last_assessment_result is {raw_bytes // 1024}KB — "
                    f"trimming to prevent session bloat (cap={_MAX_ASSESSMENT_BYTES // 1024}KB)"
                )
                stub = {"_trimmed": True, "datasets": list((raw_assess.get("datasets") or {}).keys())}
                if ctx:
                    ctx["last_assessment_result"] = stub
                else:
                    payload["last_assessment_result"] = stub
        except Exception as _size_err:
            logger.debug(f"Session payload size check failed (non-critical): {_size_err}")
    # ────────────────────────────────────────────────────────────────────────

    conn = _connect()
    try:
        row = conn.execute("SELECT version FROM sessions WHERE session_id = ?", (sid,)).fetchone()
        if not row:
            payload["_version"] = 1
            conn.execute(
                "INSERT INTO sessions (session_id, created_at, updated_at, version, payload_json) VALUES (?,?,?,?,?)",
                (sid, now, now, 1, json.dumps(payload, cls=SessionJSONEncoder)),
            )
        else:
            current_version = row[0] if row[0] is not None else 1
            if loaded_version is not None and loaded_version != current_version:
                logger.warning(
                    f"Concurrency conflict: Session '{sid}' version mismatch (loaded: {loaded_version}, current: {current_version}). Auto-merging payloads."
                )
                db_payload_json = conn.execute("SELECT payload_json FROM sessions WHERE session_id = ?", (sid,)).fetchone()[0]
                try:
                    db_payload = json.loads(db_payload_json)
                except Exception:
                    db_payload = {}
                merged_payload = db_payload.copy()
                merged_payload.update(payload)
                payload = merged_payload
            
            new_version = current_version + 1
            payload["_version"] = new_version
            conn.execute(
                "UPDATE sessions SET updated_at = ?, version = ?, payload_json = ? WHERE session_id = ?",
                (now, new_version, json.dumps(payload, cls=SessionJSONEncoder), sid)
            )
        conn.commit()
    finally:
        conn.close()


def list_sessions(limit: int = 50) -> List[Dict[str, Any]]:
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT session_id, created_at, updated_at FROM sessions ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            {
                "session_id": r[0],
                "created_at": r[1],
                "updated_at": r[2],
            }
            for r in rows
        ]
    finally:
        conn.close()


def reset_session(session_id: str) -> Dict[str, Any]:
    """Wipe a session back to clean defaults and persist."""
    payload = dict(_DEFAULT_SESSION_PAYLOAD)
    save_session(session_id, payload)
    return payload


def add_experience(
    session_id: str,
    user_text: Optional[str],
    action: Optional[str],
    success: bool,
    notes: Optional[str] = None,
) -> None:
    sid = (session_id or "default").strip() or "default"
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO experiences (session_id, ts, user_text, action, success, notes) VALUES (?,?,?,?,?,?)",
            (sid, time.time(), user_text, action, int(success), notes),
        )
        conn.commit()
    finally:
        conn.close()


def list_recent_experiences(
    session_id: str,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    sid = (session_id or "default").strip() or "default"
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT ts, user_text, action, success, notes FROM experiences "
            "WHERE session_id = ? ORDER BY ts DESC LIMIT ?",
            (sid, limit),
        ).fetchall()
        return [
            {
                "ts": r[0],
                "user_text": r[1],
                "action": r[2],
                "success": bool(r[3]),
                "notes": r[4],
            }
            for r in rows
        ]
    finally:
        conn.close()


# Backward compatibility aliases
log_experience = add_experience
get_experiences = list_recent_experiences


def save_pipeline_run(
    session_id: str,
    *,
    dataset_names: List[str],
    schema_hash: str,
    dq_score: int,
    dq_issue_count: int,
    etl_phase: str = "",
    etl_engine: str = "",
    etl_outcome: str = "",
    generation_mode: str = "",
    notes: str = ""
) -> int:
    sid = (session_id or "default").strip() or "default"
    now = time.time()
    conn = _connect()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO pipeline_runs (
                session_id, run_ts, dataset_names, schema_hash, dq_score,
                dq_issue_count, etl_phase, etl_engine, etl_outcome,
                generation_mode, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sid,
                now,
                json.dumps(dataset_names, cls=SessionJSONEncoder),
                schema_hash,
                dq_score,
                dq_issue_count,
                etl_phase,
                etl_engine,
                etl_outcome,
                generation_mode,
                notes
            )
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def get_latest_pipeline_run(
    session_id: str,
    dataset_names: Optional[List[str]] = None
) -> Optional[Dict[str, Any]]:
    sid = (session_id or "default").strip() or "default"
    conn = _connect()
    try:
        if dataset_names:
            rows = conn.execute(
                "SELECT id, session_id, run_ts, dataset_names, schema_hash, dq_score, "
                "dq_issue_count, etl_phase, etl_engine, etl_outcome, generation_mode, notes "
                "FROM pipeline_runs WHERE session_id = ? ORDER BY run_ts DESC",
                (sid,)
            ).fetchall()
            
            target_set = set(dataset_names)
            for r in rows:
                try:
                    ds_list = json.loads(r[3] or "[]")
                    if target_set.intersection(ds_list):
                        return {
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
                        }
                except Exception:
                    pass
            return None
        else:
            row = conn.execute(
                "SELECT id, session_id, run_ts, dataset_names, schema_hash, dq_score, "
                "dq_issue_count, etl_phase, etl_engine, etl_outcome, generation_mode, notes "
                "FROM pipeline_runs WHERE session_id = ? ORDER BY run_ts DESC LIMIT 1",
                (sid,)
            ).fetchone()
            if not row:
                return None
            try:
                ds_list = json.loads(row[3] or "[]")
            except Exception:
                ds_list = []
            return {
                "id": row[0],
                "session_id": row[1],
                "run_ts": row[2],
                "dataset_names": ds_list,
                "schema_hash": row[4],
                "dq_score": row[5],
                "dq_issue_count": row[6],
                "etl_phase": row[7],
                "etl_engine": row[8],
                "etl_outcome": row[9],
                "generation_mode": row[10],
                "notes": row[11],
            }
    finally:
        conn.close()


def get_pipeline_runs_for_datasets(
    dataset_names: List[str],
    limit: int = 10
) -> List[Dict[str, Any]]:
    if not dataset_names:
        return []
    conn = _connect()
    try:
        clauses = []
        params = []
        for name in dataset_names:
            safe = name.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            clauses.append("dataset_names LIKE ? ESCAPE '\\'")
            params.append(f'%"{safe}"%')
            
        sql = (
            "SELECT id, session_id, run_ts, dataset_names, schema_hash, dq_score, "
            "dq_issue_count, etl_phase, etl_engine, etl_outcome, generation_mode, notes "
            f"FROM pipeline_runs WHERE {' OR '.join(clauses)} ORDER BY run_ts DESC"
        )
        rows = conn.execute(sql, params).fetchall()
        
        out = []
        target_set = set(dataset_names)
        for r in rows:
            try:
                ds_list = json.loads(r[3] or "[]")
                if target_set.intersection(ds_list):
                    out.append({
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
                    if len(out) >= limit:
                        break
            except Exception:
                pass
    finally:
        conn.close()


def list_recent_experiences(
    session_id: str,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    sid = (session_id or "default").strip() or "default"
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT ts, user_text, action, success, notes FROM experiences "
            "WHERE session_id = ? ORDER BY ts DESC LIMIT ?",
            (sid, limit),
        ).fetchall()
        return [
            {
                "ts": r[0],
                "user_text": r[1],
                "action": r[2],
                "success": bool(r[3]),
                "notes": r[4],
            }
            for r in rows
        ]
    finally:
        conn.close()


# Backward compatibility aliases
log_experience = add_experience
get_experiences = list_recent_experiences


def save_pipeline_run(
    session_id: str,
    *,
    dataset_names: List[str],
    schema_hash: str,
    dq_score: int,
    dq_issue_count: int,
    etl_phase: str = "",
    etl_engine: str = "",
    etl_outcome: str = "",
    generation_mode: str = "",
    notes: str = ""
) -> int:
    sid = (session_id or "default").strip() or "default"
    now = time.time()
    conn = _connect()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO pipeline_runs (
                session_id, run_ts, dataset_names, schema_hash, dq_score,
                dq_issue_count, etl_phase, etl_engine, etl_outcome,
                generation_mode, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sid,
                now,
                json.dumps(dataset_names, cls=SessionJSONEncoder),
                schema_hash,
                dq_score,
                dq_issue_count,
                etl_phase,
                etl_engine,
                etl_outcome,
                generation_mode,
                notes
            )
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def get_latest_pipeline_run(
    session_id: str,
    dataset_names: Optional[List[str]] = None
) -> Optional[Dict[str, Any]]:
    sid = (session_id or "default").strip() or "default"
    conn = _connect()
    try:
        if dataset_names:
            rows = conn.execute(
                "SELECT id, session_id, run_ts, dataset_names, schema_hash, dq_score, "
                "dq_issue_count, etl_phase, etl_engine, etl_outcome, generation_mode, notes "
                "FROM pipeline_runs WHERE session_id = ? ORDER BY run_ts DESC",
                (sid,)
            ).fetchall()
            
            target_set = set(dataset_names)
            for r in rows:
                try:
                    ds_list = json.loads(r[3] or "[]")
                    if target_set.intersection(ds_list):
                        return {
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
                        }
                except Exception:
                    pass
            return None
        else:
            row = conn.execute(
                "SELECT id, session_id, run_ts, dataset_names, schema_hash, dq_score, "
                "dq_issue_count, etl_phase, etl_engine, etl_outcome, generation_mode, notes "
                "FROM pipeline_runs WHERE session_id = ? ORDER BY run_ts DESC LIMIT 1",
                (sid,)
            ).fetchone()
            if not row:
                return None
            try:
                ds_list = json.loads(row[3] or "[]")
            except Exception:
                ds_list = []
            return {
                "id": row[0],
                "session_id": row[1],
                "run_ts": row[2],
                "dataset_names": ds_list,
                "schema_hash": row[4],
                "dq_score": row[5],
                "dq_issue_count": row[6],
                "etl_phase": row[7],
                "etl_engine": row[8],
                "etl_outcome": row[9],
                "generation_mode": row[10],
                "notes": row[11],
            }
    finally:
        conn.close()


def get_pipeline_runs_for_datasets(
    dataset_names: List[str],
    limit: int = 10
) -> List[Dict[str, Any]]:
    if not dataset_names:
        return []
    conn = _connect()
    try:
        clauses = []
        params = []
        for name in dataset_names:
            safe = name.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            clauses.append("dataset_names LIKE ? ESCAPE '\\'")
            params.append(f'%"{safe}"%')
            
        sql = (
            "SELECT id, session_id, run_ts, dataset_names, schema_hash, dq_score, "
            "dq_issue_count, etl_phase, etl_engine, etl_outcome, generation_mode, notes "
            f"FROM pipeline_runs WHERE {' OR '.join(clauses)} ORDER BY run_ts DESC"
        )
        rows = conn.execute(sql, params).fetchall()
        
        out = []
        target_set = set(dataset_names)
        for r in rows:
            try:
                ds_list = json.loads(r[3] or "[]")
                if target_set.intersection(ds_list):
                    out.append({
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
                    if len(out) >= limit:
                        break
            except Exception:
                pass
        return out
    finally:
        conn.close()


import threading
from collections import OrderedDict

_MAX_SESSION_LOCKS = 1000
_SESSION_LOCKS: OrderedDict[str, threading.Lock] = OrderedDict()
_SESSION_LOCK_META = threading.Lock()

def get_session_lock(session_id: str) -> threading.Lock:
    sid = (session_id or "default").strip() or "default"
    with _SESSION_LOCK_META:
        if sid in _SESSION_LOCKS:
            _SESSION_LOCKS.move_to_end(sid)
            return _SESSION_LOCKS[sid]
        
        # Evict oldest unlocked lock if capacity reached
        if len(_SESSION_LOCKS) >= _MAX_SESSION_LOCKS:
            to_remove = None
            for key, lck in _SESSION_LOCKS.items():
                if not lck.locked():
                    to_remove = key
                    break
            if to_remove:
                _SESSION_LOCKS.pop(to_remove)
            elif len(_SESSION_LOCKS) >= _MAX_SESSION_LOCKS * 2:
                # Fallback if all locks are held: pop oldest anyway
                _SESSION_LOCKS.popitem(last=False)
                
        lock = threading.Lock()
        _SESSION_LOCKS[sid] = lock
        return lock
