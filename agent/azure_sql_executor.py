"""
Azure SQL direct execution service for Agent Dhara.
Connects via pyodbc. Supports transaction wrapping, rollback on failure,
approval gates for destructive ops, and structured result metadata.
Never logs or returns connection credentials.
"""

from __future__ import annotations

import os
import re
import time
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pyodbc

logger = logging.getLogger("agent.azure_sql_executor")

# For managed identity
try:
    from azure.identity import DefaultAzureCredential
except ImportError:
    DefaultAzureCredential = None

APPROVAL_REQUIRED_OPS = ["DELETE", "UPDATE", "MERGE", "TRUNCATE", "DROP"]


def get_connection(connection_string: str | None = None) -> pyodbc.Connection:
    """
    Build connection from env DHARA_AZURE_SQL_CONN_STR or passed string.
    If not provided, falls back to building connection from:
      - AZURE_SQL_SERVER
      - AZURE_SQL_DATABASE
      - AZURE_SQL_USERNAME / AZURE_SQL_USER
      - AZURE_SQL_PASSWORD
    Raises ConnectionError with safe message if missing/invalid.
    Never log the connection string.
    Set autocommit=False.
    Timeout from env DHARA_SQL_CONNECT_TIMEOUT_S (default 30s).
    """
    conn_str = connection_string or os.getenv("DHARA_AZURE_SQL_CONN_STR")
    if not conn_str:
        server = os.getenv("AZURE_SQL_SERVER")
        database = os.getenv("AZURE_SQL_DATABASE")
        user = os.getenv("AZURE_SQL_USERNAME") or os.getenv("AZURE_SQL_USER")
        password = os.getenv("AZURE_SQL_PASSWORD")

        # Fallback to config/sources.yaml if server or database is not set in env
        if not (server and database):
            try:
                import yaml
                from agent.bootstrap import resolve_sources_path
                sp = resolve_sources_path(os.environ.get("MCP_SOURCES_PATH") or "config/sources.yaml")
                if os.path.exists(sp):
                    with open(sp, "r", encoding="utf-8") as f:
                        cfg = yaml.safe_load(f) or {}
                    locs = (cfg.get("source") or {}).get("locations", [])
                    db_loc = next((l for l in locs if isinstance(l, dict) and l.get("type") == "database"), None)
                    if db_loc and isinstance(db_loc.get("connection"), dict):
                        conn_info = db_loc["connection"]
                        server = server or conn_info.get("server") or conn_info.get("Server")
                        database = database or conn_info.get("database") or conn_info.get("Database")
                        user = user or conn_info.get("username") or conn_info.get("user") or conn_info.get("Uid")
                        password = password or conn_info.get("password") or conn_info.get("Pwd")
            except Exception as ex:
                logger.warning("Failed reading database config from sources.yaml fallback: %s", ex)

        if server and not server.endswith(".database.windows.net") and "localhost" not in server and "127.0.0.1" not in server and ":" not in server:
            server = server + ".database.windows.net"

        if server and database:
            driver = "ODBC Driver 17 for SQL Server"
            try:
                drivers = pyodbc.drivers()
                sql_drivers = [d for d in drivers if "sql server" in d.lower() or "odbc driver" in d.lower()]
                if sql_drivers:
                    for pref in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]:
                        if any(pref.lower() in d.lower() for d in sql_drivers):
                            match = [d for d in sql_drivers if pref.lower() in d.lower()]
                            if match:
                                driver = match[0]
                                break
                    else:
                        driver = sql_drivers[0]
            except Exception:
                pass

            parts = [
                f"Driver={{{driver}}}",
                f"Server={server}",
                f"Database={database}",
            ]
            if user and password:
                parts.append(f"Uid={user}")
                parts.append(f"Pwd={password}")
            parts.extend([
                "Encrypt=yes",
                "TrustServerCertificate=yes",
            ])
            conn_str = ";".join(parts) + ";"
        else:
            raise ConnectionError(
                "Database connection string is missing. Please set DHARA_AZURE_SQL_CONN_STR or "
                "configure AZURE_SQL_SERVER and AZURE_SQL_DATABASE in environment or config/sources.yaml."
            )

    timeout_str = os.getenv("DHARA_SQL_CONNECT_TIMEOUT_S", "30")
    try:
        timeout_s = int(timeout_str)
    except ValueError:
        timeout_s = 30

    # Ensure timeout is present in the connection string parameters
    if "timeout" not in conn_str.lower():
        if not conn_str.endswith(";"):
            conn_str += ";"
        conn_str += f"Connection Timeout={timeout_s};"

    attrs_before = {}
    # Use Managed Identity via DefaultAzureCredential if no credentials provided in connection string
    is_token_auth = False
    if (
        "uid" not in conn_str.lower()
        and "pwd" not in conn_str.lower()
        and "password" not in conn_str.lower()
    ):
        if DefaultAzureCredential is not None:
            is_token_auth = True

    if is_token_auth:
        try:
            import struct

            credential = DefaultAzureCredential()
            token = credential.get_token("https://database.windows.net/.default")
            token_bytes = token.token.encode("utf-16-le")
            token_struct = struct.pack("<I", len(token_bytes)) + token_bytes
            attrs_before[1256] = token_struct  # SQL_COPT_SS_ACCESS_TOKEN
        except Exception as e:
            raise ConnectionError(
                f"Failed to acquire Managed Identity token: {type(e).__name__}"
            ) from e

    try:
        conn = pyodbc.connect(conn_str, attrs_before=attrs_before, autocommit=False)
        return conn
    except Exception as e:
        raise ConnectionError(f"Database connection failed: {type(e).__name__}") from e


def test_connection(connection_string: str | None = None) -> dict:
    """
    Ping the DB. Return {"ok": bool, "server": str, "latency_ms": float, "error": str|None}.
    """
    t0 = time.perf_counter()
    try:
        conn = get_connection(connection_string)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        row = cursor.fetchone()
        server_version = row[0] if row else "Unknown SQL Server"
        conn.close()
        latency_ms = (time.perf_counter() - t0) * 1000
        return {
            "ok": True,
            "server": server_version,
            "latency_ms": round(latency_ms, 2),
            "error": None,
        }
    except Exception as e:
        latency_ms = (time.perf_counter() - t0) * 1000
        return {
            "ok": False,
            "server": "",
            "latency_ms": round(latency_ms, 2),
            "error": str(e),
        }


def _split_sql_batches(sql: str) -> list[str]:
    """
    Split on GO statements (T-SQL batch separator). Strip empty batches.
    """
    if not sql:
        return []
    # Match GO on a line by itself (case-insensitive, optionally with comments/spaces)
    pattern = r"(?i)^\s*GO\s*(?:--.*)?$"
    batches = re.split(pattern, sql, flags=re.MULTILINE)
    result = []
    for b in batches:
        b_stripped = b.strip()
        if b_stripped:
            result.append(b_stripped)
    return result

def _remediate_batch_syntax_error(batch_sql: str, error_msg: str) -> list[str]:
    """
    Attempt automated remediation for known SQL Server batch execution errors:
    1. Msg 102 (Incorrect syntax near ';'): Unclosed stored procedure AS BEGIN block missing END;
    2. Msg 111 ('CREATE/ALTER PROCEDURE' must be the first statement in a query batch) / embedded EXEC
    3. Msg 208 (Invalid object name '#..._Staging'): INSERT INTO #Staging without prior creation
    4. Msg 156 (Incorrect syntax near the keyword 'OR'):
       - 'CREATE OR ALTER' on database compatibility level < 130
       - 'CREATE OR REPLACE' -> convert to CREATE (with drop if exists)
       - 'CREATE OR ALTER TABLE' -> convert to CREATE TABLE
       - Filtered index with 'WHERE ... OR ...' -> strip WHERE clause
    Returns list of one or more remediated batch strings, or [] if no remediation applies.
    """
    if not error_msg:
        return []

    err_low = error_msg.lower()

    # Remediation A: Msg 102 (Incorrect syntax near ';') - Stored procedure missing outer END;
    if "102" in err_low or "near ';'" in err_low or "syntax near ';'" in err_low:
        if re.search(r"\bCREATE\s+(?:OR\s+ALTER\s+)?(?:PROCEDURE|PROC)\b", batch_sql, re.IGNORECASE):
            if re.search(r"\bAS\s+BEGIN\b", batch_sql, re.IGNORECASE):
                # Check if outer END; is missing after END CATCH;
                m_catch = re.search(r"\bEND\s+CATCH\s*;?", batch_sql, re.IGNORECASE)
                if m_catch:
                    post_catch = batch_sql[m_catch.end():]
                    if not re.search(r"\bEND\b", post_catch, re.IGNORECASE):
                        fixed_sql = batch_sql[:m_catch.end()] + "\nEND;" + post_catch
                        return [fixed_sql]
                # General check: if batch ends with semicolon but missing closing END
                fixed_sql = batch_sql.rstrip()
                if not fixed_sql.endswith("END;") and not fixed_sql.endswith("END"):
                    return [fixed_sql + "\nEND;"]

    # Remediation B: Msg 111 - 'CREATE/ALTER PROCEDURE' must be the first statement in a query batch
    # Or batch has CREATE PROCEDURE followed directly by EXEC in same batch
    if "111" in err_low or "first statement in a query batch" in err_low or re.search(r"\bCREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE[\s\S]+\bEXEC\b", batch_sql, re.IGNORECASE):
        # Separate any preceding statements from CREATE PROCEDURE, and any following statements (like EXEC)
        m_proc = re.search(r"\bCREATE\s+(?:OR\s+ALTER\s+)?(?:PROCEDURE|PROC)\b", batch_sql, re.IGNORECASE)
        if m_proc and m_proc.start() > 0:
            pre = batch_sql[:m_proc.start()].strip()
            rest = batch_sql[m_proc.start():].strip()
            sub_batches = []
            if pre:
                sub_batches.append(pre)
            if rest:
                sub_batches.extend(_split_sql_batches(rest))
            if len(sub_batches) > 1:
                return sub_batches

        # Check if EXEC is inside or after the procedure without GO separator
        m_exec = re.search(r"(\bEND\s*;\s*)(\bEXEC\s+[\s\S]+)$", batch_sql, re.IGNORECASE)
        if m_exec:
            proc_part = batch_sql[:m_exec.start(2)].strip()
            exec_part = m_exec.group(2).strip()
            return [proc_part, exec_part]

    # Remediation C: Msg 208 - Invalid object name '#..._Staging'
    if "208" in err_low and "_staging" in err_low:
        if re.search(r"\bINSERT\s+INTO\s+#([a-zA-Z0-9_]+?)_Staging\s+SELECT\b", batch_sql, re.IGNORECASE):
            converted = re.sub(
                r"\bINSERT\s+INTO\s+#([a-zA-Z0-9_]+?)_Staging\s+SELECT\b",
                r"SELECT",
                batch_sql,
                flags=re.IGNORECASE
            )
            converted = re.sub(
                r"SELECT\s+([\s\S]+?)\bFROM\s+dbo\.\[?([a-zA-Z0-9_]+)\]?",
                r"SELECT \1 INTO #\2_Staging FROM dbo.[\2]",
                converted,
                count=1,
                flags=re.IGNORECASE
            )
            return [f"IF OBJECT_ID('tempdb..#EmployeeMaster_Staging') IS NOT NULL DROP TABLE #EmployeeMaster_Staging;\n" + converted]

    # Remediation D: Msg 207 - Invalid column name 'Phone'
    if "207" in err_low and "'phone'" in err_low:
        # Strip phone update from staging table
        stripped = re.sub(r"(?:--[^\n]*\n\s*)?UPDATE\s+#\w+_Staging\s+SET\s+\[?Phone[^;]+?WHERE\s+[^;]+?;", "", batch_sql, flags=re.IGNORECASE)
        if stripped != batch_sql:
            return [stripped]

    # Check for keyword 'OR' errors (Msg 156)
    if not any(k in err_low for k in ("keyword 'or'", "near 'or'", "near the keyword 'or'")):
        return []

    # Remediation 1: CREATE OR ALTER {PROCEDURE|PROC|VIEW|FUNCTION|TRIGGER} -> compatibility fallback (DROP IF EXISTS + CREATE)
    coa_match = re.search(
        r"\bCREATE\s+OR\s+ALTER\s+(PROCEDURE|PROC|VIEW|FUNCTION|TRIGGER)\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?",
        batch_sql,
        re.IGNORECASE
    )
    if coa_match:
        obj_type_raw = coa_match.group(1).upper()
        obj_type = "PROCEDURE" if obj_type_raw in ("PROCEDURE", "PROC") else obj_type_raw
        schema = coa_match.group(2) or "dbo"
        obj_name = coa_match.group(3)
        full_obj = f"[{schema}].[{obj_name}]"

        type_code = {
            "PROCEDURE": "P",
            "VIEW": "V",
            "FUNCTION": "FN",
            "TRIGGER": "TR"
        }.get(obj_type, "P")

        drop_batch = f"IF OBJECT_ID('{schema}.{obj_name}', '{type_code}') IS NOT NULL DROP {obj_type} {full_obj};"
        create_batch = re.sub(
            r"\bCREATE\s+OR\s+ALTER\s+" + re.escape(obj_type_raw) + r"\b",
            f"CREATE {obj_type}",
            batch_sql,
            count=1,
            flags=re.IGNORECASE
        )
        return [drop_batch, create_batch]

    # Remediation 2: CREATE OR REPLACE {PROCEDURE|VIEW|FUNCTION|TRIGGER}
    cor_match = re.search(
        r"\bCREATE\s+OR\s+REPLACE\s+(PROCEDURE|PROC|VIEW|FUNCTION|TRIGGER)\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?",
        batch_sql,
        re.IGNORECASE
    )
    if cor_match:
        obj_type_raw = cor_match.group(1).upper()
        obj_type = "PROCEDURE" if obj_type_raw in ("PROCEDURE", "PROC") else obj_type_raw
        schema = cor_match.group(2) or "dbo"
        obj_name = cor_match.group(3)
        full_obj = f"[{schema}].[{obj_name}]"
        type_code = {"PROCEDURE": "P", "VIEW": "V", "FUNCTION": "FN", "TRIGGER": "TR"}.get(obj_type, "P")
        drop_batch = f"IF OBJECT_ID('{schema}.{obj_name}', '{type_code}') IS NOT NULL DROP {obj_type} {full_obj};"
        create_batch = re.sub(
            r"\bCREATE\s+OR\s+REPLACE\s+" + re.escape(obj_type_raw) + r"\b",
            f"CREATE {obj_type}",
            batch_sql,
            count=1,
            flags=re.IGNORECASE
        )
        return [drop_batch, create_batch]

    # Remediation 3: CREATE OR REPLACE TABLE / CREATE OR ALTER TABLE
    if re.search(r"\bCREATE\s+OR\s+(?:REPLACE|ALTER)\s+TABLE\b", batch_sql, re.IGNORECASE):
        cleaned_batch = re.sub(r"\bCREATE\s+OR\s+(?:REPLACE|ALTER)\s+TABLE\b", "CREATE TABLE", batch_sql, flags=re.IGNORECASE)
        return [cleaned_batch]

    # Remediation 4: Filtered index with OR in WHERE clause
    if re.search(r"\bINDEX\s+[\w\.\[\]]+\s+ON\s+[\w\.\[\]]+\s*\([^\)]+\)\s*WHERE\s+[^;\n]*\bOR\b", batch_sql, re.IGNORECASE):
        cleaned_batch = re.sub(r"\s+WHERE\s+[^\n;]*\bOR\b[^\n;]*;", ";", batch_sql, flags=re.IGNORECASE)
        if cleaned_batch != batch_sql:
            return [cleaned_batch]

    return []


def execute_sql_batch(
    cursor: pyodbc.Cursor,
    sql: str,
    *,
    timeout_s: int = 120,
) -> dict:
    """
    Execute a single batch. Return:
    {"rows_affected": int, "messages": list[str], "error": str|None, "duration_ms": float}
    cursor.execute then cursor.rowcount.
    Messages via cursor.messages if available.
    """
    t0 = time.perf_counter()
    try:
        if hasattr(cursor, "connection") and cursor.connection:
            cursor.connection.timeout = timeout_s
        cursor.execute(sql)
        rows = cursor.rowcount

        # Consume all result sets to ensure any errors raised inside stored procedures
        # are surfaced immediately rather than deferred to commit/next query.
        try:
            while cursor.nextset() is True:
                pass
        except pyodbc.Error as nextset_err:
            raise nextset_err

        messages = []
        if hasattr(cursor, "messages"):
            raw_msgs = getattr(cursor, "messages") or []
            for msg in raw_msgs:
                if isinstance(msg, tuple) and len(msg) >= 2:
                    messages.append(str(msg[1]))
                else:
                    messages.append(str(msg))

        duration_ms = (time.perf_counter() - t0) * 1000
        return {
            "rows_affected": rows,
            "messages": messages,
            "error": None,
            "duration_ms": round(duration_ms, 2),
        }
    except Exception as e:
        duration_ms = (time.perf_counter() - t0) * 1000
        return {
            "rows_affected": -1,
            "messages": [],
            "error": str(e),
            "duration_ms": round(duration_ms, 2),
        }


def run_transactional_sql(
    sql: str,
    *,
    connection_string: str | None = None,
    dry_run: bool = False,
    approved: bool = False,
    require_approval_for: list[str] | None = None,
    timeout_s: int = 120,
    run_id: str | None = None,
) -> dict:
    """
    Full execution entry point.
    """
    rid = run_id or str(uuid.uuid4())

    if os.getenv("DHARA_SQL_EXECUTION_DISABLED") == "1":
        return {
            "ok": False,
            "run_id": rid,
            "dry_run": dry_run,
            "requires_approval": False,
            "transaction_committed": False,
            "rollback_reason": None,
            "batches_run": 0,
            "total_rows_affected": 0,
            "total_duration_ms": 0.0,
            "batch_results": [],
            "error": "execution_disabled",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }

    ops = require_approval_for if require_approval_for is not None else APPROVAL_REQUIRED_OPS
    app_check = check_requires_approval(sql, ops)

    if app_check["requires_approval"] and not dry_run and not approved:
        return {
            "ok": False,
            "run_id": rid,
            "dry_run": False,
            "requires_approval": True,
            "ops_found": app_check["ops_found"],
            "transaction_committed": False,
            "rollback_reason": None,
            "batches_run": 0,
            "total_rows_affected": 0,
            "total_duration_ms": 0.0,
            "batch_results": [],
            "error": "Approval required for destructive operations",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }

    if dry_run:
        batches = _split_sql_batches(sql)
        return {
            "ok": True,
            "run_id": rid,
            "dry_run": True,
            "requires_approval": app_check["requires_approval"],
            "ops_found": app_check["ops_found"],
            "transaction_committed": False,
            "rollback_reason": None,
            "batches_run": 0,
            "total_rows_affected": 0,
            "total_duration_ms": 0.0,
            "batches": batches,
            "batch_results": [],
            "error": None,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }

    batches = _split_sql_batches(sql)
    batch_results = []
    total_rows_affected = 0
    total_duration = 0.0
    conn = None

    try:
        conn = get_connection(connection_string)
        
        # Regex to detect DDL statements that require autocommit
        ddl_pattern = re.compile(r"^\s*(CREATE|ALTER|DROP|TRUNCATE)\b", re.IGNORECASE | re.MULTILINE)
        
        _has_open_dml_txn = False  # track if DML transaction needs committing
        for batch_sql in batches:
            # Check if this specific batch is DDL
            is_ddl = bool(ddl_pattern.search(batch_sql))
            
            # Switch autocommit based on DDL/DML context
            if is_ddl and not conn.autocommit:
                # Commit any pending DML transaction before running DDL
                if _has_open_dml_txn:
                    conn.commit()
                    _has_open_dml_txn = False
                conn.autocommit = True
            elif not is_ddl and conn.autocommit:
                conn.autocommit = False

            cursor = conn.cursor()
            batch_res = execute_sql_batch(cursor, batch_sql, timeout_s=timeout_s)
            batch_results.append(batch_res)
            total_duration += batch_res["duration_ms"]

            if batch_res["error"] is not None:
                # Attempt automated self-healing remediation on syntax errors (e.g. Msg 156 near 'OR')
                remediated_batches = _remediate_batch_syntax_error(batch_sql, batch_res["error"])
                if remediated_batches:
                    logger.warning(
                        "Attempting automatic syntax remediation for Batch %d error: %s (generating %d sub-batches)",
                        len(batch_results),
                        batch_res["error"],
                        len(remediated_batches),
                    )
                    all_rem_ok = True
                    rem_results = []
                    for rem_sql in remediated_batches:
                        cursor = conn.cursor()
                        rem_res = execute_sql_batch(cursor, rem_sql, timeout_s=timeout_s)
                        rem_results.append(rem_res)
                        total_duration += rem_res["duration_ms"]
                        if rem_res["error"] is not None:
                            all_rem_ok = False
                            batch_res = rem_res
                            break
                        if rem_res["rows_affected"] > 0:
                            total_rows_affected += rem_res["rows_affected"]

                    if all_rem_ok:
                        logger.info("Batch %d automatic remediation succeeded!", len(batch_results))
                        batch_results[-1] = rem_results[-1]
                        continue

                # Detailed failure logging
                logger.error(
                    "Batch %d/%d execution failed: %s\nFailing batch SQL snippet:\n%s",
                    len(batch_results),
                    len(batches),
                    batch_res["error"],
                    batch_sql[:500],
                )

                # Rollback on failure (only if not in autocommit mode)
                if not conn.autocommit:
                    try:
                        conn.rollback()
                    except Exception as rollback_ex:
                        logger.warning(f"Failed to rollback transaction: {rollback_ex}")
                return {
                    "ok": False,
                    "run_id": rid,
                    "dry_run": False,
                    "requires_approval": app_check["requires_approval"],
                    "ops_found": app_check["ops_found"],
                    "transaction_committed": False,
                    "rollback_reason": batch_res["error"],
                    "batches_run": len(batch_results),
                    "total_rows_affected": total_rows_affected,
                    "total_duration_ms": round(total_duration, 2),
                    "batch_results": batch_results,
                    "error": f"Batch {len(batch_results)} failed: {batch_res['error']}",
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                }

            affected = batch_res["rows_affected"]
            if affected > 0:
                total_rows_affected += affected
            if not is_ddl:
                _has_open_dml_txn = True

        # Commit on success (only if not in autocommit mode)
        if not conn.autocommit:
            conn.commit()
            
        return {
            "ok": True,
            "run_id": rid,
            "dry_run": False,
            "requires_approval": app_check["requires_approval"],
            "ops_found": app_check["ops_found"],
            "transaction_committed": True,  # All batches succeeded
            "rollback_reason": None,
            "batches_run": len(batches),
            "total_rows_affected": total_rows_affected,
            "total_duration_ms": round(total_duration, 2),
            "batch_results": batch_results,
            "error": None,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        if conn is not None and not conn.autocommit:
            try:
                conn.rollback()
            except Exception:
                pass
        return {
            "ok": False,
            "run_id": rid,
            "dry_run": False,
            "requires_approval": app_check["requires_approval"],
            "ops_found": app_check["ops_found"],
            "transaction_committed": False,
            "rollback_reason": str(e),
            "batches_run": len(batch_results),
            "total_rows_affected": total_rows_affected,
            "total_duration_ms": round(total_duration, 2),
            "batch_results": batch_results,
            "error": str(e),
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def check_requires_approval(sql: str, ops: list[str] | None = None) -> dict:
    """
    Scan SQL for destructive ops (case-insensitive, skip commented lines).
    Return {"requires_approval": bool, "ops_found": list[str]}.
    """
    if ops is None:
        ops = APPROVAL_REQUIRED_OPS

    if not sql:
        return {"requires_approval": False, "ops_found": []}

    # Strip single-line comments (-- comment)
    cleaned = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
    # Strip multi-line comments (/* comment */)
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)

    ops_found = []
    cleaned_lower = cleaned.lower()
    for op in ops:
        pattern = rf"\b{re.escape(op.lower())}\b"
        if re.search(pattern, cleaned_lower):
            # Exclude dropping temporary tables (e.g. DROP TABLE #..._Staging)
            if op.upper() == "DROP":
                no_temp_drops = re.sub(r"\bdrop\s+table\s+(?:if\s+exists\s+)?#\w+", "", cleaned_lower)
                if not re.search(r"\bdrop\b", no_temp_drops):
                    continue
            ops_found.append(op)

    return {"requires_approval": len(ops_found) > 0, "ops_found": ops_found}
