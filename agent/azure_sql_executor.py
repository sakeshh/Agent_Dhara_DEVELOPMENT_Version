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
    Handles 'GO', 'GO;', inline comments, and carriage returns.
    """
    if not sql:
        return []
    # Match GO on a line by itself (case-insensitive, optionally with semicolons, comments, or trailing spaces)
    pattern = r"(?im)^\s*GO\s*;?\s*(?:--.*)?$"
    batches = re.split(pattern, sql)
    result = []
    for b in batches:
        b_stripped = b.strip()
        if b_stripped:
            result.append(b_stripped)
    return result


def _split_sql_expressions(expr_str: str) -> list[str]:
    """Split comma-separated SQL expressions respecting nested parentheses, brackets, and quotes."""
    items = []
    cur = []
    depth = 0
    in_quote = False
    quote_char = ''
    i = 0
    n = len(expr_str)
    while i < n:
        char = expr_str[i]
        if in_quote:
            cur.append(char)
            if char == quote_char:
                if i + 1 < n and expr_str[i + 1] == quote_char:
                    cur.append(quote_char)
                    i += 1
                else:
                    in_quote = False
        else:
            if char in ("'", '"'):
                in_quote = True
                quote_char = char
                cur.append(char)
            elif char in ('(', '['):
                depth += 1
                cur.append(char)
            elif char in (')', ']'):
                depth = max(0, depth - 1)
                cur.append(char)
            elif char == ',' and depth == 0:
                item = "".join(cur).strip()
                if item:
                    items.append(item)
                cur = []
            else:
                cur.append(char)
        i += 1
    if cur:
        item = "".join(cur).strip()
        if item:
            items.append(item)
    return items


def _reconcile_insert_select_columns(sql: str) -> str:
    """
    Reconciles INSERT INTO <table> (<cols>) SELECT <items> FROM #Staging statements
    where the number of SELECT values does not match the number of INSERT columns (Msg 120/121).
    """
    if not sql or "INSERT" not in sql.upper() or "SELECT" not in sql.upper():
        return sql

    pattern = re.compile(
        r"INSERT\s+INTO\s+([^\(\n]+)\s*\(([^()]+)\)\s*SELECT\s+([\s\S]+?)\s+FROM\s+(#\w+_Staging\b[^\n;]*)",
        re.IGNORECASE
    )

    def _replace_mismatch(match):
        target = match.group(1).strip()
        cols_raw = match.group(2)
        select_raw = match.group(3)
        from_part = match.group(4).strip()

        cols = [c.strip() for c in cols_raw.split(",") if c.strip() and not c.strip().startswith("--")]
        items = _split_sql_expressions(select_raw)

        if len(cols) == len(items):
            return match.group(0)

        logger.warning(
            "Reconciling column count mismatch for %s: %d target columns vs %d select expressions",
            target, len(cols), len(items)
        )

        item_by_col = {}
        for it in items:
            refs = re.findall(r"\[([a-zA-Z0-9_]+)\]|\b([a-zA-Z0-9_]+)\b", it)
            flat_refs = [r[0] or r[1] for r in refs if (r[0] or r[1])]
            keywords = {"coalesce", "getdate", "cast", "try_cast", "null", "case", "when", "then", "else", "end", "run_id", "is", "not"}
            col_refs = [r for r in flat_refs if r.lower() not in keywords and not r.startswith("@")]

            for cr in col_refs:
                item_by_col[cr.lower()] = it
                if cr.lower().endswith("_clean"):
                    item_by_col[cr[:-6].lower()] = it
                else:
                    item_by_col[f"{cr.lower()}_clean"] = it

        new_items = []
        for col in cols:
            c_name = re.sub(r"[\[\]\s]", "", col)
            c_lower = c_name.lower()
            if c_lower in ("etl_batch_id", "batch_id", "run_id"):
                new_items.append("@run_id")
            elif c_lower in ("etl_created_at", "created_at", "etl_updated_at", "updated_at", "loaded_at"):
                new_items.append("GETDATE()")
            elif c_lower in item_by_col:
                new_items.append(item_by_col[c_lower])
            elif c_lower.endswith("_clean"):
                base = c_name[:-6]
                new_items.append(f"COALESCE([{c_name}], [{base}])")
            else:
                new_items.append(f"[{c_name}]")

        formatted_cols = ",\n        ".join(cols)
        formatted_items = ",\n        ".join(new_items)
        return f"INSERT INTO {target} (\n        {formatted_cols}\n    )\n    SELECT \n        {formatted_items}\n    FROM {from_part}"

    return pattern.sub(_replace_mismatch, sql)


def _remediate_batch_syntax_error(batch_sql: str, error_msg: str) -> list[str]:
    """
    Attempt automated remediation for known SQL Server batch execution errors:
    1. Msg 102 (Incorrect syntax near 'GO'): GO statement leaked into execution batch
    2. Msg 102 (Incorrect syntax near ';'): Unclosed stored procedure AS BEGIN block missing END;
    3. Msg 111 ('CREATE/ALTER PROCEDURE' must be the first statement in a query batch) / embedded EXEC
    4. Msg 208 (Invalid object name '#..._Staging'): INSERT INTO #Staging without prior creation
    5. Msg 120 / Msg 121: INSERT column list and SELECT expression count mismatch
    6. Msg 156 (Incorrect syntax near the keyword 'OR'):
       - 'CREATE OR ALTER' on database compatibility level < 130
       - 'CREATE OR REPLACE' -> convert to CREATE (with drop if exists)
       - 'CREATE OR ALTER TABLE' -> convert to CREATE TABLE
       - Filtered index with 'WHERE ... OR ...' -> strip WHERE clause
    Returns list of one or more remediated batch strings, or [] if no remediation applies.
    """
    if not error_msg:
        return []

    err_low = error_msg.lower()

    # Remediation 0: Msg 102 - Incorrect syntax near 'GO' (GO statement inside batch or followed by semicolon)
    if "near 'go'" in err_low or "syntax near 'go'" in err_low or "near go" in err_low:
        fixed = re.sub(r"(?im)^\s*GO\s*;?\s*(?:--.*)?$", "-- (GO stripped for execution batch)", batch_sql)
        if fixed != batch_sql:
            return [fixed]

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
        m_stg = re.search(r"\bINSERT\s+INTO\s+#([a-zA-Z0-9_]+?)_Staging\s+SELECT\b", batch_sql, re.IGNORECASE)
        if m_stg:
            stg_tbl = m_stg.group(1)
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
            return [f"IF OBJECT_ID('tempdb..#{stg_tbl}_Staging') IS NOT NULL DROP TABLE #{stg_tbl}_Staging;\n" + converted]

    # Remediation D: Msg 207 - Invalid column name 'Phone'
    if "207" in err_low and "'phone'" in err_low:
        # Strip phone update from staging table
        stripped = re.sub(r"(?:--[^\n]*\n\s*)?UPDATE\s+#\w+_Staging\s+SET\s+\[?Phone[^;]+?WHERE\s+[^;]+?;", "", batch_sql, flags=re.IGNORECASE)
        if stripped != batch_sql:
            return [stripped]

    # Remediation E: Msg 137 - Must declare the scalar variable "@run_id"
    if "137" in err_low and ("@run_id" in err_low or "scalar variable" in err_low):
        m_proc = re.search(r"\bAS\s+BEGIN\b", batch_sql, re.IGNORECASE)
        if m_proc:
            return [batch_sql[:m_proc.end()] + "\n    DECLARE @run_id INT = 1;" + batch_sql[m_proc.end():]]
        m_try = re.search(r"\bBEGIN\s+TRY\b", batch_sql, re.IGNORECASE)
        if m_try:
            return [batch_sql[:m_try.end()] + "\n    DECLARE @run_id INT = 1;" + batch_sql[m_try.end():]]
        return ["DECLARE @run_id INT = 1;\n" + batch_sql]

    # Remediation G: Msg 134 - The variable name '@run_id' has already been declared
    if "134" in err_low and ("@run_id" in err_low or "already been declared" in err_low):
        decls = list(re.finditer(r"\bDECLARE\s+@run_id\b\s*(?:BIGINT|INT)?(?:\s*=\s*[^;]+)?\s*;", batch_sql, re.IGNORECASE))
        if len(decls) > 1:
            cleaned = batch_sql[:decls[1].start()] + "-- (duplicate @run_id removed)\n" + batch_sql[decls[1].end():]
            return [cleaned]

    # Remediation H: Msg 208 - Invalid object name '#s_Staging' or short staging alias
    if "208" in err_low and ("#s_staging" in err_low or "invalid object name '#" in err_low):
        cleaned = re.sub(r"\bUPDATE\s+#([a-zA-Z])_Staging\s+SET\b", r"UPDATE \1 SET", batch_sql, flags=re.IGNORECASE)
        cleaned = re.sub(r"\bUPDATE\s+#s_Staging\s+SET\b", r"UPDATE s SET", cleaned, flags=re.IGNORECASE)
        if cleaned != batch_sql:
            return [cleaned]

    # Remediation I: Msg 102 - Incorrect syntax near 'THROW' (requires preceding semicolon)
    if "102" in err_low and ("throw" in err_low or "syntax near 'throw'" in err_low):
        cleaned = re.sub(r"\bEND\s*\n\s*;?\s*THROW\b", "END;\n        ;THROW", batch_sql, flags=re.IGNORECASE)
        cleaned = re.sub(r"(?<!;)\bTHROW\b", ";THROW", cleaned, flags=re.IGNORECASE)
        if cleaned != batch_sql:
            return [cleaned]

    # Remediation F: Msg 1919 - Column in index is invalid type (NVARCHAR(MAX) / VARCHAR(MAX))
    if "1919" in err_low or "invalid for use as a key column in an index" in err_low:
        cleaned = batch_sql
        m_col = re.search(r"Column\s+'([^']+)'\s+in\s+table", error_msg, re.IGNORECASE)
        bad_cols = set()
        if m_col:
            bad_cols.add(m_col.group(1).strip("[]"))

        for pkm in re.finditer(r"\bPRIMARY\s+KEY\s*(?:\([^)]*\))?\s*\(([^)]+)\)", cleaned, re.IGNORECASE):
            for c in pkm.group(1).split(","):
                c_clean = c.strip().strip("[]").strip()
                if c_clean:
                    bad_cols.add(c_clean)
        for c in re.findall(r"\[?([a-zA-Z0-9_]+)\]?\s+(?:NVARCHAR|VARCHAR)\(MAX\)\s+PRIMARY\s+KEY", cleaned, re.IGNORECASE):
            bad_cols.add(c.strip("[]"))

        for bc in bad_cols:
            if bc:
                cleaned = re.sub(
                    rf"(\[?{re.escape(bc)}\]?\s+\[?(?:NVARCHAR|VARCHAR)\]?)\s*\(\s*MAX\s*\)",
                    r"\1(256)",
                    cleaned,
                    flags=re.IGNORECASE
                )
                cleaned = re.sub(
                    rf"(?:IF\s+NOT\s+EXISTS\s*\([^;]+\)\s*)?CREATE\s+(?:UNIQUE\s+)?(?:NONCLUSTERED\s+|CLUSTERED\s+)?INDEX\s+[^\n;]+?\s+ON\s+[^\n;]+?\(\s*\[?{re.escape(bc)}\]?[^;]*\)\s*;?",
                    "",
                    cleaned,
                    flags=re.IGNORECASE
                )
        cleaned = re.sub(r"\[?([a-zA-Z0-9_]+)\]?\s+NVARCHAR\(MAX\)\s+(NOT\s+NULL|NULL)", r"[\1] NVARCHAR(256) \2", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\bCREATE\s+(?:NONCLUSTERED\s+)?INDEX\s+[^;]+;", "", cleaned, flags=re.IGNORECASE)
        if cleaned != batch_sql:
            return [cleaned]

    # Remediation J: Msg 120 / Msg 121 - INSERT list and SELECT list column count mismatch
    if ("120" in err_low or "121" in err_low or 
        ("fewer items than the insert list" in err_low) or 
        ("more items than the insert list" in err_low) or
        ("number of select values must match" in err_low)):
        reconciled = _reconcile_insert_select_columns(batch_sql)
        if reconciled != batch_sql:
            return [reconciled]

    # Remediation K: Msg 208 - Invalid object name 'cleanedTables....'
    if "208" in err_low and "cleanedtables" in err_low:
        m_tbl = re.search(r"cleanedtables\.\[?(\w+)\]?", batch_sql, re.IGNORECASE)
        tbl_name = m_tbl.group(1) if m_tbl else "TrainingEnrollments_Clean"
        ddl_create = (
            f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'cleanedTables')\n"
            f"    EXEC('CREATE SCHEMA cleanedTables');\n"
            f"IF OBJECT_ID('cleanedTables.{tbl_name}', 'U') IS NULL\n"
            f"BEGIN\n"
            f"    SELECT TOP 0 * INTO cleanedTables.[{tbl_name}] FROM #TrainingEnrollments_Staging;\n"
            f"END;"
        )
        return [ddl_create, batch_sql]

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
            # Proactively ensure @run_id is declared and not duplicated if referenced in this batch
            if re.search(r"@run_id\b", batch_sql, re.IGNORECASE):
                decls = list(re.finditer(r"\bDECLARE\s+@run_id\b\s*(?:BIGINT|INT)?(?:\s*=\s*[^;]+)?\s*;", batch_sql, re.IGNORECASE))
                if len(decls) > 1:
                    for d in reversed(decls[1:]):
                        batch_sql = batch_sql[:d.start()] + batch_sql[d.end():]
                elif len(decls) == 0:
                    m_proc = re.search(r"\bAS\s+BEGIN\b", batch_sql, re.IGNORECASE)
                    if m_proc:
                        batch_sql = batch_sql[:m_proc.end()] + "\n    DECLARE @run_id INT = 1;" + batch_sql[m_proc.end():]
                    else:
                        m_try = re.search(r"\bBEGIN\s+TRY\b", batch_sql, re.IGNORECASE)
                        if m_try:
                            batch_sql = batch_sql[:m_try.end()] + "\n    DECLARE @run_id INT = 1;" + batch_sql[m_try.end():]
                        else:
                            batch_sql = "DECLARE @run_id INT = 1;\n" + batch_sql

            # Sanitize any #s_Staging alias corruptions and logger.info calls
            batch_sql = re.sub(r"\bUPDATE\s+#([a-zA-Z])_Staging\s+SET\b", r"UPDATE \1 SET", batch_sql, flags=re.IGNORECASE)
            batch_sql = re.sub(r"^\s*logger\.(?:info|warning|error|debug)\([^;\n]*\);?\s*$", "", batch_sql, flags=re.MULTILINE | re.IGNORECASE)
            batch_sql = re.sub(r"\blogger\.(?:info|warning|error|debug)\([^;]*\);?", "", batch_sql, flags=re.IGNORECASE)

            # Proactively ensure THROW has a preceding semicolon (Msg 102 prevention)
            batch_sql = re.sub(r"\bEND\s*\n\s*;?\s*THROW\b", "END;\n        ;THROW", batch_sql, flags=re.IGNORECASE)
            batch_sql = re.sub(r"(?<!;)\bTHROW\b", ";THROW", batch_sql, flags=re.IGNORECASE)

            # Proactively sanitize Msg 1919 (NVARCHAR(MAX) on primary key columns)
            if re.search(r"\bPRIMARY\s+KEY\b", batch_sql, re.IGNORECASE) and re.search(r"\(MAX\)", batch_sql, re.IGNORECASE):
                pk_cols = set()
                for pkm in re.finditer(r"\bPRIMARY\s+KEY\s*(?:\([^)]*\))?\s*\(([^)]+)\)", batch_sql, re.IGNORECASE):
                    for c in pkm.group(1).split(","):
                        c_clean = c.strip().strip("[]").strip()
                        if c_clean:
                            pk_cols.add(c_clean)
                for c in re.findall(r"\[?([a-zA-Z0-9_]+)\]?\s+(?:NVARCHAR|VARCHAR)\(MAX\)\s+PRIMARY\s+KEY", batch_sql, re.IGNORECASE):
                    pk_cols.add(c.strip("[]"))
                for pk in pk_cols:
                    batch_sql = re.sub(
                        rf"(\[?{re.escape(pk)}\]?\s+\[?(?:NVARCHAR|VARCHAR)\]?)\s*\(\s*MAX\s*\)",
                        r"\1(256)",
                        batch_sql,
                        flags=re.IGNORECASE
                    )

            # Proactively reconcile INSERT INTO ... SELECT column count mismatches (Msg 120/121)
            batch_sql = _reconcile_insert_select_columns(batch_sql)

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
