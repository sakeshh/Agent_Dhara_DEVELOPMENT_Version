from __future__ import annotations

import re
from typing import List, Tuple

_DANGEROUS = [
    (r"\bdrop\s+table\s+(?!.*\b\w*(?:_clean|_transformed|_stg|temp_|_temp)\b|.*#)", "contains DROP TABLE on non-staging/clean/transformed table — remove for safety"),
    (r"\btruncate\s+table\s+(?!.*\b\w*(?:_clean|_transformed|_stg|temp_|_temp)\b|.*#)", "contains TRUNCATE TABLE on non-staging/clean/transformed table — remove for safety"),
    (r"\bdelete\s+from\s+(?!.*\b\w*(?:_clean|_transformed|_stg|_dedup|temp_|etl_log|cte|_temp)\b|.*#)", "contains DELETE FROM on non-staging/clean/transformed table — remove for safety"),
    (r"\bxp_cmdshell\b", "contains xp_cmdshell — system command execution not allowed"),
    (r"\bopenrowset\b", "contains OPENROWSET — external data source access not allowed in execution mode"),
]


def _bracket_balance(source: str) -> List[str]:
    issues: List[str] = []
    # Strip block comments, single-line comments, string literals, and bracketed identifiers
    cleaned = re.sub(r"/\*[\s\S]*?\*/", "", source)
    cleaned = re.sub(r"--[^\n]*", "", cleaned)
    cleaned = re.sub(r"'(?:''|[^'])*'", "''", cleaned)
    cleaned = re.sub(r"\[[^\]\n]*\]", "[]", cleaned)

    depth = 0
    for ch in cleaned:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                issues.append("unbalanced parentheses")
                break
    if depth > 0:
        issues.append("unclosed parentheses")
    elif depth < 0 and "unbalanced parentheses" not in issues:
        issues.append("unbalanced parentheses")
    return issues


def _has_fake_default_value(sql: str) -> bool:
    low = sql.lower()
    banned = ["'99999'", "'10120631.5'", "'1900-01-01'", "'19000101'"]
    for b in banned:
        start = 0
        while True:
            idx = low.find(b, start)
            if idx == -1:
                break
            line_start = low.rfind("\n", 0, idx) + 1
            line_end = low.find("\n", idx)
            if line_end == -1:
                line_end = len(low)
            line_content = low[line_start:line_end]
            if "etl_invalid_values" in line_content:
                start = idx + len(b)
                continue

            downstream = low[idx + len(b):idx + len(b) + 150]
            if "then null" in downstream:
                then_idx = downstream.find("then null")
                if ";" not in downstream[:then_idx]:
                    start = idx + len(b)
                    continue
            return True
    return False


def validate_security(sql: str) -> Tuple[bool, List[str]]:
    """Verify security constraints, dangerous operations, and syntax blockers."""
    issues: List[str] = []
    if not sql or not sql.strip():
        return False, ["SQL script is empty"]

    cleaned = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
    if not cleaned.strip():
        return False, ["SQL script is empty after stripping comments"]

    cleaned_lower = cleaned.lower()

    # 1. Raw credentials patterns
    if re.search(r"\b(password|pwd|connectionstring)\s*=", cleaned_lower):
        issues.append("contains raw credential patterns (password, pwd, or connectionstring)")

    # 2. xp_cmdshell system commands
    if re.search(r"\bxp_cmdshell\b", cleaned_lower):
        issues.append("contains xp_cmdshell — system command execution not allowed")

    # 3. OPENROWSET / BULK INSERT external source access
    if re.search(r"\bopenrowset\b", cleaned_lower):
        issues.append("contains OPENROWSET — external data source access not allowed in execution mode")
    if re.search(r"\bbulk\s+insert\b", cleaned_lower):
        issues.append("contains BULK INSERT — external data source access not allowed in execution mode")

    # 4. USE [db] switching — automatically strip for Azure SQL DB compatibility
    cleaned_lower = re.sub(r"(?i)\buse\s+\[?\w+\]?;?\s*", "", cleaned_lower)

    # 5. DANGEROUS DROP/TRUNCATE/DELETE commands
    for pattern, msg in _DANGEROUS:
        if re.search(pattern, cleaned_lower):
            for line in cleaned_lower.splitlines():
                stripped = line.strip()
                if re.search(pattern, stripped) and not stripped.startswith("--"):
                    issues.append(msg)
                    break

    # 6. Bracket balancing (unclosed parens)
    issues.extend(_bracket_balance(sql))

    # 7. BEGIN TRY without matching END CATCH (parsing blocker)
    has_begin_try = "begin try" in cleaned_lower
    has_end_catch = "end catch" in cleaned_lower
    if has_begin_try and not has_end_catch:
        issues.append("BEGIN TRY without matching END CATCH — SQL Server will reject this script")

    # 8. Check for GO inside BEGIN TRY ... END CATCH
    if has_begin_try:
        try_catch_blocks = re.findall(r"begin\s+try([\s\S]*?)end\s+catch", cleaned_lower)
        for block in try_catch_blocks:
            if re.search(r"^\s*go\b", block, flags=re.MULTILINE):
                issues.append("Fatal: 'GO' batch separator inside BEGIN TRY ... END CATCH block")
                break

    # 9. Check for unreplaced template placeholders
    if re.search(r"\{\{[a-zA-Z0-9_]+\}\}", sql):
        issues.append("Unreplaced template placeholder like '{{variable}}' found in SQL")

    try:
        import sqlparse  # type: ignore
        parsed = sqlparse.parse(sql)
        if not parsed or not parsed[0].tokens:
            issues.append("Empty or unparseable SQL")
    except ImportError:
        pass
    except Exception as e:
        # sqlparse has a hardcoded MAX_GROUPING_TOKENS = 10000 limit that triggers on large valid T-SQL scripts
        if "Maximum number of tokens exceeded" in str(e):
            pass
        else:
            issues.append(f"SQL validation error: {str(e)}")

    return len(issues) == 0, issues


def validate_style(sql: str) -> List[str]:
    """Collect SQL style and optimization advisory warnings."""
    warnings: List[str] = []
    low = sql.lower()

    # 1. Balanced Transactions Checks (Advisory warnings)
    has_begin_tran = "begin tran" in low or "begin transaction" in low
    has_commit = "commit" in low or "commit transaction" in low
    has_rollback = "rollback" in low or "rollback transaction" in low

    if has_begin_tran and not has_commit:
        warnings.append("BEGIN TRANSACTION without COMMIT TRANSACTION — verify rollback is intentional")
    if (has_commit or has_rollback) and not has_begin_tran:
        warnings.append("COMMIT/ROLLBACK TRANSACTION found without BEGIN TRANSACTION")

    # 2. etl_rejects check
    if "etl_rejects" in low:
        low_no_infra = re.sub(
            r"if\s+object_id\s*\(\s*'dbo\.etl_rejects'.*?end\s*;\s*go",
            "", low, flags=re.DOTALL | re.IGNORECASE
        )
        if "etl_rejects" in low_no_infra:
            pattern_rejects = r"insert\s+into\s+(?:dbo\s*\.\s*)?\[?etl_rejects\]?"
            if not re.search(pattern_rejects, low_no_infra):
                warnings.append("etl_rejects table is defined but never inserted into (reject pipeline not used)")

    # 3. Fake default values
    if _has_fake_default_value(sql):
        warnings.append("contains hardcoded fake default values ('99999', '10120631.5', or '1900-01-01')")

    # 4. Wrong deduplication ordering
    if re.search(r"over\s*\([^\)]*etl_created_at", low):
        warnings.append("deduplication partitions or orders by etl_created_at instead of a business column")

    # 5. SELECT DISTINCT * for dedup
    if "select distinct *" in low:
        warnings.append("contains SELECT DISTINCT * instead of key-aware CTE deduplication")

    # 6. Non-production safe SELECT INTO
    into_matches = re.finditer(r"\bselect\b(?:(?!insert|update|delete|create|procedure|\bgo\b|declare|begin|end|commit|rollback)\b[\s\S])*?\binto\s+([\w\.\_\[\]#]+)", low)
    for match in into_matches:
        tbl = match.group(1).strip("[]")
        if not tbl.startswith("#") and not any(x in tbl for x in ("temp_", "staging", "log", "watermark", "reject")):
            warnings.append(f"contains SELECT INTO on clean/joined table '{tbl}' instead of CREATE VIEW or INSERT INTO")

    # 7. Destructive multi-column NULL update
    if re.search(r"set\s+[\w\.\_\[\]#]+\s*=\s*null\s*,\s*[\w\.\_\[\]#]+\s*=\s*null", low):
        warnings.append("contains destructive multi-column NULL update statement (data wipe pattern)")

    # 8. Redundant casting
    if re.search(r"cast\(\s*(?:try_)?cast\(", low) or re.search(r"try_cast\(\s*(?:try_)?cast\(", low):
        warnings.append("contains redundant double CAST statements (e.g. CAST(CAST(...)))")
    if re.search(r"lower\(\s*cast\(\s*(?:ltrim|rtrim|replace|lower|upper|coalesce)", low):
        warnings.append("contains redundant nested CAST operations inside LOWER/LTRIM string wrappers")

    # 9. Email format constraint check
    if "email" in low and not any(pat in low for pat in ("%_@_%._%", "%_@_%._%")):
        warnings.append("Email column detected but missing format check constraint (e.g. Email LIKE '%_@_%._%')")

    # 10. Phone normalization check
    if "phone" in low:
        if "replace" not in low:
            warnings.append("Phone column detected but missing symbol cleaning operations (nested REPLACE for spaces/dashes)")
        if not any(x in low for x in ("len(", "length(", "[^0-9]")):
            warnings.append("Phone column detected but missing validation checks (length >= 7 or only numeric digits)")

    # 11. Date parsing checks
    if "orderdate" in low or "createddate" in low:
        if not any(x in low for x in ("try_convert(", "try_cast(", "to_date(", "to_datetime(")):
            warnings.append("Date columns detected but missing TRY_CAST/TRY_CONVERT date parsing or validation")

    # 12. Empty column wildcard check
    if "select *, @run_id" in low or "select *, @batch_id" in low or "select *," in low:
        warnings.append("contains empty column wildcard select list (symptom of empty metadata)")

    return warnings


def validate_sql_basic_dict(source: str) -> dict:
    """Parse SQL and return structured validation result."""
    ok, issues = validate_security(source)
    warnings = validate_style(source)
    return {
        "valid": ok,
        "issues": issues,
        "warnings": warnings,
    }


def validate_sql_basic(source: str) -> Tuple[bool, List[str], List[str]]:
    """
    Returns: (hard_pass, hard_errors, advisory_warnings)
    hard_pass=False only for security/correctness violations.
    advisory_warnings are style/quality issues — log but don't block.
    """
    # 1. Run the existing comprehensive validation
    result = validate_sql_basic_dict(source)
    hard_pass = result["valid"]
    hard_errors = list(result["issues"])
    advisory_warnings = list(result["warnings"])

    # 2. Integrate the specific patterns from the audit plan to align perfectly
    # Strip comments first to avoid false positives (e.g. drop table in comment block)
    cleaned = re.sub(r"--.*$", "", source, flags=re.MULTILINE)
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)

    # --- HARD FAILURES (always block) ---
    SECURITY_PATTERNS = [
        (r"xp_cmdshell", "Forbidden: xp_cmdshell"),
        (r"BULK\s+INSERT", "Forbidden: BULK INSERT"),
        (r"OPENROWSET", "Forbidden: OPENROWSET"),
        (r"DROP\s+TABLE\s+(?!\s*#|\s*dbo\.etl_)", "Dangerous DROP TABLE on non-temp/non-etl table"),
        (r"DELETE\s+FROM\s+\w+_Raw", "Forbidden: DELETE on Raw table"),
        (r"\bCREATE\s+OR\s+REPLACE\b", "Invalid T-SQL: CREATE OR REPLACE is not supported in T-SQL"),
        (r"\bCREATE\s+OR\s+ALTER\s+TABLE\b", "Invalid T-SQL: CREATE OR ALTER TABLE is not valid syntax in T-SQL"),
        (r"\bINDEX\s+[\w\.\[\]]+\s+ON\s+[\w\.\[\]]+\s*\([^\)]+\)\s*WHERE\s+[^;\n]*\bOR\b", "Invalid T-SQL: Filtered indexes cannot use OR in WHERE clause"),
        (r"\bEXEC\s+(?:\[?dbo\]?\.)?\[?ProcName\]?", "Forbidden: Unresolved mock procedure placeholder 'EXEC dbo.ProcName'"),
        (r"\bCREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE\s+\[?\w+\]?\s+AS\s+BEGIN\s+(?:--[^\n]*\n\s*)*EXEC\s+dbo\.ProcName", "Forbidden: Dummy procedure calling 'EXEC dbo.ProcName'"),
    ]
    for pattern, msg in SECURITY_PATTERNS:
        if re.search(pattern, cleaned, re.IGNORECASE):
            if not any(msg in e for e in hard_errors):
                hard_errors.append(msg)
                hard_pass = False

    # Check staging column consistency to prevent Msg 207 invalid column errors
    staging_tables = re.findall(r"#([a-zA-Z0-9_]+)_Staging\b", cleaned, re.IGNORECASE)
    for st_name in set(staging_tables):
        st_full = f"#{st_name}_Staging"
        create_st = re.search(rf"SELECT\s+([^;]+?)\bINTO\s+{re.escape(st_full)}\b", cleaned, re.IGNORECASE)
        if create_st:
            create_body = create_st.group(1)
            alter_stmts = re.findall(rf"ALTER\s+TABLE\s+{re.escape(st_full)}\s+ADD\s+\[?([a-zA-Z0-9_]+)\]?", cleaned, re.IGNORECASE)
            declared_cols = set(alter_stmts)
            # Add columns declared in SELECT
            for c_match in re.finditer(r"(?:AS\s+\[?([a-zA-Z0-9_]+)\]?|src\.\[?([a-zA-Z0-9_]+)\]?|\[([a-zA-Z0-9_]+)\]|\b([a-zA-Z0-9_]+)\b)", create_body, re.IGNORECASE):
                col_name = c_match.group(1) or c_match.group(2) or c_match.group(3) or c_match.group(4)
                if col_name and col_name.upper() not in ("SELECT", "CAST", "NULL", "AS", "NVARCHAR", "VARCHAR", "MAX", "BIT", "INT", "DATETIME2", "DECIMAL", "FROM"):
                    declared_cols.add(col_name)

            upds = re.findall(rf"UPDATE\s+{re.escape(st_full)}\s+SET\s+([^;]+?)(?:WHERE\b|;)", cleaned, re.IGNORECASE)
            for upd in upds:
                set_cols = [m[0] or m[1] for m in re.findall(r"(?:\[([a-zA-Z0-9_]+)\]|\b([a-zA-Z0-9_]+)\b)\s*=", upd) if (m[0] or m[1])]
                for sc in set_cols:
                    if sc not in declared_cols and not sc.startswith("etl_"):
                        err_msg = f"Undeclared column '{sc}' in UPDATE on staging table '{st_full}' (Msg 207)"
                        if err_msg not in hard_errors:
                            hard_errors.append(err_msg)
                            hard_pass = False

    # --- ADVISORY (warn, never block) ---
    ADVISORY_PATTERNS = [
        (r"SELECT\s+DISTINCT\s+\*", "Advisory: SELECT DISTINCT * is expensive, prefer key-based ROW_NUMBER dedup"),
        (r"etl_created_at.*ORDER\s+BY", "Advisory: Avoid etl_created_at in ROW_NUMBER ordering"),
    ]
    for pattern, msg in ADVISORY_PATTERNS:
        if re.search(pattern, cleaned, re.IGNORECASE):
            if not any(msg in w for w in advisory_warnings):
                advisory_warnings.append(msg)

    return hard_pass, hard_errors, advisory_warnings


def validate_sql_assembly(sql: str, execution_target: Optional[str] = None) -> Tuple[bool, List[str]]:
    """
    Validates the structure and assembly of generated SQL scripts before execution.
    Checks for:
    1. Duplicate CREATE TABLE
    2. Duplicate CREATE PROCEDURE / CREATE OR ALTER PROCEDURE
    3. Duplicate etl_main definition
    4. Multiple EXEC dbo.etl_main invocations
    5. Multiple infrastructure definitions (etl_log, etl_rejects, etl_watermark)
    6. Multiple ETL pipelines concatenated
    7. Invalid THROW usage for ODBC / Azure SQL executor
    8. Residual GO statements for Azure executor (when target specifies single batch execution)
    """
    issues: List[str] = []
    if not sql or not sql.strip():
        return False, ["SQL script is empty"]

    # Remove comments and strings for parsing
    cleaned_no_comments = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
    cleaned_no_comments = re.sub(r"/\*.*?\*/", "", cleaned_no_comments, flags=re.DOTALL)

    # 0. Unresolved dynamic SQL generator tokens
    forbidden_patterns = [
        "+ @clean_table_name +",
        "+ @target_table +",
        "+ @table_name +",
        "'+ @clean_table_name",
        "'+ @target_table",
        "'+ @table_name",
        "' + @clean_table_name",
        "' + @target_table",
        "' + @table_name",
        "@clean_table_name",
        "@target_table",
        "+ N'",
        "N' +",
    ]
    for fp in forbidden_patterns:
        if fp in sql:
            issues.append(f"Dynamic SQL Syntax Bug: Unresolved generator token '{fp}' detected outside dynamic execution block")

    # 1. Duplicate CREATE TABLE
    # Match CREATE TABLE [dbo].[Table] or CREATE TABLE dbo.Table or CREATE TABLE #Table
    table_creations = re.findall(r"\bCREATE\s+TABLE\s+([^\s\(]+)", cleaned_no_comments, re.IGNORECASE)
    seen_tables = set()
    dup_tables = set()
    for tbl in table_creations:
        norm_tbl = tbl.replace("[", "").replace("]", "").strip().lower()
        if norm_tbl in seen_tables:
            dup_tables.add(norm_tbl)
        else:
            seen_tables.add(norm_tbl)
    for dt in sorted(dup_tables):
        issues.append(f"Assembly Conflict: Duplicate CREATE TABLE statement for '{dt}'")

    # 2. Duplicate CREATE PROCEDURE / CREATE OR ALTER PROCEDURE
    proc_creations = re.findall(r"\bCREATE\s+(?:OR\s+ALTER\s+)?(?:PROCEDURE|PROC)\s+([^\s\(]+)", cleaned_no_comments, re.IGNORECASE)
    seen_procs = set()
    dup_procs = set()
    for proc in proc_creations:
        norm_proc = proc.replace("[", "").replace("]", "").strip().lower()
        if norm_proc in seen_procs:
            dup_procs.add(norm_proc)
        else:
            seen_procs.add(norm_proc)
    for dp in sorted(dup_procs):
        issues.append(f"Assembly Conflict: Duplicate procedure definition for '{dp}'")

    # 3. Duplicate etl_main
    etl_main_procs = [p for p in proc_creations if "etl_main" in p.lower()]
    if len(etl_main_procs) > 1:
        issues.append(f"Assembly Conflict: Multiple ({len(etl_main_procs)}) etl_main procedure definitions found")

    # 4. Multiple EXEC dbo.etl_main
    exec_main_matches = re.findall(r"\bEXEC\s+(?:\[?dbo\]?\.)?\[?etl_main\]?", cleaned_no_comments, re.IGNORECASE)
    if len(exec_main_matches) > 1:
        issues.append(f"Assembly Conflict: Multiple ({len(exec_main_matches)}) EXEC dbo.etl_main invocations found in script")

    # 5. Multiple infrastructure definitions
    etl_log_count = len(re.findall(r"\bCREATE\s+TABLE\s+(?:\[?dbo\]?\.)?\[?etl_log\]?", cleaned_no_comments, re.IGNORECASE))
    if etl_log_count > 1:
        issues.append(f"Assembly Conflict: Duplicate dbo.etl_log infrastructure definitions ({etl_log_count} found, expected <= 1)")

    etl_rejects_count = len(re.findall(r"\bCREATE\s+TABLE\s+(?:\[?dbo\]?\.)?\[?etl_rejects\]?", cleaned_no_comments, re.IGNORECASE))
    if etl_rejects_count > 1:
        issues.append(f"Assembly Conflict: Duplicate dbo.etl_rejects infrastructure definitions ({etl_rejects_count} found, expected <= 1)")

    etl_watermark_count = len(re.findall(r"\bCREATE\s+TABLE\s+(?:\[?dbo\]?\.)?\[?etl_watermark\]?", cleaned_no_comments, re.IGNORECASE))
    if etl_watermark_count > 1:
        issues.append(f"Assembly Conflict: Duplicate dbo.etl_watermark infrastructure definitions ({etl_watermark_count} found, expected <= 1)")

    # 6. Multiple ETL pipelines concatenated
    header_count = len(re.findall(r"--\s*ETL\s+SQL\s*—\s*Agent\s+Dhara", sql, re.IGNORECASE))
    if header_count > 1:
        issues.append(f"Assembly Conflict: Multiple ETL pipeline scripts ({header_count}) concatenated into single output")

    auto_exec_markers = len(re.findall(r"--\s*Auto-Execute:\s*Run\s+ETL\s+pipeline", sql, re.IGNORECASE))
    if auto_exec_markers > 1:
        issues.append(f"Assembly Conflict: Multiple concatenated Auto-Execute orchestrator blocks ({auto_exec_markers} found)")

    # 7. Invalid THROW usage for ODBC / Azure SQL executor
    is_odbc_or_azure = (
        execution_target and execution_target.upper() in ("AZURE_SQL_EXECUTOR", "ODBC", "AZURE_SQL")
    )
    if is_odbc_or_azure:
        throw_matches = re.findall(r"\bTHROW\s*;", cleaned_no_comments, re.IGNORECASE)
        if throw_matches:
            issues.append(f"Compatibility Violation: THROW syntax detected ({len(throw_matches)} occurrence(s)). Execution target '{execution_target}' requires RAISERROR compatibility pattern.")

    # 8. Residual GO statements for Azure executor (when checking single-batch execution mode)
    if execution_target and execution_target.upper() in ("AZURE_SQL_SINGLE_BATCH", "AZURE_SQL_NO_GO"):
        go_matches = re.findall(r"^\s*GO\s*(?:--.*)?$", sql, flags=re.MULTILINE | re.IGNORECASE)
        if go_matches:
            issues.append(f"Compatibility Violation: Residual GO statement ({len(go_matches)} found) not supported in single-batch execution mode")

    return len(issues) == 0, issues


def validate_for_execution(sql: str, execution_target: Optional[str] = None) -> dict:
    """Stricter pre-execution gate checking security and assembly issues."""
    ok, issues = validate_security(sql)
    asm_ok, asm_issues = validate_sql_assembly(sql, execution_target=execution_target)
    issues.extend(asm_issues)
    warnings = validate_style(sql)
    return {
        "valid": ok and asm_ok,
        "issues": issues,
        "warnings": warnings,
    }


def validate_issue_coverage(sql: str, manifest: List[dict]) -> dict:
    """
    Level 2 validation: Verify every issue in issue_coverage_manifest is explicitly
    addressed with a remediation query and/or annotated with '-- COVERS: {issue_id}'.
    """
    if not manifest:
        return {"valid": True, "coverage_pct": 100.0, "covered": 0, "total": 0, "missing": []}

    sql_lower = (sql or "").lower()
    covered = []
    missing = []

    for item in manifest:
        iid = str(item.get("issue_id") or "").lower()
        col = str(item.get("column") or "").lower()
        ds = str(item.get("dataset") or "").lower()
        itype = str(item.get("issue_type") or "").lower()

        is_covered = False
        # Match by explicit ID in comments (e.g. -- COVERS: ISSUE-abcd1234 or -- COVERS: [ISSUE-abcd1234])
        if iid and (iid in sql_lower or f"covers: {iid}" in sql_lower or f"covers: [{iid}]" in sql_lower):
            is_covered = True
        # Or match by column + issue_type / action
        elif col and col != "*" and itype:
            if col in sql_lower and (itype in sql_lower or (item.get("planned_action") and str(item.get("planned_action")).lower() in sql_lower)):
                is_covered = True
        elif itype and itype in sql_lower:
            is_covered = True

        if is_covered:
            covered.append(item)
        else:
            missing.append(item)

    total = len(manifest)
    cov_pct = (len(covered) / total * 100.0) if total > 0 else 100.0
    return {
        "valid": len(missing) == 0,
        "coverage_pct": round(cov_pct, 1),
        "covered": len(covered),
        "total": total,
        "missing": missing,
    }


