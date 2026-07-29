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
    depth = 0
    for ch in source:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                issues.append("unbalanced parentheses")
                break
    if depth != 0:
        issues.append("unclosed parentheses")
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

    # 4. USE [db] switching
    if re.search(r"\buse\s+\[?\w+\]?\b", cleaned_lower):
        issues.append("contains USE statement — database switching not allowed mid-script")

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

    try:
        import sqlparse  # type: ignore
        parsed = sqlparse.parse(sql)
        if not parsed or not parsed[0].tokens:
            issues.append("Empty or unparseable SQL")
    except ImportError:
        pass
    except Exception as e:
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
    ]
    for pattern, msg in SECURITY_PATTERNS:
        if re.search(pattern, cleaned, re.IGNORECASE):
            if not any(msg in e for e in hard_errors):
                hard_errors.append(msg)
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


def validate_for_execution(sql: str) -> dict:
    """Stricter pre-execution gate checking only security issues."""
    ok, issues = validate_security(sql)
    warnings = validate_style(sql)
    return {
        "valid": ok,
        "issues": issues,
        "warnings": warnings,
    }
