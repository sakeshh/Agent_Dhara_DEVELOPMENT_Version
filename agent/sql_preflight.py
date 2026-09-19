"""
Preflight validation and linting for generated SQL code.
Uses sqlfluff as an optional dependency for checking dialect correctness.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

logger = logging.getLogger("agent.sql_preflight")


def lint_generated_sql(sql: str, dialect: str = "tsql") -> Dict[str, Any]:
    """
    Lints the generated SQL using sqlfluff if available.
    Returns:
        Dict: {
            "ok": bool,
            "errors": List[Dict[str, Any]],
            "message": str
        }
    """
    if not sql or not sql.strip():
        return {"ok": True, "errors": [], "message": "SQL is empty."}

    try:
        import sqlfluff
    except ImportError:
        logger.debug("sqlfluff is not installed; skipping SQL linting preflight checks.")
        return {
            "ok": True,
            "errors": [],
            "message": "sqlfluff not installed. Linting skipped."
        }

    try:
        # sqlfluff.lint returns a list of violations (dicts)
        violations = sqlfluff.lint(sql, dialect=dialect)
        if not violations:
            return {"ok": True, "errors": [], "warnings": [], "message": "SQL linting passed."}

        errors = []
        warnings = []
        for v in violations:
            entry = {
                "line": v.get("line_no"),
                "column": v.get("line_pos"),
                "code": v.get("code"),
                "description": v.get("description")
            }
            code_str = str(v.get("code") or "").upper()
            if code_str.startswith("PRS"):
                errors.append(entry)
            else:
                warnings.append(entry)

        return {
            "ok": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "message": f"SQL linting completed with {len(errors)} parse errors and {len(warnings)} style warnings."
        }
    except Exception as e:
        logger.warning("Error running sqlfluff lint: %s", e)
        return {
            "ok": True,
            "errors": [],
            "warnings": [],
            "message": f"sqlfluff failed internally: {e}. SQL skipped."
        }


def run_sql_preflight(sql: str) -> Dict[str, Any]:
    """
    Run SQL preflight syntax linting (sqlfluff) plus custom pattern/compatibility checks.
    Returns:
        Dict: {
            "passed": bool,
            "errors": List[str],
            "warnings": List[str]
        }
    """
    errors: List[str] = []
    warnings: List[str] = []

    if not sql or not sql.strip():
        return {"passed": True, "errors": [], "warnings": []}

    # Custom Dialect compatibility check (Regex patterns)
    import re
    upper_sql = sql.upper()

    # Look for common non-T-SQL syntax
    # 1. CREATE TABLE IF NOT EXISTS
    if "CREATE TABLE IF NOT EXISTS" in upper_sql:
        errors.append("Invalid T-SQL: 'CREATE TABLE IF NOT EXISTS' is not supported in T-SQL. Use sys.tables check instead.")

    # 2. CREATE OR REPLACE
    if "CREATE OR REPLACE" in upper_sql:
        errors.append("Invalid T-SQL: 'CREATE OR REPLACE' is not supported in T-SQL (typically MySQL/PostgreSQL syntax).")

    # 3. AUTO_INCREMENT
    if "AUTO_INCREMENT" in upper_sql:
        errors.append("Invalid T-SQL: 'AUTO_INCREMENT' is MySQL syntax. Use 'IDENTITY(1,1)' in T-SQL.")

    # 4. ON CONFLICT
    if "ON CONFLICT" in upper_sql:
        errors.append("Invalid T-SQL: 'ON CONFLICT' is PostgreSQL syntax. Use MERGE or IF NOT EXISTS check.")

    # 5. SERIAL (must be word boundaries to not flag words like SERIALIZE/SERIALIZER)
    if re.search(r"\bSERIAL\b", upper_sql):
        errors.append("Invalid T-SQL: 'SERIAL' is PostgreSQL syntax. Use 'IDENTITY(1,1)' in T-SQL.")

    # Run sqlfluff if available
    lint_res = lint_generated_sql(sql)
    if not lint_res.get("ok", True):
        # Translate fatal parse errors
        for err in lint_res.get("errors") or []:
            desc = err.get("description") or "SQL parse error"
            line = err.get("line")
            col = err.get("column")
            errors.append(f"Parse error (line {line}, col {col}): {desc}")
    for w in lint_res.get("warnings") or []:
        warnings.append(f"Lint warning: {w.get('description')}")

    return {
        "passed": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
    }

