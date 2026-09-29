"""
Post-generation structural validators for each codegen engine.
Returns (ok: bool, errors: list[str], warnings: list[str])
"""
from __future__ import annotations

import ast
import json
import re
from typing import Tuple, List, Dict, Any, Callable

def validate_python(code: str, never_drop_rows: bool = False) -> Tuple[bool, List[str], List[str]]:
    errors = []
    try:
        ast.parse(code)
    except SyntaxError as e:
        errors.append(f"SyntaxError line {e.lineno}: {e.msg}")
        return False, errors, []
    
    required_patterns = [
        (r"def transform_", "Missing transform_<dataset> function"),
        (r"logging\.getLogger", "Missing logger"),
        (r"df\.copy\(\)", "Missing df.copy() — mutation safety"),
    ]
    for pattern, msg in required_patterns:
        if not re.search(pattern, code):
            errors.append(msg)
    
    # Forbidden patterns
    forbidden = [
        (r"\beval\b", "eval() is forbidden"),
        (r"\bexec\b", "exec() is forbidden"),
        (r"\bos\.system\b", "os.system() is forbidden"),
        (r"pd\.read_csv\(['\"].*\.xml", "read_csv on .xml file — use read_xml"),
        (r"drop_duplicates\([^)]*subset=\[[^\]]*['\"]\*['\"][^\]]*\]", "Invalid Pandas: drop_duplicates(subset=['*']) is invalid ('*' is not a DataFrame column)"),
    ]
    for pattern, msg in forbidden:
        if re.search(pattern, code):
            errors.append(msg)
            
    if never_drop_rows:
        if ".dropna(" in code:
            errors.append("Validation failed: '.dropna()' is forbidden when never_drop_rows is enabled.")
        if ".drop(" in code and ("axis=0" in code or "index=" in code):
            errors.append("Validation failed: Row dropping '.drop()' is forbidden when never_drop_rows is enabled.")
            
    return len(errors) == 0, errors, []

def validate_tsql(code: str, never_drop_rows: bool = False) -> Tuple[bool, List[str], List[str]]:
    errors = []
    warnings = []
    upper = code.upper()
    
    # Must-have structural elements
    hard_checks = [
        ("CREATE TABLE IF NOT EXISTS" in upper, 
         "Invalid T-SQL: CREATE TABLE IF NOT EXISTS (use sys.tables guard)"),
        ("CREATE OR REPLACE" in upper, 
         "Invalid T-SQL: CREATE OR REPLACE is MySQL syntax"),
        ("CREATE OR ALTER TABLE" in upper,
         "Invalid T-SQL: CREATE OR ALTER TABLE is not valid syntax in T-SQL (tables do not support CREATE OR ALTER)"),
        (bool(re.search(r"\bINDEX\s+[\w\.\[\]]+\s+ON\s+[\w\.\[\]]+\s*\([^\)]+\)\s*WHERE\s+[^;\n]*\bOR\b", code, re.IGNORECASE)),
         "Invalid T-SQL: Filtered indexes cannot use OR in WHERE clause"),
        ("AUTO_INCREMENT" in upper, 
         "Invalid T-SQL: AUTO_INCREMENT is MySQL syntax, use IDENTITY"),
        ("ON CONFLICT" in upper, 
         "Invalid T-SQL: ON CONFLICT is PostgreSQL syntax"),
        ("SERIAL" in upper and "SERIAL" not in upper.replace("SERIAL", ""), 
         "Possible SERIAL usage — invalid in T-SQL, use IDENTITY(1,1)"),
        (bool(re.search(r"\bOBJECTID\s*\(", code, re.IGNORECASE)),
         "Invalid T-SQL: OBJECTID() is invalid, use OBJECT_ID()"),
        (bool(re.search(r"\bROWNUMBER\s*\(", code, re.IGNORECASE)),
         "Invalid T-SQL: ROWNUMBER() is invalid, use ROW_NUMBER()"),
        (bool(re.search(r"\bSELECT\s*,\s*", code, re.IGNORECASE)),
         "Invalid T-SQL: Syntax error 'SELECT ,' (leading comma)"),
        (bool(re.search(r"</?(?:a|em|strong|span|div)\b", code, re.IGNORECASE)),
         "Invalid T-SQL: Prompt/HTML tag contamination found in SQL code"),
        (bool(re.search(r"\[[a-zA-Z0-9_\. ]+\]\(https?://", code, re.IGNORECASE)),
         "Invalid T-SQL: Markdown link contamination found in SQL code"),
    ]
    for condition, msg in hard_checks:
        if condition:
            errors.append(msg)
    
    # Check if GO appears inside BEGIN TRY ... END CATCH
    try_catch_match = re.search(r"BEGIN\s+TRY([\s\S]*?)END\s+CATCH", code, re.IGNORECASE)
    if try_catch_match:
        inner_body = try_catch_match.group(1)
        if re.search(r"^\s*GO\s*(?:--.*)?$", inner_body, re.MULTILINE | re.IGNORECASE):
            errors.append("Invalid T-SQL: 'GO' batch separator found inside BEGIN TRY ... END CATCH block (splits batch and causes syntax failure)")

    required = [
        ("ETL_LOG", "Missing dbo.etl_log DDL"),
        ("SCOPE_IDENTITY", "Missing SCOPE_IDENTITY() for @run_id capture"),
        ("BEGIN TRY", "Missing BEGIN TRY block"),
        ("BEGIN CATCH", "Missing BEGIN CATCH block"),
        ("ROLLBACK", "Missing ROLLBACK in CATCH block"),
    ]
    for keyword, msg in required:
        if keyword not in upper:
            errors.append(msg)

    # Check transaction balance: if ROLLBACK is present, ensure BEGIN TRANSACTION exists
    if "ROLLBACK" in upper and "BEGIN TRANSACTION" not in upper and "BEGIN TRAN" not in upper:
        errors.append("Invalid T-SQL: ROLLBACK encountered without corresponding BEGIN TRANSACTION")

    # Check for invalid wildcard column identifier
    code_no_strings = re.sub(r"'(?:''|[^'])*'", "''", code)
    code_no_strings = re.sub(r"--[^\n]*", "", code_no_strings)
    if "[*]" in code_no_strings:
        errors.append("Invalid T-SQL: '[*]' used as column identifier")
        
    # Check for comma inside bracketed identifier
    if re.search(r"\[\s*[^\]\n']+,[^\]\n']+\]", code_no_strings):
        errors.append("Invalid T-SQL: Bracketed identifier contains comma")

    # Check for ALTER TABLE inside stored procedure body
    proc_match = re.search(r"CREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE[\s\S]*?END\s*;", code, re.IGNORECASE)
    if proc_match and re.search(r"\bALTER\s+TABLE\b", proc_match.group(0), re.IGNORECASE):
        warnings.append("Advisory: ALTER TABLE statement found inside stored procedure body (violates DBA best practices; use pre-procedure DDL)")

    # Advisory only
    if "ETL_REJECTS" not in upper:
        warnings.append("Advisory: Missing dbo.etl_rejects table — recommended for DQ reject tracking")
            
    if never_drop_rows:
        if "DELETE FROM" in upper or "TRUNCATE TABLE" in upper:
            errors.append("Validation failed: DELETE/TRUNCATE is forbidden when never_drop_rows is enabled.")
            
    return len(errors) == 0, errors, warnings

def validate_pyspark(code: str, never_drop_rows: bool = False) -> Tuple[bool, List[str], List[str]]:
    errors = []
    warnings = []
    try:
        ast.parse(code)
    except SyntaxError as e:
        errors.append(f"SyntaxError line {e.lineno}: {e.msg}")
        return False, errors, warnings
    
    if re.search(r"partitionBy\(\s*['\"]\*['\"]\s*\)", code):
        errors.append("Invalid PySpark: Window.partitionBy('*') is invalid (asterisk is not a column name)")
    
    if "spark.read.csv" in code and ".xml" in code:
        errors.append("read.csv on .xml file — use XML format reader")
    has_spark = (
        "SparkSession" in code
        or "from pyspark.sql import" in code
        or ("try:" in code and "spark" in code)
    )
    if not has_spark:
        errors.append("Missing SparkSession creation or import")

    # Advisory only
    if "_resolve_data_path" not in code and "abfss://" not in code:
        warnings.append("Advisory: Missing _resolve_data_path or direct ABFSS path — verify blob access")
        
    is_cleanse_only = "cleanse_only" in code.lower() or "phase_1_cleanse_only" in code.lower() or "cleanse" in code.lower()
    
    if is_cleanse_only:
        # Error if inner join is used in Phase 1
        if ".join(" in code and ("'inner'" in code or '"inner"' in code):
            errors.append("Validation failed: Inner join is forbidden in Phase 1 (cleanse-only) mode.")
            
        # Error if row_number() with lit(1) in a window context
        if "row_number()" in code and "lit(1)" in code:
            errors.append("Validation failed: Nondeterministic duplicate survivorship selection row_number().orderBy(lit(1)) is forbidden in Phase 1.")
            
        # Warning if fillna / fill_nulls on business key is found
        from agent.etl_pipeline.issue_to_step_compiler import _BUSINESS_KEY_PATTERNS
        for pattern in _BUSINESS_KEY_PATTERNS:
            if f"fillna" in code or "fill_nulls" in code:
                warnings.append(f"Advisory: Potential null imputation on business key pattern '{pattern}' detected.")
                break

    if never_drop_rows:
        if ".dropna(" in code or "dropna" in code:
            errors.append("Validation failed: '.dropna()' is forbidden when never_drop_rows is enabled.")
            
    return len(errors) == 0, errors, warnings


def validate_adf(obj: dict, never_drop_rows: bool = False) -> Tuple[bool, List[str], List[str]]:
    errors = []
    if not isinstance(obj, dict):
        errors.append("ADF output is not a JSON object")
        return False, errors, []
    if "flows" not in obj and "pipeline" not in obj:
        errors.append("ADF output missing 'flows' or 'pipeline' key")
        
    if never_drop_rows:
        # Check if ADF has any delete or filter transforms that discard rows
        flows = obj.get("flows") or []
        for flow in flows:
            for transform in flow.get("transformations") or []:
                if transform.get("type") in ("filter", "alterRow"):
                    # Check if delete is enabled on alterRow
                    props = transform.get("typeProperties") or {}
                    if props.get("delete") or transform.get("type") == "filter":
                        errors.append(f"Validation failed: transformation '{transform.get('name')}' uses filtering/deletion which is forbidden when never_drop_rows is enabled.")
                        
    return len(errors) == 0, errors, []

VALIDATORS: Dict[str, Callable[[Any], Tuple[bool, List[str], List[str]]]] = {
    "python": validate_python,
    "sql-tsql": validate_tsql,
    "sql-ansi": validate_tsql,  # same guard set
    "pyspark": validate_pyspark,
    "adf": validate_adf,
}

def get_validator(engine_key: str) -> Callable[[Any], Tuple[bool, List[str], List[str]]]:
    val = VALIDATORS.get(engine_key)
    if not val:
        raise ValueError(f"Unrecognized codegen engine key: '{engine_key}'. Valid engines: {list(VALIDATORS.keys())}")
    return val
