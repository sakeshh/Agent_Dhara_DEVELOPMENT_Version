"""
LLM-based ETL Code Generator.
Translates an approved ETL plan + assessment metadata into production-ready code
for Python, SQL, PySpark, and Azure Data Factory.
"""
from __future__ import annotations

import ast
import json
import os
import re
import time
import html
import asyncio
import threading
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple

# Per-plan-id locks — process-wide cross-thread coordination
_PLAN_LOCKS: dict[str, threading.Lock] = defaultdict(threading.Lock)
_PLAN_FAILURE_COUNTS: dict[str, int] = defaultdict(int)
_MAX_PLAN_FAILURES = 3  # after this, circuit-open for 30s (reduced from 5 min so a code fix clears it quickly)

try:
    from openai import AzureOpenAI, OpenAI, RateLimitError, APITimeoutError
except ImportError:
    AzureOpenAI = None
    OpenAI = None
    RateLimitError = None
    APITimeoutError = None

from agent.errors import ConnectorConfigError, GeneratedCodeIncompleteError, MechanicalRepairViolation
from agent.model_config import load_llm_config, LLM_REQUEST_TIMEOUT, get_context_window
from agent.etl_pipeline.codegen_policy import llm_codegen_extra_context, plan_policy_block
from agent.etl_pipeline.io_snippets import (
    resolve_path_pyspark_helper,
    resolve_path_fabric_pyspark_helper,
)
from agent.etl_pipeline.payload_trimmer import trim_payload, _FLOOR_TRIM_CONFIG, _CODEGEN_TRIM_CONFIG, TrimConfig
import logging
logger = logging.getLogger("agent.etl_pipeline.llm_codegen")

LLM_ERROR_PREFIX = "# Error"

from dataclasses import dataclass

PROMPT_VERSION = "1.5.0"
VALIDATOR_VERSION = "1.1.0"
POSTPROCESSOR_VERSION = "1.2.0"

@dataclass(frozen=True)
class GeneratedCodeArtifact:
    generation_id: str
    attempt: int
    source: str
    source_sha256: str
    plan_sha256: str
    prompt_version: str
    validator_version: str
    postprocessor_version: str
    model_deployment: str

    @classmethod
    def create(cls, source: str, plan_hash: str, attempt: int, generation_id: str) -> GeneratedCodeArtifact:
        import hashlib
        import os
        source_sha256 = hashlib.sha256(source.encode("utf-8")).hexdigest()
        MODEL_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT") or os.getenv("OPENAI_MODEL") or "default"
        return cls(
            generation_id=generation_id,
            attempt=attempt,
            source=source,
            source_sha256=source_sha256,
            plan_sha256=plan_hash,
            prompt_version=PROMPT_VERSION,
            validator_version=VALIDATOR_VERSION,
            postprocessor_version=POSTPROCESSOR_VERSION,
            model_deployment=MODEL_DEPLOYMENT,
        )


class LLMInfraError(Exception):
    """Raised for LLM infrastructure errors like rate limits or timeouts."""
    pass

from collections import OrderedDict

_CACHE_MAX_SIZE = 512
_CODE_CACHE: OrderedDict[str, Any] = OrderedDict()
_VALIDATOR_ACCEPTS_NDR: dict[str, bool] = {}
_FAILURE_CACHE: OrderedDict[str, tuple[str, float]] = OrderedDict()  # key -> (error_msg, expiry_time)

def clear_codegen_caches() -> None:
    """Flush all in-memory codegen caches (CODE_CACHE, FAILURE_CACHE, PLAN_FAILURE_COUNTS).

    Call this after deploying validator or codegen fixes so the circuit breaker
    and failure cache don't serve stale pre-fix errors.
    """
    _CODE_CACHE.clear()
    _FAILURE_CACHE.clear()
    _PLAN_FAILURE_COUNTS.clear()
    logger.info("[clear_codegen_caches] All codegen caches flushed.")


def _code_cache_put(cache_key: str, value: Any) -> None:
    if cache_key in _CODE_CACHE:
        _CODE_CACHE.move_to_end(cache_key)
    _CODE_CACHE[cache_key] = value
    if len(_CODE_CACHE) > _CACHE_MAX_SIZE:
        _CODE_CACHE.popitem(last=False)

def _is_failure_cached(cache_key: str) -> str | None:
    item = _FAILURE_CACHE.get(cache_key)
    if not item:
        return None
    error_msg, expiry_time = item
    if time.time() > expiry_time:
        _FAILURE_CACHE.pop(cache_key, None)
        return None
    _FAILURE_CACHE.move_to_end(cache_key)
    return error_msg

def _write_failure_cache(cache_key: str, error: str, ttl_seconds: int = 30):
    if cache_key in _FAILURE_CACHE:
        _FAILURE_CACHE.move_to_end(cache_key)
    _FAILURE_CACHE[cache_key] = (error, time.time() + ttl_seconds)
    if len(_FAILURE_CACHE) > _CACHE_MAX_SIZE:
        _FAILURE_CACHE.popitem(last=False)

_PYSPARK_REQUIRED_IMPORTS = [
    ("from pyspark.sql import SparkSession, DataFrame, Window", ["SparkSession", "DataFrame"]),
    ("from pyspark.sql import functions as F", ["F."]),
    ("from pyspark.sql.window import Window", ["Window."]),
    ("import logging", ["logging."]),
]

def _inject_pyspark_imports(code: str) -> str:
    """
    Ensure required PySpark module-level imports are present at the top of the file.
    If the LLM placed them inside function bodies, we inject them at the module level
    so that the import check always passes without needing a retry.
    """
    if not code or not code.strip():
        return code

    lines = code.splitlines(keepends=True)

    # Step 1: Determine the end of the module docstring block
    insert_at = 0
    i = 0
    if lines and lines[0].strip().startswith('"""'):
        # Multi-line or single-line module docstring
        if lines[0].strip().count('"""') >= 2 and len(lines[0].strip()) > 3:
            # Single-line docstring: e.g. """plan_id: ..."""
            insert_at = 1
            i = 1
        else:
            # Multi-line: scan forward until closing """
            i = 1
            while i < len(lines):
                if '"""' in lines[i]:
                    i += 1
                    break
                i += 1
            insert_at = i

    # Step 2: Skip any blank lines and comments immediately after the docstring
    while insert_at < len(lines):
        stripped = lines[insert_at].strip()
        if not stripped or stripped.startswith("#"):
            insert_at += 1
        else:
            break

    # Step 3: Check what is already in the header (first 40 lines after insert_at)
    header = "".join(lines[:min(insert_at + 40, len(lines))])

    to_inject = []
    # Replace any LLM-generated bare 'from pyspark.sql import SparkSession, DataFrame' with Window included
    code = re.sub(
        r"from\s+pyspark\.sql\s+import\s+SparkSession\s*,\s*DataFrame\b(?!,\s*Window)",
        "from pyspark.sql import SparkSession, DataFrame, Window",
        code
    )
    if "Window" in code and "from pyspark.sql import SparkSession, DataFrame, Window" not in code and "from pyspark.sql.window import Window" not in code:
        to_inject.append("from pyspark.sql import Window\n")

    for import_line, triggers in _PYSPARK_REQUIRED_IMPORTS:
        if not any(t in code for t in triggers):
            continue
        if import_line in header or import_line in code:
            continue
        to_inject.append(import_line + "\n")

    if not to_inject:
        return code

    injected = lines[:insert_at] + to_inject + ["\n"] + lines[insert_at:]
    return "".join(injected)


def _inject_pyspark_fabric_ids(code: str) -> str:
    """
    Ensure the fallback values in _resolve_data_path are correctly populated
    with the actual workspace_id and lakehouse_id from the environment.
    """
    if not code:
        return code
        
    ws_id = os.getenv("FABRIC_WORKSPACE_ID") or ""
    lh_id = os.getenv("FABRIC_LAKEHOUSE_ID") or os.getenv("FABRIC_LAKEHOUSE_NAME") or ""
    
    if lh_id and not (len(lh_id) == 36 and lh_id.count("-") == 4):
        try:
            from agent.fabric_api_client import FabricAPIClient
            client = FabricAPIClient()
            resolved = client.resolve_lakehouse_id_by_name(ws_id, lh_id)
            if resolved:
                lh_id = resolved
        except Exception:
            pass
            
    # Locate ws_id = "" and lh_id = "" patterns and replace them
    code = re.sub(
        r'ws_id\s*=\s*["\']["\']',
        f'ws_id = "{ws_id}"',
        code,
    )
    code = re.sub(
        r'lh_id\s*=\s*["\']["\']',
        f'lh_id = "{lh_id}"',
        code,
    )
    return code


def extract_defined_functions(source: str) -> list[str]:
    try:
        tree = ast.parse(source)
        return sorted({node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)})
    except Exception:
        return sorted(list(set(re.findall(r"\bdef\s+(\w+)\s*\(", source))))


def extract_called_transform_functions(source: str) -> list[str]:
    called = set()
    try:
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    called.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    called.add(node.func.attr)
    except Exception:
        pass
    for match in re.findall(r"\b(transform_\w+)\s*\(", source):
        called.add(match)
    return sorted([c for c in called if c.startswith("transform_")])


def log_stage_diagnostics(stage_name: str, code: str):
    import hashlib
    h = hashlib.sha256(code.encode('utf-8')).hexdigest()[:16]
    
    defined = extract_defined_functions(code)
    called = extract_called_transform_functions(code)
    
    # Step markers
    step_markers = re.findall(r"#\s*(?:DHARA_)?(?:STEP|ACTION):\s*([^\n]+)", code, re.I)
    
    # Detected actions
    detected_actions = re.findall(r"#\s*(?:DHARA_ACTION):\s*([a-zA-Z_]\w*)", code, re.I)
    
    diagnostics = {
        "stage": stage_name,
        "code_hash": h,
        "code_length": len(code),
        "defined_functions": defined,
        "called_functions": called,
        "step_markers": step_markers,
        "detected_actions": detected_actions,
    }
    logger.info(f"[Codegen Stage] {stage_name}: {json.dumps(diagnostics)}")


def save_stage_debug_artifact(generation_id: str, attempt: int, stage_name: str, content: str):
    if os.environ.get("DHARA_ENV", "development").lower() not in ("development", "test"):
        return
    try:
        local_app_data = os.getenv("LOCALAPPDATA") or os.path.expanduser("~")
        debug_dir = os.path.join(local_app_data, "AgentDhara", "codegen_debug", generation_id)
        os.makedirs(debug_dir, exist_ok=True)
        
        # Redact credentials/secrets
        redacted = content
        redacted = re.sub(r"(?i)(password|secret|key|token|credential|pwd)\s*=\s*['\"][^'\"]+['\"]", r"\1 = 'REDACTED'", redacted)
        
        filename = f"attempt_{attempt}_{stage_name}"
        if stage_name == "raw":
            filename += ".txt"
        elif stage_name == "validation":
            filename += ".json"
        else:
            filename += ".py"
            
        filepath = os.path.join(debug_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(redacted)
        logger.info(f"[Debug Artifact] Saved {filename} to {filepath}")
    except Exception as e:
        logger.warning(f"Could not save stage debug artifact {stage_name}: {e}")


def _fix_mismatched_transform_calls(code: str) -> str:
    """Reconcile mismatched transform function calls to match defined transform function names."""
    if not code or "def transform_" not in code:
        return code
    defined = set()
    try:
        tree = ast.parse(code)
        defined = {
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name.startswith("transform_")
        }
    except Exception:
        pass

    if not defined:
        defined = set(re.findall(r"\bdef\s+(transform_\w+)\s*\(", code))

    if not defined:
        return code

    for fn_name in defined:
        # Fix calls where LLM appended file extension suffixes (e.g. transform_data_quality_issues_csv -> transform_data_quality_issues)
        pattern = re.compile(rf"\b{re.escape(fn_name)}_[a-zA-Z0-9_]+\b")
        code = pattern.sub(fn_name, code)
    return code


def _fix_unsafe_pyspark_first_calls(code: str) -> str:
    """Rewrite unsafe PySpark .first()[0] expressions and paired fillna calls to prevent NoneType crashes."""
    if not code or ".first()[" not in code:
        return code

    # 1. Paired agg + fillna
    def repl_fillna(m):
        var = m.group(1)
        select_expr = m.group(2)
        target_var = m.group(3)
        col = m.group(4)
        return (
            f"_{var}_row = {select_expr}.first()\n"
            f"    _{var}_val = _{var}_row[0] if _{var}_row else None\n"
            f"    if _{var}_val is not None:\n"
            f"        {target_var} = {target_var}.fillna({{\"{col}\": _{var}_val}})"
        )

    pat_fillna = re.compile(
        r"(\w+)\s*=\s*(\w+\.select\(.*?\))\.\s*first\(\)\s*\[\s*0\s*\]\s*\n\s*(\w+)\s*=\s*\3\.fillna\(\{\s*[\"\'](\w+)[\"\']\s*:\s*\1\s*\}\)"
    )
    code = pat_fillna.sub(repl_fillna, code)

    # 2. Remaining standalone var = expr.first()[0] or expr.first()["..."]
    def repl_standalone(m):
        var = m.group(1)
        select_expr = m.group(2)
        idx = m.group(3)
        return (
            f"_{var}_row = {select_expr}.first()\n"
            f"    {var} = _{var}_row[{idx}] if (_{var}_row and _{var}_row[{idx}] is not None) else None"
        )

    pat_standalone = re.compile(
        r"(\w+)\s*=\s*(\w+\.select\(.*?\))\.\s*first\(\)\s*\[\s*(0|[\"\']\w+[\"\'])\s*\]"
    )
    code = pat_standalone.sub(repl_standalone, code)

    # 3. Fix df = df.withColumn(col, F.when(F.col(col).isNull(), var).otherwise(F.col(col))) where var may be None
    def repl_when(m):
        target_df = m.group(1)
        col = m.group(2)
        var = m.group(3)
        return (
            f"if {var} is not None:\n"
            f"        {target_df} = {target_df}.withColumn({col}, F.when(F.col({col}).isNull(), F.lit({var})).otherwise(F.col({col})))"
        )

    pat_when = re.compile(
        r"(\w+)\s*=\s*\1\.withColumn\(\s*([\"\']\w+[\"\'])\s*,\s*F\.when\(\s*F\.col\(\2\)\.isNull\(\)\s*,\s*(\w+)\s*\)\.otherwise\(F\.col\(\2\)\)\)"
    )
    return pat_when.sub(repl_when, code)


def _fix_pyspark_none_literals(code: str) -> str:
    """Replace PySpark raw None with F.lit(None), convert loops to isin(), and strip no-op null fills."""
    if not code:
        return code
        
    # 1. Fix F.when(condition, None) -> F.when(condition, F.lit(None))
    code = re.sub(r"\bF\.when\(([^,\n]+),\s*None\s*\)", r"F.when(\1, F.lit(None))", code)
    
    # 2. Fix .otherwise(None) -> .otherwise(F.lit(None))
    code = re.sub(r"\.otherwise\(\s*None\s*\)", r".otherwise(F.lit(None))", code)

    # 3. Strip no-op null fills: df = df.withColumn('col', F.when(F.col('col').isNull(), ...).otherwise(F.col('col')))
    code = re.sub(
        r"(\w+)\s*=\s*\1\.withColumn\(\s*([\"\']\w+[\"\'])\s*,\s*F\.when\(\s*F\.col\(\2\)\.isNull\(\)\s*,\s*(?:None|F\.lit\(None\))\s*\)\.otherwise\(F\.col\(\2\)\)\s*\)",
        r"# skipped no-op null fill on \2",
        code
    )

    # 4. Convert Python for-loops over value lists into single .isin() expressions
    def repl_loop(m):
        arr_name = m.group(1)
        var_item = m.group(2)
        arr_name_2 = m.group(3)
        target_df = m.group(4)
        col = m.group(5)
        if arr_name == arr_name_2:
            return (
                f"if {arr_name}:\n"
                f"        {target_df} = {target_df}.withColumn({col}, F.when(F.col({col}).isin({arr_name}), F.lit(None)).otherwise(F.col({col})))"
            )
        return m.group(0)

    pat_loop = re.compile(
        r"(\w+)\s*=\s*\[.*?\]\s*\n\s*for\s+(\w+)\s+in\s+(\w+):\s*\n\s*(\w+)\s*=\s*\4\.withColumn\(\s*([\"\']\w+[\"\'])\s*,\s*F\.when\(\s*F\.col\(\5\)\s*==\s*\2\s*,\s*(?:None|F\.lit\(None\))\s*\)\.otherwise\(F\.col\(\5\)\)\s*\)"
    )
    code = pat_loop.sub(repl_loop, code)
    return code


def _fix_pyspark_numeric_rlike_filters(code: str) -> str:
    """Rewrite any string rlike/regex checks on numeric columns to double casts to pass validation."""
    if not code:
        return code
    lines = code.splitlines()
    new_lines = []
    numeric_keywords = ("amount", "budget", "qty", "quantity", "price", "cost", "score", "total", "val")
    for line in lines:
        if "rlike" in line and "withColumn" in line:
            match = re.search(r"([a-zA-Z_][\w]*)\s*=\s*\1\.withColumn\(['\"]([a-zA-Z_][\w]*)['\"]\s*,", line)
            if match:
                df_var = match.group(1)
                col = match.group(2)
                col_lower = col.lower()
                if any(k in col_lower for k in numeric_keywords):
                    indent = len(line) - len(line.lstrip())
                    indent_str = line[:indent]
                    line = f"{indent_str}{df_var} = {df_var}.withColumn({repr(col)}, F.col({repr(col)}).cast('double'))"
        new_lines.append(line)
    return "\n".join(new_lines)


def _fix_pyspark_categorical_digit_filters(code: str) -> str:
    """Remove or simplify digits-only regex filters on categorical columns to prevent validation failures."""
    if not code:
        return code
    lines = code.splitlines()
    new_lines = []
    cat_keywords = ("region", "channel", "location", "category", "status", "country", "state", "city", "zone", "segment", "type", "machine", "site", "facility", "plant", "warehouse", "floor")
    for line in lines:
        if "rlike" in line and "withColumn" in line:
            match = re.search(r"([a-zA-Z_][\w]*)\s*=\s*\1\.withColumn\(['\"]([a-zA-Z_][\w]*)['\"]\s*,\s*F\.when\(.*rlike.*(?:[0-9]|\\d).*otherwise", line)
            if match:
                df_var = match.group(1)
                col = match.group(2)
                col_lower = col.lower()
                if any(k in col_lower for k in cat_keywords):
                    indent = len(line) - len(line.lstrip())
                    indent_str = line[:indent]
                    # Rewrite to a simple trim and cast to string, avoiding the digit-only filter entirely
                    line = f"{indent_str}{df_var} = {df_var}.withColumn({repr(col)}, F.trim(F.col({repr(col)})).cast('string'))"
        new_lines.append(line)
    return "\n".join(new_lines)


def _fix_invalid_pyspark_dedup_window(code: str) -> str:
    """Replace inline deduplication window code with the canonical _flag_source_duplicates(df) helper call."""
    if not code:
        return code

    lines = code.splitlines()
    new_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Replace comment-only deduplicate step markers
        if "# Step: deduplicate" in stripped:
            indent = len(line) - len(line.lstrip())
            indent_str = line[:indent]
            new_lines.append(f"{indent_str}# Step: identify exact duplicate source rows (flagged, rows preserved)")
            new_lines.append(f"{indent_str}df = _flag_source_duplicates(df)")
            
            # Peek ahead to skip the inline Window block if it is present immediately below
            j = i + 1
            while j < len(lines) and (not lines[j].strip() or lines[j].strip().startswith("#")):
                j += 1
            if j < len(lines) and "Window as _DedupWindow" in lines[j]:
                k = j
                while k < len(lines) and "_is_duplicate_row" not in lines[k]:
                    k += 1
                if k < len(lines):
                    i = k + 1
                    continue
            i += 1
            continue

        # Replace the inline Window dedup block (4 lines, any indentation):
        if (
            "Window as _DedupWindow" in stripped
            and i + 3 < len(lines)
            and "_data_cols" in lines[i + 1]
            and "_dup_w" in lines[i + 2]
            and "_is_duplicate_row" in lines[i + 3]
        ):
            indent = len(line) - len(line.lstrip())
            indent_str = line[:indent]
            new_lines.append(f"{indent_str}df = _flag_source_duplicates(df)")
            i += 4  # consume all 4 lines
            continue

        new_lines.append(line)
        i += 1

    return "\n".join(new_lines)


def _deduplicate_repeated_dedup_calls(code: str) -> str:
    if not code:
        return code
    return re.sub(
        r"([ \t]*df\s*=\s*_flag_source_duplicates\(df\)\s*\n)+",
        r"\1",
        code
    )



def _fix_string_ops_on_numeric_cols(code: str) -> str:
    """Remove F.lower() and F.upper() calls on numeric/metric column names.
    
    The LLM sometimes applies lowercase/uppercase to numeric columns (e.g. budget, production_qty)
    because they arrive as strings before type casting. These ops are semantically wrong
    and can produce unexpected string values before safe numeric conversion.
    """
    if not code:
        return code
    numeric_col_keywords = (
        "amount", "budget", "qty", "quantity", "price", "cost",
        "score", "total", "val", "production_qty", "revenue", "sales",
        "count", "num", "number", "rate", "ratio", "margin", "fee",
    )
    lines = code.splitlines()
    new_lines = []
    for line in lines:
        if ("F.lower(" in line or "F.upper(" in line) and "withColumn" in line:
            match = re.search(r'withColumn\((["\'])([a-zA-Z_][\w]*)\1\s*,\s*F\.(lower|upper)\(', line)
            if match:
                col = match.group(2).lower()
                if any(k in col for k in numeric_col_keywords):
                    # Comment out the line — it is semantically wrong on a numeric column
                    indent = len(line) - len(line.lstrip())
                    new_lines.append(f"{line[:indent]}# [removed: string op on numeric column] {line.lstrip()}")
                    continue
        new_lines.append(line)
    return "\n".join(new_lines)


def _fix_string_fills_on_numeric_cols(code: str) -> str:
    """Remove invalid string fill values (like 'unknown') on numeric/metric columns."""
    if not code:
        return code
    numeric_col_keywords = (
        "amount", "budget", "qty", "quantity", "price", "cost",
        "score", "total", "val", "production_qty", "revenue", "sales",
        "count", "num", "number", "rate", "ratio", "margin", "fee",
    )
    lines = code.splitlines()
    new_lines = []
    for line in lines:
        stripped = line.strip()
        
        # 1. Match fillna with string values on numeric columns, e.g. .fillna({ 'production_qty': 'unknown' })
        m_fillna = re.search(r"\.fillna\s*\(\s*\{(['\"])([a-zA-Z_][\w]*)\1\s*:\s*(['\"])([^'\"]+)\3\}\s*\)", stripped)
        if m_fillna:
            col = m_fillna.group(2)
            val = m_fillna.group(4)
            if any(k in col.lower() for k in numeric_col_keywords):
                if not re.match(r"^-?(?:\d+(?:\.\d+)?|\.\d+)$", val):
                    indent = len(line) - len(line.lstrip())
                    new_lines.append(f"{line[:indent]}# [removed invalid string fill on numeric column] {stripped}")
                    continue
                    
        # 2. Match coalesce with F.lit("unknown") on numeric columns
        m_coalesce = re.search(r"F\.coalesce\s*\(\s*.*,\s*(?:F\.lit\s*\(\s*(['\"])([^'\"]+)\1\s*\)|(['\"])([^'\"]+)\3)\s*\)", stripped)
        if m_coalesce:
            m_assign = re.search(r"withColumn\s*\(\s*(['\"])([a-zA-Z_][\w]*)\1", stripped)
            if m_assign:
                col = m_assign.group(2)
                val = m_coalesce.group(2) or m_coalesce.group(4)
                if any(k in col.lower() for k in numeric_col_keywords):
                    if not re.match(r"^-?(?:\d+(?:\.\d+)?|\.\d+)$", val):
                        indent = len(line) - len(line.lstrip())
                        new_lines.append(f"{line[:indent]}# [removed invalid string coalesce on numeric column] {stripped}")
                        continue
                        
        new_lines.append(line)
    return "\n".join(new_lines)


def _filter_unrelated_transforms(code: str, plan: Dict[str, Any]) -> str:
    """Filter out transform functions that are not defined in the plan datasets."""
    if not code or not plan:
        return code
    
    def _canonical_id(name: str) -> str:
        return re.sub(r"[^0-9a-zA-Z_]+", "_", name or "x").strip("_")
        
    active_canons = set()
    for ds in (plan.get("datasets") or {}):
        canon = _canonical_id(ds)
        active_canons.add(canon)
        for ext in ("_xml", "_csv", "_json", "_parquet", "_txt"):
            if canon.lower().endswith(ext):
                active_canons.add(canon[:-len(ext)])
    
    lines = code.splitlines()
    new_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        m_fn = re.match(r"\s*def\s+(transform_(\w+))\s*\(", line)
        if m_fn:
            fn_name = m_fn.group(1)
            ds_canon = m_fn.group(2)
            if ds_canon not in active_canons:
                # Skip this entire function!
                j = i + 1
                while j < len(lines):
                    if lines[j].strip() and not lines[j].startswith(" ") and not lines[j].startswith("\t"):
                        break
                    j += 1
                i = j
                continue
        new_lines.append(line)
        i += 1
        
    return "\n".join(new_lines)


def _fix_skipped_fill_comments(code: str) -> str:
    """Replace '# skipped empty fillna' comment lines with an explicit is_missing_<col> flag.
    
    When the plan has a fill step but no fill value, the LLM emits a skip comment.
    Instead, we inject an is_missing_<col> flag so downstream DQ auditing still has lineage.
    """
    if not code:
        return code
    # Match: # skipped empty fillna for df  (or similar variants)
    _SKIP_PAT = re.compile(
        r"# skipped (?:empty )?fillna\s+(?:for (?:df|\w+))?\s*$",
        re.IGNORECASE,
    )
    # Try to extract the column from the preceding step comment
    _STEP_COL_PAT = re.compile(r"# Step: (?:fill_or_drop|fill_nulls_simple)\s+on\s+\[([a-zA-Z_][\w]*)\]")
    lines = code.splitlines()
    new_lines = []
    last_step_col: str | None = None
    for line in lines:
        stripped = line.strip()
        step_m = _STEP_COL_PAT.search(stripped)
        if step_m:
            last_step_col = step_m.group(1)
        if _SKIP_PAT.search(stripped) and last_step_col:
            indent = len(line) - len(line.lstrip())
            indent_str = line[:indent]
            flag_col = f"is_missing_{last_step_col}"
            # Only inject if the flag doesn't already exist
            if flag_col not in code:
                new_lines.append(f"{indent_str}# No fill value provided — flagging missingness for DQ lineage")
                new_lines.append(
                    f"{indent_str}df = df.withColumn({repr(flag_col)}, "
                    f"F.col({repr(last_step_col)}).isNull() | "
                    f"(F.trim(F.col({repr(last_step_col)}).cast('string')) == ''))"
                )
            else:
                new_lines.append(f"{indent_str}# {flag_col} already present above — no fill value provided")
            last_step_col = None
            continue
        new_lines.append(line)
    return "\n".join(new_lines)


def _fix_bare_run_pipeline_call(code: str) -> str:
    """Replace bare 'run_pipeline(spark)' top-level calls with the safe try/except NameError wrapper.
    
    The LLM sometimes emits a bare 'run_pipeline(spark)' at the top level. On non-Fabric
    (local PySpark) environments this crashes with NameError because 'spark' is not pre-injected.
    """
    if not code or "run_pipeline" not in code:
        return code

    _SAFE_WRAPPER = (
        "# Fabric notebooks pre-inject 'spark'; fall back to creating one for local dev.\n"
        "try:\n"
        "    spark  # type: ignore[name-defined]\n"
        "except NameError:\n"
        "    from pyspark.sql import SparkSession\n"
        '    spark = SparkSession.builder.appName("AgentDharaETL").getOrCreate()\n'
        "run_pipeline(spark)"
    )

    # Replace a bare standalone 'run_pipeline(spark)' line that has no try/except guard above it
    if "try:" not in code or "NameError" not in code:
        code = re.sub(
            r"(?m)^([ \t]*)run_pipeline\(spark\)\s*$",
            _SAFE_WRAPPER,
            code,
        )
    return code


def _deduplicate_helper_functions(code: str) -> str:
    """Remove duplicate helper function definitions from generated script."""
    if not code:
        return code
    lines = code.splitlines()
    new_lines = []
    seen_defs = set()
    in_dup_def = False
    indent_level = 0
    
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("def _"):
            match = re.search(r"def (_[a-zA-Z0-9_]+)\(", stripped)
            if match:
                fn_name = match.group(1)
                if fn_name in ("_require_columns", "_warn_duplicate_keys", "_warn_nulls_in_columns", "_log_row_count", "_normalize_column_names", "_parse_timestamp_safely", "_flag_source_duplicates", "_resolve_data_path", "_prefix_columns", "_iqr_bounds"):
                    if fn_name in seen_defs:
                        in_dup_def = True
                        indent_level = len(line) - len(line.lstrip())
                        continue
                    else:
                        seen_defs.add(fn_name)
                        in_dup_def = False
        if in_dup_def:
            if stripped and (len(line) - len(line.lstrip()) <= indent_level) and not stripped.startswith("#"):
                in_dup_def = False
            else:
                continue
        new_lines.append(line)
    return "\n".join(new_lines)


def _inject_transform_header_block(code: str, plan: Optional[Dict[str, Any]] = None) -> str:
    """
    Inject normalize/require/hash header lines immediately after the function def line.
    Uses safe line-based insertion instead of character-position slicing to avoid
    corrupting function bodies.
    """
    if not code or not plan:
        return code

    datasets = plan.get("datasets") or {}
    _safe = lambda name: re.sub(r"[^0-9a-zA-Z_]+", "_", name or "x").strip("_")
    ignored_keys = {"monotonically_increasing_id", "plan_id", "batch_id", "run_id", "job_id", "execution_id"}

    # Build a lookup: fn_name -> header lines to inject
    fn_headers: Dict[str, List[str]] = {}
    for ds_name, block in datasets.items():
        base_name = ds_name.split(".")[0]
        possible_names = [f"transform_{_safe(ds_name)}", f"transform_{_safe(base_name)}"]
        fn_name = next((n for n in possible_names if n in code), None)
        if not fn_name:
            continue

        steps = (block or {}).get("steps") or []
        cols = []
        for st in steps:
            c = st.get("column")
            if c:
                for sc in str(c).split(","):
                    sc = sc.strip()
                    if sc and not sc.startswith(("[", "_")) and sc not in cols:
                        cols.append(sc)

        if not cols:
            continue

        key_cols = [
            c for c in cols
            if any(k in c.lower() for k in ("id", "key", "code", "ref", "num", "no", "nbr", "uuid", "guid", "sku", "ean", "isbn", "asin"))
            and c not in ignored_keys
        ]

        hdr: List[str] = [
            "    df = _normalize_column_names(df)",
            f"    _require_columns(df, {repr(cols)}, {repr(ds_name)})",
            "    _src_cols = [F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in df.columns if not c.startswith('_') and not c.startswith('is_')]",
            "    df = df.withColumn('_source_row_hash', F.xxhash64(*_src_cols))",
        ]
        for k in sorted(key_cols):
            flag_col = f"is_missing_{k}"
            hdr.append(f"    df = df.withColumn({repr(flag_col)}, F.col({repr(k)}).isNull() | (F.trim(F.col({repr(k)})) == ''))")

        fn_headers[fn_name] = hdr

    if not fn_headers:
        return code

    # Line-based injection — safe for arbitrarily long function bodies
    _SKIP_TOKENS = ("_normalize_column_names(", "_require_columns(", "_source_row_hash", "_src_cols =", "is_missing_")
    output_lines: List[str] = []
    lines = code.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        output_lines.append(line)

        fn_match = re.match(r"\s*def\s+(transform_\w+)\s*\(", line)
        if fn_match:
            fn_name = fn_match.group(1)
            if fn_name in fn_headers:
                # Consume the colon-terminated signature (may span multiple lines)
                while ":" not in line and i + 1 < len(lines):
                    i += 1
                    line = lines[i]
                    output_lines.append(line)
                # Inject header lines
                for hdr_line in fn_headers[fn_name]:
                    output_lines.append(hdr_line)
                # Skip any pre-existing header injections in the next body lines
                i += 1
                while i < len(lines):
                    stripped = lines[i].strip()
                    if stripped and any(tok in stripped for tok in _SKIP_TOKENS):
                        i += 1
                        continue
                    break
                continue  # do not increment i again below

        i += 1

    return "\n".join(output_lines)


def _fix_safe_numeric_casts_and_flags(code: str) -> str:
    if not code:
        return code
    numeric_keywords = ("amount", "budget", "qty", "quantity", "price", "cost", "score", "total", "val", "production_qty")
    lines = code.splitlines()
    new_lines = []
    converted_cols = set()
    skip_next_line = False

    for line in lines:
        if skip_next_line:
            skip_next_line = False
            continue

        stripped = line.strip()
        # Clean up nullify_punctuation on numeric columns
        m_punc = re.search(r"#\s*Step:\s*nullify_punctuation\s*on\s*\[([a-zA-Z_][\w]*)\]", stripped)
        if m_punc:
            c_name = m_punc.group(1)
            if any(k in c_name.lower() for k in numeric_keywords):
                indent_str = line[:len(line)-len(stripped)]
                new_lines.append(f"{indent_str}# Step: nullify_punctuation on [{c_name}]")
                new_lines.append(f"{indent_str}# Resolution: merged into safe numeric validation above. Punctuation-only strings fail _valid_{c_name} and become NULL.")
                skip_next_line = True
                continue

        # Clean up coerce_numeric on already converted numeric columns
        m_coerce = re.search(r"#\s*Step:\s*coerce_numeric\s*on\s*\[([a-zA-Z_][\w]*)\]", stripped)
        if m_coerce:
            c_name = m_coerce.group(1)
            if c_name in converted_cols or any(k in c_name.lower() for k in numeric_keywords):
                indent_str = line[:len(line)-len(stripped)]
                new_lines.append(f"{indent_str}# Step: coerce_numeric on [{c_name}] — merged into safe numeric conversion above")
                skip_next_line = True
                continue

        # Convert simple cast('double') or other variations on numeric columns to safe regex-gated casts
        m = re.search(
            r"(\w+)\s*=\s*\1\.withColumn\((['\"])([a-zA-Z_][\w]*)\2\s*,\s*.*?(?:\bcast\()\s*(?:['\"](?:double|float|long|int|integer|decimal)['\"]|DoubleType|FloatType|LongType|IntegerType|DecimalType)(?:\(\))?\s*\)\s*\)",
            line
        )
        if m:
            df_var, quote_char, col = m.group(1), m.group(2), m.group(3)
            src_col = col[:-6] if col.endswith("_clean") else col
            col_lower = col.lower()
            if any(k in col_lower for k in numeric_keywords):
                flag_col = f"is_invalid_{col}"
                missing_col = f"is_missing_{col}"
                indent = len(line) - len(line.lstrip())
                indent_str = line[:indent]
                if col not in converted_cols:
                    new_lines.append(f"{indent_str}# Safe numeric conversion with missing & invalid flags for {col}")
                    new_lines.append(f"{indent_str}_raw_{col} = F.trim(F.col({repr(src_col)}).cast('string'))")
                    if missing_col not in code:
                        new_lines.append(f"{indent_str}{df_var} = {df_var}.withColumn({repr(missing_col)}, _raw_{col}.isNull() | (_raw_{col} == ''))")
                    new_lines.append(f"{indent_str}_valid_{col} = _raw_{col}.rlike(r'^-?(?:\\d+(?:\\.\\d+)?|\\.\\d+)$')")
                    new_lines.append(f"{indent_str}{df_var} = {df_var}.withColumn({repr(flag_col)}, (~F.col({repr(missing_col)}) & ~_valid_{col}).cast('boolean'))")
                    new_lines.append(f"{indent_str}{df_var} = {df_var}.withColumn({repr(col)}, F.when(_valid_{col}, _raw_{col}.cast('double')).otherwise(F.lit(None).cast('double')))")
                    converted_cols.add(col)
                    continue
                else:
                    # Skip redundant second cast on already converted column
                    continue

        # Skip redundant secondary F.when(F.col(c).cast('double').isNotNull()...) casts on converted cols
        if any(c in line for c in converted_cols) and "cast('double')" in line and "otherwise" in line:
            continue

        new_lines.append(line)
    return "\n".join(new_lines)


def _fix_safe_date_parsing_and_flags(code: str) -> str:
    if not code:
        return code
    date_keywords = ("date", "time", "dt", "timestamp")
    lines = code.splitlines()
    new_lines = []
    converted_date_cols = set()

    for line in lines:
        stripped = line.strip()
        m = re.search(
            r"(\w+)\s*=\s*\1\.withColumn\((['\"])([a-zA-Z_][\w]*)\2\s*,\s*F\.(?:to_date|to_timestamp)\(F\.col\((['\"])([a-zA-Z_][\w]*)\4\)",
            line
        )
        if m:
            df_var, q1, out_col, q2, src_col = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)
            col_lower = src_col.lower()
            if any(k in col_lower for k in date_keywords):
                indent_str = line[:len(line)-len(stripped)]
                if src_col not in converted_date_cols:
                    new_lines.append(f"{indent_str}# Safe ANSI timestamp parsing & invalid flag for {src_col}")
                    new_lines.append(f"{indent_str}{df_var} = _parse_timestamp_safely({df_var}, {repr(src_col)})")
                    if out_col != src_col:
                        new_lines.append(f"{indent_str}{df_var} = {df_var}.withColumn({repr(out_col)}, F.col({repr(src_col)}))")
                    converted_date_cols.add(src_col)
                    continue

        new_lines.append(line)
    return "\n".join(new_lines)


def _sanitize_unquoted_nan_in_code(code: str) -> str:
    if not code:
        return code
    # Fix unquoted bare nan in list literals like [..., nan, ...] -> [..., pd.NA, ...]
    code = re.sub(r'(\[\s*(?:[^\]]*?,\s*)*)\bnan\b(\s*(?:,\s*[^\]]*?)*\])', r"\1pd.NA\2", code)
    # Fix logging lines missing f-prefix
    code = re.sub(r"logging\.(info|warning|error)\('([^']*?\{_\w+\}[^']*?)'\)", r"logging.\1(f'\2')", code)
    code = re.sub(r'logging\.(info|warning|error)\("([^"]*?\{_\w+\}[^"]*?)"\)', r'logging.\1(f"\2")', code)
    return code


def _sanitize_html_entities_in_code(code: str) -> str:
    if not code:
        return code
    code = html.unescape(code)
    return _sanitize_unquoted_nan_in_code(code)


def _fix_toplevel_pipeline_duplication(code: str) -> str:
    """Comment out top-level standalone execution lines so run_pipeline is the sole execution path."""
    if not code:
        return code
    lines = code.splitlines()
    new_lines = []
    in_def = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("def ") or stripped.startswith("class ") or stripped.startswith("if __name__"):
            in_def = True
            new_lines.append(line)
            continue
        if in_def:
            if stripped and not line.startswith(" ") and not line.startswith("\t"):
                in_def = False
            else:
                new_lines.append(line)
                continue
        # Module-level unindented execution check
        if re.search(r"^\s*(?:\w+_df|\w+)\s*=\s*(?:spark\.read|transform_\w+)", line) or \
           re.search(r"^\s*(?:\w+_df|\w+)\.write\b", line) or \
           re.search(r"^\s*_log_row_count\b", line):
            new_lines.append(f"# {stripped}")
            continue
        new_lines.append(line)
    return "\n".join(new_lines)


def _fix_numeric_aggregations_cast_double(code: str) -> str:
    if not code:
        return code
    numeric_keywords = ("amount", "budget", "qty", "quantity", "price", "cost", "score", "total", "val", "production_qty", "revenue", "sales", "count", "num", "number", "rate", "ratio", "margin", "fee")
    for col in numeric_keywords:
        code = re.sub(
            rf"F\.percentile_approx\(\s*(?:F\.col\(\s*(['\"]){re.escape(col)}\1\s*\)|(['\"]){re.escape(col)}\2)\s*,\s*([-+]?\d*\.?\d+)\s*\)",
            f"F.percentile_approx(F.col({col!r}).cast('double'), \\3)",
            code,
            flags=re.IGNORECASE
        )
        code = re.sub(
            rf"F\.avg\(\s*(?:F\.col\(\s*(['\"]){re.escape(col)}\1\s*\)|(['\"]){re.escape(col)}\2)\s*\)",
            f"F.avg(F.col({col!r}).cast('double'))",
            code,
            flags=re.IGNORECASE
        )
        code = re.sub(
            rf"F\.mean\(\s*(?:F\.col\(\s*(['\"]){re.escape(col)}\1\s*\)|(['\"]){re.escape(col)}\2)\s*\)",
            f"F.mean(F.col({col!r}).cast('double'))",
            code,
            flags=re.IGNORECASE
        )
    return code


def _fix_imputation_lineage_flags(code: str) -> str:
    """Inject is_imputed_<col> boolean flag column before fillna calls, and rewrite invalid fillna(F.lit()) or fillna(None) calls."""
    if not code:
        return code
    lines = code.splitlines()
    new_lines = []
    for line in lines:
        m = re.search(r"(\w+)\s*=\s*\1\.fillna\s*\(\s*\{(['\"])([a-zA-Z_][\w]*)\2\s*:\s*([^}]+)\}\s*\)", line)
        if m:
            df_var, quote, col, val = m.group(1), m.group(2), m.group(3), m.group(4).strip()
            flag_col = f"is_imputed_{col}"
            indent = len(line) - len(line.lstrip())
            indent_str = line[:indent]
            
            # Inject flag column if not already present
            if flag_col not in code:
                new_lines.append(f"{indent_str}{df_var} = {df_var}.withColumn({repr(flag_col)}, F.col({repr(col)}).isNull().cast('boolean'))")
            
            # Rewrite bad fillna calls containing F.lit or None
            if "F.lit" in val or val == "None":
                new_lines.append(f"{indent_str}{df_var} = {df_var}.withColumn({repr(col)}, F.coalesce(F.col({repr(col)}), {val}))")
                continue
        new_lines.append(line)
    return "\n".join(new_lines)


def _inject_pyspark_datasets_decl(code: str, plan: Optional[Dict[str, Any]] = None) -> str:
    if not code:
        return code
        
    real_datasets = list((plan.get("datasets") or {}).keys()) if plan else []
    if "DATASETS" in code and re.search(r"\bDATASETS\s*=\s*\[", code):
        if real_datasets:
            code = re.sub(r"\bDATASETS\s*=\s*\[.*?\]", f"DATASETS = {repr(real_datasets)}", code)
        return code
    
    datasets = real_datasets or re.findall(r"def transform_(\w+)\s*\(", code) or ["default"]
    decl = f"DATASETS = {repr(datasets)}\n"
    
    lines = code.splitlines(keepends=True)
    insert_at = 0
    i = 0
    if lines and lines[0].strip().startswith('"""'):
        if lines[0].strip().count('"""') >= 2 and len(lines[0].strip()) > 3:
            insert_at = 1
        else:
            i = 1
            while i < len(lines):
                if '"""' in lines[i]:
                    i += 1
                    break
                i += 1
            insert_at = i
            
    while insert_at < len(lines):
        stripped = lines[insert_at].strip()
        if not stripped or stripped.startswith("#"):
            insert_at += 1
        else:
            break
            
    injected = lines[:insert_at] + [decl] + lines[insert_at:]
    return "".join(injected)



def _inject_pyspark_helpers(code: str) -> str:
    if not code:
        return code
        
    from agent.etl_pipeline.io_snippets import (
        pyspark_iqr_bounds_helper,
        pyspark_production_helpers,
        pyspark_prefix_non_key_columns_helper,
        resolve_path_fabric_pyspark_helper,
    )
    
    # Strip invalid df.fillna({"col": None}) or df.fillna(value=None) or df.fillna(None)
    # Handles both single and multiple keys mapped to None
    code = re.sub(
        r"\b(\w+)\s*=\s*\1\.fillna\s*\(\s*\{\s*(?:['\"][^'\"]+['\"]\s*:\s*None\s*,\s*)*['\"][^'\"]+['\"]\s*:\s*None\s*\}\s*\)",
        r"# skipped empty fillna for \1",
        code
    )
    code = re.sub(
        r"\b(\w+)\s*=\s*\1\.fillna\s*\(\s*None\s*\)",
        r"# skipped empty fillna for \1",
        code
    )
    
    to_inject = []
    if "_resolve_data_path" in code:
        # Overwrite any LLM-generated static _resolve_data_path with our Fabric OneLake mount path helper
        code = re.sub(
            r"def _resolve_data_path\s*\([\s\S]*?return\s+.*?\n",
            resolve_path_fabric_pyspark_helper() + "\n",
            code,
            count=1
        )
        if "def _resolve_data_path" not in code:
            to_inject.append(resolve_path_fabric_pyspark_helper() + "\n\n")

    if "_iqr_bounds" in code and "def _iqr_bounds" not in code:
        to_inject.append(pyspark_iqr_bounds_helper() + "\n\n")
        
    if "_prefix_columns" in code and "def _prefix_columns" not in code:
        to_inject.append(pyspark_prefix_non_key_columns_helper() + "\n\n")
        
    for helper_name in ("_normalize_column_names", "_warn_nulls_in_columns", "_warn_duplicate_keys", "_require_columns", "_log_row_count"):
        if helper_name in code and f"def {helper_name}" not in code:
            to_inject.append(pyspark_production_helpers() + "\n\n")
            break
            
    if not to_inject:
        return code
        
    lines = code.splitlines(keepends=True)
    insert_at = 0
    i = 0
    if lines and lines[0].strip().startswith('"""'):
        if lines[0].strip().count('"""') >= 2 and len(lines[0].strip()) > 3:
            insert_at = 1
        else:
            i = 1
            while i < len(lines):
                if '"""' in lines[i]:
                    i += 1
                    break
                i += 1
            insert_at = i
            
    while insert_at < len(lines):
        stripped = lines[insert_at].strip()
        if (not stripped or 
            stripped.startswith("#") or 
            stripped.startswith("import ") or 
            stripped.startswith("from ") or 
            stripped.startswith("DATASETS =")):
            insert_at += 1
        else:
            break
            
    injected = lines[:insert_at] + to_inject + lines[insert_at:]
    return "".join(injected)


from dataclasses import dataclass
@dataclass
class RepairResult:
    code: str
    repairs_applied: list[str]


def find_forbidden_imports(source: str) -> list[str]:
    try:
        tree = ast.parse(source)
    except Exception:
        return []
    forbidden_roots = {"os", "pandas", "subprocess"}
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".", 1)[0]
                if root in forbidden_roots:
                    found.add(root)
        elif isinstance(node, ast.ImportFrom):
            root = (node.module or "").split(".", 1)[0]
            if root in forbidden_roots:
                found.add(root)
    return sorted(list(found))


def apply_guarded_fixup(
    source: str,
    fixup_fn,
    plan: dict | None,
) -> str:
    before_functions = set(extract_defined_functions(source))
    
    def extract_step_markers(code: str) -> set[str]:
        return set(re.findall(r"#\s*(?:DHARA_)?(?:STEP|ACTION):\s*([^\n]+)", code, re.I))

    before_markers = extract_step_markers(source)
    raw_imports = set(find_forbidden_imports(source))

    import inspect
    sig = inspect.signature(fixup_fn)
    if len(sig.parameters) >= 2:
        repaired = fixup_fn(source, plan)
    else:
        repaired = fixup_fn(source)

    after_functions = set(extract_defined_functions(repaired))
    after_markers = extract_step_markers(repaired)
    after_imports = set(find_forbidden_imports(repaired))
    
    introduced = after_imports - raw_imports
    if introduced:
        raise MechanicalRepairViolation(
            message=f"{fixup_fn.__name__} introduced forbidden imports: {sorted(list(introduced))}",
            fixup_name=fixup_fn.__name__
        )

    before_transforms = {f for f in before_functions if f.startswith("transform_")}
    after_transforms = {f for f in after_functions if f.startswith("transform_")}
    removed_transforms = before_transforms - after_transforms

    if removed_transforms:
        raise MechanicalRepairViolation(
            message=f"{fixup_fn.__name__} removed required transforms: {sorted(list(removed_transforms))}",
            fixup_name=fixup_fn.__name__
        )

    removed_markers = before_markers - after_markers
    if removed_markers:
        raise MechanicalRepairViolation(
            message=f"{fixup_fn.__name__} removed step markers: {sorted(list(removed_markers))}",
            fixup_name=fixup_fn.__name__
        )

    return repaired


def _fix_uncreated_clean_col_refs(code: str) -> str:
    """If F.col('col_clean') is referenced in a line before 'col_clean' is created, replace F.col('col_clean') with F.col('col')."""
    if not code:
        return code
    lines = code.splitlines()
    new_lines = []
    for line in lines:
        m = re.search(r"df\.withColumn\(['\"]([a-zA-Z_][\w]*_clean)['\"]\s*,\s*.*?F\.col\(['\"](\1)['\"]\)", line)
        if m:
            clean_col = m.group(1)
            raw_col = clean_col[:-6]
            line = line.replace(f"F.col('{clean_col}')", f"F.col('{raw_col}')")
            line = line.replace(f'F.col("{clean_col}")', f'F.col("{raw_col}")')
        new_lines.append(line)
    return "\n".join(new_lines)


def apply_mechanical_codegen_repairs(
    code: str,
    plan: dict | None = None,
) -> RepairResult:
    repairs = []
    
    # 1. basic syntax normalization
    repaired_code = apply_guarded_fixup(code, _sanitize_html_entities_in_code, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _inject_pyspark_imports, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _inject_pyspark_fabric_ids, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _inject_pyspark_datasets_decl, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _inject_transform_header_block, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _inject_pyspark_helpers, plan)
    
    # 2. safe mechanical fixups
    repaired_code = apply_guarded_fixup(repaired_code, _uncreated_clean_col_refs, plan) if False else repaired_code
    repaired_code = apply_guarded_fixup(repaired_code, _fix_uncreated_clean_col_refs, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _filter_unrelated_transforms, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _fix_string_ops_on_numeric_cols, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _fix_string_fills_on_numeric_cols, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _fix_skipped_fill_comments, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _fix_safe_numeric_casts_and_flags, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _fix_safe_date_parsing_and_flags, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _fix_toplevel_pipeline_duplication, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _fix_unsafe_pyspark_first_calls, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _fix_pyspark_none_literals, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _fix_pyspark_numeric_rlike_filters, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _fix_pyspark_categorical_digit_filters, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _fix_invalid_pyspark_dedup_window, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _deduplicate_repeated_dedup_calls, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _fix_numeric_aggregations_cast_double, plan)
    repaired_code = apply_guarded_fixup(repaired_code, _fix_imputation_lineage_flags, plan)
    
    return RepairResult(code=repaired_code, repairs_applied=repairs)


def _remove_row_count_logging_from_transforms(code: str) -> str:
    if not code:
        return code
    lines = code.splitlines()
    new_lines = []
    in_transform = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("def transform_"):
            in_transform = True
        elif stripped.startswith("def ") or (stripped.startswith("class ") and in_transform):
            if not stripped.startswith("def transform_"):
                in_transform = False
        
        if in_transform and "_log_row_count(" in line:
            indent = len(line) - len(line.lstrip())
            new_lines.append(f"{line[:indent]}# [removed redundant row count log] {line.lstrip()}")
            continue
        
        new_lines.append(line)
    return "\n".join(new_lines)


def _ensure_non_empty_functions(code: str) -> str:
    if not code:
        return code
    lines = code.splitlines()
    out = []
    i = 0
    while i < len(lines):
        out.append(lines[i])
        if re.match(r"^\s*def\s+\w+\(.*\)\s*:\s*$", lines[i]):
            indent = len(lines[i]) - len(lines[i].lstrip())
            body_indent = " " * (indent + 4)

            # Look ahead for next meaningful line
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1

            if j >= len(lines) or len(lines[j]) - len(lines[j].lstrip()) <= indent:
                out.append(f"{body_indent}pass")
        i += 1
    return "\n".join(out)


def _debug_lines(code: str, center: int, radius: int = 8) -> str:
    lines = code.splitlines()
    start = max(0, center - radius - 1)
    end = min(len(lines), center + radius)
    return "\n".join(
        f"{idx+1:04d}: {lines[idx]}" for idx in range(start, end)
    )


def _remove_run_pipeline_code(code: str) -> str:
    try:
        import ast
        tree = ast.parse(code)
        lines = code.splitlines()
        ranges_to_remove = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "run_pipeline":
                start = node.lineno - 1
                end = getattr(node, "end_lineno", None)
                if end is not None:
                    ranges_to_remove.append((start, end))
            elif isinstance(node, ast.Try) and any(
                isinstance(child, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "spark" for t in child.targets
                ) for child in node.body
            ):
                start = node.lineno - 1
                end = getattr(node, "end_lineno", None)
                if end is not None:
                    ranges_to_remove.append((start, end))
            elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name) and node.value.func.id == "run_pipeline":
                start = node.lineno - 1
                end = getattr(node, "end_lineno", None)
                if end is not None:
                    ranges_to_remove.append((start, end))

        if ranges_to_remove:
            ranges_to_remove.sort(key=lambda r: r[0], reverse=True)
            for start, end in ranges_to_remove:
                del lines[start:end]
            return "\n".join(lines)
        else:
            return code
    except Exception:
        pass

    # Line-by-line fallback
    lines = code.splitlines()
    out = []
    in_run_pipeline = False
    in_try_spark = False
    
    for line in lines:
        stripped = line.strip()
        if re.match(r"^def\s+run_pipeline\b", stripped):
            in_run_pipeline = True
            continue
        if re.match(r"^try:\s*$", stripped):
            in_try_spark = True
            try_block_lines = [line]
            continue
        if in_run_pipeline:
            if line.startswith((" ", "\t")) or not stripped or stripped.startswith("#"):
                continue
            else:
                in_run_pipeline = False
        if in_try_spark:
            try_block_lines.append(line)
            if line.startswith((" ", "\t")) or not stripped or stripped.startswith("#"):
                continue
            else:
                in_try_spark = False
                block_str = "\n".join(try_block_lines)
                if "SparkSession" not in block_str and "spark" not in block_str:
                    out.extend(try_block_lines)
                continue
        if re.match(r"^run_pipeline\s*\(", stripped):
            continue
        out.append(line)
    return "\n".join(out)


def _inject_pyspark_run_pipeline(code: str, plan: Dict[str, Any]) -> str:
    if not code:
        return code

    # Strip any LLM-generated run_pipeline block, try/except blocks, or run_pipeline calls
    # to guarantee we always use our robust, deterministic template code.
    code = _remove_run_pipeline_code(code)

    from agent.etl_pipeline.join_emitters import (
        emit_pyspark_output_contract,
        emit_pyspark_load,
        emit_pyspark_joins,
        emit_pyspark_write_outputs,
    )

    manifest = plan.get("connector_manifest") or {}
    rules = plan.get("business_rules") or {}
    rel = plan.get("relationships") or {}

    # Strip any default empty logger info/messages at the end
    code = re.sub(
        r"# Fabric notebooks pre-inject 'spark'; fall back to creating one for local dev\..*$",
        "",
        code.strip(),
        flags=re.DOTALL
    )

    lines = [code.strip(), ""]
    
    def _canonical_id(name: str) -> str:
        return re.sub(r"[^0-9a-zA-Z_]+", "_", name or "x").strip("_")

    non_nullable = [str(c) for c in (rules.get("non_nullable") or []) if c]
    if manifest.get("datasets") or rel.get("joins"):
        for sl in emit_pyspark_output_contract(plan, manifest):
            lines.append(sl)
        lines.append("def run_pipeline(spark):")
        lines.append("    dfs = {}")
        lines.append("    def _canonical_id(name: str) -> str:")
        lines.append('        import re')
        lines.append('        return re.sub(r"[^0-9a-zA-Z_]+", "_", name or "x").strip("_")')
        lines.append("")
        lines.append("    active_canons = {_canonical_id(d) for d in DATASETS}")
        
        # Load block
        load_lines = emit_pyspark_load(plan, manifest)
        i_load = 0
        while i_load < len(load_lines):
            l_line = load_lines[i_load]
            m_set = re.match(r'^\s*dfs\["([^"]+)"\]\s*=\s*(.*)$', l_line)
            if m_set:
                c_id = m_set.group(1)
                expr = m_set.group(2)
                lines.append(f'    if "{c_id}" in active_canons:')
                lines.append(f'        dfs["{c_id}"] = {expr}')
                if i_load + 1 < len(load_lines) and "_require_columns" in load_lines[i_load + 1]:
                    lines.append(f'        {load_lines[i_load + 1].strip()}')
                    i_load += 1
            else:
                if l_line.strip():
                    lines.append(f"    {l_line}")
            i_load += 1
            
        for ds_name in (plan.get("datasets") or {}):
            canon_id = _canonical_id(ds_name)
            fn = f"transform_{canon_id}"
            lines.append(f'    if "{canon_id}" in dfs:')
            lines.append(f'        dfs["{canon_id}"] = {fn}(dfs["{canon_id}"], dfs)')
            if non_nullable:
                lines.append(f'        _warn_nulls_in_columns(dfs["{canon_id}"], {non_nullable!r}, "{canon_id}")')
            lines.append(f'        _log_row_count(dfs["{canon_id}"], "{canon_id}")')
        for sl in emit_pyspark_joins(plan):
            lines.append(f"    {sl}")
        for sl in emit_pyspark_write_outputs(plan, manifest):
            lines.append(f"    {sl}")
        lines.append("    return dfs, OUTPUT_PATHS")
        lines.append("")
        lines.append("# Fabric notebooks pre-inject 'spark'; fall back to creating one for local dev.")
        lines.append("try:")
        lines.append("    spark  # type: ignore[name-defined]")
        lines.append("except NameError:")
        lines.append("    from pyspark.sql import SparkSession")
        lines.append('    spark = SparkSession.builder.appName("AgentDharaETL").getOrCreate()')
        lines.append("_dfs, _paths = run_pipeline(spark)")
    else:
        lines.append("# Fabric notebooks pre-inject 'spark'; fall back to creating one for local dev.")
        lines.append("try:")
        lines.append("    spark  # type: ignore[name-defined]")
        lines.append("except NameError:")
        lines.append("    from pyspark.sql import SparkSession")
        lines.append('    spark = SparkSession.builder.appName("AgentDharaETL").getOrCreate()')
        lines.append("logger.info('Empty pipeline executed successfully')")
        
    return "\n".join(lines)


def _estimate_tokens(text: str | dict) -> int:
    if isinstance(text, dict):
        try:
            text = json.dumps(text, default=str)
        except Exception:
            text = str(text)
    try:
        import tiktoken
        enc = tiktoken.get_encoding("cl100k_base")
        return len(enc.encode(text, disallowed_special=()))
    except (ImportError, Exception):
        is_json_like = text.lstrip().startswith(("{", "["))
        chars_per_token = 2 if is_json_like else 4
        return max(1, len(text) // chars_per_token)


# Actions the planner may emit — LLM must implement each one correctly for the target engine.
_PLAN_PARAMS = """
Each plan step includes a "params" dict — use it as the source of truth (not only evidence):
- params.fill_strategy: "mean" | "median" | "value" — for fill_or_drop / fill_nulls_simple
- params.fill_value: scalar when fill_strategy is "value" or precomputed mean/median
- params.outlier_method: "flag" | "clip" | "cap"
- params.outlier_iqr_multiplier: float (default 1.5)
- params.privacy: "hash" | "mask" | "exclude" for phone/privacy columns
- params.enforcement_mode: "flag" | "quarantine" for referential integrity steps
- params.execution_mode: "in_place" | "new_column" | "new_table"
"""

_PLAN_ACTIONS = """
Supported plan step actions (implement ALL steps in order per dataset):
- trim: strip whitespace on strings
- lowercase / uppercase: case normalization
- fill_or_drop / fill_nulls_simple: fill nulls (if never_drop_rows in business_rules, NEVER delete rows)
- cast_type: nullable integer use Int64 (pandas) / long (spark); preserve nulls
- coerce_numeric: safe numeric conversion
- parse_dates: safe datetime parsing
- sanitize_email: trim, lower, and validate with regex `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$` (if invalid, set to null, never drop row)
- replace_sentinel_values: replace placeholder/sentinel values in params.sentinel_values list (e.g. -999, 999999) with NULL
- normalize_phone: digits only
- hash_phone: F.sha2(column.cast('string'), 256) for privacy (per business notes / manual_review)
- mask_phone: keep last 4 digits with *** prefix
- regex_replace: clean per plan note if present
- range_clip: bound numeric values (e.g. lower bound 0)
- clip_or_flag / flag_outliers: IQR-based outlier flag column (suffix _outlier_flagged)
- clip_outliers: IQR clip values to bounds
- cap_outliers: IQR replace outliers with median
- standardize_boolean: map yes/no/1/0/true/false to 0/1
- replace_values: map values per business_rules.valid_values when provided
- zero_to_null: replace 0 with null
- deduplicate: drop duplicate rows (subset column/business keys if set, else full row; preserve latest by sorting watermark if key dedup is used)
- validate_referential_integrity_or_stage: emit validation/staging comments + checks, do not skip
"""

_BASE_RULES = """
UNIVERSAL RULES (mandatory):
1. Implement EVERY step in plan.datasets[*].steps in ascending "order". Do not skip or merge steps.
2. Read step["params"] for fill/outlier/privacy — match template codegen semantics.
3. Honor business_rules: never_drop_rows, required_columns, exclude_columns, non_nullable, valid_values, notes.
4. Preserve exact column name casing from the plan.
5. Add clear comments for manual_review items from the plan.
6. Production quality: logging.getLogger("agent_dhara"), guards for required columns, no placeholder TODOs for listed actions.
7. Output ONLY the artifact — no markdown fences, no prose before/after.
8. DATA COMPLETENESS MANDATE: For EVERY column in the plan, at minimum apply:
   - Whitespace trim (strings)
   - Type coercion to target_dtype
   - Null fill/flag per fill_strategy
   If a column has issues in the assessment but no explicit step was planned, ADD the
   most appropriate step from the supported actions list — DO NOT leave the column untouched.
9. NEVER emit placeholder comments like "# TODO: handle nulls" or "# Add cleaning here".
   Every step must be executable, production code.
10. BOOLEAN columns: ALWAYS standardize to 0/1 (int). Map: yes/true/1/y -> 1; no/false/0/n -> 0.
11. CATEGORY/ENUM columns: if valid_values is provided in business_rules, enforce it — 
    map unknowns to NULL or a 'Other' category, never leave invalid values in clean output.
12. ROW COUNT INTEGRITY (MANDATORY): The output row count must equal the input row count,
    UNLESS the plan explicitly includes a "deduplicate" step or an explicit filtering step.
    NEVER use .filter()/.where()/dropna()/inner-join to clean invalid formatting (nullify/flag instead).
13. Every dataset's transformation/write pipeline must log its row counts:
    logger.info("<dataset>: row_count=%s", df.count())
"""

SYSTEM_PROMPTS: Dict[str, str] = {
    "python": f"""You are a senior data engineer writing production Python ETL with pandas.

{_BASE_RULES}
{_PLAN_PARAMS}
{_PLAN_ACTIONS}

PYTHON REQUIREMENTS:
- Module docstring with plan_id summary.
- Imports: pandas, logging (and sys if needed). No os/subprocess/socket/shutil/ctypes/eval/exec.
- One transform_<dataset> function per dataset; each receives pd.DataFrame and returns pd.DataFrame.
- Start each function with df.copy(); use nullable Int64 for integer columns.
- Required columns: raise ValueError with clear message if missing.
- never_drop_rows: use fillna only, never dropna on rows.
- I/O: use connector_manifest read_snippet_python and write_snippet_python EXACTLY per dataset.
- NEVER read .xml with read_csv. Use read_xml for format=xml. NEVER write CSV to a .xml path.
- Use _resolve_data_path(location) helper when manifest shows blob paths (not bare filenames).
- if __name__ == "__main__": load_all_datasets / transform_all / run_joins / write_outputs from manifest.
- Executable, syntactically valid Python 3.10+.
""",
    "sql-tsql": f"""You are a senior data engineer writing production T-SQL ETL scripts.

{_BASE_RULES}
{_PLAN_PARAMS}
{_PLAN_ACTIONS}

T-SQL REQUIREMENTS:
- Header comment block with plan_id.
- Idempotent T-SQL DDL (CRITICAL): NEVER use `CREATE TABLE IF NOT EXISTS` — that is MySQL/PostgreSQL syntax and is INVALID in T-SQL/SQL Server. For every infrastructure table (etl_log, etl_rejects, etl_watermark, clean tables, indexes, procedures), you MUST use the correct T-SQL idempotent guard pattern:
  - Tables: `IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'TableName' AND schema_id = SCHEMA_ID('dbo')) CREATE TABLE dbo.TableName (...);`
  - Indexes: `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IndexName' AND object_id = OBJECT_ID('dbo.TableName')) CREATE NONCLUSTERED INDEX IndexName ON dbo.TableName (...);`
  - Procedures (SAFE & NON-DESTRUCTIVE): ALWAYS use `CREATE OR ALTER PROCEDURE dbo.ProcName AS BEGIN ... END; GO`. NEVER generate `DROP PROCEDURE` or `DROP TABLE`. `CREATE OR ALTER PROCEDURE` automatically creates or updates stored procedures safely without deleting existing tables or dropping objects.
  Each outer script DDL statement (infrastructure tables, staging tables, procedures, views) MUST be followed by a GO batch separator on its own line. NEVER output GO inside a stored procedure body.
- Execution Logging & Log ID Bugfix: Output DDL to create a logging table named `dbo.etl_log` with columns `id INT IDENTITY(1,1) PRIMARY KEY`, `process_name VARCHAR(100) NOT NULL`, `start_time DATETIME NOT NULL`, `end_time DATETIME NULL`, `status VARCHAR(20) NOT NULL`, `error_message VARCHAR(MAX) NULL` using the T-SQL idempotent guard described above. 
  Inside each stored procedure's TRY block, you MUST first run the `INSERT INTO dbo.etl_log (process_name, start_time, status) VALUES ('...', GETDATE(), 'RUNNING');` statement. IMMEDIATELY AFTER this insert, define and set the batch run ID: `DECLARE @run_id INT = SCOPE_IDENTITY();`. NEVER declare `@run_id = SCOPE_IDENTITY();` before any insert statement has occurred in the procedure, as this returns NULL and breaks audit batch tracking. Wrap the block in a transaction. Commit on success and rollback on failure.
- Balanced Transactions: Every Try-Catch block MUST wrap data modifications in an explicit transaction block. Begin the transaction inside `BEGIN TRY` using `BEGIN TRANSACTION;` immediately after defining `@run_id`. Commit the transaction using `COMMIT TRANSACTION;` at the very end of the `BEGIN TRY` block (after all updates and logging are completed). At the beginning of the `BEGIN CATCH` block, you MUST verify if a transaction is still active and roll it back using: `IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;`. Never omit `BEGIN TRANSACTION` if `COMMIT/ROLLBACK` are used, or vice versa, to ensure compilation succeeds.
- FORBIDDEN MySQL/PostgreSQL Syntax: NEVER output `CREATE TABLE IF NOT EXISTS`, `CREATE OR REPLACE`, `ON CONFLICT`, `RETURNING`, `ILIKE`, `SERIAL`, `AUTO_INCREMENT`, or any other non-T-SQL syntax. These are invalid in SQL Server and will cause the entire batch to fail with a hard rollback. T-SQL equivalents MUST be used at all times.
- Rule Merging & Single-Scan Consolidation: NEVER generate separate, sequential `UPDATE` statements for each plan step on the same table. Instead:
  1. Merge and consolidate all expression-based updates (like `LTRIM/RTRIM`, `LOWER/UPPER`, case normalization, formatting, phone normalization, date parsing, and range clipping) into a **single multi-column `UPDATE` statement** on the staging table `#<TableName>_Staging`.
  2. Merge all default value fillings and invalid/sentinel replacements into a **single join-based `UPDATE` statement** joining `dbo.etl_default_values` and `dbo.etl_invalid_values` via `LEFT JOIN`s.
- Raw -> Staging -> Transform -> Clean Architecture: Raw tables are completely immutable. Never write updates/modifications/deletions directly on raw tables. Create the target clean table (e.g., `dbo.Orders_Clean` for `dbo.Orders_Raw`) if it does not exist with standard audit columns: `etl_created_at DATETIME DEFAULT GETDATE()`, `etl_updated_at DATETIME DEFAULT GETDATE()`, and `etl_batch_id INT`. 
  Inside each table-cleaning stored procedure, initialize a temporary staging table (e.g. `#Orders_Staging`) by doing `SELECT * INTO #Orders_Staging FROM dbo.Orders_Clean WHERE 1=0;`. Copy the raw data (utilizing candidate key CTE deduplication and watermarking filters) into the staging table (populating `@run_id` to `etl_batch_id`). Execute all transformations, updates, and validations directly on the staging table `#Orders_Staging`. Finally, truncate/delete records in the target clean table `dbo.Orders_Clean` and insert the fully transformed records from the staging table into `dbo.Orders_Clean`.
- Modular Stored Procedures: Wrap all cleaning steps for each table into dedicated stored procedures named `dbo.etl_clean_<table_base_name>`.
- Master Orchestration: Generate a master coordinator procedure named `dbo.etl_main` that calls all the individual table cleaning stored procedures.
- Incremental Loading, Watermarking & Watermark Storage: Stored procedures and the main procedure must accept parameters `@load_type VARCHAR(20) = 'FULL'` and `@last_run DATETIME = NULL`. Generate DDL for `dbo.etl_watermark (process_name VARCHAR(100) PRIMARY KEY, last_run_time DATETIME NOT NULL)`. 
  If `@load_type = 'INCREMENTAL'` and `@last_run IS NULL`, retrieve it using `SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = '...';`. Ensure this `@last_run` filter is fully applied to the copy queries (e.g. `WHERE watermark_col > @last_run`). Update the watermark using `MERGE` upon successful completion.
- Performance Indexing: When creating the clean target table, add DDL commands to create NONCLUSTERED indexes on the primary keys, join/relationship keys, and watermark columns.
- Outlier Mitigation Safety (Catalog Checks): Implement the outlier flagging logic using a reusable stored procedure `dbo.sp_flag_outliers_iqr` that computes IQR and updates outlier flags dynamically. The procedure takes exactly two arguments: `@table_name` and `@column_name`. Validate that input table and column parameters exist in `sys.tables` and `sys.columns` (or `tempdb.sys.columns` for `#` temp tables) before dynamic SQL executions. NEVER call the procedure with extra parameters.
- Reusable Outlier Procedure Call: When executing IQR flagging on a column, invoke it exactly as `EXEC dbo.sp_flag_outliers_iqr '#TableName_Staging', 'ColumnName'`. Do not repeat the execution or call it multiple times for the same column. Only run outlier stored procedures on numeric/metric columns. NEVER execute it on string identifiers, phones, or emails.
- Zero Redundant Operations: Do not output duplicate or redundant CTE statements, updates, or procedure calls. Verify that any deduplication logic, outlier checks, or date/email validation is written once per column.
- Type-Safe String Transformations: If applying `LTRIM`, `RTRIM`, `LOWER`, or `UPPER` on a non-string column (such as numeric/date columns), first cast the column explicitly to a string type (e.g. `CAST(col AS NVARCHAR(MAX))`) before calling the string function, then cast back to the target type. (e.g. `TRY_CAST(NULLIF(LTRIM(RTRIM(CAST(credits AS NVARCHAR(50)))), '') AS INT)`).
- Rejects & Quarantine Logging: Enforce the use of `dbo.etl_rejects` table. Generate DDL to create the rejects logging table `dbo.etl_rejects` using the T-SQL idempotent guard (NEVER `CREATE TABLE IF NOT EXISTS`):
  ```sql
  IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'etl_rejects' AND schema_id = SCHEMA_ID('dbo'))
  CREATE TABLE dbo.etl_rejects (
      id INT IDENTITY(1,1) PRIMARY KEY,
      table_name VARCHAR(100) NOT NULL,
      rejected_row_data NVARCHAR(MAX) NOT NULL,
      reject_reason VARCHAR(255) NOT NULL,
      etl_batch_id INT NOT NULL,
      rejected_at DATETIME DEFAULT GETDATE()
  );
  GO
  ```
  For any row that fails validation format constraints (e.g. invalid date formats, invalid email patterns) or referential integrity (joins), you MUST insert the violating records into `dbo.etl_rejects` and delete them from the staging table prior to any transformation/cast steps. Use `(SELECT * FROM staging_alias r2 WHERE r2.[pk] = r.[pk] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)` to serialize the violating row data. Perform validation deletes before applying transformations/casts to preserve the original invalid values.
- Default Value Sanity & No Fake/Placeholder Defaults: Seed defaults/invalid values dynamically. Use `NULL` as the default value strategy for date, email, and phone/identifier columns to prevent downstream data pollution. NEVER hardcode placeholder default values (like `'10120631.5'` for dates, or `'99999'` for Phone/IDs) when filling nulls; un-defaulted values must remain `NULL`. Replace literal default values with lookup queries using `TRY_CAST(default_value AS <type>)` from `dbo.etl_default_values` (dynamic casting based on column data type).
- Multi-Format Date Parsing: When parsing date columns, use a coalesced chain of `TRY_CONVERT` with different format styles (e.g. style 120, 103, 101, 111) to check if the date is valid. For example: `COALESCE(TRY_CONVERT(DATETIME, [date_col], 120), TRY_CONVERT(DATETIME, [date_col], 103), TRY_CONVERT(DATETIME, [date_col], 101), TRY_CONVERT(DATETIME, [date_col], 111))`. If all conversion attempts fail and the value is not empty/null, treat it as a format validation failure, insert it into `dbo.etl_rejects`, and delete it from the staging table.
- Business-Key Deduplication: For row-level deduplication, partition by the candidate primary key and business keys (names containing `id`, `key`, `email`, `code`) instead of all non-key columns, and order the partition descending by watermark column (`ORDER BY <watermark> DESC`) to preserve the latest record. Perform deduplication inside the initial staging `INSERT INTO ... SELECT` statement using a CTE.
- Use bracket quoting [column] and TRY_CAST / TRY_CONVERT for safe casts.
- never_drop_rows: UPDATE/SET only, no DELETE FROM for data quality fixes (except when logging to rejects table).
- Email Validation: For Email columns, check for valid email syntax using the exact SQL pattern `NOT LIKE '%_@_%._%'`. If invalid, quarantine any invalid emails into `dbo.etl_rejects` and delete them from the staging table.
- Phone Normalization and Validation: For Phone columns, strip symbols `-`, ` `, `(`, `)` using nested `REPLACE` functions. If the cleaned phone number length is less than 7 or contains non-numeric characters (tested using `LIKE '%[^0-9]%'`), treat it as an invalid phone validation failure, quarantine it to `dbo.etl_rejects`, and delete them from the staging table.
- No Redundant Casts: Prohibit redundant string castings. Do not emit nested castings like `LOWER(CAST(LTRIM(RTRIM(CAST(col AS NVARCHAR(MAX)))) AS NVARCHAR(MAX)))`. If a column is already cast to a string type, or is the result of string functions (`LOWER`, `LTRIM`, `REPLACE`), do not wrap it in additional `CAST` statements.
- Orders Pipeline: Do not make the Orders pipeline a simple copy. Enforce strict date parsing (via coalesced `TRY_CONVERT` chains), status normalization (trim and case normalization), and invalid/null values handling.
- Duplicates Deduplication ordering: Never use non-existent columns (like `etl_created_at`) in `ROW_NUMBER() OVER (PARTITION BY LOWER(LTRIM(RTRIM(CAST([pk] AS NVARCHAR(400))))) ORDER BY ...)` inside the staging copy CTE. Use a business column (like `CreatedDate DESC` or `OrderDate DESC`) for ordering.
- Idempotent and Production-Safe views/joins: Ensure join views use `CREATE VIEW` instead of `SELECT ... INTO` to prevent duplicate view compilation failures or schema write conflicts.
- Round-number anomaly handling: flag/tag round-number anomalies if required.
- Month-end date clumping: flag month-end date clumping if required.
- Active curated views: listing selected fields and prefixing duplicate fields.
- Index-Friendly numeric checks: check numeric column placeholders without casting (e.g. `Quantity IN (-999, 999999)`).
- No SELECT DISTINCT * for deduplication: Avoid using expensive, non-key-aware `SELECT DISTINCT` statements. Deduplication must be key-aware using CTE `ROW_NUMBER()`.
""",
    "sql-ansi": f"""You are a senior data engineer writing portable ANSI SQL ETL scripts.

{_BASE_RULES}
{_PLAN_ACTIONS}

ANSI SQL REQUIREMENTS:
- Header comment block with plan_id.
- Raw -> Staging -> Transform -> Clean Architecture: Raw tables are completely immutable. Never write updates/modifications/deletions directly on raw tables. Create the target clean table (e.g., `Orders_Clean` for `Orders_Raw`) if it does not exist with standard audit columns: `etl_created_at`, `etl_updated_at`, and `etl_batch_id`. 
  Inside each table-cleaning stored procedure, initialize a temporary staging table (e.g. `#Orders_Staging`) by doing `SELECT * INTO #Orders_Staging FROM Orders_Clean WHERE 1=0;`. Copy the raw data (utilizing candidate key CTE deduplication and watermarking filters) into the staging table (populating `@run_id` to `etl_batch_id`). Execute all transformations, updates, and validations directly on the staging table `#Orders_Staging`. Finally, truncate/delete records in the target clean table `Orders_Clean` and insert the fully transformed records from the staging table into `Orders_Clean`.
- Modular Stored Procedures: Wrap all cleaning steps for each table into dedicated stored procedures named `etl_clean_<table_name>`.
- Master Orchestration: Generate a master coordinator procedure named `etl_main` that calls all the individual table cleaning stored procedures.
- Execution Logging & Log ID Bugfix: Output DDL to create a logging table named `etl_log` and log the start, success, and failure status (with errors) within exception blocks. Always insert the log row first, and then capture the generated identity/auto-increment variable immediately to define the batch run ID safely without race conditions.
- Balanced Transactions: Every block/procedure performing data modifications MUST wrap them in an explicit transaction. Start with `BEGIN TRANSACTION;` inside the try block immediately after capturing `@run_id`. End with `COMMIT TRANSACTION;` at the end of the success path. In exception handlers, verify and rollback using `ROLLBACK TRANSACTION;`.
- Incremental Loading, Watermarking & Watermark Storage: Accept `@load_type` and `@last_run` parameters. If incremental, retrieve the watermark value from `etl_watermark` if not provided, and filter raw source rows using the watermark. Prior to inserting the incremental batch, delete matching clean rows by primary key to prevent duplicate records.
- Performance Indexing: Add statements or comments recommending indexes on primary keys, join keys, and watermark columns.
- Rule Merging & Single-Scan Consolidation: NEVER generate separate, sequential `UPDATE` statements for each plan step on the same table. Instead:
  1. Merge and consolidate all expression-based updates (like `LTRIM/RTRIM`, `LOWER/UPPER`, case normalization, formatting, phone normalization, date parsing, and range clipping) into a **single multi-column `UPDATE` statement** on the staging table.
  2. Merge all default value fillings and invalid/sentinel replacements into a **single join-based `UPDATE` statement** joining `etl_default_values` and `etl_invalid_values` via `LEFT JOIN`s.
- Zero Redundant Operations: Do not output duplicate or redundant CTE statements, updates, or procedure calls. Verify that any deduplication logic, outlier checks, or date/email validation is written once per column.
- Type-Safe String Transformations: When trimming or lowercasing non-string columns, first cast the column explicitly to a string type (e.g. `CAST(col AS VARCHAR(50))`) before applying the string function.
- Rejects & Quarantine Logging: Validate date and email constraints and quarantine violating records to an `etl_rejects` table before removing them from the staging table. Perform validation deletes before applying transformations/casts to preserve the original invalid values. Create the table DDL if not exists: `CREATE TABLE etl_rejects (id INT, table_name VARCHAR(100), rejected_row_data VARCHAR(MAX), reject_reason VARCHAR(255), etl_batch_id INT, rejected_at TIMESTAMP)`.
- Default Value Sanity & No Fake/Placeholder Defaults: Use `NULL` as the default value strategy for date, email, and phone/identifier columns to prevent downstream data pollution. NEVER hardcode placeholder default values (like `'10120631.5'` for dates, or `'99999'` for Phone/IDs) when filling nulls; un-defaulted values must remain `NULL`. Use `etl_default_values` lookup table queries (with type-safe dynamic casting based on column data type) and `etl_invalid_values` lookup table queries instead of hardcoded default/sentinel values.
- Multi-Format Date Parsing: Cascaded parsing using conditional conversion attempts (e.g., trying style 120, 103, 101, 111). If all fail, quarantine the row as invalid format into `etl_rejects`.
- Business-Key Deduplication: Partition row-level deduplication by business keys/primary keys, sorting descending by the watermark column to preserve the latest record. Perform deduplication inside the initial staging `INSERT` statement using a CTE rather than doing standalone `DELETE` statements.
- Index-Friendly Numeric Checks: Avoid casting columns for sentinel/placeholder checks where possible (especially for numeric columns).
- Active Curated Views: Generate active view layers `CREATE VIEW vw_<table_base>_Fact AS` explicitly listing selected fields and renaming duplicate joined fields to prevent duplicate column errors.
- Safe casts (CAST/TRY semantics via CASE WHERE not available).
- IQR outlier logic with subqueries or CTEs, not dialect-specific hacks unless noted in comments. Only run outlier checks on numeric/metric columns.
- never_drop_rows: no DELETE for quality fixes (except when logging to rejects table).
- Email Validation: For Email columns, check for valid email syntax using the exact SQL pattern `NOT LIKE '%_@_%._%'`. If invalid, quarantine any invalid emails into `etl_rejects` and delete them from the staging table.
- Phone Normalization and Validation: For Phone columns, strip symbols `-`, ` `, `(`, `)` using nested `REPLACE` functions. If the cleaned phone number length is less than 7 or contains non-numeric characters (tested using `LIKE '%[^0-9]%'`), treat it as an invalid phone validation failure, quarantine it to `etl_rejects`, and delete them from the staging table.
- No Redundant Casts: Prohibit redundant string castings. Do not emit nested castings like `LOWER(CAST(LTRIM(RTRIM(CAST(col AS VARCHAR(MAX)))) AS VARCHAR(MAX)))`. If a column is already cast to a string type, or is the result of string functions (`LOWER`, `LTRIM`, `REPLACE`), do not wrap it in additional `CAST` statements.
- Orders Pipeline: Do not make the Orders pipeline a simple copy. Enforce strict date parsing (via coalesced `TRY_CONVERT` chains), status normalization (trim and case normalization), and invalid/null values handling.
- Duplicates Deduplication ordering: Never use non-existent columns (like `etl_created_at`) in `ROW_NUMBER() OVER (PARTITION BY LOWER(LTRIM(RTRIM(CAST([pk] AS VARCHAR(400))))) ORDER BY ...)` inside the staging copy CTE. Use a business column (like `CreatedDate DESC` or `OrderDate DESC`) for ordering.
- No SELECT DISTINCT * for deduplication: Avoid using expensive, non-key-aware `SELECT DISTINCT` statements. Deduplication must be key-aware using CTE `ROW_NUMBER()`.
- Idempotent and Production-Safe views/joins: Ensure join views use `CREATE VIEW` instead of `SELECT ... INTO` to prevent duplicate view compilation failures or schema write conflicts.
""",
    "pyspark": f"""You are a senior data engineer writing production PySpark ETL.

{_BASE_RULES}
{_PLAN_PARAMS}

PYSPARK REQUIREMENTS:
- Module docstring with plan_id.
- CRITICAL: The FIRST lines of code after the docstring MUST be module-level imports (before any def or class):
    from typing import Optional, Dict
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    import logging
  DO NOT place these inside function bodies. They must be at the very top of the file.
- DATASETS DECLARATION: At the module level, ALWAYS declare: `DATASETS = ["<dataset_name>"]` (one entry per dataset).
- FUNCTION SIGNATURE: `def transform_<dataset>(df: DataFrame, all_dfs: Optional[dict] = None) -> DataFrame:` per dataset. ALWAYS include `all_dfs: Optional[dict] = None` as the second parameter so calling `transform_<dataset>(df, dfs)` with 2 arguments never raises a TypeError.
- REQUIRED HELPERS: ALWAYS define `_require_columns`, `_warn_duplicate_keys`, `_warn_nulls_in_columns`, `_log_row_count`, and `_prefix_columns` at the top of the script.
- NO CAST ON CATEGORICAL COLUMNS: NEVER cast categorical text columns (e.g. location, channel, region, status, customer_name, machine_name) to double, long, or int. Casting text like 'NY', 'Mumbai', 'email', 'social', 'tv' to numbers destroys data by turning text into NULL. Keep them as string. NEVER overwrite these columns with NULLs using digit-only filters (e.g. `rlike('^[0-9]+$')` returning NULL is strictly forbidden).
- NO LOWERCASE FOR IDENTIFIERS: NEVER lowercase business identifier keys (e.g. campaign_id, product_id, factory_id, sale_id) using F.lower or .lower(). Keep their original casing intact, applying only whitespace F.trim().
- NO NUMERIC IMPUTATION ON BUSINESS KEYS: NEVER fill business keys / identifiers (e.g. factory_id, campaign_id, product_id, sale_id) with mean, median, 123, or arbitrary numbers. Emit `is_missing_<col>` flag and keep NULL intact.
- NO IDENTITY COMPARISON WITH LITERALS: NEVER use identity comparisons (`is` or `is not`) against literals (e.g. `if 123 is not None:` or `if 'unknown' is not None:`). Identity checks must only be used with `None` or object identity (e.g. `if val is not None:`). For literals, use `!=` or `==` or omit the check entirely.
- SAFE DATE PARSING: For `parse_dates` / `parse_dates_safe`, use `F.to_timestamp(F.col(c))` without hardcoding rigid date format strings like `'yyyy-MM-dd'`. Pre-filter bad strings or sentinels with `rlike` before converting to prevent `SparkDateTimeException` in Spark ANSI mode.
- MULTILINE JSON READS: For JSON files, ALWAYS include `.option('multiLine', 'true')` when reading (e.g. `spark.read.option('multiLine', 'true').json(...)`) so multiline JSON objects are parsed into DataFrame columns instead of `_corrupt_record`.
- EXPLICIT STEP ANNOTATIONS: Include a comment line `# Step: <action> on [<column>]` before every transformation step so step action reflection validation succeeds.
- NO INVENTED DEFAULTS: NEVER invent fill values (e.g. 'unknown@example.com', 0) unless fill_value is explicitly provided in the step params. If no fill_value is given in the step params, DO NOT generate any fillna or coalesce code for that column (keep the original column as-is). NEVER emit fillna(None) or fillna({{'col': None}}) as it is invalid PySpark.
- DYNAMIC MEAN/MEDIAN IMPUTATION: NEVER hardcode static values for mean or median imputation (e.g. mean_id = 4993.0656). ALWAYS compute them dynamically from the DataFrame using Spark aggregations (e.g. `_avg_row = df.select(F.avg(F.col(c).cast('double')).alias('m')).first()`).
- MEDIAN/MEAN IMPUTATION RULE: For every mean or median imputation on a numeric column (like amount, budget, qty), always cast the aggregation input to double inside the selector:
    Correct:
    `_median_row = df.select(F.percentile_approx(F.col("amount").cast("double"), 0.5).alias("m")).first()`
    Incorrect:
    `_median_row = df.select(F.percentile_approx(F.col("amount"), 0.5).alias("m")).first()`
- NEVER USE fillna WITH F.lit(): fillna() only accepts primitive values (numbers, strings). If using F.lit() or a dynamic variable, use `withColumn` and `F.coalesce` instead (e.g. `df = df.withColumn("amount", F.coalesce(F.col("amount"), F.lit(median_amount).cast("double")))`).
- NO AVG/MEDIAN ON STRING COLUMNS: NEVER call `F.avg()`, `avg()`, `F.mean()`, `mean()`, `percentile_approx()`, or `F.percentile_approx()` on a string/text/categorical column. If the column dtype is 'string', 'object', or the column name matches location, region, channel, machine_name, status, category, site, facility, or similar text-categorical patterns, use `df.withColumn(col, F.coalesce(F.col(col), F.lit('unknown')))` instead of any numeric aggregation. For numeric-typed columns that are stored as string (e.g. production_qty, amount, budget), YOU MUST ALWAYS ADD `.cast('double')` before calling percentile_approx or F.avg: `F.percentile_approx(F.col(c).cast('double'), 0.5)` and `F.avg(F.col(c).cast('double'))`. Failing to cast double first crashes Spark with an AnalysisException.
- NO RLIKE ON NUMERIC COLUMNS: NEVER use `.rlike()` or `rlike` to check, clean, or nullify numeric columns (columns containing 'amount', 'budget', 'qty', 'quantity', 'price', 'cost', 'score', 'total', 'val'). To nullify or coerce non-numeric string values in numeric columns, cast the column to double (`F.col(col).cast('double')`) which automatically converts non-numeric strings to NULL. Do not filter with regex pattern `rlike` on numeric columns.
- SAFE .first() ACCESS: ALWAYS use `_row['m'] if (_row and _row['m'] is not None) else None` pattern when accessing `.first()` results. NEVER use `_row['m'] if _row else None` as this will incorrectly return None when the row exists but the aggregation result is None (e.g. all-null column).
- NO ROW COUNT LOGGING IN TRANSFORM: NEVER call `_log_row_count` inside `transform_*` functions. Row count logging is handled automatically at the pipeline level inside `run_pipeline` after all steps. Calling it inside transform functions causes redundant Spark jobs.
- CRITICAL PYSPARK LITERAL RULE: In PySpark `F.when(...)` or `.otherwise(...)`, ALWAYS wrap Python nulls in `F.lit(None)`. NEVER pass raw Python `None` directly as a value argument (e.g. use `F.when(cond, F.lit(None)).otherwise(F.col(c))`, NEVER `F.when(cond, None)`). Raw `None` causes PySpark Py4J runtime crashes.
- NO ITERATION FOR VALUE LISTS: NEVER use a Python `for` loop to repeat `withColumn` for multiple values. Use `F.col(c).isin(value_list)` in a single `F.when(...)` expression.
- NO NO-OP STEPS: NEVER output steps that replace null with null (e.g. `withColumn('col', F.when(F.col('col').isNull(), None).otherwise(F.col('col')))`).
- NULLIFY PUNCTUATION: To nullify punctuation-only strings on string columns, use `df = df.withColumn(col, F.when(~F.col(col).cast('string').rlike('[a-zA-Z0-9]'), F.lit(None)).otherwise(F.col(col)))`. For numeric columns (e.g. budget, amount, qty, total), NEVER use `.rlike()` or string regex; instead, implement `nullify_punctuation` by casting the column to double: `df = df.withColumn(col, F.col(col).cast('double'))`. Do not skip or omit `nullify_punctuation` steps from the plan.
- DEDUPLICATE: For every deduplicate plan step, you MUST emit actual working PySpark code. NEVER emit a comment like `# Manual review required for deduplication` as the sole output. If business_rules.never_drop_rows is True, flag duplicates using a window function partitioned by row hash — do NOT drop rows: `from pyspark.sql import Window as _DedupWindow; _data_cols = [F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in df.columns if not c.startswith('_') and not c.startswith('is_')]; _dup_w = _DedupWindow.partitionBy(F.xxhash64(*_data_cols) if _data_cols else F.lit(1)).orderBy(F.monotonically_increasing_id()); df = df.withColumn('_is_duplicate_row', (F.row_number().over(_dup_w) > 1).cast('boolean'))`. NEVER emit `Window.orderBy(...)` without `partitionBy(...)`. If never_drop_rows is False (or not set), use `df = df.dropDuplicates()`.
- FLAG_OUTLIERS: Outlier detection (`_iqr_bounds`) is ONLY valid for numeric metric columns (e.g. amount, budget, quantity, price, cost, total). NEVER emit `_iqr_bounds` or outlier detection on business keys or text columns (e.g. product_id, sale_id, factory_id, location, region, customer). For valid numeric metrics, emit: `_stats, _iqr, _lower, _upper = _iqr_bounds(df, '<col>'); df = df.withColumn('<col>_outlier_flagged', (F.col('<col>') < _lower) | (F.col('<col>') > _upper))`.
- IMPUTATION LINEAGE: When imputing nulls on a column using `fillna`, emit a boolean flag column `is_imputed_<col>` before `fillna`: `df = df.withColumn('is_imputed_col', F.col('col').isNull().cast('boolean'))` followed by `df = df.fillna({{'col': fill_val}})`.
- SAFE CASTS & DATES: To prevent Spark ANSI mode runtime exceptions on malformed strings (e.g. 'error', 'TBD', '00/00/0000'), use regex-gated double casts `F.when(F.trim(F.col(c).cast('string')).rlike(r'^-?(?:\\d+(?:\\.\\d+)?|\\.\\d+)$'), F.trim(F.col(c).cast('string')).cast('double')).otherwise(F.lit(None).cast('double'))` for numeric columns, and safe timestamp parsing for date columns.
- NO TOP-LEVEL EXECUTION: Do NOT emit standalone pipeline execution lines (like `sales_df = spark.read...` or `sales_df.write...`) at the top level outside function definitions. All pipeline loading, transforming, logging, and writing MUST take place inside `def run_pipeline(spark):`.
- never_drop_rows: coalesce/fill only, no filter that drops null-quality rows.
- I/O: use connector_manifest read_snippet_pyspark and write_snippet_pyspark per dataset.
- NEVER use spark.read.csv for .xml files. Use format("com.databricks.spark.xml") for format=xml.
- NEVER write .csv() to a path ending in .xml -- use parquet or json matching manifest output_path.
- Load paths via _resolve_data_path(manifest location), not bare filenames like "data_1.json".
- COPY the full _resolve_data_path helper from the user message (uses AZURE_STORAGE_ACCOUNT, DHARA_BLOB_CONTAINER, DHARA_BLOB_BASE_PATH). NEVER return only f"abfss://{{location}}".
- Pipeline order: load -> transform each dataset -> join (if needed) -> write ALL outputs (per-dataset + joined_* if joined).
- CLEANSE ONLY / PHASE 1 ISOLATION: If the plan generation_mode is 'cleanse_only' or plan_id ends with '_cleanse', you MUST NOT generate any join operations (e.g. df1.join(df2)) or write joined outputs. Only load, transform each dataset independently, and write dataset-specific clean output tables.
- Joins: prefix right-hand columns with _prefix_columns before join; store result in dfs["joined_<parent>_<child>"] and WRITE it to parquet.
- Do NOT assign a join to a variable that is never written (no dead df_joined).
- Pre-join: _require_columns for business_rules.required_columns; _warn_duplicate_keys on join keys.
- When plan is per-dataset normalization only (lowercase/hash, no enrichment need): SKIP joins; write each cleaned dataset only.
- never_drop_rows + joins: use how="left" only, never inner.
- TOP-LEVEL PIPELINE EXECUTION FOR FABRIC: Call `run_pipeline(spark)` directly at top level so Fabric notebook cells execute pipeline code immediately.
- Valid Python 3.10+ invoking PySpark APIs only.
""",
    "adf": f"""You are a senior Azure Data Factory engineer.

{_BASE_RULES}
{_PLAN_PARAMS}
{_PLAN_ACTIONS}

ADF REQUIREMENTS:
- Output JSON with bundle.flows: [clean_only flow, clean_and_joined flow] when relationships.joins exist.
- Use ADF expression language: toLower, toUpper, trim, coalesce, iif, percentile, sha2, regexpReplace.
- derivedColumn transformations: typeProperties.columns[] with name + expression per step params.
- Join transforms: joinType left (never inner when never_drop_rows), leftStream/rightStream from upstream chain.
- Linked services: LS_AzureBlob, datasets DS_<dataset>, DS_<dataset>_cleaned.
- Valid JSON only (no markdown).
""",
}


def is_llm_generation_error(text: str) -> bool:
    return (text or "").strip().startswith(LLM_ERROR_PREFIX)


def normalize_codegen_engine(engine: str, sql_dialect: str = "tsql") -> str:
    e = (engine or "python").lower().strip()
    d = (sql_dialect or "tsql").lower().strip()
    if e in ("spark", "pyspark"):
        return "pyspark"
    if e == "adf":
        return "adf"
    if e in ("sql", "tsql", "ansi") or "sql" in e:
        if e == "ansi" or d == "ansi":
            return "sql-ansi"
        return "sql-tsql"
    return "python"


def _get_llm_client():
    cfg = load_llm_config(purpose="etl_codegen")
    if not cfg:
        return None, None
    if cfg.provider == "azure_openai" and AzureOpenAI and cfg.endpoint:
        client = AzureOpenAI(
            azure_endpoint=cfg.endpoint,
            api_key=cfg.api_key,
            api_version=cfg.api_version or "2024-02-01",
        )
        return client, cfg.model
    if cfg.provider == "openai" and OpenAI:
        return OpenAI(api_key=cfg.api_key), cfg.model
    return None, None


def _strip_markdown_fences(text: str) -> str:
    code = (text or "").strip()
    code = re.sub(r"^```[a-zA-Z0-9_-]*\s*\n?", "", code)
    code = re.sub(r"\n?```\s*$", "", code)
    code = code.strip()
    if "&" in code or "<" in code:
        code = html.unescape(code)
    return code.strip()


def _safe_max_tokens(prompt_text: str, engine_key: str) -> int:
    cfg = load_llm_config(purpose="etl_codegen")
    context_window = get_context_window(cfg.model) if cfg else 128_000
    # Measured: sql-tsql system prompt ≈ 7500 tokens; python ≈ 4500; pyspark ≈ 5000
    overhead_map = {
        "sql-tsql": 8000,
        "sql-ansi": 8000,
        "python":   5000,
        "pyspark":  5500,
        "adf":      3500,
    }
    system_overhead = overhead_map.get(engine_key, 5000)
    input_tokens = _estimate_tokens(prompt_text)
    available = context_window - input_tokens - system_overhead - 500  # 500 buffer
    
    # Model-specific caps to prevent out-of-limits API errors
    cap_map = {
        "gpt-4o-mini": 16000,
        "gpt-4o": 4096,
        "gpt-4": 4096,
        "gpt-35-turbo": 4096,
    }
    model_name = (cfg.model.lower() if cfg else "gpt-4o")
    cap = 4096
    for k, v in cap_map.items():
        if k in model_name:
            cap = v
            break
            
    if engine_key == "adf":
        cap = min(cap, 6000)
        
    return max(2000, min(cap, available))


def _classify_column(
    col_name: str,
    col_meta: dict | None,
    sem_schema: dict | None = None,
    ds_name: str = "",
) -> str:
    from agent.etl_pipeline.semantic_classifier import profile_column
    meta = col_meta or {}
    dtype = meta.get("dtype") or meta.get("target_dtype") or meta.get("inferred_type") or "string"
    semantic_type = meta.get("semantic_type") or ""
    
    if sem_schema:
        desc = sem_schema.get(f"{ds_name}.{col_name}") or {}
        if desc.get("semantic_type"):
            semantic_type = desc.get("semantic_type")
            
    prof = profile_column(col_name, dtype, semantic_type)
    if prof.is_temporal:
        return "date"
    if prof.is_numeric:
        return "metric"
    if prof.is_categorical:
        return "categorical"
    if prof.is_identifier:
        return "id"
    return "string"


def _is_numeric_column(col_name: str, col_meta: dict) -> bool:
    from agent.etl_pipeline.semantic_classifier import profile_column
    col_str = str(col_name or "").lower()
    is_key = any(k in col_str for k in ("id", "key", "code", "ref", "num", "no", "nbr", "uuid", "guid", "sku", "ean", "isbn", "asin"))
    is_cat = any(k in col_str for k in ("region", "channel", "location", "category", "status", "country", "state", "city", "zone", "segment", "type", "customer", "name", "machine", "factory", "plant", "site"))
    if is_key or is_cat:
        return False
    dtype = col_meta.get("dtype") or col_meta.get("target_dtype") or col_meta.get("inferred_type") or "string"
    semantic_type = col_meta.get("semantic_type") or ""
    prof = profile_column(col_name, dtype, semantic_type)
    return prof.is_numeric and not prof.is_identifier



def _consolidate_and_filter_datasets(
    datasets: Dict[str, Any],
    source_metadata: Dict[str, Any],
    sem_schema: Dict[str, Any] = None,
    strict_mode: bool = False,
) -> Dict[str, Any]:
    cleaned_datasets = {}
    
    # Priority for sorting steps
    priority = {
        "trim": 10,
        "lowercase": 11,
        "uppercase": 11,
        "sanitize_email": 12,
        
        "zero_to_null": 30,
        "replace_sentinel_values": 31,
        "cast_type": 35,
        "coerce_numeric": 40,
        
        "parse_dates": 50,
        
        "regex_replace": 60,
        "replace_values": 61,
        "standardize_boolean": 62,
        "normalize_phone": 63,
        "hash_phone": 64,
        "mask_phone": 65,
        
        "range_clip": 70,
        "clip_or_flag": 71,
        "flag_outliers": 72,
        "clip_outliers": 73,
        "cap_outliers": 74,
        
        "fill_or_drop": 78,
        "fill_nulls_simple": 78,
        
        "deduplicate": 80,
        "validate_referential_integrity_or_stage": 90
    }
    
    for ds_name, block in (datasets or {}).items():
        if not isinstance(block, dict):
            cleaned_datasets[ds_name] = block
            continue
            
        steps = block.get("steps") or []
        if not steps:
            cleaned_datasets[ds_name] = block
            continue
            
        ds_meta = source_metadata.get(ds_name) or {}
        columns_meta = ds_meta.get("columns") or {}
        
        filtered_steps = []
        seen_operations = set()
        
        def _get_col_meta(col_name: str, step_dict: dict) -> dict:
            meta = columns_meta.get(col_name) or {}
            if not meta and step_dict and "params" in step_dict and isinstance(step_dict["params"], dict):
                params_dict = step_dict["params"]
                dtype_val = params_dict.get("dtype") or params_dict.get("target_dtype") or params_dict.get("target_type")
                if dtype_val:
                    meta = {"dtype": dtype_val}
            return meta
        
        for step in steps:
            if not isinstance(step, dict):
                continue
            action = str(step.get("action") or "").strip().lower()
            col = step.get("column")
            # Governance-aware step filtering:
            if col:
                col_str = str(col)
                col_lower = col_str.lower()
                is_key = any(k in col_lower for k in ("id", "key", "code", "ref", "num", "no", "nbr", "uuid", "guid", "sku", "ean", "isbn", "asin"))
                is_cat = any(k in col_lower for k in ("region", "channel", "location", "category", "status", "country", "state", "city", "zone", "segment", "type"))

                # A. Convert lowercase/uppercase on business keys/metric/date/metadata columns to trim
                if col and action in ("lowercase", "uppercase"):
                    col_meta = _get_col_meta(col, step)
                    col_class = _classify_column(col, col_meta, sem_schema, ds_name)
                    if is_key or col_class in ("metric", "date", "metadata") or _is_numeric_column(col, col_meta):
                        logger.info(f"[Step Filter] Converted '{action}' on column '{col}' (class={col_class}) to 'trim'")
                        step["action"] = "trim"
                        action = "trim"

                # B. preserve numeric cast on categorical text columns to trigger domain flag generation in codegen
                pass

                # C. Convert null imputation on business keys to flag_missing_key
                if is_key and action in ("fill_nulls_simple", "fill_or_drop"):
                    logger.info(f"[Step Filter] Converted null imputation '{action}' on business key '{col}' to 'flag_missing_key'")
                    step["action"] = "flag_missing_key"
                    action = "flag_missing_key"
                    step.pop("value", None)
                    step.pop("fill_value", None)
                    if "params" in step and isinstance(step["params"], dict):
                        step["params"].pop("value", None)
                        step["params"].pop("fill_value", None)

                # D. Convert mean/median strategies and numeric fill values on categorical/string columns to value = 'unknown'
                col_meta_for_fill = _get_col_meta(col, step)
                if (is_cat or not _is_numeric_column(col, col_meta_for_fill)) and action in ("fill_nulls_simple", "fill_or_drop"):
                    params = step.get("params") or {}
                    strat = params.get("fill_strategy") or step.get("strategy") or ""
                    fval = step.get("value") if "value" in step else step.get("fill_value")
                    if fval is None:
                        fval = params.get("fill_value") if "fill_value" in params else params.get("value")
                    
                    if strat in ("mean", "median") or isinstance(fval, (int, float)) or (fval is not None and str(fval).strip().lstrip("-").replace(".", "", 1).isdigit()):
                        logger.info(f"[Step Filter] Converted fill step on categorical/string column '{col}' to strategy='value', value='unknown'")
                        step["strategy"] = "value"
                        step["value"] = "unknown"
                        step["fill_value"] = "unknown"
                        if "params" in step and isinstance(step["params"], dict):
                            step["params"]["fill_strategy"] = "value"
                            step["params"]["strategy"] = "value"
                            step["params"]["fill_value"] = "unknown"
                            step["params"]["value"] = "unknown"

                # E. Universal Numeric Coercion: For measure columns (budget, amount, production_qty, qty, price, count, total, score),
                # map cast_type to target_dtype='double' and enforce nullify_sentinels
                is_measure = any(m in col_lower for m in ("budget", "amount", "qty", "quantity", "price", "cost", "score", "total", "val", "production_qty", "revenue", "sales", "count", "fee", "balance"))
                if is_measure and action in ("cast_type", "cast"):
                    params = step.setdefault("params", {})
                    params["target_dtype"] = "double"
                    params["target_type"] = "double"
                    logger.info(f"[Step Filter] Enforced target_dtype='double' for measure column '{col}'")

            # F. Candidate Key Auto-Resolution for Row-Level Duplicate Steps
            if action in ("flag_duplicate_candidates", "flag_business_key_duplicate", "deduplicate"):
                params = step.setdefault("params", {})
                if not params.get("keys") and not params.get("column_keys"):
                    # Find candidate business keys from dataset schema metadata
                    ds_cols = list((columns_meta or {}).keys())
                    bk_keys = [c for c in ds_cols if any(k in c.lower() for k in ("id", "key", "code", "date"))]
                    if bk_keys:
                        params["keys"] = bk_keys[:3]
                        params["column_keys"] = bk_keys[:3]
                        logger.info(f"[Step Filter] Auto-assigned candidate keys {bk_keys[:3]} to duplicate step on dataset '{ds_name}'")

            # 1. Type-aware filtering: trim/lower/upper/sanitize_email are string-only
            if not strict_mode and action in ("trim", "lowercase", "uppercase", "sanitize_email"):
                if col:
                    col_meta = _get_col_meta(col, step)
                    col_class = _classify_column(col, col_meta, sem_schema, ds_name)
                    if col_class in ("metric", "date", "metadata"):
                        logger.info(f"[Step Filter] Dropped '{action}' on '{col}' — classifier={col_class} (ds={ds_name})")
                        continue
            
            # Type-aware filtering: outlier logic is numeric-only (always enforce regardless of strict_mode)
            if action in ("flag_outliers", "clip_or_flag", "clip_outliers", "cap_outliers"):
                if col:
                    col_meta = _get_col_meta(col, step)
                    col_str = str(col).lower()
                    is_key = any(k in col_str for k in ("id", "key", "code", "ref", "num", "no", "nbr", "uuid", "guid", "sku", "ean", "isbn", "asin"))
                    is_cat = any(k in col_str for k in ("region", "channel", "location", "category", "status", "country", "state", "city", "zone", "segment", "type", "customer", "name", "machine", "factory", "plant", "site"))
                    if is_key or is_cat or not _is_numeric_column(col, col_meta):
                        logger.info(f"[Step Filter] Dropped '{action}' on '{col}' — non-numeric/identifier/categorical (ds={ds_name})")
                        continue
                        
            # 2. Operation Deduplication / Normalization
            norm_action = action
            if action in ("clip_or_flag", "flag_outliers"):
                norm_action = "flag_outliers"
            elif action in ("fill_nulls_simple", "fill_or_drop"):
                norm_action = "fill_or_drop"
            elif action in ("clip_outliers", "cap_outliers"):
                norm_action = "modify_outliers"
                
            is_internal_trim = (norm_action == "trim") and (step.get("source_issue_type") == "internal_whitespace" or step.get("issue_type") == "internal_whitespace")
            op_key = (norm_action, str(col).lower() if col else None, is_internal_trim)
            if op_key in seen_operations:
                logger.info(f"[Step Filter] Dropped duplicate/normalized '{action}' on '{col}' (ds={ds_name})")
                continue
            seen_operations.add(op_key)
            
            filtered_steps.append(step)
            
        # 3. Sort steps according to transform priority order
        def get_step_priority(st):
            act = str(st.get("action") or "").strip().lower()
            return priority.get(act, 99)
            
        sorted_steps = sorted(filtered_steps, key=get_step_priority)
        
        # Re-assign order field
        import copy
        for idx, st in enumerate(sorted_steps):
            st_copy = copy.deepcopy(st)
            st_copy["order"] = idx + 1
            sorted_steps[idx] = st_copy
            
        cleaned_block = copy.deepcopy(block)
        cleaned_block["steps"] = sorted_steps
        cleaned_datasets[ds_name] = cleaned_block
        
    return cleaned_datasets


def _build_codegen_payload(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    *,
    output_mode: str = "dataframe_only",
    output_path: Optional[str] = None,
    strict_mode: bool = False,
) -> Dict[str, Any]:
    source_metadata: Dict[str, Any] = {}
    sem_schema = plan.get("semantic_schema") or {}
    for ds_name, meta in (assessment.get("datasets") or {}).items():
        cols = meta.get("columns") or {}
        source_metadata[ds_name] = {
            "row_count": meta.get("row_count"),
            "columns": {
                col: {
                    "dtype": cmeta.get("dtype") or cmeta.get("inferred_type"),
                    "null_percentage": cmeta.get("null_percentage"),
                    "semantic_type": (sem_schema.get(f"{ds_name}.{col}") or {}).get("semantic_type") or cmeta.get("semantic_type", "unknown"),
                    "sub_type": (sem_schema.get(f"{ds_name}.{col}") or {}).get("sub_type") or cmeta.get("sub_type", "unknown"),
                    "pii_level": (sem_schema.get(f"{ds_name}.{col}") or {}).get("pii_level") or "none",
                }
                for col, cmeta in cols.items()
                if isinstance(cmeta, dict)
            },
        }
    
    # Consolidate, deduplicate, type-filter and sort plan steps before passing to LLM
    raw_datasets = plan.get("datasets") or {}
    strict = strict_mode or bool(
        plan.get("strict_mode")
        or plan.get("settings", {}).get("strict_mode", False)
        or (plan.get("business_rules") or {}).get("strict_mode", False)
    )
    cleaned_datasets = _consolidate_and_filter_datasets(
        raw_datasets, source_metadata, plan.get("semantic_schema"), strict_mode=strict
    )
    
    from agent.etl_pipeline.dq_gate import evaluate_dq_gate
    try:
        gate_res = evaluate_dq_gate(assessment, plan.get("business_rules"))
        dq_gate_summary = {
            "passed": gate_res.get("passed"),
            "blocking_issues": gate_res.get("blocking_issues") or [],
            "warnings": gate_res.get("warnings") or [],
        }
    except Exception:
        dq_gate_summary = {"passed": True, "blocking_issues": [], "warnings": []}

    from agent.etl_pipeline.llm_rec_mapper import enrich_with_catalog
    manual_review_enriched = enrich_with_catalog(plan.get("manual_review") or [])

    base = {
        "plan_id": plan.get("plan_id"),
        "engine": plan.get("engine"),
        "output_mode": output_mode,
        "output_path": output_path,
        "business_rules": plan.get("business_rules"),
        "datasets": cleaned_datasets,
        "global_steps": plan.get("global_steps"),
        "manual_review": manual_review_enriched,
        "blocked": plan.get("blocked"),
        "source_metadata": source_metadata,
        "source_context": plan.get("source_context") or {},
        "connector_manifest": plan.get("connector_manifest") or {},
        "engine_recommendation": plan.get("engine_recommendation") or {},
        "relationships": plan.get("relationships") or {},
        "etl_intent": plan.get("etl_intent") or {},
        "dq_gate_summary": dq_gate_summary,
        "domain_rules": plan.get("domain_rules") or {},
        "business_keys": plan.get("business_keys") or {},
    }
    
    required_transforms = []
    if plan and plan.get("datasets"):
        for ds_name in plan["datasets"]:
            canon_ds = re.sub(r"[^0-9a-zA-Z_]+", "_", ds_name).strip("_")
            required_transforms.append({
                "dataset_id": ds_name,
                "function_name": f"transform_{canon_ds}"
            })
    base["required_transform_functions"] = required_transforms

    base.update(llm_codegen_extra_context(plan))
    return base


_READ_TEMPLATES: Dict[str, str] = {
    "csv_file": 'df = pd.read_csv(r"{loc}")',
    "excel": 'df = pd.read_excel(r"{loc}", sheet_name=0)',
    "json": 'df = pd.read_json(r"{loc}")',
    "parquet": 'df = pd.read_parquet(r"{loc}")',
    "sql_server": (
        'engine = create_engine("mssql+pyodbc://...")\n'
        'df = pd.read_sql("SELECT * FROM {loc}", engine)'
    ),
    "azure_sql": (
        'engine = create_engine("mssql+pyodbc://...database.windows.net/...")\n'
        'df = pd.read_sql("SELECT * FROM {loc}", engine)'
    ),
    "postgres": (
        'engine = create_engine("postgresql://...")\n'
        'df = pd.read_sql("SELECT * FROM {loc}", engine)'
    ),
    "blob_storage": (
        "# Read from Azure Blob — configure connection string\n"
        'df = pd.read_csv("downloaded_{loc}")'
    ),
    "unknown": 'df = pd.read_csv(r"{loc}")  # TODO: adjust read for your source',
}

_PYSPARK_READ_TEMPLATES: Dict[str, str] = {
    "csv_file": 'df = spark.read.option("header","true").csv(r"{loc}")',
    "parquet": 'df = spark.read.parquet(r"{loc}")',
    "json": 'df = spark.read.json(r"{loc}")',
    "sql_server": (
        'df = spark.read.format("jdbc").option("dbtable", "{loc}").load()'
    ),
    "unknown": 'df = spark.read.option("header","true").csv(r"{loc}")',
}


def _get_blob_read_template(conn: dict) -> str:
    account = conn.get("storage_account") or conn.get("account") or "{storage_account}"
    container = conn.get("container_name") or conn.get("container") or "{container_name}"
    return (
        f'df = spark.read.option("header","true")'
        f'.csv("wasbs://{container}@{account}.blob.core.windows.net/{{loc}}")'
    )


def _read_hint_for_payload(engine_key: str, payload: Dict[str, Any]) -> str:
    ctx = payload.get("source_context") or {}
    src_type = str(ctx.get("type") or "unknown")
    loc = str(ctx.get("location") or "data_file")

    # Connector validation gate: only enforce when the payload has been explicitly flagged
    # as requiring a resolved connection (i.e., connector_validation_required=True).
    # Standard blob sessions use env-var credentials via _read_blob_pandas / _resolve_data_path
    # and do NOT need resolved_connection in source_context.
    if (
        payload.get("connector_validation_required")
        and src_type in ("blob_storage", "azure_sql", "sql_server", "postgres")
        and not ctx.get("resolved_connection")
    ):
        raise ConnectorConfigError(
            f"Source type '{src_type}' requires resolved_connection details before codegen; "
            "connector validation must complete first."
        )

    if engine_key == "pyspark":
        if src_type == "blob_storage":
            conn = ctx.get("resolved_connection") or {}
            tmpl = _get_blob_read_template(conn)
        else:
            tmpl = _PYSPARK_READ_TEMPLATES.get(src_type, _PYSPARK_READ_TEMPLATES["unknown"])
    else:
        if src_type == "blob_storage":
            conn = ctx.get("resolved_connection") or {}
            from agent.etl_pipeline.io_snippets import _get_pandas_blob_read_snippet
            tmpl = _get_pandas_blob_read_snippet(loc, conn)
            return tmpl
        else:
            tmpl = _READ_TEMPLATES.get(src_type, _READ_TEMPLATES["unknown"])
    return tmpl.format(loc=loc)


def _trim_payload_for_window(payload: dict, engine_key: str, force_level: int = 0) -> dict:
    from agent.etl_pipeline.payload_trimmer import trim_payload, _FLOOR_TRIM_CONFIG
    return trim_payload(payload, _FLOOR_TRIM_CONFIG)


def _build_dynamic_sql_prompt(payload: dict, base_prompt: str) -> str:
    has_outliers = False
    has_dates = False
    
    datasets = payload.get("datasets") or {}
    for ds_name, block in datasets.items():
        if not isinstance(block, dict):
            continue
        steps = block.get("steps") or []
        for st in steps:
            act = str(st.get("action") or "").lower().strip()
            if act in ("flag_outliers", "clip_outliers", "cap_outliers", "clip_or_flag"):
                has_outliers = True
            if act == "parse_dates":
                has_dates = True
                
    br = payload.get("business_rules") or {}
    notes = str(br.get("notes") or "").lower()
    has_incremental = "incremental" in notes or "watermark" in notes
    
    rel = payload.get("relationships") or {}
    joins = rel.get("joins") or []
    has_joins = len(joins) > 0
    
    SECTION_GUARDS = {
        "Incremental Loading": has_incremental,
        "Outlier Mitigation Safety": has_outliers,
        "Reusable Outlier Procedure": has_outliers,
        "Multi-Format Date Parsing": has_dates,
        "Active curated views": has_joins,
        "Idempotent and Production-Safe views": has_joins,
    }
    
    lines = base_prompt.splitlines()
    result, skip = [], False
    for line in lines:
        heading = next((k for k in SECTION_GUARDS if k in line), None)
        if heading is not None:
            skip = not SECTION_GUARDS[heading]
        if not skip:
            result.append(line)
        if skip and line.strip() == "":  # blank line = end of section
            skip = False
            
    return "\n".join(result)


def _build_user_message_parts(
    engine_key: str,
    payload: Dict[str, Any],
    fix_errors: Optional[List[str]] = None,
    previous_output: Optional[str] = None,
) -> List[str]:
    br = payload.get("business_rules") or {}
    user_parts = [
        f"Target engine: {engine_key}",
        f"ETL policy (must follow):\n{payload.get('policy') or ''}",
        f"Generate complete ETL for this approved plan:\n{json.dumps(payload, separators=(',', ':'), default=str)}",
    ]
    
    if payload.get("required_transform_functions"):
        req_fns = payload["required_transform_functions"]
        req_lines = [f"- Define function '{item['function_name']}' to transform dataset '{item['dataset_id']}'" for item in req_fns]
        user_parts.append(
            "REQUIRED TRANSFORM FUNCTION REGISTRY (you MUST define exactly these transform function names):\n"
            + "\n".join(req_lines)
        )
    manifest = payload.get("connector_manifest") or {}
    m_ds = manifest.get("datasets") or {}
    if m_ds:
        read_lines = []
        for ds_name, ent in m_ds.items():
            if not isinstance(ent, dict):
                continue
            snip = ent.get("read_snippet_python") or ent.get("read_snippet_pyspark") or ""
            read_lines.append(f"- {ds_name}: {ent.get('source_type')} @ {ent.get('location')}")
            if snip:
                read_lines.append(f"  read: {snip}")
            if ent.get("output_path"):
                read_lines.append(f"  write: {ent.get('output_path')}")
            fmt = ent.get("format")
            if fmt == "xml":
                read_lines.append(
                    "  CRITICAL: format=xml — do NOT use read_csv or write.csv; use XML read + parquet/json write"
                )
        user_parts.append(
            "CONNECTOR MANIFEST (use these exact read/write patterns per dataset):\n"
            + "\n".join(read_lines[:40])
        )
        if engine_key == "pyspark":
            user_parts.append(
                "REQUIRED _resolve_data_path helper (copy verbatim into generated code):\n"
                f"```python\n{resolve_path_fabric_pyspark_helper(None, None)}\n```"
            )
            user_parts.append(
                "REQUIRED production helpers (copy ALL verbatim into generated code):\n"
                "```python\n"
                "def _require_columns(df, required, label):\n"
                "    existing_lower = [c.lower() for c in df.columns]\n"
                "    missing = [c for c in required if c.lower() not in existing_lower]\n"
                "    if missing:\n"
                "        raise ValueError(f'{label}: missing required columns: {missing}')\n\n"
                "def _warn_duplicate_keys(df, key_col, label):\n"
                "    if key_col not in df.columns:\n"
                "        return\n"
                "    row = df.agg(F.count(F.col(key_col)).alias('_n'), F.approx_count_distinct(F.col(key_col)).alias('_d')).first()\n"
                "    if row and row['_n'] and row['_d'] and int(row['_n']) > int(row['_d']):\n"
                "        logger.warning('%s: ~%d possible duplicate key value(s) on %s', label, int(row['_n']) - int(row['_d']), key_col)\n\n"
                "def _warn_nulls_in_columns(df, columns, label):\n"
                "    for col in columns or []:\n"
                "        if col in df.columns and df.filter(F.col(col).isNull()).limit(1).count() > 0:\n"
                "            logger.warning('%s: column %s has null values after transform', label, col)\n\n"
                "def _log_row_count(df, label: str) -> None:\n"
                "    logger.info('%s: row_count=%s', label, df.count())\n\n"
                "def _prefix_columns(df, prefix, except_cols):\n"
                "    for col in df.columns:\n"
                "        if col not in except_cols:\n"
                "            df = df.withColumnRenamed(col, f'{prefix}_{col}')\n"
                "    return df\n"
                "```\n"
            )
    gen_mode = str(payload.get("generation_mode") or "").strip().lower()
    pid = str(payload.get("plan_id") or "").strip().lower()
    if gen_mode == "cleanse_only" or pid.endswith("_cleanse"):
        payload["relationships"] = {"joins": [], "bridge_tables": []}
        user_parts.append(
            "CLEANSE ONLY MODE MANDATE:\n"
            "- This run is Phase 1 (Cleanse Only).\n"
            "- DO NOT emit any DataFrame joins (e.g. df1.join(df2)).\n"
            "- DO NOT emit any joined output tables (e.g. joined_sales_marketing).\n"
            "- DO NOT perform referential integrity checks across datasets.\n"
            "- ONLY clean each dataset independently and write <dataset>_clean tables."
        )
    elif payload.get("source_context"):
        ctx = payload["source_context"]
        sources = ctx.get("sources") or []
        if len(sources) > 1:
            src_lines = [
                f"- {s.get('dataset')}: {s.get('type')} @ {s.get('location')} ({s.get('row_count', 0):,} rows)"
                for s in sources[:15]
            ]
            user_parts.append(
                "MULTI-SOURCE CONTEXT (one loader per dataset):\n" + "\n".join(src_lines)
            )
        read_hint = _read_hint_for_payload(engine_key, payload)
        user_parts.append(
            f"PRIMARY READ PATTERN:\n```python\n{read_hint}\n```"
        )
    if br.get("notes"):
        user_parts.append(
            "BUSINESS NOTES (must honor in generated transforms):\n" + str(br.get("notes"))
        )
    manual = payload.get("manual_review") or []
    if manual:
        mr_lines = []
        PII_ACTIONS = {"hash_phone", "mask_phone", "hash_email", "exclude"}
        manual_sorted = sorted(
            manual,
            key=lambda x: 0 if str(x.get("action") or x.get("catalog_guidance") or "").lower().strip() in PII_ACTIONS else 1
        )
        for item in manual_sorted[:20]:
            ds = item.get("dataset") or "?"
            col = item.get("column") or "?"
            msg = item.get("message") or item.get("guidance") or ""
            cat_guidance = item.get("catalog_guidance") or ""
            if cat_guidance:
                msg = f"{msg} (Catalog guidance: {cat_guidance})"
            mr_lines.append(f"- [{ds}] {col}: {msg}")
        user_parts.append(
            "MANUAL REVIEW (implement in code when business notes require it, especially phone hash/mask):\n"
            + "\n".join(mr_lines)
        )
    gate = payload.get("dq_gate_summary") or {}
    blocking = gate.get("blocking_issues") or []
    if blocking:
        block_lines = [f"- [{b.get('dataset')}] {b.get('reason')}" for b in blocking[:10]]
        user_parts.append(
            "DQ GATE BLOCKING ISSUES (these columns/datasets must be cleaned — do NOT skip these steps):\n"
            + "\n".join(block_lines)
        )

    dr = payload.get("domain_rules") or {}
    if dr:
        dr_lines = []
        for col_name, rule in dr.items():
            dr_lines.append(f"- {col_name}: rule_name={rule.get('rule_name')}, regex={rule.get('regex')}, msg={rule.get('message')}")
        user_parts.append(
            "DOMAIN RULES (enforce these: domain rules take priority over default heuristic cleaning):\n"
            + "\n".join(dr_lines)
        )
        
    bk = payload.get("business_keys") or {}
    if bk:
        bk_lines = []
        for ds_name, keys in bk.items():
            bk_lines.append(f"- {ds_name}: keys={keys}")
        user_parts.append(
            "BUSINESS KEYS / DEDUPLICATION KEYS (use these columns in ROW_NUMBER PARTITION BY for deduplication):\n"
            + "\n".join(bk_lines)
        )

    user_parts.append(
        "ANSI-SAFE DATA CLEANING CONTRACT (Mandatory for Microsoft Fabric / Spark 3.4+):\n"
        "1. DATE PARSING: For action 'parse_dates' on date/timestamp columns, ALWAYS use helper '_parse_timestamp_safely(df, col)' or 'F.coalesce(F.try_to_timestamp(...), F.to_date(...))'. NEVER call bare F.to_date() directly on un-validated strings to prevent SparkDateTimeException in Microsoft Fabric.\n"
        "2. SENTINELS: For actions 'nullify_sentinels', 'replace_sentinel_values', or when columns contain sentinels ('???', 'unknown', 'N/A', 'TBD', '99/99/9999', '-999'), replace sentinel strings with NULL using 'F.when(F.trim(F.col(c)).isin(\"???\", \"unknown\", \"N/A\", \"TBD\", \"99/99/9999\"), F.lit(None)).otherwise(F.col(c))'.\n"
        "3. NUMERIC TYPE COERCION: For action 'cast_type' on numeric measures (e.g. budget, amount, qty, price), cast strictly to double/decimal. Non-numeric text strings ('???', 'unknown') MUST become NULL.\n"
        "4. BUSINESS KEY DUPLICATES: For action 'flag_business_key_duplicate' or 'flag_duplicate_candidates', use PySpark Window functions (Window.partitionBy('key')) to count group size and set boolean 'is_business_key_duplicate'.\n"
        "5. GOVERNANCE QUARANTINE: Automatically route all records flagged with invalid/null mandatory fields or duplicate keys into all_dfs['*_quarantine'] tables.\n"
    )

    if br.get("never_drop_rows"):
        user_parts.append(
            "NEVER_DROP_ROWS (mandatory): preserve every input row. "
            "No inner join, dropna(), or row-filtering that removes records. "
            "For normalization-only plans, transform and write each dataset separately — "
            "skip joins unless business_rules.notes explicitly require a join."
        )
    rel = payload.get("relationships") or {}
    joins = rel.get("joins") or []
    if joins:
        join_lines = []
        for j in joins[:8]:
            join_lines.append(
                f"- {j.get('left_dataset')}.{j.get('left_key')} "
                f"{j.get('join_type', 'inner')} join "
                f"{j.get('right_dataset')}.{j.get('right_key')} "
                f"({j.get('cardinality')}, overlap={j.get('overlap_count')})"
            )
        per_ds_only = all(
            str(st.get("action") or "")
            in (
                "lowercase",
                "uppercase",
                "trim",
                "sanitize_email",
                "normalize_phone",
                "hash_phone",
                "mask_phone",
            )
            for block in (payload.get("datasets") or {}).values()
            for st in (block or {}).get("steps") or []
        )
        if per_ds_only and br.get("never_drop_rows"):
            user_parts.append(
                "JOIN POLICY: Per-dataset normalization only — do NOT emit joins unless "
                "business_rules.notes explicitly require enrichment. Write each cleaned dataset."
            )
        else:
            user_parts.append(
                "DETECTED JOINS (after per-dataset transforms; write joined_* parquet):\n"
                + "\n".join(join_lines)
            )
        if rel.get("load_order"):
            user_parts.append(f"LOAD ORDER: {rel.get('load_order')}")
    if fix_errors:
        user_parts.append(
            "PREVIOUS ATTEMPT FAILED VALIDATION. Fix these specific issues:\n"
            + "\n".join(f"  - {e}" for e in fix_errors)
            + "\nDo NOT repeat these errors."
        )
        if previous_output:
            user_parts.append(f"Previous output (truncated):\n{previous_output}")
    return user_parts


def _call_llm(
    engine_key: str,
    payload: Dict[str, Any],
    *,
    fix_errors: Optional[List[str]] = None,
    previous_output: Optional[str] = None,
) -> str:
    client, model = _get_llm_client()
    if not client or not model:
        return f"{LLM_ERROR_PREFIX} No LLM credentials (configure AZURE_OPENAI_* or OPENAI_API_KEY)."

    # 1.5 dynamic system prompt injection
    system = SYSTEM_PROMPTS.get(engine_key, SYSTEM_PROMPTS["python"])
    if engine_key in ("sql-tsql", "sql-ansi"):
        system = _build_dynamic_sql_prompt(payload, system)

    user_parts = _build_user_message_parts(engine_key, payload, fix_errors, previous_output)
    user_message = "\n\n".join(user_parts)
    max_tokens = _safe_max_tokens(user_message, engine_key)

    # 1.1 Floor check: if max_tokens < 3000, force level 2 trim and rebuild message
    if max_tokens < 3000:
        logger.info(f"Available tokens too small ({max_tokens}). Forcing Level 2 payload trim...")
        payload = trim_payload(payload, _FLOOR_TRIM_CONFIG)
        user_parts = _build_user_message_parts(engine_key, payload, fix_errors, previous_output)
        user_message = "\n\n".join(user_parts)
        max_tokens = _safe_max_tokens(user_message, engine_key)

    # 1.3 OpenAI/Azure retry & timeout handling loop
    max_attempts = 3
    response = None
    last_err = None
    for attempt in range(max_attempts):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user_message},
                ],
                temperature=0,
                max_tokens=max_tokens,
                timeout=LLM_REQUEST_TIMEOUT or 120,
            )
            break
        except (RateLimitError, APITimeoutError) as e:
            last_err = e
            if attempt == max_attempts - 1:
                raise LLMInfraError(f"LLM request failed after retries: {e}") from e
            sleep_time = 3.0 if attempt > 0 else 1.0
            logger.warning(f"LLM rate limit or timeout on attempt {attempt + 1}, retrying in {sleep_time}s... Error: {e}")
            time.sleep(sleep_time)
        except Exception as e:
            raise LLMInfraError(f"LLM infrastructure error: {e}") from e

    if not response:
        raise LLMInfraError(f"LLM request failed entirely. Last error: {last_err}")

    choice = response.choices[0]
    raw_content = choice.message.content or ""
    text = _strip_markdown_fences(raw_content)
    logger.info(
        "[LLM Response Debug] Raw length: %d, Stripped length: %d, finish_reason: %s",
        len(raw_content), len(text), choice.finish_reason,
    )
    if choice.finish_reason == "length":
        raise LLMInfraError("LLM response truncated (finish_reason=length) — plan/payload too large for available tokens.")
    if choice.finish_reason == "content_filter":
        raise LLMInfraError("LLM response blocked by content filter (finish_reason=content_filter) — adjust the prompt or payload.")

    return raw_content, text


def parse_adf_json_from_llm(text: str) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """Parse ADF mapping JSON from LLM text; returns (object, errors)."""
    errs: List[str] = []
    raw = _strip_markdown_fences(text)
    if not raw:
        return None, ["empty ADF response"]
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj, []
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{[\s\S]*\}", raw)
    if match:
        try:
            obj = json.loads(match.group(0))
            if isinstance(obj, dict):
                return obj, []
        except json.JSONDecodeError as e:
            errs.append(f"JSON parse: {e}")
    else:
        errs.append("no JSON object found in LLM response")
    return None, errs or ["invalid ADF JSON"]


def _build_retry_context(code: str, errors: List[str]) -> str:
    lines = code.splitlines()
    error_lines = set()
    for e in errors:
        m = re.search(r"line (\d+)", e, re.IGNORECASE)
        if m:
            error_lines.add(int(m.group(1)))
            
    extra_hints = []
    if errors:
        err_str = " ".join(errors).lower()
        if "must cast to double first" in err_str:
            extra_hints.append(
                "CRITICAL FIX: Any percentile_approx, avg, mean, or median calculation must use "
                "F.col(column).cast('double') inside the aggregation to prevent Spark schema issues."
            )
        if "fillna() usage" in err_str or "fillna() accepts primitive values" in err_str:
            extra_hints.append(
                "CRITICAL FIX: NEVER pass F.lit() expressions or Python None directly to fillna(). "
                "fillna() only accepts primitive literals (like numbers or strings). If you must fill "
                "with dynamic values, use F.coalesce(F.col(column), F.lit(value).cast('double'))."
            )

    hints_prefix = ""
    if extra_hints:
        hints_prefix = "CRITICAL INSTRUCTIONS FOR RETRY:\n" + "\n".join(f"- {h}" for h in extra_hints) + "\n\n"
    
    if error_lines:
        excerpt_lines = []
        for ln in sorted(error_lines):
            start = max(0, ln - 10)
            end = min(len(lines), ln + 10)
            excerpt_lines.append(f"... (lines {start + 1}-{end}) ...")
            excerpt_lines.extend(lines[start:end])
        return hints_prefix + "\n".join(excerpt_lines[:150])
    else:
        error_msg = "; ".join(errors) if isinstance(errors, list) else str(errors)
        return hints_prefix + f"[FULL REWRITE REQUESTED]\nPrevious attempt failed with: {error_msg}\nGenerate a completely new implementation."


_RETRY_BUDGET: Dict[str, int] = {
    "python": 2,
    "sql-tsql": 3,
    "sql-ansi": 2,
    "pyspark": 2,
    "adf": 2,
}




def _get_plan_hash(plan: Dict[str, Any]) -> str:
    import hashlib
    core_plan = {
        "datasets": plan.get("datasets") or {},
        "business_rules": plan.get("business_rules") or {},
        "relationships": plan.get("relationships") or {},
        "global_steps": plan.get("global_steps") or [],
        "manual_review": plan.get("manual_review") or [],
    }
    try:
        plan_json = json.dumps(core_plan, sort_keys=True, default=str)
    except Exception:
        plan_json = str(core_plan)
    return hashlib.sha256(plan_json.encode("utf-8")).hexdigest()


def _get_cache_key(plan: Dict[str, Any], assessment: Dict[str, Any], engine_key: str) -> str:
    import hashlib
    plan_hash = _get_plan_hash(plan)
    MODEL_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT") or os.getenv("OPENAI_MODEL") or "default"
    cache_identity = {
        "plan_hash": plan_hash,
        "engine": engine_key,
        "prompt_version": PROMPT_VERSION,
        "validator_version": VALIDATOR_VERSION,
        "postprocessor_version": POSTPROCESSOR_VERSION,
        "model_deployment": MODEL_DEPLOYMENT,
        "tenant_id": plan.get("tenant_id") or "default",
    }
    return hashlib.sha256(
        json.dumps(cache_identity, sort_keys=True).encode("utf-8")
    ).hexdigest()


async def _reset_circuit_breaker(plan_id: str, delay: int = 300):
    await asyncio.sleep(delay)
    _PLAN_FAILURE_COUNTS[plan_id] = 0
    logger.info(f"Circuit breaker reset for plan_id: {plan_id}")


async def generate_etl_with_llm(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    engine: str = "python",
    *,
    sql_dialect: str = "tsql",
    output_mode: str = "dataframe_only",
    output_path: Optional[str] = None,
    validation_errors: Optional[List[str]] = None,
    validate_fn: Optional[Callable[[str], Tuple[bool, List[str]]]] = None,
    strict_mode: bool = False,
) -> Tuple[Optional[str], Optional[str]]:
    engine_key = normalize_codegen_engine(engine, sql_dialect)
    cache_key = _get_cache_key(plan, assessment, engine_key)
    plan_hash = _get_plan_hash(plan)
    
    # Invalidate/flush only this plan hash for debugging
    for k in list(_CODE_CACHE.keys()):
        if plan_hash in k:
            _CODE_CACHE.pop(k, None)
    for k in list(_FAILURE_CACHE.keys()):
        if plan_hash in k:
            _FAILURE_CACHE.pop(k, None)

    MODEL_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT") or os.getenv("OPENAI_MODEL") or "default"
    tenant_id = plan.get("tenant_id") or "default"
    breaker_key = f"{tenant_id}_{plan_hash}_{engine_key}_{MODEL_DEPLOYMENT}"
    hit = cache_key in _CODE_CACHE
    logger.info(
        f"[Cache check] key: {cache_key}, hit: {hit}, plan_hash: {plan_hash}, "
        f"breaker_key: {breaker_key}, prompt_ver: {PROMPT_VERSION}"
    )

    # Check failure cache first:
    if cached_err := _is_failure_cached(cache_key):
        return None, f"[Cached failure] {cached_err}"

    if not validation_errors:
        now = time.time()
        if cache_key in _CODE_CACHE:
            _entry = _CODE_CACHE[cache_key]
            if isinstance(_entry, dict):
                cached_code = _entry["code"]
                cached_time = _entry["time"]
                cached_ok = _entry.get("ok", True)
            else:
                cached_code, cached_time = _entry
                cached_ok = True
            if cached_ok and isinstance(cached_code, str) and cached_code.strip().startswith("# VALIDATION WARNING:"):
                cached_ok = False
            if now - cached_time < 3600:
                if cached_ok:
                    logger.info("Returning cached generated code for key: %s", cache_key)
                    return cached_code, None
                else:
                    logger.info("Skipping failed cached code for key: %s — will regenerate", cache_key)
                    _CODE_CACHE.pop(cache_key, None)

    plan_id = plan.get("plan_id", "unknown")
    lock = _PLAN_LOCKS[plan_id]
    await asyncio.to_thread(lock.acquire)
    try:
        if _PLAN_FAILURE_COUNTS[plan_id] >= _MAX_PLAN_FAILURES:
            return None, "Circuit open: too many failures for this plan"

        # Execute blocking LLM call in a thread pool
        code, err = await asyncio.to_thread(
            _generate_etl_with_llm_impl,
            plan,
            assessment,
            engine,
            sql_dialect=sql_dialect,
            output_mode=output_mode,
            output_path=output_path,
            validation_errors=validation_errors,
            validate_fn=validate_fn,
            cache_key=cache_key,
            strict_mode=strict_mode,
        )

        if code is None:
            _PLAN_FAILURE_COUNTS[plan_id] += 1
            if _PLAN_FAILURE_COUNTS[plan_id] >= _MAX_PLAN_FAILURES:
                def reset_circuit():
                    _PLAN_FAILURE_COUNTS[plan_id] = 0
                    logger.info(f"Circuit breaker reset for plan_id: {plan_id}")
                threading.Timer(30.0, reset_circuit).start()
        else:
            _PLAN_FAILURE_COUNTS[plan_id] = 0  # reset on success
            
        return code, err
    finally:
        lock.release()


def _generate_etl_with_llm_impl(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    engine: str = "python",
    *,
    sql_dialect: str = "tsql",
    output_mode: str = "dataframe_only",
    output_path: Optional[str] = None,
    validation_errors: Optional[List[str]] = None,
    validate_fn: Optional[Callable[[str], Tuple[bool, List[str]]]] = None,
    cache_key: Optional[str] = None,
    strict_mode: bool = False,
) -> Tuple[Optional[str], Optional[str]]:
    engine_key = normalize_codegen_engine(engine, sql_dialect)
    if engine_key == "adf":
        return None, "Use generate_adf_with_llm for ADF engine."

    if not cache_key:
        cache_key = _get_cache_key(plan, assessment, engine_key)

    payload = _build_codegen_payload(
        plan, assessment, output_mode=output_mode, output_path=output_path, strict_mode=strict_mode
    )

    prev: Optional[str] = None
    fix_errors = list(validation_errors or [])
    
    from agent.etl_pipeline.codegen_validator import get_validator
    validator = get_validator(engine_key)
    
    max_retries = _RETRY_BUDGET.get(engine_key, 2)
    
    code = ""
    for attempt in range(max_retries + 1):
        import copy
        if attempt == 0:
            trimmed_payload = trim_payload(payload, _CODEGEN_TRIM_CONFIG)
        elif attempt == 1:
            logger.info("Attempt 2: applying aggressive payload trim")
            p_copy = copy.deepcopy(payload)
            p_copy.pop("source_metadata", None)
            p_copy.pop("domain_rules", None)
            if "manual_review" in p_copy:
                p_copy["manual_review"] = p_copy["manual_review"][:5]
            aggressive_config = TrimConfig(mode="codegen", token_budget=30_000, field_priority=["manual_review"])
            trimmed_payload = trim_payload(p_copy, aggressive_config)
        else:
            logger.info("Attempt 3: applying minimal payload trim")
            p_copy = copy.deepcopy(payload)
            keys_to_keep = ("datasets", "business_rules", "output_mode", "output_path", "plan_id", "connector_manifest")
            minimal_payload = {k: p_copy[k] for k in keys_to_keep if k in p_copy}
            minimal_payload["additional_instruction"] = (
                "CRITICAL: Generate only the core transform steps and transformations. "
                "Do not include any I/O helper functions (like _resolve_data_path or read_blob_pandas) "
                "or dataset loading/writing boilerplate, as they will be injected deterministically post-generation."
            )
            trimmed_payload = minimal_payload

        if fix_errors and attempt > 0:
            prev = _build_retry_context(code, fix_errors)
        elif validation_errors:
            prev = "(retry — see validation errors in user message)"
        else:
            prev = None

        try:
            raw_code, stripped_code = _call_llm(
                engine_key,
                trimmed_payload,
                fix_errors=fix_errors or None,
                previous_output=prev,
            )
        except (LLMInfraError, ConnectorConfigError) as exc:
            last_error = str(exc)
            _write_failure_cache(cache_key, last_error)
            if isinstance(exc, ConnectorConfigError):
                return None, f"Connector configuration error: {exc}. Check your source connection settings."
            if "truncated" in last_error.lower() and attempt < max_retries:
                logger.info(f"LLM output truncated. Retrying with harder payload trim...")
                trimmed_payload = trim_payload(trimmed_payload, _FLOOR_TRIM_CONFIG)
                fix_errors = []
                continue
            if attempt < max_retries:
                continue
            return None, f"LLM infrastructure error: {last_error}"

        if is_llm_generation_error(stripped_code):
            _write_failure_cache(cache_key, stripped_code)
            return None, stripped_code

        # Guard: LLM returned empty/whitespace response — treat as payload-too-large and retry
        if not stripped_code or not stripped_code.strip():
            logger.warning(
                "[Codegen Debug] LLM returned empty response on attempt %d — retrying with trimmer payload",
                attempt + 1,
            )
            if attempt < max_retries:
                fix_errors = []
                continue
            return None, "LLM returned empty response after all retries — payload may be too large"

        generation_id = plan.get("plan_id") or "run"
        attempt_idx = attempt + 1
        
        # Log raw and stripped stages
        log_stage_diagnostics("raw", raw_code)
        save_stage_debug_artifact(generation_id, attempt_idx, "raw", raw_code)
        
        log_stage_diagnostics("stripped", stripped_code)
        save_stage_debug_artifact(generation_id, attempt_idx, "stripped", stripped_code)

        try:
            if engine_key == "pyspark":
                # Stage 3: Fixed code (basic syntax normalization + safe mechanical repairs)
                repair_res = apply_mechanical_codegen_repairs(stripped_code, plan)
                fixed_code = repair_res.code
                
                log_stage_diagnostics("fixed", fixed_code)
                save_stage_debug_artifact(generation_id, attempt_idx, "fixed", fixed_code)
                
                # Invariant: fixed code must compile and contain no forbidden imports
                try:
                    ast.parse(fixed_code)
                except SyntaxError as e:
                    raise ValueError(f"Fixed code syntax error: {e}")
                raw_forbidden = set(find_forbidden_imports(stripped_code))
                fixed_forbidden = set(find_forbidden_imports(fixed_code))
                introduced = fixed_forbidden - raw_forbidden
                if introduced:
                    raise ValueError(f"Forbidden imports {sorted(list(introduced))} introduced in fixed stage")
                    
                # Stage 4: Orchestration (after checking defined transforms registry)
                defined = set(extract_defined_functions(fixed_code))
                called_expected = set()
                if plan and plan.get("datasets"):
                    for ds_name in plan["datasets"]:
                        canon_ds = re.sub(r"[^0-9a-zA-Z_]+", "_", ds_name).strip("_")
                        called_expected.add(f"transform_{canon_ds}")
                
                # Auto-alias dataset transform functions if LLM omitted file extension suffix
                for missing_fn in list(called_expected - defined):
                    base_fn = re.sub(r"_(json|csv|xml|parquet|tsv|xlsx|xls|txt)$", "", missing_fn)
                    if base_fn in defined:
                        fixed_code += f"\n\n{missing_fn} = {base_fn}\n"
                        defined.add(missing_fn)

                missing = sorted(list(called_expected - defined))
                if missing:
                    raise GeneratedCodeIncompleteError(
                        code="MISSING_TRANSFORM_FUNCTIONS",
                        missing_functions=missing,
                    )
                    
                orchestrated_code = _inject_pyspark_run_pipeline(fixed_code, plan)
                orchestrated_code = _fix_bare_run_pipeline_call(orchestrated_code)
                
                log_stage_diagnostics("orchestrated", orchestrated_code)
                save_stage_debug_artifact(generation_id, attempt_idx, "orchestrated", orchestrated_code)
                
                # Invariant: orchestrated code
                orchestrated_defined = set(extract_defined_functions(orchestrated_code))
                orchestrated_called = set(extract_called_transform_functions(orchestrated_code))
                orchestrated_missing = orchestrated_called - orchestrated_defined
                
                # Auto-alias dataset transform functions in orchestrated code if extension suffix missing
                for missing_fn in list(orchestrated_missing):
                    base_fn = re.sub(r"_(json|csv|xml|parquet|tsv|xlsx|xls|txt)$", "", missing_fn)
                    if base_fn in orchestrated_defined:
                        orchestrated_code += f"\n\n{missing_fn} = {base_fn}\n"
                        orchestrated_defined.add(missing_fn)

                orchestrated_missing = orchestrated_called - orchestrated_defined
                if orchestrated_missing:
                    raise ValueError(f"Missing called transform functions in orchestrated code: {orchestrated_missing}")
                if orchestrated_code.count("def run_pipeline") > 1:
                    raise ValueError("Duplicate run_pipeline definitions found")
                    
                # Stage 5: Final code (orchestrated call repair + helper deduplication)
                final_code = _fix_mismatched_transform_calls(orchestrated_code)
                final_code = _remove_row_count_logging_from_transforms(final_code)
                final_code = _deduplicate_helper_functions(final_code)
                final_code = _ensure_non_empty_functions(final_code)
                final_code = _sanitize_html_entities_in_code(final_code)
                
                log_stage_diagnostics("final", final_code)
                save_stage_debug_artifact(generation_id, attempt_idx, "final", final_code)
                
                # Final Invariant
                try:
                    ast.parse(final_code)
                except SyntaxError as e:
                    raise ValueError(f"Final code syntax error: {e}")
                    
                if plan:
                    expected_steps = []
                    for ds_name, block in plan.get("datasets", {}).items():
                        for st in (block.get("steps") or []):
                            st_copy = dict(st)
                            st_copy["dataset"] = ds_name
                            expected_steps.append(st_copy)
                    from agent.etl_pipeline.validate_pyspark import inspect_plan_step_conformance
                    report = inspect_plan_step_conformance(final_code, expected_steps, plan)
                    if report.errors:
                        raise ValueError(f"Plan conformance errors in final code: {report.errors}")
                        
                code = final_code
            else:
                code = _sanitize_unquoted_nan_in_code(stripped_code)
        except (ValueError, GeneratedCodeIncompleteError, MechanicalRepairViolation) as e:
            logger.warning(f"Error during codegen stages processing: {e}")
            fix_errors = [str(e)]
            save_stage_debug_artifact(generation_id, attempt_idx, "validation", json.dumps({"ok": False, "errors": fix_errors}))
            if attempt < max_retries:
                continue
            else:
                return None, f"Validation errors on final attempt: {fix_errors}"

        # Construct immutable artifact and assert hashes match
        artifact = GeneratedCodeArtifact.create(
            source=code,
            plan_hash=_get_plan_hash(plan),
            attempt=attempt_idx,
            generation_id=generation_id,
        )
        import hashlib
        assert hashlib.sha256(artifact.source.encode("utf-8")).hexdigest() == artifact.source_sha256
        logger.info(f"final_code_hash before validation: {artifact.source_sha256}")

        # Run local validation loop
        rules = plan.get("business_rules") or {}
        never_drop_rows = bool(rules.get("never_drop_rows"))

        active_validator = validate_fn if validate_fn is not None else validator
        if active_validator:
            logger.info("Running validator on attempt %d code...", attempt + 1)
            import inspect
            fn_key = f"fn_{id(active_validator)}" if validate_fn is not None else engine_key
            accepts_ndr = _VALIDATOR_ACCEPTS_NDR.get(fn_key)
            if accepts_ndr is None:
                sig = inspect.signature(active_validator)
                accepts_ndr = "never_drop_rows" in sig.parameters
                _VALIDATOR_ACCEPTS_NDR[fn_key] = accepts_ndr

            if accepts_ndr:
                res = active_validator(code, never_drop_rows=never_drop_rows)
            else:
                res = active_validator(code)
            
            if len(res) == 3:
                ok, errs, warnings = res
                if warnings:
                    logger.warning(f"[Validation Advisory] {warnings}")
            else:
                ok, errs = res

            save_stage_debug_artifact(generation_id, attempt_idx, "validation", json.dumps({"ok": ok, "errors": errs}))

            if ok:
                _code_cache_put(cache_key, {"code": code, "time": time.time(), "ok": True})
                return code, None
            if attempt < max_retries:
                logger.info(f"[Retry Debug] Attempt {attempt + 1} code failed validation with errors: {errs}.")
                fix_errors = errs
                continue
            # After max retries, return with validation warnings prepended
            warning = "\n".join(f"# VALIDATION WARNING: {e}" for e in errs)
            res_code = f"{warning}\n\n{code}"
            _code_cache_put(cache_key, {"code": res_code, "time": time.time(), "ok": False})
            _write_failure_cache(cache_key, f"Validation errors: {errs}")
            return res_code, f"Validation errors: {errs}"
        else:
            logger.warning(f"No validator or validate_fn found for engine '{engine_key}' — skipping validation")
            _code_cache_put(cache_key, {"code": code, "time": time.time(), "ok": True})
            return code, None

    return code, None

    return code, None


async def generate_adf_with_llm(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    *,
    validation_errors: Optional[List[str]] = None,
    validate_fn: Optional[Callable[[Dict[str, Any]], Tuple[bool, List[str]]]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Returns (adf_object, error_message).
    error_message empty on success; on LLM failure error_message is set and first element may be None.
    """
    payload = _build_codegen_payload(plan, assessment)
    trimmed = trim_payload(payload, _CODEGEN_TRIM_CONFIG)
    last_error = None
    fix_errors = list(validation_errors or [])
    raw = ""
    
    MAX_ADF_RETRIES = 3
    for attempt in range(MAX_ADF_RETRIES):
        prev_out = raw if attempt > 0 else None
        
        try:
            _, raw = await asyncio.to_thread(
                _call_llm,
                "adf",
                trimmed,
                fix_errors=fix_errors or None,
                previous_output=prev_out,
            )
        except Exception as e:
            last_error = str(e)
            import copy
            trimmed = copy.deepcopy(trimmed)
            trimmed["_adf_fix_context"] = {
                "attempt": attempt + 1,
                "parse_error": last_error,
                "prev_output_snippet": ""
            }
            continue

        if is_llm_generation_error(raw):
            last_error = raw
            import copy
            trimmed = copy.deepcopy(trimmed)
            trimmed["_adf_fix_context"] = {
                "attempt": attempt + 1,
                "parse_error": raw,
                "prev_output_snippet": ""
            }
            continue
            
        parsed, errs = parse_adf_json_from_llm(raw)
        if parsed:
            if validate_fn:
                ok, val_errs = validate_fn(parsed)
                if ok:
                    return parsed, None
                else:
                    last_error = "; ".join(val_errs)
                    fix_errors = val_errs
            else:
                return parsed, None
        else:
            last_error = "; ".join(errs)
            fix_errors = errs

        # On failure, add error context for next attempt
        import copy
        trimmed = copy.deepcopy(trimmed)
        trimmed["_adf_fix_context"] = {
            "attempt": attempt + 1,
            "parse_error": last_error,
            "prev_output_snippet": (raw or "")[:2000]
        }

    return None, f"ADF JSON generation failed after {MAX_ADF_RETRIES} attempts: {last_error}"


def run_codegen_sync(coro):
    """Bridge async code into a synchronous calling environment."""
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(lambda: asyncio.run(coro))
            return future.result()
    else:
        new_loop = asyncio.new_event_loop()
        try:
            return new_loop.run_until_complete(coro)
        finally:
            new_loop.close()

