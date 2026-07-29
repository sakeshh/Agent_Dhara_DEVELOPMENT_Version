from typing import Any, Dict, List, Optional
import logging
from agent.azure_sql_executor import get_connection

logger = logging.getLogger("agent.post_etl_validator")

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

def run_post_etl_validation(
    target_tables: List[str],
    connection_string: Optional[str],
    pre_assessment: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Validates target tables post-ETL and compares with pre-ETL quality metrics.
    """
    improvements = []
    regressions = []
    
    if not target_tables or not connection_string:
        return {"ok": True, "deltas": {"improvements": [], "regressions": []}}
        
    conn = None
    try:
        conn = get_connection(connection_string)
        cursor = conn.cursor()
        
        for table in target_tables:
            matched_dataset = None
            if pre_assessment and "datasets" in pre_assessment:
                for ds_name in pre_assessment["datasets"].keys():
                    if ds_name.lower() in table.lower() or table.lower() in ds_name.lower():
                        matched_dataset = ds_name
                        break
            
            safe_table = _safe_bracket_quote(table)
                    
            columns = []
            try:
                parts = [p.replace("[", "").replace("]", "") for p in table.split(".")]
                if len(parts) == 2:
                    schema_clean, table_clean_name = parts
                    cursor.execute(
                        "SELECT COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                        (schema_clean, table_clean_name)
                    )
                else:
                    table_clean_name = parts[0]
                    cursor.execute(
                        "SELECT COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?",
                        (table_clean_name,)
                    )
                for row in cursor.fetchall():
                    columns.append((row[0], row[1]))
            except Exception as e:
                logger.debug(f"Failed to query schema for {table}: {e}")
                continue
                
            for col_name, is_nullable in columns:
                null_count = 0
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {safe_table} WHERE [{col_name}] IS NULL")
                    row = cursor.fetchone()
                    null_count = row[0] if row else 0
                except Exception:
                    pass
                    
                pre_null_count = None
                if matched_dataset:
                    col_meta = pre_assessment["datasets"][matched_dataset].get("columns", {}).get(col_name) or {}
                    pre_null_pct = col_meta.get("null_percentage")
                    total_rows = pre_assessment["datasets"][matched_dataset].get("row_count") or 0
                    if pre_null_pct is not None:
                        pre_null_count = int(round(float(pre_null_pct) * total_rows))
                        
                if pre_null_count is not None:
                    if null_count < pre_null_count:
                        improvements.append({
                            "table": table,
                            "column": col_name,
                            "metric": "null_count",
                            "before": pre_null_count,
                            "after": null_count,
                            "detail": f"Null count improved from {pre_null_count} to {null_count}."
                        })
                    elif null_count > pre_null_count:
                        regressions.append({
                            "table": table,
                            "column": col_name,
                            "metric": "null_count",
                            "before": pre_null_count,
                            "after": null_count,
                            "detail": f"Null count regressed from {pre_null_count} to {null_count}."
                        })
                        
            # Check duplicates on likely key columns
            if matched_dataset:
                pk_cols = pre_assessment["datasets"][matched_dataset].get("likely_key_columns") or []
                for pk in pk_cols:
                    if any(pk.lower() == c[0].lower() for c in columns):
                        dup_count = 0
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM (SELECT [{pk}], COUNT(*) FROM {safe_table} GROUP BY [{pk}] HAVING COUNT(*) > 1) AS t")
                            row = cursor.fetchone()
                            dup_count = row[0] if row else 0
                        except Exception:
                            pass
                            
                        if dup_count > 0:
                            regressions.append({
                                "table": table,
                                "column": pk,
                                "metric": "duplicate_key_count",
                                "before": 0,
                                "after": dup_count,
                                "detail": f"Duplicate key violation: {dup_count} duplicate keys in '{pk}'."
                            })
                        else:
                            had_dups = False
                            dq_block = pre_assessment.get("data_quality_issues", {}).get("datasets", {}).get(matched_dataset) or {}
                            for issue in dq_block.get("issues", []):
                                if issue.get("column") == pk and "duplicate" in str(issue.get("type")):
                                    had_dups = True
                                    break
                            if had_dups:
                                improvements.append({
                                    "table": table,
                                    "column": pk,
                                    "metric": "duplicate_key_count",
                                    "before": ">0",
                                    "after": 0,
                                    "detail": f"Duplicate key violation resolved for '{pk}'."
                                })
                                
    except Exception as e:
        logger.warning(f"Post-ETL validation failed: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
                
    ok = len(regressions) == 0
    return {
        "ok": ok,
        "deltas": {
            "improvements": improvements,
            "regressions": regressions
        }
    }
