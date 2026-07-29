"""
fabric_lakehouse_connector.py - Fabric OneLake Delta Lake Connector

Writes pandas DataFrames directly to Microsoft Fabric Lakehouse (OneLake)
as Delta Tables using the `deltalake` library (delta-rs).
"""

import os
import re
import logging
from typing import Dict, Any, Optional
import pandas as pd

logger = logging.getLogger("connectors.fabric_lakehouse_connector")

def _clean_env_value(v: Optional[str]) -> Optional[str]:
    """
    Be resilient to env var quoting/pasting issues:
    - trim whitespace/newlines
    - remove surrounding quotes
    """
    if v is None:
        return None
    s = str(v).strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        s = s[1:-1].strip()
    return s

def _is_uuid(val: str) -> bool:
    """Check if the string is a valid UUID."""
    val_clean = val.lower().strip()
    return bool(re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", val_clean))

def get_lakehouse_folder(lakehouse: str) -> str:
    """
    Determine the physical directory name of the Lakehouse.
    Names (like 'Dhara_Lake') are postfixed with '.Lakehouse' in OneLake.
    UUIDs are used as-is.
    """
    lh = lakehouse.strip()
    if not lh.lower().endswith(".lakehouse") and not _is_uuid(lh):
        return f"{lh}.Lakehouse"
    return lh

def is_fabric_mirror_enabled() -> bool:
    """Check if Fabric mirroring is explicitly enabled in env."""
    enabled_val = _clean_env_value(os.getenv("DHARA_FABRIC_MIRROR_ENABLED"))
    return enabled_val in ("1", "true", "TRUE", "yes", "YES")

def get_fabric_storage_options() -> Dict[str, str]:
    """
    Build the storage options dictionary for delta-rs.
    If FABRIC_SERVICE_PRINCIPAL_ID (or FABRIC_CLIENT_ID) is set, use service principal only.
    Otherwise, fall back to DefaultAzureCredential. Do not mix them.
    """
    tenant_id = _clean_env_value(os.getenv("FABRIC_TENANT_ID"))
    client_id = _clean_env_value(os.getenv("FABRIC_SERVICE_PRINCIPAL_ID") or os.getenv("FABRIC_CLIENT_ID"))
    client_secret = _clean_env_value(os.getenv("FABRIC_CLIENT_SECRET"))

    options = {
        "use_fabric_endpoint": "true"
    }

    if client_id and client_secret:
        logger.info("Using Service Principal authentication for Fabric OneLake.")
        options["client_id"] = client_id
        options["client_secret"] = client_secret
        if tenant_id:
            options["tenant_id"] = tenant_id
        return options

    # Default authentication path
    logger.info("Executing token-based authentication via DefaultAzureCredential...")
    try:
        from azure.identity import DefaultAzureCredential
        if tenant_id:
            os.environ["AZURE_TENANT_ID"] = tenant_id
            cred = DefaultAzureCredential()
            token_response = cred.get_token("https://storage.azure.com/.default", tenant_id=tenant_id)
        else:
            cred = DefaultAzureCredential()
            token_response = cred.get_token("https://storage.azure.com/.default")
        options["bearer_token"] = token_response.token
        logger.info("Successfully acquired storage access token via DefaultAzureCredential.")
    except Exception as e:
        logger.warning(f"Could not acquire token via DefaultAzureCredential: {e}. "
                       f"Please ensure you have configured Azure CLI and executed 'az login' in your shell.")

    return options


def _write_chunk_with_retry(
    target_uri: str, 
    chunk: pd.DataFrame, 
    mode: str, 
    storage_options: dict, 
    schema_mode: Optional[str] = None,
    max_attempts: int = 3
):
    import time
    from deltalake import write_deltalake
    backoff = 2.0
    for attempt in range(max_attempts):
        try:
            write_deltalake(
                target_uri,
                chunk,
                mode=mode,
                storage_options=storage_options,
                schema_mode=schema_mode
            )
            return
        except Exception as e:
            if attempt == max_attempts - 1:
                raise e
            logger.warning(f"Fabric OneLake write attempt {attempt + 1} failed: {e}. Retrying in {backoff}s...")
            time.sleep(backoff)
            backoff *= 2.0


def write_to_lakehouse(df: pd.DataFrame, table_name: str, mode: str = "append", schema_mode: Optional[str] = None) -> Dict[str, Any]:
    """
    Writes a pandas DataFrame to Fabric OneLake as a Delta table.
    
    Args:
        df: The pandas DataFrame to write.
        table_name: The target table name (becomes folder in Tables/ zone).
        mode: Write mode ('overwrite' or 'append').
        schema_mode: Delta schema evolution mode ('overwrite', 'merge', or None).
        
    Returns:
        A dictionary summarizing the result of the write operation.
    """
    if df.empty:
        logger.warning(f"DataFrame is empty, skipping Fabric mirror for table {table_name}")
        return {"ok": True, "status": "skipped", "message": "DataFrame is empty"}

    workspace = _clean_env_value(os.getenv("FABRIC_WORKSPACE_ID") or os.getenv("FABRIC_WORKSPACE_NAME"))
    lakehouse = _clean_env_value(os.getenv("FABRIC_LAKEHOUSE_NAME") or os.getenv("FABRIC_LAKEHOUSE_ID"))

    if not workspace or not lakehouse:
        err_msg = "Fabric Workspace ID/Name or Lakehouse Name/ID is not configured in environment."
        logger.error(err_msg)
        return {"ok": False, "error": "MISSING_CONFIG", "message": err_msg}

    # Format table name to be safe for folders (alphanumeric and underscores)
    safe_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name.split(".")[-1])
    lakehouse_folder = get_lakehouse_folder(lakehouse)

    # Build the ABFSS target URI
    target_uri = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse_folder}/Tables/{safe_table_name}"
    
    logger.info(f"Preparing to write to Fabric Lakehouse: {target_uri}")

    max_rows = int(os.getenv("DHARA_FABRIC_MAX_ROWS_PER_WRITE") or 100000)
    chunk_size = int(os.getenv("DHARA_FABRIC_WRITE_CHUNK_SIZE") or 50000)
    
    if len(df) > max_rows:
        logger.warning(f"DataFrame size ({len(df)} rows) exceeds maximum row threshold ({max_rows}). Chunking writes...")

    try:
        from deltalake import write_deltalake, DeltaTable
    except ImportError as ie:
        err_msg = "The 'deltalake' library is not installed. Run 'pip install deltalake'."
        logger.error(err_msg, exc_info=ie)
        return {"ok": False, "error": "LIBRARY_NOT_FOUND", "message": err_msg}

    storage_options = get_fabric_storage_options()

    # Meta check & strict schema validation
    table_exists = False
    existing_schema = None
    try:
        dt = DeltaTable(target_uri, storage_options=storage_options)
        table_exists = True
        existing_schema = dt.schema()
    except Exception:
        pass

    try:
        if table_exists and existing_schema:
            existing_cols = {f.name for f in existing_schema.fields}
            new_cols = set(df.columns)
            
            if mode == "append":
                if new_cols != existing_cols:
                    logger.warning(f"Schema mismatch on append write to {table_name}. Existing columns: {existing_cols}, New columns: {new_cols}.")
                    if schema_mode not in ("merge", "overwrite"):
                        raise ValueError(f"Schema mismatch on append to table '{table_name}' without schema evolution mode enabled.")
            
            elif mode == "overwrite":
                # Enforce non-nullable constraint checks
                for field in existing_schema.fields:
                    if not field.nullable and field.name in df.columns:
                        if df[field.name].isnull().any():
                            raise ValueError(f"Constraint violation: Column '{field.name}' is non-nullable in target schema, but overwrite data contains null values.")
    except Exception as e:
        logger.error(f"Gating validation failed for {safe_table_name}: {e}")
        return {
            "ok": False,
            "error": "SCHEMA_VALIDATION_FAILED",
            "message": str(e),
            "table": safe_table_name,
            "uri": target_uri
        }

    actual_schema_mode = schema_mode or ("overwrite" if mode == "overwrite" else "merge")
    try:
        if len(df) <= chunk_size:
            _write_chunk_with_retry(
                target_uri,
                df,
                mode=mode,
                storage_options=storage_options,
                schema_mode=actual_schema_mode
            )
        else:
            first = True
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i : i + chunk_size]
                chunk_mode = mode if first else "append"
                chunk_schema_mode = actual_schema_mode if chunk_mode == mode else "merge"
                _write_chunk_with_retry(
                    target_uri,
                    chunk,
                    mode=chunk_mode,
                    storage_options=storage_options,
                    schema_mode=chunk_schema_mode
                )
                first = False

        logger.info(f"Successfully mirrored table {safe_table_name} to Fabric Lakehouse in {mode} mode.")
        return {
            "ok": True,
            "status": "success",
            "table": safe_table_name,
            "uri": target_uri,
            "mode": mode,
            "rows": len(df)
        }
    except Exception as e:
        logger.exception(f"Failed to write Delta table {safe_table_name} to Fabric OneLake: {e}")
        return {
            "ok": False,
            "error": "WRITE_FAILED",
            "message": str(e),
            "table": safe_table_name,
            "uri": target_uri
        }
