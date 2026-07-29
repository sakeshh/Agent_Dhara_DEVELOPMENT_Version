"""
fabric_shortcut_service.py - Manages landing raw data in Fabric OneLake Files/ zone using physical file uploads (copy).

Connects to Azure Blob Storage to download files, and uploads them directly to Microsoft Fabric
OneLake via ADLS Gen2 APIs, registering the local metadata in the SQLite database.
"""

from __future__ import annotations

import os
import logging
from typing import Dict, Any, List, Optional
import requests
from urllib.parse import quote

from agent.fabric_api_client import FabricAPIClient
from agent.blob_fabric_registry import register_shortcut, make_safe_shortcut_name
from connectors.azure_blob_storage import AzureBlobStorageConnector
from connectors.fabric_lakehouse_connector import get_lakehouse_folder, get_fabric_storage_options

logger = logging.getLogger("agent.fabric_shortcut_service")

def _clean_env_value(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        s = s[1:-1].strip()
    return s or None

def _get_storage_token() -> Optional[str]:
    """Acquires a bearer token for https://storage.azure.com/.default scope."""
    opts = get_fabric_storage_options()
    if "bearer_token" in opts:
        return opts["bearer_token"]
        
    # If using Service Principal credentials
    client_id = opts.get("client_id")
    client_secret = opts.get("client_secret")
    tenant_id = opts.get("tenant_id") or os.getenv("FABRIC_TENANT_ID")
    
    if client_id and client_secret and tenant_id:
        logger.info("Acquiring Storage token via Service Principal credentials.")
        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://storage.azure.com/.default"
        }
        try:
            res = requests.post(url, data=data, timeout=15)
            res.raise_for_status()
            return res.json().get("access_token")
        except Exception as e:
            logger.error(f"Failed to acquire storage token via Service Principal: {e}")
            
    return None

def upload_file_to_onelake(
    workspace: str,
    lakehouse_folder: str,
    dest_path: str,
    file_bytes: bytes,
    token: str
) -> bool:
    """Uploads file bytes directly to Fabric OneLake using ADLS Gen2 REST API."""
    import time
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    # URL encode each segment of the path
    encoded_dest = "/".join(quote(seg) for seg in dest_path.split("/") if seg)
    url = f"https://onelake.dfs.fabric.microsoft.com/{workspace}/{lakehouse_folder}/{encoded_dest}"
    
    def _request_with_retry(method: str, req_url: str, max_retries: int = 3, initial_delay: float = 1.0, **kwargs) -> requests.Response:
        delay = initial_delay
        last_ex = None
        for attempt in range(max_retries):
            try:
                res = requests.request(method, req_url, timeout=30, **kwargs)
                if res.status_code in (500, 502, 503, 504, 408):
                    logger.warning(f"Transient HTTP {res.status_code} for {method} {req_url}. Attempt {attempt + 1}/{max_retries}. Retrying in {delay}s...")
                else:
                    return res
            except requests.RequestException as e:
                last_ex = e
                logger.warning(f"Request exception for {method} {req_url}. Attempt {attempt + 1}/{max_retries}. Retrying in {delay}s... Error: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
        if last_ex:
            raise last_ex
        return res

    created = False
    try:
        # 1. Create file placeholder (overwrites if already exists)
        create_url = f"{url}?resource=file"
        logger.info(f"Creating file placeholder in OneLake: {create_url}")
        res = _request_with_retry("PUT", create_url, headers=headers)
        if res.status_code not in (200, 201):
            logger.error(f"Failed to create file placeholder: {res.status_code} - {res.text}")
            return False
        created = True
        
        # 2. Append data
        append_url = f"{url}?action=append&position=0"
        logger.info(f"Appending data to OneLake: {append_url}")
        headers_append = {
            **headers,
            "Content-Type": "application/octet-stream"
        }
        res = _request_with_retry("PATCH", append_url, headers=headers_append, data=file_bytes)
        if res.status_code not in (200, 201, 202):
            logger.error(f"Failed to append data: {res.status_code} - {res.text}")
            raise RuntimeError(f"Failed to append data: {res.status_code}")
            
        # 3. Flush data
        flush_url = f"{url}?action=flush&position={len(file_bytes)}"
        logger.info(f"Flushing data in OneLake: {flush_url}")
        res = _request_with_retry("PATCH", flush_url, headers=headers)
        if res.status_code not in (200, 201, 202):
            logger.error(f"Failed to flush data: {res.status_code} - {res.text}")
            raise RuntimeError(f"Failed to flush data: {res.status_code}")
            
        logger.info("File physical upload completed successfully.")
        return True
    except Exception as e:
        logger.error(f"Physical upload failed: {e}. Cleaning up orphaned placeholder if created.")
        if created:
            try:
                del_res = requests.delete(url, headers=headers, timeout=15)
                logger.info(f"Delete cleanup returned status: {del_res.status_code}")
            except Exception as del_err:
                logger.error(f"Failed to delete orphaned placeholder: {del_err}")
        return False

def create_shortcuts_for_blobs(
    session_id: str,
    selected_blob_paths: List[str],
    blob_account_name: Optional[str] = None,
    blob_container: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Downloads raw files from Azure Blob Storage and uploads them physically
    to the 'Files/raw/' zone of the Fabric Lakehouse (mirroring by copy).
    
    Returns a list of dicts mapping each blob to its new Files/ zone OneLake path.
    """
    account = blob_account_name or _clean_env_value(os.getenv("AZURE_STORAGE_ACCOUNT_NAME"))
    container = blob_container or _clean_env_value(os.getenv("AZURE_ASSESSMENT_CONTAINER") or os.getenv("AZURE_STORAGE_CONTAINER"))
    
    workspace = _clean_env_value(os.getenv("FABRIC_WORKSPACE_ID") or os.getenv("FABRIC_WORKSPACE_NAME"))
    lakehouse = _clean_env_value(os.getenv("FABRIC_LAKEHOUSE_NAME") or os.getenv("FABRIC_LAKEHOUSE_ID"))
    
    if not account or not container:
        raise ValueError("Missing required Azure Storage credentials/container configuration.")
    if not workspace or not lakehouse:
        raise ValueError("Missing required Fabric Workspace or Lakehouse configuration.")

    client = FabricAPIClient()
    results = []
    
    # Initialize the blob connector to download the files
    blob_connector = None
    if not client.mock_mode:
        try:
            blob_connector = AzureBlobStorageConnector({
                "account_name": account,
                "container": container
            })
        except Exception as e:
            logger.error(f"Failed to initialize AzureBlobStorageConnector: {e}")
            
    # Acquire token once for the entire batch in live mode
    cached_token = None
    if not client.mock_mode:
        try:
            cached_token = _get_storage_token()
        except Exception as e:
            logger.error(f"Failed to acquire storage token on startup: {e}")
            
    for blob_path in selected_blob_paths:
        clean_blob_path = blob_path.replace("azure_blob:", "")
        shortcut_name = make_safe_shortcut_name(clean_blob_path)
        _, ext = os.path.splitext(clean_blob_path)
        files_zone_path = f"Files/raw/{shortcut_name}{ext}"
        
        # Build standard OneLake URI: abfss://workspace@onelake.dfs.fabric.microsoft.com/Lakehouse/Files/raw/ndta_csv
        lakehouse_folder = get_lakehouse_folder(lakehouse)
        lakehouse_uri = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse_folder}/{files_zone_path}"
        
        logger.info(f"Physical copy plan: '{files_zone_path}' mirroring blob://{account}/{container}/{clean_blob_path}")
        
        if client.mock_mode:
            logger.info(f"[MOCK] Copying '{clean_blob_path}' to Fabric Files zone.")
            register_shortcut(
                session_id=session_id,
                blob_path=blob_path,
                files_zone_path=files_zone_path,
                shortcut_name=shortcut_name,
                lakehouse_uri=lakehouse_uri,
                method="copy_mock"
            )
            results.append({
                "blob": blob_path,
                "files_path": files_zone_path,
                "shortcut_name": shortcut_name,
                "uri": lakehouse_uri,
                "status": "success"
            })
            continue

        # --- LIVE MODE UPLOAD ---
        if not blob_connector:
            err_msg = "AzureBlobStorageConnector failed to initialize. Cannot copy files."
            logger.error(err_msg)
            results.append({
                "blob": blob_path,
                "files_path": files_zone_path,
                "shortcut_name": shortcut_name,
                "uri": lakehouse_uri,
                "status": "failed",
                "error": err_msg
            })
            continue
            
        try:
            # Fetch properties for staleness checks
            blob_etag = None
            blob_last_modified = None
            try:
                props = blob_connector.get_blob_properties(clean_blob_path)
                blob_etag = props.get("etag")
                blob_last_modified = props.get("last_modified")
            except Exception as pe:
                logger.warning(f"Could not retrieve blob properties for {clean_blob_path}: {pe}")

            # 1. Download bytes from Blob Storage
            logger.info(f"Downloading blob '{clean_blob_path}'...")
            file_bytes = blob_connector._download_blob_bytes(clean_blob_path)
            
            # 2. Acquire/Reuse Storage Access Token
            token = cached_token or _get_storage_token()
            if not token:
                raise RuntimeError("Failed to acquire Azure Storage bearer token for OneLake data plane access.")
                
            # 3. Upload to OneLake
            success = upload_file_to_onelake(
                workspace=workspace,
                lakehouse_folder=lakehouse_folder,
                dest_path=files_zone_path,
                file_bytes=file_bytes,
                token=token
            )
            
            if success:
                register_shortcut(
                    session_id=session_id,
                    blob_path=blob_path,
                    files_zone_path=files_zone_path,
                    shortcut_name=shortcut_name,
                    lakehouse_uri=lakehouse_uri,
                    method="copy",
                    blob_etag=blob_etag,
                    blob_last_modified=blob_last_modified
                )
                results.append({
                    "blob": blob_path,
                    "files_path": files_zone_path,
                    "shortcut_name": shortcut_name,
                    "uri": lakehouse_uri,
                    "status": "success"
                })
            else:
                results.append({
                    "blob": blob_path,
                    "files_path": files_zone_path,
                    "shortcut_name": shortcut_name,
                    "uri": lakehouse_uri,
                    "status": "failed",
                    "error": "Failed to upload file to OneLake via REST API"
                })
        except Exception as e:
            err_msg = str(e)
            logger.exception(f"Failed to copy blob '{clean_blob_path}' to OneLake: {err_msg}")
            results.append({
                "blob": blob_path,
                "files_path": files_zone_path,
                "shortcut_name": shortcut_name,
                "uri": lakehouse_uri,
                "status": "failed",
                "error": err_msg
            })
            
    return results
