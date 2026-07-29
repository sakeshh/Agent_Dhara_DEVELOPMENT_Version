"""
mirror_service.py - Orchestrates mirroring Azure Blob storage locations to Microsoft Fabric OneLake via Shortcuts.
"""

from __future__ import annotations

import logging
from typing import Dict, Any, List

from agent.blob_fabric_registry import get_shortcut
from agent.fabric_shortcut_service import create_shortcuts_for_blobs

logger = logging.getLogger("agent.mirror_service")

def mirror_blobs_to_fabric(
    session_id: str,
    blob_paths: List[str]
) -> Dict[str, Any]:
    """
    Ensures zero-copy shortcuts are established in Microsoft Fabric OneLake for the given blob paths.
    Checks existing shortcuts first to avoid redundant API calls.
    
    Returns a dictionary mapping blob_path -> shortcut metadata/status.
    """
    from agent.fabric_api_client import FabricAPIClient
    from connectors.azure_blob_storage import AzureBlobStorageConnector
    import os
    
    client = FabricAPIClient()
    blob_connector = None
    if not client.mock_mode:
        try:
            account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
            container = os.getenv("AZURE_ASSESSMENT_CONTAINER") or os.getenv("AZURE_STORAGE_CONTAINER")
            if account and container:
                blob_connector = AzureBlobStorageConnector({
                    "account_name": account,
                    "container": container
                })
        except Exception as e:
            logger.warning(f"Could not initialize AzureBlobStorageConnector in mirror_service: {e}")

    results = {}
    to_create = []
    
    # 1. Identify which paths are already mirrored
    for raw_path in blob_paths:
        clean_path = raw_path.replace("azure_blob:", "").lstrip("/")
        try:
            existing = get_shortcut(session_id, clean_path)
            
            is_stale = False
            if existing and blob_connector and not client.mock_mode:
                try:
                    props = blob_connector.get_blob_properties(clean_path)
                    current_etag = props.get("etag")
                    current_modified = props.get("last_modified")
                    
                    stored_etag = existing.get("blob_etag")
                    stored_modified = existing.get("blob_last_modified")
                    
                    if current_etag and stored_etag and current_etag != stored_etag:
                        is_stale = True
                        logger.info(f"Blob '{clean_path}' ETag changed (stored: {stored_etag}, current: {current_etag}). Forcing re-mirror.")
                    elif current_modified and stored_modified and current_modified > stored_modified:
                        is_stale = True
                        logger.info(f"Blob '{clean_path}' LastModified is newer than stored. Forcing re-mirror.")
                except Exception as pe:
                    logger.warning(f"Could not verify staleness for '{clean_path}': {pe}")
            
            if existing and not is_stale:
                results[raw_path] = {
                    "lakehouse_uri": existing["lakehouse_uri"],
                    "shortcut_name": existing["shortcut_name"],
                    "files_zone_path": existing["files_zone_path"],
                    "status": "already_mirrored",
                    "ok": True
                }
                logger.info(f"Blob '{clean_path}' is already mirrored to Fabric in session '{session_id}'.")
            else:
                to_create.append(raw_path)
        except Exception as e:
            logger.exception(f"Failed checking shortcut registry for {raw_path}")
            results[raw_path] = {
                "status": "failed",
                "error": f"Registry check failed: {e}",
                "ok": False
            }
            
    # 2. Call shortcut service for any un-mirrored paths
    for raw_path in to_create:
        try:
            logger.info(f"Creating shortcut for '{raw_path}' in session '{session_id}'...")
            create_results = create_shortcuts_for_blobs(
                session_id=session_id,
                selected_blob_paths=[raw_path]
            )
            
            if not create_results:
                raise ValueError("No result returned from shortcut service")
                
            item = create_results[0]
            blob = item["blob"]
            if item.get("status") == "success":
                results[blob] = {
                    "lakehouse_uri": item["uri"],
                    "shortcut_name": item["shortcut_name"],
                    "files_zone_path": item["files_path"],
                    "status": "mirrored",
                    "ok": True
                }
            else:
                results[blob] = {
                    "status": "failed",
                    "error": item.get("error", "Failed to create Fabric shortcut"),
                    "ok": False
                }
        except Exception as e:
            logger.exception(f"Error mirroring blob {raw_path} to Fabric")
            results[raw_path] = {
                "status": "failed",
                "error": f"Mirroring error: {e}",
                "ok": False
            }
                
    # 3. Log structured summary
    mirrored_count = sum(1 for r in results.values() if r.get("status") == "mirrored")
    already_count = sum(1 for r in results.values() if r.get("status") == "already_mirrored")
    failed_count = sum(1 for r in results.values() if r.get("status") == "failed")
    logger.info(
        f"Mirror summary: total={len(blob_paths)}, mirrored={mirrored_count}, "
        f"already_mirrored={already_count}, failed={failed_count}"
    )
    return results
