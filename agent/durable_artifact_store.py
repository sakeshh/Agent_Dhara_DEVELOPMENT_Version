"""
Durable Shared Artifact and Job State Store for Agent Dhara.

Ensures that assessment job state and generated artifacts (HTML reports,
cleaning manifests, JSON evaluations) survive Azure App Service container transitions,
slot swaps, service restarts, and multi-instance scale-outs.

Storage Hierarchy:
1. Azure Blob Storage (if AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_NAME is set).
2. Persistent Azure Files mount (/home/data) on Azure App Service.
3. Configured DHARA_DATA_DIR or local output directory.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any, Dict, Optional, Union

logger = logging.getLogger("agent.durable_artifact_store")


def get_instance_id() -> str:
    """Returns a unique identifier for the current running process / container instance."""
    return (
        os.getenv("WEBSITE_INSTANCE_ID")
        or os.getenv("CONTAINER_ID")
        or os.getenv("HOSTNAME")
        or f"local-{os.getpid()}"
    )


def get_durable_base_dir() -> str:
    """
    Returns the durable base directory for storing files.
    Prioritizes DHARA_DATA_DIR, then /home/data on Azure App Service, then local project output.
    """
    data_dir = os.environ.get("DHARA_DATA_DIR")
    if not data_dir:
        # On Azure App Service, /home is mounted to durable Azure Files SMB storage.
        if os.environ.get("WEBSITE_SITE_NAME") and os.path.isdir("/home") and os.access("/home", os.W_OK):
            data_dir = "/home/data"
        else:
            root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            data_dir = os.path.join(root, "output")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir


def _get_blob_service_client():
    """Builds a BlobServiceClient if Azure storage credentials are configured."""
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        return None

    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn_str:
        try:
            return BlobServiceClient.from_connection_string(conn_str)
        except Exception as e:
            logger.warning("Failed to initialize BlobServiceClient from connection string: %s", e)

    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY") or os.getenv("AZURE_STORAGE_KEY")
    if account_name and account_key:
        cs = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={account_name};"
            f"AccountKey={account_key};"
            f"EndpointSuffix=core.windows.net"
        )
        try:
            return BlobServiceClient.from_connection_string(cs)
        except Exception as e:
            logger.warning("Failed to initialize BlobServiceClient from account/key: %s", e)

    if account_name:
        try:
            from azure.identity import DefaultAzureCredential
            cred = DefaultAzureCredential()
            account_url = f"https://{account_name}.blob.core.windows.net"
            return BlobServiceClient(account_url=account_url, credential=cred)
        except Exception:
            pass

    return None


def get_storage_container_name() -> str:
    """Returns the container name for storing assessment artifacts."""
    return (
        os.getenv("AZURE_ASSESSMENT_CONTAINER")
        or os.getenv("AZURE_STORAGE_CONTAINER")
        or "agentdhararawdata"
    )


def save_durable_artifact(
    job_id: str,
    filename: str,
    content: Union[str, bytes],
    content_type: str = "application/octet-stream",
) -> str:
    """
    Saves an artifact (HTML, JSON, markdown) to durable storage.
    Writes to local durable disk ({base_dir}/assessment-runs/{job_id}/{filename})
    AND uploads to Azure Blob Storage if available.

    Returns the canonical durable path relative to the root (e.g. 'assessment-runs/{job_id}/{filename}').
    """
    relative_path = f"assessment-runs/{job_id}/{filename}".replace("\\", "/")
    base_dir = get_durable_base_dir()
    local_path = os.path.join(base_dir, relative_path)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # 1. Write to local durable filesystem
    data_bytes = content.encode("utf-8") if isinstance(content, str) else content
    try:
        with open(local_path, "wb") as f:
            f.write(data_bytes)
        logger.debug("[DurableStore] Saved local artifact: %s (%d bytes)", local_path, len(data_bytes))
    except Exception as e:
        logger.warning("[DurableStore] Failed writing local artifact %s: %s", local_path, e)

    # 2. Upload to Azure Blob Storage if configured
    client = _get_blob_service_client()
    if client:
        try:
            container_name = get_storage_container_name()
            container_client = client.get_container_client(container_name)
            try:
                if not container_client.exists():
                    container_client.create_container()
            except Exception:
                pass  # Best effort if container already exists or permission restricted
            blob_client = container_client.get_blob_client(relative_path)
            from azure.storage.blob import ContentSettings
            blob_client.upload_blob(
                data_bytes,
                overwrite=True,
                content_settings=ContentSettings(content_type=content_type),
            )
            logger.info(
                "[DurableStore] Uploaded blob artifact: %s/%s (%d bytes)",
                container_name, relative_path, len(data_bytes)
            )
        except Exception as e:
            logger.warning("[DurableStore] Blob upload failed for %s: %s", relative_path, e)

    return relative_path


def load_durable_artifact(job_id: str, filename: str) -> Optional[bytes]:
    """
    Loads an artifact from local durable disk or Azure Blob Storage.
    """
    relative_path = f"assessment-runs/{job_id}/{filename}".replace("\\", "/")
    base_dir = get_durable_base_dir()
    local_path = os.path.join(base_dir, relative_path)

    # 1. Try local disk first
    if os.path.isfile(local_path):
        try:
            with open(local_path, "rb") as f:
                return f.read()
        except Exception as e:
            logger.debug("[DurableStore] Error reading local file %s: %s", local_path, e)

    # 2. Try Azure Blob Storage
    client = _get_blob_service_client()
    if client:
        try:
            container_name = get_storage_container_name()
            blob_client = client.get_blob_client(container=container_name, blob=relative_path)
            if blob_client.exists():
                stream = blob_client.download_blob()
                content = stream.readall()
                # Cache locally on disk for fast subsequent reads
                try:
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    with open(local_path, "wb") as f:
                        f.write(content)
                except Exception:
                    pass
                return content
        except Exception as e:
            logger.warning("[DurableStore] Error fetching blob %s: %s", relative_path, e)

    return None


def verify_durable_artifact_exists(job_id: str, filename: str) -> bool:
    """Checks if an artifact exists in local durable storage or in Azure Blob Storage."""
    relative_path = f"assessment-runs/{job_id}/{filename}".replace("\\", "/")
    base_dir = get_durable_base_dir()
    local_path = os.path.join(base_dir, relative_path)

    if os.path.isfile(local_path) and os.path.getsize(local_path) > 0:
        return True

    client = _get_blob_service_client()
    if client:
        try:
            container_name = get_storage_container_name()
            blob_client = client.get_blob_client(container=container_name, blob=relative_path)
            return bool(blob_client.exists())
        except Exception:
            return False

    return False


def save_durable_job_state(job_id: str, state: Dict[str, Any]) -> None:
    """
    Saves a snapshot of the job record (metadata, stage, progress, artifact paths)
    to durable storage so another container instance can read it immediately.
    """
    state_copy = dict(state)
    state_copy["worker_instance_id"] = get_instance_id()
    state_copy["heartbeat_at"] = time.time()
    try:
        content = json.dumps(state_copy, ensure_ascii=False, indent=2, default=str)
        save_durable_artifact(
            job_id=job_id,
            filename="job_state.json",
            content=content,
            content_type="application/json",
        )
    except Exception as e:
        logger.warning("[DurableStore] Failed to save durable job state for %s: %s", job_id, e)


def load_durable_job_state(job_id: str) -> Optional[Dict[str, Any]]:
    """
    Loads job metadata state from durable storage if available.
    """
    raw_bytes = load_durable_artifact(job_id=job_id, filename="job_state.json")
    if not raw_bytes:
        return None
    try:
        return json.loads(raw_bytes.decode("utf-8"))
    except Exception as e:
        logger.warning("[DurableStore] Failed parsing job_state.json for %s: %s", job_id, e)
        return None
