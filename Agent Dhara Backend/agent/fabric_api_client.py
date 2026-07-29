"""
fabric_api_client.py - Microsoft Fabric REST API Client

Provides authenticated access to Microsoft Fabric endpoints for:
- Creating shortcuts in the Files/ zone of a Lakehouse
- Creating and updating Notebooks
- Triggering and monitoring Notebook runs
Supports mock fallback mode for local development when credentials are not configured.
"""

import os
import re
import logging
import requests
from typing import Dict, Any, List, Optional

logger = logging.getLogger("agent.fabric_api_client")

def _clean_env_value(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        s = s[1:-1].strip()
    return s or None

class FabricAPIClient:
    def __init__(self):
        self.tenant_id = _clean_env_value(os.getenv("FABRIC_TENANT_ID"))
        self.client_id = _clean_env_value(os.getenv("FABRIC_CLIENT_ID") or os.getenv("FABRIC_SERVICE_PRINCIPAL_ID"))
        self.client_secret = _clean_env_value(os.getenv("FABRIC_CLIENT_SECRET"))
        self.workspace_id = _clean_env_value(os.getenv("FABRIC_WORKSPACE_ID"))
        self.lakehouse_id = _clean_env_value(os.getenv("FABRIC_LAKEHOUSE_NAME") or os.getenv("FABRIC_LAKEHOUSE_ID"))
        
        self.base_url = "https://api.fabric.microsoft.com/v1"
        self.token: Optional[str] = None
        self._token_expires_at: float = 0.0
        
        # Check if we should run in mock mode
        self.mock_mode = os.getenv("DHARA_FABRIC_MOCK", "0").strip().lower() in ("1", "true", "yes")
            
    def _acquire_token(self) -> Optional[str]:
        import time
        if self.mock_mode:
            return "mock-token"
            
        if self.token and time.time() < self._token_expires_at - 60:
            return self.token

        if not self.workspace_id:
            raise RuntimeError("FABRIC_WORKSPACE_ID environment variable is missing or empty.")
        if not self.lakehouse_id:
            raise RuntimeError("FABRIC_LAKEHOUSE_ID / FABRIC_LAKEHOUSE_NAME environment variable is missing or empty.")
            
        # Try service principal first
        if self.tenant_id and self.client_id and self.client_secret:
            logger.info("Acquiring Fabric API token via Service Principal credentials.")
            url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "https://api.fabric.microsoft.com/.default"
            }
            try:
                res = requests.post(url, data=data, timeout=15)
                res.raise_for_status()
                payload = res.json()
                self.token = payload.get("access_token")
                expires_in = int(payload.get("expires_in", 3600))
                self._token_expires_at = time.time() + expires_in
                return self.token
            except Exception as e:
                logger.error(f"Failed to acquire token via Service Principal: {e}")
                raise RuntimeError(f"Fabric token acquisition failed via Service Principal: {e}") from e
                
        # Fall back to DefaultAzureCredential
        logger.info("Falling back to DefaultAzureCredential for Fabric API token.")
        try:
            from azure.identity import DefaultAzureCredential
            cred = DefaultAzureCredential()
            token_response = cred.get_token("https://api.fabric.microsoft.com/.default")
            self.token = token_response.token
            self._token_expires_at = getattr(token_response, "expires_on", time.time() + 3600)
            return self.token
        except Exception as e:
            logger.error(f"Could not acquire Fabric token via DefaultAzureCredential: {e}")
            raise RuntimeError(f"Fabric token acquisition failed: {e}") from e

    def _headers(self) -> Dict[str, str]:
        token = self._acquire_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    # ── Shortcuts API ──
    def create_shortcut(
        self,
        shortcut_name: str,
        target_blob_account: str,
        target_container: str,
        target_path: str,
        destination_path: str = "Files/raw",
    ) -> Dict[str, Any]:
        """
        Creates a shortcut in a Lakehouse pointing to an Azure Blob Storage location.
        """
        if self.mock_mode:
            logger.info(f"[MOCK] Creating shortcut '{shortcut_name}' pointing to blob://{target_blob_account}/{target_container}/{target_path}")
            return {
                "ok": True,
                "name": shortcut_name,
                "path": destination_path,
                "target": {"type": "BlobStorage", "location": f"https://{target_blob_account}.blob.core.windows.net/{target_container}", "subpath": f"/{target_path}"}
            }

        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.lakehouse_id}/shortcuts"
        
        # Build the payload according to Fabric specifications
        payload = {
            "path": destination_path,
            "name": shortcut_name,
            "target": {
                "azureBlobStorage": {
                    "location": f"https://{target_blob_account}.blob.core.windows.net",
                    "subpath": f"/{target_container}/{target_path.lstrip('/')}"
                }
            }
        }
        
        try:
            res = requests.post(url, json=payload, headers=self._headers(), timeout=20)
            if res.status_code in (200, 201):
                return {"ok": True, **res.json()}
            else:
                logger.error(f"Fabric API error ({res.status_code}): {res.text}")
                return {"ok": False, "error": "FABRIC_API_ERROR", "message": res.text}
        except Exception as e:
            logger.exception(f"Failed to call create_shortcut Fabric REST endpoint: {e}")
            return {"ok": False, "error": "CONNECTIVITY_FAILED", "message": str(e)}

    # ── Notebooks API ──
    def create_or_update_notebook(
        self,
        notebook_name: str,
        pyspark_code: str,
        lakehouse_id: str
    ) -> Dict[str, Any]:
        """
        Deploys a Spark Notebook item in a Fabric Workspace.
        """
        if self.mock_mode:
            import uuid
            logger.info(f"[MOCK] Deploying notebook '{notebook_name}' to Fabric Workspace.")
            return {
                "ok": True,
                "id": str(uuid.uuid4()),
                "name": notebook_name,
                "type": "Notebook"
            }

        # First check if the notebook already exists by listing notebooks
        notebook_id = self._find_notebook_by_name(notebook_name)
        
        # Convert code cell to Fabric notebook definition format
        notebook_payload = self._build_notebook_definition_payload(notebook_name, pyspark_code, lakehouse_id)
        
        try:
            if notebook_id:
                # Update existing
                url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{notebook_id}/updateDefinition"
                update_payload = {
                    "definition": notebook_payload
                }
                res = requests.post(url, json=update_payload, headers=self._headers(), timeout=30)
                res.raise_for_status()
                return {"ok": True, "id": notebook_id, "name": notebook_name}
            else:
                # Create new
                url = f"{self.base_url}/workspaces/{self.workspace_id}/items"
                create_payload = {
                    "displayName": notebook_name,
                    "type": "Notebook",
                    "definition": notebook_payload
                }
                res = requests.post(url, json=create_payload, headers=self._headers(), timeout=30)
                if res.status_code in (200, 201, 202):
                    item_id = None
                    try:
                        data = res.json()
                        if isinstance(data, dict):
                            item_id = data.get("id")
                    except Exception:
                        pass
                        
                    # Check Location or Operation-Location headers
                    if not item_id:
                        loc_header = res.headers.get("Location") or res.headers.get("X-MS-Operation-Location") or ""
                        if "/items/" in loc_header:
                            item_id = loc_header.split("/items/")[-1].split("/")[0].split("?")[0]
                            
                    # Poll workspace if needed (asynchronous creation status 202)
                    if not item_id:
                        logger.info(f"Asynchronous creation returned status {res.status_code}. Polling workspace for notebook '{notebook_name}'...")
                        import time
                        for _ in range(10):
                            time.sleep(2)
                            item_id = self._find_notebook_by_name(notebook_name)
                            if item_id:
                                break
                                
                    if not item_id:
                        return {"ok": False, "error": "NOTEBOOK_ID_NOT_FOUND", "message": f"Notebook created but failed to retrieve its ID. Response headers: {dict(res.headers)}"}
                        
                    return {"ok": True, "id": item_id, "name": notebook_name}
                else:
                    logger.error(f"Fabric API notebook creation error: {res.text}")
                    return {"ok": False, "error": "CREATE_NOTEBOOK_FAILED", "message": res.text}
        except Exception as e:
            logger.exception(f"Failed to create/update notebook '{notebook_name}': {e}")
            return {"ok": False, "error": "NOTEBOOK_DEPLOY_FAILED", "message": str(e)}

    def trigger_notebook_run(self, notebook_id: str) -> str:
        """
        Starts a Notebook execution job via the Fabric Jobs API. Returns a run ID.
        """
        if self.mock_mode:
            import uuid
            r_id = f"mock-run-{uuid.uuid4().hex[:8]}"
            logger.info(f"[MOCK] Triggered notebook run on notebook {notebook_id}. Run ID: {r_id}")
            return r_id

        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"
        try:
            # POST request to run notebook job
            res = requests.post(url, headers=self._headers(), timeout=20)
            res.raise_for_status()
            
            # The run job instance ID is typically returned in the headers Location or Response Body
            # Fabric API v1 usually returns 202 Accepted with jobInstanceId in Location or headers
            job_id = res.headers.get("Location", "").split("/")[-1] or res.json().get("id")
            if not job_id:
                # Fallback to look at response body
                job_id = res.json().get("id") or "unknown-run"
            return job_id
        except Exception as e:
            logger.exception(f"Failed to trigger notebook run for {notebook_id}: {e}")
            raise RuntimeError(f"Failed to trigger notebook job in Fabric: {e}")

    def get_run_status(self, notebook_id: str, run_id: str) -> Dict[str, Any]:
        """
        Checks status of a notebook run.
        Returns e.g. {"status": "Succeeded" | "Failed" | "InProgress" | "NotStarted"}
        """
        if self.mock_mode or run_id.startswith("mock-run"):
            # Simulate status transition
            import time
            # Check a state counter in env/local memory or just auto-succeed
            logger.info(f"[MOCK] Checking run status of job {run_id}")
            return {"status": "Succeeded", "ok": True}

        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{notebook_id}/jobs/instances/{run_id}"
        try:
            res = requests.get(url, headers=self._headers(), timeout=15)
            res.raise_for_status()
            data = res.json()
            # Standard Fabric states: Completed, Failed, InProgress, Cancelled
            status = data.get("status", "InProgress")
            
            # Map to standard workflow states
            mapped_status = "InProgress"
            if status == "Completed":
                mapped_status = "Succeeded"
            elif status in ("Failed", "Cancelled"):
                mapped_status = "Failed"
                
            error_detail = None
            if mapped_status == "Failed":
                error_detail = data.get("failureReason")
                if not error_detail and "error" in data:
                    err_obj = data["error"]
                    if isinstance(err_obj, dict):
                        error_detail = err_obj.get("message") or err_obj.get("code")
                    else:
                        error_detail = str(err_obj)
                if not error_detail:
                    error_detail = "Execution failed without detail message"

            return {
                "status": mapped_status,
                "raw_status": status,
                "ok": mapped_status == "Succeeded",
                "error": error_detail
            }
        except Exception as e:
            logger.warning(f"Failed to check run status for job {run_id}: {e}")
            return {"status": "InProgress", "ok": False, "error": str(e)}

    def get_lakehouse_properties(self) -> Optional[Dict[str, Any]]:
        """Used to test connection capability and retrieve lakehouse properties."""
        if self.mock_mode:
            return {"displayName": "mock-lakehouse", "id": self.lakehouse_id}
            
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.lakehouse_id}"
        try:
            res = requests.get(url, headers=self._headers(), timeout=15)
            res.raise_for_status()
            return res.json()
        except Exception as e:
            logger.error(f"Failed to retrieve properties for lakehouse {self.lakehouse_id}: {e}")
            return None

    def _find_notebook_by_name(self, name: str) -> Optional[str]:
        """Finds notebook ID in workspace by displayName."""
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items"
        try:
            res = requests.get(url, headers=self._headers(), timeout=15)
            res.raise_for_status()
            items = res.json().get("value", [])
            for item in items:
                if item.get("displayName") == name:
                    return item.get("id")
            return None
        except Exception as e:
            logger.warning(f"Could not list notebooks to find by name: {e}")
            return None

    def _build_notebook_definition_payload(self, name: str, code: str, lakehouse_id: str) -> Dict[str, Any]:
        """
        Builds the base64 payload format required by Fabric for notebooks updateDefinition/creation.
        Fabric REST API expects the notebook format in Jupyter JSON, then base64 encoded.
        """
        import base64
        import json
        import html
        
        clean_code = html.unescape(code or "")
        
        # Query lakehouse displayName to set the name in dependencies
        lakehouse_name = self.resolve_lakehouse_name_by_id(self.workspace_id, lakehouse_id) or ""
        
        notebook_json = {
            "nbformat": 4,
            "nbformat_minor": 2,
            "metadata": {
                "language_info": {"name": "python"},
                "kernel_info": {
                    "name": "synapse_pyspark"
                },
                "trident": {
                    "lakehouse": {
                        "defaultLakehouse": lakehouse_id,
                        "defaultLakehouseWorkspace": self.workspace_id
                    }
                },
                "dependencies": {
                    "lakehouse": {
                        "default_lakehouse": lakehouse_id,
                        "default_lakehouse_name": lakehouse_name,
                        "default_lakehouse_workspace_id": self.workspace_id,
                        "known_lakehouses": [
                            {
                                "id": lakehouse_id
                            }
                        ]
                    }
                }
            },
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [line + "\n" for line in clean_code.splitlines()]
                }
            ]
        }
        
        json_str = json.dumps(notebook_json)
        json_bytes = json_str.encode("utf-8")
        b64_content = base64.b64encode(json_bytes).decode("utf-8")
        
        return {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": b64_content,
                    "payloadType": "inlineBase64"
                }
            ]
        }

    def resolve_lakehouse_name_by_id(self, workspace_id: str, item_id: str) -> Optional[str]:
        """Finds lakehouse displayName in workspace by ID."""
        if self.mock_mode:
            return "mock-lakehouse"
        url = f"{self.base_url}/workspaces/{workspace_id}/items"
        try:
            res = requests.get(url, headers=self._headers(), timeout=15)
            res.raise_for_status()
            items = res.json().get("value", [])
            for item in items:
                if item.get("id") == item_id and item.get("type") == "Lakehouse":
                    return item.get("displayName")
            return None
        except Exception as e:
            logger.warning(f"Could not list items to resolve lakehouse ID '{item_id}': {e}")
            return None

    def resolve_lakehouse_id_by_name(self, workspace_id: str, name: str) -> Optional[str]:
        """Finds lakehouse ID in workspace by displayName."""
        if self.mock_mode:
            return name
        url = f"{self.base_url}/workspaces/{workspace_id}/items"
        try:
            res = requests.get(url, headers=self._headers(), timeout=15)
            res.raise_for_status()
            items = res.json().get("value", [])
            for item in items:
                if item.get("displayName") == name and item.get("type") == "Lakehouse":
                    return item.get("id")
            return None
        except Exception as e:
            logger.warning(f"Could not list items to resolve lakehouse name '{name}': {e}")
            return None

