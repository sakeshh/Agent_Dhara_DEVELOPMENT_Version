import os
import unittest
import asyncio
from unittest.mock import patch, MagicMock, mock_open
from mcp_server.mcp_fabric import (
    mcp,
    _get_token,
    copy_blob_to_onelake,
    copy_local_file_to_onelake,
    run_fabric_notebook,
    run_fabric_spark_job,
    get_fabric_job_status,
)


class TestMSFabricMCP(unittest.TestCase):
    def test_mcp_tool_registration(self):
        """Assert that all tools are successfully registered with the FastMCP instance."""
        tools = asyncio.run(mcp.list_tools())
        tool_names = [t.name for t in tools]
        self.assertIn("copy_blob_to_onelake", tool_names)
        self.assertIn("copy_local_file_to_onelake", tool_names)
        self.assertIn("run_fabric_notebook", tool_names)
        self.assertIn("run_fabric_spark_job", tool_names)
        self.assertIn("get_fabric_job_status", tool_names)

    @patch("azure.identity.ClientSecretCredential")
    @patch.dict(os.environ, {
        "FABRIC_CLIENT_ID": "client-123",
        "FABRIC_CLIENT_SECRET": "secret-123",
        "FABRIC_TENANT_ID": "tenant-123"
    })
    def test_get_token_with_client_secret(self, mock_cred_cls):
        """Verify that _get_token uses ClientSecretCredential when environment variables are set."""
        mock_cred = MagicMock()
        mock_token = MagicMock()
        mock_token.token = "fake-token"
        mock_cred.get_token.return_value = mock_token
        mock_cred_cls.return_value = mock_cred

        token = _get_token("https://api.fabric.microsoft.com/.default")
        self.assertEqual(token, "fake-token")
        mock_cred_cls.assert_called_once_with(
            tenant_id="tenant-123",
            client_id="client-123",
            client_secret="secret-123"
        )
        mock_cred.get_token.assert_called_once_with("https://api.fabric.microsoft.com/.default")

    @patch("mcp_server.mcp_fabric._get_token")
    @patch("mcp_server.mcp_fabric._get_source_sas_url")
    @patch("mcp_server.mcp_fabric.BlobServiceClient")
    def test_copy_blob_to_onelake_success(self, mock_blob_service_cls, mock_sas_url, mock_token):
        """Verify copy_blob_to_onelake triggers cloud copy successfully."""
        mock_token.return_value = "fake-token"
        mock_sas_url.return_value = "https://src.blob.core.windows.net/src/blob?sas"

        # Mock destination blob client
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_blob = MagicMock()
        mock_properties = MagicMock()
        
        # Properties return copy success
        mock_properties.copy.status = "success"
        mock_blob.get_blob_properties.return_value = mock_properties
        
        mock_container.get_blob_client.return_value = mock_blob
        mock_client.get_container_client.return_value = mock_container
        mock_blob_service_cls.return_value = mock_client

        res = copy_blob_to_onelake(
            src_account_name="srcacc",
            src_container="raw",
            src_blob_name="data.csv",
            workspace_id="ws-123",
            lakehouse_id="lh-123",
            dest_path="raw/data.csv",
            src_account_key="key-123"
        )

        self.assertIn("Success: Cloud copy complete", res)
        mock_blob.start_copy_from_url.assert_called_once_with("https://src.blob.core.windows.net/src/blob?sas")

    @patch("mcp_server.mcp_fabric._get_token")
    @patch("requests.post")
    def test_run_fabric_notebook_accepted(self, mock_post, mock_token):
        """Verify run_fabric_notebook triggers REST call and returns 202 status."""
        mock_token.return_value = "fake-token"
        
        mock_response = MagicMock()
        mock_response.status_code = 202
        mock_response.headers = {"Location": "https://api.fabric.microsoft.com/jobs/instances/inst-123"}
        mock_post.return_value = mock_response

        res = run_fabric_notebook(
            workspace_id="ws-123",
            notebook_id="nb-123",
            parameters={"param1": "val1"}
        )

        self.assertTrue(res["ok"])
        self.assertEqual(res["job_instance_id"], "inst-123")
        self.assertEqual(res["status"], "Accepted")
        mock_post.assert_called_once_with(
            "https://api.fabric.microsoft.com/v1/workspaces/ws-123/items/nb-123/jobs/instances?jobType=RunNotebook",
            headers={
                "Authorization": "Bearer fake-token",
                "Content-Type": "application/json"
            },
            json={"executionParameters": {"param1": "val1"}}
        )


if __name__ == "__main__":
    unittest.main()
