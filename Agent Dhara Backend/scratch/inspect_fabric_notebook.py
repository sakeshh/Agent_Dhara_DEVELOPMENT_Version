import requests
import base64
import json
import time
from dotenv import load_dotenv
load_dotenv()

from agent.fabric_api_client import FabricAPIClient

client = FabricAPIClient()
workspace_id = client.workspace_id
notebook_name = "Notebook_1_test"

print(f"Workspace ID: {workspace_id}")
notebook_id = client._find_notebook_by_name(notebook_name)
if not notebook_id:
    # Try Mohan_nb_test
    notebook_name = "Mohan_nb_test"
    notebook_id = client._find_notebook_by_name(notebook_name)

if not notebook_id:
    print("No test notebook found!")
    exit(1)

print(f"Notebook ID: {notebook_id} ({notebook_name})")
url = f"{client.base_url}/workspaces/{workspace_id}/items/{notebook_id}/getDefinition"
headers = client._headers()
res = requests.post(url, headers=headers, timeout=20)

if res.status_code == 202:
    op_location = res.headers.get("Location") or res.headers.get("X-MS-Operation-Location")
    while True:
        op_res = requests.get(op_location, headers=headers, timeout=15)
        op_data = op_res.json()
        if op_data.get("status") == "Succeeded":
            break
        time.sleep(1)
        
    res_result = requests.get(op_location + "/result", headers=headers, timeout=20)
    definition = res_result.json()
else:
    definition = res.json()

parts = definition.get("definition", {}).get("parts", [])
for p in parts:
    if p.get("path") == "notebook-content.py":
        payload = p.get("payload")
        decoded_bytes = base64.b64decode(payload)
        text = decoded_bytes.decode("utf-8")
        print("\n--- METADATA BLOCK ---")
        # Print the metadata comments from the top of the .py file
        lines = text.splitlines()
        for line in lines[:40]:
            print(line)
