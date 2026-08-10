import requests
import json
from dotenv import load_dotenv
load_dotenv()

from agent.fabric_api_client import FabricAPIClient

client = FabricAPIClient()
workspace_id = client.workspace_id
notebook_name = "dhara_a3dee8ef_data_quality_issues"

print(f"Workspace ID: {workspace_id}")
notebook_id = client._find_notebook_by_name(notebook_name)
if not notebook_id:
    print("Notebook not found!")
    exit(1)

url = f"{client.base_url}/workspaces/{workspace_id}/items/{notebook_id}/getDefinition"
headers = client._headers()
res = requests.post(url, headers=headers, timeout=20)

if res.status_code == 202:
    op_location = res.headers.get("Location") or res.headers.get("X-MS-Operation-Location")
    print(f"Operation Location: {op_location}")
    while True:
        op_res = requests.get(op_location, headers=headers, timeout=15)
        op_data = op_res.json()
        if op_data.get("status") == "Succeeded":
            break
        time.sleep(1)
        
    res_result = requests.get(op_location + "/result", headers=headers, timeout=20)
    print("Result Status:", res_result.status_code)
    try:
        data = res_result.json()
        print("Keys:", list(data.keys()))
        if 'definition' in data:
            print("Definition Keys:", list(data['definition'].keys()))
            parts = data['definition'].get('parts', [])
            print("Parts:", [p.get('path') for p in parts])
    except Exception as e:
        print("Error parsing json or key:", e)
        print("Raw text:", res_result.text[:500])
