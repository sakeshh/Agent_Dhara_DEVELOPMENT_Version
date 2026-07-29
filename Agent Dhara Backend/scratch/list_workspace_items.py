import requests
from dotenv import load_dotenv
load_dotenv()

from agent.fabric_api_client import FabricAPIClient

client = FabricAPIClient()
workspace_id = client.workspace_id

url = f"{client.base_url}/workspaces/{workspace_id}/items"
headers = client._headers()

print("Listing all items in the workspace...")
res = requests.get(url, headers=headers, timeout=20)
print("Status Code:", res.status_code)
if res.status_code == 200:
    data = res.json()
    for item in data.get("value", []):
        print(f"  Name: {item.get('displayName')}, Type: {item.get('type')}, ID: {item.get('id')}")
else:
    print("Response:", res.text)
