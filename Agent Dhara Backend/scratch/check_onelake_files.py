import requests
from dotenv import load_dotenv
load_dotenv()

from agent.fabric_shortcut_service import _get_storage_token
from agent.fabric_api_client import FabricAPIClient

client = FabricAPIClient()
workspace_id = client.workspace_id

# Let's check with Dhara_Lake.Lakehouse instead of the GUID
lakehouse_folder = "Dhara_Lake.Lakehouse"

token = _get_storage_token()
if not token:
    from azure.identity import DefaultAzureCredential
    cred = DefaultAzureCredential()
    token_response = cred.get_token("https://storage.azure.com/.default")
    token = token_response.token

url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_folder}?directory=Files&recursive=true&resource=filesystem"
headers = {
    "Authorization": f"Bearer {token}"
}

print(f"Checking files in real lakehouse folder '{lakehouse_folder}'...")
res = requests.get(url, headers=headers, timeout=20)
print("Status Code:", res.status_code)
if res.status_code == 200:
    try:
        data = res.json()
        print("Files:")
        for item in data.get("paths", []):
            print(f"  Name: {item.get('name')}, Size: {item.get('contentLength')}, isDirectory: {item.get('isDirectory')}")
    except Exception as e:
        print("Failed to parse json:", e)
else:
    print("Response text:", res.text[:1000])
