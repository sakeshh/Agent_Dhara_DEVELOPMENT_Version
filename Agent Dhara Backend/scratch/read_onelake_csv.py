import requests
from dotenv import load_dotenv
load_dotenv()

from agent.fabric_shortcut_service import _get_storage_token
from agent.fabric_api_client import FabricAPIClient
from connectors.fabric_lakehouse_connector import get_lakehouse_folder

client = FabricAPIClient()
workspace_id = client.workspace_id
lakehouse_id = client.lakehouse_id
lakehouse_folder = get_lakehouse_folder(lakehouse_id)

token = _get_storage_token()
if not token:
    from azure.identity import DefaultAzureCredential
    cred = DefaultAzureCredential()
    token_response = cred.get_token("https://storage.azure.com/.default")
    token = token_response.token

url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_folder}/Files/raw/data_quality_issues_csv.csv"
headers = {
    "Authorization": f"Bearer {token}"
}

print(f"Reading first few bytes of data_quality_issues_csv.csv...")
res = requests.get(url, headers=headers, timeout=20)
print("Status Code:", res.status_code)
if res.status_code == 200:
    print("CSV Content (first 1000 chars):")
    print(res.text[:1000])
else:
    print("Response text:", res.text[:1000])
