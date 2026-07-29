import os
import requests
from urllib.parse import quote
from connectors.fabric_lakehouse_connector import get_fabric_storage_options, get_lakehouse_folder

def main():
    workspace = os.getenv("FABRIC_WORKSPACE_ID")
    lakehouse = os.getenv("FABRIC_LAKEHOUSE_NAME") or os.getenv("FABRIC_LAKEHOUSE_ID")
    
    # Resolve storage options (bearer token, tenant, client_id, etc.)
    opts = get_fabric_storage_options()
    token = opts.get("bearer_token")
    
    if not token:
        # Try acquiring token via Service Principal
        client_id = opts.get("client_id")
        client_secret = opts.get("client_secret")
        tenant_id = opts.get("tenant_id") or os.getenv("FABRIC_TENANT_ID")
        if client_id and client_secret and tenant_id:
            url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
            data = {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "https://storage.azure.com/.default"
            }
            res = requests.post(url, data=data, timeout=15)
            token = res.json().get("access_token")
            
    # Try local CLI credential fallback if available
    if not token:
        try:
            from azure.identity import AzureCliCredential
            cred = AzureCliCredential()
            token_obj = cred.get_token("https://storage.azure.com/.default")
            token = token_obj.token
        except Exception as e:
            print("Failed fallback token acquire:", e)

    if not token:
        print("Error: Could not acquire Azure Storage token.")
        return

    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    lakehouse_folder = get_lakehouse_folder(lakehouse)
    encoded_dest = "Files/raw/data_quality_issues_csv.csv"
    url = f"https://onelake.dfs.fabric.microsoft.com/{workspace}/{lakehouse_folder}/{encoded_dest}"
    
    print(f"Downloading from OneLake: {url}")
    res = requests.get(url, headers=headers, timeout=30)
    if res.status_code == 200:
        content = res.text
        lines = content.splitlines()
        print(f"File downloaded successfully! Total lines in OneLake file: {len(lines)}")
        print("First 5 lines:")
        for line in lines[:5]:
            print(line)
        print("Last 5 lines:")
        for line in lines[-5:]:
            print(line)
    else:
        print(f"Failed to download: {res.status_code} - {res.text}")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))
    main()
