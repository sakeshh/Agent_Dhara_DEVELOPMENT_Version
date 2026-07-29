import os
import requests
import io
import pandas as pd
from connectors.fabric_lakehouse_connector import get_fabric_storage_options

def main():
    workspace = os.getenv("FABRIC_WORKSPACE_ID")
    lakehouse = os.getenv("FABRIC_LAKEHOUSE_NAME") or os.getenv("FABRIC_LAKEHOUSE_ID")
    
    opts = get_fabric_storage_options()
    token = opts.get("bearer_token")
    if not token:
        try:
            from azure.identity import AzureCliCredential
            cred = AzureCliCredential()
            token = cred.get_token("https://storage.azure.com/.default").token
        except Exception as e:
            print("Failed token fallback:", e)

    if not token:
        print("Error: No Azure Storage token.")
        return

    headers = {"Authorization": f"Bearer {token}"}
    
    # Try single Tables/ segment
    parquet_path = "Tables/dbo/data_quality_issues_csv_clean/part-00000-8c3370ee-1ad6-4ab9-91f6-346643769cba-c000.snappy.parquet"
    url = f"https://onelake.dfs.fabric.microsoft.com/{workspace}/{lakehouse}/{parquet_path}"
    
    print(f"Downloading parquet data file: {url}")
    res = requests.get(url, headers=headers, timeout=30)
    print(f"Status Code: {res.status_code}")
    if res.status_code == 200:
        df = pd.read_parquet(io.BytesIO(res.content))
        print(f"\n=======================================================")
        print(f"Actual Row Count in Cleaned Delta Table: {len(df)}")
        print(f"=======================================================")
        print("First 10 rows in Delta table:")
        print(df.head(10))
    else:
        print(f"Response: {res.text}")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))
    main()
