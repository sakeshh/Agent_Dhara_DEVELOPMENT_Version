import os
import sys
import yaml
from dotenv import load_dotenv

backend_dir = r"c:\Users\ssakesh\Downloads\DHARA-GX\Agent Dhara Backend"

# Load env variables
dotenv_path = os.path.join(backend_dir, ".env")
load_dotenv(dotenv_path)

sys.path.append(backend_dir)
from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector

def main():
    sources_yaml_path = os.path.join(backend_dir, "config", "sources.yaml")
    with open(sources_yaml_path, "r", encoding="utf-8") as f:
        sources_data = yaml.safe_load(f)
    
    locations = sources_data.get("source", {}).get("locations", [])
    db_location = None
    for loc in locations:
        if loc.get("type") == "database":
            db_location = loc
            break
            
    if not db_location:
        return
        
    conn_cfg = db_location.get("connection", {})
    connector = AzureSQLPythonNetConnector(conn_cfg)
    
    try:
        print("=== dbo.Orders_Raw cleaned preview (first 10 rows) ===")
        print(connector.preview_table("dbo.Orders_Raw", rows=10).to_string())
        
        print("\n=== dbo.Sales_Raw cleaned preview (first 10 rows) ===")
        print(connector.preview_table("dbo.Sales_Raw", rows=10).to_string())
        
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()
