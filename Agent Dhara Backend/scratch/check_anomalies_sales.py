import os
import sys
import yaml
import pandas as pd
from dotenv import load_dotenv

backend_dir = r"c:\Users\ssakesh\Downloads\DHARA-GX\Agent Dhara Backend"
load_dotenv(os.path.join(backend_dir, ".env"))
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
        print("No database source found in sources.yaml")
        return
        
    conn_cfg = db_location.get("connection", {})
    connector = AzureSQLPythonNetConnector(conn_cfg)
    
    df = connector.load_table("dbo.Sales_Raw")
    
    print("\n=== 4. Near Duplicate Rows (Excluding ID columns) ===")
    from rapidfuzz import fuzz
    txt_cols = [c for c in df.columns if not c.lower().endswith("id")]
    row_strings = df[txt_cols].fillna("").astype(str).agg(" | ".join, axis=1).tolist()
    near_dup_pairs = []
    for i in range(len(df)):
        for j in range(i + 1, len(df)):
            ratio = fuzz.token_sort_ratio(row_strings[i], row_strings[j])
            if ratio >= 92:
                near_dup_pairs.append((i, j, df.iloc[i].to_dict(), df.iloc[j].to_dict(), ratio))
    print(f"Total near-duplicate pairs found: {len(near_dup_pairs)}")
    for pair in near_dup_pairs[:5]:
        print(f"Similarity: {pair[4]}%")
        print(f"  Row A [Idx {pair[0]}]:", {k: v for k, v in pair[2].items() if k in txt_cols or k.endswith("ID")})
        print(f"  Row B [Idx {pair[1]}]:", {k: v for k, v in pair[3].items() if k in txt_cols or k.endswith("ID")})

if __name__ == "__main__":
    main()
