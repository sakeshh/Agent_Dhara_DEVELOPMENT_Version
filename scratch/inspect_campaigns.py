import os
import pandas as pd
import json
import xml.etree.ElementTree as ET

# Define paths
cache_dir = r"c:\Users\ssakesh\OneDrive - Capgemini\Desktop\python\DHARA-GX\Agent Dhara Backend\agent\data\cache"
marketing_path = os.path.join(cache_dir, "78929749b88ea786936c1a8dce8fe29d_marketing_relational_test.csv")
sales_path = os.path.join(cache_dir, "0216a49eee85e911b9d961d360f2439d_sales_relational_test.xml")

# Load marketing
m_df = pd.read_csv(marketing_path)
marketing_campaigns = set(m_df["campaign_id"].dropna().astype(str).str.strip())
print("Marketing campaigns:", len(marketing_campaigns))

# Load sales XML
tree = ET.parse(sales_path)
root = tree.getroot()
sales_rows = []
for record in root.findall(".//record") or root.findall(".//row") or root.findall(".//*"):
    # If the tag has child elements
    row = {}
    for child in record:
        row[child.tag] = child.text
    if row:
        sales_rows.append(row)

s_df = pd.DataFrame(sales_rows)
print("Sales raw rows count:", len(s_df))
if "campaign_id" in s_df.columns:
    # Print some stats
    s_df["campaign_id_clean"] = s_df["campaign_id"].astype(str).str.strip()
    null_count = s_df[s_df["campaign_id"].isna() | (s_df["campaign_id_clean"] == "") | (s_df["campaign_id_clean"].str.lower() == "nan") | (s_df["campaign_id_clean"].str.lower() == "none") | (s_df["campaign_id_clean"].str.lower() == "n/a")].shape[0]
    print("Null/placeholder campaign_id count:", null_count)
    
    # We strip and clean placeholders: UNKNOWN, N/A, etc.
    # What are the unique campaign_ids?
    print("Unique campaign_ids:", s_df["campaign_id"].nunique())
