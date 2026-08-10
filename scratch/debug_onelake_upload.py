import os
from dotenv import load_dotenv
load_dotenv()

from connectors.fabric_lakehouse_connector import get_lakehouse_folder
from agent.fabric_shortcut_service import _clean_env_value

workspace = _clean_env_value(os.getenv("FABRIC_WORKSPACE_ID"))
lakehouse = _clean_env_value(os.getenv("FABRIC_LAKEHOUSE_NAME") or os.getenv("FABRIC_LAKEHOUSE_ID"))
lakehouse_folder = get_lakehouse_folder(lakehouse)

print("Environment Variables:")
print(f"  FABRIC_WORKSPACE_ID: {workspace}")
print(f"  FABRIC_LAKEHOUSE_NAME: {os.getenv('FABRIC_LAKEHOUSE_NAME')}")
print(f"  FABRIC_LAKEHOUSE_ID: {os.getenv('FABRIC_LAKEHOUSE_ID')}")
print(f"  lakehouse_folder: {lakehouse_folder}")

shortcut_name = "data_quality_issues_csv"
ext = ".csv"
files_zone_path = f"Files/raw/{shortcut_name}{ext}"
encoded_dest = "/".join(files_zone_path.split("/"))

upload_url = f"https://onelake.dfs.fabric.microsoft.com/{workspace}/{lakehouse_folder}/{encoded_dest}"
print(f"\nUpload URL: {upload_url}")

# Let's check the mount path used in spark read:
print(f"\nSpark read path: /lakehouse/default/{files_zone_path}")
