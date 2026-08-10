import os
import sys
import re

backend_dir = r"c:\Users\ssakesh\Documents\DHARA-GX\Agent Dhara Backend"
sql_path = os.path.join(backend_dir, "scratch", "session_sql_output.sql")
with open(sql_path, "r", encoding="utf-8") as f:
    sql = f.read()

pattern = r"(?i)^\s*GO\s*(?:--.*)?$"
batches = re.split(pattern, sql, flags=re.MULTILINE)
result_batches = [b.strip() for b in batches if b.strip()]

print("Total Batches:", len(result_batches))
if len(result_batches) >= 15:
    print("\n----- BATCH 15 START -----")
    print(result_batches[14])
    print("----- BATCH 15 END -----")
else:
    print("Fewer than 15 batches found")
