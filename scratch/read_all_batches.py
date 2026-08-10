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
for idx, b in enumerate(result_batches):
    first_lines = [l for l in b.splitlines() if l.strip()][:5]
    print(f"\n--- BATCH {idx+1} ---")
    for l in first_lines:
        print(" ", l)
