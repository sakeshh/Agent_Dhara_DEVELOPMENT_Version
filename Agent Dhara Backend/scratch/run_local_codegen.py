import sqlite3
import json
import os
import sys

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)

db_path = os.path.join(root, "output", "chat_sessions.sqlite3")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("SELECT payload_json FROM sessions WHERE session_id = 'a3447b6b-1763-4282-82c5-73b5a32119fc'")
row = cursor.fetchone()
conn.close()

if not row:
    print("Session not found!")
    sys.exit(1)

payload = json.loads(row[0])
ctx = payload.get("context", {})
flow = ctx.get("etl_flow", {})
plan = flow.get("approved_plan", {})
assess = flow.get("last_assessment_result", {})

# Let's inspect generation_mode
generation_mode = plan.get("generation_mode", "full")
print(f"Plan Generation Mode: {generation_mode}")

# Let's call generate_sql_etl and see what it outputs
from agent.etl_pipeline.sql_codegen import generate_sql_etl

# We will intercept print statements or debug output by running generate_sql_etl
print("\n--- Running generate_sql_etl ---")
sql_code = generate_sql_etl(plan, assess, dialect="tsql")

# Find all occurrences of CREATE PROCEDURE
print("\nGenerated procedures:")
for line in sql_code.splitlines():
    if "CREATE PROCEDURE" in line:
        print(" ", line.strip())

print("\nLength of generated SQL:", len(sql_code))
