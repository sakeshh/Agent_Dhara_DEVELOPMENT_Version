import sqlite3
import json
import os

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(root, "output", "chat_sessions.sqlite3")
print("Connecting to:", db_path)

if not os.path.exists(db_path):
    print("Database does not exist at", db_path)
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("SELECT session_id, created_at, updated_at, payload_json FROM sessions ORDER BY updated_at DESC")
rows = cursor.fetchall()
print(f"Total sessions: {len(rows)}")
for idx, r in enumerate(rows):
    sid = r[0]
    payload = json.loads(r[3])
    flow = payload.get("context", {}).get("etl_flow", {})
    print(f"\n--- Session {idx+1} ---")
    print("Session ID:", sid)
    print("Target Engine:", flow.get("target_engine"))
    print("Codegen Engine:", flow.get("codegen_engine"))
    print("Phase:", flow.get("phase"))
    print("Validation Errors:", flow.get("validation_errors"))
    print("Plan Validation Errors:", flow.get("plan_validation_errors"))

conn.close()
