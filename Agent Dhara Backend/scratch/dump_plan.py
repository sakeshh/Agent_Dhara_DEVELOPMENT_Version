import sqlite3
import json
import os

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(root, "output", "chat_sessions.sqlite3")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()
row = cursor.execute("SELECT payload_json FROM sessions WHERE session_id = '1952cb8f-1b1f-47fc-a647-9769b8742b11'").fetchone()
conn.close()

if row:
    payload = json.loads(row[0])
    etl_flow = payload.get("context", {}).get("etl_flow", {})
    plan = etl_flow.get("plan", {})
    
    out_path = os.path.join(root, "scratch", "plan_dump.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2)
    print(f"Dumped plan to {out_path}")
else:
    print("Session not found")
