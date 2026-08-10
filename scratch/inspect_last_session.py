import sqlite3
import json
import os

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(root, "output", "chat_sessions.sqlite3")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

session_id = "1952cb8f-1b1f-47fc-a647-9769b8742b11"
row = cursor.execute("SELECT payload_json FROM sessions WHERE session_id = ?", (session_id,)).fetchone()
if row:
    payload = json.loads(row[0])
    context = payload.get("context", {})
    etl_flow = context.get("etl_flow", {})
    
    plan = etl_flow.get("plan")
    print("Type of etl_flow.plan:", type(plan))
    if plan:
        print("Keys of etl_flow.plan:", list(plan.keys()))
        if "datasets" in plan:
            print("Datasets in etl_flow.plan:", list(plan["datasets"].keys()))
        else:
            print("No datasets key in plan!")
    else:
        print("etl_flow.plan is empty/None!")

else:
    print("Session not found!")

conn.close()
