import sqlite3
import json
import os

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(root, "output", "chat_sessions.sqlite3")

print("Checking DB:", db_path)
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

rows = cursor.execute("SELECT session_id, updated_at, payload_json FROM sessions ORDER BY updated_at DESC").fetchall()
print(f"Total sessions found: {len(rows)}")

for idx, (session_id, updated_at, payload_json) in enumerate(rows):
    print(f"\n[{idx}] Session ID: {session_id} (Updated: {updated_at})")
    try:
        payload = json.loads(payload_json)
        print("  Payload keys:", list(payload.keys()))
        selected_tables = payload.get("selected_tables")
        if selected_tables:
            print("  Selected Tables:", selected_tables)
        context = payload.get("context", {})
        etl_flow = context.get("etl_flow") or payload.get("etl_flow")
        if etl_flow:
            print("  ETL Flow Phase:", etl_flow.get("phase"))
            if "approved_plan" in etl_flow:
                plan = etl_flow["approved_plan"]
                print("    Approved Plan present. Keys:", list(plan.keys()) if plan else None)
                if plan and "datasets" in plan:
                    print("    Datasets in approved plan:", list(plan["datasets"].keys()))
            if "fabric_mirror_result" in etl_flow:
                print("    Fabric Mirror Result:", etl_flow["fabric_mirror_result"])
    except Exception as e:
        print("  Error reading payload:", e)

conn.close()
