import sqlite3
import json
import os

db_path = r"c:\Users\ssakesh\Desktop\python\DHARA-GX\Agent Dhara Backend\output\chat_sessions.sqlite3"

if not os.path.exists(db_path):
    print("Database does not exist at:", db_path)
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Query sessions
cursor.execute("SELECT session_id, updated_at, payload_json FROM sessions ORDER BY updated_at DESC LIMIT 5")
rows = cursor.fetchall()

for row in rows:
    session_id, updated_at, data_str = row
    print(f"\n--- Session ID: {session_id} | Updated At: {updated_at} ---")
    data = json.loads(data_str)
    
    # Check etl_flow
    ctx = data.get("context", {})
    flow = ctx.get("etl_flow", {})
    if flow:
        print("ETL Flow Phase:", flow.get("phase"))
        print("Validation Ok:", flow.get("validation_ok"))
        print("Validation Errors:", flow.get("validation_errors"))
        print("Target Engine:", flow.get("target_engine"))
        
        plan = flow.get("approved_plan") or flow.get("plan") or {}
        datasets = plan.get("datasets", {})
        print("Plan Datasets:", list(datasets.keys()))
        for ds, val in datasets.items():
            steps = val.get("steps", [])
            print(f"  {ds} Steps:")
            for s in steps:
                print(f"    - Action: {s.get('action')}, Col: {s.get('column')}, Order: {s.get('order')}")
                
conn.close()
