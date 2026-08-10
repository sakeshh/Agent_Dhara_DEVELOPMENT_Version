import sqlite3
import json

db_path = r"c:\Users\ssakesh\Desktop\python\DHARA-GX\Agent Dhara Backend\output\chat_sessions.sqlite3"
conn = sqlite3.connect(db_path)
row = conn.execute("SELECT payload_json FROM sessions WHERE session_id = 'a3dee8ef-0ee8-47c9-a616-d1f1a7eb333f'").fetchone()
if row:
    payload = json.loads(row[0])
    plan = payload.get("context", {}).get("etl_flow", {}).get("plan", {})
    print("Plan ID:", plan.get("plan_id"))
    print("Datasets:", list(plan.get("datasets", {}).keys()))
    for ds_name, ds_data in plan.get("datasets", {}).items():
        print(f"Dataset {ds_name} steps:")
        for step in ds_data.get("steps", []):
            print(f"  Col: {step.get('column')}, Action: {step.get('action')}, Order: {step.get('order')}")
else:
    print("Session not found")
