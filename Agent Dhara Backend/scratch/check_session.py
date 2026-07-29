import sqlite3
import json
import os

db_path = r"c:\Users\ssakesh\Desktop\python\DHARA-GX\Agent Dhara Backend\output\chat_sessions.sqlite3"
conn = sqlite3.connect(db_path)
row = conn.execute("SELECT payload_json FROM sessions WHERE session_id = 'a3dee8ef-0ee8-47c9-a616-d1f1a7eb333f'").fetchone()
if row:
    payload = json.loads(row[0])
    flow = payload.get("context", {}).get("etl_flow", {})
    print("validation_ok:", flow.get("plan_validation_ok"))
    print("plan_validation_errors:", flow.get("plan_validation_errors"))
    print("target_engine:", flow.get("target_engine"))
    print("validation_errors (code):", flow.get("validation_errors"))
    print("validation_ok (code):", flow.get("validation_ok"))
else:
    print("Session not found")
