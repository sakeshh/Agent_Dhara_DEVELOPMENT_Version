import sqlite3
import json
import os

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(root, "output", "chat_sessions.sqlite3")

conn = sqlite3.connect(db_path)
try:
    row = conn.execute("SELECT payload_json FROM sessions ORDER BY updated_at DESC LIMIT 1").fetchone()
    payload = json.loads(row[0])
    flow = payload.get("context", {}).get("etl_flow", {}) or {}
    code = flow.get("code") or ""
    
    lines = code.split("\n")
    print(f"Total lines of SQL: {len(lines)}")
    print("\n--- FIRST 50 LINES ---")
    for i, line in enumerate(lines[:50]):
        print(f"{i+1:03d}: {line}")
        
    print("\n--- LAST 50 LINES ---")
    for i, line in enumerate(lines[-50:]):
        print(f"{len(lines)-50+i+1:03d}: {line}")
finally:
    conn.close()
