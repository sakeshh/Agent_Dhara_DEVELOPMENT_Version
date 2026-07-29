import sqlite3
import json
import os

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(root, "output", "chat_sessions.sqlite3")

if not os.path.exists(db_path):
    print(f"Database not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
try:
    rows = conn.execute("SELECT session_id, updated_at, payload_json FROM sessions ORDER BY updated_at DESC").fetchall()
    print(f"Found {len(rows)} sessions.")
    for idx, (sid, updated_at, payload_json) in enumerate(rows[:5]):
        print(f"\n--- Session #{idx+1}: {sid} (updated_at: {updated_at}) ---")
        try:
            payload = json.loads(payload_json)
            context = payload.get("context", {}) or {}
            flow = context.get("etl_flow", {}) or {}
            
            # Print state/phase
            print(f"Phase: {flow.get('phase')}")
            print(f"Target Engine: {flow.get('target_engine')}")
            print(f"Validation OK: {flow.get('validation_ok')}")
            
            # Print SQL execution result if exists
            exec_res = flow.get("sql_execution_result")
            if exec_res:
                print(f"SQL execution OK: {exec_res.get('ok')}")
                print(f"SQL execution message: {exec_res.get('message')}")
                print(f"SQL execution error: {exec_res.get('error')}")
                print(f"SQL execution status code: {exec_res.get('status_code')}")
                print(f"SQL execution summary: {exec_res.get('post_execution_summary')}")
            else:
                print("No SQL execution result found.")
                
            # Print fabric mirror result
            fabric_res = flow.get("fabric_mirror_result")
            if fabric_res:
                print(f"Fabric mirror OK: {fabric_res.get('ok')}")
                print(f"Fabric mirror details: {json.dumps(fabric_res, indent=2)}")
            else:
                print("No fabric mirror result found in session payload.")
                
        except Exception as e:
            print(f"Error parsing payload: {e}")
finally:
    conn.close()
