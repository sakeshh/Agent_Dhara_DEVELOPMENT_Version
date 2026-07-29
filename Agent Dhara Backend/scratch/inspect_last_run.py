import sqlite3
import os
import json
import sys

sys.stdout.reconfigure(encoding='utf-8')

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(root, "output", "chat_sessions.sqlite3")

print(f"Connecting to: {db_path}")
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

cursor = conn.cursor()
cursor.execute("SELECT session_id, created_at, updated_at, payload_json FROM sessions ORDER BY updated_at DESC LIMIT 5")
rows = cursor.fetchall()

if not rows:
    print("No sessions found.")
else:
    for idx, r in enumerate(rows, 1):
        print(f"\n=== Session {idx} ===")
        print(f"Session ID: {r['session_id']}")
        print(f"Created At: {r['created_at']}")
        print(f"Updated At: {r['updated_at']}")
        
        payload = json.loads(r['payload_json'])
        # Check execution_result
        exec_res = payload.get("execution_result") or {}
        print(f"Target Engine: {payload.get('selected_engine')}")
        print(f"Phase: {payload.get('current_phase')}")
        print(f"Execution Result OK: {exec_res.get('ok')}")
        print(f"Execution Result Error: {exec_res.get('error')}")
        print(f"Execution Result Rollback Reason: {exec_res.get('rollback_reason')}")
        
        # Let's print batch results errors if any
        batch_res = exec_res.get("batch_results") or []
        print(f"Batches run: {len(batch_res)}")
        for b_idx, b in enumerate(batch_res, 1):
            if b.get("error"):
                print(f"  Batch {b_idx} Error: {b.get('error')}")
            elif b.get("messages"):
                print(f"  Batch {b_idx} Messages: {b.get('messages')[:3]}")
