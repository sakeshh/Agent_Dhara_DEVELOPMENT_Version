import sqlite3
import os
import json
import sys

sys.stdout.reconfigure(encoding='utf-8')

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(root, "output", "chat_sessions.sqlite3")

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Find the most recently updated session
cursor.execute("SELECT session_id, payload_json FROM sessions ORDER BY updated_at DESC LIMIT 5")
rows = cursor.fetchall()

for idx, r in enumerate(rows, 1):
    payload = json.loads(r['payload_json'])
    ctx = payload.get("context") or {}
    flow = ctx.get("etl_flow") or {}
    exec_res = flow.get("sql_execution_result") or {}
    
    print(f"\n=== Session {idx}: {r['session_id']} ===")
    print(f"Target Engine: {flow.get('target_engine')}")
    print(f"SQL execution result keys: {list(exec_res.keys())}")
    print(f"SQL execution result ok: {exec_res.get('ok')}")
    print(f"Error: {exec_res.get('error')}")
    print(f"Message: {exec_res.get('message')}")
    
    execution_dict = exec_res.get("execution") or {}
    print(f"Execution keys: {list(execution_dict.keys())}")
    print(f"Execution ok: {execution_dict.get('ok')}")
    print(f"Execution error: {execution_dict.get('error')}")
    print(f"Execution rollback_reason: {execution_dict.get('rollback_reason')}")
    
    batches = execution_dict.get("batch_results") or []
    print(f"Batches: {len(batches)}")
    for b_idx, b in enumerate(batches, 1):
        print(f"  Batch {b_idx} ok: {b.get('error') is None}")
        if b.get("error"):
            print(f"  Batch {b_idx} Error: {b.get('error')}")
        if b.get("messages"):
            print(f"  Batch {b_idx} Messages: {b.get('messages')}")
            
    summary = exec_res.get("post_execution_summary") or {}
    print(f"Summary rollback reason: {summary.get('rollback_reason')}")
    
    batches_summary = summary.get("batch_results") or []
    for b_idx, b in enumerate(batches_summary, 1):
        print(f"  Summary Batch {b_idx} Ok: {b.get('error') is None}")
        if b.get("error"):
            print(f"  Summary Batch {b_idx} Error: {b.get('error')}")
            
    if flow.get("code") and exec_res.get("ok") is False:
        print("\n--- SQL Code ---")
        print(flow.get("code")[:2000])
        print("----------------")
