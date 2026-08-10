import sqlite3
import json
import os
import sys

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)

db_path = os.path.join(root, "output", "chat_sessions.sqlite3")

conn = sqlite3.connect(db_path)
try:
    row = conn.execute("SELECT payload_json FROM sessions ORDER BY updated_at DESC LIMIT 1").fetchone()
    payload = json.loads(row[0])
    flow = payload.get("context", {}).get("etl_flow", {}) or {}
    
    # We want to print key fields of flow, excluding the huge code block if it is too long, but print its length
    print(f"Session ID: {payload.get('session_id')}")
    print(f"Phase: {flow.get('phase')}")
    print(f"Target Engine: {flow.get('target_engine')}")
    print(f"Validation OK: {flow.get('validation_ok')}")
    print(f"Generation Mode: {flow.get('etl_intent', {}).get('generation_mode')}")
    print(f"Target Destination: {flow.get('etl_intent', {}).get('target_destination')}")
    print(f"Target Path: {flow.get('etl_intent', {}).get('target_path')}")
    
    print("\n--- Approved Plan Keys ---")
    plan = flow.get("approved_plan") or {}
    print(f"Plan ID: {plan.get('plan_id')}")
    print(f"Datasets: {list(plan.get('datasets', {}).keys())}")
    
    # Check if there is an execution result
    exec_res = flow.get("sql_execution_result") or {}
    print("\n--- SQL Execution Result ---")
    for k, v in exec_res.items():
        if k != 'execution_log': # Skip potentially long logs for now
            print(f"{k}: {v}")
            
    print("\n--- Fabric Mirror Result ---")
    print(json.dumps(flow.get("fabric_mirror_result"), indent=2))
finally:
    conn.close()
