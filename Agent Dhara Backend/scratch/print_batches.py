import sqlite3
import os
import json
import sys

sys.stdout.reconfigure(encoding='utf-8')

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root not in sys.path:
    sys.path.insert(0, root)
db_path = os.path.join(root, "output", "chat_sessions.sqlite3")

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("SELECT session_id, payload_json FROM sessions ORDER BY updated_at DESC LIMIT 1")
row = cursor.fetchone()

payload = json.loads(row['payload_json'])
ctx = payload.get("context") or {}
flow = ctx.get("etl_flow") or {}
exec_res = flow.get("sql_execution_result") or {}
execution_dict = exec_res.get("execution") or {}
batches = execution_dict.get("batch_results") or []

print(f"Total batch results: {len(batches)}")
for idx, b in enumerate(batches):
    print(f"\n--- Batch {idx + 1} ---")
    print(f"Rows Affected: {b.get('rows_affected')}")
    print(f"Error: {b.get('error')}")
    print(f"Messages: {b.get('messages')}")
    # Print the sql text corresponding to this batch
    from agent.azure_sql_executor import _split_sql_batches
    sql_batches = _split_sql_batches(flow.get("code"))
    if idx < len(sql_batches):
        print(f"SQL Snippet:\n{sql_batches[idx][:500]}")
