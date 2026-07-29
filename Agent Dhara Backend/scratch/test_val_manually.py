import sqlite3
import json
from agent.etl_pipeline.validate_pyspark import validate_pyspark_source

conn = sqlite3.connect("output/chat_sessions.sqlite3")
row = conn.execute("SELECT payload_json FROM sessions WHERE session_id = ?", ("a3dee8ef-0ee8-47c9-a616-d1f1a7eb333f",)).fetchone()
conn.close()

if row:
    payload = json.loads(row[0])
    ctx = payload.get("context", {})
    flow = ctx.get("etl_flow", {})
    plan = flow.get("approved_plan") or flow.get("plan") or {}
    
    with open("scratch/active_code.py", "r", encoding="utf-8") as f:
        code = f.read()
        
    ok, errs = validate_pyspark_source(code, plan)
    print("MANUAL VALIDATION:", ok, errs)
else:
    print("Session not found")
