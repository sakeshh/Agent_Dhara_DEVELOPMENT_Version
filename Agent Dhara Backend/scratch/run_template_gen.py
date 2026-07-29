import sqlite3
import json
from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl

conn = sqlite3.connect("output/chat_sessions.sqlite3")
row = conn.execute("SELECT payload_json FROM sessions WHERE session_id = ?", ("a3dee8ef-0ee8-47c9-a616-d1f1a7eb333f",)).fetchone()
conn.close()

if row:
    payload = json.loads(row[0])
    ctx = payload.get("context", {})
    flow = ctx.get("etl_flow", {})
    plan = flow.get("approved_plan") or flow.get("plan") or {}
    assess = payload.get("last_assessment_result") or {}
    
    code = generate_pyspark_etl(plan, assess)
    with open("scratch/template_code.py", "w", encoding="utf-8") as f:
        f.write(code)
    print("Template generation done!")
    
    from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
    ok, errs = validate_pyspark_source(code, plan)
    print("VALIDATION:", ok, errs)
else:
    print("Session not found")
