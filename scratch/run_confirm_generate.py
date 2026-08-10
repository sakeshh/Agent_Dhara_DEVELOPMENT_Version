from agent.session_store import load_session
from agent.etl_handlers import etl_confirm_plan, etl_generate_code
import os
import sqlite3
import json

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(root, "output", "chat_sessions.sqlite3")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()
session_id = "1952cb8f-1b1f-47fc-a647-9769b8742b11"
print(f"Testing session: {session_id}")

# 1. Run confirm plan
confirm_res = etl_confirm_plan(session_id)
print("\n--- Confirm Plan Result ---")
print("OK:", confirm_res.get("ok"))
print("Error:", confirm_res.get("error"))
print("Message:", confirm_res.get("message"))
print("Plan Validation Errors:", confirm_res.get("plan_validation_errors"))

# 2. Run generate code regardless of confirm result (since it might already be confirmed/ready)
print("\n--- Generating Code ---")
gen_res = etl_generate_code(session_id, engine="sql", sql_dialect="tsql", codegen_mode="template", generation_mode="full")
print("Generate OK:", gen_res.get("ok"))
print("Generate Error:", gen_res.get("error"))
print("Generate Message:", gen_res.get("message"))
print("Validation OK:", gen_res.get("validation_ok"))
print("Validation Errors:", gen_res.get("validation_errors"))
if gen_res.get("code"):
    print("Generated Code Length:", len(gen_res.get("code", "")))
    # Save generated code to scratch
    out_path = os.path.join(root, "scratch", "current_session_sql.sql")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(gen_res.get("code", ""))
    print("Saved generated code to:", out_path)
conn.close()
