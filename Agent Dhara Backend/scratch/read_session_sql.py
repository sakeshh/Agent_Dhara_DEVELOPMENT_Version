import os
import sys
from dotenv import load_dotenv

backend_dir = r"c:\Users\ssakesh\Documents\DHARA-GX\Agent Dhara Backend"
dotenv_path = os.path.join(backend_dir, ".env")
load_dotenv(dotenv_path)

sys.path.append(backend_dir)
from agent.session_store import list_sessions, load_session

sessions = list_sessions()
if not sessions:
    print("No sessions found")
    sys.exit(0)

latest_sid = sessions[0]["session_id"]
print("Latest Session ID:", latest_sid)
sess = load_session(latest_sid)

context = sess.get("context", {})
etl_flow = context.get("etl_flow", {})
target_engine = etl_flow.get("target_engine")
code = etl_flow.get("code")

print("Target Engine:", target_engine)
print("Code Length:", len(code) if code else 0)

if code:
    out_path = os.path.join(backend_dir, "scratch", "session_sql_output.sql")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(code)
    print("Wrote full SQL code to:", out_path)
    
    print("\nLines containing 'Customers_Clean':")
    lines = code.splitlines()
    for idx, line in enumerate(lines):
        if "Customers_Clean" in line:
            print(f"Line {idx+1}: {line}")
else:
    print("No code generated yet in this session.")
