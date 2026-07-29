import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Reconfigure stdout to support UTF-8 characters
sys.stdout.reconfigure(encoding='utf-8')

from agent.session_store import load_session

session_id = "1952cb8f-1b1f-47fc-a647-9769b8742b11"
sess = load_session(session_id)
ctx = sess.get("context", {})

print(f"--- Session '{session_id}' Context Keys ---")
for k, v in ctx.items():
    if k == "last_assessment_result":
        print(f" - {k}: <dict with keys: {list(v.keys()) if isinstance(v, dict) else type(v)}>")
        if isinstance(v, dict) and 'datasets' in v:
            print(f"   last_assessment_result['datasets']: {list(v['datasets'].keys())}")
    elif k == "etl_flow":
        print(f" - {k}: <dict with keys: {list(v.keys()) if isinstance(v, dict) else type(v)}>")
    else:
        print(f" - {k}: {v}")
