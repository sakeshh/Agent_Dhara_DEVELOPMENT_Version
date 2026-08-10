import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from agent.session_store import load_session

session_id = "1952cb8f-1b1f-47fc-a647-9769b8742b11"
sess = load_session(session_id)
flow = sess.get("context", {}).get("etl_flow", {})

mirror_result = flow.get("fabric_mirror_result")
print("--- Fabric Lakehouse Mirror Result ---")
if mirror_result:
    import json
    print(json.dumps(mirror_result, indent=2))
else:
    print("No fabric_mirror_result found in session context.")
