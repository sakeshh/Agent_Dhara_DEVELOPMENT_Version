import sqlite3
import json

conn = sqlite3.connect("output/chat_sessions.sqlite3")
row = conn.execute("SELECT payload_json FROM sessions WHERE session_id = ?", ("a3dee8ef-0ee8-47c9-a616-d1f1a7eb333f",)).fetchone()
if row:
    payload = json.loads(row[0])
    print("PAYLOAD KEYS:", list(payload.keys()))
    flow = payload.get("etl_flow", {})
    print("FLOW KEYS:", list(flow.keys()))
    plan = flow.get("approved_plan") or flow.get("plan") or {}
    print("PLAN KEYS:", list(plan.keys()))
    if "datasets" in plan:
        print("PLAN DATASETS:", list(plan["datasets"].keys()))
        for ds_name, ds in plan["datasets"].items():
            print(f"\nDataset: {ds_name}")
            for step in ds.get("steps", []):
                print(f"  Step: {step.get('order')}. col={step.get('column')} action={step.get('action')} params={step.get('params')}")
else:
    print("Session not found")
conn.close()
