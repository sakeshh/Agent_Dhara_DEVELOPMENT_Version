from agent.session_store import load_session
import json

session_id = 'a3dee8ef-0ee8-47c9-a616-d1f1a7eb333f'
sess = load_session(session_id)
ctx = sess.get('context', {})
assess = ctx.get('last_assessment_result', {})
datasets = assess.get('datasets', {})
for ds_name, ds_info in datasets.items():
    print(f"Dataset: {ds_name}")
    print("Columns:", list(ds_info.get('columns', {}).keys()))
    print("Profile keys:", list(ds_info.keys()))
