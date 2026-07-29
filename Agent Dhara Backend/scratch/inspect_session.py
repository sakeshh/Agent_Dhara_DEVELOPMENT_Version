import json
from agent.session_store import load_session

session_id = 'a3dee8ef-0ee8-47c9-a616-d1f1a7eb333f'
sess = load_session(session_id)
ctx = sess.get('context', {})

manifest = ctx.get('connector_manifest', {})
print("--- CONNECTOR MANIFEST ---")
print(json.dumps(manifest, indent=2))

print("\n--- LAST ASSESSMENT RESULT ---")
assess = ctx.get('last_assessment_result', {})
print("Keys in assessment:", list(assess.keys()))
if 'datasets' in assess:
    print("Assessment datasets:", list(assess['datasets'].keys()))
