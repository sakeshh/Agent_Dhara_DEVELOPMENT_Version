import sqlite3
import json
import os
import sys

# Add parent path to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.session_store import _db_path

print("DB Path:", _db_path())
if not os.path.exists(_db_path()):
    print("Database file does not exist!")
    sys.exit(0)

conn = sqlite3.connect(_db_path())
rows = conn.execute("SELECT session_id, payload_json FROM sessions ORDER BY updated_at DESC").fetchall()
print(f"Found {len(rows)} sessions:")
for r in rows:
    sid = r[0]
    payload = json.loads(r[1])
    print(f"\nSession ID: {sid}")
    ctx = payload.get("context", {})
    pending_rules = ctx.get("pending_business_rules")
    if pending_rules:
        print("  Pending Business Rules:")
        print("    never_drop_rows:", pending_rules.get("never_drop_rows"))
        print("    custom_assertions:", pending_rules.get("custom_assertions"))
        print("    notes:", repr(pending_rules.get("notes")))
    else:
        print("  No pending business rules in context.")
conn.close()
