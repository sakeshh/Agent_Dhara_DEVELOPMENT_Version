import sqlite3
import os
import json
import sys

sys.stdout.reconfigure(encoding='utf-8')

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root not in sys.path:
    sys.path.insert(0, root)
db_path = os.path.join(root, "output", "chat_sessions.sqlite3")

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("SELECT session_id, payload_json FROM sessions ORDER BY updated_at DESC LIMIT 1")
row = cursor.fetchone()

payload = json.loads(row['payload_json'])
ctx = payload.get("context") or {}
flow = ctx.get("etl_flow") or {}

sql = flow.get("code")
if sql:
    print(sql)
