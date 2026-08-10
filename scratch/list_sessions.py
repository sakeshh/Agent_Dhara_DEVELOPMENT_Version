import sqlite3
conn = sqlite3.connect("output/chat_sessions.sqlite3")
rows = conn.execute("SELECT session_id, updated_at FROM sessions ORDER BY updated_at DESC LIMIT 10").fetchall()
for r in rows:
    print(f"Session: {r[0]} updated_at={r[1]}")
conn.close()
