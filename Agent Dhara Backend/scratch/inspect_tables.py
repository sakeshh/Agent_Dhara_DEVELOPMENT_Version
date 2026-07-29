import sqlite3
import json
import os
import sys

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)

db_path = os.path.join(root, "output", "chat_sessions.sqlite3")

conn = sqlite3.connect(db_path)
try:
    row = conn.execute("SELECT payload_json FROM sessions ORDER BY updated_at DESC LIMIT 1").fetchone()
    payload = json.loads(row[0])
    flow = payload.get("context", {}).get("etl_flow", {}) or {}
    print("\n--- TARGET TABLES IN PLAN ---")
    print(list(flow.get("approved_plan", {}).get("datasets", {}).keys()))
finally:
    conn.close()

# Also try connecting to Azure SQL if possible to see what tables exist
try:
    from dotenv import load_dotenv
    load_dotenv()
    from agent.azure_sql_executor import get_connection
    print("\n--- AZURE SQL TABLES ---")
    sql_conn = get_connection()
    cursor = sql_conn.cursor()
    cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES")
    tables = cursor.fetchall()
    for schema, name in tables:
        # Get row count for each table
        try:
            cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{name}]")
            cnt = cursor.fetchone()[0]
            print(f"[{schema}].[{name}]: {cnt} rows")
        except Exception as e:
            print(f"[{schema}].[{name}]: Error counting: {e}")
    sql_conn.close()
except Exception as e:
    print(f"Failed to query Azure SQL: {e}")
