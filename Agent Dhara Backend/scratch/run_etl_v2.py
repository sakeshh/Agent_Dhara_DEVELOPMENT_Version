import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Reconfigure stdout to support UTF-8 characters
sys.stdout.reconfigure(encoding='utf-8')

import pyodbc
from dotenv import load_dotenv
from agent.session_store import load_session, save_session
from agent.azure_sql_executor import get_connection
from agent.etl_handlers import etl_execute_sql

load_dotenv()

# Define session
session_id = "1952cb8f-1b1f-47fc-a647-9769b8742b11"
sess = load_session(session_id)
flow = sess.setdefault("context", {}).setdefault("etl_flow", {})

# 1. Inspect SQL in session
sql_code = flow.get("code")
print("Session SQL Code Length:", len(sql_code) if sql_code else 0)
has_nvarchar = "AccountType] NVARCHAR" in sql_code if sql_code else False
print("Does Session SQL have corrected AccountType type (NVARCHAR)?", has_nvarchar)

# If they don't match, or just to be safe, read generated_etl_v2.sql and update session SQL code
v2_path = os.path.join(os.path.dirname(__file__), "..", "output", "generated_etl_v2.sql")
if os.path.exists(v2_path):
    print("Reading generated_etl_v2.sql...")
    with open(v2_path, "r", encoding="utf-8") as f:
        v2_sql = f.read()
    flow["code"] = v2_sql
    save_session(sess)
    print("Updated session SQL code to match generated_etl_v2.sql.")
else:
    print("WARNING: generated_etl_v2.sql not found!")

# 2. Connect to database and drop old tables and procedures
conn = None
try:
    print("Connecting to DB to drop old tables and procedures...")
    conn = get_connection()
    cursor = conn.cursor()
    
    statements = [
        "DROP TABLE IF EXISTS dbo.Accounts_Clean;",
        "DROP TABLE IF EXISTS dbo.Accounts_Transformed;",
        "DROP PROCEDURE IF EXISTS dbo.etl_clean_Accounts;",
        "DROP PROCEDURE IF EXISTS dbo.etl_transform_Accounts;",
    ]
    
    for stmt in statements:
        try:
            print(f"Executing: {stmt}")
            cursor.execute(stmt)
        except Exception as e:
            print(f"Error executing statement '{stmt}': {e}")
            
    conn.commit()
    print("Stale tables and procedures dropped successfully.")
except Exception as e:
    print("Failed to drop tables/procedures:", e)
finally:
    if conn:
        conn.close()

# 3. Execute the ETL pipeline using the official handler
print("\nInvoking etl_execute_sql...")
try:
    res = etl_execute_sql(session_id, approved=True, dry_run=False)
    print("ETL Execution Result:")
    import json
    print(json.dumps(res, indent=2))
except Exception as e:
    print("ETL execution handler failed:", e)
