import sqlite3
import os
import sys

# Reconfigure stdout to support UTF-8 characters
sys.stdout.reconfigure(encoding='utf-8')

db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output", "jobs.sqlite3"))
print(f"Connecting to jobs DB: {db_path}...")

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get column names of jobs table
    cursor.execute("PRAGMA table_info(jobs)")
    cols = cursor.fetchall()
    print("Columns in jobs table:")
    for col in cols:
        print(f" - {col[1]} ({col[2]})")
        
    print("\n--- RECENT JOBS ---")
    col_names = [col[1] for col in cols]
    select_cols = ", ".join(col_names)
    
    cursor.execute(f"SELECT {select_cols} FROM jobs ORDER BY rowid DESC LIMIT 10")
    jobs = cursor.fetchall()
    for job in jobs:
        record = dict(zip(col_names, job))
        # Truncate large fields for display
        if "input_json" in record and record["input_json"]:
            record["input_json"] = record["input_json"][:100] + "..."
        if "result_json" in record and record["result_json"]:
            record["result_json"] = record["result_json"][:100] + "..."
        print(f"Job ID: {record['job_id']} | Status: {record['status']} | Kind: {record['kind']} | Progress: {record['progress']} | Error: {record['error']}")
        
except Exception as e:
    print("Failed to inspect jobs database:", e)
