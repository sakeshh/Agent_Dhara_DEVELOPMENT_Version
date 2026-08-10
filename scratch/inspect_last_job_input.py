import sqlite3
import os
import sys
import json

db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output", "jobs.sqlite3"))
try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT job_id, input_json 
        FROM jobs 
        WHERE status = 'succeeded' AND kind = 'assess'
        ORDER BY rowid DESC 
        LIMIT 1
    """)
    row = cursor.fetchone()
    if row:
        job_id, input_json = row
        print(f"Job ID: {job_id}")
        print("Input JSON:")
        print(json.dumps(json.loads(input_json), indent=2))
    else:
        print("No succeeded assess jobs found.")
except Exception as e:
    print("Error:", e)
