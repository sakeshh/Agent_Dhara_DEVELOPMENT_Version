import sqlite3
import os
import sys
import json

# Reconfigure stdout to support UTF-8 characters
sys.stdout.reconfigure(encoding='utf-8')

db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output", "jobs.sqlite3"))
print(f"Connecting to jobs DB: {db_path}...")

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT job_id, status, kind, result_json, error 
        FROM jobs 
        WHERE status = 'succeeded' AND kind = 'assess'
        ORDER BY rowid DESC 
        LIMIT 1
    """)
    row = cursor.fetchone()
    if not row:
        print("No succeeded assess jobs found.")
    else:
        job_id, status, kind, result_json, error = row
        print(f"Last Job ID: {job_id} | Status: {status} | Kind: {kind} | Error: {error}")
        if result_json:
            result = json.loads(result_json)
            print("\nResult Keys:", list(result.keys()))
            
            # Print structure of 'result'
            if 'result' in result:
                sub_res = result['result']
                print("Result['result'] type:", type(sub_res))
                if isinstance(sub_res, dict):
                    print("Result['result'] keys:", list(sub_res.keys()))
                    if 'datasets' in sub_res:
                        print("Result['result']['datasets'] keys:", list(sub_res['datasets'].keys()))
                        # Print sample dataset metadata
                        for k, v in list(sub_res['datasets'].items())[:1]:
                            print(f"Sample dataset '{k}' keys:", list(v.keys()) if isinstance(v, dict) else type(v))
            else:
                print("Result has no 'result' key.")
                if 'datasets' in result:
                    print("Result['datasets'] keys:", list(result['datasets'].keys()))
                    for k, v in list(result['datasets'].items())[:1]:
                        print(f"Sample dataset '{k}' keys:", list(v.keys()) if isinstance(v, dict) else type(v))
        else:
            print("result_json is empty/null.")
            
except Exception as e:
    print("Failed to inspect jobs:", e)
