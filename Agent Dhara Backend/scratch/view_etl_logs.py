import sys
import os

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)

from dotenv import load_dotenv
load_dotenv()
from agent.azure_sql_executor import get_connection

try:
    conn = get_connection()
    cursor = conn.cursor()
    
    print("--- LATEST ETL LOG ENTRIES ---")
    cursor.execute("SELECT TOP 20 id, process_name, start_time, end_time, status, error_message FROM dbo.etl_log ORDER BY start_time DESC")
    rows = cursor.fetchall()
    for row in rows:
        print(f"ID={row[0]} | Process={row[1]} | Start={row[2]} | End={row[3]} | Status={row[4]} | Error={row[5]}")
        
    print("\n--- LATEST ETL REJECTS ---")
    cursor.execute("SELECT TOP 20 id, process_name, table_name, rejected_at, error_reason FROM dbo.etl_rejects ORDER BY rejected_at DESC")
    rejects = cursor.fetchall()
    for r in rejects:
        print(f"ID={r[0]} | Process={r[1]} | Table={r[2]} | Time={r[3]} | Reason={r[4]}")
        
    conn.close()
except Exception as e:
    print(f"Failed: {e}")
