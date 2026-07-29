import os
import sys
from dotenv import load_dotenv

# Ensure we can import modules from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load env
load_dotenv()

from agent.azure_sql_executor import get_connection

print("Connecting to database using azure_sql_executor...")
try:
    conn = get_connection()
    cursor = conn.cursor()
    print("Connected successfully.")
    
    # Check if tables exist
    tables = [r[2] for r in cursor.tables(schema="dbo").fetchall()]
    
    if "etl_log" in tables:
        print("\n--- Last 10 rows from dbo.etl_log ---")
        cursor.execute("SELECT TOP 10 id, process_name, start_time, end_time, status, error_message FROM dbo.etl_log ORDER BY start_time DESC")
        for row in cursor.fetchall():
            print(row)
            
    if "etl_rejects" in tables:
        print("\n--- Last 5 rows from dbo.etl_rejects ---")
        cursor.execute("SELECT TOP 5 id, process_name, table_name, rejected_at, error_reason FROM dbo.etl_rejects ORDER BY rejected_at DESC")
        for row in cursor.fetchall():
            print(row)
            
    conn.close()
except Exception as e:
    print("Failed to query database logs:", e)
