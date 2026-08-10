import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path so we can import agent modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.azure_sql_executor import get_connection

def list_tables():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        print("Connected successfully!")
        cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES")
        rows = cursor.fetchall()
        print("\nTables in database:")
        for r in rows:
            print(f"- {r[0]}.{r[1]}")
            
        cursor.execute("SELECT COUNT(*) FROM dbo.etl_log")
        log_count = cursor.fetchone()[0]
        print(f"\nTotal etl_log rows: {log_count}")
        
        cursor.execute("SELECT TOP 5 id, process_name, start_time, status, error_message FROM dbo.etl_log ORDER BY start_time DESC")
        logs = cursor.fetchall()
        print("\nRecent ETL Log Entries:")
        for l in logs:
            print(f"ID: {l[0]} | Process: {l[1]} | Started: {l[2]} | Status: {l[3]} | Error: {l[4]}")
            
        conn.close()
    except Exception as e:
        print(f"Error checking database: {e}", file=sys.stderr)

if __name__ == "__main__":
    list_tables()
