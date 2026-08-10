import sys
import os
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.azure_sql_executor import get_connection

def main():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        print("Connected successfully!")
        
        # 1. Check current database and schema context
        cursor.execute("SELECT DB_NAME(), SCHEMA_NAME()")
        db, schema = cursor.fetchone()
        print(f"Current DB: {db}, Default Schema: {schema}")
        
        # 2. List all user tables matching *Clean* or *Accounts* or *Citizens*
        cursor.execute("""
            SELECT SCHEMA_NAME(schema_id) AS schema_name, name 
            FROM sys.tables 
            WHERE name LIKE '%Clean%' OR name LIKE '%Accounts%' OR name LIKE '%Citizens%'
        """)
        rows = cursor.fetchall()
        print("\nMatching Tables:")
        for r in rows:
            print(f"- {r[0]}.{r[1]}")
            
        # 3. List all procedures matching *etl*
        cursor.execute("""
            SELECT SCHEMA_NAME(schema_id) AS schema_name, name 
            FROM sys.procedures 
            WHERE name LIKE '%etl%'
        """)
        rows = cursor.fetchall()
        print("\nProcedures in DB:")
        for r in rows:
            print(f"- {r[0]}.{r[1]}")
            
        # 4. Check contents of etl_log
        try:
            cursor.execute("SELECT TOP 10 id, process_name, start_time, end_time, status, error_message FROM dbo.etl_log ORDER BY start_time DESC")
            logs = cursor.fetchall()
            print("\nRecent etl_log rows:")
            for l in logs:
                print(f"ID: {l[0]} | Process: {l[1]} | Started: {l[2]} | Ended: {l[3]} | Status: {l[4]} | Error: {l[5]}")
        except Exception as e:
            print("Failed to query etl_log:", e)
            
        # 5. Check if there are any specific errors in etl_rejects or etl_invalid_values
        for tbl in ["dbo.etl_rejects", "dbo.etl_invalid_values"]:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {tbl}")
                count = cursor.fetchone()[0]
                print(f"Row count in {tbl}: {count}")
            except Exception as e:
                print(f"Failed to query {tbl}: {e}")
                
        conn.close()
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()
