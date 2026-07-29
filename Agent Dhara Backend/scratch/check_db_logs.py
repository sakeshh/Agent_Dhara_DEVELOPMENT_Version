import os
import sys
from dotenv import load_dotenv
import pyodbc

# Load env
load_dotenv()

server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")

conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"

print("Connecting to database...")
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Connected successfully.")
    
    # Check if etl_log table exists
    tables = [r[2] for r in cursor.tables(schema="dbo").fetchall()]
    if "etl_log" in tables:
        print("\n--- Last 5 rows from dbo.etl_log ---")
        cursor.execute("SELECT TOP 5 id, process_name, start_time, end_time, status, SUBSTRING(error_message, 1, 200) FROM dbo.etl_log ORDER BY start_time DESC")
        for row in cursor.fetchall():
            print(row)
    else:
        print("\nTable dbo.etl_log does not exist.")
        
    if "etl_rejects" in tables:
        print("\n--- Last 5 rows from dbo.etl_rejects ---")
        cursor.execute("SELECT TOP 5 id, process_name, table_name, rejected_at, SUBSTRING(error_reason, 1, 200) FROM dbo.etl_rejects ORDER BY rejected_at DESC")
        for row in cursor.fetchall():
            print(row)
    else:
        print("\nTable dbo.etl_rejects does not exist.")
        
    conn.close()
except Exception as e:
    print("Database connection or query failed:", e)
