import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

server = os.environ.get("AZURE_SQL_SERVER")
if server and not server.endswith(".database.windows.net") and "localhost" not in server and "127.0.0.1" not in server:
    server += ".database.windows.net"
database = os.environ.get("AZURE_SQL_DATABASE")
username = os.environ.get("AZURE_SQL_USERNAME")
password = os.environ.get("AZURE_SQL_PASSWORD")

driver = "ODBC Driver 17 for SQL Server"
try:
    drivers = pyodbc.drivers()
    sql_drivers = [d for d in drivers if "sql server" in d.lower() or "odbc driver" in d.lower()]
    if sql_drivers:
        for pref in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]:
            if any(pref.lower() in d.lower() for d in sql_drivers):
                match = [d for d in sql_drivers if pref.lower() in d.lower()]
                if match:
                    driver = match[0]
                    break
        else:
            driver = sql_drivers[0]
except Exception:
    pass

conn_str = f"DRIVER={{{driver}}};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=yes;"

try:
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()
    
    # Query etl_log
    print("--- RECENT ETL LOGS ---")
    try:
        cursor.execute("SELECT TOP 15 id, process_name, start_time, end_time, status, LEFT(error_message, 200) FROM dbo.etl_log ORDER BY id DESC")
        rows = cursor.fetchall()
        for row in rows:
            print(f"ID: {row[0]} | Process: {row[1]} | Start: {row[2]} | End: {row[3]} | Status: {row[4]} | Error: {row[5]}")
    except Exception as e:
        print(f"Error querying etl_log: {e}")
        
    # Query etl_rejects
    print("\n--- RECENT ETL REJECTS ---")
    try:
        cursor.execute("SELECT TOP 15 id, process_name, table_name, LEFT(error_reason, 200), rejected_at FROM dbo.etl_rejects ORDER BY id DESC")
        rows = cursor.fetchall()
        for row in rows:
            print(f"ID: {row[0]} | Process: {row[1]} | Table: {row[2]} | Reason: {row[3]} | Rejected At: {row[4]}")
    except Exception as e:
        print(f"Error querying etl_rejects: {e}")
        
except Exception as e:
    print("Failed to query logs:", e)
