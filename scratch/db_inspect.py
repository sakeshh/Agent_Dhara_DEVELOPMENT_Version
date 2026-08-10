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

print(f"Connecting to server: {server}, database: {database}...")
try:
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()
    
    # 1. Check existing tables
    tables = ["dbo.Accounts", "dbo.Accounts_Clean", "dbo.Accounts_Transformed"]
    for t in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {t}")
            row = cursor.fetchone()
            print(f"Table {t}: {row[0]} rows")
        except Exception as e:
            print(f"Table {t} error/not found: {e}")
            
    # 2. Print columns for Accounts_Clean if exists
    try:
        cursor.execute("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Accounts_Clean'")
        cols = cursor.fetchall()
        print("\nColumns in Accounts_Clean:")
        for col in cols:
            print(f" - {col[0]}: {col[1]} ({col[2]})")
    except Exception as e:
        print(f"Error reading column schema for Accounts_Clean: {e}")

    # 3. Print existing procedures
    try:
        cursor.execute("SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE'")
        procs = cursor.fetchall()
        print("\nStored Procedures in DB:")
        for proc in procs:
            print(f" - {proc[0]}")
    except Exception as e:
        print(f"Error reading procedures: {e}")
        
except Exception as e:
    print("Failed connection/execution:", e)
