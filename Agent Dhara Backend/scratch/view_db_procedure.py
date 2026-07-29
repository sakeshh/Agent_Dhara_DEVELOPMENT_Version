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
        
        cursor.execute("SELECT OBJECT_DEFINITION(OBJECT_ID('dbo.etl_main'))")
        row = cursor.fetchone()
        if row and row[0]:
            print("\nDefinition of dbo.etl_main:")
            print(row[0])
        else:
            print("\ndbo.etl_main not found or has no definition.")
            
        cursor.execute("SELECT name FROM sys.procedures WHERE name LIKE '%etl%'")
        rows = cursor.fetchall()
        print("\nAll etl-related procedures:")
        for r in rows:
            print(f"- {r[0]}")
            
        conn.close()
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()
