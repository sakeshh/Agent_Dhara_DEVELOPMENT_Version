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
        
        # Query sys.objects for Accounts or Citizens
        query = """
            SELECT name, type, type_desc, OBJECT_ID(name) as id
            FROM sys.objects 
            WHERE name LIKE '%Accounts%' OR name LIKE '%Citizens%'
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        print("\nAll database objects related to Accounts or Citizens:")
        for r in rows:
            print(f"Name: {r[0]} | Type: {r[1]} | Type Desc: {r[2]} | ID: {r[3]}")
            
        conn.close()
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()
