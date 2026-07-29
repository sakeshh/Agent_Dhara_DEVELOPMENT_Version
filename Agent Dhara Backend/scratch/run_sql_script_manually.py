import sys
import os
import re
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.azure_sql_executor import get_connection

def split_batches(sql: str) -> list[str]:
    pattern = r"(?i)^\s*GO\s*(?:--.*)?$"
    batches = re.split(pattern, sql, flags=re.MULTILINE)
    result = []
    for b in batches:
        b_stripped = b.strip()
        if b_stripped:
            result.append(b_stripped)
    return result

def main():
    sql_path = os.path.join(os.path.dirname(__file__), "current_session_sql.sql")
    if not os.path.exists(sql_path):
        print(f"File not found: {sql_path}")
        return
        
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
        
    batches = split_batches(sql)
    print(f"Loaded SQL script. Total batches: {len(batches)}")
    
    try:
        conn = get_connection()
        # Ensure autocommit is False to handle transaction
        conn.autocommit = False
        cursor = conn.cursor()
        print("Connected successfully!")
        
        for i, batch in enumerate(batches):
            print(f"\n--- Batch {i+1}/{len(batches)} (Length: {len(batch)}) ---")
            snippet = batch[:150] + "..." if len(batch) > 150 else batch
            print(snippet)
            
            try:
                cursor.execute(batch)
                rows = cursor.rowcount
                print(f"Status: Success, Rows affected: {rows}")
                if cursor.messages:
                    print("Messages:", cursor.messages)
            except Exception as e:
                print(f"ERROR in Batch {i+1}: {e}")
                print("Rolling back transaction...")
                conn.rollback()
                return
                
        print("\nAll batches completed. Committing transaction...")
        conn.commit()
        print("Transaction committed successfully!")
        conn.close()
        
    except Exception as e:
        print("Connection/Transaction error:", e)

if __name__ == "__main__":
    main()
