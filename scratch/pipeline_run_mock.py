import os
import sys
import json
import sqlite3

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)

from agent.etl_handlers import etl_generate_code, load_session, save_session

def test_combination():
    session_id = "combination_simulation_session"
    
    # Clone real session from DB
    real_sess = load_session("a3447b6b-1763-4282-82c5-73b5a32119fc")
    
    # Save as new session
    sess = load_session(session_id)
    sess["context"] = real_sess["context"]
    flow = sess["context"]["etl_flow"]
    flow["phase"] = "approved"
    save_session(sess)
    
    # Save session
    save_session(sess)
    
    print("Session pre-populated. Generating cleanse_only code...")
    res1 = etl_generate_code(
        session_id,
        engine="sql",
        sql_dialect="tsql",
        codegen_mode="template",
        generation_mode="cleanse_only"
    )
    print("Cleanse_only ok:", res1["ok"])
    
    # Load session to check code_cleanse
    sess = load_session(session_id)
    flow = sess["context"]["etl_flow"]
    print("Saved code_cleanse length:", len(flow.get("code_cleanse", "")))
    print("Current combined code length:", len(flow.get("code", "")))
    
    print("\nGenerating transform_only code...")
    res2 = etl_generate_code(
        session_id,
        engine="sql",
        sql_dialect="tsql",
        codegen_mode="template",
        generation_mode="transform_only"
    )
    print("Transform_only ok:", res2["ok"])
    
    # Load session again to check combination
    sess = load_session(session_id)
    flow = sess["context"]["etl_flow"]
    print("Saved code_transform length:", len(flow.get("code_transform", "")))
    
    final_sql = flow.get("code", "")
    print("Final combined code length:", len(final_sql))
    
    # Check that both etl_clean_Accounts and etl_transform_Accounts are defined in final_sql
    has_clean = "CREATE PROCEDURE dbo.etl_clean_Accounts" in final_sql
    has_transform = "CREATE PROCEDURE dbo.etl_transform_Accounts" in final_sql
    print(f"\nVerification:")
    print(f"  Has etl_clean_Accounts: {has_clean}")
    print(f"  Has etl_transform_Accounts: {has_transform}")
    
    if has_clean and has_transform:
        print("\nSUCCESS: Both cleanse and transform procedures are combined inside flow['code']!")
    else:
        print("\nFAILURE: Omitted procedure(s) in flow['code']!")

if __name__ == "__main__":
    test_combination()
