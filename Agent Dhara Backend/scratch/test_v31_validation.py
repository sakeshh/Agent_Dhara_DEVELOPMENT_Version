import os
import sys
from dotenv import load_dotenv
load_dotenv(".env")

from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
from agent.session_store import load_session

def main():
    session_id = "a3dee8ef-0ee8-47c9-a616-d1f1a7eb333f"
    sess = load_session(session_id)
    ctx = sess.get("context", {})
    plan = ctx.get("etl_flow", {}).get("plan", {})
    
    file_path = "output/etl_code/a3dee8ef-0ee8-47c9-a616-d1f1a7eb333f/etl_plan_1783937469_pyspark_v31_1783937501.py"
    with open(file_path, "r", encoding="utf-8") as f:
        source = f.read()
        
    ok, errs = validate_pyspark_source(source, plan)
    print("Verification:")
    print("OK:", ok)
    print("Errors:", errs)

if __name__ == "__main__":
    main()
