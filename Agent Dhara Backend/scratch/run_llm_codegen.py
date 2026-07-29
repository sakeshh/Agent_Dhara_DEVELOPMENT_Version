import asyncio
import sqlite3
import json
import logging
import sys

# Configure logging to stdout
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

from dotenv import load_dotenv
load_dotenv(".env")

from agent.session_store import load_session
from agent.etl_pipeline.llm_codegen import generate_etl_with_llm
from agent.etl_pipeline.validate_pyspark import validate_pyspark_source

async def main():
    session_id = "a3dee8ef-0ee8-47c9-a616-d1f1a7eb333f"
    sess = load_session(session_id)
    ctx = sess.get("context", {})
    plan = ctx.get("etl_flow", {}).get("plan", {})
    assess = ctx.get("last_assessment_result") or {}
    
    print(f"Plan ID: {plan.get('plan_id')}")
    print(f"Engine: pyspark")
    
    # Run LLM generation
    code, err = await generate_etl_with_llm(
        plan,
        assess,
        engine="pyspark",
        validate_fn=lambda src: validate_pyspark_source(src, plan),
    )
    
    print("--- GENERATED CODE ---")
    print(code)
    print("--- ERROR FROM GENERATION ---")
    print(err)
    
    if code:
        ok, errs = validate_pyspark_source(code, plan)
        print("--- VALIDATION RESULTS ---")
        print("OK:", ok)
        print("Errors:", errs)

if __name__ == "__main__":
    asyncio.run(main())
