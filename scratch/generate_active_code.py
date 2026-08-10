import sqlite3
import json
import asyncio
from agent.etl_pipeline.llm_codegen import generate_etl_with_llm

async def main():
    conn = sqlite3.connect("output/chat_sessions.sqlite3")
    row = conn.execute("SELECT payload_json FROM sessions WHERE session_id = ?", ("a3dee8ef-0ee8-47c9-a616-d1f1a7eb333f",)).fetchone()
    conn.close()

    if row:
        payload = json.loads(row[0])
        ctx = payload.get("context", {})
        flow = ctx.get("etl_flow", {})
        plan = flow.get("approved_plan") or flow.get("plan") or {}
        assess = payload.get("last_assessment_result") or {}
        
        print("Calling generate_etl_with_llm...")
        from agent.etl_pipeline.llm_codegen import _CODE_CACHE, _FAILURE_CACHE
        _CODE_CACHE.clear()
        _FAILURE_CACHE.clear()
        
        code, last_err = await generate_etl_with_llm(plan, assess, engine="pyspark")
        print("\n--- GENERATED CODE ---")
        print(code)
        print("\n--- ERROR ---")
        print(last_err)
    else:
        print("Session not found")

if __name__ == "__main__":
    asyncio.run(main())
