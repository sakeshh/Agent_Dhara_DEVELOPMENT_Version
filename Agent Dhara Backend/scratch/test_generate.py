import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.session_store import load_session
from agent.etl_handlers import etl_generate_code

session_id = "a3dee8ef-0ee8-47c9-a616-d1f1a7eb333f"
sess = load_session(session_id)

res1 = etl_generate_code(
    session_id,
    engine="pyspark",
    codegen_mode="template",
    generation_mode="cleanse_only"
)

code = res1.get("code") or ""
# Write to temp file for viewing
with open("scratch/generated_debug_out.py", "w", encoding="utf-8") as f:
    f.write(code)

print("Result ok:", res1.get("ok"))
print("Validation errors:", res1.get("validation_errors"))
