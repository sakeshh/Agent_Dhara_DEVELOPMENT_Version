import re
import os

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
codegen_path = os.path.join(root, "agent", "etl_pipeline", "sql_codegen.py")

with open(codegen_path, "r", encoding="utf-8") as f:
    code = f.read()

# Let's find lines from 560 to 760 and list all lines containing 'continue'
lines = code.splitlines()
print("Lines with 'continue' in generator range (560-1550):")
for idx, line in enumerate(lines):
    line_num = idx + 1
    if 560 <= line_num <= 1550:
        if "continue" in line:
            print(f"Line {line_num}: {line.strip()}")
