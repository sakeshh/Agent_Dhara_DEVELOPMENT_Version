import re

main_path = r"c:\Users\srevanku\OneDrive - Capgemini\Desktop\New folder (2)\DHARA-GX\Agent Dhara Backend\main.py"
with open(main_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

for idx, line in enumerate(lines):
    if any(k in line for k in ["gx_results", "data_quality_issues", "unified_issues", "run_gx"]):
        print(f"Line {idx+1}: {line.strip()}")
