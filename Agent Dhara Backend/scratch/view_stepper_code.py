import os

file_path = r"c:\Users\srevanku\OneDrive - Capgemini\Desktop\New folder (2)\DHARA-GX\app\data-pipeline\page.tsx"
with open(file_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

for idx in range(900, 949):
    if idx < len(lines):
        print(f"Line {idx+1}: {lines[idx].rstrip()}")
