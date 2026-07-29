import os

file_path = r"c:\Users\srevanku\OneDrive - Capgemini\Desktop\New folder (2)\DHARA-GX\app\data-pipeline\page.tsx"
with open(file_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

print(f"Total lines: {len(lines)}")
print("Searching for currentStep or rendering of database/files/stepper...")
for idx, line in enumerate(lines):
    if "currentStep" in line or "step" in line.lower() or "database" in line.lower():
        if idx < 150 or idx > 700:  # print only some lines to keep output concise
            print(f"Line {idx+1}: {line.strip()}")
