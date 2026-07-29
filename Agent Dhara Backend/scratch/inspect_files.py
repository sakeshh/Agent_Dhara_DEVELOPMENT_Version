import glob
import os
from agent.session_store import load_session

etl_dir = r"output\etl_code\a3dee8ef-0ee8-47c9-a616-d1f1a7eb333f"
files = sorted(glob.glob(os.path.join(etl_dir, "*.py")))
for f in files[-5:]:
    content = open(f, encoding='utf-8').read()
    print("----------------------------------------")
    print(f"File: {os.path.basename(f)}")
    lines = content.splitlines()
    print(f"Total Lines: {len(lines)}")
    # Print the first 15 lines of each file to see what it is
    print("First 15 lines:")
    for line in lines[:15]:
        print("  ", line)
