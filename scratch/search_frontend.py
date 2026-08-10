import os

frontend_dir = r"c:\Users\srevanku\OneDrive - Capgemini\Desktop\New folder (2)\DHARA-GX"
keywords = ["data_quality_issues", "unified_issues", "gx_results", "gx_expectation"]

for root, dirs, files in os.walk(frontend_dir):
    if "node_modules" in dirs:
        dirs.remove("node_modules")
    if ".next" in dirs:
        dirs.remove(".next")
    for file in files:
        if file.endswith((".tsx", ".ts", ".js", ".json")):
            path = os.path.join(root, file)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
                for kw in keywords:
                    if kw in content:
                        print(f"Found '{kw}' in {os.path.relpath(path, frontend_dir)}")
            except Exception:
                pass
