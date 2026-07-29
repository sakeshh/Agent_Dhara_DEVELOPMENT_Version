import os
root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sql_path = os.path.join(root, "scratch", "current_session_sql.sql")
with open(sql_path, "r", encoding="utf-8") as f:
    for line in f:
        if "CREATE PROCEDURE" in line:
            print(line.strip())
