import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Reconfigure stdout to support UTF-8 characters
sys.stdout.reconfigure(encoding='utf-8')

import inspect
from agent import etl_handlers

try:
    source_code = inspect.getsource(etl_handlers.etl_execute_sql)
    print("--- etl_execute_sql source ---")
    print(source_code)
except Exception as e:
    print("Error:", e)
