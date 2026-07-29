import os
import sys
from dotenv import load_dotenv

backend_dir = r"c:\Users\ssakesh\Documents\DHARA-GX\Agent Dhara Backend"
dotenv_path = os.path.join(backend_dir, ".env")
load_dotenv(dotenv_path)

sys.path.append(backend_dir)
from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector, AzureSQLPythonNetConnector

# Using pythonnet connector to discover tables
connector = AzureSQLPythonNetConnector({})
tables = connector.discover_tables()
print("Discovering Tables:")
for t in tables:
    print(" -", t)
