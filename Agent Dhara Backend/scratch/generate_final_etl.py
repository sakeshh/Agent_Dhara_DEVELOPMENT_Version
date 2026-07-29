import os
import sys
import yaml
from dotenv import load_dotenv

backend_dir = r"c:\Users\ssakesh\Downloads\DHARA-GX\Agent Dhara Backend"

# Load env variables
dotenv_path = os.path.join(backend_dir, ".env")
load_dotenv(dotenv_path)

sys.path.append(backend_dir)
from agent.etl_pipeline.sql_codegen import generate_sql_etl

def main():
    # Load sources to simulate actual run
    plan = {
        "plan_id": "plan_1779351994",
        "relationships": {
            "load_order": ["dbo.Orders_Raw", "dbo.Sales_Raw"],
            "joins": [
                {
                    "parent_dataset": "dbo.Orders_Raw",
                    "child_dataset": "dbo.Sales_Raw",
                    "parent_key": "OrderID",
                    "child_key": "OrderID",
                    "join_type": "inner",
                    "cardinality": "one_to_many"
                }
            ]
        },
        "datasets": {
            "dbo.Orders_Raw": {
                "steps": [
                    {"column": "OrderDate", "action": "trim", "order": 1},
                    {"column": "OrderAmount", "action": "zero_to_null", "order": 2, "params": {"replace_values": ["0", "-999", "999999", "9999999", "###"]}},
                    {"column": "OrderDate", "action": "fill_or_drop", "order": 3},
                    {"column": "OrderDate", "action": "parse_dates", "order": 4},
                    {"column": "OrderStatus", "action": "fill_or_drop", "order": 5},
                    {"column": "OrderStatus", "action": "zero_to_null", "order": 6, "params": {"replace_values": ["0", "-999", "999999", "9999999", "###"]}},
                    {"column": "[Row-level]", "action": "deduplicate", "order": 7}
                ]
            },
            "dbo.Sales_Raw": {
                "steps": [
                    {"column": "Quantity", "action": "trim", "order": 1},
                    {"column": "SalesDate", "action": "trim", "order": 2},
                    {"column": "Quantity", "action": "coerce_numeric", "order": 3},
                    {"column": "SalesDate", "action": "fill_or_drop", "order": 4},
                    {"column": "SalesDate", "action": "parse_dates", "order": 5},
                    {"column": "TotalAmount", "action": "zero_to_null", "order": 6, "params": {"replace_values": ["0", "-999", "999999", "9999999", "###"]}},
                    {"column": "[Row-level]", "action": "deduplicate", "order": 7}
                ]
            }
        }
    }
    
    assessment = {
        "datasets": {
            "dbo.Orders_Raw": {
                "columns": {
                    "OrderID": {},
                    "CustomerID": {},
                    "OrderDate": {},
                    "OrderAmount": {},
                    "OrderStatus": {}
                }
            },
            "dbo.Sales_Raw": {
                "columns": {
                    "SaleID": {},
                    "OrderID": {},
                    "ProductName": {},
                    "Quantity": {},
                    "TotalAmount": {},
                    "SalesDate": {}
                }
            }
        }
    }
    
    sql_code = generate_sql_etl(plan, assessment, dialect="tsql")
    with open("scratch/final_output.sql", "w", encoding="utf-8") as f:
        f.write(sql_code)
    print("Generated SQL written to scratch/final_output.sql successfully.")

if __name__ == "__main__":
    main()
