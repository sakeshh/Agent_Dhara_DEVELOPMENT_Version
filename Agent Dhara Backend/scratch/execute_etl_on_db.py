import os
import sys
import yaml
from dotenv import load_dotenv

backend_dir = r"c:\Users\ssakesh\Downloads\DHARA-GX\Agent Dhara Backend"

# Load env variables
dotenv_path = os.path.join(backend_dir, ".env")
load_dotenv(dotenv_path)

sys.path.append(backend_dir)
from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector

# We import the exact SQL connection classes from pythonnet provider
from connectors.azure_sql_pythonnet import SqlConnection, SqlCommand

def run_query(conn, sql):
    cmd = SqlCommand(sql, conn)
    return cmd.ExecuteNonQuery()

def main():
    sources_yaml_path = os.path.join(backend_dir, "config", "sources.yaml")
    with open(sources_yaml_path, "r", encoding="utf-8") as f:
        sources_data = yaml.safe_load(f)
    
    locations = sources_data.get("source", {}).get("locations", [])
    db_location = None
    for loc in locations:
        if loc.get("type") == "database":
            db_location = loc
            break
            
    if not db_location:
        print("No database source found in sources.yaml")
        return
        
    conn_cfg = db_location.get("connection", {})
    connector = AzureSQLPythonNetConnector(conn_cfg)
    conn = connector._connect()
    
    # 1. Inspect table counts before execution
    print("--- Row counts BEFORE ETL ---")
    try:
        conn.Open()
        
        cmd = SqlCommand("SELECT COUNT(*) FROM dbo.Orders_Raw", conn)
        before_orders = cmd.ExecuteScalar()
        print("dbo.Orders_Raw row count:", before_orders)
        
        cmd = SqlCommand("SELECT COUNT(*) FROM dbo.Sales_Raw", conn)
        before_sales = cmd.ExecuteScalar()
        print("dbo.Sales_Raw row count:", before_sales)
        
        # 2. Formulate ETL code blocks
        print("\n--- Executing Orders_Raw ETL script ---")
        orders_etl_sql = """
BEGIN TRY
    BEGIN TRAN;
    UPDATE [dbo].[Orders_Raw] SET [OrderDate] = LTRIM(RTRIM(CAST([OrderDate] AS NVARCHAR(MAX)))) WHERE [OrderDate] IS NOT NULL;
    UPDATE [dbo].[Orders_Raw] SET [OrderAmount] = NULL WHERE LTRIM(RTRIM(CAST([OrderAmount] AS NVARCHAR(MAX)))) IN (N'-100', N'999999', N'-0.0', N'1111', N'33333', N'-9999', N'0.0', N'12345', N'-99', N'88888', N'1234', N'11111', N'22222', N'###', N'123456', N'44444', N'-99999', N'1234567', N'-1000', N'null', N'-999999', N'0', N'98765', N'9999999', N'-999', N'77777', N'nan', N'-1', N'9876543', N'9999', N'66666', N'999', N'9876', N'-9999999', N'55555', N'99999');
    UPDATE [dbo].[Orders_Raw] SET [OrderAmount] = NULL WHERE LTRIM(RTRIM(CAST([OrderAmount] AS NVARCHAR(MAX)))) IN (N'0', N'-999', N'999999', N'9999999', N'###');
    UPDATE [dbo].[Orders_Raw] SET [OrderDate] = COALESCE([OrderDate], N'') WHERE [OrderDate] IS NULL;
    UPDATE [dbo].[Orders_Raw] SET [OrderDate] = TRY_CONVERT(date, [OrderDate], 120) WHERE [OrderDate] IS NOT NULL;
    UPDATE [dbo].[Orders_Raw] SET [OrderStatus] = COALESCE([OrderStatus], N'') WHERE [OrderStatus] IS NULL;
    UPDATE [dbo].[Orders_Raw] SET [OrderStatus] = NULL WHERE LTRIM(RTRIM(CAST([OrderStatus] AS NVARCHAR(MAX)))) IN (N'0', N'-999', N'999999', N'9999999', N'###');
    
    ;WITH _dedup AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY [CustomerID], [OrderDate], [OrderAmount], [OrderStatus] ORDER BY [OrderID]) AS _rn
        FROM [dbo].[Orders_Raw]
    )
    DELETE FROM _dedup WHERE _rn > 1;
    COMMIT;
    SELECT 'Success' AS Status;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK;
    THROW;
END CATCH;
"""
        cmd = SqlCommand(orders_etl_sql, conn)
        cmd.ExecuteNonQuery()
        print("Orders_Raw ETL executed successfully.")
        
        print("\n--- Executing Sales_Raw ETL script ---")
        sales_etl_sql = """
BEGIN TRY
    BEGIN TRAN;
    UPDATE [dbo].[Sales_Raw] SET [Quantity] = LTRIM(RTRIM(CAST([Quantity] AS NVARCHAR(MAX)))) WHERE [Quantity] IS NOT NULL;
    UPDATE [dbo].[Sales_Raw] SET [SalesDate] = LTRIM(RTRIM(CAST([SalesDate] AS NVARCHAR(MAX)))) WHERE [SalesDate] IS NOT NULL;
    UPDATE [dbo].[Sales_Raw] SET [Quantity] = TRY_CAST(CAST([Quantity] AS NVARCHAR(MAX)) AS BIGINT) WHERE [Quantity] IS NOT NULL;
    UPDATE [dbo].[Sales_Raw] SET [SalesDate] = COALESCE([SalesDate], N'') WHERE [SalesDate] IS NULL;
    UPDATE [dbo].[Sales_Raw] SET [SalesDate] = TRY_CONVERT(date, [SalesDate], 120) WHERE [SalesDate] IS NOT NULL;
    UPDATE [dbo].[Sales_Raw] SET [TotalAmount] = NULL WHERE LTRIM(RTRIM(CAST([TotalAmount] AS NVARCHAR(MAX)))) IN (N'-100', N'999999', N'-0.0', N'1111', N'33333', N'-9999', N'0.0', N'12345', N'-99', N'88888', N'1234', N'11111', N'22222', N'###', N'123456', N'44444', N'-99999', N'1234567', N'-1000', N'null', N'-999999', N'0', N'98765', N'9999999', N'-999', N'77777', N'nan', N'-1', N'9876543', N'9999', N'66666', N'999', N'9876', N'-9999999', N'55555', N'99999');
    UPDATE [dbo].[Sales_Raw] SET [TotalAmount] = NULL WHERE LTRIM(RTRIM(CAST([TotalAmount] AS NVARCHAR(MAX)))) IN (N'0', N'-999', N'999999', N'9999999', N'###');
    
    ;WITH _dedup AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY [OrderID], [ProductName], [Quantity], [TotalAmount], [SalesDate] ORDER BY [SaleID]) AS _rn
        FROM [dbo].[Sales_Raw]
    )
    DELETE FROM _dedup WHERE _rn > 1;
    COMMIT;
    SELECT 'Success' AS Status;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK;
    THROW;
END CATCH;
"""
        cmd = SqlCommand(sales_etl_sql, conn)
        cmd.ExecuteNonQuery()
        print("Sales_Raw ETL executed successfully.")
        
        # 3. Inspect table counts after execution
        print("\n--- Row counts AFTER ETL ---")
        cmd = SqlCommand("SELECT COUNT(*) FROM dbo.Orders_Raw", conn)
        after_orders = cmd.ExecuteScalar()
        print("dbo.Orders_Raw row count:", after_orders)
        print("Rows cleaned/deleted from Orders_Raw:", before_orders - after_orders)
        
        cmd = SqlCommand("SELECT COUNT(*) FROM dbo.Sales_Raw", conn)
        after_sales = cmd.ExecuteScalar()
        print("dbo.Sales_Raw row count:", after_sales)
        print("Rows cleaned/deleted from Sales_Raw:", before_sales - after_sales)
        
    except Exception as e:
        print("Execution failed:", e)
    finally:
        conn.Close()

if __name__ == "__main__":
    main()
