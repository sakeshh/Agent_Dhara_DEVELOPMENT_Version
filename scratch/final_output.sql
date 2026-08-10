-- ETL SQL — Agent Dhara — plan_id=plan_1779351994
-- dialect=tsql — review before executing against production.

-- === dataset: dbo.Orders_Raw ===
BEGIN TRY
    BEGIN TRAN;
UPDATE [dbo].[Orders_Raw] SET [OrderDate] = LTRIM(RTRIM(CAST([OrderDate] AS NVARCHAR(MAX)))) WHERE [OrderDate] IS NOT NULL;
UPDATE [dbo].[Orders_Raw] SET [OrderAmount] = NULL WHERE LTRIM(RTRIM(CAST([OrderAmount] AS NVARCHAR(MAX)))) IN (N'0', N'-999', N'999999', N'9999999', N'###');
UPDATE [dbo].[Orders_Raw] SET [OrderDate] = COALESCE([OrderDate], N'') WHERE [OrderDate] IS NULL;
UPDATE [dbo].[Orders_Raw] SET [OrderDate] = TRY_CONVERT(date, [OrderDate], 120) WHERE [OrderDate] IS NOT NULL;
UPDATE [dbo].[Orders_Raw] SET [OrderStatus] = COALESCE([OrderStatus], N'') WHERE [OrderStatus] IS NULL;
UPDATE [dbo].[Orders_Raw] SET [OrderStatus] = NULL WHERE LTRIM(RTRIM(CAST([OrderStatus] AS NVARCHAR(MAX)))) IN (N'0', N'-999', N'999999', N'9999999', N'###');
-- Deduplicate [dbo].[Orders_Raw] on row-level (auto-partitioned by non-key columns)
;WITH _dedup AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY [CustomerID], [OrderDate], [OrderAmount], [OrderStatus] ORDER BY [OrderID]) AS _rn
    FROM [dbo].[Orders_Raw]
)
DELETE FROM _dedup WHERE _rn > 1;
    COMMIT;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK;
    THROW;
END CATCH;

-- === dataset: dbo.Sales_Raw ===
BEGIN TRY
    BEGIN TRAN;
UPDATE [dbo].[Sales_Raw] SET [Quantity] = LTRIM(RTRIM(CAST([Quantity] AS NVARCHAR(MAX)))) WHERE [Quantity] IS NOT NULL;
UPDATE [dbo].[Sales_Raw] SET [SalesDate] = LTRIM(RTRIM(CAST([SalesDate] AS NVARCHAR(MAX)))) WHERE [SalesDate] IS NOT NULL;
UPDATE [dbo].[Sales_Raw] SET [Quantity] = TRY_CAST(CAST([Quantity] AS NVARCHAR(MAX)) AS BIGINT) WHERE [Quantity] IS NOT NULL;
UPDATE [dbo].[Sales_Raw] SET [SalesDate] = COALESCE([SalesDate], N'') WHERE [SalesDate] IS NULL;
UPDATE [dbo].[Sales_Raw] SET [SalesDate] = TRY_CONVERT(date, [SalesDate], 120) WHERE [SalesDate] IS NOT NULL;
UPDATE [dbo].[Sales_Raw] SET [TotalAmount] = NULL WHERE LTRIM(RTRIM(CAST([TotalAmount] AS NVARCHAR(MAX)))) IN (N'0', N'-999', N'999999', N'9999999', N'###');
-- Deduplicate [dbo].[Sales_Raw] on row-level (auto-partitioned by non-key columns)
;WITH _dedup AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY [OrderID], [ProductName], [Quantity], [TotalAmount], [SalesDate] ORDER BY [SaleID]) AS _rn
    FROM [dbo].[Sales_Raw]
)
DELETE FROM _dedup WHERE _rn > 1;
    COMMIT;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK;
    THROW;
END CATCH;


-- ── Staging / load order (connector manifest) ──
-- dbo.Orders_Raw: -- file staging required
-- dbo.Sales_Raw: -- file staging required

-- Join dbo.Orders_Raw -> dbo.Sales_Raw (one_to_many)
-- CREATE VIEW vw_dbo_Sales_Raw_enriched AS
SELECT c.*, p.*
FROM [dbo].[Sales_Raw] c
INNER JOIN [dbo].[Orders_Raw] p ON c.[OrderID] = p.[OrderID];
