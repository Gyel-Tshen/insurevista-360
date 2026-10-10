-- =============================================================================
-- InsureVista 360 | SQL Server source (HollardDW)
-- 01_source_profiling.sql
-- Purpose: confirm source row counts and policy distribution before ingestion.
-- Expected: Customers 35,000 | Brokers 320 | Policies 55,000 | Claims 130,000
--           | Premium_Transactions 220,000
-- =============================================================================
USE HollardDW;
GO

-- Row counts per source table
SELECT 'Customers' AS table_name, COUNT(*) AS row_count FROM dbo.Customers
UNION ALL SELECT 'Brokers', COUNT(*) FROM dbo.Brokers
UNION ALL SELECT 'Policies', COUNT(*) FROM dbo.Policies
UNION ALL SELECT 'Claims', COUNT(*) FROM dbo.Claims
UNION ALL SELECT 'Premium_Transactions', COUNT(*) FROM dbo.Premium_Transactions;

-- Policy distribution by state
SELECT state, COUNT(*) AS policies
FROM dbo.Policies
GROUP BY state
ORDER BY state;
