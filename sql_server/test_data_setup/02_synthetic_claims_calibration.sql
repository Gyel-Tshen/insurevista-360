-- =============================================================================
-- InsureVista 360 | SQL Server source (HollardDW)
-- 04_claims_calibration.sql
-- Purpose: scale synthetic claim amounts per line of business to
--          industry-typical loss ratios (overall ~534% -> 60.6%).
-- Safety:  1) full backup table first
--          2) factors computed into #factors (same session as the UPDATE)
--          3) UPDATE inside an explicit transaction, verified before COMMIT
-- Result:  130,000 rows updated; incurred claims $470,534,897.33;
--          overall loss ratio 60.6%. Reconciled SQL -> Databricks -> Snowflake
--          -> Power BI.
-- Note:    run the whole script in ONE SSMS session (#factors is session-scoped).
--          Check @@TRANCOUNT = 0 afterwards.
-- =============================================================================
USE HollardDW;

-- Step 1: backup ------------------------------------------------------------
SELECT * INTO dbo.Claims_backup_20261008 FROM dbo.Claims;
SELECT COUNT(*) AS backup_rows FROM dbo.Claims_backup_20261008;   -- expect 130000

-- Step 2: per-LOB factors -----------------------------------------------------
IF OBJECT_ID('tempdb..#factors') IS NOT NULL DROP TABLE #factors;

WITH targets AS (
    SELECT * FROM (VALUES
        ('Motor', 70.0), ('Home', 60.0), ('Commercial', 55.0), ('Farm', 65.0),
        ('Pet', 70.0), ('Travel', 60.0), ('Health', 85.0), ('Life', 60.0)
    ) AS t(line_of_business, target_pct)
),
prem AS (
    SELECT p.line_of_business, SUM(t.amount_received_aud) AS gwp
    FROM dbo.Premium_Transactions t JOIN dbo.Policies p ON t.policy_id = p.policy_id
    GROUP BY p.line_of_business
),
clm AS (
    SELECT p.line_of_business, SUM(c.approved_amount_aud) AS incurred
    FROM dbo.Claims c JOIN dbo.Policies p ON c.policy_id = p.policy_id
    GROUP BY p.line_of_business
)
SELECT t.line_of_business,
       CAST(t.target_pct AS FLOAT) / (100.0 * CAST(c.incurred AS FLOAT) / CAST(p.gwp AS FLOAT)) AS factor
INTO #factors
FROM targets t
JOIN prem p ON p.line_of_business = t.line_of_business
JOIN clm  c ON c.line_of_business = t.line_of_business;

SELECT * FROM #factors ORDER BY factor;   -- 8 rows, Life smallest (~0.001063)

-- Step 3: apply inside a transaction -----------------------------------------
BEGIN TRANSACTION;

UPDATE c
SET claim_amount_aud    = CAST(ROUND(c.claim_amount_aud    * f.factor, 2) AS DECIMAL(12,2)),
    approved_amount_aud = CAST(ROUND(c.approved_amount_aud * f.factor, 2) AS DECIMAL(12,2))
FROM dbo.Claims c
JOIN dbo.Policies p ON c.policy_id = p.policy_id
JOIN #factors     f ON f.line_of_business = p.line_of_business;

SELECT @@ROWCOUNT AS rows_updated;   -- expect 130000

-- Check the new loss ratios BEFORE committing
SELECT p.line_of_business,
       CAST(100.0 * SUM(c.approved_amount_aud) /
            (SELECT SUM(t.amount_received_aud) FROM dbo.Premium_Transactions t
             JOIN dbo.Policies p2 ON t.policy_id = p2.policy_id
             WHERE p2.line_of_business = p.line_of_business) AS DECIMAL(8,1)) AS new_loss_ratio_pct
FROM dbo.Claims c JOIN dbo.Policies p ON c.policy_id = p.policy_id
GROUP BY p.line_of_business
ORDER BY p.line_of_business;

-- Review the ratios above. If they match the targets: COMMIT. Otherwise: ROLLBACK.
COMMIT;
-- ROLLBACK;

-- Step 4: verify --------------------------------------------------------------
SELECT COUNT(*)                  AS claims,
       SUM(approved_amount_aud)  AS incurred,          -- expect 470534897.33
       CAST(100.0 * SUM(approved_amount_aud)
            / (SELECT SUM(amount_received_aud) FROM dbo.Premium_Transactions) AS DECIMAL(6,1)) AS overall_loss_ratio_pct,  -- expect 60.6
       (SELECT COUNT(*) FROM dbo.Claims WHERE approved_amount_aud > claim_amount_aud) AS approved_gt_claimed
FROM dbo.Claims;

SELECT @@TRANCOUNT AS open_transactions;   -- expect 0
