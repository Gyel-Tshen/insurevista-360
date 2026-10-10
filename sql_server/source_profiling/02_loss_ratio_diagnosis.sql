-- =============================================================================
-- InsureVista 360 | SQL Server source (HollardDW)
-- 03_loss_ratio_diagnosis.sql
-- Purpose: diagnose the unrealistic synthetic claim amounts before calibration.
-- Finding: overall loss ratio was ~534% (claims >5x premium), so a flat factor
--          would not give realistic per-line ratios. Per-LOB factors were
--          derived against industry-typical target loss ratios instead.
-- Incurred claims = SUM(approved_amount_aud); GWP = SUM(amount_received_aud).
-- =============================================================================
USE HollardDW;

-- Query A: current loss ratio by line of business, and the effect of a flat x0.12
WITH prem AS (
    SELECT p.line_of_business, SUM(t.amount_received_aud) AS gwp
    FROM dbo.Premium_Transactions t
    JOIN dbo.Policies p ON t.policy_id = p.policy_id
    GROUP BY p.line_of_business
),
clm AS (
    SELECT p.line_of_business, SUM(c.approved_amount_aud) AS incurred
    FROM dbo.Claims c
    JOIN dbo.Policies p ON c.policy_id = p.policy_id
    GROUP BY p.line_of_business
)
SELECT pr.line_of_business,
       pr.gwp,
       cl.incurred,
       CAST(100.0 * cl.incurred        / pr.gwp AS DECIMAL(8,1)) AS loss_ratio_now_pct,
       CAST(100.0 * cl.incurred * 0.12 / pr.gwp AS DECIMAL(8,1)) AS loss_ratio_if_x012_pct
FROM prem pr
JOIN clm  cl ON cl.line_of_business = pr.line_of_business

UNION ALL

SELECT 'TOTAL',
       (SELECT SUM(amount_received_aud) FROM dbo.Premium_Transactions),
       (SELECT SUM(approved_amount_aud) FROM dbo.Claims),
       CAST(100.0 * (SELECT SUM(approved_amount_aud) FROM dbo.Claims)
                  / (SELECT SUM(amount_received_aud) FROM dbo.Premium_Transactions) AS DECIMAL(8,1)),
       CAST(100.0 * 0.12 * (SELECT SUM(approved_amount_aud) FROM dbo.Claims)
                  / (SELECT SUM(amount_received_aud) FROM dbo.Premium_Transactions) AS DECIMAL(8,1))
ORDER BY loss_ratio_now_pct DESC;

-- Query B: per-LOB factor needed to hit each target, with the effect on
-- average claim size (exposes Life: a claim-frequency problem, not severity)
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
    SELECT p.line_of_business, SUM(c.approved_amount_aud) AS incurred,
           COUNT(*) AS claim_count, COUNT(DISTINCT c.policy_id) AS policies_with_claims
    FROM dbo.Claims c JOIN dbo.Policies p ON c.policy_id = p.policy_id
    GROUP BY p.line_of_business
)
SELECT t.line_of_business,
       CAST(100.0 * c.incurred / p.gwp AS DECIMAL(10,1))                    AS loss_ratio_now_pct,
       t.target_pct,
       CAST(t.target_pct / (100.0 * c.incurred / p.gwp) AS DECIMAL(10,6))  AS factor,
       c.claim_count,
       CAST(c.incurred / c.claim_count AS DECIMAL(14,2))                    AS avg_claim_now,
       CAST(c.incurred / c.claim_count
            * t.target_pct / (100.0 * c.incurred / p.gwp) AS DECIMAL(14,2)) AS avg_claim_after,
       CAST(p.gwp / NULLIF((SELECT COUNT(*) FROM dbo.Policies x
                            WHERE x.line_of_business = t.line_of_business), 0)
            AS DECIMAL(12,2))                                               AS premium_per_policy
FROM targets t
JOIN prem p ON p.line_of_business = t.line_of_business
JOIN clm  c ON c.line_of_business = t.line_of_business
ORDER BY loss_ratio_now_pct DESC;
