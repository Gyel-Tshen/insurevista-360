-- =====================================================================
-- InsureVista 360° | Snowflake
-- File: 04_semantic_views.sql
-- Purpose: flat, pre-joined views for Power BI (Serve + Compute)
-- Fixes vs design doc: no is_current filter; no fact-to-fact fan-out
-- =====================================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ANALYTICS_WH;
USE SCHEMA HOLLARD_DW.ANALYTICS;

-- VIEW 1: Claims (one row per claim; customer = as they were at lodgement)
CREATE OR REPLACE VIEW V_CLAIMS_DASHBOARD AS
SELECT
    f.claim_key, f.claim_id, f.claim_type, f.claim_status, f.catastrophe_code,
    f.claim_amount_aud, f.approved_amount_aud, f.days_to_settle, f.is_fraud_flagged,
    d.full_date AS lodgement_date, d.year_month, d.month_name, d.quarter, d.year, d.financial_year_au,
    c.customer_id, c.full_name AS customer_name,
    c.state AS customer_state, c.suburb, c.postcode, c.risk_tier,
    c.is_current AS customer_version_is_current,
    p.policy_id, p.product_name, p.premium_aud, p.sum_insured_aud,
    p.vehicle_make, p.property_type, p.bushfire_level, p.natcat_risk_index,
    l.lob_name, l.lob_category,
    b.broker_name, b.broker_group, b.commission_rate_pct
FROM FACT_CLAIMS f
JOIN DIM_DATE     d ON f.date_key     = d.date_key
JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
JOIN DIM_POLICY   p ON f.policy_key   = p.policy_key
JOIN DIM_LOB      l ON f.lob_key      = l.lob_key
JOIN DIM_BROKER   b ON f.broker_key   = b.broker_key;

-- VIEW 2: Premium collection (one row per payment)
CREATE OR REPLACE VIEW V_PREMIUM_DASHBOARD AS
SELECT
    f.premium_key, f.transaction_id,
    f.amount_due_aud, f.amount_received_aud, f.gst_aud, f.stamp_duty_aud,
    f.payment_status, f.payment_method, f.instalment_number,
    ROUND(f.collection_rate * 100, 1) AS collection_rate_pct,
    d.full_date AS transaction_date, d.year_month, d.month_name, d.quarter, d.year, d.financial_year_au,
    p.policy_id, p.product_name,
    l.lob_name, l.lob_category,
    c.customer_id, c.state AS customer_state, c.suburb, c.risk_tier,
    b.broker_name, b.broker_group
FROM FACT_PREMIUMS f
JOIN DIM_DATE     d ON f.date_key     = d.date_key
JOIN DIM_POLICY   p ON f.policy_key   = p.policy_key
JOIN DIM_LOB      l ON f.lob_key      = l.lob_key
JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
JOIN DIM_BROKER   b ON f.broker_key   = b.broker_key;

-- VIEW 3: LOB KPIs by month (each fact aggregated separately, then aligned)
CREATE OR REPLACE VIEW V_LOB_KPI AS
WITH months AS (
    SELECT DISTINCT year, month_number, year_month, month_name, financial_year_au
    FROM DIM_DATE WHERE date_key <> -1
),
prem AS (
    SELECT f.lob_key, d.year_month,
           COUNT(DISTINCT f.policy_key) AS policies_paying,
           SUM(f.amount_received_aud)   AS gross_written_premium
    FROM FACT_PREMIUMS f JOIN DIM_DATE d ON f.date_key = d.date_key
    GROUP BY f.lob_key, d.year_month
),
clm AS (
    SELECT f.lob_key, d.year_month,
           COUNT(*)                   AS claim_count,
           SUM(f.approved_amount_aud) AS incurred_claims
    FROM FACT_CLAIMS f JOIN DIM_DATE d ON f.date_key = d.date_key
    GROUP BY f.lob_key, d.year_month
)
SELECT
    l.lob_name, l.lob_category,
    m.year, m.month_number, m.year_month, m.month_name, m.financial_year_au,
    COALESCE(p.policies_paying, 0)       AS policies_paying,
    COALESCE(p.gross_written_premium, 0) AS gross_written_premium,
    COALESCE(c.claim_count, 0)           AS claim_count,
    COALESCE(c.incurred_claims, 0)       AS incurred_claims,
    ROUND(c.incurred_claims / NULLIF(p.gross_written_premium, 0) * 100, 1) AS loss_ratio_pct
FROM DIM_LOB l
CROSS JOIN months m
LEFT JOIN prem p ON p.lob_key = l.lob_key AND p.year_month = m.year_month
LEFT JOIN clm  c ON c.lob_key = l.lob_key AND c.year_month = m.year_month
WHERE p.gross_written_premium IS NOT NULL OR c.incurred_claims IS NOT NULL;

-- VIEW 4: Broker scorecard (premiums and claims aggregated per broker first)
CREATE OR REPLACE VIEW V_BROKER_SCORECARD AS
WITH prem AS (
    SELECT broker_key,
           COUNT(DISTINCT policy_key) AS policies_on_book,
           SUM(amount_received_aud)   AS total_gwp
    FROM FACT_PREMIUMS GROUP BY broker_key
),
clm AS (
    SELECT broker_key,
           COUNT(*)                 AS total_claims,
           SUM(approved_amount_aud) AS incurred_claims
    FROM FACT_CLAIMS GROUP BY broker_key
)
SELECT
    b.broker_id, b.broker_name, b.broker_group, b.state, b.commission_rate_pct, b.is_active,
    COALESCE(p.policies_on_book, 0) AS policies_on_book,
    COALESCE(p.total_gwp, 0)        AS total_gwp,
    ROUND(COALESCE(p.total_gwp, 0) * b.commission_rate_pct / 100, 2) AS commission_earned,
    COALESCE(c.total_claims, 0)     AS total_claims,
    COALESCE(c.incurred_claims, 0)  AS incurred_claims,
    ROUND(c.incurred_claims / NULLIF(p.total_gwp, 0) * 100, 1) AS loss_ratio_pct
FROM DIM_BROKER b
LEFT JOIN prem p ON p.broker_key = b.broker_key
LEFT JOIN clm  c ON c.broker_key = b.broker_key
WHERE b.broker_key <> -1;


-- =====================================================================
-- VALIDATION — run each query separately (highlight + Ctrl + Enter)
-- =====================================================================

-- V1. Every view reconciles to the facts (expect 4150530208.71 / 776976280.62)
SELECT 'V_CLAIMS_DASHBOARD' AS source, COUNT(*) AS row_count,
       SUM(approved_amount_aud) AS claims_total, NULL AS premium_total
FROM V_CLAIMS_DASHBOARD
UNION ALL
SELECT 'V_PREMIUM_DASHBOARD', COUNT(*), NULL, SUM(amount_received_aud) FROM V_PREMIUM_DASHBOARD
UNION ALL
SELECT 'V_LOB_KPI', COUNT(*), SUM(incurred_claims), SUM(gross_written_premium) FROM V_LOB_KPI
UNION ALL
SELECT 'V_BROKER_SCORECARD', COUNT(*), SUM(incurred_claims), SUM(total_gwp) FROM V_BROKER_SCORECARD;

-- V2. Grace's pre-move claim is still visible (doc's is_current filter would hide it)
SELECT claim_id, lodgement_date, suburb, customer_version_is_current
FROM V_CLAIMS_DASHBOARD
WHERE customer_id = 'HL-CUS-00001';

-- V3. What the design doc's join pattern would have reported (fan-out evidence)
SELECT SUM(fc.approved_amount_aud) AS doc_pattern_claims_total,
       SUM(fp.amount_received_aud) AS doc_pattern_premium_total
FROM DIM_POLICY p
LEFT JOIN FACT_PREMIUMS fp ON fp.policy_key = p.policy_key
LEFT JOIN FACT_CLAIMS   fc ON fc.policy_key = p.policy_key;