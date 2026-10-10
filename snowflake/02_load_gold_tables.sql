-- =====================================================================
-- InsureVista 360° | Snowflake
-- File: 02_load_gold_tables.sql
-- Purpose: create the 7 star-schema tables and full-refresh them from Gold
-- =====================================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ANALYTICS_WH;
USE SCHEMA HOLLARD_DW.ANALYTICS;

-- ---------------------------------------------------------------------
-- PART 1 — table definitions (safe to re-run; never drops data or grants)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DIM_DATE (
  date_key INT, full_date DATE, day_of_month INT, day_name VARCHAR, month_number INT,
  month_name VARCHAR, year_month VARCHAR, quarter INT, year INT, financial_year_au VARCHAR,
  fy_quarter INT, is_weekend BOOLEAN, is_public_holiday_au BOOLEAN, holiday_name VARCHAR,
  _gold_loaded_ts TIMESTAMP_NTZ);

CREATE TABLE IF NOT EXISTS DIM_LOB (
  lob_key BIGINT, lob_name VARCHAR, lob_category VARCHAR, product_list VARCHAR,
  _gold_loaded_ts TIMESTAMP_NTZ);

CREATE TABLE IF NOT EXISTS DIM_BROKER (
  broker_key BIGINT, broker_id VARCHAR, broker_name VARCHAR, broker_group VARCHAR, state VARCHAR,
  commission_rate_pct NUMBER(4,2), afsl_number VARCHAR, is_active BOOLEAN, onboarded_date DATE,
  _gold_loaded_ts TIMESTAMP_NTZ);

CREATE TABLE IF NOT EXISTS DIM_CUSTOMER (
  customer_key BIGINT, customer_id VARCHAR, full_name VARCHAR, date_of_birth DATE, gender VARCHAR,
  state VARCHAR, suburb VARCHAR, postcode VARCHAR, preferred_channel VARCHAR, is_multi_policy BOOLEAN,
  customer_since DATE, total_policies BIGINT, active_policies BIGINT, claim_count BIGINT,
  lifetime_value_aud NUMBER(14,2), complaint_count BIGINT, afca_escalations BIGINT, nps_avg FLOAT,
  natcat_risk_index FLOAT, risk_tier VARCHAR, valid_from DATE, valid_to DATE, is_current BOOLEAN,
  _gold_loaded_ts TIMESTAMP_NTZ);

CREATE TABLE IF NOT EXISTS DIM_POLICY (
  policy_key BIGINT, policy_id VARCHAR, lob_key BIGINT, line_of_business VARCHAR, product_name VARCHAR,
  premium_aud NUMBER(12,2), sum_insured_aud NUMBER(15,2), excess_aud NUMBER(10,2),
  effective_date DATE, expiry_date DATE, policy_status VARCHAR, distribution_channel VARCHAR,
  state VARCHAR, underwriter_id VARCHAR,
  vehicle_make VARCHAR, vehicle_model VARCHAR, vehicle_year INT, body_type VARCHAR, usage_type VARCHAR,
  agreed_value_aud NUMBER(12,2),
  property_type VARCHAR, construction_type VARCHAR, flood_zone BOOLEAN, bushfire_level VARCHAR,
  distance_to_coast_km FLOAT,
  natcat_risk_index FLOAT, bushfire_risk_score FLOAT, flood_risk_score FLOAT, cyclone_risk_score FLOAT,
  climate_trend_indicator VARCHAR,
  _gold_loaded_ts TIMESTAMP_NTZ);

CREATE TABLE IF NOT EXISTS FACT_CLAIMS (
  claim_key BIGINT, policy_key BIGINT, customer_key BIGINT, broker_key BIGINT, lob_key BIGINT,
  date_key INT, incident_date_key INT, settlement_date_key INT,
  claim_id VARCHAR, claim_type VARCHAR, claim_status VARCHAR, catastrophe_code VARCHAR,
  claim_amount_aud NUMBER(12,2), approved_amount_aud NUMBER(12,2), days_to_settle INT,
  is_fraud_flagged BOOLEAN,
  _gold_loaded_ts TIMESTAMP_NTZ);

CREATE TABLE IF NOT EXISTS FACT_PREMIUMS (
  premium_key BIGINT, policy_key BIGINT, customer_key BIGINT, broker_key BIGINT, lob_key BIGINT,
  date_key INT, transaction_id VARCHAR, payment_method VARCHAR, payment_status VARCHAR,
  instalment_number INT, amount_due_aud NUMBER(12,2), amount_received_aud NUMBER(12,2),
  gst_aud NUMBER(10,2), stamp_duty_aud NUMBER(10,2), collection_rate NUMBER(18,4),
  _gold_loaded_ts TIMESTAMP_NTZ);
CREATE TABLE IF NOT EXISTS FACT_COMPLAINTS (
  complaint_key BIGINT, customer_key BIGINT, date_key INT, resolution_date_key INT,
  complaint_id VARCHAR, related_claim_id VARCHAR,
  category VARCHAR, severity VARCHAR, resolution_outcome VARCHAR,
  nps_post_resolution INT, days_to_resolve INT,
  is_idr_escalation BOOLEAN, is_afca_escalation BOOLEAN,
  _gold_loaded_ts TIMESTAMP_NTZ);

CREATE TABLE IF NOT EXISTS FACT_PARTNER_SALES (
  sale_key BIGINT, policy_key BIGINT, customer_key BIGINT, broker_key BIGINT, lob_key BIGINT,
  date_key INT, sale_id VARCHAR, partner_brand VARCHAR, sale_channel VARCHAR, campaign_code VARCHAR,
  premium_aud NUMBER(12,2), acquisition_cost_aud NUMBER(12,2), acquisition_cost_ratio NUMBER(18,4),
  _gold_loaded_ts TIMESTAMP_NTZ);
-- ---------------------------------------------------------------------
-- PART 2 — full refresh from the Gold export
-- PATTERN skips Spark's _SUCCESS / _committed / _started marker files
-- ---------------------------------------------------------------------
TRUNCATE TABLE DIM_DATE;
COPY INTO DIM_DATE      FROM @HOLLARD_GOLD_STAGE/dim_date/      PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

TRUNCATE TABLE DIM_LOB;
COPY INTO DIM_LOB       FROM @HOLLARD_GOLD_STAGE/dim_lob/       PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

TRUNCATE TABLE DIM_BROKER;
COPY INTO DIM_BROKER    FROM @HOLLARD_GOLD_STAGE/dim_broker/    PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

TRUNCATE TABLE DIM_CUSTOMER;
COPY INTO DIM_CUSTOMER  FROM @HOLLARD_GOLD_STAGE/dim_customer/  PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

TRUNCATE TABLE DIM_POLICY;
COPY INTO DIM_POLICY    FROM @HOLLARD_GOLD_STAGE/dim_policy/    PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

TRUNCATE TABLE FACT_CLAIMS;
COPY INTO FACT_CLAIMS   FROM @HOLLARD_GOLD_STAGE/fact_claims/   PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

TRUNCATE TABLE FACT_PREMIUMS;
COPY INTO FACT_PREMIUMS FROM @HOLLARD_GOLD_STAGE/fact_premiums/ PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

TRUNCATE TABLE FACT_COMPLAINTS;
COPY INTO FACT_COMPLAINTS    FROM @HOLLARD_GOLD_STAGE/fact_complaints/    PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

TRUNCATE TABLE FACT_PARTNER_SALES;
COPY INTO FACT_PARTNER_SALES FROM @HOLLARD_GOLD_STAGE/fact_partner_sales/ PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
-- ---------------------------------------------------------------------
-- PART 3 — reconcile against Databricks Gold
-- ---------------------------------------------------------------------
SELECT 'DIM_DATE' AS tbl, COUNT(*) AS rows_loaded, 7671 AS expected FROM DIM_DATE
UNION ALL SELECT 'DIM_LOB',            COUNT(*), 9      FROM DIM_LOB
UNION ALL SELECT 'DIM_BROKER',         COUNT(*), 321    FROM DIM_BROKER
UNION ALL SELECT 'DIM_CUSTOMER',       COUNT(*), 35002  FROM DIM_CUSTOMER
UNION ALL SELECT 'DIM_POLICY',         COUNT(*), 55001  FROM DIM_POLICY
UNION ALL SELECT 'FACT_CLAIMS',        COUNT(*), 130000 FROM FACT_CLAIMS
UNION ALL SELECT 'FACT_PREMIUMS',      COUNT(*), 220000 FROM FACT_PREMIUMS
UNION ALL SELECT 'FACT_COMPLAINTS',    COUNT(*), 8000   FROM FACT_COMPLAINTS
UNION ALL SELECT 'FACT_PARTNER_SALES', COUNT(*), 25000  FROM FACT_PARTNER_SALES;

SELECT 'incurred claims' AS measure, SUM(approved_amount_aud) AS loaded, 470534897.33 AS expected FROM FACT_CLAIMS
UNION ALL SELECT 'premium received', SUM(amount_received_aud), 776976280.62 FROM FACT_PREMIUMS
UNION ALL SELECT 'partner premium',  SUM(premium_aud),         105364325.51 FROM FACT_PARTNER_SALES
UNION ALL SELECT 'AFCA escalations', SUM(IFF(is_afca_escalation, 1, 0)), 436 FROM FACT_COMPLAINTS;