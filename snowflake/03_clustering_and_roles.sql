-- =====================================================================
-- InsureVista 360° | Snowflake
-- File: 03_clustering_and_roles.sql
-- Purpose: clustering (Optimise), RBAC (Secure), credit guard
-- =====================================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ANALYTICS_WH;
USE SCHEMA HOLLARD_DW.ANALYTICS;

-- ---------------------------------------------------------------------
-- 1. OPTIMISE — clustering keys on the facts only
-- ---------------------------------------------------------------------
ALTER TABLE FACT_CLAIMS   CLUSTER BY (date_key, lob_key);
ALTER TABLE FACT_PREMIUMS CLUSTER BY (date_key, lob_key);

-- How well clustered is it? (expect 1 partition today — the table is tiny)
SELECT SYSTEM$CLUSTERING_INFORMATION('FACT_CLAIMS');


-- ---------------------------------------------------------------------
-- 2. SECURE — roles
-- ---------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS ANALYST_ROLE  COMMENT = 'Read-only: BI and Power BI';
CREATE ROLE IF NOT EXISTS ENGINEER_ROLE COMMENT = 'Read-write: data team';

GRANT ROLE ANALYST_ROLE  TO ROLE ENGINEER_ROLE;   -- engineers inherit analyst access
GRANT ROLE ENGINEER_ROLE TO ROLE SYSADMIN;        -- best practice: roll up to SYSADMIN

-- ANALYST: the full privilege chain (warehouse → database → schema → objects)
GRANT USAGE ON WAREHOUSE ANALYTICS_WH                TO ROLE ANALYST_ROLE;
GRANT USAGE ON DATABASE  HOLLARD_DW                   TO ROLE ANALYST_ROLE;
GRANT USAGE ON SCHEMA    HOLLARD_DW.ANALYTICS         TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES    IN SCHEMA HOLLARD_DW.ANALYTICS TO ROLE ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA HOLLARD_DW.ANALYTICS TO ROLE ANALYST_ROLE;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA HOLLARD_DW.ANALYTICS TO ROLE ANALYST_ROLE;  -- covers Step 24's views

-- ENGINEER: read-write on the schema, can run loads and build views
GRANT OPERATE ON WAREHOUSE ANALYTICS_WH                        TO ROLE ENGINEER_ROLE;
GRANT ALL ON SCHEMA HOLLARD_DW.ANALYTICS                        TO ROLE ENGINEER_ROLE;
GRANT ALL ON ALL TABLES    IN SCHEMA HOLLARD_DW.ANALYTICS       TO ROLE ENGINEER_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA HOLLARD_DW.ANALYTICS       TO ROLE ENGINEER_ROLE;
GRANT USAGE ON STAGE HOLLARD_DW.ANALYTICS.HOLLARD_GOLD_STAGE    TO ROLE ENGINEER_ROLE;

-- Give yourself both roles so you can test them
SET me = CURRENT_USER();
GRANT ROLE ANALYST_ROLE  TO USER IDENTIFIER($me);
GRANT ROLE ENGINEER_ROLE TO USER IDENTIFIER($me);


-- ---------------------------------------------------------------------
-- 3. CREDIT GUARD — protect the trial
-- ---------------------------------------------------------------------
CREATE RESOURCE MONITOR IF NOT EXISTS TRIAL_GUARD
  WITH CREDIT_QUOTA = 50
       FREQUENCY = MONTHLY
       START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 75  PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE ANALYTICS_WH SET RESOURCE_MONITOR = TRIAL_GUARD;


-- ---------------------------------------------------------------------
-- 4. PROVE IT — analyst can read but not change anything
--    SECONDARY ROLES NONE is essential: otherwise your other roles
--    (e.g. ACCOUNTADMIN) silently authorise the TRUNCATE
-- ---------------------------------------------------------------------
USE ROLE ANALYST_ROLE;
USE SECONDARY ROLES NONE;
USE WAREHOUSE ANALYTICS_WH;

SELECT COUNT(*) AS analyst_can_read FROM HOLLARD_DW.ANALYTICS.FACT_CLAIMS;   -- works: 130000

TRUNCATE TABLE HOLLARD_DW.ANALYTICS.FACT_CLAIMS;   -- expected to FAIL: insufficient privileges


-- ---------------------------------------------------------------------
-- 5. Restore the normal admin session
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
USE SECONDARY ROLES ALL;