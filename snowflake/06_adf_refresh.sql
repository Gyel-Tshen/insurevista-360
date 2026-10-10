-- =====================================================================
-- InsureVista 360° | Snowflake
-- File: 06_adf_refresh.sql
-- Purpose: atomic Gold refresh procedure + least-privilege ADF service user
-- =====================================================================
USE WAREHOUSE ANALYTICS_WH;

-- ---------------------------------------------------------------------
-- 1. The procedure — created BY ENGINEER_ROLE so it runs with its rights
-- ---------------------------------------------------------------------
USE ROLE ENGINEER_ROLE;

CREATE OR REPLACE PROCEDURE HOLLARD_DW.ANALYTICS.SP_REFRESH_GOLD()
RETURNS TABLE (table_name VARCHAR, rows_loaded NUMBER)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    BEGIN TRANSACTION;

    DELETE FROM HOLLARD_DW.ANALYTICS.DIM_DATE;
    COPY INTO HOLLARD_DW.ANALYTICS.DIM_DATE FROM @HOLLARD_DW.ANALYTICS.HOLLARD_GOLD_STAGE/dim_date/
        PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;

    DELETE FROM HOLLARD_DW.ANALYTICS.DIM_LOB;
    COPY INTO HOLLARD_DW.ANALYTICS.DIM_LOB FROM @HOLLARD_DW.ANALYTICS.HOLLARD_GOLD_STAGE/dim_lob/
        PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;

    DELETE FROM HOLLARD_DW.ANALYTICS.DIM_BROKER;
    COPY INTO HOLLARD_DW.ANALYTICS.DIM_BROKER FROM @HOLLARD_DW.ANALYTICS.HOLLARD_GOLD_STAGE/dim_broker/
        PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;

    DELETE FROM HOLLARD_DW.ANALYTICS.DIM_CUSTOMER;
    COPY INTO HOLLARD_DW.ANALYTICS.DIM_CUSTOMER FROM @HOLLARD_DW.ANALYTICS.HOLLARD_GOLD_STAGE/dim_customer/
        PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;

    DELETE FROM HOLLARD_DW.ANALYTICS.DIM_POLICY;
    COPY INTO HOLLARD_DW.ANALYTICS.DIM_POLICY FROM @HOLLARD_DW.ANALYTICS.HOLLARD_GOLD_STAGE/dim_policy/
        PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;

    DELETE FROM HOLLARD_DW.ANALYTICS.FACT_CLAIMS;
    COPY INTO HOLLARD_DW.ANALYTICS.FACT_CLAIMS FROM @HOLLARD_DW.ANALYTICS.HOLLARD_GOLD_STAGE/fact_claims/
        PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;

    DELETE FROM HOLLARD_DW.ANALYTICS.FACT_PREMIUMS;
    COPY INTO HOLLARD_DW.ANALYTICS.FACT_PREMIUMS FROM @HOLLARD_DW.ANALYTICS.HOLLARD_GOLD_STAGE/fact_premiums/
        PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;

    DELETE FROM HOLLARD_DW.ANALYTICS.FACT_COMPLAINTS;
    COPY INTO HOLLARD_DW.ANALYTICS.FACT_COMPLAINTS FROM @HOLLARD_DW.ANALYTICS.HOLLARD_GOLD_STAGE/fact_complaints/
        PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;

    DELETE FROM HOLLARD_DW.ANALYTICS.FACT_PARTNER_SALES;
    COPY INTO HOLLARD_DW.ANALYTICS.FACT_PARTNER_SALES FROM @HOLLARD_DW.ANALYTICS.HOLLARD_GOLD_STAGE/fact_partner_sales/
        PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;

    COMMIT;

    res := (
        SELECT 'DIM_DATE' AS table_name, COUNT(*) AS rows_loaded FROM HOLLARD_DW.ANALYTICS.DIM_DATE
        UNION ALL SELECT 'DIM_LOB',            COUNT(*) FROM HOLLARD_DW.ANALYTICS.DIM_LOB
        UNION ALL SELECT 'DIM_BROKER',         COUNT(*) FROM HOLLARD_DW.ANALYTICS.DIM_BROKER
        UNION ALL SELECT 'DIM_CUSTOMER',       COUNT(*) FROM HOLLARD_DW.ANALYTICS.DIM_CUSTOMER
        UNION ALL SELECT 'DIM_POLICY',         COUNT(*) FROM HOLLARD_DW.ANALYTICS.DIM_POLICY
        UNION ALL SELECT 'FACT_CLAIMS',        COUNT(*) FROM HOLLARD_DW.ANALYTICS.FACT_CLAIMS
        UNION ALL SELECT 'FACT_PREMIUMS',      COUNT(*) FROM HOLLARD_DW.ANALYTICS.FACT_PREMIUMS
        UNION ALL SELECT 'FACT_COMPLAINTS',    COUNT(*) FROM HOLLARD_DW.ANALYTICS.FACT_COMPLAINTS
        UNION ALL SELECT 'FACT_PARTNER_SALES', COUNT(*) FROM HOLLARD_DW.ANALYTICS.FACT_PARTNER_SALES
    );
    RETURN TABLE(res);

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;     -- any failure: undo ALL nine tables, BI keeps yesterday's data
        RAISE;        -- and still fail loudly so ADF's alert fires
END;
$$;

-- Test it as ENGINEER_ROLE: expect the 9 familiar counts
CALL HOLLARD_DW.ANALYTICS.SP_REFRESH_GOLD();


-- ---------------------------------------------------------------------
-- 2. LOADER_ROLE — may run the procedure, nothing else
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS LOADER_ROLE COMMENT = 'ADF: may only call SP_REFRESH_GOLD';
GRANT ROLE LOADER_ROLE TO ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE ANALYTICS_WH        TO ROLE LOADER_ROLE;
GRANT USAGE ON DATABASE  HOLLARD_DW          TO ROLE LOADER_ROLE;
GRANT USAGE ON SCHEMA    HOLLARD_DW.ANALYTICS TO ROLE LOADER_ROLE;
GRANT USAGE ON PROCEDURE HOLLARD_DW.ANALYTICS.SP_REFRESH_GOLD() TO ROLE LOADER_ROLE;


-- ---------------------------------------------------------------------
-- 3. ADF_SVC — key-pair service user
-- ---------------------------------------------------------------------
CREATE USER IF NOT EXISTS ADF_SVC
  TYPE              = SERVICE
  DEFAULT_ROLE      = LOADER_ROLE
  DEFAULT_WAREHOUSE = ANALYTICS_WH
  DEFAULT_NAMESPACE = 'HOLLARD_DW.ANALYTICS'
  RSA_PUBLIC_KEY    = '<public-key>'
  COMMENT           = 'ADF pipeline: Gold refresh (key-pair auth)';

GRANT ROLE LOADER_ROLE TO USER ADF_SVC;


-- ---------------------------------------------------------------------
-- 4. PROVE least privilege: LOADER_ROLE can refresh, but cannot read data
-- ---------------------------------------------------------------------
USE ROLE LOADER_ROLE;
USE SECONDARY ROLES NONE;

CALL HOLLARD_DW.ANALYTICS.SP_REFRESH_GOLD();                    -- works
SELECT COUNT(*) FROM HOLLARD_DW.ANALYTICS.FACT_CLAIMS;          -- expected to FAIL


-- ---------------------------------------------------------------------
-- 5. Restore your admin session (run on its own after the expected failure)
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
USE SECONDARY ROLES ALL;