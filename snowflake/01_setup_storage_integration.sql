-- =====================================================================
-- InsureVista 360° | Snowflake setup
-- File: 01_setup_storage_integration.sql
-- Purpose: compute, database, and secure (keyless) access to ADLS Gold
-- =====================================================================

-- ---------------------------------------------------------------------
-- PART 1 — run now
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Compute: smallest size, auto-suspends after 60s idle
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS HOLLARD_DW;
CREATE SCHEMA IF NOT EXISTS HOLLARD_DW.ANALYTICS;

-- Snowflake's identity in Azure; can only see the Gold export folder
CREATE STORAGE INTEGRATION IF NOT EXISTS HOLLARD_ADLS_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<tenant-id>'
  STORAGE_ALLOWED_LOCATIONS = ('azure://hollardlake.blob.core.windows.net/gold/export/');

-- Copy AZURE_CONSENT_URL and AZURE_MULTI_TENANT_APP_NAME from this result
DESC STORAGE INTEGRATION HOLLARD_ADLS_INT;


-- ---------------------------------------------------------------------
-- PART 2 — run AFTER Azure consent + "Storage Blob Data Reader" role
-- ---------------------------------------------------------------------
USE WAREHOUSE ANALYTICS_WH;
USE SCHEMA HOLLARD_DW.ANALYTICS;

CREATE STAGE IF NOT EXISTS HOLLARD_GOLD_STAGE
  URL = 'azure://hollardlake.blob.core.windows.net/gold/export/'
  STORAGE_INTEGRATION = HOLLARD_ADLS_INT
  FILE_FORMAT = (TYPE = PARQUET);

LIST @HOLLARD_GOLD_STAGE;