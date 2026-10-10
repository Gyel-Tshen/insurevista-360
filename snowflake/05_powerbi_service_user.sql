-- =====================================================================
-- InsureVista 360° | Snowflake
-- File: 05_powerbi_service_user.sql
-- Purpose: key-pair service user for Power BI (no password, read-only)
-- =====================================================================
USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS POWERBI_SVC
  TYPE              = SERVICE                     -- non-human: cannot use a password at all
  DEFAULT_ROLE      = ANALYST_ROLE
  DEFAULT_WAREHOUSE = ANALYTICS_WH
  DEFAULT_NAMESPACE = 'HOLLARD_DW.ANALYTICS'
  RSA_PUBLIC_KEY    = '<public-key>'
  COMMENT           = 'Power BI connection (key-pair auth)';

GRANT ROLE ANALYST_ROLE TO USER POWERBI_SVC;     -- ONLY the read-only role

-- Check: RSA_PUBLIC_KEY_FP should show a fingerprint (SHA256:...)
DESC USER POWERBI_SVC;
SHOW GRANTS TO USER POWERBI_SVC;