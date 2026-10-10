-- =============================================================================
-- InsureVista 360 | SQL Server source (HollardDW)
-- 02_scd2_test_changes.sql
-- Purpose: simulate source changes to prove the Silver SCD logic end to end.
--   * Customer 00001 moves address -> Type 2 change: Silver keeps history
--     (old row closed with valid_to, new current row inserted).
--   * Customer 00002 changes email -> Type 1 change: overwritten in place.
-- Run, then re-run the ADF ingestion and the Silver notebook.
-- Result: MERGE reported 1 insert + 2 updates.
-- =============================================================================
USE HollardDW;

-- Before
SELECT customer_id, state, suburb, postcode, preferred_channel, email
FROM dbo.Customers
WHERE customer_id IN ('HL-CUS-00001', 'HL-CUS-00002');

-- SCD2 change: customer 00001 moves (history must be kept)
UPDATE dbo.Customers
SET suburb = 'St Kilda', postcode = '3182', state = 'VIC'
WHERE customer_id = 'HL-CUS-00001';

-- Type 1 change: customer 00002 updates their email (just overwrite)
UPDATE dbo.Customers
SET email = 'updated.email@example.com.au'
WHERE customer_id = 'HL-CUS-00002';
