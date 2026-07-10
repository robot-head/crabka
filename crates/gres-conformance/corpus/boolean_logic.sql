-- Subset of pg_regress boolean.sql: three-valued logic within the SP2 slice.
SELECT true;
SELECT false;
SELECT true AND false;
SELECT true OR false;
SELECT NOT true;
SELECT NOT false;
SELECT true AND true;
SELECT false OR false;
SELECT 1 < 2 AND 3 > 2;
SELECT 1 = 1 OR 2 = 3;
