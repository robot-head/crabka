-- Subset of pg_regress int4.sql: literal integer arithmetic and comparison
-- within the SP2 slice (no INT4_TBL fixture, no casts).
SELECT 2 + 2;
SELECT 4 - 1;
SELECT 3 * 4;
SELECT 12 / 4;
SELECT 7 / 2;
SELECT 2 + 2 * 3;
SELECT (2 + 2) * 3;
SELECT 1 < 2;
SELECT 2 <= 2;
SELECT 3 <> 4;
SELECT 5 = 5;
SELECT 10 > 9;
SELECT 9 >= 10;
