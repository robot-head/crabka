-- SP34: numeric transcendentals (sqrt/ln/log/exp/power) return numeric for
-- numeric input, matching PostgreSQL's value AND display scale. Diffed vs PG 18
-- in CI; validated locally vs PG 17.10. ASCII + ORDER BY-stable.
CREATE TABLE nt (id int4, x numeric);
INSERT INTO nt VALUES (1, 2), (2, 4), (3, 100), (4, 0.04), (5, 1000000);

SELECT sqrt(2::numeric), sqrt(4::numeric), sqrt(0.04::numeric);
SELECT ln(2::numeric), ln(10::numeric), ln(1000000::numeric);
SELECT log(100::numeric), log(1000000::numeric);
SELECT exp(0::numeric), exp(1::numeric), exp(10::numeric);
SELECT power(2::numeric, 10::numeric), power(2::numeric, 3::numeric);
SELECT power(2::numeric, 100::numeric), power(-2::numeric, 3::numeric);
SELECT power(5::numeric, -2::numeric), power(2::numeric, 0.5::numeric);
SELECT id, sqrt(x), ln(x) FROM nt ORDER BY id;

-- ---------------------------------------------------------------------------
-- Display scale of division, power and logarithm
-- ---------------------------------------------------------------------------
-- The scale is part of the answer: PostgreSQL picks enough fractional digits for
-- sixteen significant ones at the result's estimated weight, but never fewer
-- than either operand's own display scale, and equal leading digits still assume
-- a quotient below one.
SELECT 70.0 / 70;
SELECT 1.00 / 1.00;
SELECT 12345.6789 / 1.1;
SELECT 1.0 / 3;
SELECT 1.00000000000000000000 / 1.00000000000000000000;
SELECT 1.5 / 0.0000000000000000000000001;
SELECT 100.0 / 3;
SELECT 0.0001 / 7;

SELECT 2::numeric ^ 10;
SELECT 0.2::numeric ^ 2;
SELECT 0.5::numeric ^ 3;
SELECT 0.1::numeric ^ 3;
SELECT 0.1::numeric ^ 1;
SELECT 100::numeric ^ 3;
SELECT 10.0 ^ 20;
SELECT 10.0 ^ (-20);
SELECT 0.000001::numeric ^ 3;
SELECT 0.000001::numeric ^ (-3);
SELECT 1.2 ^ 345;
SELECT 9.9 ^ 100;
SELECT 0.99 ^ 1000;
SELECT 0.12 ^ (-20);
SELECT 0.12 ^ (-25);
SELECT 0.5678 ^ (-85);
SELECT 3.789 ^ 21.0000000000000000;
SELECT 3.789 ^ 35.0000000000000000;
SELECT 1.0001::numeric ^ 10000;
SELECT 32.1 ^ 9.8;
SELECT 32.1 ^ (-9.8);
SELECT 12.3 ^ 45.6;
SELECT 12.3 ^ (-45.6);
SELECT 2::numeric ^ 0.5;
SELECT 2::numeric ^ 100.5;
SELECT 0.5::numeric ^ 0.5;
SELECT 1.5::numeric ^ (-3.5);
SELECT 0.0001::numeric ^ 2.25;
SELECT n, 10.0 ^ n FROM generate_series(-20, 20) n ORDER BY n;

-- Between 0.9 and 1.1 the logarithm's weight comes from `arg - 1`.
SELECT ln(0.99949452);
SELECT ln(1.00049687395);
SELECT ln(1.0000000001);
SELECT ln(1.005);
SELECT ln(1.05);
SELECT ln(0.95);
SELECT ln(0.9);
SELECT ln(1.1);
SELECT ln(0.89);
SELECT ln(1.11);
SELECT ln(2);
SELECT ln(1000);
SELECT ln(0.0001);
SELECT log(1.005);
SELECT log(0.999);
SELECT log(2.0);
