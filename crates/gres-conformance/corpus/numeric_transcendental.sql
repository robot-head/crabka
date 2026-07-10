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
