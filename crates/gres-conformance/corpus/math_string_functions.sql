-- SP33: math & string functions, diffed against PostgreSQL 18. Rounding family
-- (floor/ceil/round/trunc/sign), transcendental (sqrt/power/exp/ln/log) via
-- float8, and string utilities. Transcendental functions return float8 here
-- (a documented deviation from PG's numeric-in/numeric-out); the corpus drives
-- them through float8 values whose results are exactly printable, so the text
-- diff is deterministic (irrational float8 text is exercised in the unit/wire
-- tests, not here). Every result query is ORDER BY-stable and ASCII-only.
CREATE TABLE msf_m (id int4, x float8, q numeric);
INSERT INTO msf_m VALUES (1, 2.5, 2.567), (2, -2.5, -1.5), (3, 9.0, 1234.0);

-- rounding family (numeric: half-away-from-zero; preserves type)
SELECT floor(2.9), ceil(2.1), trunc(2.99), round(2.5), sign(-3);
SELECT round(2.567, 2), trunc(2.567, 1), round(1234, -2);
SELECT id, floor(q), round(q, 1) FROM msf_m ORDER BY id;

-- rounding family (float8: round half-to-even)
SELECT round(0.5::float8), round(1.5::float8), round(2.5::float8), round(3.5::float8);
SELECT id, floor(x), ceil(x), trunc(x), sign(x) FROM msf_m ORDER BY id;

-- transcendental (float8) — exactly-printable results only
SELECT sqrt(4.0::float8), sqrt(9.0::float8), power(2.0::float8, 10.0::float8);
SELECT exp(0.0::float8), ln(1.0::float8), log(1000.0::float8);
SELECT id, sqrt(x) FROM msf_m WHERE x = 9.0 ORDER BY id;

-- string padding / slicing
SELECT lpad('hi', 5, '*'), rpad('hi', 5, 'ab');
SELECT lpad('hello', 3), rpad('hello', 3);
SELECT left('abcdef', 2), left('abcdef', -2), right('abcdef', 2), right('abcdef', -2);
SELECT repeat('ab', 3), repeat('x', 0);

-- string transforms / search
SELECT reverse('abcdef'), initcap('hello WORLD foo');
SELECT strpos('abcde', 'cd'), strpos('abcde', 'xy'), strpos('abc', '');
SELECT ascii('A'), ascii('z'), chr(65), chr(97);

-- `to_number(text, text)` reads the digits out of a decorated string, dropping
-- the decoration a `to_char` template would have emitted: group separators,
-- currency, and sign markers. A sign after the digits negates.
SELECT to_number('-34,338,492', '99G999G999');
SELECT to_number('-34,338,492.654,878', '99G999G999D999G999');
SELECT to_number('0.01', 'FM9.99');
SELECT to_number('.0', 'FM9.99');
SELECT to_number('123', '999');
SELECT to_number('$1,234.56', 'L9G999D99');
SELECT to_number('5.01-', 'FM9.99S');
SELECT to_number('12,454.8-', '99G999D9S');
SELECT to_number('-1234.56', 'S9999.99');
SELECT to_number('abc', '999');
