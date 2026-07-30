-- `numeric` special values (`NaN`, `Infinity`, `-Infinity`), diffed against
-- PostgreSQL 18.4.
--
-- PostgreSQL 14 gave `numeric` the two infinities alongside the `NaN` it has
-- always had, so 18.4 accepts every spelling below. The rules are not the float
-- ones: `NaN` is EQUAL to itself and sorts ABOVE every other numeric (including
-- `Infinity`), `numeric → integer` of a special is 0A000 rather than 22003, and
-- a zero divisor still wins over an infinite dividend (22012) while losing to a
-- `NaN` one.

-- ---------------------------------------------------------------------------
-- Input spellings (numeric_in) and output spellings (numeric_out)
-- ---------------------------------------------------------------------------
SELECT 'NaN'::numeric, 'nan'::numeric, 'NAN'::numeric, 'nAn'::numeric;
SELECT '  NaN  '::numeric, 'nan'::decimal;
SELECT 'Infinity'::numeric, 'infinity'::numeric, 'INFINITY'::numeric;
SELECT 'inf'::numeric, 'INF'::numeric, '+inf'::numeric, '+Infinity'::numeric;
SELECT '-Infinity'::numeric, '-infinity'::numeric, '-inf'::numeric, '  -inf  '::numeric;
SELECT 'inf'::decimal, '-inf'::decimal;
-- malformed input keeps 22P02; `NaN` takes no sign and `inf` no trailing junk
SELECT '-nan'::numeric;
SELECT '+nan'::numeric;
SELECT 'infi'::numeric;
SELECT 'infinityy'::numeric;
SELECT ' + inf'::numeric;
SELECT 'zzz'::numeric;
-- text round-trip
SELECT 'NaN'::numeric::text, 'Infinity'::numeric::text, '-Infinity'::numeric::text;
SELECT 'NaN'::text::numeric, '-Infinity'::text::numeric;

-- ---------------------------------------------------------------------------
-- Arithmetic
-- ---------------------------------------------------------------------------
SELECT 'NaN'::numeric + 1, 'NaN'::numeric - 1, 'NaN'::numeric * 1, 1 + 'NaN'::numeric;
SELECT 'Infinity'::numeric + 1, 'Infinity'::numeric + 'Infinity'::numeric;
SELECT 'Infinity'::numeric - 'Infinity'::numeric, 'Infinity'::numeric + '-Infinity'::numeric;
SELECT '-Infinity'::numeric - '-Infinity'::numeric, '-Infinity'::numeric + '-Infinity'::numeric;
SELECT 'Infinity'::numeric - '-Infinity'::numeric, 4.2 - 'Infinity'::numeric;
SELECT 0 * 'Infinity'::numeric, 'Infinity'::numeric * 0, 0.0 * '-Infinity'::numeric;
SELECT 'Infinity'::numeric * 2, 'Infinity'::numeric * -2, '-Infinity'::numeric * -1;
SELECT 'Infinity'::numeric * 'Infinity'::numeric, 'Infinity'::numeric * '-Infinity'::numeric;
SELECT -'Infinity'::numeric, -'-Infinity'::numeric, -'NaN'::numeric;

-- division: NaN beats a zero divisor, an infinite dividend does not
SELECT 'Infinity'::numeric / 'Infinity'::numeric, 'Infinity'::numeric / '-Infinity'::numeric;
SELECT 'Infinity'::numeric / 2, 'Infinity'::numeric / -2, '-Infinity'::numeric / 4.2;
SELECT 1 / 'Infinity'::numeric, -1 / 'Infinity'::numeric, 4.2 / '-Infinity'::numeric;
SELECT 'NaN'::numeric / 0;
SELECT 'Infinity'::numeric / 0;
SELECT '-Infinity'::numeric / 0;
SELECT 0::numeric / 0;

-- modulo follows the same ordering; a finite dividend over an infinity is itself
SELECT 'Infinity'::numeric % 2, 2 % 'Infinity'::numeric, 4.2 % 'Infinity'::numeric;
SELECT -1 % 'Infinity'::numeric, 'Infinity'::numeric % 'Infinity'::numeric;
SELECT 'NaN'::numeric % 2, 2 % 'NaN'::numeric;
SELECT 'NaN'::numeric % 0;
SELECT 'Infinity'::numeric % 0;

-- div() truncating division
SELECT div('Infinity'::numeric, 2), div(2::numeric, 'Infinity'::numeric), div('NaN'::numeric, 2);
SELECT div('Infinity'::numeric, 'Infinity'::numeric);
SELECT div('NaN'::numeric, 0);
SELECT div('Infinity'::numeric, 0);

-- ---------------------------------------------------------------------------
-- Rounding and sign
-- ---------------------------------------------------------------------------
SELECT abs('Infinity'::numeric), abs('-Infinity'::numeric), abs('NaN'::numeric);
SELECT sign('Infinity'::numeric), sign('-Infinity'::numeric), sign('NaN'::numeric);
SELECT floor('Infinity'::numeric), ceil('-Infinity'::numeric), floor('NaN'::numeric);
SELECT round('Infinity'::numeric), round('NaN'::numeric), round('Infinity'::numeric, 2);
SELECT round('-Infinity'::numeric, -3), trunc('Infinity'::numeric), trunc('NaN'::numeric, 3);
SELECT scale('NaN'::numeric), scale('Infinity'::numeric), min_scale('NaN'::numeric);
SELECT trim_scale('NaN'::numeric), trim_scale('-Infinity'::numeric);
SELECT gcd('Infinity'::numeric, 2), gcd('NaN'::numeric, 2), lcm('Infinity'::numeric, 2);

-- ---------------------------------------------------------------------------
-- Transcendentals
-- ---------------------------------------------------------------------------
SELECT sqrt('Infinity'::numeric), sqrt('NaN'::numeric);
SELECT sqrt('-Infinity'::numeric);
SELECT ln('Infinity'::numeric), ln('NaN'::numeric);
SELECT ln('-Infinity'::numeric);
SELECT ln(0::numeric);
SELECT ln(-1::numeric);
SELECT log('Infinity'::numeric), log('NaN'::numeric);
SELECT log('-Infinity'::numeric);
SELECT log(0::numeric);
SELECT exp('Infinity'::numeric), exp('-Infinity'::numeric), exp('NaN'::numeric);

-- power: POSIX pow(3) rules — NaN^0 and 1^NaN are both 1
SELECT power('NaN'::numeric, 0), power(1::numeric, 'NaN'::numeric), power('NaN'::numeric, 2);
SELECT power(2::numeric, 'NaN'::numeric), power(0::numeric, 'NaN'::numeric);
SELECT power('Infinity'::numeric, 2), power('Infinity'::numeric, 0), power('Infinity'::numeric, -2);
SELECT power('Infinity'::numeric, 'Infinity'::numeric), power('Infinity'::numeric, '-Infinity'::numeric);
SELECT power('-Infinity'::numeric, 2), power('-Infinity'::numeric, 3), power('-Infinity'::numeric, 0);
SELECT power('-Infinity'::numeric, -2), power('-Infinity'::numeric, -3);
SELECT power('-Infinity'::numeric, 'Infinity'::numeric), power('-Infinity'::numeric, '-Infinity'::numeric);
SELECT power('-Infinity'::numeric, 4.5);
SELECT power(2::numeric, 'Infinity'::numeric), power(0.5::numeric, 'Infinity'::numeric), power(1::numeric, 'Infinity'::numeric);
SELECT power(2::numeric, '-Infinity'::numeric), power(0.5::numeric, '-Infinity'::numeric), power(0::numeric, 'Infinity'::numeric);
SELECT power(-1::numeric, 'Infinity'::numeric), power(-2::numeric, 'Infinity'::numeric), power(-0.5::numeric, 'Infinity'::numeric);
SELECT power(0::numeric, '-Infinity'::numeric);
SELECT 'Infinity'::numeric ^ 2, 2 ^ 'Infinity'::numeric;

-- ---------------------------------------------------------------------------
-- Comparison and ordering: NaN = NaN and NaN > Infinity
-- ---------------------------------------------------------------------------
SELECT 'NaN'::numeric = 'NaN'::numeric, 'NaN'::numeric > 'Infinity'::numeric;
SELECT 'NaN'::numeric < 'Infinity'::numeric, 'NaN'::numeric <> 1;
SELECT 'Infinity'::numeric = 'Infinity'::numeric, '-Infinity'::numeric < 0;
SELECT 'Infinity'::numeric > 1e1000::numeric, '-Infinity'::numeric < -1e1000::numeric;
SELECT 'Infinity'::numeric >= 'Infinity'::numeric, '-Infinity'::numeric <= '-Infinity'::numeric;

CREATE TABLE nspec (id int4, v numeric);
INSERT INTO nspec VALUES (1, 'NaN'), (2, 'Infinity'), (3, '-Infinity'), (4, 0), (5, 1), (6, -1), (7, 'NaN');
SELECT id, v FROM nspec ORDER BY v, id;
SELECT DISTINCT v FROM nspec ORDER BY v;
SELECT v, count(*) FROM nspec GROUP BY v ORDER BY v;
SELECT count(DISTINCT v) FROM nspec;
SELECT v FROM nspec WHERE v = 'NaN'::numeric ORDER BY id;
SELECT v FROM nspec WHERE v > 1 ORDER BY v;
SELECT max(v), min(v) FROM nspec;

-- ---------------------------------------------------------------------------
-- Aggregates
-- ---------------------------------------------------------------------------
CREATE TABLE nagg (g int4, v numeric);
INSERT INTO nagg VALUES (1, 'Infinity'), (1, 1), (1, 2);
INSERT INTO nagg VALUES (2, 'Infinity'), (2, '-Infinity');
INSERT INTO nagg VALUES (3, 'NaN'), (3, 1);
INSERT INTO nagg VALUES (4, 'Infinity'), (4, 'Infinity');
SELECT g, sum(v), avg(v), min(v), max(v), count(v) FROM nagg GROUP BY g ORDER BY g;
SELECT g, variance(v), stddev(v), var_pop(v), stddev_pop(v) FROM nagg GROUP BY g ORDER BY g;
SELECT sum(v), avg(v), min(v), max(v) FROM nagg;
SELECT variance(v), stddev(v) FROM nagg;

-- ---------------------------------------------------------------------------
-- Casts
-- ---------------------------------------------------------------------------
SELECT 'NaN'::numeric::float8, 'Infinity'::numeric::float8, '-Infinity'::numeric::float8;
SELECT 'NaN'::numeric::float4, 'Infinity'::numeric::float4, '-Infinity'::numeric::float4;
SELECT 'NaN'::float8::numeric, 'Infinity'::float8::numeric, '-Infinity'::float8::numeric;
SELECT 'NaN'::float4::numeric, 'Infinity'::float4::numeric, '-Infinity'::float4::numeric;
SELECT 'NaN'::numeric::int2;
SELECT 'NaN'::numeric::int4;
SELECT 'NaN'::numeric::int8;
SELECT 'Infinity'::numeric::int2;
SELECT 'Infinity'::numeric::int4;
SELECT 'Infinity'::numeric::int8;
SELECT '-Infinity'::numeric::int2;
SELECT '-Infinity'::numeric::int4;
SELECT '-Infinity'::numeric::int8;
-- typmod: NaN passes any numeric(p,s); an infinity is 22003
SELECT 'NaN'::numeric::numeric(10,2);
SELECT 'Infinity'::numeric::numeric(10,2);
SELECT '-Infinity'::numeric::numeric(4,4);
-- to_jsonb has no non-finite number, so PostgreSQL emits a JSON string
SELECT to_jsonb('NaN'::numeric), to_jsonb('Infinity'::numeric), to_jsonb('-Infinity'::numeric);

-- ---------------------------------------------------------------------------
-- to_char lays the numeric_out spelling into the digit grid
-- ---------------------------------------------------------------------------
SELECT to_char('NaN'::numeric, '999'), to_char('Infinity'::numeric, '999'), to_char('-Infinity'::numeric, '999');
SELECT to_char('NaN'::numeric, '99'), to_char('NaN'::numeric, '99999'), to_char('NaN'::numeric, '0999');
SELECT to_char('NaN'::numeric, '999.99'), to_char('NaN'::numeric, '9.9'), to_char('NaN'::numeric, 'FM999.999');
SELECT to_char('Infinity'::numeric, '99999999'), to_char('-Infinity'::numeric, '99999999');
SELECT to_char('Infinity'::numeric, 'FM999.999'), to_char('-Infinity'::numeric, 'FM999.999');
SELECT to_char('NaN'::numeric, 'S999'), to_char('-Infinity'::numeric, 'S999'), to_char('NaN'::numeric, 'MI999');
SELECT to_char('-Infinity'::numeric, 'MI999'), to_char('NaN'::numeric, 'L999'), to_char('-Infinity'::numeric, 'L999');
SELECT to_char('NaN'::numeric, '999PR'), to_char('-Infinity'::numeric, '999PR'), to_char('Infinity'::numeric, '999PR');
SELECT to_char('NaN'::numeric, '99V99'), to_char('Infinity'::numeric, '99V99');
SELECT to_char('Infinity'::numeric, '999TH');

-- ---------------------------------------------------------------------------
-- Storage: row encoding, defaults, and unique index keys round-trip a special
-- ---------------------------------------------------------------------------
CREATE TABLE nstore (id int4, v numeric DEFAULT 'NaN'::numeric, w numeric(6,3));
INSERT INTO nstore (id) VALUES (1);
INSERT INTO nstore VALUES (2, 'Infinity', 1.5), (3, '-Infinity', 2.25), (4, 12.5, 3.5);
SELECT id, v, w FROM nstore ORDER BY id;
INSERT INTO nstore VALUES (5, 'NaN', 'NaN');
SELECT id, v, w FROM nstore ORDER BY id;
INSERT INTO nstore VALUES (6, 1, 'Infinity');
UPDATE nstore SET v = 'NaN'::numeric WHERE id = 4;
SELECT id, v FROM nstore WHERE v = 'NaN'::numeric ORDER BY id;
DELETE FROM nstore WHERE v = '-Infinity'::numeric;
SELECT id, v FROM nstore ORDER BY id;

CREATE TABLE nuniq (v numeric UNIQUE);
INSERT INTO nuniq VALUES ('NaN');
INSERT INTO nuniq VALUES ('NaN');
INSERT INTO nuniq VALUES ('Infinity'), ('-Infinity'), (1.0);
INSERT INTO nuniq VALUES ('Infinity');
INSERT INTO nuniq VALUES (1.00);
SELECT v FROM nuniq ORDER BY v;

DROP TABLE nuniq;
DROP TABLE nstore;
DROP TABLE nagg;
DROP TABLE nspec;
