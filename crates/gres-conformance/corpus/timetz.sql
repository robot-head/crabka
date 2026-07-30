-- `time with time zone` (OID 1266), diffed against PostgreSQL 18.4.
--
-- Scope: parsing, output, ordering by the UTC-equivalent instant, the casts
-- PostgreSQL defines, interval arithmetic, AT TIME ZONE, extract, and storage
-- round-tripping through a table.
--
-- Exclusions (intentional): the `24:00:00` reading, which has no jiff
-- representation, and the fractional-seconds typmod, which crabka parses and
-- discards (every value is stored at microsecond resolution).

SET TIME ZONE 'UTC';

-- ---------------------------------------------------------------------------
-- Input and output
-- ---------------------------------------------------------------------------

SELECT '00:01 PDT'::timetz;
SELECT '01:00 PDT'::timetz;
SELECT '07:07 PST'::timetz;
SELECT '08:08 EDT'::timetz;
SELECT '11:59:59.99 PM PDT'::timetz;
SELECT '23:59:59.999999 PDT'::timetz;
SELECT '12:00:00'::timetz;
SELECT '12:00:00-05'::timetz;
SELECT '12:00:00+05:30'::timetz;
SELECT '12:00:00 UTC'::timetz;
SELECT '2003-03-07 15:36:39 America/New_York'::timetz;
SELECT '2003-07-07 15:36:39 America/New_York'::timetz;
SELECT ('12:34:56-05'::timetz)::text;
SELECT 'time with time zone'::text;
SELECT pg_typeof('12:00:00-05'::timetz);

-- Failures: a zone name needs a date to resolve, and an out-of-range clock
-- field is a different SQLSTATE from malformed text.
SELECT '15:36:39 America/New_York'::timetz;
SELECT '15:36:39 m2'::timetz;
SELECT '15:36:39 MSK m2'::timetz;
SELECT '25:00:00 PDT'::timetz;
SELECT '24:01:00 PDT'::timetz;
SELECT '23:59:60.01 PDT'::timetz;
SELECT 'garbage'::timetz;

-- ---------------------------------------------------------------------------
-- Ordering and equality follow the UTC-equivalent instant, not the clock
-- ---------------------------------------------------------------------------

SELECT '12:00:00-05'::timetz = '17:00:00+00'::timetz AS t;
SELECT '12:00:00-05'::timetz > '12:00:00+00'::timetz AS t;
SELECT '12:00:00-05'::timetz < '18:00:00+00'::timetz AS t;
SELECT f1 FROM (VALUES ('12:00:00-05'::timetz), ('12:00:00+00'), ('12:00:00+05')) v(f1) ORDER BY f1;
SELECT max(f1), min(f1) FROM (VALUES ('12:00:00-05'::timetz), ('12:00:00+00'), ('12:00:00+05')) v(f1);
SELECT count(DISTINCT f1) FROM (VALUES ('12:00:00-05'::timetz), ('17:00:00+00')) v(f1);

-- ---------------------------------------------------------------------------
-- Casts
-- ---------------------------------------------------------------------------

SELECT ('12:34:56-05'::timetz)::time;
SELECT ('12:34:56'::time)::timetz;
SELECT (timestamptz '2020-05-26 13:30:25-04')::timetz;
SELECT ('12:34:56-05'::timetz)::text;
SELECT ('12:34:56-05'::text)::timetz;
SELECT (timestamp '2020-05-26 13:30:25')::timetz;

-- ---------------------------------------------------------------------------
-- Arithmetic and AT TIME ZONE
-- ---------------------------------------------------------------------------

SELECT timetz '11:27:42' + interval '1 hour';
SELECT timetz '11:27:42-05' + interval '1 hour';
SELECT timetz '11:27:42-05' - interval '1 hour';
SELECT timetz '23:27:42-05' + interval '1 hour';
SELECT timetz '11:27:42-05' AT TIME ZONE 'UTC';
SELECT timezone('UTC', timetz '11:27:42-05');

-- ---------------------------------------------------------------------------
-- extract / date_part
-- ---------------------------------------------------------------------------

SELECT EXTRACT(MICROSECOND FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT EXTRACT(MILLISECOND FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT EXTRACT(SECOND      FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT EXTRACT(MINUTE      FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT EXTRACT(HOUR        FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT EXTRACT(TIMEZONE    FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04:30');
SELECT EXTRACT(TIMEZONE_HOUR   FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04:30');
SELECT EXTRACT(TIMEZONE_MINUTE FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04:30');
SELECT EXTRACT(EPOCH       FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT EXTRACT(DAY         FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT EXTRACT(FORTNIGHT   FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT date_part('microsecond', TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT date_part('second',      TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT date_part('epoch',       TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');

-- ---------------------------------------------------------------------------
-- Storage: the value survives a table round trip and orders the same way
-- ---------------------------------------------------------------------------

CREATE TABLE timetz_corpus (id int, f1 time with time zone);
INSERT INTO timetz_corpus VALUES (1, '00:01 PDT');
INSERT INTO timetz_corpus VALUES (2, '12:00:00-05');
INSERT INTO timetz_corpus VALUES (3, '17:00:00+00');
INSERT INTO timetz_corpus VALUES (4, '2003-03-07 15:36:39 America/New_York');
INSERT INTO timetz_corpus VALUES (5, NULL);
SELECT id, f1 FROM timetz_corpus ORDER BY id;
SELECT id, f1 FROM timetz_corpus WHERE f1 IS NOT NULL ORDER BY f1, id;
SELECT id FROM timetz_corpus WHERE f1 = '12:00:00-05' ORDER BY id;
SELECT id FROM timetz_corpus WHERE f1 > '05:06:07-07' ORDER BY id;
SELECT count(*) FROM timetz_corpus WHERE f1 IS NULL;
UPDATE timetz_corpus SET f1 = '09:00:00+02'::timetz WHERE id = 1;
SELECT id, f1 FROM timetz_corpus WHERE id = 1;
DELETE FROM timetz_corpus WHERE id = 5;
SELECT count(*) FROM timetz_corpus;
DROP TABLE timetz_corpus;

-- timetz +/- interval: a modular 24-hour shift of the local time that keeps the
-- operand's zone offset, including when it wraps past midnight in either
-- direction. The runtime arithmetic existed; only the plan-time type rule was
-- missing, so these were 42883.
SELECT '10:30:00-05'::timetz + '1 hour'::interval;
SELECT '10:30:00-05'::timetz - '1 hour'::interval;
SELECT '23:30:00+02'::timetz + '2 hours'::interval;
SELECT '00:30:00+02'::timetz - '2 hours'::interval;
SELECT '1 hour'::interval + '10:30:00-05'::timetz;
