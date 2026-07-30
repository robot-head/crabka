-- SP37 (interval focus): interval literal output, arithmetic, and grouping,
-- diffed against PostgreSQL 18.
--
-- `interval` uses PostgreSQL's postgres IntervalStyle (the default): fields
-- are stored as months / days / microseconds separately — a `1 month` interval
-- is NOT normalised to `30 days` for storage, but the canonical-estimate
-- comparison (`'1 month' = '30 days'`) treats a month as 30 days, matching PG.
--
-- All values stay within jiff's calendar range (years 1..9999).

SET TIME ZONE 'UTC';

-- ---------------------------------------------------------------------------
-- Interval literals and output (various unit combinations)
-- ---------------------------------------------------------------------------
-- whole-day forms
SELECT INTERVAL '0 days';
SELECT INTERVAL '1 day';
SELECT INTERVAL '-1 day';
SELECT INTERVAL '2 days';
SELECT INTERVAL '7 days';

-- clock-only forms
SELECT INTERVAL '01:00:00';
SELECT INTERVAL '02:30:00';
SELECT INTERVAL '-01:30:00';
SELECT INTERVAL '00:00:01';
SELECT INTERVAL '00:00:00.5';

-- combined
SELECT INTERVAL '1 day 02:30:00';
SELECT INTERVAL '3 days 04:05:06';
SELECT INTERVAL '-3 days 04:05:06';

-- months and years
SELECT INTERVAL '1 month';
SELECT INTERVAL '2 months';
SELECT INTERVAL '1 year';
SELECT INTERVAL '1 year 2 months';
SELECT INTERVAL '2 years 3 months';

-- mixed month/day/clock
SELECT INTERVAL '1 year 2 months 3 days 04:05:06';

-- unit aliases the parser accepts
SELECT INTERVAL '2 hours 30 minutes';
SELECT INTERVAL '90 seconds';
SELECT INTERVAL '500 milliseconds';

-- ---------------------------------------------------------------------------
-- Interval arithmetic
-- ---------------------------------------------------------------------------
SELECT INTERVAL '1 day' + INTERVAL '2 hours';
SELECT INTERVAL '3 days' - INTERVAL '1 day';
SELECT INTERVAL '1 day' * 3;
SELECT INTERVAL '6 hours' / 2;
SELECT INTERVAL '1 month' + INTERVAL '15 days';
SELECT INTERVAL '2 years' - INTERVAL '6 months';
-- negation via multiplication
SELECT INTERVAL '1 day' * -1;

-- ---------------------------------------------------------------------------
-- Interval in timestamp arithmetic
-- ---------------------------------------------------------------------------
SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '1 month';
SELECT TIMESTAMP '2024-03-31 12:00:00' + INTERVAL '1 month';
SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '1 year 2 months 3 days';
SELECT TIMESTAMP '2024-07-15 13:45:06' - INTERVAL '1 month';

-- ---------------------------------------------------------------------------
-- Interval comparison (canonical-estimate order)
-- ---------------------------------------------------------------------------
-- These evaluate to boolean, which the conformance runner checks.
SELECT INTERVAL '1 day' < INTERVAL '1 month';
SELECT INTERVAL '24 hours' = INTERVAL '1 day';
SELECT INTERVAL '1 month' > INTERVAL '29 days';
SELECT INTERVAL '1 month' = INTERVAL '30 days';

-- ---------------------------------------------------------------------------
-- Interval GROUP BY and DISTINCT: PG groups by canonical estimate
-- (so '1 month' and '30 days' are ONE group; '24 hours' and '1 day' are ONE group)
-- ---------------------------------------------------------------------------
CREATE TABLE iv_grp (label text, iv interval);
INSERT INTO iv_grp VALUES
    ('a', INTERVAL '1 month'),
    ('b', INTERVAL '30 days'),
    ('c', INTERVAL '1 day'),
    ('d', INTERVAL '24 hours'),
    ('e', INTERVAL '2 days');

-- GROUP BY iv: expects 3 groups (1 month/30 days, 1 day/24 hours, 2 days)
SELECT iv, count(*) FROM iv_grp GROUP BY iv ORDER BY iv;

-- DISTINCT: same deduplication by canonical estimate
SELECT DISTINCT iv FROM iv_grp ORDER BY iv;

-- ---------------------------------------------------------------------------
-- extract from interval
-- ---------------------------------------------------------------------------
SELECT extract(year  FROM INTERVAL '2 years 3 months');
SELECT extract(month FROM INTERVAL '2 years 3 months');
SELECT extract(day   FROM INTERVAL '10 days 02:00:00');
SELECT extract(hour  FROM INTERVAL '10 days 02:30:00');
SELECT extract(epoch FROM INTERVAL '1 day');
SELECT extract(epoch FROM INTERVAL '1 month');

-- ---------------------------------------------------------------------------
-- Repeated interval fields (PostgreSQL rejects, it does not sum)
-- ---------------------------------------------------------------------------
-- PostgreSQL's decoder records which fields a literal already supplied; a second
-- one is 22007. A fractional second reaches the millisecond and microsecond
-- fields, and a clock term supplies hours through microseconds, so both collide
-- with a later sub-second term.
SELECT '1 second 2 seconds'::interval;
SELECT '10 milliseconds 20 milliseconds'::interval;
SELECT '5.5 seconds 3 milliseconds'::interval;
SELECT '3 milliseconds 5.5 seconds'::interval;
SELECT '1:20:05 5 microseconds'::interval;
SELECT '1:00 2:00'::interval;
SELECT '1 day 1 day'::interval;
SELECT '1 day 2 hours 3 hours'::interval;
SELECT '1 year 1 month 1 year'::interval;
SELECT '1 week 1 week'::interval;
SELECT '1 mon 1 month'::interval;
SELECT '1 decade 1 decade'::interval;
SELECT '1-2 3-4'::interval;
SELECT '@ 1 day 1 day ago'::interval;
SELECT '123 11'::interval;
SELECT '1 2 3'::interval;
SELECT '3 1-2'::interval;

-- the distinct-field literals that must still be accepted
SELECT '1 week 2 days'::interval;
SELECT '1 month 1 week'::interval;
SELECT '5 seconds 3 microseconds'::interval;
SELECT '5 milliseconds 3 microseconds'::interval;
SELECT '5.5 milliseconds 3 microseconds'::interval;
SELECT '1 minute 30 seconds'::interval;
SELECT '1.5 hours 30 minutes'::interval;
SELECT '1 decade 1 year'::interval;
SELECT '1 century 1 decade'::interval;
SELECT '1-2 3'::interval;
SELECT '1 day 2'::interval;
SELECT '1.5 days 01:00:00'::interval;

-- a bare quantity keeps its neighbour's unit, stepping to DAY only after an hour
SELECT interval '4 5' day to hour;
SELECT interval '1 2' hour;
SELECT interval '1 2:03' day to hour;
SELECT interval '1 2' day to minute;
SELECT interval '1 2' hour to minute;
SELECT interval '1 2' minute to second;
SELECT interval '1 2' day to second;
SELECT interval '1 2' hour to second;
SELECT interval '1 2' day;
SELECT interval '1 2' minute;
SELECT interval '1 2' year to month;
SELECT interval '123 11' day;

-- ---------------------------------------------------------------------------
-- ISO-8601 designators belong to one half of the duration
-- ---------------------------------------------------------------------------
SELECT 'P1Y2M3DT4H5M6S'::interval;
SELECT 'P0002-10-15T10:30:20'::interval;
SELECT 'PT1Y'::interval;
SELECT 'PT1W'::interval;
SELECT 'PT1D'::interval;
SELECT 'P1H'::interval;
SELECT 'P1S'::interval;
SELECT 'P1DT1D'::interval;

-- ---------------------------------------------------------------------------
-- Quantity precision, and the infinity encoding
-- ---------------------------------------------------------------------------
-- The whole part of a quantity is exact: a maximal interval must read back to
-- the microsecond, and must still be FINITE.
SELECT interval '2562047788.01521550194 hours';
SELECT interval '-2562047788.01521550222 hours';
SELECT interval '9223372036854.775807 seconds';
SELECT interval '-9223372036854.775808 seconds';
SELECT interval 'PT2562047788H54.775807S';
SELECT interval 'PT-2562047788H-54.775808S';
SELECT isfinite(interval '2562047788:00:54.775807');
SELECT isfinite(interval 'infinity');
SELECT interval 'infinity' > interval '2562047788:00:54.775807';
SELECT interval '-infinity' < interval '-2562047788:00:54.775808';
SELECT interval '.5 seconds';
SELECT interval '-.5 seconds';
SELECT interval '+.5 seconds';

-- ---------------------------------------------------------------------------
-- time / timetz cannot be shifted by an infinite interval
-- ---------------------------------------------------------------------------
SELECT time '11:27:42' + interval 'infinity';
SELECT time '11:27:42' + interval '-infinity';
SELECT time '11:27:42' - interval 'infinity';
SELECT time '11:27:42' - interval '-infinity';
SELECT interval 'infinity' + time '11:27:42';
SELECT timetz '11:27:42+02' + interval 'infinity';
SELECT timetz '11:27:42+02' + interval '-infinity';
SELECT timetz '11:27:42+02' - interval 'infinity';
SELECT timetz '11:27:42+02' - interval '-infinity';
SELECT time '11:27:42' + interval '1 hour';
SELECT timetz '11:27:42+02' - interval '1 hour';

-- Two FINITE operands that land exactly on the reserved non-finite encoding have
-- run out of range; they have not produced an infinity.
SELECT interval '2147483647 months 2147483647 days 9223372036854775806 us';
SELECT interval '-2147483648 months -2147483648 days -9223372036854775807 us';
SELECT -interval '-2147483647 months -2147483647 days -9223372036854775807 us';
SELECT interval '-2147483647 months -2147483647 days -9223372036854775807 us' + interval '-1 month -1 day -1 us';
SELECT interval '-2147483647 months -2147483647 days -9223372036854775807 us' - interval '1 month 1 day 1 us';
SELECT interval '2147483646 months 2147483646 days 9223372036854775806 us' + interval '1 month 1 day 1 us';
SELECT interval '2147483646 months 2147483646 days 9223372036854775806 us' - interval '-1 month -1 day -1 us';
SELECT interval 'infinity' + interval '1 day';
SELECT interval 'infinity' - interval 'infinity';
SELECT -interval 'infinity';

-- Labeled (named) arguments: `make_interval(years := 1)`. An unsupplied field
-- takes the function's own default of zero; a positional argument must precede
-- the labeled ones; a duplicate label is 42P08 and an unknown one 42883.
SELECT make_interval(years := 1);
SELECT make_interval(years := 1, months := 2);
SELECT make_interval(days := 5, hours := 3);
SELECT make_interval(secs := 1.5);
SELECT make_interval(mins := 90);
SELECT make_interval(weeks := 2);
SELECT make_interval(1, months := 2);
SELECT make_interval(years := 1, years := 2);
SELECT make_interval(nosuch := 1);
