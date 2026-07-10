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
