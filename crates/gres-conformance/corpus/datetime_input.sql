-- Date/time literal input breadth, special values, and output formatting,
-- diffed against PostgreSQL 18.4.
--
-- Scope: the spellings PostgreSQL's DecodeDateTime accepts for `date`,
-- `time`, `timestamp` and `timestamptz`; the `infinity` / `-infinity` /
-- `epoch` reserved values; the DateStyle field-order setting; and the
-- SQLSTATEs a malformed or out-of-range literal raises.
--
-- Exclusions (intentional):
--   * `now` / `today` / `tomorrow` / `yesterday` resolve against the clock, so
--     they are only exercised through relations that hold between them.
--   * Dates outside jiff's year range (PostgreSQL reaches 5874897 AD) and the
--     `24:00:00` reading of `time`, which has no jiff representation.
--
-- The session is pinned to UTC so every timestamptz renders with a stable +00.

SET TIME ZONE 'UTC';
SET DateStyle TO 'ISO, MDY';

-- ---------------------------------------------------------------------------
-- Special values: representation, ordering and identity
-- ---------------------------------------------------------------------------

SELECT 'infinity'::date, '-infinity'::date;
SELECT 'infinity'::timestamp, '-infinity'::timestamp;
SELECT 'infinity'::timestamptz, '-infinity'::timestamptz;
SELECT timestamp 'infinity' = timestamp '+infinity' AS t;
SELECT date 'infinity' = date '+infinity' AS t;
SELECT 'infinity'::date > '2000-01-01'::date AS t;
SELECT '-infinity'::date < '2000-01-01'::date AS t;
SELECT 'infinity'::timestamp > '2000-01-01'::timestamp AS t;
SELECT '-infinity'::timestamptz < '2000-01-01'::timestamptz AS t;
SELECT isfinite('infinity'::date), isfinite('-infinity'::date), isfinite('2000-01-01'::date);
SELECT isfinite('infinity'::timestamp), isfinite('2000-01-01'::timestamp);
SELECT isfinite('infinity'::timestamptz), isfinite('2000-01-01'::timestamptz);
SELECT isfinite(interval '1 day'), isfinite(interval 'infinity');
SELECT 'epoch'::date, 'epoch'::timestamp, 'epoch'::timestamptz;
SELECT 'allballs'::time;

-- Ordering puts the two non-finite values at the ends.
SELECT d FROM (VALUES ('infinity'::timestamp), ('2000-01-01'), ('-infinity')) v(d) ORDER BY d;
SELECT max(d), min(d) FROM (VALUES ('infinity'::date), ('2000-01-01'), ('-infinity')) v(d);

-- Casts carry infinity through unchanged.
SELECT (date 'infinity')::timestamp, (date '-infinity')::timestamp;
SELECT (date 'infinity')::timestamptz, (date '-infinity')::timestamptz;
SELECT (timestamp 'infinity')::date, (timestamp '-infinity')::date;
SELECT (timestamp 'infinity')::timestamptz, (timestamptz '-infinity')::timestamp;
SELECT (timestamptz 'infinity')::date;
SELECT (timestamp 'infinity')::time;

-- Arithmetic propagates it, and two cancelling infinities are an error.
SELECT date 'infinity' + 1, date '-infinity' - 1;
SELECT timestamp 'infinity' + interval '1 day';
SELECT timestamp '-infinity' - interval '1 day';
SELECT timestamp 'infinity' - timestamp '1995-08-06 12:12:12';
SELECT timestamp '-infinity' - timestamp '1995-08-06 12:12:12';
SELECT timestamp 'infinity' - timestamp '-infinity';
SELECT timestamp '-infinity' - timestamp 'infinity';
SELECT timestamp 'infinity' - timestamp 'infinity';
SELECT date 'infinity' - date '2000-01-01';
SELECT date_trunc('week', timestamp 'infinity');
SELECT date_trunc('day', timestamptz '-infinity');

-- extract splits into monotonic fields (±Infinity) and oscillating ones (NULL).
SELECT extract(epoch from timestamp 'infinity'), extract(year from timestamp 'infinity');
SELECT extract(epoch from timestamp '-infinity'), extract(century from timestamp '-infinity');
SELECT extract(day from timestamp 'infinity') IS NULL AS t;
SELECT extract(month from date 'infinity') IS NULL AS t;
SELECT extract(julian from date 'infinity'), extract(isoyear from date '-infinity');

-- ---------------------------------------------------------------------------
-- Input breadth: the documented timestamp spellings
-- ---------------------------------------------------------------------------

SELECT 'Mon Feb 10 17:32:01 1997 PST'::timestamp;
SELECT 'Mon Feb 10 17:32:01.000001 1997 PST'::timestamp;
SELECT 'Mon Feb 10 17:32:01.4 1997 PST'::timestamp;
SELECT '1997-01-02'::timestamp;
SELECT '1997-01-02 03:04:05'::timestamp;
SELECT '1997-02-10 17:32:01-08'::timestamp;
SELECT '1997-02-10 17:32:01-0800'::timestamp;
SELECT '1997-02-10 17:32:01 -08:00'::timestamp;
SELECT '19970210 173201 -0800'::timestamp;
SELECT '2001-09-22T18:19:20'::timestamp;
SELECT '2000-03-15 08:14:01 GMT+8'::timestamp;
SELECT 'Feb 10 17:32:01 1997 -0800'::timestamp;
SELECT 'Feb 10 17:32:01 1997'::timestamp;
SELECT 'Feb 10 5:32PM 1997'::timestamp;
SELECT '1997/02/10 17:32:01-0800'::timestamp;
SELECT '1997-02-10 17:32:01 PST'::timestamp;
SELECT 'Feb-10-1997 17:32:01 PST'::timestamp;
SELECT '02-10-1997 17:32:01 PST'::timestamp;
SELECT '19970210 173201 PST'::timestamp;
SELECT '1997.041 17:32:01 UTC'::timestamp;
SELECT '19970210 173201 America/New_York'::timestamp;
SELECT 'Feb 16 17:32:01 0097 BC'::timestamp;
SELECT 'Feb 16 17:32:01 0097'::timestamp;
SELECT 'Feb 16 17:32:01 0597'::timestamp;
SELECT 'Feb 16 17:32:01 2097'::timestamp;
SELECT 'Feb 29 17:32:01 1996'::timestamp;
SELECT '1999-12-31 24:00:00'::timestamp;
SELECT '2001-09-22T18:19:20Z'::timestamp;

-- The same literals as timestamptz, where the zone fixes the instant.
SELECT 'Mon Feb 10 17:32:01 1997 PST'::timestamptz;
SELECT '1997-02-10 17:32:01-08'::timestamptz;
SELECT '2000-03-15 08:14:01 GMT+8'::timestamptz;
SELECT '2000-03-15 13:14:02 GMT-1'::timestamptz;
SELECT '2000-03-15 03:14:04 PST+8'::timestamptz;
SELECT '2000-03-15 02:14:05 MST+7:00'::timestamptz;
SELECT '19970210 173201 America/New_York'::timestamptz;
SELECT '2011-03-27 00:00:00 MSK'::timestamptz;
SELECT '2011-03-27 00:00:00 Europe/Moscow'::timestamptz;
SELECT '2014-10-26 00:00:00 MSK'::timestamptz;
SELECT '2001-09-22T18:19:20Z'::timestamptz;
SELECT 'Feb 16 17:32:01 0097 BC'::timestamptz;
SELECT '1997-02-10 17:32:01+05:30'::timestamptz;

-- date spellings
SELECT 'January 8, 1999'::date;
SELECT '1999-01-08'::date;
SELECT '1/8/1999'::date;
SELECT '19990108'::date;
SELECT '990108'::date;
SELECT '1999.008'::date;
SELECT 'J2451187'::date;
SELECT 'January 8, 99 BC'::date;
SELECT '1999-Jan-08'::date;
SELECT '08-Jan-1999'::date;
SELECT 'Jan-08-1999'::date;
SELECT '1999 Jan 08'::date;
SELECT '08 Jan 1999'::date;
SELECT 'Jan 08 1999'::date;
SELECT '01-08-1999'::date;
SELECT '01 08 1999'::date;
SELECT '01/02/03'::date;
SELECT '2040-04-10 BC'::date;
SELECT '4714-11-24 BC'::date;

-- time spellings: a leading date and a trailing zone are accepted and dropped.
SELECT '00:00'::time;
SELECT '02:03 PST'::time;
SELECT '11:59 EDT'::time;
SELECT '11:59:59.99 PM'::time;
SELECT '2003-03-07 15:36:39 America/New_York'::time;
SELECT '23:59:59.999999'::time;
SELECT '12:34.5'::time;

-- ---------------------------------------------------------------------------
-- DateStyle decides an otherwise ambiguous all-numeric date
-- ---------------------------------------------------------------------------

SET datestyle TO ymd;
SELECT date '99 01 08';
SELECT date '1999 01 08';
SELECT date '01/02/03';
SELECT date '1999-01-08';
SELECT date '19990108';
SELECT date 'January 8, 1999';

SET datestyle TO dmy;
SELECT date '08 01 99';
SELECT date '08 01 1999';
SELECT date '18/1/1999';
SELECT date '01/02/03';
SELECT date '1999-01-08';

SET datestyle TO mdy;
SELECT date '01 08 99';
SELECT date '01 08 1999';
SELECT date '1/18/1999';
SELECT date '01/02/03';
SELECT date '1999-01-08';
RESET datestyle;

-- ---------------------------------------------------------------------------
-- Malformed and out-of-range literals: the SQLSTATE is what distinguishes them
-- ---------------------------------------------------------------------------

SELECT 'garbage'::date;
SELECT 'garbage'::timestamp;
SELECT 'garbage'::time;
SELECT '2023-02-29'::date;
SELECT 'Feb 29 17:32:01 1997'::timestamp;
SELECT TIME '25:00:00';
SELECT TIME '24:01:00';
SELECT TIME '24:00:00.01';
SELECT TIME '23:59:60.01';
SELECT '2001-01-01 25:00:00'::timestamp;
SELECT 'Feb 16 17:32:01 -0097'::timestamp;
SELECT '19970710 173201 America/Does_not_exist'::timestamp;
SELECT '4714-11-23 BC'::date;
SELECT '15:36:39 America/New_York'::time;
SELECT 'infinity'::time;

-- ---------------------------------------------------------------------------
-- Output formatting: BC era suffix, fractional-second trimming, zone rendering
-- ---------------------------------------------------------------------------

SELECT ('0097-02-16 17:32:01 BC'::timestamp)::text;
SELECT ('0001-01-01 BC'::date)::text;
SELECT ('2024-01-15 12:00:00.450000'::timestamp)::text;
SELECT ('2024-01-15 12:00:00.500000'::timestamp)::text;
SELECT ('2024-01-15 12:00:00.000001'::timestamp)::text;
SELECT ('2024-01-15 12:00:00.000000'::timestamp)::text;
SELECT ('2024-01-15 12:00:00+05:30'::timestamptz)::text;
SELECT ('0097-02-16 17:32:01 BC'::timestamptz)::text;

-- ---------------------------------------------------------------------------
-- extract returns numeric at PostgreSQL's fixed scales; date_part returns float8
-- ---------------------------------------------------------------------------

SELECT extract(epoch from timestamp '2024-01-15');
SELECT extract(epoch from timestamptz '2024-01-15Z');
SELECT extract(epoch from date '2024-01-15');
SELECT extract(epoch from time '01:02:03.5');
SELECT extract(epoch from interval '1 day');
SELECT extract(second from timestamp '2024-01-15 01:02:03.5');
SELECT extract(second from interval '3.25 sec');
SELECT extract(milliseconds from timestamp '2024-01-15 00:00:03.5');
SELECT extract(microseconds from timestamp '2024-01-15 00:00:03.5');
SELECT extract(julian from date '2020-08-11');
SELECT date_part('second', timestamp '2024-01-15 01:02:03.5');
SELECT date_part('epoch', timestamp '2024-01-15');
SELECT pg_typeof(extract(epoch from date '2024-01-15'));
SELECT pg_typeof(date_part('epoch', date '2024-01-15'));

-- A date has no clock or zone, so those units are refused outright (0A000),
-- while a word that is not a unit at all is 22023.
SELECT EXTRACT(SECOND FROM DATE '2020-08-11');
SELECT EXTRACT(HOUR FROM DATE '2020-08-11');
SELECT EXTRACT(TIMEZONE FROM DATE '2020-08-11');
SELECT EXTRACT(FORTNIGHT FROM TIME '01:02:03');
SELECT EXTRACT(DAY FROM TIME '01:02:03');

-- Century and millennium count backwards through the astronomical year 0.
SELECT EXTRACT(CENTURY FROM DATE '0101-12-31 BC');
SELECT EXTRACT(CENTURY FROM DATE '0100-12-31 BC');
SELECT EXTRACT(CENTURY FROM DATE '0001-12-31 BC');
SELECT EXTRACT(CENTURY FROM DATE '0001-01-01');
SELECT EXTRACT(MILLENNIUM FROM DATE '0001-12-31 BC');
SELECT EXTRACT(DECADE FROM DATE '0001-01-01 BC');
SELECT EXTRACT(DECADE FROM DATE '0002-12-31 BC');

-- ---------------------------------------------------------------------------
-- date_trunc, date_bin and AT TIME ZONE
-- ---------------------------------------------------------------------------

SELECT date_trunc('day', date '2024-01-15'), pg_typeof(date_trunc('day', date '2024-01-15'));
SELECT date_trunc('day', timestamp '2024-01-15 10:00');
SELECT date_trunc('week', timestamp '2004-02-29 15:44:17.71393');
SELECT date_trunc('timezone', timestamp '2004-02-29 15:44:17.71393');
SELECT date_trunc('ago', timestamp '2004-02-29 15:44:17.71393');
SELECT date_bin('5 min'::interval, timestamp '2020-02-01 01:01:01', timestamp '2020-02-01 00:02:30');
SELECT date_bin('30 minutes'::interval, timestamp '2024-02-01 15:00:00', timestamp '2024-02-01 17:00:00');
SELECT date_bin('15 minutes'::interval, timestamp '2020-02-11 15:44:17.71393', timestamp '2001-01-01');
SELECT date_bin('5 months'::interval, timestamp '2020-02-01 01:01:01', timestamp '2001-01-01');
SELECT date_bin('0 days'::interval, timestamp '1970-01-01 01:00:00', timestamp '1970-01-01 00:00:00');
SELECT date_bin('-2 days'::interval, timestamp '1970-01-01 01:00:00', timestamp '1970-01-01 00:00:00');
SELECT timestamp '2001-01-01 10:00' AT TIME ZONE 'UTC';
SELECT timestamp '2011-03-27 00:00:00' AT TIME ZONE 'MSK';
SELECT timestamp '2011-03-27 00:00:00' AT TIME ZONE 'Europe/Moscow';
SELECT timestamptz '2001-01-01 10:00Z' AT TIME ZONE 'America/New_York';
SELECT timezone('UTC', timestamp '2001-01-01 10:00');
SELECT timestamp '2001-01-01 10:00' AT TIME ZONE 'Nowhere/Nothing';
