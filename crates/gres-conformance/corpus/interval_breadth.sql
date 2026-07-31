-- `interval` input breadth and field ranges, diffed against PostgreSQL 18.4.
--
-- Scope: the unit spellings PostgreSQL accepts, the `@ … ago` verbose form, the
-- ISO-8601 designator and alternative forms, the `Y-M` and `D HH:MM:SS`
-- shorthands, the `INTERVAL '…' <field> [TO <field>]` qualifiers, the
-- `infinity` / `-infinity` values, and the `postgres` output style's sign
-- placement.
--
-- Exclusions (intentional): IntervalStyle settings other than the default
-- `postgres`, and `justify_*` on values whose fields would overflow int32.

SET TIME ZONE 'UTC';
SET IntervalStyle TO postgres;

-- ---------------------------------------------------------------------------
-- Unit spellings
-- ---------------------------------------------------------------------------

SELECT interval '1 millennium';
SELECT interval '1 century';
SELECT interval '10 decades';
SELECT interval '1 millennium 2 centuries';
SELECT interval '1 year';
SELECT interval '2 years 3 months';
SELECT interval '3 mons';
SELECT interval '1 week';
SELECT interval '1.5 weeks';
SELECT interval '10 days';
SELECT interval '5 hours';
SELECT interval '5 hrs';
SELECT interval '1 min';
SELECT interval '14 secs';
SELECT interval '250 milliseconds';
SELECT interval '250 microseconds';
SELECT interval '1 day 2 hours 3 mins 4 secs';
SELECT interval '1 year 2 mons 3 days 04:05:06.699999';
SELECT interval '0';
SELECT interval '10';

-- Shorthands and the verbose form.
SELECT interval '1-2';
SELECT interval '-1-2';
SELECT interval '1 2:03:04';
SELECT interval '@ 1 year 2 mons';
SELECT interval '@ 1 year 2 mons ago';
SELECT interval '@ 14 secs ago';
SELECT interval '@ 1 day 2 hours 3 mins 4 secs';

-- ISO 8601, both the designator form and the alternative all-numeric form.
SELECT interval 'P1Y2M3DT4H5M6S';
SELECT interval 'P1Y';
SELECT interval 'P3D';
SELECT interval 'PT4H5M6S';
SELECT interval 'P1W';
SELECT interval 'P0001-02-03T04:05:06';

-- ---------------------------------------------------------------------------
-- Field qualifiers supply the unit and truncate to the range's last field
-- ---------------------------------------------------------------------------

SELECT interval '1' year;
SELECT interval '1' month;
SELECT interval '1' day;
SELECT interval '90' minute;
SELECT interval '90' second;
SELECT interval '1.5' day;
SELECT interval '1' year to month;
SELECT interval '1-2' year to month;
SELECT interval '4 5' day to hour;
SELECT interval '1 2:03:04' day to second;
SELECT interval '2:03' hour to minute;
SELECT interval '2:03:04' hour to second;
SELECT interval '1 2:03' day to minute;
SELECT interval '1 year 2 mons 3 days 04:05:06' day to hour;
SELECT interval '1 year 2 mons 3 days 04:05:06' year to month;

-- ---------------------------------------------------------------------------
-- Non-finite intervals
-- ---------------------------------------------------------------------------

SELECT interval 'infinity';
SELECT interval '-infinity';
SELECT interval '+infinity';
SELECT -interval 'infinity';
SELECT interval 'infinity' + interval '1 day';
SELECT interval '-infinity' - interval '1 day';
SELECT interval 'infinity' - interval 'infinity';
SELECT interval 'infinity' * 2;
SELECT interval 'infinity' * -1;
SELECT interval '1 day' < interval 'infinity' AS t;
SELECT interval '-infinity' < interval '1 day' AS t;
SELECT justify_days(interval 'infinity');
SELECT justify_hours(interval '-infinity');
SELECT justify_interval(interval 'infinity');
SELECT timestamp '2001-01-01' + interval 'infinity';
SELECT timestamp '2001-01-01' + interval '-infinity';
SELECT extract(epoch from interval 'infinity');
SELECT i FROM (VALUES (interval 'infinity'), (interval '1 day'), (interval '-infinity')) v(i) ORDER BY i;

-- ---------------------------------------------------------------------------
-- Output: the `postgres` style's per-field sign placement
-- ---------------------------------------------------------------------------

SELECT interval '-3 days 4 hours 5 min 6 sec';
SELECT interval '3 days -4 hours';
SELECT interval '-1 year -2 mons +3 days 4:05:06';
SELECT interval '1 year -2 mons';
SELECT interval '-1 day 01:00:00';
SELECT interval '1 day -01:00:00';
SELECT -interval '1 day 01:00:00';
SELECT interval '00:00:00';
SELECT interval '1 second';
SELECT interval '2 seconds';
SELECT interval '-1 day';
SELECT interval '1 day';

-- ---------------------------------------------------------------------------
-- justify_days / justify_hours / justify_interval
-- ---------------------------------------------------------------------------

SELECT justify_days(interval '6 months 36 days 5 hours 4 minutes 3 seconds');
SELECT justify_hours(interval '6 months 3 days 52 hours 3 minutes 2 seconds');
SELECT justify_interval(interval '1 month -1 hour');
SELECT justify_interval(interval '-1 month 1 hour');
SELECT justify_days(interval '30 days');
SELECT justify_hours(interval '24 hours');
SELECT justify_interval(interval '1 mon 30 days');

-- ---------------------------------------------------------------------------
-- Malformed input
-- ---------------------------------------------------------------------------

SELECT interval 'garbage';
SELECT interval '';
SELECT interval '1 fortnight';
SELECT interval 'day';
