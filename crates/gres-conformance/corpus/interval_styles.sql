-- IntervalStyle output fidelity: `interval_out` renders one stored value four
-- different ways, and the setting has to reach every place an interval is
-- spelled -- the wire, a `::text` cast, `||`, an array, a composite, and JSON.
--
-- Every statement here is diffed against PostgreSQL 18.4; the file resets the
-- GUCs it touches so the corpus files that run after it are unaffected.

SET TimeZone = 'UTC';

-- ---------------------------------------------------------------------------
-- postgres (the default): each field carries its own sign, and a positive field
-- gets an explicit `+` when the previous non-zero field was negative.
-- ---------------------------------------------------------------------------
SET IntervalStyle = 'postgres';
SELECT interval '1 year 2 months 3 days 4 hours 5 minutes 6 seconds';
SELECT interval '-1 years -2 mons -3 days -04:05:06';
SELECT interval '0';
SELECT interval '1 day', interval '-1 day', interval '1 mon', interval '-1 mon';
SELECT interval '1 year', interval '-1 year', interval '11 mons', interval '-11 mons';
SELECT interval '3 days' - interval '04:05:06';
SELECT interval '-3 days' + interval '04:05:06';
SELECT interval '1 mon' - interval '1 day';
SELECT interval '-1 mon' + interval '1 day';
SELECT interval '00:00:00.25', interval '-00:00:00.25', interval '10:00:00.5';
SELECT interval '24:00:00', interval '-24:00:00';
SELECT interval '00:00:00.000001', interval '-00:00:00.000001';
SELECT interval '178000000 years', interval '-178000000 years';
SELECT interval 'infinity', interval '-infinity';
SELECT interval '-2147483647 months -2147483648 days -9223372036854775808 us';

-- ---------------------------------------------------------------------------
-- postgres_verbose: `@ ... [ago]`, where the sign of the FIRST non-zero field
-- decides `ago` and flips every other field.
-- ---------------------------------------------------------------------------
SET IntervalStyle = 'postgres_verbose';
SELECT interval '1 year 2 months 3 days 4 hours 5 minutes 6 seconds';
SELECT interval '-1 years -2 mons -3 days -04:05:06';
SELECT interval '0';
SELECT interval '1 day', interval '-1 day', interval '1 mon', interval '-1 mon';
SELECT interval '1 year', interval '-1 year', interval '11 mons', interval '-11 mons';
SELECT interval '3 days' - interval '04:05:06';
SELECT interval '-3 days' + interval '04:05:06';
SELECT interval '1 mon' - interval '1 day';
SELECT interval '-1 mon' + interval '1 day';
SELECT interval '1 mon' - interval '1 hour';
SELECT interval '1 mon' - interval '1 minute';
SELECT interval '1 mon' - interval '1 second';
SELECT interval '00:00:00.25', interval '-00:00:00.25', interval '10:00:00.5';
SELECT interval '24:00:00', interval '-24:00:00';
SELECT interval '00:00:00.000001', interval '-00:00:00.000001';
SELECT interval '00:00:59.999999';
SELECT interval '178000000 years', interval '-178000000 years';
SELECT interval 'infinity', interval '-infinity';
SELECT interval '-2147483647 months -2147483648 days -9223372036854775808 us';

-- ---------------------------------------------------------------------------
-- sql_standard: one leading sign for a value that fits the standard's shape,
-- and an explicit sign on every component for one that does not.
-- ---------------------------------------------------------------------------
SET IntervalStyle = 'sql_standard';
SELECT interval '1 year 2 months 3 days 4 hours 5 minutes 6 seconds';
SELECT interval '-1 years -2 mons -3 days -04:05:06';
SELECT interval '0';
SELECT interval '1 day', interval '-1 day', interval '1 mon', interval '-1 mon';
SELECT interval '1 year', interval '-1 year', interval '11 mons', interval '-11 mons';
SELECT interval '3 days' - interval '04:05:06';
SELECT interval '-3 days' + interval '04:05:06';
SELECT interval '1 mon' - interval '1 day';
SELECT interval '-1 mon' + interval '1 day';
SELECT interval '1 year' + interval '00:00:01';
SELECT interval '-1 year' - interval '00:00:01';
SELECT interval '00:00:00.25', interval '-00:00:00.25', interval '10:00:00.5';
SELECT interval '24:00:00', interval '-24:00:00';
SELECT interval '00:00:00.000001', interval '-00:00:00.000001';
SELECT interval '178000000 years', interval '-178000000 years';
SELECT interval 'infinity', interval '-infinity';
SELECT interval '-2147483647 months -2147483648 days -9223372036854775808 us';

-- ---------------------------------------------------------------------------
-- iso_8601: `P[n]Y[n]M[n]DT[n]H[n]M[n]S`, zero fields dropped, `PT0S` for zero.
-- ---------------------------------------------------------------------------
SET IntervalStyle = 'iso_8601';
SELECT interval '1 year 2 months 3 days 4 hours 5 minutes 6 seconds';
SELECT interval '-1 years -2 mons -3 days -04:05:06';
SELECT interval '0';
SELECT interval '1 day', interval '-1 day', interval '1 mon', interval '-1 mon';
SELECT interval '1 year', interval '-1 year', interval '11 mons', interval '-11 mons';
SELECT interval '3 days' - interval '04:05:06';
SELECT interval '-3 days' + interval '04:05:06';
SELECT interval '1 mon' - interval '1 day';
SELECT interval '-1 mon' + interval '1 day';
SELECT interval '1 year' + interval '00:00:01';
SELECT interval '00:00:00.25', interval '-00:00:00.25', interval '10:00:00.5';
SELECT interval '24:00:00', interval '-24:00:00';
SELECT interval '00:00:00.000001', interval '-00:00:00.000001';
SELECT interval '178000000 years', interval '-178000000 years';
SELECT interval 'infinity', interval '-infinity';
SELECT interval '-2147483647 months -2147483648 days -9223372036854775808 us';

-- ---------------------------------------------------------------------------
-- The setting has to reach every rendering path, not just the wire.
-- ---------------------------------------------------------------------------
SET IntervalStyle = 'postgres_verbose';
SELECT (interval '1 day 2 hours')::text;
SELECT 'x' || interval '1 day 2 hours';
SELECT ARRAY[interval '1 day 2 hours', interval '-3 mons'];
SELECT (ARRAY[interval '1 day 2 hours'])::text;
SELECT ROW(interval '1 day 2 hours');
SELECT (ROW(interval '1 day 2 hours'))::text;
SELECT to_json(interval '1 day 2 hours');
SELECT to_jsonb(interval '1 day 2 hours');
SELECT row_to_json(ROW(interval '1 day 2 hours'));

SET IntervalStyle = 'iso_8601';
SELECT (interval '1 day 2 hours')::text;
SELECT 'x' || interval '1 day 2 hours';
SELECT ARRAY[interval '1 day 2 hours', interval '-3 mons'];
SELECT ROW(interval '1 day 2 hours');
SELECT to_json(interval '1 day 2 hours');

SET IntervalStyle = 'sql_standard';
SELECT (interval '1 day 2 hours')::text;
SELECT ARRAY[interval '1 day 2 hours', interval '-3 mons'];
SELECT ROW(interval '1 day 2 hours');

-- `to_char` of a non-finite interval is NULL whatever the template says.
SET IntervalStyle = 'postgres';
SELECT to_char(interval 'infinity', 'YYYY');
SELECT to_char(interval '-infinity', 'YYYY');
SELECT to_char(interval 'infinity', 'HH24:MI:SS');
SELECT to_char(interval 'infinity', 'YYYY') IS NULL;

RESET IntervalStyle;
RESET TimeZone;
