-- SP37: date/time types, diffed against PostgreSQL 18.
--
-- Scope: date (OID 1082), time (OID 1083), timestamp (OID 1114),
-- timestamptz (OID 1184), interval (OID 1186).
--
-- Exclusions (intentional):
--   * now() / current_date / current_timestamp / clock_timestamp() are
--     non-deterministic; their results cannot be diffed against a real PG
--     instance. They are verified via the executor::datetime wire test using
--     a FixedClock.
--   * timetz / time with time zone is unsupported (deferred).
--   * SAVEPOINT-level GUC stacking is deferred (no SAVEPOINT support yet).
--   * to_char / to_timestamp / to_date / make_date / make_interval /
--     justify_interval are deferred to SP38.
--
-- Session is pinned to UTC so all timestamptz literals render with a stable
-- offset (+00) and DST-sensitive assertions are reproducible.  The America/New_York
-- section uses stable-DST dates (Jan 15 = EST = -05:00; Jun 1 = EDT = -04:00;
-- these offsets have been stable since the 2007 US DST rule change).
--
-- All dates are within jiff's supported calendar range (years 1..9999).

SET TIME ZONE 'UTC';

-- ---------------------------------------------------------------------------
-- Table DDL: one column per date/time type
-- ---------------------------------------------------------------------------
CREATE TABLE dt_demo (
    id       int4,
    d        date,
    tm       time,
    ts       timestamp,
    tz       timestamptz,
    iv       interval
);

-- ---------------------------------------------------------------------------
-- INSERT: typed literals
-- ---------------------------------------------------------------------------
INSERT INTO dt_demo VALUES (
    1,
    DATE '2024-01-15',
    TIME '13:45:06',
    TIMESTAMP '2024-01-15 13:45:06',
    TIMESTAMPTZ '2024-01-15 13:45:06+00',
    INTERVAL '1 day 02:30:00'
);

-- INSERT: bare-string assignment coercion (text → type on the column)
INSERT INTO dt_demo VALUES (
    2,
    '2024-06-01',
    '08:00:00',
    '2024-06-01 08:00:00',
    '2024-06-01 12:00:00+00',
    '2 hours'
);

-- SELECT back: all five columns in insertion order
SELECT id, d, tm, ts, tz, iv FROM dt_demo ORDER BY id;

-- ---------------------------------------------------------------------------
-- Typed literal projections
-- ---------------------------------------------------------------------------
SELECT DATE '2024-01-15';
SELECT TIME '13:45:06';
SELECT TIMESTAMP '2024-01-15 13:45:06';
SELECT TIMESTAMPTZ '2024-01-15 13:45:06+00';
SELECT INTERVAL '1 day 02:30:00';

-- Fractional seconds round-trip (trailing-zero trimming)
SELECT TIMESTAMP '2024-06-30 23:59:59.5';
SELECT TIME '01:02:03.45';

-- Zero interval
SELECT INTERVAL '0 days';

-- ---------------------------------------------------------------------------
-- ORDER BY date / timestamp / timestamptz
-- ---------------------------------------------------------------------------
SELECT d FROM dt_demo ORDER BY d ASC;
SELECT ts FROM dt_demo ORDER BY ts DESC;
SELECT tz FROM dt_demo ORDER BY tz ASC;

-- ---------------------------------------------------------------------------
-- Arithmetic: date
-- ---------------------------------------------------------------------------
-- date + int → date (add days)
SELECT DATE '2024-01-01' + 31;
-- date - int → date (subtract days)
SELECT DATE '2024-02-01' - 1;
-- date - date → int4 (days between)
SELECT DATE '2024-02-01' - DATE '2024-01-01';
-- int + date → date (commutative)
SELECT 10 + DATE '2024-01-01';

-- ---------------------------------------------------------------------------
-- Arithmetic: timestamp + interval
-- ---------------------------------------------------------------------------
SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '1 day';
SELECT TIMESTAMP '2024-03-31 12:00:00' + INTERVAL '1 month';
SELECT TIMESTAMP '2024-01-15 12:00:00' - INTERVAL '1 hour';
SELECT TIMESTAMP '2024-01-03 00:00:00' - TIMESTAMP '2024-01-01 00:00:00';

-- date + interval → timestamp (promotes to midnight, then adds)
SELECT DATE '2024-01-01' + INTERVAL '1 day 02:00:00';

-- ---------------------------------------------------------------------------
-- Arithmetic: interval
-- ---------------------------------------------------------------------------
SELECT INTERVAL '1 day' + INTERVAL '2 hours';
SELECT INTERVAL '3 days' - INTERVAL '1 day';
SELECT INTERVAL '1 day' * 3;
SELECT INTERVAL '6 hours' / 2;
-- Interval addition/subtraction preserves fields (months not folded to days)
SELECT INTERVAL '1 month' + INTERVAL '15 days';

-- ---------------------------------------------------------------------------
-- Comparison predicates on date/timestamp
-- ---------------------------------------------------------------------------
SELECT DATE '2024-03-01' > DATE '2024-01-15';
SELECT TIMESTAMP '2024-01-15 09:00:00' < TIMESTAMP '2024-01-15 13:00:00';
SELECT INTERVAL '1 day' = INTERVAL '24 hours';

-- ---------------------------------------------------------------------------
-- extract / date_part
-- ---------------------------------------------------------------------------
-- extract(field FROM source) → numeric
SELECT extract(year   FROM DATE '2024-07-15');
SELECT extract(month  FROM DATE '2024-07-15');
SELECT extract(day    FROM DATE '2024-07-15');
SELECT extract(quarter FROM DATE '2024-04-10');
SELECT extract(dow    FROM DATE '2024-01-15');
SELECT extract(doy    FROM DATE '2024-01-15');
SELECT extract(hour   FROM TIMESTAMP '2024-07-15 13:45:06');
SELECT extract(minute FROM TIMESTAMP '2024-07-15 13:45:06');
SELECT extract(second FROM TIMESTAMP '2024-07-15 13:45:06.5');
SELECT extract(epoch  FROM TIMESTAMP '2024-01-15 00:00:00');
-- extract from timestamptz (absolute epoch, then zone fields)
SELECT extract(epoch  FROM TIMESTAMPTZ '2024-01-15 12:00:00+00');
SELECT extract(timezone FROM TIMESTAMPTZ '2024-01-15 12:00:00+00');
-- extract from interval
SELECT extract(year   FROM INTERVAL '2 years 3 months');
SELECT extract(month  FROM INTERVAL '2 years 3 months');
SELECT extract(day    FROM INTERVAL '10 days');
SELECT extract(hour   FROM INTERVAL '02:30:00');
SELECT extract(epoch  FROM INTERVAL '1 day');

-- date_part(text, source) → float8 (historical form, same values, different type)
SELECT date_part('month', DATE '2024-07-01');
SELECT date_part('second', TIMESTAMP '2024-07-01 08:30:45.5');
SELECT date_part('epoch', TIMESTAMP '2024-01-15 00:00:00');

-- ---------------------------------------------------------------------------
-- date_trunc
-- ---------------------------------------------------------------------------
SELECT date_trunc('year',   TIMESTAMP '2024-07-15 13:45:06');
SELECT date_trunc('month',  TIMESTAMP '2024-07-15 13:45:06');
SELECT date_trunc('day',    TIMESTAMP '2024-07-15 13:45:06');
SELECT date_trunc('hour',   TIMESTAMP '2024-07-15 13:45:06');
SELECT date_trunc('minute', TIMESTAMP '2024-07-15 13:45:06');
-- date source: promoted to timestamp at midnight, then truncated
SELECT date_trunc('month', DATE '2024-07-15');

-- ---------------------------------------------------------------------------
-- age(end, start): symbolic interval with month borrowing
-- ---------------------------------------------------------------------------
SELECT age(TIMESTAMP '2024-03-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00');
SELECT age(TIMESTAMP '2025-06-15 00:00:00', TIMESTAMP '2024-01-01 00:00:00');
SELECT age(TIMESTAMP '2024-04-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00');

-- ---------------------------------------------------------------------------
-- Casts
-- ---------------------------------------------------------------------------
-- text → date (::)
SELECT '2024-01-15'::date;
-- text → timestamp
SELECT '2024-01-15 13:45:06'::timestamp;
-- text → timestamptz
SELECT '2024-01-15 13:45:06+00'::timestamptz;
-- text → interval
SELECT '1 day 02:30:00'::interval;
-- CAST spelling is equivalent
SELECT CAST('2024-06-01' AS date);

-- date → text
SELECT DATE '2024-01-15'::text;
-- timestamp → text
SELECT TIMESTAMP '2024-01-15 13:45:06'::text;
-- timestamptz → text (renders in UTC = session zone)
SELECT TIMESTAMPTZ '2024-01-15 13:45:06+00'::text;
-- interval → text
SELECT INTERVAL '2 days 03:00:00'::text;

-- date → timestamp (at midnight)
SELECT DATE '2024-01-15'::timestamp;
-- timestamp → date (truncates time)
SELECT TIMESTAMP '2024-01-15 13:45:06'::date;

-- Chained casts
SELECT '2024-01-15'::date::text;

-- ---------------------------------------------------------------------------
-- AT TIME ZONE
-- ---------------------------------------------------------------------------
-- timestamp AT TIME ZONE 'UTC' → timestamptz
SELECT TIMESTAMP '2024-01-15 12:00:00' AT TIME ZONE 'UTC';
-- timestamptz AT TIME ZONE zone → timestamp (wall clock in that zone)
SELECT TIMESTAMPTZ '2024-01-15 17:00:00+00' AT TIME ZONE 'America/New_York';
-- timestamp AT TIME ZONE zone → timestamptz (interpret as local, return absolute)
SELECT TIMESTAMP '2024-01-15 12:00:00' AT TIME ZONE 'America/New_York';

-- ---------------------------------------------------------------------------
-- SET TIME ZONE + SHOW TIME ZONE section
-- (uses stable-DST dates: Jan 15 = EST = -05:00; Jun 1 = EDT = -04:00)
-- ---------------------------------------------------------------------------
SET TIME ZONE 'America/New_York';
SHOW TIME ZONE;
-- Render a winter timestamptz in EST (-05)
SELECT TIMESTAMPTZ '2024-01-15 17:00:00+00';
-- Render a summer timestamptz in EDT (-04)
SELECT TIMESTAMPTZ '2024-06-01 16:00:00+00';
-- extract(timezone FROM ...) in NY zone
SELECT extract(timezone FROM TIMESTAMPTZ '2024-01-15 12:00:00-05');

-- Reset to UTC for the remainder of the corpus
SET TIME ZONE 'UTC';
SHOW TIME ZONE;

-- ---------------------------------------------------------------------------
-- Error parity (same SQLSTATE on crabgresql and PostgreSQL)
-- ---------------------------------------------------------------------------
-- Feb 30 does not exist → 22008 (datetime field overflow)
SELECT DATE '2024-02-30';
-- Malformed date literal → 22007 (invalid datetime format)
SELECT DATE 'not-a-date';
-- Bad time literal (hour 25) → 22007
SELECT TIME '25:00:00';
-- interval → date has no defined cast → 42846 (cannot cast)
SELECT INTERVAL '1 day'::date;
-- Unknown timezone → 22023 (invalid parameter value)
SET TIME ZONE 'Mars/Phobos';
