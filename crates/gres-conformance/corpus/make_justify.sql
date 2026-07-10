-- SP38: make_* field constructors + justify_* interval normalizers, diffed
-- against PostgreSQL 18.
--
-- Scope (spec §1.1):
--   make_date(year, month, day)                              → date
--   make_time(hour, min, sec)                                → time
--   make_timestamp(y, mo, d, h, mi, sec)                     → timestamp
--   make_timestamptz(y, mo, d, h, mi, sec [, zone])          → timestamptz
--   make_interval([years, months, weeks, days, h, mi, secs]) → interval (POSITIONAL)
--   justify_days(interval) / justify_hours(interval) / justify_interval(interval)
--
-- Exclusions (intentional, spec §1.3 deferred): NAMED arguments
-- (make_interval(days => 5)) — the parser has no name => value syntax; SP38
-- supports the positional call only.  make_interval supports 0–7 positional args
-- with trailing-omitted args defaulting to 0.
--
-- Session pinned to UTC so make_timestamptz (6-arg) renders with the +00 offset;
-- a single 7-arg case names America/New_York (Jan 15 = EST = -05:00).  All values
-- are fixed; nothing non-deterministic.  Dates stay within jiff's range (1..9999).

SET TIME ZONE 'UTC';

-- ---------------------------------------------------------------------------
-- make_date
-- ---------------------------------------------------------------------------
SELECT make_date(2024, 7, 4);
SELECT make_date(2024, 2, 29);
SELECT make_date(1, 1, 1);
SELECT make_date(9999, 12, 31);

-- ---------------------------------------------------------------------------
-- make_time (fractional seconds → micros)
-- ---------------------------------------------------------------------------
SELECT make_time(8, 0, 0);
SELECT make_time(13, 45, 6);
SELECT make_time(13, 45, 6.5);
SELECT make_time(23, 59, 59.999999);

-- ---------------------------------------------------------------------------
-- make_timestamp
-- ---------------------------------------------------------------------------
SELECT make_timestamp(2024, 7, 4, 13, 45, 6);
SELECT make_timestamp(2024, 7, 4, 13, 45, 6.5);
SELECT make_timestamp(2024, 2, 29, 0, 0, 0);

-- ---------------------------------------------------------------------------
-- make_timestamptz (6-arg interpreted in the session zone; 7-arg names a zone)
-- ---------------------------------------------------------------------------
SELECT make_timestamptz(2024, 1, 15, 12, 0, 0);
SELECT make_timestamptz(2024, 7, 4, 8, 30, 0);
SELECT make_timestamptz(2024, 1, 15, 12, 0, 0, 'America/New_York');
SELECT make_timestamptz(2024, 1, 15, 12, 0, 0, 'UTC');

-- ---------------------------------------------------------------------------
-- make_interval (POSITIONAL; trailing args default to 0)
-- ---------------------------------------------------------------------------
SELECT make_interval(1, 2, 0, 3);
SELECT make_interval(0, 0, 2, 0, 0, 0, 1.5);
SELECT make_interval(1);
SELECT make_interval(0, 0, 0, 0, 5, 30, 15);
SELECT make_interval(2, 6);
SELECT make_interval(0, 0, 0, 0, 0, 0, 90.5);
SELECT make_interval();

-- ---------------------------------------------------------------------------
-- justify_days (30-day groups → months)
-- ---------------------------------------------------------------------------
SELECT justify_days(INTERVAL '35 days');
SELECT justify_days(INTERVAL '70 days');
SELECT justify_days(INTERVAL '29 days');

-- ---------------------------------------------------------------------------
-- justify_hours (24-hour groups → days)
-- ---------------------------------------------------------------------------
SELECT justify_hours(INTERVAL '27 hours');
SELECT justify_hours(INTERVAL '49:30:00');
SELECT justify_hours(INTERVAL '23:00:00');

-- ---------------------------------------------------------------------------
-- justify_interval (hours then days, then sign-normalize)
-- ---------------------------------------------------------------------------
SELECT justify_interval(INTERVAL '35 days 27 hours');
SELECT justify_interval(INTERVAL '1 mon -1 hour');
SELECT justify_interval(INTERVAL '-1 mon 1 hour');
SELECT justify_interval(INTERVAL '1 year 1 mon -1 day');
SELECT justify_interval(INTERVAL '0 days');

-- ---------------------------------------------------------------------------
-- NULL strictness
-- ---------------------------------------------------------------------------
SELECT make_date(NULL::int4, 1, 1);
SELECT make_interval(NULL::int4);
SELECT justify_hours(NULL::interval);

-- ---------------------------------------------------------------------------
-- Error parity (SQLSTATE diffed): out-of-range field, unknown zone
-- ---------------------------------------------------------------------------
SELECT make_date(2024, 13, 1);
SELECT make_date(2024, 2, 30);
SELECT make_time(25, 0, 0);
SELECT make_timestamptz(2024, 1, 1, 0, 0, 0, 'Mars/Olympus');
