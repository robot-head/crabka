-- SP38: to_char(value, format) for date/time values, diffed against PostgreSQL 18.
--
-- Scope (spec §1.2 "Date/time to_char patterns — in scope"):
--   year     YYYY YYY YY Y Y,YYY IYYY IYY IY I CC AD/BC/A.D./B.C. (+ lowercase)
--   month    MM Mon/MON/mon Month/MONTH/month RM/rm
--   day      DD DDD IDDD D ID Day/DAY/day Dy/DY/dy
--   week/qtr W WW IW Q
--   time     HH/HH12 HH24 MI SS SSSS/SSSSS MS US FF1..FF6
--   meridiem AM/PM/am/pm A.M./P.M./a.m./p.m.
--   tz       TZH TZM OF (timestamptz only)
--   mods     FM (fill) TH/th (ordinal)
--   literals "quoted text" + non-pattern passthrough
--
-- Exclusions (intentional, spec §1.3 deferred): J (Julian), SP (spell-out), FX,
-- TM (locale-translated names); TZ/tz abbreviation lookup (we render the offset);
-- all non-C-locale / non-English names.  English / C-locale names only.
--
-- Session pinned to UTC; a single timestamptz-zone block uses America/New_York with
-- a post-2007 stable-DST date (Jan 15 = EST = -05:00).  All dates are within jiff's
-- supported calendar range (years 1..9999).  Nothing non-deterministic (no clock
-- functions): every value is a fixed typed literal.

SET TIME ZONE 'UTC';

-- ---------------------------------------------------------------------------
-- Year patterns (over a timestamp literal)
-- ---------------------------------------------------------------------------
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'YYYY');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'YYY');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'YY');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'Y');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'Y,YYY');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'IYYY');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'IYY');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'IY');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'I');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'CC');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'AD');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'BC');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'A.D.');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'ad');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'a.d.');

-- ---------------------------------------------------------------------------
-- Month patterns
-- ---------------------------------------------------------------------------
SELECT to_char(DATE '2024-01-15', 'MM');
SELECT to_char(DATE '2024-01-15', 'Mon');
SELECT to_char(DATE '2024-01-15', 'MON');
SELECT to_char(DATE '2024-01-15', 'mon');
SELECT to_char(DATE '2024-01-15', 'Month');
SELECT to_char(DATE '2024-01-15', 'MONTH');
SELECT to_char(DATE '2024-01-15', 'month');
SELECT to_char(DATE '2024-08-15', 'Month');
SELECT to_char(DATE '2024-01-15', 'FMMonth');
-- RM / rm Roman month: PG left-justifies in a width-4 field; FM strips it.
SELECT to_char(DATE '2024-01-15', 'RM');
SELECT to_char(DATE '2024-03-15', 'RM');
SELECT to_char(DATE '2024-08-15', 'RM');
SELECT to_char(DATE '2024-12-15', 'RM');
SELECT to_char(DATE '2024-08-15', 'rm');
SELECT to_char(DATE '2024-08-15', 'FMRM');

-- ---------------------------------------------------------------------------
-- Day / week / quarter patterns
-- ---------------------------------------------------------------------------
SELECT to_char(DATE '2024-07-15', 'DD');
SELECT to_char(DATE '2024-07-15', 'DDD');
SELECT to_char(DATE '2024-07-15', 'IDDD');
SELECT to_char(DATE '2024-07-15', 'D');
SELECT to_char(DATE '2024-07-15', 'ID');
SELECT to_char(DATE '2024-07-15', 'Day');
SELECT to_char(DATE '2024-07-15', 'DAY');
SELECT to_char(DATE '2024-07-15', 'day');
SELECT to_char(DATE '2024-07-15', 'Dy');
SELECT to_char(DATE '2024-07-15', 'DY');
SELECT to_char(DATE '2024-07-15', 'dy');
SELECT to_char(DATE '2024-07-15', 'FMDay');
SELECT to_char(DATE '2024-07-15', 'W');
SELECT to_char(DATE '2024-07-15', 'WW');
SELECT to_char(DATE '2024-07-15', 'IW');
SELECT to_char(DATE '2024-04-10', 'Q');
SELECT to_char(DATE '2024-07-15', 'Q');

-- ---------------------------------------------------------------------------
-- Time patterns
-- ---------------------------------------------------------------------------
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'HH');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'HH12');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'HH24');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'MI');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'SS');
-- SSSS / SSSSS seconds-past-midnight: PG does NOT zero-pad.
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'SSSS');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'SSSSS');
SELECT to_char(TIMESTAMP '2024-07-15 00:00:05', 'SSSS');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06.5', 'MS');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06.5', 'US');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06.123456', 'FF1');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06.123456', 'FF3');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06.123456', 'FF6');

-- ---------------------------------------------------------------------------
-- Meridiem
-- ---------------------------------------------------------------------------
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'HH12:MI AM');
SELECT to_char(TIMESTAMP '2024-07-15 08:30:00', 'HH12:MI AM');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'HH12:MI pm');
SELECT to_char(TIMESTAMP '2024-07-15 13:45:06', 'HH12:MI P.M.');
SELECT to_char(TIMESTAMP '2024-07-15 08:30:00', 'HH12:MI a.m.');

-- ---------------------------------------------------------------------------
-- Composite formats, FM, TH, quoted literals, passthrough
-- ---------------------------------------------------------------------------
SELECT to_char(TIMESTAMP '2024-01-15 13:45:06', 'YYYY-MM-DD HH24:MI:SS');
SELECT to_char(DATE '2024-07-04', 'FMMonth FMDD, YYYY');
SELECT to_char(DATE '2024-07-04', 'FMDay, FMMonth FMDDth, YYYY');
SELECT to_char(DATE '2024-07-01', 'DDth');
SELECT to_char(DATE '2024-07-02', 'DDth');
SELECT to_char(DATE '2024-07-03', 'DDth');
SELECT to_char(DATE '2024-07-11', 'DDth');
SELECT to_char(DATE '2024-07-21', 'DDTH');
SELECT to_char(TIMESTAMP '2024-01-15 13:45:06', '"Year:" YYYY "Month:" MM');
SELECT to_char(DATE '2024-07-04', 'HH24"h"MI"m"');

-- ---------------------------------------------------------------------------
-- to_char(time, fmt)
-- ---------------------------------------------------------------------------
SELECT to_char(TIME '13:45:06', 'HH24:MI:SS');
SELECT to_char(TIME '13:45:06', 'HH12:MI AM');
SELECT to_char(TIME '01:02:03', 'SSSS');

-- ---------------------------------------------------------------------------
-- to_char(interval, fmt): PG renders STORED fields (interval2tm) — clock reads the
-- micros component (hours may exceed 24); DD reads days; MM/YYYY read months.
-- ---------------------------------------------------------------------------
SELECT to_char(INTERVAL '36 hours', 'HH24:MI:SS');
SELECT to_char(INTERVAL '1 day 02:03:04', 'DD HH24:MI:SS');
SELECT to_char(INTERVAL '2 years 3 months', 'YYYY-MM');
SELECT to_char(INTERVAL '90 minutes', 'HH24:MI:SS');
SELECT to_char(INTERVAL '1 day 02:30:00', 'FMDD "days" HH24:MI');

-- ---------------------------------------------------------------------------
-- to_char(timestamptz, fmt) under a stable-DST zone (Jan 15 = EST = -05:00).
-- TZH/TZM/OF render the session-zone offset; a plain timestamp's tz patterns
-- are exercised below (empty / passthrough is PG behavior, kept out of the diff).
-- ---------------------------------------------------------------------------
SET TIME ZONE 'America/New_York';
SHOW TIME ZONE;
SELECT to_char(TIMESTAMPTZ '2024-01-15 17:00:00+00', 'YYYY-MM-DD HH24:MI:SS');
SELECT to_char(TIMESTAMPTZ '2024-01-15 17:00:00+00', 'TZH:TZM');
SELECT to_char(TIMESTAMPTZ '2024-01-15 17:00:00+00', 'OF');
SELECT to_char(TIMESTAMPTZ '2024-06-01 16:00:00+00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM');
SET TIME ZONE 'UTC';

-- ---------------------------------------------------------------------------
-- NULL strictness
-- ---------------------------------------------------------------------------
SELECT to_char(NULL::timestamp, 'YYYY');
SELECT to_char(TIMESTAMP '2024-01-15 13:45:06', NULL::text);
