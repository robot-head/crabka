-- SP38: to_char(numeric/int/float8, format), diffed against PostgreSQL 18.
--
-- Scope (spec §1.2 "Numeric to_char patterns — in scope"):
--   9 (leading-zero → blank) 0 (leading-zero shown)
--   . / D (decimal point)    , / G (group separator)
--   S MI PL SG PR            (sign decorations)
--   L $                      (currency, C-locale glyph = '$')
--   V                        (digit-shift)
--   FM (suppress padding)    TH/th (ordinal)    B (blank-on-zero — no-op in PG 18)
--   # overflow               (integer part wider than the grid)
--
-- Exclusions (intentional, spec §1.3 deferred): RN/rn (Roman), EEEE (scientific),
-- and any TRUE non-C-locale L/D/G (locale currency/decimal/grouping; SET lc_* is
-- unsupported).  The C-locale defaults ($, ., ,) are exercised.
--
-- All values are fixed literals; nothing non-deterministic.  The integer overflow
-- to '#' fill is PG behavior.  Sign placement, the C-locale currency glyph, the
-- '#'-overflow composition, and the B no-op were validated against PostgreSQL 18.

SET TIME ZONE 'UTC';

-- ---------------------------------------------------------------------------
-- Digit positions: 9 blanks leading zeros, 0 shows them; the default sign column
-- reserves a leading blank (anchored to the first digit for a negative value).
-- ---------------------------------------------------------------------------
SELECT to_char(485, '999');
SELECT to_char(-485, '999');
SELECT to_char(12, '9999');
SELECT to_char(-12, '9999');
SELECT to_char(-1, '999');
SELECT to_char(12, '0000');
SELECT to_char(-12, '0000');
SELECT to_char(0, '9999');
SELECT to_char(0, '0000');
SELECT to_char(5, '9');
SELECT to_char(485, 'FM999');
SELECT to_char(12, 'FM9999');

-- ---------------------------------------------------------------------------
-- Decimal point (. and D) + rounding (half away from zero)
-- ---------------------------------------------------------------------------
SELECT to_char(1234.5, '9999.9');
SELECT to_char(12.34, '99D99');
SELECT to_char(1.235, '9.99');
SELECT to_char(1.245, '9.99');
SELECT to_char(-1.235, '9.99');
SELECT to_char(2.5, '9');
SELECT to_char(-2.5, '9');
SELECT to_char(0.5, '9.9');
SELECT to_char(0.5, '0.9');
SELECT to_char(148.5, 'FM999.999');
SELECT to_char(-0.1, 'FM9.99');
SELECT to_char(5, 'FM9.99');

-- ---------------------------------------------------------------------------
-- Group separators (, and G); a separator with an all-blank left renders blank.
-- ---------------------------------------------------------------------------
SELECT to_char(1234567, '9,999,999');
SELECT to_char(1234567, '9G999G999');
SELECT to_char(1234567, 'FM9,999,999');
SELECT to_char(1234.5, '9,999.9');
SELECT to_char(12, '9,999');
SELECT to_char(123, '9,999');
SELECT to_char(5, '9,999');

-- ---------------------------------------------------------------------------
-- Sign decorations: S (anchored), MI (fixed minus / blank), PL (additive +),
-- SG (fixed + or -), PR (negatives in <…>, brackets hug the number).
-- ---------------------------------------------------------------------------
SELECT to_char(485, 'S999');
SELECT to_char(-485, 'S999');
SELECT to_char(485, '999S');
SELECT to_char(-485, '999S');
SELECT to_char(485, 'MI999');
SELECT to_char(-485, 'MI999');
SELECT to_char(-12, 'MI9999');
SELECT to_char(485, '999MI');
SELECT to_char(-485, '999MI');
SELECT to_char(485, 'FM999MI');
SELECT to_char(12, 'PL999');
SELECT to_char(-12, 'PL999');
SELECT to_char(0, 'PL999');
SELECT to_char(12, '999PL');
SELECT to_char(-1, '999PL');
SELECT to_char(12, 'SG999');
SELECT to_char(-12, 'SG999');
SELECT to_char(-12, '999PR');
SELECT to_char(12, '999PR');
SELECT to_char(-12, '9999PR');

-- ---------------------------------------------------------------------------
-- Currency (L and $) — C-locale glyph is '$', placed outside the sign column.
-- ---------------------------------------------------------------------------
SELECT to_char(485, 'L999');
SELECT to_char(485, '999L');
SELECT to_char(485, '$999');
SELECT to_char(485, '999$');
SELECT to_char(-485, 'L999');
SELECT to_char(-485, '999L');

-- ---------------------------------------------------------------------------
-- V digit-shift (multiply by 10^n where n = count of 9/0 after V)
-- ---------------------------------------------------------------------------
SELECT to_char(12.45, '999V99');
SELECT to_char(12.4, '99V999');
SELECT to_char(1, '9V9');
SELECT to_char(12.45, '99V9');

-- ---------------------------------------------------------------------------
-- TH / th ordinal (suppressed for a negative value)
-- ---------------------------------------------------------------------------
SELECT to_char(1, 'FM9TH');
SELECT to_char(2, 'FM9TH');
SELECT to_char(3, 'FM9TH');
SELECT to_char(11, 'FM99TH');
SELECT to_char(23, 'FM99th');
SELECT to_char(485, '999TH');
SELECT to_char(-12, 'FM999TH');

-- ---------------------------------------------------------------------------
-- B (blank-on-zero): a no-op in PostgreSQL 18 — zero renders normally.
-- ---------------------------------------------------------------------------
SELECT to_char(0, 'B9999');
SELECT to_char(0, 'FMB9999');
SELECT to_char(12, 'B9999');

-- ---------------------------------------------------------------------------
-- # overflow: integer part wider than the grid → '#'-fill the digit positions
-- (the sign / currency decoration still renders).
-- ---------------------------------------------------------------------------
SELECT to_char(123456, '999');
SELECT to_char(-123456, '999');
SELECT to_char(123456, 'FM999');
SELECT to_char(1234.5, '999.99');
SELECT to_char(123456, '9,99');
SELECT to_char(99.6, '99');
SELECT to_char(123456, 'L99');

-- ---------------------------------------------------------------------------
-- int4 / int8 / float8 / numeric argument types (all promote to the numeric grid)
-- ---------------------------------------------------------------------------
SELECT to_char(1234567::int4, 'FM9,999,999');
SELECT to_char(1234567::int8, 'FM9,999,999');
SELECT to_char(1234.5::float8, '9999.99');
SELECT to_char(1234.50::numeric, '9999.99');

-- ---------------------------------------------------------------------------
-- NULL strictness
-- ---------------------------------------------------------------------------
SELECT to_char(NULL::numeric, '999');
SELECT to_char(485, NULL::text);
