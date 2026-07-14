-- SP30: `double precision` (float8) + `AVG`, diffed against PostgreSQL 18.
--
-- Discipline (see the SP30 design doc): floats are exercised through float8
-- COLUMNS and float8 aggregates, where PostgreSQL also computes in float8 so the
-- text output matches byte-for-byte. Two deliberate exclusions, because crabgresql
-- has no `numeric` (so a bare decimal literal is `float8`, not PG's `numeric`):
--   * no bare-literal ARITHMETIC (`1.5 + 1.5`) — PG computes it in `numeric` and
--     scale-pads the result (`3.0`), which a `float8` cannot match;
--   * no `avg(int_col)` — PG returns `numeric` (`2.5000000000000000`), crabgresql
--     `float8` (`2.5`). Both are proven instead by `executor::agg` unit tests.
-- All magnitudes stay in the range where shortest-round-trip text agrees (no
-- scientific notation, no Infinity/NaN — those have no SQL literal without a cast).
CREATE TABLE m (g int4, x double precision);
INSERT INTO m VALUES (1, 1.5), (1, 2.5), (2, 2.0), (2, 4.0), (3, NULL);

-- bare float literals that render identically as numeric and float8
SELECT 1.5;
SELECT 0.25;

-- float8 column round-trip (2.0/4.0 render without a trailing `.0`; NULL stays NULL)
SELECT g, x FROM m ORDER BY g, x;
SELECT x FROM m WHERE x IS NOT NULL ORDER BY x DESC;

-- whole-table aggregates over a float8 column (sum/avg/min/max in float8)
SELECT sum(x), avg(x), min(x), max(x), count(x), count(*) FROM m;

-- grouped aggregates; the all-NULL group's sum/avg are NULL and count(x) is 0
SELECT g, sum(x), avg(x), count(x) FROM m GROUP BY g ORDER BY g;

-- count(DISTINCT float)
SELECT count(DISTINCT x) FROM m;

-- float arithmetic through the column (float8 ⊕ int → float8, real division)
SELECT x + 1 FROM m WHERE g = 2 ORDER BY x;
SELECT x / 2 FROM m WHERE g = 2 ORDER BY x;
SELECT x * 2 FROM m WHERE g = 1 ORDER BY x;

-- float comparison in WHERE and a projected float8 = literal predicate
SELECT x FROM m WHERE x > 2 ORDER BY x;
SELECT x = 2.5 FROM m WHERE g = 1 ORDER BY x;

-- abs over float8
SELECT abs(-2.5), abs(2.5);

-- error parity (same SQLSTATE on both sides)
-- float division by zero (22012)
SELECT x / 0 FROM m WHERE g = 1;
-- an unknown function (42883)
SELECT frobnicate(x) FROM m;
