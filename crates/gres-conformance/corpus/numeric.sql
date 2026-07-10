-- SP32: arbitrary-precision `numeric` / `decimal`, diffed against PostgreSQL 18.
--
-- numeric is exact, so unlike the float8 corpus there is no magnitude/rounding
-- caveat: bare decimal literals are `numeric` in BOTH crabgresql and PostgreSQL,
-- and arithmetic/aggregate results follow PostgreSQL's scale rules (max scale for
-- +/-, summed scale for *, and `select_div_scale` for / and avg). The deferred
-- specials (`NaN`/`Infinity`) have no SQL literal here, so they do not appear.
CREATE TABLE num_acct (id int4, bal numeric(10,2), ratio numeric);
INSERT INTO num_acct VALUES (1, 10, 1.5), (2, 2.5, 0.333), (3, 9.999, 2);

-- bare decimal / exponent literals are scale-faithful numeric
SELECT 1.5, 2.50, 1e3, 0.0015, 100.0;

-- numeric(p,s) rounds to scale on store; unconstrained numeric keeps its scale
SELECT id, bal, ratio FROM num_acct ORDER BY id;

-- exact arithmetic with PostgreSQL scale rules (+/- max scale, * summed scale)
SELECT 1.50 + 1.5, 2.5 - 1.25, 1.5 * 1.5, 1.50 * 2, 2 * 1.5;

-- division and int/numeric mix use select_div_scale
SELECT 10 / 3.0, 1.0 / 3, 6.0 / 2.0, 22.0 / 7, 1000000.0 / 7;

-- aggregates: sum keeps max scale; avg/sum/min/max over a numeric column
SELECT sum(ratio), min(ratio), max(ratio), count(ratio) FROM num_acct;
SELECT sum(bal), avg(bal) FROM num_acct;
-- avg over integers is numeric (exact)
SELECT avg(id) FROM num_acct;

-- grouped aggregate + value-equality grouping (1.0 and 1.00 are one group)
CREATE TABLE num_g (g int4, v numeric);
INSERT INTO num_g VALUES (1, 1.0), (1, 1.00), (2, 2.5), (2, 2.50);
SELECT v, count(*) FROM num_g GROUP BY v ORDER BY v;
SELECT count(DISTINCT v) FROM num_g;

-- casts: int<->numeric, text->numeric, numeric->int (round half away from zero),
-- numeric<->float8, and a typmod-bearing cast target
SELECT 5::numeric, '12.34'::numeric, 2.5::int4, (-2.5)::int4, 3.7::int8;
SELECT (0.1::float8)::numeric, (1.5::numeric)::float8, 1.236::numeric(5,2);
SELECT 42::numeric::text, '1.50'::numeric;

-- abs / mod over numeric
SELECT abs(-2.5), mod(7.5, 2), mod(-7.5, 2);

-- comparison and ordering over numeric
SELECT v FROM num_g WHERE v > 1 ORDER BY v DESC;

-- error parity (same SQLSTATE)
-- numeric division by zero (22012)
SELECT 1.5 / 0;
-- numeric field overflow on store (22003)
INSERT INTO num_acct VALUES (4, 99999999.999, 1);
-- a non-existent cast: numeric -> bool (42846)
SELECT 1.5::bool;
-- bad text -> numeric (22P02)
SELECT 'abc'::numeric;
