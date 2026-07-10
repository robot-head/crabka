-- SP31: explicit casts — `CAST(expr AS type)` and `expr::type`, diffed against
-- PostgreSQL 18.
--
-- Discipline (mirroring the SP30 "no numeric" constraint): float8→int casts are
-- exercised through float8 COLUMNS, where PostgreSQL also rounds with float8→int
-- (round-half-to-even / rint). A *bare* decimal literal is `numeric` in PG but
-- `float8` here, so a bare-literal `2.5::int` would diverge (PG rounds the numeric
-- 2.5 half-away-from-zero → 3; crabgresql rounds the float8 → 2); those live in
-- `pgtypes::cast` / `executor::eval` unit tests instead. The unsupported-cast and
-- bad-input cases below match on SQLSTATE only (the message text differs because
-- the bare literal is numeric in PG, float8 here).
CREATE TABLE cst (id int4, label text, ratio double precision, flag bool);
INSERT INTO cst VALUES (1, '10', 2.5, true), (2, '20', 3.5, false), (3, '-7', 0.5, true);

-- text → integer (through a column and as a literal; signs and whitespace)
SELECT id, label::int4 FROM cst ORDER BY id;
SELECT '42'::int4, ' -7 '::integer, '+7'::int4;
SELECT '9000000000'::int8;

-- text → float8 (finite, and the IEEE specials by name)
SELECT '1.5'::float8, '2'::double precision;
SELECT 'Infinity'::float8, '-Infinity'::float8, 'NaN'::float8;

-- float8 column → int4: round half-to-even (2.5→2, 3.5→4, 0.5→0)
SELECT id, ratio::int4 FROM cst ORDER BY id;

-- numeric ↔ numeric
SELECT 5::int8, 5::float8, (5::int8)::int4;

-- bool ↔ int4, and any type → text (bool renders true/false, not t/f)
SELECT true::int4, false::int4, 0::bool, 5::bool;
SELECT 42::text, true::text, false::text;
SELECT id::text, flag::text FROM cst ORDER BY id;

-- text → bool (PostgreSQL boolean input spellings)
SELECT 'true'::bool, 't'::bool, 'no'::bool, 'off'::bool, '1'::bool, '0'::bool;

-- the CAST(_ AS _) spelling is interchangeable with ::
SELECT CAST('42' AS int4), CAST(ratio AS int4) FROM cst WHERE id = 2;

-- precedence: :: binds tighter than unary minus and +, and chains left-to-right
SELECT -2::int8, 1 + 2::int8, '5'::int4::float8;

-- a cast in WHERE
SELECT id FROM cst WHERE label::int4 >= 20 ORDER BY id;

-- error parity (same SQLSTATE on both sides)
-- bad text syntax for the target type (22P02)
SELECT 'abc'::int4;
SELECT '1.5'::int4;
-- well-formed but out of range (22003)
SELECT '99999999999'::int4;
SELECT 3000000000::int4;
-- an undefined cast (42846)
SELECT 1.5::bool;
SELECT true::int8;
