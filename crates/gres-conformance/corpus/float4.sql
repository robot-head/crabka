-- `real` / `float4` (OID 700), diffed against PostgreSQL 18.
--
-- Covers float4 literals (`'1.5'::float4`, `real '1.5'`, `float4 '2.25'`),
-- float4 and float4[] columns, the float4-native operators (float4 ⊕ float4
-- stays float4; anything else promotes to float8), the IEEE specials, casts in
-- both directions, and the aggregate result types (`sum` → real, `avg` →
-- double precision, `min`/`max` → real).
--
-- Output formatting is the point of the `f4_fmt` fixture: PostgreSQL prints the
-- shortest text that round-trips through a 32-bit float, switching to
-- scientific notation once the decimal exponent is < -4 or >= 6. So 100000 and
-- 0.0001 print in full while 1000000 prints as `1e+06` and 0.00001 as `1e-05`.
-- 16777216 is 2^24, the point where consecutive integers stop being
-- representable, so 16777217 rounds back to it and both print as
-- `1.6777216e+07`. `NaN` equals itself and sorts above `Infinity`, so `f4_spec`
-- pins the whole float4 sort order.
--
-- All magnitudes are chosen so the aggregates are exact in binary floating
-- point (halves and quarters), keeping sum/avg independent of accumulation
-- order. Rows are ORDER BY-stable; the error cases are compared on SQLSTATE only.
CREATE TABLE f4_t (id int4, g int4, v float4, vs float4[]);
INSERT INTO f4_t VALUES (1, 1, 1.5, '{1.5,2.5}'), (2, 1, 2.5, '{0.25}'), (3, 2, 0.5, '{}'), (4, 2, 4, NULL), (5, 2, NULL, NULL);
CREATE TABLE f4_fmt (id int4, v float4);
INSERT INTO f4_fmt VALUES (1, 100000), (2, 1000000), (3, 16777216), (4, 0.0001), (5, 0.00001), (6, '3.4028235e38'), (7, '1.1754944e-38'), (8, 123456.7), (9, '1e-45');
CREATE TABLE f4_spec (id int4, v float4);
INSERT INTO f4_spec VALUES (1, 'NaN'), (2, 'Infinity'), (3, '-Infinity'), (4, 0), (5, 1.5);

-- literals and accepted input spellings (padding, signed zero, special names)
SELECT '1.5'::float4, real '1.5', float4 '2.25', '0.1'::float4, 1.0::float4;
SELECT 0::float4, (-1.5)::float4, '-0'::float4, '  2.5  '::float4;
SELECT 'Infinity'::float4, '-Infinity'::float4, 'NaN'::float4, 'inf'::float4, '-inf'::float4, 'nan'::float4;

-- output formatting: fixed inside [1e-4, 1e6), scientific outside it
SELECT 100000::float4, 1000000::float4, 10000000::float4, 16777216::float4;
SELECT 0.001::float4, 0.0001::float4, 0.00001::float4, 0.000001::float4;
SELECT 123456.7::float4, 1234567::float4, 12345678::float4, 16777217::float4;
SELECT '3.4028235e38'::float4, '1.1754944e-38'::float4, '1e-45'::float4;
SELECT 1.1::float4, 2.2::float4, 3.3::float4, 0.1::float4 + 0.2::float4;
SELECT id, v FROM f4_fmt ORDER BY id;

-- float4 column and float4[] column round-trip
SELECT id, g, v, vs FROM f4_t ORDER BY id;
SELECT v FROM f4_t ORDER BY v;
SELECT id FROM f4_t WHERE v IS NULL;

-- float4 ⊕ float4 stays float4
SELECT 1.5::float4 + 2.5::float4, 1.5::float4 - 0.25::float4, 1.5::float4 * 2::float4, 1::float4 / 3::float4;
SELECT - (1.5::float4), abs((-1.5)::float4), abs(1.5::float4);
SELECT id, v * 2::float4 FROM f4_t WHERE g = 1 ORDER BY id;
SELECT id, v / 4::float4 FROM f4_t WHERE g = 1 ORDER BY id;

-- every other numeric operand promotes the result to float8
SELECT 1.5::float4 + 1.5::float8, 1.5::float4 * 2::float8, 1::float4 / 3::float8;
SELECT 1.5::float4 + 1, 1.5::float4 + 1::int8, 1.5::float4 + 1::int2;
SELECT 1.5::float4 + 1.5, 1.5::float4 * 2.0;
SELECT id, v / 2 FROM f4_t WHERE g = 1 ORDER BY id;

-- IEEE specials propagate instead of erroring
SELECT 'Infinity'::float4 - 'Infinity'::float4, 'Infinity'::float4 * 0::float4, 'NaN'::float4 + 1::float4;
SELECT 'Infinity'::float4 + 1::float4, 'Infinity'::float4 * 2::float4, - ('Infinity'::float4);

-- comparison; NaN equals itself and sorts above every other value
SELECT 1.5::float4 = 1.5::float4, 1.5::float4 < 2::float4, 1.5::float4 = 1.5, 1.5::float4 <> 2.5::float4;
SELECT 'NaN'::float4 = 'NaN'::float4, 'NaN'::float4 > 'Infinity'::float4, 'Infinity'::float4 > 1e38::float4;
SELECT id, v FROM f4_spec ORDER BY v, id;
SELECT id, v FROM f4_spec ORDER BY v DESC, id;
SELECT id FROM f4_spec WHERE v = 'NaN'::float4;
SELECT id FROM f4_spec WHERE v > 0::float4 ORDER BY id;
SELECT DISTINCT v FROM f4_spec ORDER BY v;
SELECT count(DISTINCT v) FROM f4_spec;

-- casts out of float4 (widening to float8 exposes the stored binary value)
SELECT 0.1::float4::float8, 1.5::float4::float8, 0.1::float4::numeric, 1.5::float4::text;
SELECT 1.5::float4::int2, 1.5::float4::int4, 1.5::float4::int8;
SELECT 2.5::float4::int4, 3.5::float4::int4, (-2.5)::float4::int4, (-3.5)::float4::int4;
SELECT 'Infinity'::float4::float8, 'NaN'::float4::float8;
SELECT CAST(v AS float8), CAST(v AS numeric) FROM f4_t WHERE id = 1;

-- casts into float4
SELECT '1.5'::text::float4, 1.5::numeric::float4, 5::int2::float4, 5::int4::float4, 5::int8::float4;
SELECT 0.1::float8::float4, 1e38::float8::float4, CAST('2.5' AS real), CAST(1.5::float8 AS real);

-- aggregates: sum/min/max -> real, avg -> double precision
SELECT sum(v), avg(v), min(v), max(v), count(v), count(*) FROM f4_t;
SELECT g, sum(v), avg(v), count(v) FROM f4_t GROUP BY g ORDER BY g;
SELECT min(v), max(v), sum(v) FROM f4_spec;
SELECT sum(v) FROM f4_t WHERE v IS NULL;
SELECT v, count(*) FROM f4_t GROUP BY v ORDER BY v;
SELECT sum(v)::float8, avg(v)::float4 FROM f4_t WHERE g = 1;

-- float4[] literals, constructors, containment, and subscripting
SELECT '{1.5,2.5}'::float4[], '{}'::float4[], '{NULL,1.5}'::float4[];
SELECT '{Infinity,-Infinity,NaN}'::float4[];
SELECT ARRAY[1.5, 2.5]::float4[], array_length('{1.5,2.5}'::float4[], 1), cardinality('{}'::float4[]);
SELECT 1.5::float4 = ANY('{1.5,2.5}'::float4[]), 9.5::float4 <> ALL('{1.5,2.5}'::float4[]);
SELECT array_cat('{1.5}'::float4[], '{2.5}'::float4[]), array_append('{1.5}'::float4[], 2.5::float4);
SELECT id, vs, vs[1] FROM f4_t ORDER BY id;
SELECT id FROM f4_t WHERE vs @> '{1.5}'::float4[] ORDER BY id;

-- NULL handling
SELECT NULL::float4, NULL::float4 + 1::float4;
SELECT coalesce(v, 0::float4) FROM f4_t ORDER BY id;
SELECT x FROM (VALUES (1.5::float4), (0.5::float4), (NULL::float4)) AS t(x) ORDER BY x;

-- error parity (same SQLSTATE on both sides)
-- float division by zero, including 0/0 (22012, not NaN)
SELECT 1::float4 / 0::float4;
SELECT 0::float4 / 0::float4;
-- arithmetic that leaves the float4 range (22003)
SELECT '3.4028235e38'::float4 * 2::float4;
SELECT '3.4028235e38'::float4 + '3.4028235e38'::float4;
-- float8 -> float4 overflow and underflow (22003)
SELECT 1e39::float8::float4;
SELECT 1e-50::float8::float4;
-- input text out of the float4 range (22003)
SELECT '1e39'::float4;
-- specials and out-of-range magnitudes cannot become integers (22003)
SELECT 'NaN'::float4::int4;
SELECT 'Infinity'::float4::int4;
SELECT 40000::float4::int2;
-- malformed float4 input text (22P02)
SELECT 'abc'::float4;
SELECT ''::float4;
-- out-of-range value stored into a float4 column (22003)
INSERT INTO f4_t VALUES (6, 1, '1e39', NULL);
-- there is no real -> boolean cast (42846)
SELECT 1.5::float4::bool;

DROP TABLE f4_spec;
DROP TABLE f4_fmt;
DROP TABLE f4_t;
