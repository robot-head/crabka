-- `smallint` / `int2` (OID 21), diffed against PostgreSQL 18.
--
-- Covers the three spellings of an int2 literal (`'5'::int2`, `int2 '5'`,
-- `smallint '12'`), int2 and int2[] columns, the int2-native operators (int2 ⊕
-- int2 stays int2, and every overflow edge is 22003), the cross-width
-- promotions (⊕ int4 → int4, ⊕ int8 → int8, ⊕ numeric → numeric, ⊕ float →
-- float8), casts in both directions, and the aggregate result types
-- (`sum` → bigint, `avg` → numeric, `min`/`max` → smallint).
--
-- Every rounding cast names its source type explicitly, because the two
-- rounding rules differ: numeric→int2 rounds half away from zero (2.5 → 3)
-- while float8→int2 rounds half to even (2.5 → 2). Unary minus is parenthesised
-- (`(-32768)::int2`) because `::` binds tighter than the sign, so the bare
-- spelling would cast 32768 and overflow before the negation. Rows are
-- ORDER BY-stable; the error cases are compared on SQLSTATE only.
CREATE TABLE i2_t (id int4, g int2, v int2, tags int2[]);
INSERT INTO i2_t VALUES (1, 1, 100, '{1,2}'), (2, 1, -100, '{3}'), (3, 2, 32767, '{}'), (4, 2, -32768, NULL), (5, 2, NULL, NULL);

-- literals and accepted input spellings (leading zeros, padding, explicit sign)
SELECT '5'::int2, int2 '5', smallint '12', '-32768'::int2, 32767::int2;
SELECT '007'::int2, '  -5  '::int2, '+7'::int2, '-0'::int2;
SELECT 0::int2, (-1)::int2, - (100::int2);

-- int2 column and int2[] column round-trip (NULLs sort last on ASC, first on DESC)
SELECT id, g, v, tags FROM i2_t ORDER BY id;
SELECT v FROM i2_t ORDER BY v;
SELECT v FROM i2_t ORDER BY v DESC;
SELECT id, v IS NULL, v IS NOT NULL FROM i2_t ORDER BY id;
SELECT id FROM i2_t WHERE v IS NULL;

-- int2 ⊕ int2 stays int2; division truncates toward zero and % takes the dividend's sign
SELECT 1::int2 + 1::int2, 5::int2 - 8::int2, 3::int2 * 4::int2;
SELECT 7::int2 / 2::int2, (-7)::int2 / 2::int2, 7::int2 % 2::int2, (-7)::int2 % 2::int2;
SELECT 32767::int2 - 1::int2, (-32768)::int2 + 1::int2, (-32768)::int2 / 2::int2;
SELECT mod(7::int2, 2::int2), abs((-5)::int2), abs(5::int2);
SELECT id, v + 1::int2 FROM i2_t WHERE id IN (1, 2) ORDER BY id;
SELECT id, v * 2::int2 FROM i2_t WHERE id IN (1, 2) ORDER BY id;

-- promotion: the wider operand wins, so these never overflow
SELECT 1::int2 + 1::int4, 1::int2 - 1::int4, 100::int2 * 1000::int4;
SELECT 1::int2 + 1::int8, 32767::int2 * 1000000::int8, 100::int2 * 1000::int8;
SELECT 1::int2 + 1.5, 3::int2 / 2::numeric;
SELECT 1::int2 + 1.5::float8, 3::int2 / 2::float8, 1::int2 + 1.5::float4;
SELECT 32767::int2 + 1::int4, 32767::int2 + 1;
SELECT id, v::int4 * 2 FROM i2_t WHERE id = 3;

-- comparison across the integer widths and against numeric
SELECT 1::int2 = 1::int4, 1::int2 < 2::int8, 3::int2 > 2.5, 1::int2 <> 2::int2;
SELECT 1::int2 <= 1::int2, 2::int2 >= 3::int4, 1::int2 = 1.0, 32767::int2 = 32767;
SELECT id FROM i2_t WHERE v > 0::int2 ORDER BY id;
SELECT id FROM i2_t WHERE v BETWEEN (-100)::int2 AND 100::int2 ORDER BY id;
SELECT 1::int2 IN (1::int4, 5::int4), 9::int2 IN (1, 5);
SELECT greatest(1::int2, 2::int2), least(1::int2, 2::int2);

-- casts out of int2
SELECT 1::int2::int4, 1::int2::int8, 1::int2::numeric, 1::int2::float4, 1::int2::float8, 1::int2::text;
SELECT v::text, v::int8, v::float8 FROM i2_t WHERE id = 3;
SELECT CAST(32767::int2 AS int8), CAST((-32768)::int2 AS numeric);

-- casts into int2: numeric rounds half away from zero, float8 rounds half to even
SELECT 5::int4::int2, 5::int8::int2, 5::numeric::int2, 5::float4::int2, 5::float8::int2, '5'::text::int2;
SELECT 2.5::numeric::int2, 3.5::numeric::int2, (-2.5)::numeric::int2;
SELECT 2.5::float8::int2, 3.5::float8::int2, (-2.5)::float8::int2;
SELECT CAST('42' AS smallint), CAST('-42' AS int2);
SELECT id FROM i2_t WHERE v::int4 = 32767;

-- aggregates: sum -> bigint (so the 32767 + -32768 pair cannot overflow),
-- avg -> numeric, min/max -> smallint
SELECT sum(v), avg(v), min(v), max(v), count(v), count(*) FROM i2_t;
SELECT g, sum(v), avg(v), count(v) FROM i2_t GROUP BY g ORDER BY g;
SELECT sum(g), avg(g), min(g), max(g) FROM i2_t;
SELECT sum(v) FROM i2_t WHERE v IS NULL;
SELECT count(DISTINCT g) FROM i2_t;
SELECT DISTINCT g FROM i2_t ORDER BY g;
SELECT v, count(*) FROM i2_t GROUP BY v ORDER BY v;
SELECT min(v)::int4, max(v)::int4 FROM i2_t WHERE g = 2::int2;

-- int2[] literals, constructors, containment, and subscripting
SELECT '{1,2,3}'::int2[], '{}'::int2[], '{NULL,1}'::int2[];
SELECT ARRAY[1, 2]::int2[], array_length('{1,2,3}'::int2[], 1), cardinality('{}'::int2[]);
SELECT 2::int2 = ANY('{1,2,3}'::int2[]), 9::int2 <> ALL('{1,2,3}'::int2[]);
SELECT array_cat('{1}'::int2[], '{2,3}'::int2[]), array_append('{1}'::int2[], 2::int2);
SELECT id, tags, tags[1] FROM i2_t ORDER BY id;
SELECT id FROM i2_t WHERE tags @> '{1}'::int2[] ORDER BY id;

-- NULL handling
SELECT NULL::int2, NULL::int2 + 1::int2, NULL::int2 = NULL::int2;
SELECT coalesce(v, 0::int2) FROM i2_t ORDER BY id;
SELECT x FROM (VALUES (1::int2), (2::int2), (NULL::int2)) AS t(x) ORDER BY x;

-- error parity (same SQLSTATE on both sides)
-- int2 arithmetic overflow (22003)
SELECT 32767::int2 + 1::int2;
SELECT (-32768)::int2 - 1::int2;
SELECT 200::int2 * 200::int2;
SELECT - ((-32768)::int2);
SELECT (-32768)::int2 / (-1)::int2;
-- integer division / modulo by zero (22012)
SELECT 1::int2 / 0::int2;
SELECT 1::int2 % 0::int2;
-- narrowing casts that do not fit in 16 bits (22003)
SELECT 40000::int4::int2;
SELECT 99999999999::int8::int2;
SELECT 1e10::numeric::int2;
SELECT 40000::float8::int2;
SELECT '40000'::int2;
-- malformed int2 input text (22P02)
SELECT 'abc'::int2;
SELECT '1.5'::int2;
SELECT ''::int2;
-- out-of-range value stored into an int2 column (22003)
INSERT INTO i2_t VALUES (6, 1, 40000, NULL);
-- there is no smallint -> boolean cast (42846)
SELECT 1::int2::bool;

DROP TABLE i2_t;
