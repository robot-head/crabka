-- T4 core: one-dimensional arrays, diffed against PostgreSQL 18.
-- `ARRAY[...]` and `'{...}'` literals with PostgreSQL quoting, subscripting,
-- `= ANY` / `<> ALL`, the `array_*` functions, `|| @> <@ &&`, `unnest` in FROM,
-- `array_agg`, `int4[]`/`text[]` columns, and unique-index canonicalization.
-- `array_agg` runs before the UPDATE below so the unordered aggregate input is
-- the same physical order on both sides. Multidimensional arrays, slices,
-- non-default lower bounds, and `ARRAY(subquery)` are deferred and absent.
SELECT ARRAY[1, 2, 3];
SELECT ARRAY['a', 'b'];
SELECT ARRAY['a b', 'c,d', NULL, ''];
SELECT ARRAY[1];
SELECT '{1,2,3}'::int4[];
SELECT '{}'::int4[];
SELECT '{1, 2}'::int4[];
SELECT '{NULL,1}'::int4[];
SELECT '{a,"b c",NULL}'::text[];
SELECT '{1,x}'::int4[];
SELECT (ARRAY[1, 2, 3])[2];
SELECT (ARRAY[1, 2, 3])[9];
SELECT (ARRAY['a', 'b'])[1];
SELECT 2 = ANY(ARRAY[1, 2, 3]);
SELECT 9 = ANY(ARRAY[1, 2, 3]);
SELECT 9 <> ALL(ARRAY[1, 2, 3]);
SELECT 2 <> ALL(ARRAY[1, 2, 3]);
SELECT array_length(ARRAY[1, 2, 3], 1);
SELECT array_length('{}'::int4[], 1);
SELECT cardinality(ARRAY[1, 2, 3]), cardinality('{}'::int4[]);
SELECT array_append(ARRAY[1, 2], 3);
SELECT array_prepend(0, ARRAY[1, 2]);
SELECT array_cat(ARRAY[1], ARRAY[2, 3]);
SELECT array_to_string(ARRAY['a', NULL, 'b'], ',');
SELECT array_to_string(ARRAY[1, 2], '-');
SELECT string_to_array('a,b,c', ',');
SELECT string_to_array('', ',');
SELECT ARRAY[1, 2] || ARRAY[3];
SELECT ARRAY[1, 2, 3] @> ARRAY[2];
SELECT ARRAY[2] <@ ARRAY[1, 2, 3];
SELECT ARRAY[1, 2] && ARRAY[2, 5];
SELECT ARRAY[1, 2] && ARRAY[5, 6];
SELECT ARRAY[1, 2] = ARRAY[1, 2];
SELECT ARRAY[1, 2] = ARRAY[1, 3];
SELECT ARRAY[1, 2] < ARRAY[1, 3];
SELECT unnest FROM unnest(ARRAY[3, 1, 2]) ORDER BY unnest;
SELECT * FROM unnest(ARRAY['b', 'a']) AS t ORDER BY 1;
SELECT count(*) FROM unnest(ARRAY['a', 'b']) AS t;
CREATE TABLE arr_t (id int4, tags int4[], labels text[]);
INSERT INTO arr_t VALUES (1, '{1,2}', ARRAY['x', 'y']);
INSERT INTO arr_t VALUES (2, ARRAY[3], '{z}');
INSERT INTO arr_t VALUES (3, NULL, NULL);
SELECT id, tags, labels FROM arr_t ORDER BY id;
SELECT id FROM arr_t WHERE tags @> ARRAY[1] ORDER BY id;
SELECT id FROM arr_t WHERE 3 = ANY(tags) ORDER BY id;
SELECT id FROM arr_t WHERE tags IS NULL;
SELECT id, array_length(tags, 1) FROM arr_t ORDER BY id;
SELECT id, tags[1] FROM arr_t ORDER BY id;
SELECT array_agg(id) FROM arr_t;
SELECT array_to_string(array_agg(id), ',') FROM arr_t;
UPDATE arr_t SET tags = array_append(tags, 9) WHERE id = 2;
SELECT tags FROM arr_t WHERE id = 2;
DELETE FROM arr_t WHERE labels @> ARRAY['z'];
SELECT id FROM arr_t ORDER BY id;
CREATE TABLE arr_u (tags int4[] UNIQUE);
INSERT INTO arr_u VALUES ('{1,2}');
INSERT INTO arr_u VALUES (ARRAY[1, 2]);
INSERT INTO arr_u VALUES (ARRAY[2, 1]);
SELECT count(*) FROM arr_u;
DROP TABLE arr_u;
DROP TABLE arr_t;
