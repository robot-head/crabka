-- Set-returning functions in FROM: WITH ORDINALITY, ROWS FROM (...), function
-- alias and column-definition lists, and TABLESAMPLE. Diffed against
-- PostgreSQL 18.4.
--
-- `WITH ORDINALITY` appends a bigint `ordinality` column counting output rows
-- from 1; `ROWS FROM (f, g)` runs its functions in lockstep and pads the shorter
-- ones with NULL. A bare alias renames the column of an item whose functions
-- produce exactly one column (the ordinality column keeps its own name), and a
-- column-alias list renames a prefix positionally — naming more columns than the
-- item has is 42P10. A column-*definition* list (`AS t(a int)`) is 42601 for
-- every function here, because none of them returns `record`.
--
-- TABLESAMPLE is covered only where the result does not depend on which rows a
-- partial sample draws: the 100% and 0% ends, and the error cases. crabka's
-- sampling is row-wise for both methods and does not reproduce PostgreSQL's
-- page-level SYSTEM draw, so an intermediate percentage is deliberately absent.

-- WITH ORDINALITY over each set-returning function.
SELECT * FROM generate_series(1, 3) WITH ORDINALITY;
SELECT * FROM generate_series(5, 1, -2) WITH ORDINALITY;
SELECT * FROM generate_series(1, 0) WITH ORDINALITY;
SELECT * FROM unnest(ARRAY['a', 'b', 'c']) WITH ORDINALITY;
SELECT * FROM unnest(ARRAY[]::int4[]) WITH ORDINALITY;
SELECT * FROM unnest(ARRAY[10, NULL, 30]) WITH ORDINALITY;
SELECT * FROM string_to_table('a,b,c', ',') WITH ORDINALITY;
SELECT * FROM regexp_split_to_table('a1b22c', '[0-9]+') WITH ORDINALITY;
SELECT * FROM generate_subscripts(ARRAY['p', 'q'], 1) WITH ORDINALITY;
SELECT * FROM jsonb_each('{"a": 1, "b": 2}'::jsonb) WITH ORDINALITY;
SELECT * FROM jsonb_object_keys('{"a": 1, "b": 2}'::jsonb) WITH ORDINALITY;
SELECT * FROM jsonb_array_elements('[10, 20]'::jsonb) WITH ORDINALITY;

-- The ordinality column is a bigint and is orderable/filterable like any other.
SELECT * FROM generate_series(10, 40, 10) WITH ORDINALITY AS t(v, n) WHERE n > 2 ORDER BY n;
SELECT n, v FROM generate_series(10, 40, 10) WITH ORDINALITY AS t(v, n) ORDER BY n DESC;
SELECT sum(n) FROM generate_series(1, 4) WITH ORDINALITY AS t(v, n);
SELECT pg_typeof(n) FROM generate_series(1, 1) WITH ORDINALITY AS t(v, n);

-- Aliasing rules for a function item.
SELECT * FROM generate_series(1, 2) AS g;
SELECT g FROM generate_series(1, 2) AS g ORDER BY g;
SELECT * FROM generate_series(1, 2) WITH ORDINALITY AS g;
SELECT * FROM generate_series(1, 2) WITH ORDINALITY AS g(a);
SELECT * FROM generate_series(1, 2) WITH ORDINALITY AS g(a, b);
SELECT * FROM generate_series(1, 2) AS g(a);
SELECT * FROM jsonb_each('{"a": 1}'::jsonb) AS j;
SELECT * FROM jsonb_each('{"a": 1}'::jsonb) AS j(k, val);
SELECT * FROM jsonb_each('{"a": 1}'::jsonb) AS j(k);
SELECT * FROM generate_series(1, 2) AS g(a, b);
SELECT * FROM unnest(ARRAY[1, 2]) AS t(x, y);

-- ROWS FROM (...).
SELECT * FROM ROWS FROM (generate_series(1, 2));
SELECT * FROM ROWS FROM (generate_series(1, 2)) AS g;
SELECT * FROM ROWS FROM (generate_series(1, 2)) AS g(x);
SELECT * FROM ROWS FROM (generate_series(1, 3), unnest(ARRAY['a', 'b'])) AS t(num, letter) ORDER BY num;
SELECT * FROM ROWS FROM (unnest(ARRAY['a', 'b']), generate_series(1, 3)) AS t(letter, num) ORDER BY num;
SELECT * FROM ROWS FROM (generate_series(1, 2), generate_series(10, 11)) AS t(a, b) ORDER BY a;
SELECT * FROM ROWS FROM (generate_series(1, 2), string_to_table('x,y,z', ',')) AS t(a, b) ORDER BY b;
SELECT * FROM ROWS FROM (jsonb_each('{"a": 1}'::jsonb)) ;
SELECT * FROM ROWS FROM (jsonb_each('{"a": 1}'::jsonb), generate_series(1, 2)) AS t(k, v, n) ORDER BY n;
SELECT * FROM ROWS FROM (generate_series(1, 2)) WITH ORDINALITY AS t(a, b) ORDER BY a;
SELECT * FROM ROWS FROM (generate_series(1, 3), unnest(ARRAY['a', 'b'])) WITH ORDINALITY AS t(num, letter, n) ORDER BY n;
SELECT * FROM ROWS FROM (generate_series(1, 0), unnest(ARRAY['a'])) AS t(a, b);
SELECT * FROM ROWS FROM (generate_series(1, 2)) AS t(a, b);

-- ROWS FROM composes with WHERE, ORDER BY and LIMIT like any other FROM item.
SELECT * FROM ROWS FROM (generate_series(1, 5)) AS t(a) WHERE a % 2 = 1 ORDER BY a;
SELECT * FROM ROWS FROM (generate_series(1, 5)) AS t(a) ORDER BY a DESC LIMIT 2;
SELECT count(*) FROM ROWS FROM (generate_series(1, 5), unnest(ARRAY['a'])) AS t(a, b);

-- Column-definition lists: 42601, because no function here returns `record`.
SELECT * FROM generate_series(1, 2) AS t(x int4);
SELECT * FROM unnest(ARRAY[1, 2]) AS t(x int4);
SELECT * FROM generate_series(1, 2) AS (x int4);
SELECT * FROM ROWS FROM (generate_series(1, 2) AS (x int4));
SELECT * FROM ROWS FROM (generate_series(1, 2), unnest(ARRAY['a']) AS (y text));

-- Combined with LATERAL and joins.
CREATE TABLE q3_srf (id int4, arr text);
INSERT INTO q3_srf VALUES (1, 'a,b'), (2, 'c');
SELECT t.id, o.piece, o.n
  FROM q3_srf t, LATERAL string_to_table(t.arr, ',') WITH ORDINALITY AS o(piece, n)
  ORDER BY t.id, o.n;
SELECT t.id, o.piece, o.n
  FROM q3_srf t LEFT JOIN LATERAL string_to_table(t.arr, ',') WITH ORDINALITY AS o(piece, n) ON true
  ORDER BY t.id, o.n;
SELECT t.id, r.a, r.b
  FROM q3_srf t, LATERAL ROWS FROM (generate_series(1, t.id), string_to_table(t.arr, ',')) AS r(a, b)
  ORDER BY t.id, r.a;

-- TABLESAMPLE: the deterministic ends and the error cases.
CREATE TABLE q3_samp (i int4, s text);
INSERT INTO q3_samp VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four');
SELECT count(*) FROM q3_samp TABLESAMPLE BERNOULLI (100);
SELECT count(*) FROM q3_samp TABLESAMPLE SYSTEM (100);
SELECT count(*) FROM q3_samp TABLESAMPLE BERNOULLI (0);
SELECT count(*) FROM q3_samp TABLESAMPLE SYSTEM (0);
SELECT count(*) FROM q3_samp TABLESAMPLE bernoulli (100);
SELECT count(*) FROM q3_samp TABLESAMPLE SYSTEM (100) REPEATABLE (7);
SELECT count(*) FROM q3_samp TABLESAMPLE BERNOULLI (100) REPEATABLE (0);
SELECT count(*) FROM q3_samp TABLESAMPLE BERNOULLI (0) REPEATABLE (12345);
SELECT i FROM q3_samp TABLESAMPLE BERNOULLI (100) ORDER BY i;
SELECT i FROM q3_samp AS x TABLESAMPLE SYSTEM (100) ORDER BY i;
SELECT i FROM q3_samp TABLESAMPLE BERNOULLI (100) WHERE i > 2 ORDER BY i;
SELECT count(*) FROM q3_samp TABLESAMPLE FOO (50);
SELECT count(*) FROM q3_samp TABLESAMPLE BERNOULLI (101);
SELECT count(*) FROM q3_samp TABLESAMPLE SYSTEM (-1);
SELECT count(*) FROM q3_samp TABLESAMPLE BERNOULLI (NULL);
SELECT count(*) FROM generate_series(1, 2) TABLESAMPLE BERNOULLI (100);

-- `SELECT *` over a relation whose column names repeat expands positionally, so
-- the canonical unaliased ROWS FROM works even though a bare reference to the
-- repeated name is still 42702.
SELECT * FROM ROWS FROM (generate_series(1, 3), generate_series(1, 2));
SELECT * FROM ROWS FROM (generate_series(1, 3), generate_series(1, 2)) t;
SELECT * FROM ROWS FROM (generate_series(1, 3), generate_series(1, 2)) WITH ORDINALITY;
SELECT * FROM ROWS FROM (generate_series(1, 0), generate_series(1, 2));
SELECT t.* FROM ROWS FROM (generate_series(1, 2), generate_series(1, 3)) t;
SELECT * FROM ROWS FROM (generate_series(1, 2), generate_series(1, 2), generate_series(1, 3));
SELECT count(*) FROM ROWS FROM (generate_series(1, 3), generate_series(1, 2));
SELECT * FROM q3_srf, ROWS FROM (generate_series(1, 2), generate_series(1, 1)) ORDER BY id, 3;
SELECT * FROM (SELECT * FROM ROWS FROM (generate_series(1, 2), generate_series(1, 1))) d;
SELECT generate_series FROM ROWS FROM (generate_series(1, 2), generate_series(1, 3));
SELECT * FROM unnest(ARRAY[1, 2], ARRAY['a', 'b', 'c']);
SELECT * FROM unnest(ARRAY[1, 2], ARRAY['a', 'b', 'c']) WITH ORDINALITY;
SELECT * FROM unnest(ARRAY[1, 2], ARRAY[3, 4]) AS u(x, y) ORDER BY x;

-- A null REPEATABLE seed is 2202G (invalid_tablesample_repeat), which is NOT the
-- 2202H a null or out-of-range percentage raises.
SELECT count(*) FROM q3_samp TABLESAMPLE SYSTEM (50) REPEATABLE (NULL);
SELECT count(*) FROM q3_samp TABLESAMPLE BERNOULLI (50) REPEATABLE (NULL);
SELECT count(*) FROM q3_samp TABLESAMPLE SYSTEM (NULL) REPEATABLE (1);

DROP TABLE q3_samp;
DROP TABLE q3_srf;
