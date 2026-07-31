-- SQL/JSON standard syntax: IS JSON, the JSON_OBJECT / JSON_ARRAY constructors
-- and aggregates, and the JSON_EXISTS / JSON_VALUE / JSON_QUERY functions.
--
-- crabka stores `json` as `jsonb`, so every construct whose PostgreSQL result
-- type is `json` is written here with an explicit `::jsonb` or
-- `RETURNING jsonb`; see the compatibility matrix for the divergence the bare
-- forms carry.

-- ---- IS [NOT] JSON ----
SELECT '1' IS JSON;
SELECT '1' IS NOT JSON;
SELECT 'x' IS JSON;
SELECT NULL IS JSON;
SELECT '{"a": 1}' IS JSON;
SELECT '{"a": 1}' IS JSON VALUE;
SELECT '{"a": 1}' IS JSON OBJECT;
SELECT '{"a": 1}' IS JSON ARRAY;
SELECT '{"a": 1}' IS JSON SCALAR;
SELECT '[1]' IS JSON ARRAY;
SELECT '[1]' IS JSON OBJECT;
SELECT '"s"' IS JSON SCALAR;
SELECT 'true' IS JSON SCALAR;
SELECT 'null' IS JSON SCALAR;
SELECT '{"a": 1, "a": 2}' IS JSON;
SELECT '{"a": 1, "a": 2}' IS JSON WITH UNIQUE KEYS;
SELECT '{"a": 1, "a": 2}' IS JSON WITHOUT UNIQUE KEYS;
SELECT '{"a": 1, "b": 2}' IS JSON WITH UNIQUE KEYS;
SELECT '{"a": {"b": 1, "b": 2}}' IS JSON WITH UNIQUE KEYS;
SELECT '[1]' IS NOT JSON OBJECT;
SELECT '{"a": 1}'::jsonb IS JSON OBJECT;
SELECT 1 IS JSON;

-- ---- JSON_OBJECT ----
SELECT JSON_OBJECT('a' VALUE 1 RETURNING jsonb);
SELECT JSON_OBJECT('a': 1 RETURNING jsonb);
SELECT JSON_OBJECT('a' VALUE 1, 'b' VALUE 'x' RETURNING jsonb);
SELECT JSON_OBJECT('a': 1, 'b': 2 RETURNING jsonb);
SELECT JSON_OBJECT(RETURNING jsonb);
SELECT JSON_OBJECT('a': NULL RETURNING jsonb);
SELECT JSON_OBJECT('a': 1, 'b': NULL ABSENT ON NULL RETURNING jsonb);
SELECT JSON_OBJECT('a': 1, 'b': NULL NULL ON NULL RETURNING jsonb);
SELECT JSON_OBJECT('a': 1, 'a': 2 RETURNING jsonb);
SELECT JSON_OBJECT('a': 1, 'a': 2 WITH UNIQUE KEYS RETURNING jsonb);
SELECT JSON_OBJECT('a': 1, 'b': 2 WITH UNIQUE KEYS RETURNING jsonb);
SELECT JSON_OBJECT('b': 2, 'a': 1 RETURNING jsonb);
SELECT JSON_OBJECT(NULL: 1 RETURNING jsonb);
SELECT JSON_OBJECT('a': 1)::jsonb;
SELECT JSON_OBJECT('a': '{"x": 1}'::jsonb RETURNING jsonb);
SELECT JSON_OBJECT('a': ARRAY[1, 2] RETURNING jsonb);

-- the two-argument array function keeps its own grammar
SELECT json_object('{a, 1, b, 2}'::text[])::jsonb;
SELECT json_object('{a, b}'::text[], '{1, 2}'::text[])::jsonb;
SELECT jsonb_object('{a, 1, b, 2}'::text[]);
SELECT jsonb_object('{a, b}'::text[], '{1, 2}'::text[]);
SELECT jsonb_object('{a, 1, b}'::text[]);

-- ---- JSON_ARRAY ----
SELECT JSON_ARRAY(1, 2, 3 RETURNING jsonb);
SELECT JSON_ARRAY(RETURNING jsonb);
SELECT JSON_ARRAY(1, NULL, 2 RETURNING jsonb);
SELECT JSON_ARRAY(1, NULL, 2 NULL ON NULL RETURNING jsonb);
SELECT JSON_ARRAY(1, NULL, 2 ABSENT ON NULL RETURNING jsonb);
SELECT JSON_ARRAY('a', true, 1.5 RETURNING jsonb);
SELECT JSON_ARRAY(1, 2)::jsonb;

-- ---- JSON_SCALAR / JSON / JSON_SERIALIZE ----
SELECT JSON_SCALAR(1)::jsonb;
SELECT JSON_SCALAR('a')::jsonb;
SELECT JSON_SCALAR(true)::jsonb;
SELECT JSON_SCALAR(NULL)::jsonb;
SELECT JSON('{"a": 1}')::jsonb;
SELECT JSON('{"a": 1, "a": 2}')::jsonb;
SELECT JSON('{"a": 1, "a": 2}' WITH UNIQUE KEYS)::jsonb;
SELECT JSON('nope')::jsonb;
SELECT JSON_SERIALIZE('{"a": 1}' RETURNING text);
SELECT JSON_SERIALIZE('{"a": 1}');

-- ---- JSON_EXISTS / JSON_VALUE / JSON_QUERY ----
SELECT JSON_EXISTS(jsonb '{"a": 1}', '$.a');
SELECT JSON_EXISTS(jsonb '{"a": 1}', '$.b');
SELECT JSON_EXISTS(jsonb '{"a": 1}', 'strict $.b');
SELECT JSON_EXISTS(jsonb '{"a": 1}', 'strict $.b' TRUE ON ERROR);
SELECT JSON_EXISTS(jsonb '{"a": 1}', 'strict $.b' ERROR ON ERROR);
SELECT JSON_EXISTS(NULL::jsonb, '$.a');
SELECT JSON_EXISTS(jsonb '{"a": [1, 2]}', '$.a[*] ? (@ > 1)');
SELECT JSON_EXISTS(jsonb '{"a": 1}', '$.a ? (@ == $v)' PASSING 1 AS v);

SELECT JSON_VALUE(jsonb '{"a": 1}', '$.a');
SELECT JSON_VALUE(jsonb '{"a": "x"}', '$.a');
SELECT JSON_VALUE(jsonb '{"a": 1}', '$.a' RETURNING int);
SELECT JSON_VALUE(jsonb '{"a": 1}', '$.b');
SELECT JSON_VALUE(jsonb '{"a": 1}', '$.b' DEFAULT '42' ON EMPTY);
SELECT JSON_VALUE(jsonb '{"a": 1}', '$.b' ERROR ON EMPTY);
SELECT JSON_VALUE(jsonb '{"a": [1, 2]}', '$.a');
SELECT JSON_VALUE(jsonb '{"a": [1, 2]}', '$.a' ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '{"a": [1, 2]}', '$.a' DEFAULT 'no' ON ERROR);
SELECT JSON_VALUE(jsonb '{"a": null}', '$.a');
SELECT JSON_VALUE(NULL::jsonb, '$.a');
SELECT JSON_VALUE(jsonb '{"a": 1}', '$.a ? (@ == $v)' PASSING 1 AS v);

SELECT JSON_QUERY(jsonb '{"a": [1, 2]}', '$.a');
SELECT JSON_QUERY(jsonb '{"a": [1, 2]}', '$.a[*]');
SELECT JSON_QUERY(jsonb '{"a": [1, 2]}', '$.a[*]' WITH WRAPPER);
SELECT JSON_QUERY(jsonb '{"a": [1, 2]}', '$.a[*]' WITH UNCONDITIONAL WRAPPER);
SELECT JSON_QUERY(jsonb '{"a": [1, 2]}', '$.a' WITH CONDITIONAL WRAPPER);
SELECT JSON_QUERY(jsonb '{"a": [1, 2]}', '$.a[*]' WITH CONDITIONAL WRAPPER);
SELECT JSON_QUERY(jsonb '{"a": "x"}', '$.a');
SELECT JSON_QUERY(jsonb '{"a": "x"}', '$.a' OMIT QUOTES);
SELECT JSON_QUERY(jsonb '{"a": "x"}', '$.a' KEEP QUOTES);
SELECT JSON_QUERY(jsonb '{"a": 1}', '$.b');
SELECT JSON_QUERY(jsonb '{"a": 1}', '$.b' EMPTY ARRAY ON EMPTY);
SELECT JSON_QUERY(jsonb '{"a": 1}', '$.b' EMPTY OBJECT ON EMPTY);
SELECT JSON_QUERY(jsonb '{"a": 1}', '$.b' NULL ON EMPTY);
SELECT JSON_QUERY(jsonb '{"a": 1}', '$.b' ERROR ON EMPTY);
SELECT JSON_QUERY(jsonb '{"a": [1, 2]}', '$.a[*]' ERROR ON ERROR);
SELECT JSON_QUERY(NULL::jsonb, '$.a');

-- ---- strip_nulls with the PG18 strip_in_arrays argument ----
SELECT jsonb_strip_nulls('[1, 2, null, 3]');
SELECT jsonb_strip_nulls('[1, 2, null, 3]', true);
SELECT jsonb_strip_nulls('[1, 2, null, 3]', false);
SELECT jsonb_strip_nulls('[1, {"a": 1, "b": null}, null]', true);
SELECT jsonb_strip_nulls('{"a": {"b": null, "c": [1, null]}}', true);
SELECT jsonb_strip_nulls(NULL, true);
SELECT json_strip_nulls('null', true)::jsonb;

-- ---- the JSON aggregates ----
CREATE TABLE sjson_t (k text, v int);
INSERT INTO sjson_t VALUES ('a', 1), ('b', 2), ('c', NULL);
SELECT JSON_ARRAYAGG(v)::jsonb FROM sjson_t WHERE v IS NOT NULL;
SELECT JSON_OBJECTAGG(k VALUE v)::jsonb FROM sjson_t;
SELECT JSON_OBJECTAGG(k: v)::jsonb FROM sjson_t;
SELECT k, JSON_ARRAYAGG(v)::jsonb FROM sjson_t WHERE v IS NOT NULL GROUP BY k ORDER BY k;
DROP TABLE sjson_t;

-- ---- over a table ----
CREATE TABLE sjson_doc (id int, j jsonb);
INSERT INTO sjson_doc VALUES
  (1, '{"name": "ada", "n": 3}'),
  (2, '{"name": "bob", "n": 0}'),
  (3, '{"other": 1}');
SELECT id FROM sjson_doc WHERE JSON_EXISTS(j, '$.name') ORDER BY id;
SELECT id, JSON_VALUE(j, '$.name') FROM sjson_doc ORDER BY id;
SELECT id, JSON_VALUE(j, '$.n' RETURNING int) FROM sjson_doc ORDER BY id;
SELECT id, JSON_QUERY(j, '$.name') FROM sjson_doc ORDER BY id;
SELECT id, j IS JSON OBJECT FROM sjson_doc ORDER BY id;
DROP TABLE sjson_doc;
