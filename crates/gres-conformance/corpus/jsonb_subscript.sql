-- jsonb subscripting: `j['k']`, `j[i]`, mixed and nested paths, and the
-- subscripted `UPDATE` target with PostgreSQL's path-creation semantics.

-- ---- reads ----
SELECT ('123'::jsonb)['a'];
SELECT ('123'::jsonb)[0];
SELECT ('123'::jsonb)[NULL];
SELECT ('{"a": 1}'::jsonb)['a'];
SELECT ('{"a": 1}'::jsonb)[0];
SELECT ('{"a": 1}'::jsonb)['not_exist'];
SELECT ('{"a": 1}'::jsonb)[NULL];
SELECT ('{"0": 1}'::jsonb)[0];
SELECT ('[1, "2", null]'::jsonb)['a'];
SELECT ('[1, "2", null]'::jsonb)[0];
SELECT ('[1, "2", null]'::jsonb)['1'];
SELECT ('[1, "2", null]'::jsonb)['-1'];
SELECT ('[1, "2", null]'::jsonb)[2];
SELECT ('[1, "2", null]'::jsonb)[3];
SELECT ('[1, "2", null]'::jsonb)[-2];
SELECT ('[1, "2", null]'::jsonb)[-4];
SELECT ('[1, "2", null]'::jsonb)[1]['a'];
SELECT ('[1, "2", null]'::jsonb)[1][0];
SELECT ('{"a": 1, "b": "c", "d": [1, 2, 3]}'::jsonb)['b'];
SELECT ('{"a": 1, "b": "c", "d": [1, 2, 3]}'::jsonb)['d'];
SELECT ('{"a": 1, "b": "c", "d": [1, 2, 3]}'::jsonb)['d'][1];
SELECT ('{"a": 1, "b": "c", "d": [1, 2, 3]}'::jsonb)['d']['a'];
SELECT ('{"a": {"a1": {"a2": "aaa"}}, "b": "bbb"}'::jsonb)['a']['a1'];
SELECT ('{"a": {"a1": {"a2": "aaa"}}, "b": "bbb"}'::jsonb)['a']['a1']['a2'];
SELECT ('{"a": {"a1": {"a2": "aaa"}}, "b": "bbb"}'::jsonb)['a']['a1']['a2']['a3'];
SELECT ('{"a": ["a1", {"b1": ["aaa", "bbb"]}], "b": "bb"}'::jsonb)['a'][1]['b1'];
SELECT ('{"a": ["a1", {"b1": ["aaa", "bbb"]}], "b": "bb"}'::jsonb)['a'][1]['b1'][1];
SELECT (NULL::jsonb)['a'];
SELECT pg_typeof(('{"a": 1}'::jsonb)['a']);

-- ---- unsupported subscript types ----
SELECT ('{"a": 1}'::jsonb)[1.0];
SELECT ('{"a": 1}'::jsonb)[true];
SELECT ('[1, 2]'::jsonb)[1::bigint];
-- Slices are refused by the parser, before an operand type is known, so they
-- report 0A000 rather than PostgreSQL's 42804 "jsonb subscript does not
-- support slices"; the cases are therefore not in this corpus.

-- ---- subscripted UPDATE ----
CREATE TABLE jsub_t (id int, j jsonb);
INSERT INTO jsub_t VALUES (1, '{}'), (2, '{"key": "value"}');
UPDATE jsub_t SET j['a'] = '1' WHERE id = 1;
SELECT * FROM jsub_t ORDER BY id;
UPDATE jsub_t SET j['a'] = '1' WHERE id = 2;
SELECT * FROM jsub_t ORDER BY id;
UPDATE jsub_t SET j['a'] = '"test"';
SELECT * FROM jsub_t ORDER BY id;
UPDATE jsub_t SET j['a'] = '{"b": 1}'::jsonb;
SELECT * FROM jsub_t ORDER BY id;
UPDATE jsub_t SET j['a'] = '[1, 2, 3]'::jsonb;
SELECT * FROM jsub_t ORDER BY id;
SELECT * FROM jsub_t WHERE j['key'] = '"value"';
SELECT * FROM jsub_t WHERE j['key_doesnt_exists'] = '"value"';
SELECT * FROM jsub_t WHERE j['key'] = '"wrong_value"';
UPDATE jsub_t SET j[NULL] = '1';
UPDATE jsub_t SET j['another_key'] = NULL;
SELECT * FROM jsub_t ORDER BY id;

-- a NULL container is created from scratch
INSERT INTO jsub_t VALUES (3, NULL);
UPDATE jsub_t SET j['a'] = '1' WHERE id = 3;
SELECT * FROM jsub_t ORDER BY id;
UPDATE jsub_t SET j = NULL WHERE id = 3;
UPDATE jsub_t SET j[0] = '1';
SELECT * FROM jsub_t ORDER BY id;

-- filling the gaps
DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, '[0]');
UPDATE jsub_t SET j[5] = '1';
SELECT * FROM jsub_t;
UPDATE jsub_t SET j[-4] = '1';
SELECT * FROM jsub_t;
UPDATE jsub_t SET j[-8] = '1';
SELECT * FROM jsub_t;

DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, '[]');
UPDATE jsub_t SET j[5] = '1';
SELECT * FROM jsub_t;

-- creating the whole path
DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, '{}');
UPDATE jsub_t SET j['a'][0]['b'][0]['c'] = '1';
SELECT * FROM jsub_t;

DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, '{}');
UPDATE jsub_t SET j['a'][2]['b'][2]['c'][2] = '1';
SELECT * FROM jsub_t;

DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, '{"b": 1}');
UPDATE jsub_t SET j['a'][0] = '2';
SELECT * FROM jsub_t;

-- an object container treats an integer subscript as a key
DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, '{}');
UPDATE jsub_t SET j[0]['a'] = '1';
SELECT * FROM jsub_t;

DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, '[]');
UPDATE jsub_t SET j[0]['a'] = '1';
UPDATE jsub_t SET j[2]['b'] = '2';
SELECT * FROM jsub_t;

-- overwriting an existing path
DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, '{}');
UPDATE jsub_t SET j['a']['b'][1] = '1';
UPDATE jsub_t SET j['a']['b'][10] = '1';
SELECT * FROM jsub_t;

DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, '[]');
UPDATE jsub_t SET j[0][0][0] = '1';
UPDATE jsub_t SET j[0][0][1] = '1';
SELECT * FROM jsub_t;

DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, '{}');
UPDATE jsub_t SET j['a']['b'][10] = '1';
UPDATE jsub_t SET j['a'][10][10] = '1';
SELECT * FROM jsub_t;

-- an empty sub-element
DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, '{"a": {}}');
UPDATE jsub_t SET j['a']['b']['c'][2] = '1';
SELECT * FROM jsub_t;

DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, '{"a": []}');
UPDATE jsub_t SET j['a'][1]['c'][2] = '1';
SELECT * FROM jsub_t;

-- a path step through a scalar
DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, '{"a": 1}');
UPDATE jsub_t SET j['a']['b'] = '1';
UPDATE jsub_t SET j['a']['b']['c'] = '1';
UPDATE jsub_t SET j['a'][0] = '1';
UPDATE jsub_t SET j['a'][0]['c'] = '1';
UPDATE jsub_t SET j['a'][0][0] = '1';
SELECT * FROM jsub_t;

DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, 'null');
UPDATE jsub_t SET j[0] = '1';
UPDATE jsub_t SET j[0][0] = '1';
SELECT * FROM jsub_t;

-- several subscripted entries in one SET, and a subscript from a column
DELETE FROM jsub_t;
INSERT INTO jsub_t VALUES (1, '{}');
UPDATE jsub_t SET j['x'] = '1', j['y'] = '2';
SELECT * FROM jsub_t;
UPDATE jsub_t SET j[id::text] = '9';
SELECT * FROM jsub_t;
UPDATE jsub_t SET j['z'] = '3' RETURNING j;
SELECT j['x'], j['z'] FROM jsub_t;
DROP TABLE jsub_t;
