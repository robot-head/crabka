-- jsonpath: the `@?` / `@@` operators and the jsonb_path_* function family.
-- Every statement is diffed against a live PostgreSQL 18.4 oracle.

-- ---- path accessors ----
SELECT jsonb_path_query_array('{"a": [1, 2, 3]}', '$.a[*]');
SELECT jsonb_path_query_array('{"a": [1, 2, 3]}', '$.a[0]');
SELECT jsonb_path_query_array('{"a": [1, 2, 3]}', '$.a[0 to 1]');
SELECT jsonb_path_query_array('{"a": [1, 2, 3]}', '$.a[last]');
SELECT jsonb_path_query_array('{"a": [1, 2, 3]}', '$.a[last - 1]');
SELECT jsonb_path_query_array('{"a": [1, 2, 3]}', '$.a[1 to last]');
SELECT jsonb_path_query_array('[1, 2, 3]', '$[0, 2]');
SELECT jsonb_path_query_array('[1, 2, 3]', '$[0 to 1, 2]');
SELECT jsonb_path_query_array('{"a": 1, "b": 2}', '$.*');
SELECT jsonb_path_query_array('[{"a": 1}, {"a": 2}]', '$[*].a');
SELECT jsonb_path_query_array('{"a": {"b": {"c": 1}}}', '$.a.b.c');
SELECT jsonb_path_query_array('{}', '$.a.b');
SELECT jsonb_path_query_array('{"a": 1, "b": {"c": 2}}', '$.**');
SELECT jsonb_path_query_array('{"a": {"b": {"c": 1}}}', '$.**{1}');
SELECT jsonb_path_query_array('{"a": {"b": {"c": 1}}}', '$.**{1 to 2}');
SELECT jsonb_path_query_array('{"a": {"b": 1}, "c": 2}', '$.**.b');
SELECT jsonb_path_query_array('{"a b": 1}', '$."a b"');

-- ---- arithmetic ----
SELECT jsonb_path_query_array('{"a": 1}', '$.a + 1');
SELECT jsonb_path_query_array('{"a": 1}', '$.a - 3');
SELECT jsonb_path_query_array('{"a": 3}', '$.a * 4');
SELECT jsonb_path_query_array('{"a": 3}', '$.a / 2');
SELECT jsonb_path_query_array('{"a": 7}', '$.a % 4');
SELECT jsonb_path_query_array('{"a": 1}', '-$.a');
SELECT jsonb_path_query_array('{"a": 1}', '+$.a');
SELECT jsonb_path_query_array('{"a": 1}', '$.a / 0');

-- ---- filters ----
SELECT jsonb_path_query_array('[1, 2, 3]', '$[*] ? (@ > 1)');
SELECT jsonb_path_query_array('[1, 2, 3]', '$[*] ? (@ >= 2 && @ <= 3)');
SELECT jsonb_path_query_array('[1, 2, 3]', '$[*] ? (@ == 1 || @ == 3)');
SELECT jsonb_path_query_array('[1, 2, 3]', '$[*] ? (@ != 2)');
SELECT jsonb_path_query_array('[1, 2, 3]', '$[*] ? (@ < 3)');
SELECT jsonb_path_query_array('{"a": 1}', '$ ? (!(@.a == 2))');
SELECT jsonb_path_query_array('{"a": 1}', '$ ? (exists(@.a))');
SELECT jsonb_path_query_array('{"a": 1}', '$ ? (exists(@.b))');
SELECT jsonb_path_query_array('{"a": 1}', '$ ? ((@.b == 2) is unknown)');
SELECT jsonb_path_query_array('{"a": 1}', '$.a ? (@ == 1) ? (@ > 0)');
SELECT jsonb_path_query_array('["abc", "abd", "xyz"]', '$[*] ? (@ starts with "ab")');
SELECT jsonb_path_query_array('["abc", "abd", "xyz"]', '$[*] ? (@ like_regex "^a.c$")');
SELECT jsonb_path_query_array('["abc", "ABC"]', '$[*] ? (@ like_regex "abc" flag "i")');
SELECT jsonb_path_query_array('[1, "a", true, null]', '$[*] ? (@ > 0)');
SELECT jsonb_path_query_array('[1, "a"]', '$[*] ? (@ == "a")');

-- ---- item methods ----
SELECT jsonb_path_query_array('{"a": -3}', '$.a.abs()');
SELECT jsonb_path_query_array('{"a": 1.7}', '$.a.ceiling()');
SELECT jsonb_path_query_array('{"a": -1.7}', '$.a.ceiling()');
SELECT jsonb_path_query_array('{"a": 1.7}', '$.a.floor()');
SELECT jsonb_path_query_array('{"a": -1.7}', '$.a.floor()');
SELECT jsonb_path_query_array('{"a": "1.5"}', '$.a.double()');
SELECT jsonb_path_query_array('{"a": [1, 2]}', '$.a.size()');
SELECT jsonb_path_query_array('{"a": [1, 2]}', '$.a.type()');
SELECT jsonb_path_query_array('null', '$.type()');
SELECT jsonb_path_query_array('[]', '$.type()');
SELECT jsonb_path_query_array('"s"', '$.type()');
SELECT jsonb_path_query_array('true', '$.type()');
SELECT jsonb_path_query_array('1', '$.type()');
SELECT jsonb_path_query_array('{"a": "3"}', '$.a.bigint()');
SELECT jsonb_path_query_array('{"a": "3"}', '$.a.integer()');
SELECT jsonb_path_query_array('{"a": "3.7"}', '$.a.decimal()');
SELECT jsonb_path_query_array('{"a": "3.5"}', '$.a.number()');
SELECT jsonb_path_query_array('{"a": 3.5}', '$.a.string()');
SELECT jsonb_path_query_array('{"a": true}', '$.a.string()');
SELECT jsonb_path_query_array('{"a": "true"}', '$.a.boolean()');
SELECT jsonb_path_query_array('{"a": 0}', '$.a.boolean()');
SELECT jsonb_path_query_array('{"a": 1, "b": "x"}', '$.keyvalue()');
SELECT jsonb_path_query_array('{"a": "x"}', '$.a.double()');
SELECT jsonb_path_query_array('{"a": [1, 2]}', '$.a.abs()');

-- ---- lax versus strict ----
SELECT jsonb_path_query_array('{"a": 1}', 'lax $.b');
SELECT jsonb_path_query_array('{"a": 1}', 'strict $.b');
SELECT jsonb_path_query_array('[{"a": 1}, {"a": 2}]', 'lax $.a');
SELECT jsonb_path_query_array('[{"a": 1}, {"a": 2}]', 'strict $.a');
SELECT jsonb_path_query_array('{"a": 1}', 'lax $[*]');
SELECT jsonb_path_query_array('{"a": 1}', 'strict $[*]');
SELECT jsonb_path_query_array('1', 'lax $[0]');
SELECT jsonb_path_query_array('1', 'strict $[0]');
SELECT jsonb_path_query_array('[1, 2]', 'lax $[5]');
SELECT jsonb_path_query_array('[1, 2]', 'strict $[5]');
SELECT jsonb_path_query_array('[[1, 2], [3, 4]]', 'lax $[*][*]');
SELECT jsonb_path_query_array('{"a": [1, 2]}', 'lax $.a ? (@ > 1)');
SELECT jsonb_path_query_array('{"a": [1, 2]}', 'strict $.a ? (@ > 1)');
SELECT jsonb_path_query_array('1', 'lax $.size()');
SELECT jsonb_path_query_array('1', 'strict $.size()');
SELECT jsonb_path_query_array('[{"a": 1}]', 'strict $.keyvalue()');

-- ---- vars and silent ----
SELECT jsonb_path_query_array('{"x": 1}', '$.x ? (@ == $v)', '{"v": 1}');
SELECT jsonb_path_query_array('{"x": 1}', '$.x ? (@ == $v)', '{"v": 2}');
SELECT jsonb_path_query_array('{"x": 1}', '$.x ? (@ == $v)');
SELECT jsonb_path_query_array('{"x": 1}', '$.x', '[]');
SELECT jsonb_path_query_array('{"a": 1}', 'strict $.b', '{}', true);
SELECT jsonb_path_exists('{"a": 1}', 'strict $.b', '{}', true);
SELECT jsonb_path_exists('{"a": 1}', 'strict $.b', '{}', false);

-- ---- the function family ----
SELECT jsonb_path_exists('{"a": 1}', '$.a');
SELECT jsonb_path_exists('{"a": 1}', '$.b');
SELECT jsonb_path_exists(NULL, '$.a');
SELECT jsonb_path_exists('{"a": 1}', NULL);
SELECT jsonb_path_match('{"a": 1}', '$.a == 1');
SELECT jsonb_path_match('{"a": 1}', '$.a > 5');
SELECT jsonb_path_match('{"a": 1}', '$.zz > 5');
SELECT jsonb_path_match('{"a": 1}', 'exists($.a)');
SELECT jsonb_path_query_first('[1, 2, 3]', '$[*]');
SELECT jsonb_path_query_first('[1, 2, 3]', '$[*] ? (@ > 5)');
SELECT jsonb_path_query_array('[1, 2, 3]', '$[*] ? (@ > 5)');
SELECT jsonb_path_exists_tz('{"a": 1}', '$.a');
SELECT jsonb_path_match_tz('{"a": 1}', '$.a == 1');
SELECT jsonb_path_query_array_tz('{"a": 1}', '$.a');
SELECT jsonb_path_query_first_tz('{"a": 1}', '$.a');
SELECT * FROM jsonb_path_query('{"a": [1, 2, 3]}', '$.a[*]');
SELECT jsonb_path_query('{"a": [1, 2, 3]}', '$.a[*]');
SELECT * FROM jsonb_path_query('{"a": 1}', '$.b');
SELECT * FROM jsonb_path_query('[1, 2]', '$[*] ? (@ > $min)', '{"min": 1}');

-- ---- the operators ----
SELECT '{"a": 1}'::jsonb @? '$.a';
SELECT '{"a": 1}'::jsonb @? '$.b';
SELECT '{"a": 1}'::jsonb @? '$ ? (@.a == 1)';
SELECT NULL::jsonb @? '$.a';
SELECT '{"a": 1}'::jsonb @? NULL;
SELECT '{"a": 1}'::jsonb @@ '$.a == 1';
SELECT '{"a": 1}'::jsonb @@ '$.a > 5';
SELECT '{"a": 1}'::jsonb @@ '$.zz > 5';
SELECT '{"a": 1}'::jsonb @@ 'exists($.a)';
SELECT '{"a": 1}'::jsonb @@ 'exists($.b)';
SELECT '[1, 2, 3]'::jsonb @@ '$[*] > 2';
SELECT '{"a": 1}'::jsonb @@ 'strict $.b == 1';
SELECT NULL::jsonb @@ '$.a == 1';

-- ---- syntax errors ----
SELECT jsonb_path_query_array('{"a": 1}', '$.');
SELECT jsonb_path_query_array('{"a": 1}', '$.a.zzz()');
SELECT jsonb_path_query_array('{"a": 1}', 'bogus path');
SELECT '{"a": 1}'::jsonb @? '$[';

-- ---- over a table ----
CREATE TABLE jp_doc (id int, j jsonb);
INSERT INTO jp_doc VALUES
  (1, '{"name": "ada", "tags": ["x", "y"], "age": 36}'),
  (2, '{"name": "bob", "tags": [], "age": 25}'),
  (3, '{"name": "cy", "age": null}'),
  (4, NULL);
SELECT id FROM jp_doc WHERE j @? '$.tags[*]' ORDER BY id;
SELECT id FROM jp_doc WHERE j @@ '$.age > 30' ORDER BY id;
SELECT id FROM jp_doc WHERE j @@ '$.age == null' ORDER BY id;
SELECT count(*) FROM jp_doc WHERE j @? '$ ? (@.name starts with "a")';
SELECT id, jsonb_path_query_first(j, '$.name') FROM jp_doc ORDER BY id;
SELECT id, jsonb_path_query_array(j, '$.tags[*]') FROM jp_doc ORDER BY id;
SELECT id, jsonb_path_exists(j, '$.tags') FROM jp_doc ORDER BY id;
SELECT id, jsonb_path_match(j, '$.age >= 25') FROM jp_doc ORDER BY id;
DROP TABLE jp_doc;
