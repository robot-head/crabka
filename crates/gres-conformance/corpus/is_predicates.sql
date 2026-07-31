-- The `IS` predicate family and the SQL pattern operators, diffed against
-- PostgreSQL 18: `IS [NOT] NULL`, the three boolean tests, the null-safe
-- `IS [NOT] DISTINCT FROM`, `SIMILAR TO`, and the `ESCAPE` clause on all three
-- pattern operators. Every result-returning query is ORDER BY-stable.
CREATE TABLE isp (id int4, flag bool, note text);
INSERT INTO isp VALUES (1, true, 'a'), (2, false, NULL), (3, NULL, 'c');

-- IS [NOT] NULL.
SELECT id FROM isp WHERE flag IS NULL ORDER BY id;
SELECT id FROM isp WHERE note IS NOT NULL ORDER BY id;

-- The boolean tests, which never return NULL.
SELECT true IS TRUE, false IS TRUE, NULL IS TRUE;
SELECT true IS NOT TRUE, false IS NOT TRUE, NULL IS NOT TRUE;
SELECT true IS FALSE, false IS FALSE, NULL IS FALSE;
SELECT true IS NOT FALSE, false IS NOT FALSE, NULL IS NOT FALSE;
SELECT true IS UNKNOWN, false IS UNKNOWN, NULL IS UNKNOWN, NULL IS NOT UNKNOWN;
SELECT id, flag IS TRUE, flag IS NOT TRUE, flag IS UNKNOWN FROM isp ORDER BY id;
SELECT id FROM isp WHERE flag IS NOT TRUE ORDER BY id;
SELECT id FROM isp WHERE (id > 1) IS TRUE ORDER BY id;

-- IS [NOT] DISTINCT FROM.
SELECT NULL IS DISTINCT FROM NULL, 1 IS DISTINCT FROM NULL, 1 IS DISTINCT FROM 1, 1 IS DISTINCT FROM 2;
SELECT NULL IS NOT DISTINCT FROM NULL, 1 IS NOT DISTINCT FROM NULL, 1 IS NOT DISTINCT FROM 1;
SELECT 'a' IS DISTINCT FROM 'b', 'a' IS NOT DISTINCT FROM 'a';
SELECT id FROM isp WHERE note IS DISTINCT FROM 'a' ORDER BY id;
SELECT id FROM isp WHERE note IS NOT DISTINCT FROM NULL ORDER BY id;
SELECT id FROM isp WHERE flag IS DISTINCT FROM true AND id > 0 ORDER BY id;
SELECT id, note IS DISTINCT FROM 'c' FROM isp ORDER BY id;

-- SIMILAR TO: `%`/`_` are SQL wildcards, the regexp metacharacters keep their
-- meaning, and `.` does not.
SELECT 'abc' SIMILAR TO 'a%', 'abc' SIMILAR TO '_b_', 'abc' SIMILAR TO 'a|b';
SELECT 'abc' SIMILAR TO '(a|b)bc', 'abc' SIMILAR TO 'a{1,2}bc', 'abc' SIMILAR TO '[ab]bc';
SELECT 'ababc' SIMILAR TO '(ab)*c', 'abbc' SIMILAR TO 'ab+c', 'ac' SIMILAR TO 'ab?c';
SELECT 'abc' SIMILAR TO 'a.c', 'a.c' SIMILAR TO 'a.c';
SELECT 'abc' NOT SIMILAR TO 'a%', NULL SIMILAR TO 'a', 'abc' SIMILAR TO NULL;
SELECT 'a%c' SIMILAR TO 'a#%c' ESCAPE '#', 'a%c' SIMILAR TO 'a\%c';
SELECT 'abc' SIMILAR TO 'a%c' ESCAPE '';
SELECT note FROM isp WHERE note SIMILAR TO '[ac]' ORDER BY note;

-- The ESCAPE clause on LIKE / ILIKE.
SELECT 'a_c' LIKE 'aX_c' ESCAPE 'X', 'a_c' LIKE 'a_c' ESCAPE '';
SELECT 'aXc' LIKE 'aXc' ESCAPE 'X', 'ABC' ILIKE 'aXbc' ESCAPE 'X';
SELECT note FROM isp WHERE note LIKE '#a' ESCAPE '#' ORDER BY note;
-- With ESCAPE '%' the wildcard is out of service; a trailing lone escape only
-- fails to match once the subject is exhausted.
SELECT 'a%' LIKE 'a%%' ESCAPE '%', 'a' LIKE 'a%' ESCAPE '%';
SELECT 'a' LIKE 'a\', '' LIKE '\', 'a' SIMILAR TO 'a#' ESCAPE '#';
SELECT 'ab' LIKE 'a\';

-- Error parity.
SELECT 1 IS TRUE;
SELECT 1 IS NOT UNKNOWN;
SELECT 'abc' LIKE 'abc' ESCAPE 'xy';
SELECT 'abc' SIMILAR TO 'abc' ESCAPE 'xy';
DROP TABLE isp;
