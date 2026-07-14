-- SP39 Task 8: nested/generalized query expressions diffed against PostgreSQL
-- 18. The file keeps table names unique and uses explicit ORDER BY wherever row
-- order is observable.
CREATE TABLE nqe_nums (id int4);
INSERT INTO nqe_nums VALUES (1), (2), (3);

-- derived table over a set operation
SELECT d.id, d.label
FROM (VALUES (2, 'b') UNION SELECT 1, 'a') AS d(id, label)
ORDER BY d.id;

-- derived table over a tailed VALUES query expression
SELECT v.id
FROM (VALUES (3), (1), (2) ORDER BY 1 DESC LIMIT 2) AS v(id)
ORDER BY v.id;

-- tailed parenthesized query expressions as set-operation operands
(VALUES (2), (1) ORDER BY 1 LIMIT 1) UNION SELECT 3 ORDER BY 1;
(SELECT 1 UNION SELECT 2 ORDER BY 1 LIMIT 1) UNION SELECT 3 ORDER BY 1;

-- scalar subqueries over pure VALUES and over a set operation
SELECT (VALUES (42)) AS answer;
SELECT (VALUES (2) UNION SELECT 1 ORDER BY 1 LIMIT 1) AS first_value;

-- IN / EXISTS / ANY / ALL over VALUES and set-operation query expressions
SELECT id FROM nqe_nums WHERE id IN (VALUES (1), (3)) ORDER BY id;
SELECT EXISTS (VALUES (1) EXCEPT SELECT 1) AS has_row;
SELECT 3 > ALL (VALUES (1), (2)) AS all_less;
SELECT 2 = ANY (SELECT 1 UNION SELECT 2) AS any_equal;

-- query-level ORDER BY containing a scalar subquery
SELECT id
FROM nqe_nums
ORDER BY (SELECT id FROM (VALUES (2)) AS ord(id)), id;

-- error surface (SQLSTATE matched by the oracle)
SELECT (VALUES (1), (2));
SELECT (VALUES (1, 2));
