-- SP34: uncorrelated subquery expressions — scalar (SELECT …), x [NOT] IN
-- (SELECT …), [NOT] EXISTS (…), and x op ANY|SOME|ALL (…) — diffed against
-- PostgreSQL 18. Correlated subqueries are deferred (SP35); derived tables in
-- FROM landed with SP33 joins. Every subquery here is uncorrelated and references
-- only co-located tables (all tables live on one range in the single-engine run).
CREATE TABLE sq_emp (id int4, sq_dept int4, salary int4);
INSERT INTO sq_emp VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300), (4, 20, 50), (5, 30, 150);
CREATE TABLE sq_dept (id int4);
INSERT INTO sq_dept VALUES (10), (20);

-- scalar subquery in the projection and in WHERE
SELECT (SELECT max(salary) FROM sq_emp) AS top;
SELECT id, salary FROM sq_emp WHERE salary > (SELECT avg(salary) FROM sq_emp) ORDER BY id;
-- a scalar subquery returning zero rows is NULL
SELECT (SELECT salary FROM sq_emp WHERE id = 999) AS none;

-- IN / NOT IN (subquery)
SELECT id FROM sq_emp WHERE sq_dept IN (SELECT id FROM sq_dept) ORDER BY id;
SELECT id FROM sq_emp WHERE sq_dept NOT IN (SELECT id FROM sq_dept) ORDER BY id;

-- EXISTS / NOT EXISTS
SELECT EXISTS (SELECT 1 FROM sq_emp WHERE salary > 250) AS has_big;
SELECT EXISTS (SELECT 1 FROM sq_emp WHERE salary > 9999) AS has_huge;
SELECT id FROM sq_emp WHERE NOT EXISTS (SELECT 1 FROM sq_dept WHERE id = 99) ORDER BY id;

-- quantified ANY / ALL / SOME
SELECT id FROM sq_emp WHERE salary > ALL (SELECT salary FROM sq_emp WHERE sq_dept = 30) ORDER BY id;
SELECT id FROM sq_emp WHERE salary >= ANY (SELECT salary FROM sq_emp WHERE sq_dept = 20) ORDER BY id;
SELECT id FROM sq_emp WHERE sq_dept = SOME (SELECT id FROM sq_dept) ORDER BY id;

-- subquery composed with aggregation in the OUTER query
SELECT sq_dept, count(*) FROM sq_emp WHERE salary > (SELECT min(salary) FROM sq_emp) GROUP BY sq_dept ORDER BY sq_dept;

-- error surface (SQLSTATE matched by the oracle)
SELECT (SELECT salary FROM sq_emp);
SELECT (SELECT id, salary FROM sq_emp WHERE id = 1);

-- A FROM-subquery may omit its alias (PostgreSQL 16 and later).
SELECT * FROM (SELECT 1 AS x);
SELECT count(*) FROM (SELECT 1);
SELECT x FROM (SELECT 1 AS x) WHERE x = 1;
SELECT x FROM (SELECT 1 AS x), (SELECT 2 AS y);
SELECT q.x FROM (SELECT 1 AS x) q;
