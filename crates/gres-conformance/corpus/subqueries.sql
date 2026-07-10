-- SP34: uncorrelated subquery expressions — scalar (SELECT …), x [NOT] IN
-- (SELECT …), [NOT] EXISTS (…), and x op ANY|SOME|ALL (…) — diffed against
-- PostgreSQL 18. Correlated subqueries are deferred (SP35); derived tables in
-- FROM landed with SP33 joins. Every subquery here is uncorrelated and references
-- only co-located tables (all tables live on one range in the single-engine run).
CREATE TABLE emp (id int4, dept int4, salary int4);
INSERT INTO emp VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300), (4, 20, 50), (5, 30, 150);
CREATE TABLE dept (id int4);
INSERT INTO dept VALUES (10), (20);

-- scalar subquery in the projection and in WHERE
SELECT (SELECT max(salary) FROM emp) AS top;
SELECT id, salary FROM emp WHERE salary > (SELECT avg(salary) FROM emp) ORDER BY id;
-- a scalar subquery returning zero rows is NULL
SELECT (SELECT salary FROM emp WHERE id = 999) AS none;

-- IN / NOT IN (subquery)
SELECT id FROM emp WHERE dept IN (SELECT id FROM dept) ORDER BY id;
SELECT id FROM emp WHERE dept NOT IN (SELECT id FROM dept) ORDER BY id;

-- EXISTS / NOT EXISTS
SELECT EXISTS (SELECT 1 FROM emp WHERE salary > 250) AS has_big;
SELECT EXISTS (SELECT 1 FROM emp WHERE salary > 9999) AS has_huge;
SELECT id FROM emp WHERE NOT EXISTS (SELECT 1 FROM dept WHERE id = 99) ORDER BY id;

-- quantified ANY / ALL / SOME
SELECT id FROM emp WHERE salary > ALL (SELECT salary FROM emp WHERE dept = 30) ORDER BY id;
SELECT id FROM emp WHERE salary >= ANY (SELECT salary FROM emp WHERE dept = 20) ORDER BY id;
SELECT id FROM emp WHERE dept = SOME (SELECT id FROM dept) ORDER BY id;

-- subquery composed with aggregation in the OUTER query
SELECT dept, count(*) FROM emp WHERE salary > (SELECT min(salary) FROM emp) GROUP BY dept ORDER BY dept;

-- error surface (SQLSTATE matched by the oracle)
SELECT (SELECT salary FROM emp);
SELECT (SELECT id, salary FROM emp WHERE id = 1);
