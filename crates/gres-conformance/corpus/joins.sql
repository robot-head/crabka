-- SP33: SQL joins (same-range). Diffed against real PostgreSQL.
-- Every SELECT carries a fully-determining ORDER BY so row order matches the
-- oracle. Avoids the documented USING/NATURAL deviation (the merged column is
-- referenced only as the bare name, never `tbl.col`). Integer/text only.

CREATE TABLE emp (id int4, name text, dept_id int4);
CREATE TABLE dept (id int4, dname text);
INSERT INTO emp VALUES (1,'ann',10),(2,'bob',20),(3,'cy',NULL);
INSERT INTO dept VALUES (10,'eng'),(20,'sales'),(30,'ops');

-- INNER JOIN ... ON
SELECT emp.name, dept.dname FROM emp JOIN dept ON emp.dept_id = dept.id ORDER BY emp.id;
SELECT emp.name, dept.dname FROM emp INNER JOIN dept ON emp.dept_id = dept.id ORDER BY emp.id;

-- Outer joins (NULL-extension on the unmatched side).
SELECT emp.name, dept.dname FROM emp LEFT JOIN dept ON emp.dept_id = dept.id ORDER BY emp.id;
SELECT emp.name, dept.dname FROM emp LEFT OUTER JOIN dept ON emp.dept_id = dept.id ORDER BY emp.id;
SELECT dept.dname, emp.name FROM emp RIGHT JOIN dept ON emp.dept_id = dept.id ORDER BY dept.id;
SELECT emp.id, emp.name, dept.id, dept.dname FROM emp FULL JOIN dept ON emp.dept_id = dept.id ORDER BY emp.id, dept.id;

-- CROSS JOIN and the comma form.
SELECT emp.name, dept.dname FROM emp CROSS JOIN dept ORDER BY emp.id, dept.id;
SELECT emp.name, dept.dname FROM emp, dept WHERE emp.dept_id = dept.id ORDER BY emp.id;

-- Qualified wildcard and full wildcard over a join.
SELECT emp.* FROM emp JOIN dept ON emp.dept_id = dept.id ORDER BY emp.id;
SELECT * FROM emp JOIN dept ON emp.dept_id = dept.id ORDER BY emp.id;

-- Aggregate / GROUP BY over a join.
SELECT count(*) FROM emp JOIN dept ON emp.dept_id = dept.id;
SELECT dept.dname, count(*) FROM emp JOIN dept ON emp.dept_id = dept.id GROUP BY dept.dname ORDER BY dept.dname;

-- Self-join with aliases.
CREATE TABLE node (id int4, parent int4);
INSERT INTO node VALUES (1,NULL),(2,1),(3,2);
SELECT c.id, p.id FROM node c JOIN node p ON c.parent = p.id ORDER BY c.id;

-- Multi-way (3-table) join.
CREATE TABLE t1 (id int4);
CREATE TABLE t2 (id int4, v int4);
CREATE TABLE t3 (id int4, w int4);
INSERT INTO t1 VALUES (1),(2);
INSERT INTO t2 VALUES (1,10),(2,20);
INSERT INTO t3 VALUES (10,100),(20,200);
SELECT t1.id, t3.w FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.v = t3.id ORDER BY t1.id;

-- Derived table (subquery in FROM).
SELECT d.name FROM (SELECT name, dept_id FROM emp WHERE dept_id IS NOT NULL) d ORDER BY d.name;
SELECT t2.id, d.mx FROM t2 JOIN (SELECT max(v) AS mx FROM t2) d ON t2.v = d.mx ORDER BY t2.id;

-- USING and NATURAL: the join column is merged once (referenced bare) and
-- positioned first by SELECT *.
CREATE TABLE l (k int4, lv text);
CREATE TABLE r (k int4, rv text);
INSERT INTO l VALUES (1,'l1'),(2,'l2');
INSERT INTO r VALUES (2,'r2'),(3,'r3');
SELECT * FROM l JOIN r USING (k) ORDER BY k;
SELECT k FROM l NATURAL JOIN r ORDER BY k;
SELECT k, lv, rv FROM l LEFT JOIN r USING (k) ORDER BY k;

-- Column-resolution error surface (identical SQLSTATE on both sides).
SELECT id FROM emp JOIN dept ON emp.dept_id = dept.id;
SELECT emp.nope FROM emp JOIN dept ON emp.dept_id = dept.id;
SELECT zzz.id FROM emp JOIN dept ON emp.dept_id = dept.id;
