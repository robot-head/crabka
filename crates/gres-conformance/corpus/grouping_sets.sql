-- GROUP BY grouping sets: ROLLUP, CUBE, GROUPING SETS, the empty grouping set,
-- the ALL/DISTINCT modifiers, SQL92 output references, and GROUPING().
-- Grouping-set output order is plan-dependent in PostgreSQL, so every row-producing
-- statement carries an explicit total ORDER BY.

CREATE TABLE q3gs (a int4, b int4, c int4, v int4);
INSERT INTO q3gs VALUES (1,1,1,10),(1,1,2,20),(1,2,1,30),(2,1,1,40),(2,2,2,50),(NULL,1,1,60);

SELECT a, b, count(*) FROM q3gs GROUP BY ROLLUP(a, b) ORDER BY a, b, 3;

SELECT a, b, count(*) FROM q3gs GROUP BY CUBE(a, b) ORDER BY a, b, 3;

SELECT a, b, count(*) FROM q3gs GROUP BY GROUPING SETS ((a, b), (a), (b), ()) ORDER BY a, b, 3;

SELECT a, count(*) FROM q3gs GROUP BY GROUPING SETS (a) ORDER BY a;

SELECT count(*) FROM q3gs GROUP BY GROUPING SETS (());

SELECT count(*) FROM q3gs GROUP BY ();

SELECT a, b, count(*) FROM q3gs GROUP BY a, ROLLUP(b) ORDER BY a, b;

SELECT a, b, count(*) FROM q3gs GROUP BY ROLLUP(a), ROLLUP(b) ORDER BY a, b, 3;

SELECT a, b, count(*) FROM q3gs GROUP BY GROUPING SETS (ROLLUP(a, b), (a), ()) ORDER BY a, b, 3;

SELECT a, b, c, count(*) FROM q3gs GROUP BY ROLLUP((a, b), c) ORDER BY a, b, c;

SELECT a, b, count(*) FROM q3gs GROUP BY (a, b) ORDER BY a, b;

SELECT a, count(*) FROM q3gs GROUP BY GROUPING SETS (a, a) ORDER BY a, 2;

SELECT a, b, count(*) FROM q3gs GROUP BY DISTINCT ROLLUP(a, b), ROLLUP(a) ORDER BY a, b, 3;

SELECT a, b, count(*) FROM q3gs GROUP BY ALL ROLLUP(a, b), ROLLUP(a) ORDER BY a, b, 3;

SELECT a, count(*) FROM q3gs GROUP BY DISTINCT a, a ORDER BY a;

SELECT a, count(*) FROM q3gs GROUP BY ALL a ORDER BY a;

-- GROUPING() bitmasks, most significant bit first, and the NULL-vs-grouped-NULL
-- distinction it is there to make.
SELECT a, b, grouping(a) ga, grouping(b) gb, grouping(a, b) gab, count(*)
FROM q3gs GROUP BY CUBE(a, b) ORDER BY gab, ga, gb, a, b;

SELECT a, grouping(a), count(*) FROM q3gs GROUP BY ROLLUP(a) ORDER BY 2, 1;

SELECT grouping(b, a), count(*) FROM q3gs GROUP BY CUBE(a, b) ORDER BY 1, 2;

SELECT a, grouping(a) FROM q3gs GROUP BY a ORDER BY a;

SELECT a, b, count(*) FROM q3gs GROUP BY CUBE(a, b) HAVING grouping(a) = 0 ORDER BY a, b, 3;

SELECT a, b, count(*) FROM q3gs GROUP BY ROLLUP(a, b) HAVING a IS NOT NULL ORDER BY a, b;

SELECT a, count(*) FROM q3gs GROUP BY ROLLUP(a) HAVING count(*) > 2 ORDER BY a;

-- Aggregates still see the real column values in an aggregated row; only the
-- grouping column itself reads NULL there.
SELECT a, sum(a), count(a), count(*) FROM q3gs GROUP BY ROLLUP(a) ORDER BY a, 2;

SELECT a, sum(v), min(v), max(v) FROM q3gs GROUP BY CUBE(a) ORDER BY a, 2;

SELECT a + 1, count(*) FROM q3gs GROUP BY ROLLUP(a) ORDER BY 1, 2;

SELECT coalesce(a, -1), count(*) FROM q3gs GROUP BY ROLLUP(a) ORDER BY 1, 2;

SELECT sum(a) + coalesce(a, 0), count(*) FROM q3gs GROUP BY ROLLUP(a) ORDER BY 1, 2;

SELECT a, count(DISTINCT b) FROM q3gs GROUP BY ROLLUP(a) ORDER BY a, 2;

-- Grouping by an expression rather than a bare column.
SELECT a * 10, count(*) FROM q3gs GROUP BY ROLLUP(a * 10) ORDER BY 1, 2;

SELECT a * 10, grouping(a * 10), count(*) FROM q3gs GROUP BY CUBE(a * 10) ORDER BY 2, 1;

-- SQL92 output references: an ordinal and an output label.
SELECT a AS ka, count(*) FROM q3gs GROUP BY 1 ORDER BY 1;

SELECT a AS ka, count(*) FROM q3gs GROUP BY ka ORDER BY 1;

SELECT a + 0 AS shifted, count(*) FROM q3gs GROUP BY shifted ORDER BY 1;

SELECT a AS ka, b, count(*) FROM q3gs GROUP BY ROLLUP(1, 2) ORDER BY 1, 2, 3;

SELECT a AS ka, count(*) FROM q3gs GROUP BY ROLLUP(ka) ORDER BY 1, 2;

SELECT a AS ka, count(*) FROM q3gs GROUP BY CUBE(1) ORDER BY 1, 2;

-- ORDER BY / LIMIT / DISTINCT over grouping-set output.
SELECT DISTINCT a, count(*) FROM q3gs GROUP BY ROLLUP(a) ORDER BY a, 2;

SELECT a, count(*) FROM q3gs GROUP BY ROLLUP(a) ORDER BY a DESC LIMIT 2;

SELECT a, count(*) FROM q3gs GROUP BY ROLLUP(a) ORDER BY a NULLS FIRST OFFSET 1;

SELECT count(*) FROM q3gs GROUP BY ROLLUP(a) ORDER BY a NULLS LAST;

-- Grouping sets over an empty input still emit the grand total.
SELECT a, count(*) FROM q3gs WHERE false GROUP BY ROLLUP(a);

SELECT a, b, count(*) FROM q3gs WHERE false GROUP BY CUBE(a, b);

SELECT a, count(*) FROM q3gs WHERE false GROUP BY GROUPING SETS ((a), ());

SELECT a, count(*) FROM q3gs WHERE false GROUP BY GROUPING SETS ((a));

SELECT count(*) FROM q3gs WHERE false GROUP BY ();

SELECT a, grouping(a), count(*) FROM q3gs WHERE false GROUP BY ROLLUP(a);

-- Grouping sets over a filtered subset and a join.
SELECT a, b, count(*) FROM q3gs WHERE v >= 30 GROUP BY ROLLUP(a, b) ORDER BY a, b, 3;

SELECT x.a, y.b, count(*)
FROM q3gs x JOIN q3gs y ON x.a = y.a
GROUP BY ROLLUP(x.a, y.b) ORDER BY 1, 2, 3;

-- Grouping sets inside a derived table and a CTE.
SELECT * FROM (SELECT a, count(*) AS n FROM q3gs GROUP BY ROLLUP(a)) d ORDER BY a, n;

WITH g AS (SELECT a, b, count(*) AS n FROM q3gs GROUP BY CUBE(a, b))
SELECT a, b, n FROM g ORDER BY a, b, n;

-- Window functions run ABOVE the grouping, so they see one row per grouping-set
-- group rather than one per input row.
CREATE TABLE q3gse (a int4, v int4);

SELECT a, sum(v), count(*) OVER () FROM q3gs GROUP BY ROLLUP(a) ORDER BY a, 2;

SELECT a, b, sum(v), count(*) OVER () FROM q3gs GROUP BY CUBE(a, b) ORDER BY a, b, 3;

SELECT a, b, sum(v), rank() OVER (ORDER BY sum(v))
FROM q3gs GROUP BY GROUPING SETS ((a), (b), ()) ORDER BY 4, a, b;

SELECT count(*) OVER () FROM q3gs GROUP BY GROUPING SETS (());

SELECT a, grouping(a), count(*) OVER (PARTITION BY grouping(a))
FROM q3gs GROUP BY ROLLUP(a) ORDER BY grouping(a), a;

SELECT count(*) FROM (SELECT a, count(*) OVER () FROM q3gs GROUP BY ROLLUP(a)) d;

SELECT a, count(*) OVER () FROM q3gs GROUP BY 1 ORDER BY 1;

SELECT a AS k, count(*) OVER () FROM q3gs GROUP BY k ORDER BY 1;

SELECT a, sum(v), count(*) OVER () FROM q3gse GROUP BY ROLLUP(a) ORDER BY a;

SELECT a, sum(v), count(*) OVER () FROM q3gse GROUP BY GROUPING SETS ((), ()) ORDER BY 1;

DROP TABLE q3gse;

-- DISTINCT ON runs over the grouped output, so it dedups grouping-set rows.
SELECT DISTINCT ON (a) a, sum(v) FROM q3gs GROUP BY CUBE(a, b) ORDER BY a, sum(v);

SELECT DISTINCT ON (a) a, b, sum(v) FROM q3gs GROUP BY a, b ORDER BY a, sum(v) DESC;

SELECT DISTINCT ON (grouping(a)) grouping(a), a, sum(v)
FROM q3gs GROUP BY CUBE(a, b) ORDER BY grouping(a), a, sum(v);

-- A grouping expression is matched by the column it resolves to, not by how it
-- was spelled, so `*` and a qualified reference are both grouped-valid.
SELECT * FROM q3gs GROUP BY ROLLUP(a, b, c, v) ORDER BY grouping(a, b, c, v), a, b, c, v;

SELECT q3gs.* FROM q3gs GROUP BY a, b, c, v ORDER BY a, b, c, v;

SELECT q3gs.a, sum(v) FROM q3gs GROUP BY ROLLUP(a) ORDER BY 1;

SELECT a, sum(v) FROM q3gs GROUP BY ROLLUP(q3gs.a) ORDER BY 1;

SELECT a, count(*) FROM q3gs GROUP BY ROLLUP(a) HAVING grouping(a) = 0 ORDER BY a;

-- Errors.
SELECT a, grouping(c) FROM q3gs GROUP BY ROLLUP(a);

SELECT grouping(a) FROM q3gs;

SELECT grouping(a) FROM q3gs GROUP BY b;

SELECT a, count(*) FROM q3gs GROUP BY 5;

SELECT a, count(*) FROM q3gs GROUP BY 0;

SELECT b AS a, count(*) FROM q3gs GROUP BY a ORDER BY 1;

SELECT c, count(*) FROM q3gs GROUP BY ROLLUP(a) ORDER BY 1;

SELECT count(*) FROM q3gs GROUP BY CUBE(a, b, c, a, b, c, a, b, c, a, b, c, a);

SELECT count(*) FROM q3gs GROUP BY ROLLUP(count(*));

SELECT a FROM q3gs WHERE grouping(a) = 0 GROUP BY ROLLUP(a);

SELECT a FROM q3gs GROUP BY grouping(a);

SELECT x.a FROM q3gs x JOIN q3gs y ON grouping(x.a) = 0 GROUP BY x.a;

SELECT * FROM q3gs GROUP BY a ORDER BY a;

DROP TABLE q3gs;
