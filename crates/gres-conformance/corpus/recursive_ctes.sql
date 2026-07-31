-- WITH RECURSIVE, plus WITH breadth: several items, an item referencing an
-- earlier one, MATERIALIZED / NOT MATERIALIZED hints, column alias lists, and
-- CTEs inside subqueries. Explicit ORDER BY keeps row order deterministic.

WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5)
SELECT n FROM t ORDER BY n;

WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 3)
SELECT n FROM t ORDER BY n;

WITH RECURSIVE t(n) AS (SELECT 1 UNION SELECT n + 1 FROM t WHERE n < 4)
SELECT n FROM t ORDER BY n;

-- UNION drops rows already produced, so a cyclic step still terminates.
WITH RECURSIVE t(n) AS (SELECT 1 UNION SELECT (n + 1) % 3 FROM t)
SELECT n FROM t ORDER BY n;

WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3)
SELECT sum(n), count(*), max(n) FROM t;

-- A recursive item with no self-reference is just a plain query.
WITH RECURSIVE t(n) AS (SELECT 1) SELECT n FROM t;

WITH RECURSIVE t AS (SELECT 1 AS i UNION ALL SELECT 2) SELECT i FROM t ORDER BY i;

-- The non-recursive term may itself be a UNION; the split is at the top-level one.
WITH RECURSIVE t(n) AS (
  SELECT 1 UNION ALL SELECT 10 UNION ALL SELECT n + 100 FROM t WHERE n < 100
)
SELECT n FROM t ORDER BY n;

-- Recursion over a real table: a tree walk and a transitive closure.
CREATE TABLE q3tree (id int4, parent int4, label text);
INSERT INTO q3tree VALUES (1,NULL,'a'),(2,1,'b'),(3,1,'c'),(4,2,'d'),(5,3,'e'),(6,3,'f');

WITH RECURSIVE walk (id, parent, label, depth) AS (
  SELECT id, parent, label, 0 FROM q3tree WHERE parent IS NULL
  UNION ALL
  SELECT c.id, c.parent, c.label, w.depth + 1 FROM q3tree c JOIN walk w ON c.parent = w.id
)
SELECT id, parent, label, depth FROM walk ORDER BY depth, id;

WITH RECURSIVE walk (id, depth) AS (
  SELECT id, 0 FROM q3tree WHERE id = 3
  UNION ALL
  SELECT c.id, w.depth + 1 FROM q3tree c, walk w WHERE c.parent = w.id
)
SELECT id, depth FROM walk ORDER BY id;

WITH RECURSIVE walk (id, path) AS (
  SELECT id, label FROM q3tree WHERE parent IS NULL
  UNION ALL
  SELECT c.id, w.path || '/' || c.label FROM q3tree c JOIN walk w ON c.parent = w.id
)
SELECT id, path FROM walk ORDER BY path;

CREATE TABLE q3edge (src int4, dst int4);
INSERT INTO q3edge VALUES (1,2),(2,3),(3,4),(4,2),(1,5);

WITH RECURSIVE reach (n) AS (
  SELECT 1 UNION SELECT e.dst FROM q3edge e JOIN reach r ON e.src = r.n
)
SELECT n FROM reach ORDER BY n;

WITH RECURSIVE reach (n, hops) AS (
  SELECT 1, 0
  UNION ALL
  SELECT e.dst, r.hops + 1 FROM q3edge e JOIN reach r ON e.src = r.n WHERE r.hops < 3
)
SELECT n, hops FROM reach ORDER BY hops, n;

-- The recursive term may filter, project and rename freely.
WITH RECURSIVE t(n, sq) AS (
  SELECT 1, 1 UNION ALL SELECT n + 1, (n + 1) * (n + 1) FROM t WHERE n < 4
)
SELECT n, sq FROM t ORDER BY n;

WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 10)
SELECT n FROM t WHERE n % 2 = 0 ORDER BY n;

WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 10)
SELECT n FROM t ORDER BY n DESC LIMIT 3;

-- A recursive item beside a plain item, and a plain item reading the recursive one.
WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 4),
     doubled AS (SELECT n * 2 AS d FROM nums)
SELECT d FROM doubled ORDER BY d;

WITH RECURSIVE seed AS (SELECT 2 AS s),
     nums(n) AS (SELECT s FROM seed UNION ALL SELECT n + 1 FROM nums WHERE n < 5)
SELECT n FROM nums ORDER BY n;

-- A recursive CTE inside a subquery and inside a derived table.
SELECT (WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 4)
        SELECT sum(n) FROM t);

SELECT d.total FROM (
  WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 4)
  SELECT sum(n) AS total FROM t
) d;

SELECT EXISTS (
  WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3)
  SELECT 1 FROM t WHERE n = 3
);

-- Plain WITH breadth.
WITH a AS (SELECT 1 AS x), b AS (SELECT x + 1 AS y FROM a), c AS (SELECT y * 10 AS z FROM b)
SELECT z FROM c;

WITH a(x, y) AS (VALUES (1, 'one'), (2, 'two'))
SELECT x, y FROM a ORDER BY x;

WITH a AS MATERIALIZED (SELECT 1 AS x) SELECT x FROM a;

WITH a AS NOT MATERIALIZED (SELECT 1 AS x) SELECT x FROM a;

WITH a AS MATERIALIZED (SELECT id, label FROM q3tree WHERE id < 3),
     b AS NOT MATERIALIZED (SELECT label FROM a)
SELECT label FROM b ORDER BY label;

WITH RECURSIVE t(n) AS MATERIALIZED (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3)
SELECT n FROM t ORDER BY n;

WITH outer_cte AS (SELECT 1 AS x)
SELECT * FROM (WITH inner_cte AS (SELECT 2 AS y) SELECT y FROM inner_cte) d;

WITH shadow AS (SELECT 1 AS x)
SELECT * FROM (WITH shadow AS (SELECT 2 AS x) SELECT x FROM shadow) d;

WITH a AS (SELECT 1 AS x)
SELECT x FROM a UNION ALL SELECT x + 1 FROM a ORDER BY 1;

WITH a AS (SELECT generate_series(1, 3) AS x)
SELECT count(*) FROM a;

-- Errors: PostgreSQL's recursion rules.
WITH RECURSIVE t(n) AS (SELECT n FROM t) SELECT n FROM t;

WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT x.n + y.n FROM t x, t y WHERE x.n < 3)
SELECT n FROM t;

WITH RECURSIVE t(n) AS (
  SELECT 1 UNION ALL SELECT t.n + 1 FROM q3tree LEFT JOIN t ON true WHERE t.n < 3
)
SELECT n FROM t;

WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT count(*) FROM t) SELECT n FROM t;

WITH RECURSIVE t(n) AS (
  SELECT 1 UNION ALL SELECT (SELECT max(n) FROM t) + 1 WHERE (SELECT max(n) FROM t) < 3
)
SELECT n FROM t;

WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT DISTINCT n + 1 FROM t WHERE n < 3)
SELECT n FROM t ORDER BY n;

WITH RECURSIVE x(i) AS (SELECT 1 UNION ALL SELECT i FROM y),
     y(i) AS (SELECT 1 UNION ALL SELECT i FROM x)
SELECT i FROM x;

WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3 ORDER BY 1)
SELECT n FROM t;

WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3 LIMIT 2)
SELECT n FROM t;

WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1, 2 FROM t WHERE n < 3)
SELECT n FROM t;

WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT 'x' FROM t) SELECT n FROM t;

WITH RECURSIVE t(n) AS (SELECT 1 INTERSECT SELECT n FROM t) SELECT n FROM t;

WITH RECURSIVE t(n) AS (SELECT 1 EXCEPT SELECT n FROM t) SELECT n FROM t;

WITH RECURSIVE t(n) AS (SELECT n + 1 FROM t UNION ALL SELECT 1) SELECT n FROM t;

WITH q3fwd_x(i) AS (SELECT * FROM q3fwd_y), q3fwd_y(i) AS (SELECT 1) SELECT i FROM q3fwd_x;

WITH a AS (SELECT 1 AS x), a AS (SELECT 2 AS x) SELECT x FROM a;

WITH a(x, y) AS (SELECT 1) SELECT x FROM a;

WITH RECURSIVE t AS (SELECT 1 AS n) SEARCH DEPTH FIRST BY n SET seq SELECT n FROM t;

WITH t AS (SELECT 1 AS n) SEARCH BREADTH FIRST BY n SET seq SELECT n FROM t;

WITH t AS (SELECT 1 AS n) CYCLE n SET is_cycle USING path SELECT n FROM t;

-- A column alias list SHORTER than the query is legal: the trailing columns keep
-- the names the query gave them. Only a LONGER list is 42P10.
WITH q3short(a) AS (SELECT 1 AS x, 2 AS y) SELECT * FROM q3short;

WITH q3short(a) AS (SELECT 1 AS x, 2 AS y) SELECT a, y FROM q3short;

WITH RECURSIVE q3short(n) AS (SELECT 1, 100 UNION ALL SELECT n + 1, 200 FROM q3short WHERE n < 3)
SELECT * FROM q3short ORDER BY 1;

WITH RECURSIVE q3short(n) AS (SELECT 1, 2 UNION ALL SELECT n + 1 FROM q3short WHERE n < 3)
SELECT n FROM q3short;

-- The recursive term is type-checked before the first round, so a term that
-- widens or clashes with the seeded type is 42804 rather than a runtime cast.
WITH RECURSIVE q3ty(n) AS (SELECT 1::int UNION ALL SELECT 1.5 FROM q3ty WHERE n < 3)
SELECT n FROM q3ty;

WITH RECURSIVE q3ty(n) AS (SELECT 1 UNION ALL SELECT n::text FROM q3ty WHERE n < 3)
SELECT n FROM q3ty;

WITH RECURSIVE q3ty(n) AS (SELECT 1 UNION ALL SELECT 'x' || n FROM q3ty WHERE n < 3)
SELECT n FROM q3ty;

WITH RECURSIVE q3ty(n) AS (SELECT 1::int2 UNION ALL SELECT n + 1 FROM q3ty WHERE n < 3)
SELECT n FROM q3ty;

WITH RECURSIVE q3ty(n) AS (SELECT 1::int8 UNION ALL SELECT (n + 1)::int4 FROM q3ty WHERE n < 3)
SELECT n FROM q3ty ORDER BY 1;

-- A FROM-clause sub-SELECT is not a "subquery" for the self-reference rule.
WITH RECURSIVE q3sub(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM (SELECT n FROM q3sub) q WHERE n < 3)
SELECT n FROM q3sub ORDER BY 1;

WITH RECURSIVE q3sub(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM (SELECT n FROM q3sub UNION ALL SELECT 9) q WHERE n < 3
)
SELECT n FROM q3sub ORDER BY 1;

-- An expression subquery still is one, in the term itself or inside a derived
-- table, and the nullable side of an outer join is still refused.
WITH RECURSIVE q3sub(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM q3sub WHERE n < (SELECT max(n) FROM q3sub)
)
SELECT n FROM q3sub;

WITH RECURSIVE q3sub(n) AS (
  SELECT 1 UNION ALL
  SELECT n + 1 FROM (SELECT n FROM q3sub WHERE EXISTS (SELECT 1 FROM q3sub)) q WHERE n < 3
)
SELECT n FROM q3sub;

WITH RECURSIVE q3sub(n) AS (
  SELECT 1 UNION ALL SELECT q.n + 1 FROM q3sub, (SELECT n FROM q3sub) q WHERE q3sub.n < 3
)
SELECT n FROM q3sub;

WITH RECURSIVE q3sub(n) AS (
  SELECT 1 UNION ALL
  SELECT q.n + 1 FROM q3tree LEFT JOIN (SELECT n FROM q3sub) q ON true WHERE q.n < 3
)
SELECT n FROM q3sub;

DROP TABLE q3edge;
DROP TABLE q3tree;

-- PostgreSQL scopes its "no aggregate in the recursive term" rule to the query
-- level that actually HOLDS the self-reference, not to the recursive term as a
-- whole. An aggregate one level ABOVE the self-reference is fine…
WITH RECURSIVE ra(n) AS (SELECT 1 UNION SELECT max(n) + 1 FROM (SELECT n FROM ra WHERE n < 3) q)
SELECT n FROM ra ORDER BY 1;
WITH RECURSIVE ra(n) AS (SELECT 1 UNION SELECT sum(n)::int + 1 FROM (SELECT n FROM ra WHERE n < 3) q)
SELECT n FROM ra ORDER BY 1;
WITH RECURSIVE ra(n) AS (SELECT 1 UNION SELECT count(*)::int + n FROM (SELECT n FROM ra WHERE n < 3) q GROUP BY n)
SELECT n FROM ra ORDER BY 1;
WITH RECURSIVE ra(n) AS (SELECT 1 UNION SELECT max(n)::int + 1 FROM (SELECT n FROM (SELECT n FROM ra WHERE n < 3) r) q)
SELECT n FROM ra ORDER BY 1;
-- …and an aggregate at the self-reference's OWN level is 42P19, in the select
-- list or in HAVING, however deeply that level is nested.
WITH RECURSIVE ra(n) AS (SELECT 1 UNION SELECT n + 1 FROM (SELECT max(n) AS n FROM ra WHERE n < 3) q WHERE n IS NOT NULL)
SELECT n FROM ra ORDER BY 1;
WITH RECURSIVE ra(n) AS (SELECT 1 UNION SELECT n + 1 FROM (SELECT n FROM ra WHERE n < 3 GROUP BY n HAVING count(*) > 0) q)
SELECT n FROM ra ORDER BY 1;
WITH RECURSIVE ra(n) AS (SELECT 1 UNION SELECT n + 1 FROM (SELECT n FROM (SELECT max(n) AS n FROM ra WHERE n < 3) r) q WHERE n IS NOT NULL)
SELECT n FROM ra ORDER BY 1;
WITH RECURSIVE ra(n) AS (SELECT 1 UNION SELECT max(n) + 1 FROM ra WHERE n < 3)
SELECT n FROM ra ORDER BY 1;
-- GROUP BY and DISTINCT at the self-reference's level stay legal.
WITH RECURSIVE ra(n) AS (SELECT 1 UNION SELECT n + 1 FROM (SELECT n FROM ra WHERE n < 3 GROUP BY n) q)
SELECT n FROM ra ORDER BY 1;
WITH RECURSIVE ra(n) AS (SELECT 1 UNION SELECT n + 1 FROM (SELECT DISTINCT n FROM ra WHERE n < 3) q)
SELECT n FROM ra ORDER BY 1;
