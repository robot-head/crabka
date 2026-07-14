-- Non-recursive, read-only CTEs. Explicit ORDER BY keeps row order deterministic.

WITH c AS (SELECT 1 AS x)
SELECT x FROM c;

WITH a AS (VALUES (1), (2)), b AS (SELECT column1 + 10 AS y FROM a)
SELECT y FROM b ORDER BY y;

WITH c(x) AS (VALUES (3), (1), (2))
SELECT x FROM c ORDER BY x;

WITH u(x) AS (SELECT 1 UNION SELECT 2)
SELECT x FROM u ORDER BY x DESC;

CREATE TABLE cte_base (id int4);
INSERT INTO cte_base VALUES (9);
WITH cte_base AS (SELECT 1 AS id)
SELECT id FROM cte_base;

WITH outer_cte AS (VALUES (1))
SELECT * FROM (WITH outer_cte AS (VALUES (2)) SELECT * FROM outer_cte) AS d(x);

WITH c AS (VALUES (1))
SELECT EXISTS (WITH d AS (SELECT * FROM c) SELECT 1 FROM d);
