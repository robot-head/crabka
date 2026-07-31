-- LATERAL FROM items, diffed against PostgreSQL 18.4.
-- Explicit `LATERAL (subquery)` and `LATERAL f(...)`, the implicit lateral rule
-- for a set-returning function whose arguments name an earlier FROM item
-- (`FROM t, unnest(t.tags)`), correlated evaluation per outer row, and the
-- LEFT JOIN LATERAL case where an outer row whose lateral side is empty must
-- still survive. Also the 42P01 a non-LATERAL derived table gets for the same
-- reference.
--
-- Every multi-row SELECT carries a fully determining ORDER BY.

CREATE TABLE q3_lat (id int4, n int4, tags text);
INSERT INTO q3_lat VALUES
  (1, 2, 'a,b'),
  (2, 0, ''),
  (3, 3, 'x,y,z'),
  (4, NULL, 'q');

-- Explicit LATERAL over a comma FROM item.
SELECT t.id, u.x FROM q3_lat t, LATERAL (SELECT t.n * 10 AS x) u ORDER BY t.id;
SELECT t.id, u.x FROM q3_lat t, LATERAL (SELECT t.id + t.n AS x) u ORDER BY t.id;
SELECT t.id, u.x FROM q3_lat t, LATERAL (SELECT t.tags AS x) u ORDER BY t.id;
SELECT t.id, u.x FROM q3_lat AS t, LATERAL (SELECT t.n AS x WHERE t.n > 1) u ORDER BY t.id;

-- CROSS JOIN LATERAL and JOIN ... ON with a lateral right side.
SELECT t.id, u.x FROM q3_lat t CROSS JOIN LATERAL (SELECT t.n + 1 AS x) u ORDER BY t.id;
SELECT t.id, u.x FROM q3_lat t JOIN LATERAL (SELECT t.n AS x) u ON true ORDER BY t.id;
SELECT t.id, u.x FROM q3_lat t JOIN LATERAL (SELECT t.n AS x) u ON u.x > 1 ORDER BY t.id;
SELECT t.id, u.x FROM q3_lat t INNER JOIN LATERAL (SELECT t.id AS x) u ON u.x = t.id ORDER BY t.id;

-- LEFT JOIN LATERAL keeps outer rows whose lateral side produced nothing.
SELECT t.id, u.x FROM q3_lat t LEFT JOIN LATERAL (SELECT t.n AS x WHERE t.n > 1) u ON true ORDER BY t.id;
SELECT t.id, u.x FROM q3_lat t LEFT JOIN LATERAL (SELECT t.n AS x WHERE false) u ON true ORDER BY t.id;
SELECT t.id, u.x FROM q3_lat t LEFT OUTER JOIN LATERAL (SELECT t.id AS x) u ON u.x > 2 ORDER BY t.id;

-- LATERAL over a set-returning function, with and without the keyword.
SELECT t.id, g FROM q3_lat t, LATERAL generate_series(1, t.n) g ORDER BY t.id, g;
SELECT t.id, g FROM q3_lat t, generate_series(1, t.n) g ORDER BY t.id, g;
SELECT t.id, g FROM q3_lat t CROSS JOIN LATERAL generate_series(1, t.n) g ORDER BY t.id, g;
SELECT t.id, g FROM q3_lat t LEFT JOIN LATERAL generate_series(1, t.n) g ON true ORDER BY t.id, g;
SELECT t.id, s FROM q3_lat t, string_to_table(t.tags, ',') s ORDER BY t.id, s;
SELECT t.id, s FROM q3_lat t LEFT JOIN LATERAL string_to_table(t.tags, ',') s ON true ORDER BY t.id, s;
SELECT t.id, u FROM q3_lat t, unnest(string_to_array(t.tags, ',')) u ORDER BY t.id, u;
SELECT t.id, g FROM q3_lat t, generate_series(t.id, t.id + 1) g ORDER BY t.id, g;

-- The implicit lateral rule with an explicit alias list on the function.
SELECT t.id, v.step FROM q3_lat t, generate_series(1, t.n) AS v(step) ORDER BY t.id, v.step;
SELECT t.id, v.piece FROM q3_lat t, string_to_table(t.tags, ',') AS v(piece) ORDER BY t.id, v.piece;

-- A lateral subquery that itself has a FROM clause referencing the outer item.
CREATE TABLE q3_lat_side (owner int4, amount int4);
INSERT INTO q3_lat_side VALUES (1, 5), (1, 9), (3, 4);
SELECT t.id, s.amount
  FROM q3_lat t, LATERAL (SELECT s.amount FROM q3_lat_side s WHERE s.owner = t.id) s
  ORDER BY t.id, s.amount;
SELECT t.id, s.amount
  FROM q3_lat t LEFT JOIN LATERAL (SELECT s.amount FROM q3_lat_side s WHERE s.owner = t.id) s ON true
  ORDER BY t.id, s.amount;
SELECT t.id, best.amount
  FROM q3_lat t, LATERAL (SELECT s.amount FROM q3_lat_side s WHERE s.owner = t.id ORDER BY s.amount DESC LIMIT 1) best
  ORDER BY t.id;
SELECT t.id, c.total
  FROM q3_lat t, LATERAL (SELECT count(*) AS total FROM q3_lat_side s WHERE s.owner = t.id) c
  ORDER BY t.id;

-- An inner FROM item that shadows the outer qualifier binds to the inner one.
SELECT t.id, inner_q.v
  FROM q3_lat t, LATERAL (SELECT t.amount AS v FROM q3_lat_side t WHERE t.owner = 1 ORDER BY t.amount LIMIT 1) inner_q
  ORDER BY t.id;

-- Three FROM items, the third correlated with the second.
SELECT a.id, b.id, g
  FROM q3_lat a, q3_lat b, LATERAL generate_series(1, b.n) g
  WHERE a.id = 1
  ORDER BY a.id, b.id, g;

-- LATERAL in a derived table's own FROM.
SELECT * FROM (SELECT t.id, g FROM q3_lat t, LATERAL generate_series(1, t.n) g) d ORDER BY d.id, d.g;

-- Without LATERAL a derived table may not see an earlier FROM item (42P01).
SELECT * FROM q3_lat t, (SELECT t.n AS x) u ORDER BY t.id;
SELECT * FROM q3_lat t JOIN (SELECT t.id AS x) u ON true;
SELECT * FROM q3_lat t LEFT JOIN (SELECT t.id AS x) u ON true;

-- A lateral reference to a FROM item that is to the RIGHT is still an error.
SELECT * FROM q3_lat t, LATERAL (SELECT u.id AS x) v, q3_lat u;

-- Non-correlated LATERAL is legal and behaves like an ordinary item.
SELECT t.id, u.x FROM q3_lat t, LATERAL (SELECT 7 AS x) u ORDER BY t.id;
SELECT g FROM LATERAL generate_series(1, 3) g ORDER BY g;
SELECT u.x FROM LATERAL (SELECT 1 AS x) u;

-- LATERAL over an empty outer relation produces no rows but keeps the columns.
SELECT t.id, u.x FROM q3_lat t, LATERAL (SELECT t.n AS x) u WHERE false ORDER BY t.id;
SELECT t.id, g FROM (SELECT id, n FROM q3_lat WHERE false) t, LATERAL generate_series(1, t.n) g ORDER BY t.id;

-- An unqualified name inside a lateral item resolves against the item's own FROM
-- first and falls back to the outer row only when nothing there supplies it.
SELECT t.id, u.amount FROM q3_lat t, LATERAL (SELECT amount FROM q3_lat_side WHERE owner = id) u ORDER BY t.id, u.amount;
SELECT t.id, u.owner FROM q3_lat t, LATERAL (SELECT owner FROM q3_lat_side WHERE q3_lat_side.owner = t.id) u ORDER BY t.id, u.owner;
SELECT t.id, u.amount FROM q3_lat t, LATERAL (SELECT amount FROM q3_lat_side s WHERE s.owner = id) u ORDER BY t.id, u.amount;
SELECT t.id, u.x FROM q3_lat t, LATERAL (SELECT tags AS x) u ORDER BY t.id;
SELECT t.id, u.amount FROM q3_lat t, LATERAL (SELECT amount FROM q3_lat_side WHERE owner = t.id ORDER BY amount LIMIT 1) u ORDER BY t.id;
-- `owner` and `amount` both belong to the inner FROM, so neither binds outward.
SELECT t.id, u.x FROM q3_lat t, LATERAL (SELECT owner AS x FROM q3_lat_side WHERE amount = n) u ORDER BY t.id, u.x;
SELECT t.id, u.x FROM q3_lat t, LATERAL (SELECT nosuch AS x) u ORDER BY t.id;

-- A CTE inside a lateral item sees the outer row too.
SELECT t.id, u.v FROM q3_lat t, LATERAL (WITH c AS (SELECT t.n AS v) SELECT * FROM c) u ORDER BY t.id;
SELECT t.id, u.v FROM q3_lat t, LATERAL (WITH c AS (SELECT n AS v) SELECT * FROM c) u ORDER BY t.id;

-- LATERAL on the nullable side of a RIGHT or FULL join is legal as long as the
-- item reads nothing from the other side; an actual reference is 42P10.
SELECT * FROM q3_lat_side RIGHT JOIN LATERAL (SELECT 7 AS x) u ON true ORDER BY owner, amount;
SELECT * FROM q3_lat_side FULL JOIN LATERAL (SELECT 7 AS x) u ON true ORDER BY owner, amount;
SELECT s.owner, g FROM q3_lat_side s RIGHT JOIN LATERAL generate_series(1, 2) g ON true ORDER BY s.owner, s.amount, g;
SELECT * FROM q3_lat_side LEFT JOIN LATERAL (SELECT q3_lat_side.owner AS x) u ON true ORDER BY owner, amount;
SELECT * FROM q3_lat_side RIGHT JOIN LATERAL (SELECT q3_lat_side.owner AS x) u ON true;
SELECT * FROM q3_lat_side FULL JOIN LATERAL (SELECT q3_lat_side.owner AS x) u ON true;
SELECT * FROM q3_lat_side RIGHT JOIN LATERAL (SELECT owner AS x) u ON true;
SELECT * FROM q3_lat_side RIGHT JOIN LATERAL generate_series(1, q3_lat_side.amount) g ON true;

DROP TABLE q3_lat_side;
DROP TABLE q3_lat;
