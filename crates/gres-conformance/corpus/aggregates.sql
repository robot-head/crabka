-- SP27: aggregates + GROUP BY / HAVING, diffed against PostgreSQL 18.
-- count(*)/count(x)/sum/min/max + DISTINCT, grouping, HAVING, empty-input, and
-- the grouping (42803) / undefined-function (42883) error cases. SUM(int4) is
-- bigint on both sides; output is ORDER BY-stable so the row diff is
-- deterministic (GROUP BY order is otherwise unspecified).
CREATE TABLE agg_sales (region text, amount int4);
INSERT INTO agg_sales VALUES ('west', 10), ('west', 20), ('east', 5), ('east', 5), ('north', 100);

-- whole-table aggregates (one row)
SELECT count(*) FROM agg_sales;
SELECT count(amount), sum(amount), min(amount), max(amount) FROM agg_sales;
SELECT count(DISTINCT amount) FROM agg_sales;
SELECT max(region) FROM agg_sales;

-- grouped
SELECT region, count(*), sum(amount) FROM agg_sales GROUP BY region ORDER BY region;
SELECT region, sum(amount) FROM agg_sales GROUP BY region HAVING sum(amount) > 10 ORDER BY sum(amount) DESC;
SELECT region FROM agg_sales GROUP BY region ORDER BY region;
SELECT amount, count(*) FROM agg_sales GROUP BY amount ORDER BY amount;

-- empty-input behaviors: bare aggregate -> one row (count 0, sum NULL); grouped -> zero rows
CREATE TABLE agg_empty (v int4);
SELECT count(*), sum(v) FROM agg_empty;
SELECT v, count(*) FROM agg_empty GROUP BY v ORDER BY v;

-- error parity (same SQLSTATE on both sides)
SELECT region, amount FROM agg_sales GROUP BY region;
SELECT frobnicate(amount) FROM agg_sales;

-- `agg(...) FILTER (WHERE predicate)` on a plain (non-window) aggregate. The
-- predicate is applied per source row BEFORE the argument is evaluated, so a
-- rejected row does not count for count(*) and never enters the DISTINCT buffer;
-- a NULL predicate rejects the row, as a WHERE clause would.
CREATE TABLE ft (g int, v int);
INSERT INTO ft VALUES (1,1),(1,2),(1,3),(2,1),(2,3),(2,NULL);
SELECT count(*) FILTER (WHERE v > 1) FROM ft;
SELECT count(v) FILTER (WHERE v > 1) FROM ft;
SELECT sum(v) FILTER (WHERE v > 1), avg(v) FILTER (WHERE v > 1) FROM ft;
SELECT min(v) FILTER (WHERE v > 1), max(v) FILTER (WHERE v > 1) FROM ft;
SELECT g, count(*) FILTER (WHERE v > 1) FROM ft GROUP BY g ORDER BY g;
SELECT g, count(*) FILTER (WHERE v > 100) FROM ft GROUP BY g ORDER BY g;
SELECT g, sum(v) FILTER (WHERE v > 100) FROM ft GROUP BY g ORDER BY g;
SELECT g, array_agg(v) FILTER (WHERE v > 1) FROM ft GROUP BY g ORDER BY g;
SELECT g, count(DISTINCT v) FILTER (WHERE v <> 2) FROM ft GROUP BY g ORDER BY g;
SELECT g, array_agg(DISTINCT v) FILTER (WHERE v <> 2) FROM ft GROUP BY g ORDER BY g;
SELECT g, sum(DISTINCT v) FILTER (WHERE v <> 2) FROM ft GROUP BY g ORDER BY g;
SELECT count(*) FILTER (WHERE v IS NULL) FROM ft;
SELECT count(*) FILTER (WHERE NULL) FROM ft;
SELECT g FROM ft GROUP BY g HAVING count(*) FILTER (WHERE v > 1) > 1 ORDER BY g;
SELECT g, count(*) FILTER (WHERE v > 1), count(*) FROM ft GROUP BY g ORDER BY g;
SELECT string_agg(v::text, ',') FILTER (WHERE v > 1) FROM ft;
SELECT count(*) FILTER (WHERE 1) FROM ft;
SELECT count(*) FILTER (WHERE count(*) > 0) FROM ft;
DROP TABLE ft;
