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
