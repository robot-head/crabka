-- Q2: window functions — OVER, PARTITION BY, ORDER BY, named windows, the
-- window-function set, aggregates over a window, FILTER, and the placement
-- errors. Diffed against PostgreSQL 18.4.
--
-- Every query carries a total ORDER BY: the row order of a window query with no
-- ORDER BY is unspecified in PostgreSQL, so an unordered statement would diff
-- against itself between runs.
CREATE TABLE win_sales (region text, quarter int4, amount int4);
INSERT INTO win_sales VALUES
  ('west', 1, 10), ('west', 2, 20), ('west', 3, 20),
  ('east', 1, 5), ('east', 2, 30), ('east', 3, NULL),
  ('north', 1, 100);

-- the ranking family over one ordering
SELECT region, quarter, row_number() OVER (ORDER BY region, quarter) FROM win_sales ORDER BY region, quarter;
SELECT amount, rank() OVER (ORDER BY amount), dense_rank() OVER (ORDER BY amount) FROM win_sales ORDER BY amount, rank() OVER (ORDER BY amount);
SELECT amount, percent_rank() OVER (ORDER BY amount), cume_dist() OVER (ORDER BY amount) FROM win_sales ORDER BY amount NULLS FIRST;
SELECT quarter, ntile(2) OVER (ORDER BY quarter, region), ntile(3) OVER (ORDER BY quarter, region), ntile(99) OVER (ORDER BY quarter, region) FROM win_sales ORDER BY quarter, region;

-- an empty OVER () is one partition of every row, unordered: every row is a peer
SELECT region, count(*) OVER (), sum(amount) OVER (), max(amount) OVER () FROM win_sales ORDER BY region, quarter;

-- PARTITION BY splits the ranking; NULL partition keys form their own partition
SELECT region, quarter, rank() OVER (PARTITION BY region ORDER BY quarter DESC) FROM win_sales ORDER BY region, quarter;
SELECT region, amount, sum(amount) OVER (PARTITION BY region) FROM win_sales ORDER BY region, quarter;
SELECT region, count(*) OVER (PARTITION BY region), count(amount) OVER (PARTITION BY region) FROM win_sales ORDER BY region, quarter;

-- ASC/DESC and NULLS FIRST/LAST inside the window's own ORDER BY
SELECT amount, rank() OVER (ORDER BY amount ASC NULLS FIRST) FROM win_sales ORDER BY amount NULLS FIRST;
SELECT amount, rank() OVER (ORDER BY amount ASC NULLS LAST) FROM win_sales ORDER BY amount NULLS FIRST;
SELECT amount, rank() OVER (ORDER BY amount DESC NULLS FIRST) FROM win_sales ORDER BY amount NULLS FIRST;
SELECT amount, rank() OVER (ORDER BY amount DESC NULLS LAST) FROM win_sales ORDER BY amount NULLS FIRST;
SELECT amount, sum(amount) OVER (ORDER BY amount DESC) FROM win_sales ORDER BY amount NULLS FIRST;

-- lag / lead, with offset and default arguments
SELECT quarter, region, lag(amount) OVER w, lead(amount) OVER w FROM win_sales WINDOW w AS (ORDER BY quarter, region) ORDER BY quarter, region;
SELECT quarter, region, lag(amount, 2) OVER w, lag(amount, 2, -1) OVER w FROM win_sales WINDOW w AS (ORDER BY quarter, region) ORDER BY quarter, region;
SELECT quarter, region, lead(amount, 2, -1) OVER w, lag(amount, 0) OVER w FROM win_sales WINDOW w AS (ORDER BY quarter, region) ORDER BY quarter, region;
SELECT quarter, lag(region, 1, 'none') OVER (PARTITION BY quarter ORDER BY region) FROM win_sales ORDER BY quarter, region;
-- a NULL offset yields NULL rather than an error
SELECT quarter, lag(amount, NULL) OVER (ORDER BY quarter, region) FROM win_sales ORDER BY quarter, region;
-- a negative lag offset reads forwards, exactly like lead
SELECT quarter, region, lag(amount, -1) OVER w, lead(amount, -1) OVER w FROM win_sales WINDOW w AS (ORDER BY quarter, region) ORDER BY quarter, region;

-- first_value / last_value / nth_value read the frame, so the default frame
-- (through the current row's last peer) makes last_value the current row
SELECT quarter, region, first_value(amount) OVER w, last_value(amount) OVER w, nth_value(amount, 2) OVER w FROM win_sales WINDOW w AS (ORDER BY quarter, region) ORDER BY quarter, region;
SELECT quarter, region, nth_value(amount, 99) OVER (ORDER BY quarter, region) FROM win_sales ORDER BY quarter, region;
SELECT quarter, nth_value(amount, NULL) OVER (ORDER BY quarter, region) FROM win_sales ORDER BY quarter, region;

-- named windows: a bare OVER name, a definition reused by two calls, and one
-- window built from another
SELECT region, quarter, rank() OVER w, count(*) OVER w FROM win_sales WINDOW w AS (PARTITION BY region ORDER BY quarter) ORDER BY region, quarter;
SELECT region, quarter, count(*) OVER w1, count(*) OVER w2, rank() OVER w2 FROM win_sales WINDOW w1 AS (PARTITION BY region), w2 AS (w1 ORDER BY quarter) ORDER BY region, quarter;
SELECT region, count(*) OVER (w1) FROM win_sales WINDOW w1 AS (PARTITION BY region) ORDER BY region, quarter;
SELECT region, count(*) OVER (w1 ORDER BY quarter) FROM win_sales WINDOW w1 AS (PARTITION BY region) ORDER BY region, quarter;
-- an unused WINDOW definition is legal
SELECT region FROM win_sales WINDOW unused AS (ORDER BY amount) ORDER BY region, quarter;

-- ordinary aggregates as window functions, including the collecting ones
SELECT region, quarter, avg(amount) OVER (PARTITION BY region), min(amount) OVER (PARTITION BY region) FROM win_sales ORDER BY region, quarter;
SELECT region, quarter, string_agg(region, ',') OVER (ORDER BY quarter, region) FROM win_sales ORDER BY quarter, region;
SELECT region, quarter, array_agg(quarter) OVER (PARTITION BY region ORDER BY quarter) FROM win_sales ORDER BY region, quarter;
SELECT region, bool_or(amount > 10) OVER (PARTITION BY region), bool_and(amount > 0) OVER (PARTITION BY region) FROM win_sales ORDER BY region, quarter;
SELECT quarter, sum(amount) OVER (ORDER BY quarter, region) / 5 + 1 FROM win_sales ORDER BY quarter, region;

-- FILTER restricts the rows an aggregate window function folds
SELECT region, quarter, count(*) FILTER (WHERE amount > 10) OVER (ORDER BY quarter, region) FROM win_sales ORDER BY quarter, region;
SELECT region, sum(amount) FILTER (WHERE region <> 'east') OVER (PARTITION BY quarter) FROM win_sales ORDER BY quarter, region;

-- window functions run AFTER WHERE and BEFORE DISTINCT / ORDER BY / LIMIT
SELECT region, rank() OVER (ORDER BY region) FROM win_sales WHERE amount IS NOT NULL ORDER BY region, quarter;
SELECT DISTINCT rank() OVER (ORDER BY quarter) FROM win_sales ORDER BY 1;
SELECT quarter, row_number() OVER (ORDER BY quarter, region) FROM win_sales ORDER BY quarter, region LIMIT 3;
SELECT quarter, sum(amount) OVER () FROM win_sales ORDER BY quarter, region LIMIT 2;
SELECT quarter, count(*) OVER () FROM win_sales ORDER BY quarter, region OFFSET 5;

-- window over a grouped query: the window sees the GROUP BY output
SELECT region, sum(amount), rank() OVER (ORDER BY sum(amount) DESC) FROM win_sales GROUP BY region ORDER BY region;
SELECT region, sum(sum(amount)) OVER (ORDER BY region) FROM win_sales GROUP BY region ORDER BY region;
SELECT region, count(*), row_number() OVER (ORDER BY count(*) DESC, region) FROM win_sales GROUP BY region ORDER BY region;
SELECT region, sum(amount) FROM win_sales GROUP BY region HAVING sum(amount) > 20 ORDER BY rank() OVER (ORDER BY region);

-- window functions in a subquery and a derived table
SELECT * FROM (SELECT region, rank() OVER (ORDER BY amount DESC NULLS LAST) AS r FROM win_sales) d WHERE d.r <= 2 ORDER BY d.r, d.region;
SELECT region FROM win_sales WHERE amount = (SELECT max(amount) FROM win_sales) ORDER BY region;

-- a window query as a set-operation branch, a CTE body, and a derived table
SELECT region, rank() OVER (ORDER BY region) FROM win_sales UNION ALL SELECT region, 0 FROM win_sales ORDER BY 1, 2;
SELECT region, rank() OVER (ORDER BY region) FROM win_sales UNION SELECT region, 1 FROM win_sales ORDER BY 1, 2;
WITH ranked AS (SELECT region, quarter, rank() OVER (PARTITION BY region ORDER BY quarter) AS r FROM win_sales) SELECT * FROM ranked ORDER BY region, quarter;
SELECT * FROM (SELECT region, sum(amount) OVER (PARTITION BY region) AS s FROM win_sales) d ORDER BY region, s NULLS LAST;

-- a scalar subquery inside a window call's argument and its window specification
SELECT region, sum((SELECT 1)) OVER (PARTITION BY region) FROM win_sales ORDER BY region, quarter;
SELECT region, count(*) OVER (PARTITION BY (SELECT 1) ORDER BY region) FROM win_sales ORDER BY region, quarter;

-- a FROM-less SELECT may still carry a WINDOW clause
SELECT 1 WINDOW unused AS ();

-- a window result combined with other expressions over a grouped query
SELECT region, rank() OVER (ORDER BY region) IN (1, 3) FROM win_sales GROUP BY region ORDER BY region;
SELECT region, ARRAY[rank() OVER (ORDER BY region)] FROM win_sales GROUP BY region ORDER BY region;
SELECT region, CASE WHEN rank() OVER (ORDER BY region) = 1 THEN 'first' ELSE 'rest' END FROM win_sales GROUP BY region ORDER BY region;
SELECT region, (rank() OVER (ORDER BY region))::text, -rank() OVER (ORDER BY region) FROM win_sales GROUP BY region ORDER BY region;
SELECT region, coalesce(lag(sum(amount)) OVER (ORDER BY region), -1) FROM win_sales GROUP BY region ORDER BY region;

-- output column labels: an unaliased window call is named after its function
SELECT row_number() OVER (ORDER BY region, quarter), rank() OVER (ORDER BY region) FROM win_sales ORDER BY 1;
SELECT rank() OVER (ORDER BY region) AS r FROM win_sales ORDER BY r, 1;

-- SELECT * beside a window call expands only the relation's columns
SELECT *, row_number() OVER (ORDER BY region, quarter) FROM win_sales ORDER BY region, quarter;

-- a window function over an empty relation produces no rows
CREATE TABLE win_empty (v int4);
SELECT v, rank() OVER (ORDER BY v), count(*) OVER () FROM win_empty ORDER BY v;

-- error parity: same SQLSTATE on both sides
SELECT row_number() FROM win_sales;
SELECT * FROM win_sales WHERE row_number() OVER () > 1;
SELECT region FROM win_sales GROUP BY row_number() OVER ();
SELECT region FROM win_sales HAVING row_number() OVER () > 1;
SELECT sum(DISTINCT amount) OVER () FROM win_sales;
SELECT row_number() FILTER (WHERE amount > 1) OVER () FROM win_sales;
SELECT count(*) OVER nosuchwindow FROM win_sales;
SELECT count(*) OVER (w PARTITION BY region) FROM win_sales WINDOW w AS (PARTITION BY quarter);
SELECT count(*) OVER (w ORDER BY region) FROM win_sales WINDOW w AS (PARTITION BY quarter ORDER BY amount);
SELECT count(*) OVER (w) FROM win_sales WINDOW w AS (ORDER BY quarter ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING);
SELECT count(*) OVER () FROM win_sales WINDOW w AS (), w AS ();
SELECT ntile(0) OVER () FROM win_sales;
SELECT ntile(-1) OVER () FROM win_sales;
SELECT nth_value(amount, 0) OVER () FROM win_sales;
SELECT lag() OVER () FROM win_sales;
SELECT first_value(amount, 2) OVER () FROM win_sales;
SELECT nosuchwindowfunc() OVER () FROM win_sales;
SELECT * FROM win_sales JOIN win_sales b ON row_number() OVER () = b.quarter;
SELECT count(*) OVER () FROM win_sales LIMIT row_number() OVER ();
SELECT count(*) OVER () FROM win_sales OFFSET row_number() OVER ();

-- ntile reads its argument ONCE per partition, from that partition's first row
-- in window order: a later row's value is never looked at, not even a zero.
SELECT quarter, ntile(CASE WHEN quarter = 1 THEN 2 ELSE 6 END) OVER (ORDER BY quarter, region) FROM win_sales ORDER BY quarter, region;
SELECT quarter, ntile(CASE WHEN quarter = 2 THEN 0 ELSE 2 END) OVER (ORDER BY quarter, region) FROM win_sales ORDER BY quarter, region;
SELECT quarter, ntile(CASE WHEN quarter = 3 THEN 2 ELSE 3 END) OVER (ORDER BY quarter DESC, region DESC) FROM win_sales ORDER BY quarter, region;
SELECT region, quarter, ntile(CASE WHEN quarter = 1 THEN 1 ELSE 9 END) OVER (PARTITION BY region ORDER BY quarter) FROM win_sales ORDER BY region, quarter;
-- a NULL there is that row's own result and leaves the run unarmed, so the next
-- row arms it — over the whole partition's row count
SELECT quarter, ntile(CASE WHEN quarter = 1 THEN NULL ELSE 2 END) OVER (ORDER BY quarter, region) FROM win_sales ORDER BY quarter, region;
SELECT quarter, ntile(CASE WHEN quarter = 1 THEN 0 ELSE 2 END) OVER (ORDER BY quarter, region) FROM win_sales;

-- lag/lead are declared over anycompatible: the value and the default resolve to
-- ONE type, and the column carries only that type
SELECT quarter, lag(amount, 1, 5.5) OVER (ORDER BY quarter, region) FROM win_sales ORDER BY quarter, region;
SELECT quarter, lag(amount, 1, '7') OVER (ORDER BY quarter, region) FROM win_sales ORDER BY quarter, region;
SELECT pg_typeof(lag(amount, 1, 5.5) OVER (ORDER BY quarter, region)) FROM win_sales ORDER BY quarter, region LIMIT 1;
SELECT pg_typeof(lag(amount::float4, 1, 5.5) OVER (ORDER BY quarter, region)) FROM win_sales ORDER BY quarter, region LIMIT 1;
SELECT lag(amount, 1, 'zzz') OVER (ORDER BY quarter) FROM win_sales;
SELECT lead(amount, 1, 'zzz') OVER (ORDER BY quarter) FROM win_sales;
SELECT lag(region, 1, 5) OVER (ORDER BY quarter) FROM win_sales;
-- the counting parameter of ntile/lag/lead/nth_value is integer; smallint widens
-- into it, bigint and numeric have no overload
SELECT quarter, lag(amount, 2::smallint) OVER (ORDER BY quarter, region) FROM win_sales ORDER BY quarter, region;
SELECT lag(amount, 9223372036854775807) OVER (ORDER BY quarter) FROM win_sales;
SELECT lag(amount, 2::bigint) OVER (ORDER BY quarter) FROM win_sales;
SELECT nth_value(amount, 9223372036854775807) OVER () FROM win_sales;
SELECT nth_value(amount, 2.0) OVER () FROM win_sales;
SELECT ntile(3::bigint) OVER (ORDER BY quarter) FROM win_sales;
SELECT ntile(2.5) OVER (ORDER BY quarter) FROM win_sales;

-- OVER on a real function that is neither a window function nor an aggregate is
-- 42809; a name nothing matches stays 42883
SELECT upper(region) OVER () FROM win_sales;
SELECT abs(amount) OVER () FROM win_sales;
SELECT nosuchfn(region) OVER () FROM win_sales;
SELECT sum(region) OVER () FROM win_sales;

-- a window definition is evaluated below every window call, so it cannot contain one
SELECT count(*) OVER (ORDER BY rank() OVER ()) FROM win_sales;
SELECT count(*) OVER (PARTITION BY row_number() OVER ()) FROM win_sales;
SELECT rank() OVER (ORDER BY row_number() OVER ()) FROM win_sales;
SELECT count(*) OVER w FROM win_sales WINDOW w AS (ORDER BY rank() OVER ());

-- a locking read has no base-table row for a window result to belong to
SELECT quarter, row_number() OVER () FROM win_sales FOR UPDATE;
SELECT quarter, row_number() OVER () FROM win_sales FOR SHARE;

-- windows run over the GROUPED rows, including every extra grouping-set row
SELECT region, count(*), count(*) OVER () FROM win_sales GROUP BY ROLLUP(region) ORDER BY region NULLS LAST, 2;
SELECT region, count(*), count(*) OVER () FROM win_sales GROUP BY CUBE(region) ORDER BY region NULLS LAST, 2;
SELECT region, count(*), count(*) OVER () FROM win_sales GROUP BY GROUPING SETS ((region),(region)) ORDER BY region NULLS LAST;
SELECT sum(count(*)) OVER () FROM win_sales GROUP BY GROUPING SETS ((region),());
SELECT region, grouping(region), count(*) OVER () FROM win_sales GROUP BY ROLLUP(region) ORDER BY region NULLS LAST, 2;
SELECT region, count(*), sum(count(*)) OVER (ORDER BY region NULLS LAST) FROM win_sales GROUP BY ROLLUP(region) ORDER BY region NULLS LAST, 2;
-- a SQL92 output reference in GROUP BY names the original select list
SELECT region, count(*), count(*) OVER () FROM win_sales GROUP BY 1 ORDER BY region NULLS LAST;
SELECT region AS r, count(*), count(*) OVER () FROM win_sales GROUP BY r ORDER BY r NULLS LAST;

-- DISTINCT ON keys and ORDER BY keys match on the same window call
SELECT DISTINCT ON (row_number() OVER ()) quarter FROM win_sales ORDER BY row_number() OVER (), quarter;
SELECT DISTINCT ON (rank() OVER (ORDER BY region)) quarter FROM win_sales ORDER BY rank() OVER (ORDER BY region), quarter;
SELECT DISTINCT ON (rank() OVER (ORDER BY region)) quarter FROM win_sales ORDER BY quarter;

-- a window call in a SUBQUERY's tail belongs to that subquery
SELECT quarter FROM (SELECT quarter FROM win_sales ORDER BY rank() OVER (ORDER BY quarter DESC, region DESC) LIMIT 2) q ORDER BY quarter;
SELECT quarter FROM win_sales WHERE quarter IN (SELECT quarter FROM win_sales ORDER BY rank() OVER (ORDER BY quarter, region) LIMIT 2) ORDER BY quarter;

-- WINDOW/OVER/FILTER as identifiers: none may be a bare column label, `AS` takes
-- all three, and only the unreserved two may alias a FROM item
SELECT quarter AS window, quarter AS over, quarter AS filter FROM win_sales ORDER BY 1 LIMIT 1;
SELECT quarter window FROM win_sales;
SELECT quarter over FROM win_sales;
SELECT quarter filter FROM win_sales;
SELECT * FROM win_sales AS window;
SELECT * FROM win_sales AS fetch;
SELECT count(*) FROM win_sales over;
SELECT count(*) FROM win_sales AS over;
SELECT count(*) FROM win_sales filter;

-- A window specification is a query level of its own: PostgreSQL's ban on a
-- window call inside one applies to the SELECT that OWNS the specification, so
-- a call written in a subquery nested inside it is legal.
SELECT count(*) OVER (ORDER BY (SELECT rank() OVER ())) FROM win_sales ORDER BY 1;
SELECT count(*) OVER (PARTITION BY (SELECT count(*) OVER () FROM win_sales x LIMIT 1)) FROM win_sales ORDER BY 1;
SELECT count(*) OVER (ORDER BY (SELECT max(r) FROM (SELECT row_number() OVER () r FROM win_sales y) s)) FROM win_sales ORDER BY 1;
SELECT count(*) OVER (ORDER BY (SELECT max(quarter) FROM win_sales x)) FROM win_sales ORDER BY 1;
-- …while a call written directly in the specification is still 42P20.
SELECT count(*) OVER (ORDER BY rank() OVER ()) FROM win_sales;
SELECT count(*) OVER (PARTITION BY row_number() OVER ()) FROM win_sales;
SELECT count(*) OVER (ORDER BY (SELECT 1), rank() OVER ()) FROM win_sales;

-- An aggregate's own ORDER BY over a window is PostgreSQL's 0A000.
SELECT array_agg(amount ORDER BY amount) OVER (ORDER BY quarter) FROM win_sales;
SELECT array_agg(amount ORDER BY amount DESC NULLS LAST) OVER () FROM win_sales;
SELECT array_agg(amount ORDER BY amount) FILTER (WHERE amount > 5) OVER () FROM win_sales;
